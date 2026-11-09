import asyncio
import json
import subprocess
import sys
from pathlib import Path

import httpx
import pytest

from xmlriver_mcp.batch import build_tasks, run_batch
from xmlriver_mcp.client import XmlRiverClient
from xmlriver_mcp.config import Config
from xmlriver_mcp.models import BatchArgs
from xmlriver_mcp.query_batch import run_queries
from xmlriver_mcp.storage import DiskRecords, Storage

XML = "<yandexsearch><response><results><doc><url>https://example.org</url></doc></results></response></yandexsearch>"


@pytest.mark.parametrize("mode", ["positions", "queries"])
async def test_hard_crash_keeps_page_and_resumes_next_page(tmp_path, mode):
    proc = subprocess.run(
        [sys.executable, str(Path(__file__).with_name("crash_fixture.py")), str(tmp_path), mode],
        timeout=30,
    )
    assert proc.returncode == 23
    store = Storage(tmp_path)
    run_id = next((tmp_path / "runs").iterdir()).name
    assert store.read_manifest(run_id)["status"] == "running"
    journal = store.run_path(run_id) / "journal.jsonl"
    with journal.open("ab") as stream:
        stream.write(b'{"index":')  # Also simulate an interrupted last write.
    rows = store.read_run(run_id)
    assert rows[0]["http_attempts"] == 2  # Intent is durable even before the request completes.
    page = rows[0] if mode == "positions" else rows[0]["result"]
    assert len(page["results"]) == 1
    assert page["successful_pages"] == 1
    recovered = store.recover_run(run_id)
    assert recovered["status"] == "aborted"
    assert store.read_run(run_id)[0]["http_attempts"] == 2
    calls = []

    def handler(request):
        calls.append(request.url.params["page"])
        return httpx.Response(200, text=XML)

    async with XmlRiverClient("test", "test", transport=httpx.MockTransport(handler)) as client:
        if mode == "positions":
            cfg = Config(target_domain="example.org", depth=30)
            result = await run_batch(cfg, build_tasks(cfg, ["q"]), store, client=client)
            row = store.read_run(result["run_id"])[0]
        else:
            result = await run_queries(
                client,
                BatchArgs(operation="serp", queries=["q"], parameters={"pages": 3}),
                store=store,
            )
            row = result["results"][0]["result"]
    assert calls == ["1", "2"]
    assert row["resumed_pages"] == 1
    assert len(row["results"]) == 3
    assert result["http_attempts"] == result["successful_pages"] == 2


async def test_cancel_all_workers_keeps_journal(tmp_path):
    started = asyncio.Event()
    active = 0

    async def handler(request):
        nonlocal active
        active += 1
        if active == 9:
            started.set()
        await asyncio.sleep(60)
        return httpx.Response(200, text=XML)

    store = Storage(tmp_path)
    async with XmlRiverClient("test", "test", transport=httpx.MockTransport(handler)) as client:
        task = asyncio.create_task(
            run_queries(
                client,
                BatchArgs(operation="serp", queries=[str(i) for i in range(30)]),
                store=store,
            )
        )
        await asyncio.wait_for(started.wait(), 5)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert client.gate.active == 0
    run_id = next((tmp_path / "runs").iterdir()).name
    assert store.read_manifest(run_id)["http_attempts"] == 9
    rows = store.read_run(run_id)
    assert sum(r["http_attempts"] for r in rows) == 9
    assert sum(r["status"] == "not_started" for r in rows) == 21


async def test_cli_result_payloads_are_on_disk(tmp_path):
    def handler(request):
        return httpx.Response(200, json={"popular": [{"text": "x" * 50000, "value": 1}]})

    async with XmlRiverClient(
        "test", "test", rate_max=100, transport=httpx.MockTransport(handler)
    ) as client:
        result = await run_queries(
            client,
            BatchArgs(operation="wordstat_top", queries=[str(i) for i in range(20)]),
            store=Storage(tmp_path),
            return_results=False,
        )
    assert "results" not in result
    assert result["completed_tasks"] == 20
    assert Path(result["results_path"]).stat().st_size > 1000000
    assert Path(result["journal_path"]).exists()


def test_disk_records_evict_payloads(tmp_path):
    records = DiskRecords(tmp_path / "journal.jsonl")
    records.append_pending({"status": "pending"})
    records[0] = {"status": "ok", "result": ["payload" * 10000]}
    assert isinstance(records.entries[0], int)
    assert "result" not in records.summaries[0]
    assert records[0]["result"][0].startswith("payload")
    assert json.loads(records.path.read_text())["record"]["status"] == "ok"
