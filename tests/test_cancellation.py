import asyncio
from concurrent.futures import ProcessPoolExecutor

import httpx
import pytest

from xmlriver_mcp.batch import build_tasks, run_batch
from xmlriver_mcp.client import XmlRiverClient
from xmlriver_mcp.config import Config
from xmlriver_mcp.storage import Storage


async def test_cancel_preserves_partial_run(tmp_path):
    reached = asyncio.Event()
    calls = 0

    async def handler(request):
        nonlocal calls
        calls += 1
        if calls > 1:
            reached.set()
            await asyncio.sleep(60)
        return httpx.Response(
            200,
            text="<yandexsearch><response><results><doc><url>https://example.org</url></doc></results></response></yandexsearch>",
        )

    store = Storage(tmp_path)
    cfg = Config(target_domain="example.org", depth=30)
    async with XmlRiverClient(
        "user", "secret", transport=httpx.MockTransport(handler), rate_max=100
    ) as client:
        task = asyncio.create_task(
            run_batch(cfg, build_tasks(cfg, ["coffee"]), store, client=client)
        )
        await reached.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    manifest_path = next((tmp_path / "runs").glob("*/manifest.json"))
    manifest = store.read_manifest(manifest_path.parent.name)
    assert manifest["status"] == "aborted"
    assert manifest["http_attempts"] == 2
    assert manifest["successful_pages"] == 1
    assert len(store.read_run(manifest["run_id"])[0]["results"]) == 1
    assert calls == 2


def write_history(root, n):
    from datetime import datetime, timezone
    from pathlib import Path

    store = Storage(Path(root))
    row = dict(
        query=str(n),
        engine="yandex",
        region=213,
        device="mobile",
        pages=1,
        target_domain="example.org",
        include_subdomains=True,
        complete=True,
        source="api",
        fetched_at=datetime.now(timezone.utc).isoformat(),
        results=[],
    )
    store.save_cache(row)
    store.append_history([row], dict(run_id=str(n), created_at=row["fetched_at"]))


def test_concurrent_writers(tmp_path):
    with ProcessPoolExecutor(max_workers=3) as pool:
        futures = [pool.submit(write_history, str(tmp_path), n) for n in range(12)]
        for future in futures:
            future.result(timeout=30)
    assert len(Storage(tmp_path).history()) == 12
