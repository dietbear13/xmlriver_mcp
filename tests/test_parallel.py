import asyncio
import time

import httpx
import pytest
from pydantic import ValidationError

from xmlriver_mcp.batch import build_tasks, run_batch
from xmlriver_mcp.client import XmlRiverClient
from xmlriver_mcp.concurrency import AdaptiveGate
from xmlriver_mcp.config import Config
from xmlriver_mcp.errors import XmlRiverError
from xmlriver_mcp.models import BatchArgs
from xmlriver_mcp.query_batch import run_queries
from xmlriver_mcp.storage import Storage

XML = "<yandexsearch><response><results><doc><url>https://example.org</url></doc></results></response></yandexsearch>"


def api(handler, **kwargs):
    return XmlRiverClient(
        "user", "secret", rate_max=1000, transport=httpx.MockTransport(handler), **kwargs
    )


async def test_nine_in_flight_and_per_phrase_accounting(tmp_path):
    active = peak = 0

    async def handler(req):
        nonlocal active, peak
        active += 1
        peak = max(peak, active)
        await asyncio.sleep(0.02)
        active -= 1
        return httpx.Response(200, text=XML)

    cfg = Config(target_domain="example.org", depth=20, costs_per_1000={"yandex": 25.0})
    store = Storage(tmp_path)
    async with api(handler) as client:
        result = await run_batch(
            cfg, build_tasks(cfg, [str(i) for i in range(20)]), store, client=client
        )
    assert peak == 9
    rows = store.read_run(result["run_id"])
    assert result["http_attempts"] == 40
    assert sum(row["http_attempts"] for row in rows) == 40
    assert all(row["http_attempts"] == 2 for row in rows)
    assert result["estimated_cost_rub"] == pytest.approx(1.0)


async def test_parallel_budget_never_overshoots(tmp_path):
    calls = 0

    async def handler(req):
        nonlocal calls
        calls += 1
        await asyncio.sleep(0.01)
        return httpx.Response(200, text=XML)

    cfg = Config(target_domain="example.org", max_attempts=7, depth=20)
    async with api(handler) as client:
        result = await run_batch(
            cfg, build_tasks(cfg, [str(i) for i in range(30)]), Storage(tmp_path), client=client
        )
    assert calls == result["http_attempts"] == 7
    assert result["status"] == "aborted"


async def test_global_cooldown_reduces_then_recovers():
    stamps = []

    def handler(req):
        stamps.append(time.monotonic())
        if len(stamps) == 1:
            return httpx.Response(429, headers={"Retry-After": "0.08"})
        return httpx.Response(200, text="1")

    async with api(handler) as client:
        await asyncio.gather(*(client.get_balance() for _ in range(6)))
        assert client.gate.limit == 4
        assert stamps[1] - stamps[0] >= 0.07
        await asyncio.gather(*(client.get_balance() for _ in range(14)))
        assert client.gate.limit == 5
        assert client.gate.active == 0


@pytest.mark.parametrize("code", ["110", "115"])
async def test_provider_cooldown_without_retry_after(code):
    async with api(
        lambda req: httpx.Response(200, json={"code": int(code), "error": "busy secret"})
    ) as client:
        with pytest.raises(XmlRiverError):
            await client.fetch_wordstat("q", max_retries=0)
        assert client.gate.limit == 4
        remaining = client.gate.cooldown_until - time.monotonic()
        assert remaining >= (599 if code == "115" else 4)


async def test_provider_wait_and_cancel_during_cooldown():
    async with api(
        lambda req: httpx.Response(
            200, json={"code": 115, "error": "Доступ откроется через 12 секунд."}
        )
    ) as client:
        with pytest.raises(XmlRiverError):
            await client.fetch_wordstat("q", max_retries=0)
        assert 11 <= client.gate.cooldown_until - time.monotonic() <= 12
        task = asyncio.create_task(client.fetch_wordstat("another"))
        await asyncio.sleep(0.01)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert client.http_attempts == 1
        assert client.gate.active == 0


async def test_rate_separate_from_concurrency():
    gate = AdaptiveGate(9, 2, 0.06)
    stamps = []

    async def one():
        await gate.acquire()
        stamps.append(time.monotonic())
        await gate.release()

    await asyncio.gather(*(one() for _ in range(5)))
    assert stamps[2] - stamps[0] >= 0.05
    assert stamps[4] - stamps[2] >= 0.05


async def test_query_batch_order_cache_and_resume(tmp_path):
    calls = []
    fail = True

    async def handler(req):
        q = req.url.params["query"]
        calls.append(q)
        await asyncio.sleep(0.01 if q == "first" else 0)
        if q == "second" and fail:
            return httpx.Response(400)
        return httpx.Response(200, json={"popular": [{"text": q + " more", "value": 10}]})

    args = BatchArgs(operation="wordstat_top", queries=["first", "second", "third"])
    store = Storage(tmp_path)
    async with api(handler) as client:
        result = await run_queries(client, args, store=store)
        assert result["status"] == "partial"
        assert [r["query"] for r in result["results"]] == args.queries
        assert result["http_attempts"] == 3
        fetched = result["results"][0]["fetched_at"]
        fail = False
        result = await run_queries(client, args, store=store)
        assert result["status"] == "ok"
        assert result["http_attempts"] == 1
        assert result["cached_hits"] == 2
        assert result["results"][0]["fetched_at"] == fetched
    assert calls.count("second") == 2
    assert calls.count("first") == 1


async def test_query_batch_timeout_preserves_partial(tmp_path):
    async def handler(req):
        if req.url.params["page"] == "1":
            await asyncio.sleep(10)
        return httpx.Response(200, text=XML)

    args = BatchArgs(operation="serp", queries=["one", "two"], parameters={"pages": 3})
    async with api(handler) as client:
        result = await run_queries(client, args, store=Storage(tmp_path), deadline=0.04)
        assert client.gate.active == 0
        assert result["status"] == "aborted"
        assert result["successful_pages"] == 2
        assert result["http_attempts"] == 4
        assert all(len(r["result"]["results"]) == 1 for r in result["results"])


async def test_fatal_stops_job_but_next_job_can_recover():
    fail = True
    calls = 0

    def handler(req):
        nonlocal calls
        calls += 1
        return (
            httpx.Response(200, json={"code": 200, "error": "empty"})
            if fail
            else httpx.Response(200, json={"popular": []})
        )

    args = BatchArgs(operation="wordstat_top", queries=[str(i) for i in range(30)])
    async with api(handler) as client:
        result = await run_queries(client, args)
        assert result["status"] == "aborted"
        assert calls == 1
        fail = False
        result = await run_queries(client, args)
        assert result["status"] == "ok"


async def test_concurrent_batches_have_independent_budgets():
    async def handler(req):
        await asyncio.sleep(0.01)
        return httpx.Response(200, json={"popular": []})

    args = BatchArgs(operation="wordstat_top", queries=[str(i) for i in range(10)], max_attempts=3)
    async with api(handler) as client:
        results = await asyncio.gather(run_queries(client, args), run_queries(client, args))
        assert [r["http_attempts"] for r in results] == [3, 3]
        assert client.http_attempts == 6


async def test_dry_batch_does_not_write(tmp_path):
    args = BatchArgs(operation="wordstat_top", queries=["q"])
    result = await run_queries(None, args, store=Storage(tmp_path / "missing"), dry_run=True)
    assert not (tmp_path / "missing").exists()
    assert result["http_attempts"] == 0


@pytest.mark.parametrize(
    "extra",
    [
        dict(concurrency=10),
        dict(queries=[""]),
        dict(parameters={"pages": 11}),
        dict(parameters={"query": "q"}),
    ],
)
def test_batch_rejects_invalid_arguments_before_http(extra):
    with pytest.raises(ValidationError):
        BatchArgs.model_validate({"operation": "serp", "queries": ["q"], **extra})
