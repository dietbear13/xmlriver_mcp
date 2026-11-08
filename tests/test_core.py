import asyncio
import json
from datetime import datetime, timedelta, timezone

import httpx
import pytest
from pydantic import ValidationError

from xmlriver_mcp.batch import build_tasks, run_batch
from xmlriver_mcp.client import RateLimiter, XmlRiverClient, retry_after
from xmlriver_mcp.config import Config
from xmlriver_mcp.csv_io import read_csv, update_csv
from xmlriver_mcp.errors import XmlRiverError
from xmlriver_mcp.models import HistoryArgs, SerpArgs, TopArgs
from xmlriver_mcp.parsers import match_domain, parse_serp, parse_wordstat
from xmlriver_mcp.services import collect_serp, execute
from xmlriver_mcp.storage import Storage, make_run_id, write_json


def xml(urls=()):
    return (
        "<yandexsearch><response><results><grouping>"
        + "".join(f"<group><doc><url>{url}</url><title>Title</title></doc></group>" for url in urls)
        + "</grouping></results></response></yandexsearch>"
    )


def client(handler, **kwargs):
    return XmlRiverClient(
        "123456", "secret-test-key", transport=httpx.MockTransport(handler), rate_max=1000, **kwargs
    )


@pytest.mark.parametrize(
    "args",
    [
        {"pages": 0},
        {"pages": 11},
        {"pages": True},
        {"pages": "2"},
        {"query": "  "},
        {"unknown": 1},
        {"region": -1},
    ],
)
def test_serp_validation(args):
    with pytest.raises(ValidationError):
        SerpArgs.model_validate({"query": "coffee", **args})


@pytest.mark.parametrize("args", [{"limit": 0}, {"limit": 1001}, {"min_frequency": -1}])
def test_wordstat_validation(args):
    with pytest.raises(ValidationError):
        TopArgs(query="coffee", **args)


@pytest.mark.parametrize(
    "start,end", [("31.02.2026", None), ("1.01.2026", None), ("03.01.2026", "01.01.2026")]
)
def test_dates(start, end):
    with pytest.raises(ValidationError):
        HistoryArgs(query="q", start=start, end=end)


@pytest.mark.parametrize("engine,first", [("google", "1"), ("yandex", "0")])
async def test_pagination_short_then_more_then_empty(engine, first):
    pages = []

    def handler(request):
        pages.append(request.url.params["page"])
        return httpx.Response(200, text=xml(["https://example.org/a"]) if len(pages) < 3 else xml())

    async with client(handler) as api:
        result = await collect_serp(
            api, SerpArgs(query="q", engine=engine, pages=5, target_domain="example.org")
        )
    assert pages == [str(int(first) + n) for n in range(3)]
    assert [r["position"] for r in result["results"]] == [1, 2]
    assert result["results"][1]["page"] == 2
    assert result["checked_depth"] == 2
    assert result["successful_pages"] == 3


@pytest.mark.parametrize(
    "body", ["<html></html>", "<broken", "<yandexsearch><response/></yandexsearch>"]
)
def test_not_empty(body):
    with pytest.raises(XmlRiverError):
        parse_serp(body)


def test_empty_code():
    assert (
        parse_serp(
            '<yandexsearch><response><error code="15">empty</error></response></yandexsearch>'
        )
        == []
    )


@pytest.mark.parametrize(
    "status,kind", [(401, "auth"), (403, "auth"), (400, "params"), (429, "temp"), (503, "temp")]
)
async def test_http_errors_safe(status, kind):
    async with client(lambda req: httpx.Response(status, text=str(req.url))) as api:
        with pytest.raises(XmlRiverError) as error:
            await api.get_balance(max_retries=0)
        assert api.http_attempts == 1
        assert error.value.kind == kind
        assert "secret-test-key" not in str(error.value)


async def test_retry_and_budget():
    calls = []

    def handler(req):
        calls.append(req)
        return (
            httpx.Response(503, headers={"Retry-After": "0"})
            if len(calls) == 1
            else httpx.Response(200, text="10")
        )

    async with client(handler, max_attempts=2) as api:
        assert await api.get_balance() == 10
        with pytest.raises(XmlRiverError, match="attempt limit"):
            await api.get_balance()
        assert api.http_attempts == 2


def test_retry_after():
    assert retry_after("2") == 2
    assert retry_after("invalid") is None
    assert retry_after("NaN") is None
    assert retry_after("Wed, 21 Oct 2015 07:28:00 GMT") == 0


async def test_deadline_cancels_future_requests():
    calls = []

    async def handler(req):
        calls.append(req)
        await asyncio.sleep(1)
        return httpx.Response(200, text=xml())

    async with client(handler) as api:
        with pytest.raises(XmlRiverError) as error:
            await execute(api, "serp", {"query": "q", "pages": 10}, deadline=0.02)
        assert error.value.kind == "deadline"
        assert len(calls) == 1


async def test_limiter_shared():
    loop = asyncio.get_running_loop()
    limiter = RateLimiter(1, 0.03)
    stamps = []

    async def one():
        await limiter.acquire()
        stamps.append(loop.time())

    await asyncio.gather(one(), one(), one())
    assert stamps[-1] - stamps[0] >= 0.05


@pytest.mark.parametrize("body", ["[]", "{", "<html>", '{"popular":{}}'])
async def test_bad_wordstat(body):
    async with client(lambda req: httpx.Response(200, text=body)) as api:
        with pytest.raises(XmlRiverError):
            await api.fetch_wordstat("кофе")


def test_wordstat():
    data = {
        "popular": [{"text": "кофе", "value": "10"}, {"text": "кофе дома", "value": "1 000"}],
        "associations": [{"text": "чай", "value": "2000"}],
    }
    rows = parse_wordstat(data, include_associations=True, drop_query="кофе", min_frequency=50)
    assert rows[0]["is_association"] is True
    assert rows[1]["frequency"] == 1000


@pytest.mark.parametrize(
    "url,domain,sub,result",
    [
        ("https://www.example.org/a", "example.org", False, True),
        ("https://a.example.org", "example.org", True, True),
        ("https://a.example.org", "example.org", False, False),
        ("https://notexample.org", "example.org", True, False),
        ("https://example.org.evil.test", "example.org", True, False),
        ("https://xn--e1afmkfd.xn--p1ai", "пример.рф", True, True),
    ],
)
def test_domains(url, domain, sub, result):
    assert match_domain(url, domain, sub) is result


def record():
    return dict(
        query="coffee",
        engine="yandex",
        region=213,
        device="mobile",
        pages=1,
        target_domain="example.org",
        include_subdomains=True,
        complete=True,
        results=[],
        fetched_at=datetime.now(timezone.utc).isoformat(),
        source="api",
        position=None,
        checked_depth=0,
    )


def test_cache_context_and_ttl(tmp_path):
    store = Storage(tmp_path / "new")
    row = record()
    assert store.cached(row) is None
    assert not store.root.exists()
    store.save_cache(row)
    assert store.cached(row)["source"] == "cache"
    for key, value in [
        ("pages", 5),
        ("include_subdomains", False),
        ("device", "desktop"),
        ("region", 1),
    ]:
        assert store.cached({**row, key: value}) is None
    old = {**row, "fetched_at": (datetime.now(timezone.utc) - timedelta(days=2)).isoformat()}
    store.save_cache(old)
    assert store.cached(row) is None
    for path in (store.root / "cache-v2").glob("*.json"):
        path.write_text("{")
    assert store.cached(row) is None


async def test_batch_partial_budget(tmp_path):
    cfg = Config(target_domain="example.org", depth=20, max_attempts=1)
    store = Storage(tmp_path)
    async with client(
        lambda req: httpx.Response(200, text=xml(["https://example.org"])), max_attempts=1
    ) as api:
        result = await run_batch(cfg, build_tasks(cfg, ["coffee"]), store, client=api)
    assert result["status"] == "aborted"
    assert result["http_attempts"] == 1
    assert result["successful_pages"] == 1
    assert result["estimated_cost_rub"] is None
    rows = store.read_run(result["run_id"])
    assert rows[0]["checked_depth"] == 1
    assert rows[0]["complete"] is False


async def test_dry_run_no_files(tmp_path):
    cfg = Config(target_domain="example.org")
    store = Storage(tmp_path / "missing")
    result = await run_batch(cfg, build_tasks(cfg, ["q"]), store, dry_run=True)
    assert result["http_attempts"] == 0
    assert not store.root.exists()


@pytest.mark.parametrize("encoding", ["utf-8", "utf-8-sig", "cp1251"])
@pytest.mark.parametrize("separator", [",", ";", "\t"])
def test_csv_preserves_encoding_dates(tmp_path, encoding, separator):
    path = tmp_path / "input.csv"
    original = f"phrase{separator}other\r\ncoffee{separator}тест\r\n".encode(encoding)
    path.write_bytes(original)
    store = Storage(tmp_path / "data")
    row = {**record(), "fetched_at": "2026-01-02T12:00:00+00:00"}
    folder = store.run_path("test")
    folder.mkdir(parents=True)
    (folder / "data.jsonl").write_text(json.dumps(row) + "\n", encoding="utf-8")
    result = update_csv(store, path, "test", "phrase")
    assert __import__("pathlib").Path(result["backup_path"]).read_bytes() == original
    rows, headers, enc, sep, newline = read_csv(path)
    assert (enc, sep, newline) == (encoding, separator, "\r\n")
    assert rows[0]["Дата съёма Яндекс"] == "2026-01-02"
    assert rows[0]["Дата съёма"] == ""
    assert headers[:2] == ["phrase", "other"]


def test_history_before_run(tmp_path):
    store = Storage(tmp_path)
    for n in (1, 2, 3):
        manifest = {"run_id": str(n), "created_at": f"2026-01-0{n}T00:00:00+00:00"}
        write_json(store.run_path(str(n)) / "manifest.json", manifest)
        store.append_history([{**record(), "fetched_at": manifest["created_at"]}], manifest)
    rows = list(store.history(before_run_id="2").values())
    assert len(rows) == 1 and rows[0]["run_id"] == "1"
    assert len({make_run_id() for _ in range(100)}) == 100
