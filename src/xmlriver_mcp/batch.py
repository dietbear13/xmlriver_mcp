import asyncio
import time

from .client import XmlRiverClient, resolve_xmlriver_creds
from .concurrency import attempt_scope, workers
from .config import Config
from .errors import XmlRiverError
from .models import DEFAULT_REGION, SerpArgs
from .parsers import find_matches, normalize_domain
from .services import collect_serp, utcnow
from .storage import DiskRecords, Storage, cache_key, make_run_id, normalize_phrase


def build_tasks(cfg: Config, phrases: list[str], regional: bool = False) -> list[dict]:
    tasks = []
    seen = set()
    for phrase in phrases:
        norm = normalize_phrase(phrase)
        if not norm or norm in seen:
            continue
        seen.add(norm)
        for engine in cfg.engines:
            regions: list[tuple[int, list[str]]] = [(engine.region, [])]
            if regional:
                regions = [
                    (getattr(region, engine.name), region.expected_hosts)
                    for region in cfg.regions
                    if getattr(region, engine.name) is not None
                ]
            for region, hosts in regions:
                args = SerpArgs.model_validate(
                    dict(
                        query=phrase,
                        engine=engine.name,
                        region=region,
                        device=engine.device,
                        pages=cfg.depth // 10,
                        target_domain=cfg.target_domain,
                        include_subdomains=cfg.include_subdomains,
                    )
                )
                tasks.append({**args.model_dump(), "expected_hosts": hosts})
    if not tasks:
        raise ValueError("No collection tasks")
    return tasks


async def run_batch(
    cfg: Config,
    tasks: list[dict],
    store: Storage,
    *,
    dry_run: bool = False,
    refresh: bool = False,
    client=None,
) -> dict:
    started = time.monotonic()
    run_id = make_run_id()
    records = DiskRecords(store.run_path(run_id) / "journal.jsonl")
    pending = []
    for task in tasks:
        task["region"] = (
            task.get("region") if task.get("region") is not None else DEFAULT_REGION[task["engine"]]
        )
        cached = None if refresh else store.cached(task, cfg.cache_ttl_hours)
        if cached:
            cached["expected_hosts"] = task.get("expected_hosts", [])
            cached["expected_host_matches"] = [
                doc
                for doc in cached["results"]
                if normalize_domain(doc["url"])
                in {normalize_domain(h) for h in cached["expected_hosts"]}
            ]
            if dry_run:
                records.append_pending({"source": "cache"})
            else:
                records.append(cached)
        else:
            pending.append(task)
    estimate = (
        None
        if any(task["engine"] not in cfg.costs_per_1000 for task in pending)
        else sum(task["pages"] * cfg.costs_per_1000[task["engine"]] / 1000 for task in pending)
    )
    task_order = {cache_key(task): index for index, task in enumerate(tasks)}
    if dry_run:
        return dict(
            status="dry_run",
            tasks=len(tasks),
            cached_hits=len(records),
            successful_pages=0,
            http_attempts=0,
            estimated_cost_rub=estimate,
            pages_without_retries=sum(task["pages"] for task in pending),
            max_attempts=cfg.max_attempts,
            network_requests=False,
        )
    manifest = dict(
        run_id=run_id,
        created_at=utcnow(),
        status="running",
        http_attempts=0,
        successful_pages=0,
        estimated_cost_rub=None,
        elapsed_seconds=0,
        tasks=len(tasks),
        cached_hits=len(records),
    )
    store.save_run(manifest, records)
    own_client = client is None
    client = client or XmlRiverClient(
        *resolve_xmlriver_creds(),
        rate_max=cfg.rate_max,
        rate_window=cfg.rate_window,
        max_attempts=cfg.max_attempts,
        max_concurrent=cfg.concurrency,
    )
    attempt_start = client.http_attempts
    if own_client:
        await client.__aenter__()
    status = "ok"
    last_checkpoint = time.monotonic()

    async def process(index, task):
        nonlocal status, last_checkpoint
        async with attempt_scope() as stats:
            progress = {
                **task,
                "phrase": task["query"],
                "phrase_norm": normalize_phrase(task["query"]),
            }
            row_index = len(records)
            records.append_pending(progress)

            def save_page(page):
                progress["http_attempts"] = stats["http_attempts"]
                records[row_index] = progress
                store.save_partial(progress)

            stats["on_attempt"] = lambda: save_page(progress)

            resume = None if refresh else store.partial(task, cfg.cache_ttl_hours)
            try:
                async with asyncio.timeout(cfg.deadline):
                    await collect_serp(
                        client,
                        SerpArgs.model_validate(
                            {k: v for k, v in task.items() if k != "expected_hosts"}
                        ),
                        progress=progress,
                        checkpoint=save_page,
                        resume=resume,
                    )
            except (XmlRiverError, TimeoutError) as exc:
                error = exc if isinstance(exc, XmlRiverError) else XmlRiverError(kind="deadline")
                progress.update(status="error", complete=False, error=error.payload())
                if status != "aborted":
                    status = "partial"
                if error.kind in {"auth", "balance", "ip", "budget"}:
                    status = "aborted"
            except asyncio.CancelledError:
                status = "aborted"
                progress.update(status="incomplete", complete=False)
                raise
            finally:
                progress["http_attempts"] = stats["http_attempts"]
                matches = find_matches(
                    progress.get("results", []), task["target_domain"], task["include_subdomains"]
                )
                progress.update(
                    matches=matches,
                    position=matches[0]["position"] if matches else None,
                    url=matches[0]["url"] if matches else None,
                )
                if progress.get("complete"):
                    progress["status"] = "found" if matches else "not_found"
                progress["expected_host_matches"] = [
                    doc
                    for doc in progress.get("results", [])
                    if normalize_domain(doc["url"])
                    in {normalize_domain(h) for h in task.get("expected_hosts", [])}
                ]
                price = cfg.costs_per_1000.get(task["engine"])
                progress["estimated_cost_rub"] = (
                    None if price is None else progress["http_attempts"] * price / 1000
                )
                records[row_index] = progress
                store.save_cache(progress)
                manifest.update(
                    http_attempts=client.http_attempts - attempt_start,
                    elapsed_seconds=time.monotonic() - started,
                )
                if time.monotonic() - last_checkpoint >= 1:
                    store.save_manifest(manifest)
                    last_checkpoint = time.monotonic()

    try:
        async with attempt_scope(cfg.max_attempts):
            await workers(pending, cfg.concurrency, process, lambda: status == "aborted")
    except BaseException:
        status = "aborted"
        raise
    finally:
        if own_client:
            await client.__aexit__()
        successful_pages = 0
        completed_tasks = 0
        costs = []
        for row in records.summaries:
            completed_tasks += bool(row.get("complete"))
            if row.get("source") == "api":
                successful_pages += (row.get("successful_pages") or 0) - (
                    row.get("resumed_pages") or 0
                )
                costs.append(row.get("estimated_cost_rub"))
        manifest.update(
            status=status,
            http_attempts=client.http_attempts - attempt_start,
            successful_pages=successful_pages,
            estimated_cost_rub=None
            if any(v is None for v in costs)
            else sum(v for v in costs if v is not None),
            elapsed_seconds=time.monotonic() - started,
            completed_tasks=completed_tasks,
        )
        records.sort(key=lambda row: task_order[cache_key(row)])
        store.save_run(manifest, records)
        store.append_history(records, manifest)
    return manifest
