"""Parallel lists from MCP or CLI, with optional persistent checkpoints and cache."""

import asyncio
import hashlib
import json
import time
from datetime import datetime, timezone

from .concurrency import attempt_scope, workers
from .errors import XmlRiverError
from .models import DEFAULT_REGION, INPUTS, BatchArgs, SerpArgs
from .services import collect_serp, execute, utcnow
from .storage import DiskRecords, make_run_id, write_json


async def run_queries(
    client,
    args: BatchArgs,
    *,
    deadline=180,
    store=None,
    refresh=False,
    dry_run=False,
    cache_ttl_hours=24,
    return_results=True,
):
    started = time.monotonic()
    run_id = make_run_id()
    records: DiskRecords | list[dict] = (
        DiskRecords(store.run_path(run_id) / "journal.jsonl") if store and not dry_run else []
    )
    cache_paths = []
    for query in args.queries:
        parameters = (
            INPUTS[args.operation].model_validate({**args.parameters, "query": query}).model_dump()
        )
        key = hashlib.sha256(
            json.dumps([1, args.operation, parameters], sort_keys=True, ensure_ascii=False).encode()
        ).hexdigest()
        path = store.root / "query-cache-v1" / f"{key}.json" if store else None
        row = dict(
            operation=args.operation,
            arguments=parameters,
            query=query,
            status="pending",
            complete=False,
            http_attempts=0,
            source="api",
        )
        if path and not refresh:
            try:
                cached = json.loads(path.read_text(encoding="utf-8"))
                age = (
                    datetime.now(timezone.utc) - datetime.fromisoformat(cached["fetched_at"])
                ).total_seconds()
                if (
                    cached["complete"] is True
                    and cached["arguments"] == parameters
                    and cached["operation"] == args.operation
                    and isinstance(cached["result"], dict)
                    and 0 <= age <= cache_ttl_hours * 3600
                ):
                    row = {**cached, "source": "cache", "http_attempts": 0}
            except (OSError, ValueError, KeyError, TypeError):
                pass
        if isinstance(records, DiskRecords) and not row["complete"]:
            records.append_pending(row)
        else:
            records.append(row)
        cache_paths.append(path)
    manifest = dict(
        run_id=run_id,
        created_at=utcnow(),
        status="running",
        http_attempts=0,
        successful_pages=0,
        estimated_cost_rub=None,
        elapsed_seconds=0,
        tasks=len(records),
        completed_tasks=0,
        cached_hits=sum(r["source"] == "cache" for r in records),
    )
    if dry_run:
        result = {**manifest, "status": "dry_run", "network_requests": False}
        if return_results:
            result["results"] = records
        return result

    last_checkpoint = 0.0

    def checkpoint(force=False):
        nonlocal last_checkpoint
        if not force and (store is None or time.monotonic() - last_checkpoint < 1):
            return
        manifest.update(
            elapsed_seconds=time.monotonic() - started,
            http_attempts=sum(
                r["http_attempts"] or 0
                for r in (records.summaries if isinstance(records, DiskRecords) else records)
            ),
            completed_tasks=sum(
                bool(r["complete"])
                for r in (records.summaries if isinstance(records, DiskRecords) else records)
            ),
            successful_pages=sum(
                r.get("successful_pages") or 0
                for r in (records.summaries if isinstance(records, DiskRecords) else records)
                if r["source"] == "api"
            ),
        )
        if store:
            if force:
                store.save_run(manifest, records)
            else:
                store.save_manifest(manifest)
            last_checkpoint = time.monotonic()

    aborted = False

    async def process(index, row):
        nonlocal aborted
        if row["complete"]:
            return
        async with attempt_scope() as stats:
            row.update(status="running", fetched_at=utcnow())
            resume_request = None
            if args.operation == "serp":
                resume_request = {**row["arguments"]}
                if resume_request["region"] is None:
                    resume_request["region"] = DEFAULT_REGION[resume_request["engine"]]

            def save_page(page):
                row["http_attempts"] = stats["http_attempts"]
                row["fetched_at"] = page["fetched_at"]
                row["successful_pages"] = page["successful_pages"] - page.get("resumed_pages", 0)
                records[index] = row
                if store:
                    assert resume_request is not None
                    store.save_partial({**resume_request, **page})

            def save_attempt():
                row["http_attempts"] = stats["http_attempts"]
                records[index] = row

            stats["on_attempt"] = save_attempt

            try:
                if args.operation == "serp":
                    row["result"] = {}
                    resume = (
                        store.partial(resume_request, cache_ttl_hours)
                        if store and not refresh
                        else None
                    )
                    await collect_serp(
                        client,
                        SerpArgs.model_validate(row["arguments"]),
                        progress=row["result"],
                        checkpoint=save_page,
                        resume=resume,
                    )
                else:
                    row["result"] = await execute(
                        client, args.operation, row["arguments"], deadline=deadline
                    )
                row.update(
                    status="ok",
                    complete=True,
                    fetched_at=row["result"].get("fetched_at", utcnow())
                    if args.operation == "serp"
                    else utcnow(),
                )
            except XmlRiverError as exc:
                row.update(status="error", error=exc.payload())
                if exc.kind in {"auth", "ip", "balance", "budget"}:
                    aborted = True
            except asyncio.CancelledError:
                row.update(status="incomplete")
                raise
            finally:
                row["http_attempts"] = stats["http_attempts"]
                row["successful_pages"] = (
                    row.get("result", {}).get("successful_pages", 0)
                    - row.get("result", {}).get("resumed_pages", 0)
                    if args.operation == "serp"
                    else int(row["complete"])
                )
                path = cache_paths[index]
                records[index] = row
                if path and row["complete"]:
                    write_json(path, row)
                checkpoint()

    checkpoint(force=True)
    try:
        async with attempt_scope(args.max_attempts):
            async with asyncio.timeout(deadline):
                await workers(records, args.concurrency, process, lambda: aborted)
    except TimeoutError:
        aborted = True
    except BaseException:
        aborted = True
        raise
    finally:
        for index, row in enumerate(
            records.entries if isinstance(records, DiskRecords) else records
        ):
            if not isinstance(row, dict):
                continue
            if row["status"] == "pending":
                row["status"] = "not_started"
                if isinstance(records, DiskRecords):
                    records.entries[index] = row  # No response to persist for an unstarted item.
                else:
                    records[index] = row
        manifest["status"] = (
            "aborted"
            if aborted
            else (
                "ok"
                if all(
                    r["complete"]
                    for r in (records.summaries if isinstance(records, DiskRecords) else records)
                )
                else "partial"
            )
        )
        checkpoint(force=True)
    result = {**manifest}
    if store:
        result["results_path"] = str(store.run_path(run_id) / "data.jsonl")
        result["journal_path"] = str(store.run_path(run_id) / "journal.jsonl")
    if return_results:
        result["results"] = list(records)
    return result
