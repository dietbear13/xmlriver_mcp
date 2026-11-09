import argparse
import asyncio
import json
import logging
import math
import sys
from pathlib import Path

import yaml
from pydantic import ValidationError

from .batch import build_tasks, run_batch
from .client import XmlRiverClient, resolve_xmlriver_creds
from .config import load_config
from .csv_io import read_phrases, update_csv
from .errors import XmlRiverError
from .models import INPUTS, BatchFileArgs
from .query_batch import run_queries
from .regions import validate_regions
from .services import execute
from .storage import Storage


def parser():
    ap = argparse.ArgumentParser(
        description="Independent XMLRiver toolkit. API requests may cost money."
    )
    commands = ap.add_subparsers(dest="command", required=True)
    commands.add_parser("balance")
    recover = commands.add_parser("recover")
    recover.add_argument("--workspace", required=True, type=Path)
    recover.add_argument("--run", required=True)
    batch = commands.add_parser("batch")
    batch.add_argument("--input", required=True, type=Path)
    batch.add_argument("--workspace", type=Path)
    batch.add_argument("--dry-run", action="store_true")
    batch.add_argument("--refresh", action="store_true")
    batch.add_argument("--deadline", type=float, default=3600)
    serp = commands.add_parser("serp")
    serp.add_argument("query")
    serp.add_argument("--engine", choices=["yandex", "google"], default="yandex")
    serp.add_argument("--region", type=int)
    serp.add_argument("--device", choices=["desktop", "mobile", "tablet"], default="mobile")
    serp.add_argument("--pages", type=int, default=1)
    serp.add_argument("--target-domain")
    serp.add_argument("--include-subdomains", action=argparse.BooleanOptionalAction, default=True)
    ws = commands.add_parser("wordstat").add_subparsers(dest="action", required=True)
    for name in ("top", "history"):
        command = ws.add_parser(name)
        command.add_argument("query")
        command.add_argument("--regions", type=int, default=213)
        command.add_argument("--device", choices=["desktop", "phone", "tablet"])
        if name == "top":
            command.add_argument("--limit", type=int, default=50)
            command.add_argument("--min-frequency", type=int, default=0)
            command.add_argument("--include-associations", action="store_true")
            command.add_argument("--drop-self", action=argparse.BooleanOptionalAction, default=True)
        else:
            command.add_argument("--period", choices=["day", "week", "month"], default="month")
            command.add_argument("--start")
            command.add_argument("--end")
    for group, actions in (("positions", ("run", "update-csv")), ("regions", ("run", "validate"))):
        sub = commands.add_parser(group).add_subparsers(dest="action", required=True)
        for action in actions:
            command = sub.add_parser(action)
            command.add_argument("--config", required=True, type=Path)
            command.add_argument("--workspace", type=Path)
            command.add_argument("--dry-run", action="store_true")
            command.add_argument("--refresh", action="store_true")
            command.add_argument("--csv", type=Path)
            command.add_argument("--concurrency", type=int, choices=range(1, 10))
            if action == "update-csv":
                command.add_argument("--run", required=True)
    return ap


async def run(args):
    if args.command == "recover":
        return Storage(args.workspace).recover_run(args.run)
    if args.command == "batch":
        model = BatchFileArgs.model_validate_json(args.input.read_text(encoding="utf-8"))
        if not math.isfinite(args.deadline) or args.deadline <= 0:
            raise ValueError("Invalid deadline")
        store = Storage(args.workspace or args.input.resolve().parent / "xmlriver-data")
        if args.dry_run:
            return await run_queries(
                None,
                model,
                store=store,
                dry_run=True,
                refresh=args.refresh or model.refresh,
                return_results=False,
            )
        async with XmlRiverClient(
            *resolve_xmlriver_creds(),
            max_concurrent=model.concurrency,
            max_attempts=model.max_attempts,
        ) as client:
            return await run_queries(
                client,
                model,
                store=store,
                refresh=args.refresh or model.refresh,
                deadline=args.deadline,
                return_results=False,
            )
    if args.command in ("positions", "regions"):
        cfg = load_config(args.config)
        if args.concurrency is not None:
            cfg.concurrency = args.concurrency
        store = Storage(args.workspace or args.config.resolve().parent / "xmlriver-data")
        path = args.csv or (Path(cfg.csv) if cfg.csv else None)
        if args.action == "update-csv":
            if path is None:
                raise ValueError("CSV path required")
            return update_csv(store, path, args.run, cfg.column, dry_run=args.dry_run)
        if args.action == "validate":
            async with XmlRiverClient(
                *resolve_xmlriver_creds(),
                max_attempts=cfg.max_attempts,
                max_concurrent=cfg.concurrency,
                rate_max=cfg.rate_max,
                rate_window=cfg.rate_window,
            ) as client:
                return await validate_regions(
                    cfg, store, client, refresh=args.refresh, dry_run=args.dry_run
                )
        phrases = read_phrases(path, cfg.column) if path else cfg.phrases
        tasks = build_tasks(cfg, phrases, regional=args.command == "regions")
        return await run_batch(cfg, tasks, store, dry_run=args.dry_run, refresh=args.refresh)
    name = {
        "balance": "get_balance",
        "serp": "serp",
        "wordstat": "wordstat_" + getattr(args, "action", ""),
    }[args.command]
    data = {k: v for k, v in vars(args).items() if k not in {"command", "action"}}
    INPUTS[name].model_validate(data)
    async with XmlRiverClient(*resolve_xmlriver_creds()) as client:
        return await execute(client, name, data)


def main():
    logging.getLogger("httpx").disabled = True
    logging.getLogger("httpcore").disabled = True
    args = parser().parse_args()
    try:
        result = asyncio.run(run(args))
        code = 4 if result.get("status") in {"partial", "aborted"} else 0
    except (ValueError, ValidationError, yaml.YAMLError):
        result, code = XmlRiverError(kind="params").payload(), 2
    except XmlRiverError as exc:
        result, code = exc.payload(), 3
    except KeyboardInterrupt:
        result, code = {"status": "aborted"}, 4
    except Exception:
        result, code = XmlRiverError().payload(), 3
    if code:
        print("XMLRiver operation did not complete successfully.", file=sys.stderr)
    print(json.dumps(result, ensure_ascii=False))
    raise SystemExit(code)


if __name__ == "__main__":
    main()
