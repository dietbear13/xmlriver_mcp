"""Versioned, atomic storage. Cache reads never create directories."""

import hashlib
import json
import os
import tempfile
import uuid
from collections.abc import Iterable
from datetime import datetime, timezone
from pathlib import Path

from filelock import FileLock

from .parsers import normalize_domain

VERSION = 2


def normalize_phrase(value: str) -> str:
    return " ".join(value.casefold().split())


def identity(record: dict) -> tuple:
    return (
        normalize_phrase(record["query"]),
        record["engine"],
        record["region"],
        record["device"],
        normalize_domain(record.get("target_domain") or ""),
        record.get("include_subdomains", True),
        record["pages"],
    )


def cache_key(record: dict) -> str:
    return hashlib.sha256(
        json.dumps([VERSION, *identity(record)], ensure_ascii=False).encode()
    ).hexdigest()


def atomic_write(path: Path, data: bytes):
    atomic_write_chunks(path, [data])


def atomic_write_chunks(path: Path, chunks):
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, name = tempfile.mkstemp(prefix=".tmp-", dir=path.parent)
    try:
        with os.fdopen(descriptor, "wb") as stream:
            for chunk in chunks:
                stream.write(chunk)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(name, path)
    finally:
        if os.path.exists(name):
            os.unlink(name)


def write_json(path: Path, data):
    atomic_write(path, json.dumps(data, ensure_ascii=False, indent=2).encode())


class DiskRecords:
    """Payloads live in a fsynced append-only journal; RAM holds offsets and pending inputs."""

    def __init__(self, path: Path):
        self.path = path
        self.entries: list = []
        self.summaries: list[dict] = []

    @staticmethod
    def summary(row):
        return {
            key: row.get(key)
            for key in (
                "http_attempts",
                "complete",
                "source",
                "successful_pages",
                "resumed_pages",
                "estimated_cost_rub",
                "status",
            )
        }

    def __len__(self):
        return len(self.entries)

    def __getitem__(self, index):
        entry = self.entries[index]
        if isinstance(entry, dict):
            return entry.copy()
        with self.path.open("rb") as stream:
            stream.seek(entry)
            return json.loads(stream.readline())["record"]

    def __setitem__(self, index, row):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.path.open("ab") as stream:
            offset = stream.tell()
            stream.write(
                (json.dumps({"index": index, "record": row}, ensure_ascii=False) + "\n").encode()
            )
            stream.flush()
            os.fsync(stream.fileno())
        self.entries[index] = offset
        self.summaries[index] = self.summary(row)

    def append(self, row):
        self.entries.append(None)
        self.summaries.append({})
        self[len(self.entries) - 1] = row

    def append_pending(self, row):
        self.entries.append(row.copy())
        self.summaries.append(self.summary(row))

    def __iter__(self):
        for index in range(len(self)):
            yield self[index]

    def sort(self, *, key):
        indices = sorted(range(len(self)), key=lambda i: key(self[i]))
        self.entries = [self.entries[index] for index in indices]
        self.summaries = [self.summaries[index] for index in indices]


class Storage:
    def __init__(self, workspace: Path):
        self.root = workspace

    def cached(self, request: dict, ttl_hours: float = 24) -> dict | None:
        path = self.root / "cache-v2" / (cache_key(request) + ".json")
        try:
            record = json.loads(path.read_text(encoding="utf-8"))
            fetched = datetime.fromisoformat(record["fetched_at"])
            age = (datetime.now(timezone.utc) - fetched).total_seconds()
            if (
                record["version"] != VERSION
                or identity(record) != identity(request)
                or record.get("complete") is not True
                or not 0 <= age <= ttl_hours * 3600
                or not isinstance(record.get("results"), list)
            ):
                return None
            return {**record, "source": "cache"}
        except (OSError, ValueError, KeyError, TypeError):
            return None

    def save_cache(self, record: dict):
        if not record.get("complete"):
            return
        path = self.root / "cache-v2" / (cache_key(record) + ".json")
        path.parent.mkdir(parents=True, exist_ok=True)
        with FileLock(str(path) + ".lock"):
            write_json(path, {**record, "version": VERSION})

    def save_partial(self, record: dict):
        write_json(
            self.root / "progress-v1" / (cache_key(record) + ".json"),
            {**record, "version": VERSION},
        )

    def partial(self, request: dict, ttl_hours=24):
        try:
            record = json.loads(
                (self.root / "progress-v1" / (cache_key(request) + ".json")).read_text(
                    encoding="utf-8"
                )
            )
            age = (
                datetime.now(timezone.utc) - datetime.fromisoformat(record["fetched_at"])
            ).total_seconds()
            pages = record["successful_pages"]
            if (
                record["version"] == VERSION
                and identity(record) == identity(request)
                and record["complete"] is False
                and isinstance(record["results"], list)
                and type(pages) is int
                and 1 <= pages <= request["pages"]
                and 0 <= age <= ttl_hours * 3600
            ):
                return record
        except (OSError, ValueError, KeyError, TypeError):
            pass
        return None

    def save_run(self, manifest: dict, records: Iterable[dict]):
        folder = self.root / "runs" / manifest["run_id"]
        folder.mkdir(parents=True, exist_ok=True)
        atomic_write_chunks(
            folder / "data.jsonl",
            ((json.dumps(row, ensure_ascii=False) + "\n").encode() for row in records),
        )
        self.save_manifest(manifest)

    def save_manifest(self, manifest):
        folder = self.run_path(manifest["run_id"])
        write_json(folder / "manifest.json", manifest)
        write_json(
            folder / "summary.json",
            {
                key: manifest[key]
                for key in (
                    "run_id",
                    "status",
                    "http_attempts",
                    "successful_pages",
                    "estimated_cost_rub",
                    "elapsed_seconds",
                )
            },
        )

    def append_history(self, records: Iterable[dict], manifest: dict):
        path = self.root / "history.jsonl"
        path.parent.mkdir(parents=True, exist_ok=True)
        with FileLock(str(path) + ".lock"):

            def chunks():
                if path.exists():
                    with path.open("rb") as stream:
                        while data := stream.read(1024 * 1024):
                            yield data
                for row in records:
                    if row.get("complete") and row.get("source") == "api":
                        item = {
                            **row,
                            "run_id": manifest["run_id"],
                            "run_started_at": manifest["created_at"],
                        }
                        yield (json.dumps(item, ensure_ascii=False) + "\n").encode()

            atomic_write_chunks(path, chunks())

    def history(self, *, before_run_id: str | None = None) -> dict:
        cutoff = None
        if before_run_id:
            cutoff = datetime.fromisoformat(self.read_manifest(before_run_id)["created_at"])
        path = self.root / "history.jsonl"
        result: dict[tuple, dict] = {}
        if not path.exists():
            return result
        for line in path.read_text(encoding="utf-8").splitlines():
            try:
                row = json.loads(line)
                started = datetime.fromisoformat(row["run_started_at"])
                if cutoff and started >= cutoff:
                    continue
                key = identity(row)
                if key not in result or row["fetched_at"] > result[key]["fetched_at"]:
                    result[key] = row
            except (ValueError, KeyError, TypeError):
                continue
        return result

    def run_path(self, run_id: str) -> Path:
        if (
            not run_id
            or Path(run_id).name != run_id
            or "/" in run_id
            or "\\" in run_id
            or run_id in {".", ".."}
        ):
            raise ValueError("Invalid run ID")
        return self.root / "runs" / run_id

    def read_manifest(self, run_id: str):
        return json.loads((self.run_path(run_id) / "manifest.json").read_text(encoding="utf-8"))

    def recover_run(self, run_id: str):
        """Rebuild results after process death without loading all response payloads."""
        manifest = self.read_manifest(run_id)
        if manifest["status"] != "running":
            return manifest
        journal = self.run_path(run_id) / "journal.jsonl"
        records = DiskRecords(journal)
        latest = {}
        if journal.exists():
            with journal.open("rb") as stream:
                while True:
                    offset = stream.tell()
                    line = stream.readline()
                    if not line:
                        break
                    try:
                        item = json.loads(line)
                        latest[item["index"]] = (offset, records.summary(item["record"]))
                    except (ValueError, KeyError):
                        if not line.endswith(b"\n") and not stream.read(1):
                            break
                        raise ValueError("Corrupt checkpoint journal") from None
        for index in sorted(latest):
            offset, summary = latest[index]
            records.entries.append(offset)
            records.summaries.append(summary)
        manifest.update(
            status="aborted",
            recovered=True,
            completed_tasks=sum(bool(r["complete"]) for r in records.summaries),
            http_attempts=sum(
                (r["http_attempts"] or 0) for r in records.summaries if r["source"] == "api"
            ),
            successful_pages=sum(
                (r["successful_pages"] or 0) - (r["resumed_pages"] or 0)
                for r in records.summaries
                if r["source"] == "api"
            ),
            estimated_cost_rub=None,
        )
        self.save_run(manifest, records)
        return {**manifest, "results_path": str(self.run_path(run_id) / "data.jsonl")}

    def read_run(self, run_id: str):
        folder = self.run_path(run_id)
        with (folder / "data.jsonl").open(encoding="utf-8") as stream:
            rows = [json.loads(line) for line in stream if line.strip()]
        # An abruptly terminated process cannot finalize its manifest. Replay durable updates.
        if (folder / "journal.jsonl").exists() and self.read_manifest(run_id)[
            "status"
        ] == "running":
            indexed = dict(enumerate(rows))
            with (folder / "journal.jsonl").open(encoding="utf-8") as stream:
                for line in stream:
                    try:
                        update = json.loads(line)
                        indexed[update["index"]] = update["record"]
                    except (ValueError, KeyError):
                        if not line.endswith("\n") and not stream.read(1):
                            break
                        raise ValueError("Corrupt checkpoint journal") from None
            rows = [indexed[index] for index in sorted(indexed)]
        return rows


def make_run_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S.%fZ-") + uuid.uuid4().hex
