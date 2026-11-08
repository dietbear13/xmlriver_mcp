"""Versioned, atomic storage. Cache reads never create directories."""

import hashlib
import json
import os
import tempfile
import uuid
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
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, name = tempfile.mkstemp(prefix=".tmp-", dir=path.parent)
    try:
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(name, path)
    finally:
        if os.path.exists(name):
            os.unlink(name)


def write_json(path: Path, data):
    atomic_write(path, json.dumps(data, ensure_ascii=False, indent=2).encode())


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

    def save_run(self, manifest: dict, records: list[dict]):
        folder = self.root / "runs" / manifest["run_id"]
        folder.mkdir(parents=True, exist_ok=True)
        atomic_write(
            folder / "data.jsonl",
            "".join(json.dumps(row, ensure_ascii=False) + "\n" for row in records).encode(),
        )
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

    def append_history(self, records: list[dict], manifest: dict):
        path = self.root / "history.jsonl"
        path.parent.mkdir(parents=True, exist_ok=True)
        with FileLock(str(path) + ".lock"):
            old = path.read_bytes() if path.exists() else b""
            rows = [
                {**row, "run_id": manifest["run_id"], "run_started_at": manifest["created_at"]}
                for row in records
                if row.get("complete") and row.get("source") == "api"
            ]
            atomic_write(
                path,
                old + "".join(json.dumps(row, ensure_ascii=False) + "\n" for row in rows).encode(),
            )

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

    def read_run(self, run_id: str):
        return [
            json.loads(line)
            for line in (self.run_path(run_id) / "data.jsonl")
            .read_text(encoding="utf-8")
            .splitlines()
            if line
        ]


def make_run_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S.%fZ-") + uuid.uuid4().hex
