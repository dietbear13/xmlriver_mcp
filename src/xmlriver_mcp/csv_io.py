import csv
import io
import uuid
from pathlib import Path

from filelock import FileLock

from .storage import Storage, atomic_write, normalize_phrase


def read_csv(path: Path):
    raw = path.read_bytes()
    encoding = "utf-8-sig" if raw.startswith(b"\xef\xbb\xbf") else "utf-8"
    try:
        text = raw.decode(encoding)
    except UnicodeDecodeError:
        encoding = "cp1251"
        text = raw.decode(encoding)
    try:
        delimiter = csv.Sniffer().sniff(text[:8192], delimiters=";,\t").delimiter
    except csv.Error:
        delimiter = ";" if ";" in text.splitlines()[0] else ","
    reader = csv.DictReader(io.StringIO(text), delimiter=delimiter)
    rows = list(reader)
    return (
        rows,
        list(reader.fieldnames or []),
        encoding,
        delimiter,
        "\r\n" if "\r\n" in text else "\n",
    )


def read_phrases(path: Path, column: str) -> list[str]:
    rows, headers, *_ = read_csv(path)
    if column not in headers:
        raise ValueError("Phrase column missing")
    return [row[column] for row in rows if row.get(column)]


def update_csv(store: Storage, path: Path, run_id: str, column: str, *, dry_run: bool = False):
    records = store.read_run(run_id)
    mapping = {}
    contexts: dict[tuple, tuple] = {}
    for record in records:
        key = normalize_phrase(record["query"]), record["engine"]
        context = (
            record["region"],
            record["device"],
            record["target_domain"],
            record["include_subdomains"],
        )
        if key in contexts and contexts[key] != context:
            raise ValueError(
                "CSV update requires one region/device/domain context per phrase and engine"
            )
        contexts[key] = context
        mapping[key] = record
    if dry_run:
        read_phrases(path, column)
        return {"status": "dry_run", "records": len(records)}
    with FileLock(str(path) + ".lock"):
        rows, headers, encoding, delimiter, newline = read_csv(path)
        if column not in headers:
            raise ValueError("Phrase column missing")
        new_columns = [
            f"{prefix} {engine}"
            for engine in ("Яндекс", "Google")
            for prefix in ("Позиция", "URL", "Дата съёма")
        ] + ["Дата съёма"]
        out_headers = headers + [h for h in new_columns if h not in headers]
        updated = 0
        for row in rows:
            changed = False
            for engine, label in (("yandex", "Яндекс"), ("google", "Google")):
                record = mapping.get((normalize_phrase(row.get(column) or ""), engine))
                if record is None:
                    continue
                position = record.get("position")
                value = (
                    str(position) if position is not None else f">{record.get('checked_depth', 0)}"
                )
                if not record.get("complete"):
                    value = "ошибка" if record.get("error") else "не завершён"
                row[f"Позиция {label}"] = value
                row[f"URL {label}"] = record.get("url") or ""
                row[f"Дата съёма {label}"] = (
                    record.get("fetched_at", "")[:10] if record.get("complete") else ""
                )
                changed = True
            if changed:
                dates = [row.get(f"Дата съёма {engine}", "") for engine in ("Яндекс", "Google")]
                row["Дата съёма"] = dates[0] if dates[0] and dates[0] == dates[1] else ""
                updated += 1
        stream = io.StringIO(newline="")
        writer = csv.DictWriter(stream, out_headers, delimiter=delimiter, lineterminator=newline)
        writer.writeheader()
        writer.writerows(rows)
        encoded = stream.getvalue().encode(encoding)
        backup = store.root / "backups" / (path.name + "." + uuid.uuid4().hex + ".bak")
        atomic_write(backup, path.read_bytes())
        atomic_write(path, encoded)
    return dict(status="ok", rows_updated=updated, backup_path=str(backup))
