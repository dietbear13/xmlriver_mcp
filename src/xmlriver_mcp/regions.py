import csv
import io
import json

import httpx

from .errors import XmlRiverError
from .models import SerpArgs
from .services import collect_serp, utcnow
from .storage import atomic_write, write_json

GEO_URL = "https://xmlriver.com/files/geo.csv"


async def validate_regions(cfg, store, client, *, refresh: bool = False, dry_run: bool = False):
    if dry_run:
        return {"status": "dry_run", "regions": len(cfg.regions), "network_requests": False}
    path = store.root / "geo" / "google.csv"
    metadata = path.with_suffix(".json")
    if refresh or not path.exists() or not metadata.exists():
        async with httpx.AsyncClient(timeout=180) as http:
            try:
                response = await http.get(GEO_URL)
                response.raise_for_status()
                rows = list(csv.reader(io.StringIO(response.content.decode("utf-8-sig"))))
                if not any(row and row[0].isdigit() for row in rows):
                    raise XmlRiverError(kind="format")
            except (httpx.HTTPError, UnicodeError):
                raise XmlRiverError(kind="temp") from None
            atomic_write(path, response.content)
            write_json(metadata, dict(downloaded_at=utcnow(), source=GEO_URL))
    meta = json.loads(metadata.read_text(encoding="utf-8"))
    lookup = {
        row[0]: row for row in csv.reader(io.StringIO(path.read_text(encoding="utf-8-sig"))) if row
    }
    results = []
    for region in cfg.regions:
        if region.google is not None:
            row = lookup.get(str(region.google))
            results.append(
                dict(
                    engine="google",
                    region=region.google,
                    exists=row is not None,
                    provider_row=row,
                    name=region.name,
                    downloaded_at=meta["downloaded_at"],
                )
            )
        if region.yandex is not None:
            result = await collect_serp(
                client, SerpArgs(query="погода", engine="yandex", region=region.yandex)
            )
            hints = region.hints or [region.name]
            hits = sum(
                any(h.casefold() in (doc["title"] + " " + doc["url"]).casefold() for h in hints)
                for doc in result["results"]
            )
            results.append(
                dict(
                    engine="yandex",
                    region=region.yandex,
                    name=region.name,
                    heuristic=True,
                    hits=hits,
                    likely_match=hits >= 3,
                    guaranteed_validation=False,
                )
            )
    return dict(status="ok", results=results)
