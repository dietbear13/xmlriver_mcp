"""Pure parsing and domain matching; malformed output is never an empty SERP."""

import xml.etree.ElementTree as ET
from urllib.parse import urlsplit

from .errors import XmlRiverError


def xml_root(text: str):
    try:
        root = ET.fromstring(text)
    except ET.ParseError:
        raise XmlRiverError(kind="format") from None
    if root.tag != "yandexsearch" or root.find("response") is None:
        raise XmlRiverError(kind="format")
    return root


def extract_error(xml_text: str) -> tuple[str, str] | None:
    err = xml_root(xml_text).find(".//error")
    if err is None:
        return None
    return err.get("code", ""), "Provider error"


def parse_serp(xml_text: str) -> list[dict]:
    root = xml_root(xml_text)
    error = root.find(".//error")
    if error is not None:
        if error.get("code") == "15":
            return []
        raise XmlRiverError(code=error.get("code"))
    if root.find(".//results") is None:
        raise XmlRiverError(kind="format")
    docs = []
    for doc in root.findall(".//results//doc"):
        url = (doc.findtext("url") or "").strip()
        if not url:
            raise XmlRiverError(kind="format")
        title = doc.find("title")
        docs.append(
            {"url": url, "title": "" if title is None else "".join(title.itertext()).strip()}
        )
    return docs


def parse_wordstat(
    data: dict,
    *,
    limit: int | None = None,
    include_associations: bool = False,
    min_frequency: int = 0,
    drop_query: str | None = None,
) -> list[dict]:
    if not isinstance(data, dict) or not isinstance(data.get("popular"), list):
        raise XmlRiverError(kind="format")
    rows = []
    for bucket in ["popular"] + (["associations"] if include_associations else []):
        items = data.get(bucket, [])
        if not isinstance(items, list):
            raise XmlRiverError(kind="format")
        for item in items:
            try:
                phrase = item["text"].strip()
                frequency = int(str(item["value"]).replace(" ", "").replace("\u00a0", ""))
            except (KeyError, TypeError, ValueError, AttributeError):
                raise XmlRiverError(kind="format") from None
            if not phrase or frequency < 0:
                raise XmlRiverError(kind="format")
            if frequency < min_frequency or (
                drop_query and phrase.casefold() == drop_query.strip().casefold()
            ):
                continue
            rows.append(
                {
                    "phrase": phrase,
                    "frequency": frequency,
                    "is_association": bucket == "associations" or bool(item.get("isAssociations")),
                }
            )
    rows.sort(key=lambda row: row["frequency"], reverse=True)
    return rows if limit is None else rows[:limit]


def normalize_domain(domain: str) -> str:
    host = urlsplit(domain if "://" in domain else "//" + domain).hostname or ""
    host = host.rstrip(".").lower().encode("idna").decode("ascii")
    return host.removeprefix("www.")


def match_domain(url: str, domain: str, include_subdomains: bool = True) -> bool:
    try:
        host, target = normalize_domain(url), normalize_domain(domain)
        return bool(target) and (
            host == target or (include_subdomains and host.endswith("." + target))
        )
    except (ValueError, UnicodeError):
        return False


def find_matches(
    docs: list[dict], domain: str, include_subdomains: bool = True, *, position_offset: int = 0
) -> list[dict]:
    return [
        {
            "position": position_offset + i,
            "url": doc["url"],
            **{key: doc[key] for key in ("page", "index_in_page") if key in doc},
        }
        for i, doc in enumerate(docs, 1)
        if match_domain(doc["url"], domain, include_subdomains)
    ]
