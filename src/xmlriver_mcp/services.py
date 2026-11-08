"""Shared business logic for MCP, CLI and batch collection."""

import asyncio
from datetime import datetime, timezone

from .errors import XmlRiverError
from .models import DEFAULT_REGION, INPUTS, SerpArgs, TopArgs
from .parsers import find_matches, parse_serp, parse_wordstat


def utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


async def collect_serp(client, args: SerpArgs, *, progress: dict | None = None) -> dict:
    out = progress if progress is not None else {}
    out.update(
        query=args.query,
        engine=args.engine,
        region=args.region if args.region is not None else DEFAULT_REGION[args.engine],
        device=args.device,
        results=[],
        successful_pages=0,
        checked_depth=0,
        complete=False,
        status="incomplete",
        fetched_at=utcnow(),
        source="api",
        estimated_cost_rub=None,
        exhausted=False,
    )
    for page_index in range(args.pages):
        xml = await client.fetch_serp(
            args.query,
            engine=args.engine,
            region=out["region"],
            device=args.device,
            page_index=page_index,
        )
        docs = parse_serp(xml)
        out["successful_pages"] += 1
        for index, doc in enumerate(docs, 1):
            out["results"].append(
                {
                    **doc,
                    "position": len(out["results"]) + 1,
                    "page": page_index + 1,
                    "index_in_page": index,
                }
            )
        out["checked_depth"] = len(out["results"])
        out["fetched_at"] = utcnow()
        if not docs:
            out["exhausted"] = True
            break
    out.update(complete=True, status="ok", results_count=len(out["results"]))
    if args.target_domain:
        out["target_domain"] = args.target_domain
        out["matches"] = find_matches(out["results"], args.target_domain, args.include_subdomains)
    return out


async def wordstat_top(client, args: TopArgs) -> dict:
    data = await client.fetch_wordstat(args.query, regions=args.regions, device=args.device)
    phrases = parse_wordstat(
        data,
        limit=args.limit,
        include_associations=args.include_associations,
        min_frequency=args.min_frequency,
        drop_query=args.query if args.drop_self else None,
    )
    return dict(
        query=args.query,
        regions=args.regions,
        returned=len(phrases),
        total_popular=len(data["popular"]),
        total_associations=len(data.get("associations", [])),
        phrases=phrases,
    )


async def execute(client, name: str, arguments: dict, *, deadline: float = 180) -> dict:
    model = INPUTS[name].model_validate(arguments)
    try:
        async with asyncio.timeout(deadline):
            if name == "get_balance":
                return dict(
                    balance=await client.get_balance(),
                    cost_google_per_1000=await client.get_cost("google"),
                    cost_yandex_per_1000=await client.get_cost("yandex"),
                )
            if name == "serp":
                assert isinstance(model, SerpArgs)
                return await collect_serp(client, model)
            if name == "wordstat_top":
                assert isinstance(model, TopArgs)
                return await wordstat_top(client, model)
            return await client.fetch_wordstat(**model.model_dump(), pagetype="history")
    except TimeoutError:
        raise XmlRiverError(kind="deadline") from None
