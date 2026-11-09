"""Exit abruptly after one page, bypassing cancellation and all finally blocks."""

import asyncio
import os
import sys
from pathlib import Path

import httpx

from xmlriver_mcp.batch import build_tasks, run_batch
from xmlriver_mcp.client import XmlRiverClient
from xmlriver_mcp.config import Config
from xmlriver_mcp.models import BatchArgs
from xmlriver_mcp.query_batch import run_queries
from xmlriver_mcp.storage import Storage


async def main():
    def handler(request):
        if request.url.params["page"] == "1":
            os._exit(23)
        return httpx.Response(
            200,
            text="<yandexsearch><response><results><doc><url>https://example.org</url></doc></results></response></yandexsearch>",
        )

    store = Storage(Path(sys.argv[1]))
    async with XmlRiverClient("test", "test", transport=httpx.MockTransport(handler)) as client:
        if sys.argv[2] == "positions":
            cfg = Config(target_domain="example.org", depth=30)
            await run_batch(cfg, build_tasks(cfg, ["q"]), store, client=client)
        else:
            await run_queries(
                client,
                BatchArgs(operation="serp", queries=["q"], parameters={"pages": 3}),
                store=store,
            )


asyncio.run(main())
