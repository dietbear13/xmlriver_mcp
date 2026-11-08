"""Test-only subprocess: mock HTTP without a production test backdoor."""

import asyncio
from contextlib import asynccontextmanager

import httpx

from xmlriver_mcp.client import XmlRiverClient
from xmlriver_mcp.server import serve, server


def respond(request):
    path = request.url.path
    if "get_balance" in path:
        return httpx.Response(200, text="100")
    if "get_cost" in path:
        return httpx.Response(200, text="25")
    if "wordstat" in path:
        if request.url.params["pagetype"] == "history":
            return httpx.Response(200, json={"TotalValue": "12", "opaque": {"anything": True}})
        return httpx.Response(200, json={"popular": [{"text": "coffee beans", "value": "15"}]})
    return httpx.Response(200, text="<yandexsearch><response><results/></response></yandexsearch>")


@asynccontextmanager
async def lifespan(server):
    async with XmlRiverClient(
        "test-user", "test-secret", transport=httpx.MockTransport(respond)
    ) as api:
        yield {"client": api}


server.lifespan = lifespan
asyncio.run(serve())
