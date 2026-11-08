import json
import os
import sys
from pathlib import Path

from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client


async def test_real_stdio_success():
    params = StdioServerParameters(
        command=sys.executable, args=[str(Path(__file__).with_name("stdio_fixture.py"))]
    )
    async with stdio_client(params) as (read, write):
        async with ClientSession(read, write) as session:
            await session.initialize()
            tools = await session.list_tools()
            assert {tool.name for tool in tools.tools} == {
                "get_balance",
                "wordstat_top",
                "wordstat_history",
                "serp",
            }
            for name in ("get_balance", "wordstat_top", "wordstat_history", "serp"):
                args = {} if name == "get_balance" else {"query": "coffee"}
                result = await session.call_tool(name, args)
                assert not result.isError
                assert json.loads(result.content[0].text) == result.structuredContent
            result = await session.call_tool("serp", {"query": "q", "pages": 11})
            assert result.isError


async def test_discovery_without_credentials_and_restart():
    env = {k: v for k, v in os.environ.items() if not k.startswith("XMLRIVER_")}
    for _ in range(2):
        params = StdioServerParameters(
            command=sys.executable, args=["-m", "xmlriver_mcp.server"], env=env
        )
        async with stdio_client(params) as (read, write):
            async with ClientSession(read, write) as session:
                await session.initialize()
                assert len((await session.list_tools()).tools) == 4
                result = await session.call_tool("get_balance", {})
                assert result.isError
                assert json.loads(result.content[0].text)["kind"] == "auth"
