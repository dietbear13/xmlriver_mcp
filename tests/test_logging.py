import logging

import httpx

from xmlriver_mcp.client import XmlRiverClient


async def test_credentials_redacted_with_httpx_logging(caplog):
    caplog.set_level(logging.INFO, logger="httpx")
    async with XmlRiverClient(
        "private-user",
        "private-key",
        transport=httpx.MockTransport(lambda request: httpx.Response(200, text="10")),
    ) as client:
        await client.get_balance()
    assert "private-user" not in caplog.text
    assert "private-key" not in caplog.text
    assert "[redacted]" in caplog.text
