import asyncio
import json
import logging
import os
from contextlib import asynccontextmanager

from mcp import types
from mcp.server import Server
from mcp.server.stdio import stdio_server
from mcp.shared.exceptions import McpError
from pydantic import ValidationError

from .client import XmlRiverClient, resolve_xmlriver_creds
from .errors import XmlRiverError
from .models import INPUTS, OUTPUTS
from .services import execute


@asynccontextmanager
async def lifespan(server):
    async with XmlRiverClient(
        *resolve_xmlriver_creds(),
        rate_max=int(os.getenv("XMLRIVER_RATE_MAX", "9")),
        rate_window=float(os.getenv("XMLRIVER_RATE_WINDOW", "1")),
        max_concurrent=int(os.getenv("XMLRIVER_CONCURRENCY", "9")),
    ) as client:
        yield {"client": client}


server = Server("xmlriver", lifespan=lifespan)


@server.list_tools()
async def list_tools():
    return [
        types.Tool(
            name=name,
            description=(
                "Get account balance and approximate tariffs (network requests)."
                if name == "get_balance"
                else "Process up to 100 phrases concurrently for SERP or Wordstat. "
                "Use operation, queries and common parameters. Up to 9 concurrent requests; "
                "paid retries count toward max_attempts. Returns ordered results and partial errors. "
                "Saves a durable journal and cache automatically and returns file paths. "
                "Use CLI batch for lists larger than 100 phrases."
                if name == "batch"
                else "Query XMLRiver. Requests may charge your account; retries may also be charged. "
                "SERP positions count organic documents, not visual search blocks."
            ),
            inputSchema=model.model_json_schema(),
            outputSchema=OUTPUTS[name].model_json_schema(),
            annotations=types.ToolAnnotations(
                readOnlyHint=name == "get_balance",
                destructiveHint=False,
                openWorldHint=True,
                idempotentHint=False,
            ),
        )
        for name, model in INPUTS.items()
    ]


@server.call_tool(validate_input=False)
async def call_tool(name: str, arguments: dict):
    # Pydantic validation avoids echoing arbitrary input values in SDK error text.
    try:
        result = await execute(
            server.request_context.lifespan_context["client"],
            name,
            arguments,
            deadline=float(os.getenv("XMLRIVER_DEADLINE", "180")),
        )
        OUTPUTS[name].model_validate(result)
    except ValidationError:
        result = XmlRiverError(kind="params").payload()
        return types.CallToolResult(
            content=[types.TextContent(type="text", text=json.dumps(result))], isError=True
        )
    except XmlRiverError as exc:
        return types.CallToolResult(
            content=[types.TextContent(type="text", text=json.dumps(exc.payload()))], isError=True
        )
    except Exception:
        return types.CallToolResult(
            content=[types.TextContent(type="text", text=json.dumps(XmlRiverError().payload()))],
            isError=True,
        )
    return types.CallToolResult(
        content=[types.TextContent(type="text", text=json.dumps(result, ensure_ascii=False))],
        structuredContent=result,
        isError=name == "batch" and result.get("status") != "ok",
    )


_sdk_call_handler = server.request_handlers[types.CallToolRequest]


async def protocol_call(request):
    if request.params.name not in INPUTS:
        raise McpError(types.ErrorData(code=-32602, message="Unknown tool"))
    return await _sdk_call_handler(request)


server.request_handlers[types.CallToolRequest] = protocol_call


async def serve():
    async with stdio_server() as (read, write):
        await server.run(read, write, server.create_initialization_options())


def main():
    # HTTPX INFO logs contain authenticated request URLs. Never enable them here.
    logging.getLogger("httpx").disabled = True
    logging.getLogger("httpcore").disabled = True
    asyncio.run(serve())


if __name__ == "__main__":
    main()
