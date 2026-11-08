# XMLRiver MCP Toolkit

Independent, unofficial XMLRiver integration: four MCP tools, an asynchronous Python
client, and collection commands. Bring your own XMLRiver account and credentials.
Python 3.11+. MIT. No hosted service, OAuth or shared credential storage.

**Release status:** 0.1.0 is in preparation; publication and client compatibility
must be verified before treating this as a released package.

```sh
uvx --from xmlriver-mcp-toolkit==0.1.0 xmlriver-mcp
pip install xmlriver-mcp-toolkit==0.1.0
xmlriver balance
xmlriver wordstat top "coffee" --limit 50
xmlriver wordstat history "coffee" --start 01.01.2026 --end 31.03.2026
xmlriver serp "coffee" --engine google --pages 5 --target-domain example.org
xmlriver positions run --config examples/positions.yaml --dry-run
xmlriver positions run --config examples/positions.yaml
xmlriver positions update-csv --config examples/positions.yaml --run RUN_ID
xmlriver regions validate --config examples/positions.yaml --refresh
xmlriver regions run --config examples/positions.yaml
```

Set `XMLRIVER_USER` and `XMLRIVER_KEY` in your process environment. Never pass
credentials as command arguments. This package does not search `.mcp.json`.

## MCP clients

The executable uses standard MCP stdio. stdout contains protocol messages only.
Configure Claude Desktop or Claude Code (`mcpServers`), or Cursor (`mcpServers`)
with this server entry. The client must supply the credential environment variables
using its own configuration or inherited environment:

```json
{"mcpServers":{"xmlriver":{"command":"uvx","args":["--from","xmlriver-mcp-toolkit==0.1.0","xmlriver-mcp"]}}}
```

For Codex, add to its MCP configuration:

```toml
[mcp_servers.xmlriver]
command = "uvx"
args = ["--from", "xmlriver-mcp-toolkit==0.1.0", "xmlriver-mcp"]
env_vars = ["XMLRIVER_USER", "XMLRIVER_KEY"]
```

Discovery works without credentials. `get_balance`, `wordstat_top`,
`wordstat_history`, `serp` retain the original success fields. Responses contain
matching JSON text and structured content; execution errors set `isError`.
Wordstat history preserves the provider object without interpreting its series.

Compatibility matrix: SDK subprocess stdio is tested automatically on CI.
Claude Code, Claude Desktop, Codex and Cursor interactive testing is **pending**;
configuration examples are not claims of completed client certification.
Remote-only clients are outside this release's scope.

## Limits and cost

SERP defaults: Yandex, Moscow (213; Google 1011969), mobile, one page.
Pages: 1–10. Wordstat defaults: region 213, all devices, limit 50 (1–1000).
SERP uses `mobile`; Wordstat uses `phone`. Dates use `dd.mm.yyyy`.
The provider may impose additional period restrictions.

Each page is a separate potentially paid request. Short pages do not end collection;
only confirmed empty results or the page limit do. Positions count organic documents
in the returned sequence, not visual blocks. `page` and `index_in_page` are 1-based.
Malformed XML/JSON and HTML are errors, never "not found".

The shared client defaults to 5 requests per 1.5 seconds, at most 5 in flight.
MCP accepts `XMLRIVER_RATE_MAX`, `XMLRIVER_RATE_WINDOW`, `XMLRIVER_DEADLINE`
(default 180 seconds). Cancellation stops further requests.
Temporary errors have bounded retries and respect Retry-After; authentication,
balance and parameter errors are not retried. Retried and timed-out requests may
still incur charges. `http_attempts` is a local counter, not a billing statement.
`successful_pages` counts validated page responses, including confirmed empty pages.
`estimated_cost_rub` is approximate and null if pricing is unknown. Batch prices
are explicitly configured per 1000 requests; `balance` fetches current tariffs.
Do not enable HTTPX debug logging in embedding applications: it can log request URLs.

## Storage and CSV

Batch commands accept explicit `--config` and `--workspace`. YAML CSV paths are
relative to the configuration file; default workspace is `xmlriver-data` beside it.
`--dry-run` performs no networking and creates no files or directories. The `balance`
command and region validation are explicitly network operations; Yandex validation
uses paid "погода" queries and is only a heuristic.

Cache version 2 includes query, engine, region, device, target domain, subdomain mode
and depth. Default TTL is 24 hours. Old caches are ignored and never deleted.
Runs retain partial results, actual fetched timestamps and attempt counters. JSON
files use atomic replacement; cache/history/CSV writes use process locks.
CSV updates preserve existing columns, row order, encoding (UTF-8, BOM, CP1251),
delimiter and newline style, with a unique backup before replacement. Separate
engine dates are used; the common date is filled only when both dates match.
Multiple regional contexts cannot be merged into a single engine column.

Exit codes: 0 success, 2 invalid arguments, 3 failure, 4 partial/interrupted run.
CLI prints JSON to stdout and diagnostics to stderr.

## Python API

```python
from xmlriver_mcp.client import XmlRiverClient, resolve_xmlriver_creds
from xmlriver_mcp.models import SerpArgs
from xmlriver_mcp.services import collect_serp


async def example():
    async with XmlRiverClient(*resolve_xmlriver_creds()) as client:
        return await collect_serp(client, SerpArgs(query="coffee", pages=2))
```

Provider references: [SERP parameters](https://xmlriver.com/apidoc/api-about/),
[Wordstat parameters](https://xmlriver.com/apiwordstatnew/apiwn-about/).
Reviewed 2026-11-08. Wordstat JSON is decoded as UTF-8, matching the prior observed
responses; no claim that every provider format has been independently live-tested.
