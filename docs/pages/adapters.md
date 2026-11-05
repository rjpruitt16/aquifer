# Framework adapters

Aquifer has a framework-neutral core (idempotency, persistence, rate control, dispatch, SSE events, L8 signing, webhook delivery) with pluggable front doors:

| Adapter | Env | Purpose |
|---------|-----|---------|
| HTTP | `AQUIFER_ADAPTER=http` | REST/SSE API on `PORT` (the default) |
| MCP stdio | `AQUIFER_ADAPTER=mcp-stdio` | MCP server exposing Aquifer tools over stdio (the published Docker image's default) |
| A2A | `AQUIFER_ADAPTER=a2a` | Agent2Agent protocol (v1.0) agent over JSON-RPC/HTTPS |

**[ADAPTERS.md](../reference/ADAPTERS.md)** has the full reference for each built-in adapter (MCP tool list, A2A Agent Card details), plus how to write your own `FrameworkAdapter`, storage backend, or metrics adapter.
