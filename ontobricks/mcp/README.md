# Connecting an AI agent to the NexusRetail graph (MCP)

This is the **Tier C** piece: once OntoBricks has built the NexusRetail graph and
you publish the domain over MCP, any MCP client — Claude Code, Claude Desktop, or
the Databricks Playground — can reason over the governed graph.

OntoBricks deploys a **separate MCP companion app** (named `mcp-<app>` by the
deploy config) that exposes the server at **`/mcp`** using the **Streamable HTTP**
transport.

## 1. Publish the domain (in OntoBricks)

1. **Registry → Browse →** expand `nexusretail` → **Set as Active** on the
   reviewed version.
2. **Domain Information → Global tab →** enable the **API / MCP** flag (domains
   without it are hidden from MCP).
3. In the per-domain **MCP policy**, allow the tools this demo uses:
   `describe_ontology`, `list_entity_types`, `describe_entity`,
   `get_entity_context`, `compute_virtual_attributes`, `invoke_entity_action`,
   `get_graphql_schema`, `query_graphql`.

## 2. Point a client at it

Copy the `ontobricks` block from [`ontobricks.mcp.json`](./ontobricks.mcp.json)
and fill in:

- `<MCP_APP_URL>` — the URL of the deployed **`mcp-<app>`** app (Databricks →
  Compute → Apps → your MCP app → *App URL*).
- `<DATABRICKS_TOKEN_OR_OAUTH>` — a Databricks token/OAuth credential for a
  principal that has access to the MCP app. For a quick demo a Personal Access
  Token works; for anything shared, prefer OAuth (Databricks Apps enforce the
  app's ACL).

### Claude Code
Add the block to a `.mcp.json` at the repo root (or run `claude mcp add`). Then
in a session: `/mcp` to confirm `ontobricks` is connected and its tools are
listed.

> Note: `.mcp.json` at the repo root is committed and shared with anyone who
> clones the repo. Do **not** commit a real token — use an OAuth entry or an
> env-var placeholder and keep secrets out of git. The file in this folder is a
> template only.

### Claude Desktop
Settings → Developer → Edit Config → paste the `ontobricks` block into
`mcpServers` → restart Claude Desktop.

### Databricks Playground
The Playground can attach the OntoBricks MCP app directly as a tool source — no
client config needed. Select the `mcp-<app>` app in the Playground tool picker.

## 3. The two-step tool workflow

The server is stateful per session:

1. `list_domains` → then `select_domain("nexusretail")` — **always first**.
2. Then the graph tools operate on the selected domain: `list_entity_types`,
   `describe_entity`, `get_entity_context`, `compute_virtual_attributes`,
   `invoke_entity_action`, and `query_graphql` (after `get_graphql_schema`).

See [`../demo/DEMO_SCRIPT.md`](../demo/DEMO_SCRIPT.md) for the exact agent prompts
that walk the faulty-batch traversal end to end.
