# OntoBricks Runbook — NexusRetail Knowledge Graph (Tier C)

End-to-end, copy-paste steps to stand up OntoBricks against the NexusRetail data
this repo already produces, build the curated knowledge graph, and publish it to
an AI agent over MCP.

> **Mental model.** OntoBricks is a **separate Databricks Labs app** you clone
> and deploy — it is *not* part of this bundle. This repo produces the governed
> data + the curated ontology; OntoBricks turns that into a graph. Keeping them
> separate preserves this repo's "deploy to any workspace, no hardcoded IDs"
> property.

---

## 0. Prerequisites (do these first)

| Requirement | Check / how to get it |
|---|---|
| NexusRetail pipeline has run | `online_retail_silver.*` and `online_retail_metrics.*` tables exist (deploy this repo's bundle first). |
| Databricks **Apps** enabled | Workspace admin setting. |
| **SQL Warehouse** (serverless) | Note its ID — reuse the `warehouse_id` from your `databricks.local.yml`. |
| **Model Serving** access | A Foundation Model / external LLM endpoint for the Ontology Wizard + Auto-Map. |
| **Lakebase Autoscaling** DB | ⚠️ **Not present in this workspace yet** — provision it (step 1). Provisioned Lakebase is *not* supported; must be Autoscaling. |
| **Unity Catalog Volume** | For binary artefacts; step 1 creates one. |
| `psql` (libpq) on PATH | `brew install libpq && brew link --force libpq` (for the Lakebase bootstrap scripts). |
| `databricks` CLI, authed | `databricks auth login --host https://<workspace>` |

---

## 1. Provision the missing infrastructure

### 1a. Lakebase Autoscaling project + branch + database
```bash
# Create an Autoscaling project (UI: Compute → Postgres → New project, OR API):
databricks api post /api/2.0/postgres/projects --json '{
  "project": { "display_name": "ontobricks-app" }
}'

# Note the returned project id, then create a database in its default branch.
# Confirm the db-… resource id (needed for the deploy bundle):
databricks postgres list-databases "projects/<PROJECT_ID>/branches/main" -o json
```
Record: **project name**, **branch** (`main`), **database name**
(e.g. `ontobricks_registry`), and the `db-…` **resource id**.

### 1b. Unity Catalog Volume for the registry
```sql
-- Run on your SQL warehouse. Reuse your existing catalog.
CREATE SCHEMA IF NOT EXISTS ${catalog}.ontobricks;
CREATE VOLUME IF NOT EXISTS ${catalog}.ontobricks.OntoBricksRegistry;
```

---

## 2. Clone and deploy OntoBricks

> **Important:** everything in this step happens **inside the cloned OntoBricks
> repo**, not this DQ_UC_Ontology repo. OntoBricks is a Databricks Labs app with
> its own bundle (`databricks.yml`), its own `Makefile`, and its own config file
> `scripts/deploy.config.sh`. You edit *that* file — it is the single source of
> truth for the deploy.

### 2a. Clone + install
```bash
git clone https://github.com/databrickslabs/ontobricks.git
cd ontobricks
uv sync --frozen --extra lakebase        # committed public-PyPI lock
databricks auth login --host https://dbc-31a46e3c-394b.cloud.databricks.com   # your dev host
```

### 2b. Edit `scripts/deploy.config.sh`
Open `scripts/deploy.config.sh` **in the cloned ontobricks repo** and set the
values below. It ships with someone else's demo values (catalog `benoit_cayla`,
instance `08x`, etc.) — replace them. Pre-filled here with the values from this
repo's `databricks.local.yml`:

```bash
# ── 0a. Instance identity — the app name becomes ontobricks-<id> ─────
DEFAULT_INSTANCE_ID="nexus01"

# ── 0b. Workspace constants ──────────────────────────────────────────
DEFAULT_DATABRICKS_PROFILE="Myenv"                 # your ~/.databrickscfg profile
DEFAULT_WAREHOUSE_ID="6f01c3c8b2af0309"            # your serverless SQL warehouse

# Unity Catalog — the Volume registry (create in step 1b)
DEFAULT_REGISTRY_CATALOG="gsethi"
DEFAULT_REGISTRY_SCHEMA="ontobricks"               # matches step 1b schema
DEFAULT_REGISTRY_VOLUME="OntoBricksRegistry"       # matches step 1b volume

# Lakebase Autoscaling — from step 1a
DEFAULT_LAKEBASE_PROJECT="ontobricks-app"          # your Autoscaling project name
DEFAULT_LAKEBASE_BRANCH="main"                     # branch you created
DEFAULT_LAKEBASE_DATABASE="ontobricks_registry"    # Postgres datname (underscores OK)
DEFAULT_LAKEBASE_SCHEMA="ontobricks_registry"      # Postgres schema (per-instance)
```
> Notes: `DEFAULT_INSTANCE_ID` is the **only** line that must be unique per
> deployment — the app names (`ontobricks-<id>` + `mcp-ontobricks-<id>`) and the
> DAB target (`dev-lakebase-<id>`) are derived from it. For `DEFAULT_LAKEBASE_DATABASE`
> use the `status.postgres_database` value from `databricks postgres list-databases`,
> **not** the hyphenated `database_id`.

### 2c. Deploy
```bash
make deploy-dry-run     # runs ALL preflight/validate/resource checks, no changes
make deploy             # deploys + starts both apps (main + MCP), Lakebase backend
```

### 2d. Bind resources + bootstrap permissions
`make deploy` provisions two Databricks Apps — `ontobricks-nexus01` (web UI + REST
+ GraphQL) and `mcp-ontobricks-nexus01` (the MCP server). After it finishes:

1. In the **Databricks Apps UI**, confirm each app's bound resources: the
   `sql-warehouse`, the UC `volume`, and the Lakebase `database`.
2. Grant the app service principal what it needs:
   ```bash
   make bootstrap-perms      # app SP CAN_MANAGE on itself + the analytics job
   make bootstrap-lakebase   # app SP USAGE/DML on the Lakebase registry schema
   ```
3. Open the `ontobricks-nexus01` **App URL** → you should see the OntoBricks home.
   In **Settings → Initialize** the app creates its `ontobricks_registry` schema
   in Lakebase on first run.

---

## 3. Build the NexusRetail graph (the "four clicks", curated)

1. **Create domain** `nexusretail` (Registry → New domain).
2. **Import metadata** — Domain → Metadata → import from Unity Catalog. Select
   `${catalog}.online_retail_silver` (the dim/fact tables) and, if you want the
   product taxonomy edges, `online_retail_bronze.bronze_order_items`.
3. **Import the curated ontology** instead of only auto-generating it:
   Ontology → Import → **OWL/RDFS** → upload
   [`ontology/nexusretail.ttl`](./ontology/nexusretail.ttl). This makes the graph
   match the Genie concept Pages. (You can still run the **Ontology Wizard** first
   and then reconcile against this file, or run the **Pitfalls Detector** on it.)
4. **Auto-Map** — Mapping → Auto-Map, then reconcile every class/relationship
   against [`ontology/MAPPINGS.md`](./ontology/MAPPINGS.md). Pay attention to:
   - `nr:memberOfBatch` → filter `silver_dim_products.faulty_batch = true`.
   - `nr:containsProduct` → the `bronze_order_items` bridge.
   - `nr:returnsProduct` → explode `silver_fact_returns.returned_product_ids`.
5. **Synchronize** — Knowledge Graph → Status → Build. Confirm the triple count.

---

## 4. Add reasoning + live functions

- **SHACL** — two-part workflow (OntoBricks compiles shapes to SQL):
  1. **Import the property shapes** — Sidebar → **Ontology → Data Quality → Import**
     → upload [`shacl/faulty_batch_shapes.ttl`](./shacl/faulty_batch_shapes.ttl).
     Section A shapes (Invoice completeness, Order structure, Return completeness)
     import and score directly.
  2. **Build the two narrative shapes by hand** — Sidebar → **Ontology → Data
     Quality → Add Shape**, using the recipes in Section B of that file
     (Conformance: *IF `returnReasonCode` = faulty_product THEN
     `faultyBatchInvolved` = true*; Consistency: *IF `faultyBatch` = true THEN
     `memberOfBatch` minCount 1*). OntoBricks drops the IF block on file import,
     so the conditional rules must be created in the UI to score.
  3. **Run it** — Sidebar → **Knowledge Graph → Data Quality → Run Validation**
     (after the graph is built). A clean run proves the narrative is internally
     consistent; violations are demo talking points, and each check shows the
     generated SQL.
- **UC functions** — run [`uc_functions/virtual_attributes.sql`](./uc_functions/virtual_attributes.sql)
  on your SQL warehouse (set the `${catalog}` param), then declare them on the
  ontology classes: `customer_return_risk_score` +
  `customer_faulty_batch_exposure` as **virtual attributes** on `Customer`;
  `product_affected_cohort` as an **action** on `Product`;
  `region_faulty_batch_impact` as an **action** on `Region`.

---

## 5. Govern + publish

1. Move the domain through **DRAFT → IN-REVIEW → PUBLISHED** (collect the review
   sign-off — a governance talking point that mirrors your UC governance story).
2. **Set as Active** and enable **API / MCP** (see
   [`mcp/README.md`](./mcp/README.md)).
3. Connect an agent (Claude Code / Desktop / Playground) and run the demo in
   [`demo/DEMO_SCRIPT.md`](./demo/DEMO_SCRIPT.md).

---

## 6. Portability (optional)

Export the finished domain as an **OBX** file (Registry → Browse → Export) or via
the `registry_transfer.sh` CLI to promote it dev → staging → prod. The MCP policy
travels with the domain.

---

## Troubleshooting

| Symptom | Likely cause / fix |
|---|---|
| `make deploy` fails on Lakebase | Project is Provisioned, not **Autoscaling** — recreate as Autoscaling. |
| Auto-Map produces odd classes | You skipped the ontology import in step 3.3 — import `nexusretail.ttl` first, then reconcile. |
| MCP app not listed by client | Domain's **API / MCP** flag is off, or the token's principal lacks access to the `mcp-<app>` app. |
| `memberOfBatch` edge missing | The mapping filter `faulty_batch = true` was dropped — re-add it (only 8 products should link). |
| Graph build empty | Silver tables not populated — run this repo's pipeline first. |
