# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# NexusRetail Analytics — Unity Catalog Discover Domains + Ontology Pages
# =============================================================================
# Creates the "Online Retail" Discover domain and its three subdomains, then
# generates a bulk-import-ready Pages file for the Genie Ontology.
#
# WHY THIS EXISTS
#   Discover *Domains* group governed assets by business purpose; *Pages* are
#   governed definitions of business concepts that Genie One cites as
#   authoritative context. Together they are the human-modelled layer of the
#   Genie Ontology — which is exactly what this repo (DQ_UC_Ontology) is about.
#
# WHAT IT DOES
#   1. Registers the governed tags that back the domains (idempotent).
#   2. Creates the parent domain + 3 subdomains via the Domains API (idempotent,
#      and graceful if the account has hit its domain quota).
#   3. Reads the governed tags + table comments that run_governance.py SAVED to
#      Unity Catalog, groups tables by (sub)domain, and — together with a set of
#      curated concept definitions — writes a Markdown file you feed to Genie
#      Code's "Bulk import pages" in Discover (Discover ▸ Pages ▸ Create page ▸
#      Genie Code ▸ Bulk import pages).
#
# WHY BULK-IMPORT (and not a direct API)
#   Pages have no public create API today (Beta). The supported programmatic
#   path is Genie Code bulk import from a document — so we generate that
#   document deterministically from governed metadata.
#
# PREREQUISITES
#   • run_governance.py has run (comments + governed domain tags applied).
#   • Discover previews enabled for the account + workspace (account admin).
#   • Caller holds MANAGE DISCOVERY and APPLY TAG.
#   • Domains are ACCOUNT-level and capped per account (currently 300). If the
#     account is at the cap, domain creation is skipped with guidance; tags +
#     the Pages file are still produced.
#
# Run as a bundle job:  databricks bundle run nexus_retail_domains -t dev
# Or locally:           CATALOG=x WAREHOUSE_ID=y OWNER_USER=z \
#                       python3 setup/03_domains_setup.py

# COMMAND ----------

import os
import io
import time
import json
import requests
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState


def _get(param: str, default: str = "") -> str:
    """Read from job widgets when running as a notebook, else env vars."""
    try:
        return dbutils.widgets.get(param)          # noqa: F821  (Databricks runtime)
    except Exception:
        return os.environ.get(param.upper(), default)


try:
    dbutils.widgets.text("catalog", "your_catalog_name")          # noqa: F821
    dbutils.widgets.text("warehouse_id", "your_warehouse_id")     # noqa: F821
    dbutils.widgets.text("owner_user", "your.email@company.com")  # noqa: F821
except Exception:
    pass

CATALOG      = _get("catalog")
WAREHOUSE_ID = _get("warehouse_id")
OWNER_USER   = _get("owner_user")

if any("your_" in v or "your." in v for v in [CATALOG, WAREHOUSE_ID, OWNER_USER]):
    raise ValueError(
        "Set catalog, warehouse_id, owner_user — via job base_parameters or env "
        "vars: CATALOG=x WAREHOUSE_ID=y OWNER_USER=z python3 03_domains_setup.py"
    )

w = WorkspaceClient()

# COMMAND ----------

# ── Domain specification ──────────────────────────────────────────────────────
# One parent domain + three subdomains, mirroring the genie_domain tags that
# run_governance.py applies. Subdomain tags follow the required
# `{parentTag}/{subdomain}` naming convention. Parent and subdomain tags are
# INDEPENDENT governed tags (a table carries both).

PARENT_TAG = "online_retail"

PARENT_DOMAIN = {
    "tag_key":  PARENT_TAG,
    "subtitle": "NexusRetail — global e-commerce analytics",
    "description": (
        "The NexusRetail online-retail data platform: a governed medallion "
        "architecture (bronze → silver → gold → metrics) over 24 months of "
        "orders, customers, and returns across 7 regions and 65 countries. "
        "Home of the Q4-2025 faulty-batch return story. Browse the subdomains "
        "for Sales Performance, Customer Analytics, and Returns &amp; Quality."
    ),
    "icon": {"name": "BASKET", "color": "#1B5E20"},
}

SUBDOMAINS = [
    {
        "tag_key":  f"{PARENT_TAG}/sales_performance",
        "genie_domain": "sales_performance",
        "title":    "Sales Performance",
        "subtitle": "Revenue, channels, and seasonality",
        "description": (
            "Revenue trends, channel mix, and seasonal patterns across regions. "
            "Backed by the sales metric view and daily-revenue gold tables."
        ),
        "icon": {"name": "PRESENTATION_CHART", "color": "#1565C0"},
    },
    {
        "tag_key":  f"{PARENT_TAG}/customer_analytics",
        "genie_domain": "customer_analytics",
        "title":    "Customer Analytics",
        "subtitle": "Segments, loyalty, and lifetime value",
        "description": (
            "Customer demographics, loyalty tiers, acquisition channels, and "
            "customer-lifetime-value segmentation."
        ),
        "icon": {"name": "USERS_THREE", "color": "#6A1B9A"},
    },
    {
        "tag_key":  f"{PARENT_TAG}/returns_quality",
        "genie_domain": "returns_quality",
        "title":    "Returns & Quality",
        "subtitle": "Return rates, faulty batch, and data quality",
        "description": (
            "Return-rate analysis, the FAULT-PHON-* faulty-batch investigation, "
            "and the DQ quarantine that captured the anomalies."
        ),
        "icon": {"name": "PACKAGE", "color": "#C62828"},
    },
]

TAG_TO_TITLE = {PARENT_TAG: "Online Retail"}
TAG_TO_TITLE.update({s["tag_key"]: s["title"] for s in SUBDOMAINS})

# ── Curated concept Pages ─────────────────────────────────────────────────────
# The authoritative business terms Genie One should prioritise. Definitions
# mirror the table/column comments and Genie knowledge snippets in this repo.
# domain = the (sub)domain tag the Page belongs to.

CURATED_PAGES = [
    {
        "name": "Gross Revenue", "domain": f"{PARENT_TAG}/sales_performance",
        "synonyms": ["revenue", "sales", "gross sales", "total revenue"],
        "description": "Total sales value from delivered, shipped, and confirmed orders, after line-item discounts, before refunds.",
        "body": (
            "**Gross Revenue** = `SUM(gross_revenue)` from delivered / shipped / confirmed orders. "
            "Discounts are already applied (`line_total = qty × price × (1 − discount)`). "
            "In metric views use `MEASURE(\\`Gross Revenue\\`)`; in gold tables use `SUM(gross_revenue)`. "
            "All amounts are USD. Q4 (Oct–Dec) is the seasonal peak at +40–50% volume."
        ),
        "related": ["metrics_sales_kpis", "gold_daily_revenue", "mv_category_revenue"],
        "sources": ["gold_daily_revenue"],
    },
    {
        "name": "Net Revenue", "domain": f"{PARENT_TAG}/sales_performance",
        "synonyms": ["net sales", "revenue after refunds"],
        "description": "Gross revenue minus refund_total for the period — revenue net of returns.",
        "body": "**Net Revenue** = Gross Revenue − `refund_total`. Deducts refunds for returned goods in the same period. Diverges most from Gross Revenue in Q4 2025 where the faulty-batch return spike inflates refunds.",
        "related": ["gold_regional_performance", "mv_regional_orders"],
        "sources": ["gold_regional_performance"],
    },
    {
        "name": "Average Order Value", "domain": f"{PARENT_TAG}/sales_performance",
        "synonyms": ["AOV", "avg order value"],
        "description": "Gross revenue divided by order count — average spend per order.",
        "body": "**Average Order Value (AOV)** = `SUM(gross_revenue) / NULLIF(SUM(order_count), 0)`. Available as `MEASURE(\\`Avg Order Value\\`)`. Varies by channel (partner_api tends highest) and loyalty tier.",
        "related": ["metrics_sales_kpis"],
        "sources": ["gold_daily_revenue"],
    },
    {
        "name": "Sales Channel", "domain": f"{PARENT_TAG}/sales_performance",
        "synonyms": ["channel", "order channel"],
        "description": "The route an order was placed through: web, mobile, or partner_api.",
        "body": "**Sales Channel** values: `web` (~55%), `mobile` (~35%), `partner_api` (~10%). Use the `Channel` dimension in `metrics_sales_kpis` to slice revenue, AOV, and customer mix by channel.",
        "related": ["metrics_sales_kpis", "gold_daily_revenue"],
        "sources": ["gold_daily_revenue"],
    },
    {
        "name": "Q4 Seasonal Peak", "domain": f"{PARENT_TAG}/sales_performance",
        "synonyms": ["holiday peak", "Q4 spike", "seasonal spike"],
        "description": "October–December is the peak sales period each year, ~40–50% higher order volume.",
        "body": "**Q4 Seasonal Peak** (Oct–Dec) shows 40–50% higher order volume year over year. Use `Sale Quarter` for QoQ comparisons. Distinct from the Q4-2025 *return* spike, which is a quality anomaly, not seasonality — see the Faulty Batch page.",
        "related": ["metrics_sales_kpis", "gold_daily_revenue"],
        "sources": ["gold_daily_revenue"],
    },
    {
        "name": "Customer Lifetime Value", "domain": f"{PARENT_TAG}/customer_analytics",
        "synonyms": ["CLV", "LTV", "lifetime value", "clv segment"],
        "description": "Per-customer aggregated purchase history, bucketed into High / Medium / Low / Churned segments.",
        "body": (
            "**Customer Lifetime Value (CLV)** is summarised one row per customer in `gold_customer_lifetime_value`. "
            "`clv_segment`: **High** (total_revenue ≥ $1,000 or Platinum) · **Medium** ($200–999) · "
            "**Low** (<$200) · **Churned** (no order in 180+ days). `orders_per_30_days` normalises purchase frequency."
        ),
        "related": ["gold_customer_lifetime_value", "metrics_customer_kpis"],
        "sources": ["gold_customer_lifetime_value"],
    },
    {
        "name": "Loyalty Tier", "domain": f"{PARENT_TAG}/customer_analytics",
        "synonyms": ["loyalty", "tier", "membership tier"],
        "description": "Program tier: Bronze, Silver, Gold, or Platinum. Higher tiers show higher AOV and repeat rate.",
        "body": "**Loyalty Tier**: Bronze (~40%) · Silver (~30%) · Gold (~20%) · Platinum (~10%). Platinum customers have the highest CLV and lowest churn. Slice with the `Loyalty Tier` dimension in `metrics_customer_kpis`.",
        "related": ["metrics_customer_kpis", "mv_customer_demo_sales"],
        "sources": ["gold_customer_segment_sales"],
    },
    {
        "name": "Repeat Purchase Rate", "domain": f"{PARENT_TAG}/customer_analytics",
        "synonyms": ["repeat rate", "retention rate"],
        "description": "Share of customers in a segment who placed more than one order.",
        "body": "**Repeat Purchase Rate** = % of customers in the segment with more than one order. Available as `MEASURE(\\`Repeat Purchase Rate\\`)`. Rises with loyalty tier; Platinum is highest.",
        "related": ["metrics_customer_kpis", "mv_customer_demo_sales"],
        "sources": ["gold_customer_segment_sales"],
    },
    {
        "name": "Acquisition Channel", "domain": f"{PARENT_TAG}/customer_analytics",
        "synonyms": ["acquisition source", "how customer joined"],
        "description": "How a customer was acquired: organic_search, paid_search, social_media, referral, or email_campaign.",
        "body": "**Acquisition Channel** captures how a customer joined. Compare revenue-per-customer across channels with `mv_customer_demo_sales` / `metrics_customer_kpis`.",
        "related": ["metrics_customer_kpis", "mv_customer_demo_sales"],
        "sources": ["gold_customer_segment_sales"],
    },
    {
        "name": "Return Rate", "domain": f"{PARENT_TAG}/returns_quality",
        "synonyms": ["return rate", "returns pct", "return percentage"],
        "description": "Returns divided by orders, as a percentage. Normal 3–8%; alert threshold 25%.",
        "body": (
            "**Return Rate** = `(return_count / order_count) × 100`. Normal by category: "
            "Electronics 6–8%, Apparel 5–7%, Books 1–2%, Food 2–3%, others 3–6%. "
            "Alert threshold 25%. FAULT-PHON-* products exceed **40%** in Q4 2025. "
            "Use `MEASURE(\\`Return Rate\\`)` in `metrics_product_kpis`."
        ),
        "related": ["metrics_product_kpis", "gold_return_analysis", "mv_regional_orders"],
        "sources": ["gold_return_analysis"],
    },
    {
        "name": "Faulty Batch (FAULT-PHON-*)", "domain": f"{PARENT_TAG}/returns_quality",
        "synonyms": ["faulty batch", "defective products", "FAULT-PHON", "defective smartphones"],
        "description": "8 defective Smartphone SKUs (prefix FAULT-PHON-*, product_idx 4–11) shipped in Q3 2025 — the root cause of the Q4-2025 return spike.",
        "body": (
            "**Faulty Batch** = 8 Smartphone SKUs with prefix `FAULT-PHON-*` (product_idx 4–11), shipped Q3 2025 "
            "(Jul–Sep). They caused 78 of 120 total returns in Q4 2025, concentrated in APAC-East (REG-005). "
            "Filter `faulty_batch = TRUE` in `gold_return_analysis` / `silver_dim_products`, or "
            "`Faulty Batch = 'Defective (FAULT-PHON-*)'` in `metrics_product_kpis`. "
            "DQX quarantined 41 `faulty_product` reason codes."
        ),
        "related": ["gold_return_analysis", "metrics_product_kpis", "silver_dim_products", "silver_fact_returns"],
        "sources": ["silver_dim_products", "gold_return_analysis"],
    },
    {
        "name": "Q4 2025 Return Spike", "domain": f"{PARENT_TAG}/returns_quality",
        "synonyms": ["return spike", "APAC-East return spike", "return anomaly"],
        "description": "The Oct–Dec 2025 surge of returns in APAC-East driven by the faulty batch; support tickets rose 3×.",
        "body": "**Q4 2025 Return Spike**: 78 of 120 total returns landed in Oct–Dec 2025, concentrated in **APAC-East (REG-005)**, driven by the FAULT-PHON-* faulty batch. Support tickets rose ~3×; product_defect tickets went 10% → 40%. Returns stabilised in Q1 2026 as faulty stock cleared.",
        "related": ["gold_regional_performance", "mv_regional_orders", "gold_return_analysis"],
        "sources": ["gold_regional_performance"],
    },
    {
        "name": "DQ Quarantine", "domain": f"{PARENT_TAG}/returns_quality",
        "synonyms": ["data quality quarantine", "quarantine table", "bad records"],
        "description": "The silver_dq_quarantine MV capturing 139 records that failed critical data-quality checks.",
        "body": (
            "**DQ Quarantine** (`silver_dq_quarantine`) captures records failing critical checks: "
            "~52 NULL invoice totals (dropped), 3 duplicate customer emails, ~43 failed payments, and "
            "41 faulty_product return flags — **139** total. A non-zero row count is a data-health alert. "
            "It is the source of truth for the demo's data-quality questions."
        ),
        "related": ["silver_dq_quarantine"],
        "sources": ["silver_dq_quarantine"],
    },
    {
        "name": "PII Masking & GDPR", "domain": PARENT_TAG,
        "synonyms": ["PII", "column mask", "data masking", "GDPR"],
        "description": "UC column masks on silver_dim_customers so only the catalog owner sees raw PII.",
        "body": (
            "**PII Masking**: `silver_dim_customers` applies UC column-mask functions — `full_name` → first initial, "
            "`email` → ***@domain, `phone` → ***-***-XXXX, `date_of_birth` → year only. Only the catalog owner sees "
            "raw values (GDPR data-minimisation, Art. 4(1)). Unmasked PII exists only in the bronze layer, which is "
            "owner-restricted."
        ),
        "related": ["silver_dim_customers"],
        "sources": ["silver_dim_customers"],
    },
    {
        "name": "Region & Country Hierarchy", "domain": PARENT_TAG,
        "synonyms": ["regions", "geography", "super region", "region hierarchy"],
        "description": "7 sales regions → 65 countries, grouped into 3 super-regions (Americas, EMEA, Asia Pacific).",
        "body": (
            "**Region Hierarchy**: REG-001 AMER-North · REG-002 AMER-South · REG-003 EMEA-West · "
            "REG-004 EMEA-East · REG-005 **APAC-East** (the faulty-batch return spike) · REG-006 APAC-South · "
            "REG-007 MENA. Super-regions: Americas | EMEA | Asia Pacific. Join via `silver_dim_geography`."
        ),
        "related": ["silver_dim_geography", "mv_regional_orders"],
        "sources": ["silver_dim_geography"],
    },
]

# COMMAND ----------

# ── Helpers ───────────────────────────────────────────────────────────────────

def _http_post(path: str, body: dict, timeout: int = 30):
    """Direct POST with a hard timeout, bypassing the SDK's long retry-on-429.
    Returns (ok: bool, json_or_text). Domain creation can legitimately return
    429 (account domain quota) — we must see that immediately, not retry for 2m."""
    headers = w.config.authenticate() or {}
    headers["Content-Type"] = "application/json"
    resp = requests.post(w.config.host.rstrip("/") + path,
                         headers=headers, data=json.dumps(body), timeout=timeout)
    try:
        payload = resp.json()
    except Exception:
        payload = {"message": resp.text}
    return (resp.status_code < 300, resp.status_code, payload)


def run_sql(stmt: str):
    """Execute a read SQL statement and return rows as a list of lists."""
    r = w.statement_execution.execute_statement(
        warehouse_id=WAREHOUSE_ID, statement=stmt.strip(), wait_timeout="30s")
    sid = r.statement_id
    for _ in range(30):
        st = r.status.state
        if st in (StatementState.SUCCEEDED, StatementState.CLOSED):
            return (r.result.data_array or []) if r.result else []
        if st in (StatementState.FAILED, StatementState.CANCELED):
            raise RuntimeError(r.status.error.message if r.status.error else "query failed")
        time.sleep(2)
        r = w.statement_execution.get_statement(sid)
    raise TimeoutError("query timed out")


def ensure_governed_tag(tag_key: str, description: str):
    """Register a governed tag (Tag Policy API). Idempotent."""
    ok, code, payload = _http_post("/api/2.1/tag-policies",
                                   {"tag_key": tag_key, "description": description})
    if ok:
        print(f"  ✓  governed tag {tag_key}")
    elif code == 409 or "ALREADY_EXISTS" in str(payload):
        print(f"  ✓  governed tag {tag_key} (exists)")
    else:
        print(f"  ⚠  governed tag {tag_key}  [{code}] {str(payload.get('message',''))[:120]}")


def list_domains_by_tag() -> dict:
    """Return {tag_key: domain_id} across all existing domains (paginated)."""
    out, token = {}, None
    while True:
        q = "/api/2.0/domains?page_size=200" + (f"&page_token={token}" if token else "")
        r = w.api_client.do("GET", q)
        for d in r.get("domains", []):
            out[d.get("tag_key")] = d.get("domain_id")
        token = r.get("next_page_token")
        if not token:
            break
    return out


def create_domain(body: dict, label: str, existing: dict):
    """Create a domain if one with this tag_key doesn't already exist.
    Returns domain_id (existing or new), or None if creation was blocked."""
    tag = body["tag_key"]
    if tag in existing:
        print(f"  ✓  domain '{label}' exists ({existing[tag]})")
        return existing[tag]
    ok, code, payload = _http_post("/api/2.0/domains", body)
    if ok:
        did = payload.get("domain_id")
        print(f"  ✓  created domain '{label}' ({did})")
        existing[tag] = did
        return did
    msg = str(payload.get("message", payload))
    if code == 429 or "RESOURCE_EXHAUSTED" in msg or "maximum allowed number of domains" in msg:
        print(f"  ⚠  domain '{label}' NOT created — account domain quota reached (300).")
        print(f"      An account admin must free domain slots or raise the cap, then re-run.")
    else:
        print(f"  ✗  domain '{label}'  [{code}] {msg[:180]}")
    return None

# COMMAND ----------

# ── 1. Governed tags ──────────────────────────────────────────────────────────
print("1. Registering governed tags for domains...")
ensure_governed_tag(PARENT_TAG, PARENT_DOMAIN["description"][:255])
for s in SUBDOMAINS:
    ensure_governed_tag(s["tag_key"], f"Online Retail subdomain: {s['title']}.")

# ── 2. Create parent domain + subdomains ──────────────────────────────────────
print("\n2. Creating Discover domains...")
existing = list_domains_by_tag()
parent_id = create_domain(
    {"tag_key": PARENT_TAG, "subtitle": PARENT_DOMAIN["subtitle"],
     "description": PARENT_DOMAIN["description"], "icon": PARENT_DOMAIN["icon"]},
    "Online Retail", existing)

if parent_id:
    for s in SUBDOMAINS:
        create_domain(
            {"tag_key": s["tag_key"], "parent_domain_id": parent_id,
             "subtitle": s["subtitle"], "description": s["description"], "icon": s["icon"]},
            s["title"], existing)
else:
    print("  ↷  Skipping subdomains (no parent). Governed tags + Pages file still produced.")

# COMMAND ----------

# ── 3. Read domain membership + comments that run_governance.py saved ─────────
print("\n3. Reading governed domain tags + table comments from Unity Catalog...")

domain_tags = [PARENT_TAG] + [s["tag_key"] for s in SUBDOMAINS]
tag_list_sql = ", ".join(f"'{t}'" for t in domain_tags)

# table -> set(domain tags), from the governed tags run_governance.py applied
membership = {}
try:
    rows = run_sql(f"""
        SELECT schema_name, table_name, tag_name
        FROM {CATALOG}.information_schema.table_tags
        WHERE schema_name LIKE 'online_retail_%' AND tag_name IN ({tag_list_sql})
    """)
    for schema, table, tag in rows:
        membership.setdefault(table, set()).add(tag)
    print(f"  ✓  {len(membership)} tables carry a domain tag")
except Exception as e:
    print(f"  ⚠  could not read table_tags: {str(e)[:140]}")

# table -> comment (the definition each per-table Page leads with)
comments = {}
try:
    rows = run_sql(f"""
        SELECT table_schema, table_name, comment
        FROM {CATALOG}.information_schema.tables
        WHERE table_schema LIKE 'online_retail_%' AND comment IS NOT NULL
    """)
    for schema, table, comment in rows:
        # skip gold MV internal materialization aliases
        if table.startswith("__materialization"):
            continue
        comments[table] = comment
    print(f"  ✓  {len(comments)} table comments loaded")
except Exception as e:
    print(f"  ⚠  could not read comments: {str(e)[:140]}")

# COMMAND ----------

# ── 4. Build the Pages bulk-import Markdown ───────────────────────────────────
print("\n4. Generating Pages bulk-import file...")


def _fqn(short: str) -> str:
    """Best-effort catalog.schema.table for a short table name, for @-tagging."""
    prefix = short.split("_", 1)[0]  # bronze/silver/gold/mv/metrics
    schema = {
        "bronze": "online_retail_bronze", "silver": "online_retail_silver",
        "gold": "online_retail_gold", "mv": "online_retail_metrics",
        "metrics": "online_retail_metrics",
    }.get(prefix, "online_retail_gold")
    return f"{CATALOG}.{schema}.{short}"


def _page_md(name, domain_tag, synonyms, description, body, related, sources) -> str:
    dom = TAG_TO_TITLE.get(domain_tag, domain_tag)
    lines = [
        f"### Page: {name}", "",
        f"- **Domain:** {dom}",
        f"- **Owner:** {OWNER_USER}",
        f"- **Synonyms:** {', '.join(synonyms) if synonyms else '—'}",
        f"- **Description:** {description}", "",
        "**Page body:**", "", body, "",
    ]
    if related:
        lines.append("**Related assets:** " + ", ".join(f"`{_fqn(r)}`" for r in related))
    if sources:
        lines.append("**Sources:** " + ", ".join(f"`{_fqn(s)}`" for s in sources))
    lines.append("")
    return "\n".join(lines)


md = [
    "# NexusRetail — Genie Ontology Pages (bulk import)",
    "",
    "Import into **Discover ▸ Pages ▸ Create page ▸ Genie Code ▸ Bulk import pages**.",
    "Attach this file as a source; Genie Code drafts one Page per section below.",
    "Review, then Publish. Definitions are generated from the governed comments and",
    "domain tags applied by `governance/run_governance.py`.",
    "",
    f"_Catalog:_ `{CATALOG}`  ·  _Owner:_ {OWNER_USER}  ·  _Domains:_ "
    + ", ".join(TAG_TO_TITLE.values()),
    "",
    "---", "",
    "## Concept Pages", "",
]

for p in CURATED_PAGES:
    md.append(_page_md(p["name"], p["domain"], p.get("synonyms", []),
                       p["description"], p["body"], p.get("related", []), p.get("sources", [])))

# Per-table Pages — one per online_retail table, grouped by domain, using the
# comment run_governance.py saved as the definition.
md += ["---", "", "## Table Pages (auto-generated from governed comments)", ""]

# order tables by domain then name for a readable document
def _primary_domain(table):
    tags = membership.get(table, set())
    for s in SUBDOMAINS:                       # prefer a specific subdomain
        if s["tag_key"] in tags:
            return s["tag_key"]
    return PARENT_TAG if PARENT_TAG in tags else "online_retail"

by_domain = {}
for table, comment in comments.items():
    by_domain.setdefault(_primary_domain(table), []).append((table, comment))

for tag in [f"{PARENT_TAG}/sales_performance", f"{PARENT_TAG}/customer_analytics",
            f"{PARENT_TAG}/returns_quality", PARENT_TAG, "online_retail"]:
    tables = sorted(by_domain.pop(tag, []))
    if not tables:
        continue
    md += [f"### {TAG_TO_TITLE.get(tag, 'Online Retail')} — tables", ""]
    for table, comment in tables:
        md.append(_page_md(
            name=table, domain_tag=tag if tag != "online_retail" else PARENT_TAG,
            synonyms=[], description=comment.split(". ")[0][:200],
            body=comment, related=[table], sources=[table]))

pages_md = "\n".join(md)
total_pages = len(CURATED_PAGES) + sum(len(v) for v in ({**{}, **by_domain}).values()) + len(comments)
print(f"  ✓  built {len(CURATED_PAGES)} concept pages + {len(comments)} table pages")

# COMMAND ----------

# ── 5. Write the artifact to a UC Volume (and print it) ───────────────────────
print("\n5. Saving Pages file to a Unity Catalog Volume...")
VOL_SCHEMA = f"{CATALOG}.online_retail_metrics"
VOL_NAME = "discover_ontology"
VOL_PATH = f"/Volumes/{CATALOG}/online_retail_metrics/{VOL_NAME}"
OUT_FILE = f"{VOL_PATH}/nexus_retail_pages.md"

saved = False
try:
    run_sql(f"CREATE VOLUME IF NOT EXISTS {VOL_SCHEMA}.{VOL_NAME} "
            f"COMMENT 'Generated Genie Ontology Pages for Discover bulk import.'")
    w.files.upload(OUT_FILE, io.BytesIO(pages_md.encode("utf-8")), overwrite=True)
    print(f"  ✓  wrote {OUT_FILE}")
    saved = True
except Exception as e:
    print(f"  ⚠  could not write to volume: {str(e)[:160]}")
    print("      The full Pages Markdown is printed below — copy it into a .md file.")

print(f"\n{'='*64}")
print("Domains + Pages setup complete.")
print(f"  Domains  : parent 'Online Retail' + {len(SUBDOMAINS)} subdomains")
print(f"  Pages    : {len(CURATED_PAGES)} concept + {len(comments)} table (bulk-import file)")
if saved:
    print(f"  File     : {OUT_FILE}")
print(f"  Next     : Discover ▸ Pages ▸ Create page ▸ Genie Code ▸ Bulk import pages")
print(f"             attach the file above, review the drafts, and Publish.")
print(f"{'='*64}")

# COMMAND ----------

# Print the generated Pages Markdown so it's always retrievable from the run log.
print(pages_md)

# COMMAND ----------

if __name__ == "__main__":
    pass
