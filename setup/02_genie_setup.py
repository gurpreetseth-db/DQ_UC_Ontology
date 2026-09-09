# Databricks notebook source
# NexusRetail Analytics — Genie One Space Setup
# Creates the "NexusRetail Analytics" Genie space with:
#   - Domain: Online Retail Analytics
#   - Data sources: all gold + metrics tables
#   - Knowledge snippets: 8 semantic definitions
#   - Pages: Sales Performance, Customer Analytics, Returns & Quality
#
# Run AFTER:
#   1. Pipeline has completed (gold + metrics tables exist)
#   2. governance/run_governance.py has been executed (for mask functions + tags)
#
# Requires: databricks-sdk >= 0.20

# COMMAND ----------

import json, time
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

# catalog injected from DAB job base_parameters (set in databricks.local.yml)
dbutils.widgets.text("catalog", "your_catalog_name")
CATALOG = dbutils.widgets.get("catalog")

WAREHOUSE_ID  = "9d8a677b3c55b8a7"          # override if warehouse changed
SPACE_TITLE   = "NexusRetail Analytics"
DOMAIN_NAME   = "Online Retail Analytics"

print(f"▶ Genie One Setup — NexusRetail Analytics")
print(f"  Catalog  : {CATALOG}")
print(f"  Space    : {SPACE_TITLE}")
print(f"  Domain   : {DOMAIN_NAME}")

# ── Helper ────────────────────────────────────────────────────────────────────

def api(method: str, path: str, body: dict = None, silent: bool = False):
    """Thin wrapper around the raw Databricks REST API."""
    host  = spark.conf.get("spark.databricks.workspaceUrl")
    token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
    import urllib.request, urllib.error
    url     = f"https://{host}{path}"
    data    = json.dumps(body).encode() if body else None
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req) as resp:
            return json.loads(resp.read())
    except urllib.error.HTTPError as e:
        err = e.read().decode()
        if not silent:
            print(f"  ⚠ {method} {path}: {e.code} {err[:200]}")
        return None


# =============================================================================
# SECTION 1 — DATA SOURCES
# All gold + metrics tables wired as Genie data sources
# =============================================================================

DATA_SOURCES = [
    f"{CATALOG}.online_retail_metrics.mv_category_revenue",
    f"{CATALOG}.online_retail_metrics.mv_customer_demo_sales",
    f"{CATALOG}.online_retail_metrics.mv_regional_orders",
    f"{CATALOG}.online_retail_metrics.metrics_sales_kpis",
    f"{CATALOG}.online_retail_metrics.metrics_customer_kpis",
    f"{CATALOG}.online_retail_metrics.metrics_product_kpis",
    f"{CATALOG}.online_retail_gold.gold_daily_revenue",
    f"{CATALOG}.online_retail_gold.gold_return_analysis",
    f"{CATALOG}.online_retail_gold.gold_customer_lifetime_value",
    f"{CATALOG}.online_retail_gold.gold_category_sales",
    f"{CATALOG}.online_retail_gold.gold_regional_performance",
    f"{CATALOG}.online_retail_gold.gold_customer_segment_sales",
]

# =============================================================================
# SECTION 2 — KNOWLEDGE SNIPPETS
# Pre-wires Genie with domain semantics so NL questions land correctly
# =============================================================================

KNOWLEDGE_SNIPPETS = [
    {
        "title": "Data Model Overview",
        "content": f"""
NexusRetail is a global e-commerce platform. All data lives in catalog: {CATALOG}.

Key schemas:
- online_retail_metrics: semantic layer — USE THESE for Genie queries (mv_*, metrics_*)
- online_retail_gold: aggregated facts — use when metrics layer doesn't have what you need
- online_retail_silver: cleaned source data (not recommended for ad-hoc Genie queries)

Table hierarchy:
- metrics_sales_kpis      → daily revenue grain, slice by channel/region/month
- metrics_customer_kpis   → customer segment grain, slice by age/income/loyalty/region
- metrics_product_kpis    → product return grain, slice by category/reason/faulty_batch
- mv_category_revenue     → pre-aggregated category revenue by month
- mv_customer_demo_sales  → pre-aggregated demographic sales by month
- mv_regional_orders      → pre-aggregated regional orders and returns by month
- gold_customer_lifetime_value → one row per customer with CLV segment
- gold_return_analysis    → one row per product × return_reason × week
""".strip(),
    },
    {
        "title": "Revenue Definition",
        "content": """
GROSS REVENUE = SUM(gross_revenue) from delivered/shipped/confirmed orders.
NET REVENUE   = gross_revenue minus refund_total (deducts refunds for returns).
Discounts are already applied in gross_revenue (line_total = qty * price * (1 - discount)).

For Metric Views use: MEASURE(`Gross Revenue`) or MEASURE(`Order Count`)
For gold tables use: SUM(gross_revenue), SUM(order_count)

Fiscal year = calendar year. Q4 = October through December (seasonal peak: +40-50% volume).
All monetary values are in USD unless stated otherwise.
Currency codes are for display/reporting only — all amounts stored in USD.
""".strip(),
    },
    {
        "title": "Return Rate and Faulty Batch Story",
        "content": """
RETURN RATE = (return_count / order_count) * 100.

Normal return rates by category:
  Electronics : 6-8%  (normal) → spikes to >40% for FAULT-PHON-* in Q4 2025
  Apparel     : 5-7%
  Books       : 1-2%
  Food        : 2-3%
  All others  : 3-6%

THE FAULTY BATCH (key story):
  - 8 Smartphone SKUs with prefix FAULT-PHON-* (product_idx 4-11) were shipped Q3 2025 (Jul-Sep)
  - They caused 78 of 120 total returns in Q4 2025 (Oct-Dec)
  - Concentrated in APAC-East region (REG-005)
  - Filter: faulty_batch = TRUE in gold_return_analysis or silver_dim_products
  - DQX quarantine captured 41 'faulty_product' reason codes in silver_dq_quarantine

To investigate: filter on faulty_batch='Defective (FAULT-PHON-*)' in metrics_product_kpis
or return_reason_code='faulty_product' in gold_return_analysis.
""".strip(),
    },
    {
        "title": "Customer Segments and CLV",
        "content": """
LOYALTY TIERS: Bronze (40% of customers) | Silver (30%) | Gold (20%) | Platinum (10%)
  - Higher tiers = higher avg order value and repeat rate
  - Platinum customers have highest CLV and lowest churn

AGE BRACKETS: 18-24 | 25-34 | 35-44 | 45-54 | 55+
  - 25-34 is the largest segment (30% of customers)

INCOME BRACKETS: <$30K | $30K-$60K | $60K-$100K | $100K-$200K | $200K+

CLV SEGMENTS (gold_customer_lifetime_value.clv_segment):
  - High    : total_revenue >= $1,000 OR loyalty_tier = Platinum
  - Medium  : total_revenue $200–$999
  - Low     : total_revenue < $200
  - Churned : no order in the last 180 days

ACQUISITION CHANNELS: organic_search | paid_search | social_media | referral | email_campaign

REPEAT PURCHASE RATE = % of customers in the segment who placed more than 1 order.
""".strip(),
    },
    {
        "title": "Regions and Countries",
        "content": """
7 global regions with region IDs:
  REG-001: AMER-North (US, Canada, Mexico) — 25% of customers
  REG-002: AMER-South (Brazil, Argentina, Colombia, Chile, Peru) — 8%
  REG-003: EMEA-West  (UK, Germany, France, Spain, Italy, Netherlands, Sweden...) — 22%
  REG-004: EMEA-East  (Poland, Czech Republic, Romania, Hungary, Greece, Ukraine) — 6%
  REG-005: APAC-East  (Japan, China, South Korea, Australia, Taiwan, Hong Kong) — 20%
             ↑ This is the region with the Q4 2025 faulty batch return spike
  REG-006: APAC-South (India, Singapore, Malaysia, Thailand, Indonesia, Philippines) — 15%
  REG-007: MENA       (Saudi Arabia, UAE, Egypt, Turkey, Nigeria, South Africa, Kenya) — 4%

Super Region groupings: Americas | EMEA | Asia Pacific

When asked about regions by name, map to region_id or region_name column.
""".strip(),
    },
    {
        "title": "Date and Time Context",
        "content": """
Dataset spans 24 months: September 2024 through September 2026.

Key date columns:
  sale_month   : DATE_TRUNC('MONTH', sale_date) — use for monthly trends
  sale_quarter : DATE_TRUNC('QUARTER', sale_date) — use for QoQ analysis
  return_week  : DATE_TRUNC('WEEK', return_date) — use for weekly return trends

Key dates for the NexusRetail story:
  Q3 2025 (Jul–Sep 2025) : Faulty FAULT-PHON-* products shipped to customers
  Q4 2025 (Oct–Dec 2025) : Return spike begins; support tickets increase 3x
  Q1 2026 (Jan–Mar 2026) : Returns stabilise as faulty stock cleared

Q4 (Oct–Dec) is always the peak sales period — expect 40-50% higher order volumes year-over-year.
When comparing periods, use DATE_TRUNC('QUARTER', ...) for cleaner groupings.
""".strip(),
    },
    {
        "title": "Metric View Query Syntax",
        "content": f"""
Metric views require the MEASURE() function. SELECT * is NOT supported.
Dimension names with spaces must be backtick-quoted.

EXAMPLE — Revenue by region and month:
  SELECT `Sale Month`, `Region`, MEASURE(`Gross Revenue`) AS revenue
  FROM {CATALOG}.online_retail_metrics.metrics_sales_kpis
  WHERE YEAR(`Sale Month`) = 2025
  GROUP BY ALL ORDER BY ALL;

EXAMPLE — Return rate by product type:
  SELECT `Category`, `Faulty Batch`, MEASURE(`Return Rate`) AS return_rate
  FROM {CATALOG}.online_retail_metrics.metrics_product_kpis
  GROUP BY ALL ORDER BY ALL;

EXAMPLE — Revenue per customer by loyalty tier:
  SELECT `Loyalty Tier`, MEASURE(`Revenue per Customer`) AS rev_per_cust
  FROM {CATALOG}.online_retail_metrics.metrics_customer_kpis
  GROUP BY ALL ORDER BY ALL;

For mv_* tables and gold_* tables, use standard SQL aggregations (no MEASURE()).
""".strip(),
    },
    {
        "title": "Data Quality Context",
        "content": f"""
Three intentional data quality issues are embedded in the dataset for demo purposes:

1. NULL invoice totals (~52 records):
   - bronze_invoices has ~52 rows with invoice_total = NULL
   - These are DROPPED at silver_fact_invoices by the SDP expect_or_drop constraint
   - They appear in {CATALOG}.online_retail_silver.silver_dq_quarantine (rule: invoice_total_null)

2. Duplicate customer emails (3 records):
   - 3 customers share email 'duplicate.test@example.com'
   - Flagged in silver_dq_quarantine (rule: duplicate_email)
   - These customers ARE in silver_dim_customers (warn only, not dropped)

3. Failed payments (~43 records):
   - bronze_payments has ~43 rows with payment_status = 'failed' (~3%)
   - Flagged in silver_dq_quarantine (rule: failed_payment)

Total quarantined: 139 records in silver_dq_quarantine.
When asked about data quality, reference this table as the source of truth.
""".strip(),
    },
]

# =============================================================================
# SECTION 3 — PAGES (sample questions per topic area)
# =============================================================================

PAGES = [
    {
        "title": "📊 Sales Performance",
        "description": "Revenue trends, channel analysis, and seasonal patterns.",
        "questions": [
            "What were the top 5 product categories by gross revenue in Q4 2025?",
            "Show me monthly revenue trend for all regions in 2025",
            "Which sales channel — web, mobile, or partner API — drives the highest average order value?",
            "Compare Q4 2025 vs Q4 2024 revenue by region. Which region grew the most?",
            "What percentage of revenue comes from returning customers vs new customers?",
            "Show me the weekly revenue trend for APAC-East in Q3 and Q4 2025",
        ],
        "tables": [
            f"{CATALOG}.online_retail_metrics.metrics_sales_kpis",
            f"{CATALOG}.online_retail_metrics.mv_category_revenue",
            f"{CATALOG}.online_retail_gold.gold_daily_revenue",
        ],
    },
    {
        "title": "👥 Customer Analytics",
        "description": "Customer segment performance, CLV analysis, and demographic insights.",
        "questions": [
            "Which income bracket has the highest average customer lifetime value?",
            "What is the repeat purchase rate for Platinum vs Bronze loyalty tier customers?",
            "Which age group generates the most revenue? Break down by region.",
            "Show me the top 3 acquisition channels by revenue per customer",
            "How many customers fall into each CLV segment (High, Medium, Low, Churned)?",
            "Which customer segment has the highest revenue per customer in APAC-East?",
        ],
        "tables": [
            f"{CATALOG}.online_retail_metrics.metrics_customer_kpis",
            f"{CATALOG}.online_retail_metrics.mv_customer_demo_sales",
            f"{CATALOG}.online_retail_gold.gold_customer_lifetime_value",
        ],
    },
    {
        "title": "↩️ Returns & Quality",
        "description": "Return rate analysis, faulty batch investigation, and regional quality signals.",
        "questions": [
            "Why is the Electronics return rate elevated in Q4 2025?",
            "Show return rate by product SKU — which products have a return rate above 25%?",
            "Which region has the highest return rate in Q4 2025?",
            "How many faulty batch (FAULT-PHON-*) products were returned vs normal products?",
            "What is the trend of faulty_product return reasons week-by-week from Q3 2025?",
            "Show me the data quality quarantine summary — how many records failed each rule?",
        ],
        "tables": [
            f"{CATALOG}.online_retail_metrics.metrics_product_kpis",
            f"{CATALOG}.online_retail_metrics.mv_regional_orders",
            f"{CATALOG}.online_retail_gold.gold_return_analysis",
        ],
    },
]

# =============================================================================
# SECTION 4 — CREATE GENIE SPACE
# =============================================================================
print("\n── Creating Genie Space ─────────────────────────────────")

space_result = api("POST", "/api/2.0/genie/spaces", {
    "display_name": SPACE_TITLE,
    "description":  (
        f"NexusRetail Analytics — conversational analytics for a global e-commerce platform. "
        f"Covers sales performance, customer demographics, product returns, and data quality. "
        f"24 months of data (Sep 2024 – Sep 2026) with embedded quality story: "
        f"8 defective FAULT-PHON-* products drove a Q4 2025 return spike. "
        f"All data in catalog: {CATALOG}."
    ),
    "warehouse_id":      WAREHOUSE_ID,
    "table_identifiers": DATA_SOURCES,
})

if space_result and "space_id" in space_result:
    SPACE_ID = space_result["space_id"]
    print(f"  ✓ Space created: {SPACE_ID}")
elif space_result and "id" in space_result:
    SPACE_ID = space_result["id"]
    print(f"  ✓ Space created: {SPACE_ID}")
else:
    print("  ⚠ Space creation via API not available — note the space ID after manual creation")
    SPACE_ID = None

# =============================================================================
# SECTION 5 — ADD KNOWLEDGE SNIPPETS
# =============================================================================
if SPACE_ID:
    print("\n── Adding Knowledge Snippets ────────────────────────────")
    for snip in KNOWLEDGE_SNIPPETS:
        r = api("POST", f"/api/2.0/genie/spaces/{SPACE_ID}/knowledge-snippets", {
            "title":   snip["title"],
            "content": snip["content"],
        })
        status = "✓" if r else "⚠"
        print(f"  {status}  {snip['title']}")

# =============================================================================
# SECTION 6 — CREATE DOMAIN AND LINK SPACE
# =============================================================================
print("\n── Creating Domain ──────────────────────────────────────")

domain_result = api("POST", "/api/2.0/genie/domains", {
    "display_name": DOMAIN_NAME,
    "description":  "Online retail analytics domain covering sales, customers, and returns for NexusRetail demo.",
}, silent=True)

if domain_result:
    DOMAIN_ID = domain_result.get("domain_id") or domain_result.get("id")
    print(f"  ✓ Domain created: {DOMAIN_NAME} ({DOMAIN_ID})")

    # Link the space to the domain
    if SPACE_ID and DOMAIN_ID:
        r = api("POST", f"/api/2.0/genie/domains/{DOMAIN_ID}/spaces", {"space_id": SPACE_ID}, silent=True)
        if r:
            print(f"  ✓ Space linked to domain")
        else:
            print(f"  ⚠ Domain-space linking not available via API — link manually in Genie UI")
else:
    print("  ⚠ Domain API not available — add domain manually in Genie UI: Settings → Domains")

# =============================================================================
# SECTION 7 — CREATE PAGES (CONVERSATIONS)
# =============================================================================
if SPACE_ID:
    print("\n── Creating Pages ───────────────────────────────────────")
    for page in PAGES:
        # In Genie, pages are created by starting a conversation with a title
        conv = api("POST", f"/api/2.0/genie/spaces/{SPACE_ID}/conversations", {
            "title": page["title"],
        }, silent=True)

        if not conv:
            # Fallback: some workspaces use a different endpoint
            conv = api("POST", f"/api/2.0/genie/spaces/{SPACE_ID}/start-conversation", {
                "content": page["questions"][0],
            }, silent=True)

        conv_id = (conv or {}).get("conversation_id") or (conv or {}).get("id")
        if conv_id:
            print(f"  ✓ Page created: {page['title']}")
            # Seed the first question into the conversation
            api("POST", f"/api/2.0/genie/spaces/{SPACE_ID}/conversations/{conv_id}/messages", {
                "content": page["questions"][0],
            }, silent=True)
        else:
            print(f"  ⚠ Page: {page['title']} — create manually (see questions below)")

    print()

# =============================================================================
# SECTION 8 — MANUAL SETUP GUIDE (fallback / reference)
# =============================================================================
print("""
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
GENIE SETUP REFERENCE GUIDE
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

WORKSPACE → Genie → New Space: "NexusRetail Analytics"
""")

print("DATA SOURCES (add all of these):")
for ds in DATA_SOURCES:
    print(f"  ✦ {ds}")

print("\nDOMAIN: Create 'Online Retail Analytics' in Genie Settings → Domains")
print("        Then assign this space to that domain.")

print("\nPAGES (create one conversation per page):")
for page in PAGES:
    print(f"\n  📌 {page['title']}")
    print(f"     {page['description']}")
    for q in page["questions"]:
        print(f"     → \"{q}\"")

print("\nKNOWLEDGE SNIPPETS (add all 8):")
for snip in KNOWLEDGE_SNIPPETS:
    print(f"  • {snip['title']}")

print("\nINSTRUCTIONS (Space Settings → Instructions):")
print("""
  You are an analytics assistant for NexusRetail, a global e-commerce platform.
  Always prefer metrics from the online_retail_metrics schema (mv_* and metrics_* tables).
  When asked about returns, check whether the query relates to the Q3 2025 faulty batch (FAULT-PHON-* SKUs).
  For metric views, always use the MEASURE() function and GROUP BY ALL.
  Revenue figures are in USD. Return rate = returns / orders * 100.
  Q4 (Oct-Dec) is the seasonal peak — expect ~40% higher volumes than non-Q4 months.
""")
print("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
