# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# NexusRetail Analytics — Genie One Space Setup
# Creates the "Online Retail" Genie domain, 3 pages, knowledge snippets,
# and wires the metric views as data sources.
#
# Run AFTER:
#   1. Pipeline has run (gold + metrics tables exist)
#   2. governance/03_metric_views.sql has been executed
#
# API: Genie Conversations API (Public Preview)
# SDK : databricks-sdk >= 0.20

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import dashboards
import json

w = WorkspaceClient()
dbutils.widgets.text("catalog", "your_catalog_name")
CATALOG = dbutils.widgets.get("catalog")

# =============================================================================
# SECTION 1 — Create Genie Space (Online Retail)
# =============================================================================
# The Genie Spaces API creates spaces that appear in the Genie One UI.
# Spaces reference tables/views as data sources and accept knowledge snippets.

SPACE_TITLE       = "NexusRetail Analytics"
SPACE_DESCRIPTION = """
Conversational analytics for NexusRetail — a global e-commerce platform selling
173 products across 12 categories, 7 regions, and 65 countries. Ask natural language
questions about sales performance, customer behaviour, and product returns.

Data covers 24 months (Sep 2024 – Sep 2026), with a known Q3 2025 smartphone defect
batch (FAULT-PHON-* SKUs) that caused a Q4 2025 return spike in APAC-East.

This space is backed by the online_retail_metrics semantic layer and the published
Discover Ontology Pages (a governed business glossary). Prefer the metric views and
cite the glossary definitions when answering.
"""

# Data sources for this Genie space (tables from gold + metrics schema)
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
# SECTION 2 — Knowledge Snippets
# =============================================================================
# These teach Genie the domain-specific semantics so NL queries are accurate.

KNOWLEDGE_SNIPPETS = [
    {
        "title": "Data Model Overview",
        "content": """
NexusRetail operates across 7 regions (AMER-North, AMER-South, EMEA-West, EMEA-East,
APAC-East, APAC-South, MENA) and 65 countries. 173 products span 12 categories and
50 subcategories. 600 customers (~500 B2C, ~100 B2B).

All data lives in catalog: {CATALOG}.

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

IMPORTANT — how to query each:
  - Gross Revenue IS a metric-view measure: MEASURE(`Gross Revenue`) in metrics_sales_kpis.
  - Net Revenue is NOT a measure. It is a stored COLUMN: SUM(net_revenue) in
    mv_regional_orders / mv_category_revenue (do not call MEASURE on it).
  - Order Count: MEASURE(`Order Count`) in metrics_sales_kpis / metrics_customer_kpis.
  - metrics_sales_kpis has NO Category dimension (its grain is day x channel x region).
    For revenue by product category, use mv_category_revenue (SUM(gross_revenue)).
For gold tables use: SUM(gross_revenue), SUM(order_count)

Fiscal year = calendar year. Q4 = October through December (seasonal peak: +40-50% volume).
All monetary values are in USD unless stated otherwise.
Currency codes are for display/reporting only — all amounts stored in USD.
""".strip(),
    },
    {
        "title": "Return Rate and Faulty Batch Story",
        "content": """
return_rate_pct = (return_count / order_count) * 100

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

To investigate: in metrics_product_kpis filter the `Faulty Batch` dimension to
'Faulty Batch (FAULT-*)' (that is the exact dimension value; 'Normal Product' is the
other value), or filter return_reason_code='faulty_product' in gold_return_analysis.
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

CLV segments (gold_customer_lifetime_value.clv_segment):
- High: total_revenue >= $1,000 OR loyalty_tier = Platinum
- Medium: total_revenue $200-$999
- Low: total_revenue < $200
- Churned: no order in the last 180 days

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
    {
        "title": "Semantic Layer Cheat Sheet (term → measure → table)",
        "content": """
Map the business term to the exact measure/column and table. This mirrors the
published Discover Ontology Pages — use it to pick the right query surface.

SALES PERFORMANCE
  Gross Revenue          → MEASURE(`Gross Revenue`)            metrics_sales_kpis
  Net Revenue            → SUM(net_revenue) [COLUMN]           mv_regional_orders / mv_category_revenue
  Average Order Value    → MEASURE(`Avg Order Value`)          metrics_sales_kpis / metrics_customer_kpis
  Order Count            → MEASURE(`Order Count`)              metrics_sales_kpis
  Units Sold             → SUM(units_sold) [COLUMN]            mv_category_revenue
  Total Discount         → SUM(total_discount) [COLUMN]        mv_category_revenue
  Sales Channel          → dimension `Channel` (web|mobile|partner_api)   metrics_sales_kpis
  New / Returning Cust.  → MEASURE(`New Customer Count`) / MEASURE(`Returning Customer Count`)  metrics_sales_kpis
  Revenue per Customer   → MEASURE(`Revenue per Customer`)     metrics_sales_kpis / metrics_customer_kpis

CUSTOMER ANALYTICS
  Customer Lifetime Value→ total_revenue, clv_segment          gold_customer_lifetime_value (1 row/customer)
  CLV Segment            → clv_segment (High|Medium|Low|Churned)  gold_customer_lifetime_value
  Loyalty Tier           → dimension `Loyalty Tier`            metrics_customer_kpis
  Repeat Purchase Rate   → MEASURE(`Repeat Purchase Rate`)     metrics_customer_kpis
  Acquisition Channel    → dimension `Acquisition Channel`     metrics_customer_kpis
  Customer Type          → dimension `Customer Type` (B2C|B2B) metrics_customer_kpis
  Age / Income Bracket   → dimensions `Age Bracket` / `Income Bracket`  metrics_customer_kpis

RETURNS & QUALITY
  Return Rate            → MEASURE(`Return Rate`)              metrics_product_kpis (also return_rate_pct in mv_regional_orders)
  Faulty Batch           → dimension `Faulty Batch` ('Faulty Batch (FAULT-*)' | 'Normal Product')  metrics_product_kpis
  Return Reason          → dimension `Return Reason`           metrics_product_kpis
  Total Refund Amount    → MEASURE(`Total Refund Amount`)      metrics_product_kpis
  Avg Days to Return     → MEASURE(`Avg Days to Return`)       metrics_product_kpis
  Cancellation Rate      → cancellation_rate_pct [COLUMN]      mv_regional_orders
  DQ Quarantine          → group silver_dq_quarantine by (source_table, dq_rule, severity)

Rule of thumb: measures need MEASURE(); stored columns use SUM()/AVG(). Metric views
are the certified surface — reach for gold/mv_* only for cuts the metric views lack.
""".strip(),
    },
    {
        "title": "Discover Ontology Pages (authoritative glossary)",
        "content": """
This space is governed by published Discover Ontology Pages — a business glossary of
30 concept terms grouped into the Online Retail domain and its 3 subdomains
(Sales Performance, Customer Analytics, Returns & Quality). Each Page defines a term,
its calculation, where it lives, benchmarks/thresholds, and the questions it answers.

Treat these Page definitions as the source of truth for business meaning. When a term
below is used, apply the Page's definition exactly:
  Sales:    Gross Revenue, Net Revenue, Average Order Value, Order Count, Units Sold,
            Total Discount, Sales Channel, New vs Returning Customer, Revenue per Customer,
            Q4 Seasonal Peak
  Customer: Customer Lifetime Value, CLV Segment, Loyalty Tier, Repeat Purchase Rate,
            Acquisition Channel, Customer Type (B2C/B2B), Demographic Segments
  Returns:  Return Rate, Faulty Batch (FAULT-PHON-*), Q4 2025 Return Spike, Return Reason,
            Total Refund Amount, Avg Days to Return, Cancellation Rate, DQ Quarantine
  Cross:    Region & Country Hierarchy, Product Category Taxonomy, Fiscal Calendar,
            PII Masking & GDPR, Medallion Layers

If a question matches a glossary term, use the measure/table named on its Page and, where
helpful, mention the definition (e.g. "Churned = no order in 180+ days").
""".strip(),
    },
]


# Optional: add the natural-language questions as UI sample questions.
SAMPLE_QUESTIONS = [
    "What were the top 5 categories by revenue in Q4 2025?",
    "Show me weekly revenue trend for APAC-East in 2025",
    "Which channel drives the highest average order value?",
    "Compare Q4 2025 vs Q4 2024 revenue by region",
    "Which income bracket has the highest customer lifetime value?",
    "What is the repeat purchase rate for Platinum vs Bronze tier customers?",
    "Which age group has the highest average order value?",
    "Show me revenue by loyalty tier over the last 6 months",
    "Why is the Electronics return rate elevated in Q4 2025?",
    "Show return rate by product SKU with more than 25% return rate",
    "Which region has the highest return rate?",
    "How many support tickets were raised for product defects in Q4 2025?",
]

# Verified question → SQL examples, one per glossary "use it to answer" pattern.
# {CATALOG} is normalised to the real catalog below (see NORMALISE step). Each query
# uses the exact measure/dimension named on the matching Discover Ontology Page:
#   - measures via MEASURE(); stored columns via SUM()/AVG()
#   - GROUP BY ALL / ORDER BY ALL is required for metric-view MEASURE() queries
EXAMPLE_QUESTION_SQLS = [
    {
        "question": "Show gross revenue by region and quarter for 2025-2026",
        "sql": """
SELECT `Sale Quarter`, `Region`, MEASURE(`Gross Revenue`) AS revenue
FROM {CATALOG}.online_retail_metrics.metrics_sales_kpis
WHERE YEAR(`Sale Quarter`) IN (2025, 2026)
GROUP BY ALL ORDER BY ALL
""",
    },
    {
        "question": "Which channel drives the highest average order value?",
        "sql": """
SELECT `Channel`, MEASURE(`Avg Order Value`) AS avg_order_value
FROM {CATALOG}.online_retail_metrics.metrics_sales_kpis
GROUP BY ALL ORDER BY avg_order_value DESC
""",
    },
    {
        "question": "How many new vs returning customers by quarter?",
        "sql": """
SELECT `Sale Quarter`,
       MEASURE(`New Customer Count`)       AS new_customers,
       MEASURE(`Returning Customer Count`) AS returning_customers
FROM {CATALOG}.online_retail_metrics.metrics_sales_kpis
GROUP BY ALL ORDER BY ALL
""",
    },
    {
        "question": "Show product category revenue by month for 2025",
        "sql": """
-- metrics_sales_kpis has NO Category dimension; category revenue lives in mv_category_revenue
SELECT category_name, sale_month, SUM(gross_revenue) AS revenue
FROM {CATALOG}.online_retail_metrics.mv_category_revenue
WHERE YEAR(sale_month) = 2025
GROUP BY ALL ORDER BY ALL
""",
    },
    {
        "question": "What is net revenue after returns by region in Q4 2025?",
        "sql": """
-- Net Revenue is a COLUMN (not a measure): SUM(net_revenue)
SELECT region_name,
       SUM(gross_revenue) AS gross_revenue,
       SUM(net_revenue)   AS net_revenue,
       SUM(refund_total)  AS refunds
FROM {CATALOG}.online_retail_metrics.mv_regional_orders
WHERE sale_month >= DATE '2025-10-01' AND sale_month < DATE '2026-01-01'
GROUP BY ALL ORDER BY net_revenue DESC
""",
    },
    {
        "question": "What is the repeat purchase rate for Platinum vs Bronze loyalty tier?",
        "sql": """
SELECT `Loyalty Tier`, MEASURE(`Repeat Purchase Rate`) AS repeat_rate
FROM {CATALOG}.online_retail_metrics.metrics_customer_kpis
GROUP BY ALL ORDER BY repeat_rate DESC
""",
    },
    {
        "question": "Show revenue by loyalty tier over time",
        "sql": """
SELECT `Loyalty Tier`, `Month`, MEASURE(`Revenue`) AS revenue
FROM {CATALOG}.online_retail_metrics.metrics_customer_kpis
GROUP BY ALL ORDER BY ALL
""",
    },
    {
        "question": "How many customers fall into each CLV segment?",
        "sql": """
SELECT clv_segment,
       COUNT(*)                     AS customers,
       ROUND(AVG(total_revenue), 2) AS avg_lifetime_revenue
FROM {CATALOG}.online_retail_gold.gold_customer_lifetime_value
GROUP BY ALL ORDER BY customers DESC
""",
    },
    {
        "question": "Show return rate by product category and faulty batch",
        "sql": """
SELECT `Category`, `Faulty Batch`, MEASURE(`Return Rate`) AS return_rate
FROM {CATALOG}.online_retail_metrics.metrics_product_kpis
GROUP BY ALL ORDER BY return_rate DESC
""",
    },
    {
        "question": "Why is the Electronics return rate elevated in Q4 2025?",
        "sql": """
SELECT `Return Month`, `Faulty Batch`,
       MEASURE(`Return Rate`)  AS return_rate,
       MEASURE(`Return Count`) AS returns
FROM {CATALOG}.online_retail_metrics.metrics_product_kpis
WHERE `Category` = 'Electronics' AND `Return Month` >= DATE '2025-07-01'
GROUP BY ALL ORDER BY `Return Month`
""",
    },
    {
        "question": "Which region has the highest cancellation rate?",
        "sql": """
-- Cancellation Rate is a COLUMN: AVG(cancellation_rate_pct)
SELECT region_name, ROUND(AVG(cancellation_rate_pct), 2) AS cancellation_rate_pct
FROM {CATALOG}.online_retail_metrics.mv_regional_orders
GROUP BY ALL ORDER BY cancellation_rate_pct DESC
""",
    },
    {
        "question": "Show the data quality quarantine summary by rule",
        "sql": """
SELECT source_table, dq_rule, severity, COUNT(*) AS record_count
FROM {CATALOG}.online_retail_silver.silver_dq_quarantine
GROUP BY ALL ORDER BY severity, dq_rule
""",
    },
]

#PAGES (sample questions per topic area)
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

# ── NORMALISE {CATALOG} placeholder ───────────────────────────────────────────
# KNOWLEDGE_SNIPPETS and EXAMPLE_QUESTION_SQLS are plain (non-f) strings so the
# body stays readable; substitute the real catalog here so nothing ships the
# literal "{CATALOG}" token to Genie.
for _snip in KNOWLEDGE_SNIPPETS:
    _snip["content"] = _snip["content"].replace("{CATALOG}", CATALOG)
for _ex in EXAMPLE_QUESTION_SQLS:
    _ex["sql"] = _ex["sql"].replace("{CATALOG}", CATALOG)

# COMMAND ----------

# NOTE: The Genie Spaces API endpoint varies by workspace.
# If the SDK doesn't have a direct Genie Space creation method,
# use the REST API directly.

# Check if Genie API is available
print("Creating NexusRetail Genie Space...")
print(f"Data sources: {len(DATA_SOURCES)} tables/views")
print(f"Knowledge snippets: {len(KNOWLEDGE_SNIPPETS)}")
print(f"Sample Questions: {len(SAMPLE_QUESTIONS)}")
print(f"Example SQLs: {len(EXAMPLE_QUESTION_SQLS)}")
print(f"Pages: {len(PAGES)}")

# COMMAND ----------

import json
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

# Get the first available SQL warehouse
warehouses = w.warehouses.list()
warehouse_id = None
for wh in warehouses:
    if wh.state and wh.state.value in ("RUNNING", "STARTING"):
        warehouse_id = wh.id
        break
    if wh.id:
        warehouse_id = wh.id  # fallback to any warehouse

if not warehouse_id:
    print("WARNING: No SQL warehouse found. Please create one and re-run this cell.")
else:
    table_identifiers = DATA_SOURCES
    
    # Check if a Genie Space with this title already exists
    existing_spaces = w.api_client.do("GET", "/api/2.0/genie/spaces")
    genie_space_id = None
    for space in existing_spaces.get("spaces", []):
        if space.get("title") == SPACE_TITLE:
            genie_space_id = space.get("space_id")
            print(f"Genie Space '{SPACE_TITLE}' already exists (ID: {genie_space_id})")
            break

    if not genie_space_id:
        print(f"Creating Geneie Space '{SPACE_TITLE}'...")
        serialized = json.dumps({
            "version": 2,
            "data_sources": {"tables": [{"identifier": t} for t in sorted(table_identifiers)]}
        })
        try:
            resp = w.api_client.do("POST", "/api/2.0/genie/spaces", body={
                "title": SPACE_TITLE,
                "description": SPACE_DESCRIPTION,
                "warehouse_id": warehouse_id,
                "serialized_space": serialized,
            })
            genie_space_id = resp.get("space_id")
            print(f"Genie Space created (ID: {genie_space_id})")
        except Exception as e:
            print(f"Error creating Genie Space: {e}")

print("=================================================================")
print("Add Instructions and Sample Questions next to Complete the Setup")
print("Complete Option A (UI) first, then note your Genie Space ID for Module 07.")

# COMMAND ----------

import requests# Add knowledge snippets (if space was created)


def get_token():
    # In a Databricks notebook, the token is available via dbutils
    return dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()


HOST    = spark.conf.get("spark.databricks.workspaceUrl", "e2-demo-field-eng.cloud.databricks.com")
TOKEN   = get_token()
HEADERS = {"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"}

BASE_URL = f"https://{HOST}/api/2.0"
if genie_space_id:
    for snip in KNOWLEDGE_SNIPPETS:
        snippet_resp = requests.post(
            f"{BASE_URL}/genie/spaces/{genie_space_id}/knowledge-snippets",
            headers=HEADERS,
            json={"title": snip["title"], "content": snip["content"].strip()}
        )
        status = "✓" if snippet_resp.status_code in (200,201) else "⚠"
        print(f"{BASE_URL}/genie/spaces/{genie_space_id}/knowledge-snippets")
        print(f"  {status} Snippet: {snip['title']}")

# COMMAND ----------

import hashlib
import json
import os
import requests
import requests# Add knowledge snippets (if space was created)


def get_token():
    # In a Databricks notebook, the token is available via dbutils
    return dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()


HOST    = spark.conf.get("spark.databricks.workspaceUrl", "e2-demo-field-eng.cloud.databricks.com")
TOKEN   = get_token()
HEADERS = {"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"}

BASE_URL = f"https://{HOST}/api/2.0"


HEADERS = {
    "Authorization": f"Bearer {TOKEN}",
    "Content-Type": "application/json",
}

def stable_id(*values: str) -> str:
    """Generate a deterministic 32-character lowercase hex ID."""
    value = "\n".join(values)
    return hashlib.md5(value.encode("utf-8")).hexdigest()



# 1. Retrieve the existing serialized Genie Space.
get_url = (
    f"https://{HOST}/api/2.0/genie/spaces/{genie_space_id}"
    "?include_serialized_space=true"
)

response = requests.get(get_url, headers=HEADERS)
response.raise_for_status()

space = response.json()
serialized_space = json.loads(space["serialized_space"])

instructions = serialized_space.setdefault("instructions", {})
config = serialized_space.setdefault("config", {})

# 2. Add/update the knowledge snippets as one text instruction.
knowledge_content = [
    f"## {snippet['title']}\n{snippet['content'].strip()}"
    for snippet in KNOWLEDGE_SNIPPETS
]

knowledge_instruction_id = stable_id("knowledge-snippets", genie_space_id)

text_instructions = instructions.setdefault("text_instructions", [])

existing_instruction = next(
    (
        item
        for item in text_instructions
        if item.get("id") == knowledge_instruction_id
    ),
    None,
)

knowledge_instruction = {
    "id": knowledge_instruction_id,
    "content": knowledge_content,
}

if existing_instruction:
    existing_instruction.update(knowledge_instruction)
else:
    text_instructions.append(knowledge_instruction)

# 3. Add natural-language sample questions.
sample_questions = config.setdefault("sample_questions", [])

existing_sample_text = {
    item["question"][0]
    for item in sample_questions
    if item.get("question")
}

for question in SAMPLE_QUESTIONS:
    if question not in existing_sample_text:
        sample_questions.append({
            "id": stable_id("sample-question", question),
            "question": [question],
        })

# 4. Add question + SQL examples.
example_sqls = instructions.setdefault("example_question_sqls", [])

existing_examples = {
    item["id"]: item
    for item in example_sqls
    if item.get("id")
}

for example in EXAMPLE_QUESTION_SQLS:
    question = example["question"].strip()
    sql = example["sql"].strip()

    example_id = stable_id("example-sql", question)

    existing_examples[example_id] = {
        "id": example_id,
        "question": [question],
        "sql": [sql],
    }

# Genie expects these entries to be ordered by ID.
instructions["example_question_sqls"] = sorted(
    existing_examples.values(),
    key=lambda item: item["id"],
)

# 5. Write the updated serialized definition back to Genie.
payload = {
    "serialized_space": json.dumps(serialized_space),
}

if space.get("etag"):
    payload["etag"] = space["etag"]

patch_url = f"https://{HOST}/api/2.0/genie/spaces/{genie_space_id}"
response = requests.patch(
    patch_url,
    headers=HEADERS,
    json=payload,
)
response.raise_for_status()

print(f"Updated Genie Space: {genie_space_id}")
print(f"Knowledge snippets: {len(KNOWLEDGE_SNIPPETS)}")
print(f"Sample questions: {len(config['sample_questions'])}")
print(
    "SQL examples: "
    f"{len(instructions['example_question_sqls'])}"
)