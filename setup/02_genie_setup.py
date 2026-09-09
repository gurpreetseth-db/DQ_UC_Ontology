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
200+ products across 7 regions and 65 countries. Ask natural language questions about
sales performance, customer behaviour, and product returns.

Data covers 24 months (Sep 2024 – Sep 2026), with a known Q3 2025 electronics
defect batch (FAULT-* SKUs) that caused a Q4 2025 return spike in APAC-East.
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
APAC-East, APAC-South, MENA) and 65 countries. 200 products span 12 categories and
55 subcategories. 600 customers (500 B2C, 100 B2B).

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
    "Show return rate by product SKU with more than 20% return rate",
    "Which region has the highest return rate?",
    "How many support tickets were raised for product defects in Q4 2025?",
]

# These are the SQL examples explicitly provided in your snippets.
EXAMPLE_QUESTION_SQLS = [
    {
        "question": "Revenue by region and month?",
        "sql": """
 SELECT `Sale Month`, `Region`, MEASURE(`Gross Revenue`) AS revenue
  FROM {CATALOG}.online_retail_metrics.metrics_sales_kpis
  WHERE YEAR(`Sale Month`) = 2025
  GROUP BY ALL ORDER BY ALL
""",
   },
    {
        "question": "Show category revenue by month for 2025",
        "sql": """
SELECT
  `Sale Month`,
  `Category`,
  MEASURE(`Gross Revenue`) AS revenue
FROM gurpreet_sethi.online_retail_metrics.metrics_sales_kpis
WHERE YEAR(`Sale Month`) = 2025
GROUP BY ALL
ORDER BY ALL
""",
    },
    {
        "question": "Show return rate by product category and faulty batch",
        "sql": """
SELECT
  `Category`,
  `Faulty Batch`,
  MEASURE(`Return Rate`) AS return_rate
FROM gurpreet_sethi.online_retail_metrics.metrics_product_kpis
GROUP BY ALL
ORDER BY ALL
""",
    },
    {
        "question": "Show revenue by loyalty tier",
        "sql": """
SELECT
  `Loyalty Tier`,
  `Month`,
  MEASURE(`Revenue`) AS revenue
FROM gurpreet_sethi.online_retail_metrics.metrics_customer_kpis
GROUP BY ALL
ORDER BY ALL
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
print(f"Pages: {len})

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
