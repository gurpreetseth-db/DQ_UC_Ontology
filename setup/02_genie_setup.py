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
CATALOG = "gurpreet_sethi"

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

Key tables:
- mv_category_revenue: revenue by category/subcategory/region/month
- mv_customer_demo_sales: revenue by age bracket, income bracket, loyalty tier
- mv_regional_orders: order and return metrics by country and region
- gold_daily_revenue: day-level revenue with new vs returning customers
- gold_return_analysis: return rates by product, reason, and week
- gold_customer_lifetime_value: CLV segments (High/Medium/Low/Churned)
""",
    },
    {
        "title": "Revenue Definition",
        "content": """
GROSS REVENUE = sum of order_total for delivered/shipped/confirmed orders.
NET REVENUE = gross_revenue minus refund_total (for the same period).
Discounts are reflected in net_revenue but not gross_revenue.

Fiscal year follows calendar year. Q4 = October–December (seasonal peak).
All monetary values are in USD unless stated otherwise.
""",
    },
    {
        "title": "Return Rate Calculation",
        "content": """
return_rate_pct = (return_count / order_count) * 100

Normal return rates by category:
- Electronics: 6-8% (increases to >40% for FAULT-* batch in Q4 2025)
- Apparel: 5-7%
- Books: 1-2%
- Food: 2-3%

The FAULT-* batch: 8 electronics SKUs (faulty_batch=TRUE) shipped in Q3 2025
(Jul–Sep) caused 72 of 120 total returns. These are concentrated in APAC-East
(region_id = REG-005). Filter on faulty_batch=TRUE or return_reason_code='faulty_product'
to isolate this story.
""",
    },
    {
        "title": "Customer Segments",
        "content": """
Loyalty tiers: Bronze (40%), Silver (30%), Gold (20%), Platinum (10%)
Age brackets: 18-24, 25-34, 35-44, 45-54, 55+
Income brackets: <$30K, $30K-$60K, $60K-$100K, $100K-$200K, $200K+

CLV segments (gold_customer_lifetime_value.clv_segment):
- High: total_revenue >= $1,000 OR loyalty_tier = Platinum
- Medium: total_revenue $200-$999
- Low: total_revenue < $200
- Churned: no order in the last 180 days

Acquisition channels: organic_search, paid_search, social_media, referral, email_campaign
""",
    },
    {
        "title": "Date and Time Context",
        "content": """
Data range: September 2024 to September 7, 2026 (today).
sale_month column is DATE_TRUNC('MONTH', sale_date) — use for monthly trends.
return_week column is DATE_TRUNC('WEEK', return_date) — use for weekly return trends.

The faulty batch story key dates:
- Q3 2025 (Jul–Sep 2025): faulty electronics orders placed (mostly APAC-East)
- Q4 2025 (Oct–Dec 2025): return spike begins, support tickets spike 3x
- Q1 2026 (Jan–Mar 2026): returns stabilise as batch cleared from inventory

Q4 is always the peak sales period (Oct–Dec) — expect 40-50% higher order volumes.
""",
    },
    {
        "title": "Regions and Countries",
        "content": """
Region IDs and names:
- REG-001: AMER-North (US, Canada, Mexico)
- REG-002: AMER-South (Brazil, Argentina, Colombia, Chile, Peru)
- REG-003: EMEA-West (UK, Germany, France, Spain, Italy, Netherlands, Sweden, Switzerland, etc.)
- REG-004: EMEA-East (Poland, Czech Republic, Romania, Hungary, Greece, Ukraine)
- REG-005: APAC-East (Japan, China, South Korea, Australia, Taiwan, Hong Kong) — has faulty batch issue
- REG-006: APAC-South (India, Singapore, Malaysia, Thailand, Indonesia, Philippines, Vietnam)
- REG-007: MENA (Saudi Arabia, UAE, Egypt, Turkey, Nigeria, South Africa, Kenya, Morocco)

Customer distribution: AMER-North 25%, EMEA-West 22%, APAC-East 20%, APAC-South 15%, others 18%
""",
    }
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

