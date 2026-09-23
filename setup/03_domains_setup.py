# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# NexusRetail Analytics — Unity Catalog Discover Domains + Ontology Pages
# =============================================================================
# Creates the "Online Retail" Discover domain and its three subdomains, then
# generates a bulk-import-ready Pages file that is a TRUE BUSINESS GLOSSARY for
# the Genie Ontology — not one-line definitions.
#
# WHY THIS EXISTS
#   Discover *Domains* group governed assets by business purpose; *Pages* are
#   governed definitions of business concepts that Genie One cites as
#   authoritative context. Together they are the human-modelled layer of the
#   Genie Ontology — which is exactly what this repo (DQ_UC_Ontology) is about.
#
#   For Genie to "kick in" the ontology when you ask a natural-language question,
#   each Page must bridge two vocabularies: the BUSINESS term ("return rate",
#   "churned customer", "average order value") and the PHYSICAL query surface
#   (which metric view, which MEASURE(), which column). So every glossary entry
#   below carries:
#       • a plain-business Definition   (what a retail leader means by the term)
#       • How it's calculated           (the formula + the exact measure/column)
#       • Where it lives / how to query  (the materialised view + grain)
#       • Benchmarks & thresholds        (normal ranges, alert lines)
#       • Use it to answer               (the REAL questions Genie should map here)
#       • Related terms + Backing assets (cross-links + governed sources)
#   That "Use it to answer" block is the term → measure lookup that makes Genie
#   prioritise and cite these Pages.
#
# WHAT IT DOES
#   1. Registers the governed tags that back the domains (idempotent).
#   2. Creates the parent domain + 3 subdomains via the Domains API (idempotent,
#      and graceful if the account has hit its domain quota).
#   3. Emits a Markdown glossary you feed to Genie Code's "Bulk import pages"
#      (Discover ▸ Pages ▸ Create page ▸ Genie Code ▸ Bulk import pages):
#        • the curated concept glossary (business terms → metric views), and
#        • one auto-generated Page per governed table, grouped by (sub)domain,
#          seeded from the comments run_governance.py saved to Unity Catalog.
#
# WHY BULK-IMPORT (and not a direct API)
#   Pages have no public create API today (Beta). The supported programmatic
#   path is Genie Code bulk import from a document — so we generate that
#   document deterministically from governed metadata + this curated glossary.
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
#
# Descriptions are written as short BUSINESS CHARTERS — what the domain is for,
# the questions it answers, the headline metrics, and the certified sources —
# so a Discover user (and Genie) understands the domain at a glance.

PARENT_TAG = "online_retail"

PARENT_DOMAIN = {
    "tag_key":  PARENT_TAG,
    "subtitle": "NexusRetail — governed global e-commerce analytics",
    "description": (
        "The NexusRetail online-retail data product: a governed medallion "
        "platform (bronze → silver → gold → metrics) over 24 months of orders, "
        "customers, and returns across 7 regions and 65 countries. This is the "
        "single source of truth for how the business measures revenue, customers, "
        "and product quality. It answers questions such as: How is revenue "
        "trending by region and channel? Which customer segments drive lifetime "
        "value? Why did returns spike in Q4 2025? Certified semantic layer: the "
        "online_retail_metrics metric views. Home of the Q4-2025 faulty-batch "
        "return story. Browse the subdomains for Sales Performance, Customer "
        "Analytics, and Returns and Quality."
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
            "How much we sell, where, and through which channel. Owns the revenue "
            "and order metrics — gross revenue, net revenue, average order value, "
            "units sold, order count, and the web / mobile / partner_api channel "
            "mix — sliced by region, super-region, and time. Use it to track "
            "revenue trends, compare periods (the Q4 Oct-Dec seasonal peak runs "
            "40-50% above baseline), and split new vs returning customer revenue. "
            "Certified source: metrics_sales_kpis, backed by gold_daily_revenue "
            "and mv_category_revenue."
        ),
        "icon": {"name": "PRESENTATION_CHART", "color": "#1565C0"},
    },
    {
        "tag_key":  f"{PARENT_TAG}/customer_analytics",
        "genie_domain": "customer_analytics",
        "title":    "Customer Analytics",
        "subtitle": "Segments, loyalty, and lifetime value",
        "description": (
            "Who our customers are and how much they are worth over time. Owns "
            "customer segmentation (age, income, loyalty tier, acquisition "
            "channel, B2C vs B2B), repeat-purchase behaviour, revenue per "
            "customer, and customer-lifetime-value banding (High / Medium / Low / "
            "Churned). Use it to find the highest-value segments, measure "
            "retention by loyalty tier, and understand acquisition ROI. Certified "
            "source: metrics_customer_kpis and gold_customer_lifetime_value."
        ),
        "icon": {"name": "USERS_THREE", "color": "#6A1B9A"},
    },
    {
        "tag_key":  f"{PARENT_TAG}/returns_quality",
        "genie_domain": "returns_quality",
        "title":    "Returns & Quality",
        "subtitle": "Return rates, faulty batch, and data quality",
        "description": (
            "Why product comes back and whether the data can be trusted. Owns "
            "return rate, return reasons, refund value, days-to-return, and "
            "cancellation rate — plus the data-quality quarantine. This is where "
            "the flagship story lives: 8 defective FAULT-PHON-* smartphones "
            "shipped in Q3 2025 drove a Q4-2025 return spike concentrated in "
            "APAC-East. Use it to isolate faulty-batch impact, rank products by "
            "return rate against the 25% alert line, and audit data health. "
            "Certified source: metrics_product_kpis, mv_regional_orders, and "
            "silver_dq_quarantine."
        ),
        "icon": {"name": "PACKAGE", "color": "#C62828"},
    },
]

TAG_TO_TITLE = {PARENT_TAG: "Online Retail"}
TAG_TO_TITLE.update({s["tag_key"]: s["title"] for s in SUBDOMAINS})

# ── Curated concept Pages — the business glossary ─────────────────────────────
# The authoritative business terms Genie One should prioritise and cite. Each
# entry is a full glossary record, not a one-liner. Fields:
#   name        : the business term (Page title)
#   domain      : the (sub)domain governed tag the Page belongs to
#   synonyms    : how people phrase it in natural language (drives Genie matching)
#   definition  : plain-business meaning
#   calculation : the formula + the EXACT measure/column to use
#   lives_in    : the certified materialised view(s) + grain, i.e. how to query it
#   benchmark   : normal ranges / alert thresholds (optional)
#   use_cases   : the REAL natural-language questions this Page answers — the
#                 term → measure mapping that makes the ontology "kick in"
#   related     : cross-links to other glossary terms
#   sources     : governed backing assets (short table names → FQN at render time)
#
# Definitions are kept exact against the semantic layer in
# governance/03_metric_views.sql and src/gold_layer.py.

CURATED_PAGES = [

    # ── Sales Performance ─────────────────────────────────────────────────────
    {
        "name": "Gross Revenue", "domain": f"{PARENT_TAG}/sales_performance",
        "synonyms": ["revenue", "sales", "gross sales", "total revenue", "top line"],
        "definition": (
            "Total sales value from delivered, shipped, and confirmed orders, with "
            "line-item discounts already applied, before any refunds are deducted. "
            "The headline top-line number for the business."
        ),
        "calculation": (
            "`SUM(gross_revenue)`, where each line = `qty × unit_price × (1 − discount)`. "
            "In the semantic layer, use the `Gross Revenue` measure, e.g. "
            "SELECT MEASURE(`Gross Revenue`) FROM metrics_sales_kpis. "
            "All amounts are USD (currency codes are display-only)."
        ),
        "lives_in": (
            "Certified: `metrics_sales_kpis` (measure `Gross Revenue`, grain = day × "
            "channel × region). Also `gold_daily_revenue.gross_revenue` and "
            "`mv_category_revenue.gross_revenue` (category grain)."
        ),
        "benchmark": "Q4 (Oct-Dec) is the seasonal peak at +40-50% volume vs baseline.",
        "use_cases": [
            "What was total revenue in Q4 2025?",
            "Show the monthly gross revenue trend for APAC-East in 2025.",
            "Which region generated the most revenue last quarter?",
        ],
        "related": ["Net Revenue", "Average Order Value", "Sales Channel", "Q4 Seasonal Peak"],
        "sources": ["metrics_sales_kpis", "gold_daily_revenue", "mv_category_revenue"],
    },
    {
        "name": "Net Revenue", "domain": f"{PARENT_TAG}/sales_performance",
        "synonyms": ["net sales", "revenue after refunds", "revenue net of returns"],
        "definition": (
            "Gross revenue minus refunds paid on returned goods in the same period — "
            "the revenue the business actually keeps after returns."
        ),
        "calculation": (
            "`net_revenue = gross_revenue − refund_total`. Net Revenue is a stored "
            "COLUMN (not a metric-view measure): `SUM(net_revenue)` in "
            "`gold_regional_performance` / `mv_regional_orders` / `mv_category_revenue`."
        ),
        "lives_in": (
            "`mv_regional_orders.net_revenue` (region × country × month) and "
            "`mv_category_revenue.net_revenue` (category × region × month). Query with "
            "`SUM(net_revenue)`, not `MEASURE()`."
        ),
        "benchmark": (
            "Net and Gross diverge most in Q4 2025, where the faulty-batch return "
            "spike inflates refunds in APAC-East."
        ),
        "use_cases": [
            "What was net revenue after returns by region in Q4 2025?",
            "How much did returns reduce revenue in APAC-East last quarter?",
            "Compare gross vs net revenue for Electronics in 2025.",
        ],
        "related": ["Gross Revenue", "Total Refund Amount", "Return Rate", "Q4 2025 Return Spike"],
        "sources": ["mv_regional_orders", "mv_category_revenue", "gold_regional_performance"],
    },
    {
        "name": "Average Order Value", "domain": f"{PARENT_TAG}/sales_performance",
        "synonyms": ["AOV", "avg order value", "average basket", "spend per order"],
        "definition": "Average revenue per order — a core measure of basket size and pricing power.",
        "calculation": (
            "`SUM(gross_revenue) / NULLIF(SUM(order_count), 0)`. Available as the "
            "`Avg Order Value` measure in both `metrics_sales_kpis` and "
            "`metrics_customer_kpis`, via MEASURE(`Avg Order Value`)."
        ),
        "lives_in": (
            "`metrics_sales_kpis` (slice by Channel / Region / time) and "
            "`metrics_customer_kpis` (slice by Loyalty Tier / segment)."
        ),
        "benchmark": "Typical range $85-$120 across regions; partner_api and higher loyalty tiers skew higher.",
        "use_cases": [
            "Which channel — web, mobile, or partner API — has the highest average order value?",
            "What is the average order value for Platinum vs Bronze customers?",
            "How has AOV trended by region this year?",
        ],
        "related": ["Gross Revenue", "Sales Channel", "Loyalty Tier", "Order Count"],
        "sources": ["metrics_sales_kpis", "metrics_customer_kpis"],
    },
    {
        "name": "Order Count", "domain": f"{PARENT_TAG}/sales_performance",
        "synonyms": ["orders", "number of orders", "order volume", "transactions"],
        "definition": (
            "The number of distinct orders placed and confirmed / shipped / delivered "
            "in the period. Cancelled orders are excluded from revenue grains."
        ),
        "calculation": "`SUM(order_count)` — use MEASURE(`Order Count`) in `metrics_sales_kpis` or `metrics_customer_kpis`.",
        "lives_in": "`metrics_sales_kpis` (day × channel × region) and `metrics_customer_kpis` (segment grain).",
        "benchmark": "Order volume rises 40-50% in Q4 (seasonal peak).",
        "use_cases": [
            "How many orders were placed in each region in Q4 2025?",
            "Show order volume by channel month over month.",
            "Which loyalty tier places the most orders?",
        ],
        "related": ["Gross Revenue", "Average Order Value", "Units Sold", "Q4 Seasonal Peak"],
        "sources": ["metrics_sales_kpis", "gold_daily_revenue"],
    },
    {
        "name": "Units Sold", "domain": f"{PARENT_TAG}/sales_performance",
        "synonyms": ["units", "quantity sold", "volume", "items sold"],
        "definition": "Total physical item quantity sold, summed across order lines. Distinct from Order Count (an order can contain many units).",
        "calculation": "`SUM(units_sold)` — stored column, not a metric-view measure.",
        "lives_in": "`mv_category_revenue.units_sold` and `gold_category_sales.units_sold` (category × subcategory × region × month).",
        "use_cases": [
            "How many units did we sell in Electronics last quarter?",
            "Which subcategory sold the most units in APAC-East?",
            "Compare units sold vs revenue by category.",
        ],
        "related": ["Gross Revenue", "Order Count", "Product Category Taxonomy"],
        "sources": ["mv_category_revenue", "gold_category_sales"],
    },
    {
        "name": "Total Discount", "domain": f"{PARENT_TAG}/sales_performance",
        "synonyms": ["discount", "markdown", "promotional discount", "price reduction"],
        "definition": "The aggregate dollar value of discounts applied at the order line — the gap between list price and what customers actually paid.",
        "calculation": "`SUM(total_discount)`, where line discount = `unit_price × quantity × discount_pct/100`. Stored column.",
        "lives_in": "`mv_category_revenue.total_discount` / `gold_category_sales.total_discount` (category × region × month).",
        "use_cases": [
            "How much did we give away in discounts by category in 2025?",
            "Which region carries the deepest discounts?",
            "Is discount depth growing quarter over quarter?",
        ],
        "related": ["Gross Revenue", "Units Sold", "Average Order Value"],
        "sources": ["mv_category_revenue", "gold_category_sales"],
    },
    {
        "name": "Sales Channel", "domain": f"{PARENT_TAG}/sales_performance",
        "synonyms": ["channel", "order channel", "sales channel", "web mobile partner"],
        "definition": "The route an order was placed through: web, mobile, or partner_api.",
        "calculation": "The `Channel` dimension in `metrics_sales_kpis`; underlying column `channel` in `gold_daily_revenue`.",
        "lives_in": "`metrics_sales_kpis` — slice any sales measure (revenue, AOV, order count) by `Channel`.",
        "benchmark": "Approximate mix: web ~55%, mobile ~35%, partner_api ~10%.",
        "use_cases": [
            "What share of revenue comes from mobile vs web?",
            "Which channel has the highest average order value?",
            "Show the channel mix trend over the last 12 months.",
        ],
        "related": ["Gross Revenue", "Average Order Value", "New vs Returning Customer"],
        "sources": ["metrics_sales_kpis", "gold_daily_revenue"],
    },
    {
        "name": "New vs Returning Customer", "domain": f"{PARENT_TAG}/sales_performance",
        "synonyms": ["new customers", "returning customers", "first-time buyers", "acquisition vs retention"],
        "definition": (
            "New customers placed their first-ever order in the period; returning "
            "customers ordered before it. Splits growth into acquisition vs retention."
        ),
        "calculation": (
            "MEASURE(`New Customer Count`) and MEASURE(`Returning Customer Count`) "
            "in `metrics_sales_kpis`; columns `new_customers` / `returning_customers` in "
            "`gold_daily_revenue`."
        ),
        "lives_in": "`metrics_sales_kpis` and `gold_daily_revenue` (day × channel × region).",
        "use_cases": [
            "What percentage of revenue comes from returning vs new customers?",
            "How many new customers did we acquire in Q4 2025 by region?",
            "Is our returning-customer share growing?",
        ],
        "related": ["Revenue per Customer", "Repeat Purchase Rate", "Acquisition Channel"],
        "sources": ["metrics_sales_kpis", "gold_daily_revenue"],
    },
    {
        "name": "Revenue per Customer", "domain": f"{PARENT_TAG}/sales_performance",
        "synonyms": ["revenue per customer", "spend per customer", "customer yield"],
        "definition": "Revenue divided by the number of unique customers — a proxy for engagement depth and value density.",
        "calculation": (
            "`SUM(gross_revenue) / NULLIF(SUM(unique_customers), 0)` — "
            "MEASURE(`Revenue per Customer`) in `metrics_sales_kpis`, and the same "
            "measure (segment grain) in `metrics_customer_kpis`."
        ),
        "lives_in": "`metrics_sales_kpis` (region / channel) and `metrics_customer_kpis` (segment).",
        "use_cases": [
            "Which region has the highest revenue per customer?",
            "Which customer segment yields the most revenue per customer in APAC-East?",
            "Compare revenue per customer across acquisition channels.",
        ],
        "related": ["Customer Lifetime Value", "Average Order Value", "New vs Returning Customer"],
        "sources": ["metrics_sales_kpis", "metrics_customer_kpis"],
    },
    {
        "name": "Q4 Seasonal Peak", "domain": f"{PARENT_TAG}/sales_performance",
        "synonyms": ["holiday peak", "Q4 spike", "seasonal spike", "peak season"],
        "definition": "October-December is the peak sales period each year, running roughly 40-50% above baseline order volume.",
        "calculation": "Slice any sales measure by the `Sale Quarter` dimension (`DATE_TRUNC('QUARTER', sale_date)`) in `metrics_sales_kpis`.",
        "lives_in": "`metrics_sales_kpis` (`Sale Quarter` / `Sale Month` dimensions).",
        "benchmark": (
            "Do NOT confuse the seasonal *sales* peak with the Q4-2025 *return* "
            "spike — that is a quality anomaly, not seasonality. See the Faulty "
            "Batch and Q4 2025 Return Spike pages."
        ),
        "use_cases": [
            "How much higher is Q4 revenue than the rest of the year?",
            "Compare Q4 2025 vs Q4 2024 revenue by region.",
            "Which quarter has the highest order volume?",
        ],
        "related": ["Gross Revenue", "Order Count", "Fiscal Calendar & Time Grain", "Q4 2025 Return Spike"],
        "sources": ["metrics_sales_kpis", "gold_daily_revenue"],
    },

    # ── Customer Analytics ────────────────────────────────────────────────────
    {
        "name": "Customer Lifetime Value", "domain": f"{PARENT_TAG}/customer_analytics",
        "synonyms": ["CLV", "LTV", "lifetime value"],
        "definition": (
            "The total value a customer has generated across their full order "
            "history, summarised one row per customer and banded into value segments."
        ),
        "calculation": (
            "Per-customer aggregates in `gold_customer_lifetime_value`: `total_revenue`, "
            "`total_orders`, `avg_order_value`, `days_since_last_order`, "
            "`orders_per_30_days`, and `clv_segment`."
        ),
        "lives_in": "`gold_customer_lifetime_value` (grain = customer_id).",
        "use_cases": [
            "Which income bracket has the highest average lifetime value?",
            "Who are our top 100 customers by lifetime revenue?",
            "What is average CLV by loyalty tier?",
        ],
        "related": ["CLV Segment", "Loyalty Tier", "Revenue per Customer", "Repeat Purchase Rate"],
        "sources": ["gold_customer_lifetime_value"],
    },
    {
        "name": "CLV Segment", "domain": f"{PARENT_TAG}/customer_analytics",
        "synonyms": ["clv segment", "value segment", "high value customer", "churned customer"],
        "definition": "The value/health band assigned to each customer: High, Medium, Low, or Churned.",
        "calculation": (
            "`clv_segment` = **High** (total_revenue ≥ $1,000 OR loyalty_tier = Platinum) · "
            "**Medium** ($200-$999) · **Low** (<$200) · **Churned** (no order in 180+ days). "
            "Filter/group on `clv_segment` in `gold_customer_lifetime_value`."
        ),
        "lives_in": "`gold_customer_lifetime_value.clv_segment` (grain = customer_id).",
        "benchmark": "Churned = no order in the last 180 days; treat a rising churned share as a retention alert.",
        "use_cases": [
            "How many customers fall into each CLV segment?",
            "Which age group has the most churned customers?",
            "What share of revenue comes from High-value customers?",
        ],
        "related": ["Customer Lifetime Value", "Loyalty Tier", "Repeat Purchase Rate"],
        "sources": ["gold_customer_lifetime_value"],
    },
    {
        "name": "Loyalty Tier", "domain": f"{PARENT_TAG}/customer_analytics",
        "synonyms": ["loyalty", "tier", "membership tier", "Platinum Gold Silver Bronze"],
        "definition": "The loyalty-program tier a customer belongs to: Bronze, Silver, Gold, or Platinum.",
        "calculation": "The `Loyalty Tier` dimension in `metrics_customer_kpis`; slice any customer measure by it.",
        "lives_in": "`metrics_customer_kpis` / `mv_customer_demo_sales` (segment × region × month).",
        "benchmark": "Mix: Bronze ~40%, Silver ~30%, Gold ~20%, Platinum ~10%. Higher tiers = higher AOV, repeat rate, and CLV; lowest churn.",
        "use_cases": [
            "What is the repeat purchase rate for Platinum vs Bronze?",
            "Show revenue by loyalty tier over the last 6 months.",
            "Which tier has the highest average order value?",
        ],
        "related": ["Repeat Purchase Rate", "CLV Segment", "Average Order Value", "Customer Lifetime Value"],
        "sources": ["metrics_customer_kpis", "mv_customer_demo_sales"],
    },
    {
        "name": "Repeat Purchase Rate", "domain": f"{PARENT_TAG}/customer_analytics",
        "synonyms": ["repeat rate", "retention rate", "repeat purchase rate"],
        "definition": "The share of customers in a segment who placed more than one order — a headline retention measure.",
        "calculation": (
            "% of segment customers with more than one order. Use "
            "MEASURE(`Repeat Purchase Rate`) in `metrics_customer_kpis` "
            "(averages the pre-computed `repeat_purchase_rate_pct`)."
        ),
        "lives_in": "`metrics_customer_kpis` / `mv_customer_demo_sales`.",
        "benchmark": "Rises with loyalty tier; Platinum is highest. Falling repeat rate is an early churn signal.",
        "use_cases": [
            "What is the repeat purchase rate by loyalty tier?",
            "Which acquisition channel produces the most repeat buyers?",
            "How does repeat rate differ by age bracket?",
        ],
        "related": ["Loyalty Tier", "CLV Segment", "New vs Returning Customer", "Acquisition Channel"],
        "sources": ["metrics_customer_kpis", "mv_customer_demo_sales"],
    },
    {
        "name": "Acquisition Channel", "domain": f"{PARENT_TAG}/customer_analytics",
        "synonyms": ["acquisition source", "how customer joined", "marketing channel"],
        "definition": "How a customer was first acquired: organic_search, paid_search, social_media, referral, or email_campaign.",
        "calculation": "The `Acquisition Channel` dimension in `metrics_customer_kpis`; slice revenue-per-customer, repeat rate, or count by it.",
        "lives_in": "`metrics_customer_kpis` / `mv_customer_demo_sales`.",
        "use_cases": [
            "Which acquisition channel delivers the highest revenue per customer?",
            "Which channel brings in the most repeat buyers?",
            "Compare CLV by acquisition channel.",
        ],
        "related": ["Revenue per Customer", "Repeat Purchase Rate", "Customer Lifetime Value"],
        "sources": ["metrics_customer_kpis", "mv_customer_demo_sales"],
    },
    {
        "name": "Customer Type (B2C / B2B)", "domain": f"{PARENT_TAG}/customer_analytics",
        "synonyms": ["B2C", "B2B", "business account", "individual customer", "customer type"],
        "definition": "Whether a customer is an individual consumer (B2C) or a business account (B2B). ~500 B2C and ~100 B2B customers.",
        "calculation": "The `Customer Type` dimension in `metrics_customer_kpis` (values `B2C` / `B2B`).",
        "lives_in": "`metrics_customer_kpis` / `mv_customer_demo_sales`.",
        "use_cases": [
            "How does average order value differ between B2B and B2C?",
            "What share of revenue is B2B?",
            "Which regions have the most B2B customers?",
        ],
        "related": ["Average Order Value", "Revenue per Customer", "Loyalty Tier"],
        "sources": ["metrics_customer_kpis", "mv_customer_demo_sales"],
    },
    {
        "name": "Demographic Segments (Age & Income)", "domain": f"{PARENT_TAG}/customer_analytics",
        "synonyms": ["age bracket", "income bracket", "demographics", "age group", "income band"],
        "definition": (
            "The demographic bands used to slice customers. Age: 18-24 | 25-34 | 35-44 | "
            "45-54 | 55+. Income: <$30K | $30K-$60K | $60K-$100K | $100K-$200K | $200K+."
        ),
        "calculation": "The `Age Bracket` and `Income Bracket` dimensions in `metrics_customer_kpis`.",
        "lives_in": "`metrics_customer_kpis` / `mv_customer_demo_sales`.",
        "benchmark": "25-34 is the largest age segment (~30% of customers).",
        "use_cases": [
            "Which age group generates the most revenue?",
            "Which income bracket has the highest lifetime value?",
            "Break down revenue by age bracket and region.",
        ],
        "related": ["Customer Lifetime Value", "Revenue per Customer", "Loyalty Tier"],
        "sources": ["metrics_customer_kpis", "mv_customer_demo_sales"],
    },

    # ── Returns & Quality ─────────────────────────────────────────────────────
    {
        "name": "Return Rate", "domain": f"{PARENT_TAG}/returns_quality",
        "synonyms": ["return rate", "returns pct", "return percentage", "returns ratio"],
        "definition": "The percentage of orders (or units) that come back as returns — the core product-quality and satisfaction signal.",
        "calculation": (
            "`(return_count / order_count) × 100`. Use MEASURE(`Return Rate`) in "
            "`metrics_product_kpis`; also `return_rate_pct` in `gold_return_analysis` and "
            "`mv_regional_orders`."
        ),
        "lives_in": "`metrics_product_kpis` (product/category grain) and `mv_regional_orders` (region grain).",
        "benchmark": (
            "Normal by category: Electronics 6-8%, Apparel 5-7%, Books 1-2%, Food 2-3%, "
            "others 3-6%. **Alert threshold 25%.** FAULT-PHON-* products exceed **40%** in Q4 2025."
        ),
        "use_cases": [
            "Why is the Electronics return rate elevated in Q4 2025?",
            "Which products have a return rate above 25%?",
            "Which region has the highest return rate?",
        ],
        "related": ["Faulty Batch (FAULT-PHON-*)", "Return Reason", "Q4 2025 Return Spike", "Net Revenue"],
        "sources": ["metrics_product_kpis", "gold_return_analysis", "mv_regional_orders"],
    },
    {
        "name": "Faulty Batch (FAULT-PHON-*)", "domain": f"{PARENT_TAG}/returns_quality",
        "synonyms": ["faulty batch", "defective products", "FAULT-PHON", "defective smartphones", "bad batch"],
        "definition": (
            "8 defective Smartphone SKUs (prefix FAULT-PHON-*, product_idx 4-11) shipped "
            "in Q3 2025 — the documented root cause of the Q4-2025 return spike."
        ),
        "calculation": (
            "In `metrics_product_kpis`, use the `Faulty Batch` dimension — values are "
            "`'Faulty Batch (FAULT-*)'` vs `'Normal Product'`. In gold/silver, filter the "
            "boolean `faulty_batch = TRUE` in `gold_return_analysis` / `silver_dim_products`."
        ),
        "lives_in": "`metrics_product_kpis` (dimension `Faulty Batch`), `gold_return_analysis`, `silver_dim_products`.",
        "benchmark": (
            "Caused 78 of 120 total returns in Q4 2025, concentrated in APAC-East (REG-005). "
            "DQX quarantined 41 `faulty_product` reason codes."
        ),
        "use_cases": [
            "How many faulty-batch products were returned vs normal products?",
            "What is the return rate for the faulty batch?",
            "Show the weekly return trend for FAULT-PHON-* SKUs from Q3 2025.",
        ],
        "related": ["Return Rate", "Q4 2025 Return Spike", "Return Reason", "DQ Quarantine"],
        "sources": ["metrics_product_kpis", "gold_return_analysis", "silver_dim_products"],
    },
    {
        "name": "Q4 2025 Return Spike", "domain": f"{PARENT_TAG}/returns_quality",
        "synonyms": ["return spike", "APAC-East return spike", "return anomaly", "Q4 anomaly"],
        "definition": "The Oct-Dec 2025 surge of returns in APAC-East driven by the faulty batch; support tickets rose ~3×.",
        "calculation": (
            "Filter `Faulty Batch = 'Faulty Batch (FAULT-*)'` and `Return Month` in Q4 2025 "
            "in `metrics_product_kpis`, or region = APAC-East in `mv_regional_orders`."
        ),
        "lives_in": "`mv_regional_orders` (region × month), `gold_return_analysis` (product × week).",
        "benchmark": (
            "78 of 120 total returns landed in Oct-Dec 2025. product_defect support tickets "
            "went ~10% → ~40% of volume. Returns stabilised in Q1 2026 as faulty stock cleared."
        ),
        "use_cases": [
            "Why did returns spike in Q4 2025?",
            "Which region drove the Q4 2025 return spike?",
            "When did returns return to normal after the spike?",
        ],
        "related": ["Faulty Batch (FAULT-PHON-*)", "Return Rate", "Net Revenue", "DQ Quarantine"],
        "sources": ["mv_regional_orders", "gold_return_analysis"],
    },
    {
        "name": "Return Reason", "domain": f"{PARENT_TAG}/returns_quality",
        "synonyms": ["return reason", "reason code", "why returned"],
        "definition": (
            "The reason a customer gave for a return: faulty_product, wrong_item, "
            "changed_mind, damaged_in_transit, or not_as_described."
        ),
        "calculation": "The `Return Reason` dimension (`return_reason_code`) in `metrics_product_kpis`.",
        "lives_in": "`metrics_product_kpis` / `gold_return_analysis` (product × reason × week).",
        "benchmark": "`faulty_product` dominates the Q4 2025 spike (the faulty batch); 41 such cases were DQ-quarantined.",
        "use_cases": [
            "What are the top return reasons in Q4 2025?",
            "How many returns were due to faulty products vs changed mind?",
            "Which categories see the most 'damaged_in_transit' returns?",
        ],
        "related": ["Return Rate", "Faulty Batch (FAULT-PHON-*)", "Total Refund Amount"],
        "sources": ["metrics_product_kpis", "gold_return_analysis"],
    },
    {
        "name": "Total Refund Amount", "domain": f"{PARENT_TAG}/returns_quality",
        "synonyms": ["refund", "refunds", "refund value", "money refunded"],
        "definition": "The total dollar value refunded to customers for returned goods.",
        "calculation": (
            "`SUM(total_refund_amount)` — MEASURE(`Total Refund Amount`) in "
            "`metrics_product_kpis`; column `refund_total` in `mv_regional_orders` / `mv_category_revenue`."
        ),
        "lives_in": "`metrics_product_kpis` (product × reason) and `mv_regional_orders` (region).",
        "use_cases": [
            "How much did we refund for faulty products in Q4 2025?",
            "Which category drove the most refund value?",
            "What is total refund by region last quarter?",
        ],
        "related": ["Net Revenue", "Return Rate", "Return Reason", "Q4 2025 Return Spike"],
        "sources": ["metrics_product_kpis", "mv_regional_orders"],
    },
    {
        "name": "Avg Days to Return", "domain": f"{PARENT_TAG}/returns_quality",
        "synonyms": ["days to return", "return latency", "time to return"],
        "definition": "Average number of days between order and return request. Longer latency often signals harder-to-detect defects.",
        "calculation": "`AVG(avg_days_to_return)` — MEASURE(`Avg Days to Return`) in `metrics_product_kpis`.",
        "lives_in": "`metrics_product_kpis` / `gold_return_analysis`.",
        "use_cases": [
            "How long do customers take to return faulty products vs normal ones?",
            "Which categories have the longest return latency?",
            "Did days-to-return change during the Q4 2025 spike?",
        ],
        "related": ["Return Rate", "Faulty Batch (FAULT-PHON-*)", "Return Reason"],
        "sources": ["metrics_product_kpis", "gold_return_analysis"],
    },
    {
        "name": "Cancellation Rate", "domain": f"{PARENT_TAG}/returns_quality",
        "synonyms": ["cancellation rate", "cancelled orders", "cancel rate"],
        "definition": "The percentage of orders cancelled before fulfilment — a pre-shipment demand-quality signal, distinct from returns.",
        "calculation": "`cancellation_rate_pct = 100 × cancelled_orders / order_count`. Stored column (query with `AVG`/`SUM`).",
        "lives_in": "`mv_regional_orders.cancellation_rate_pct` and `gold_regional_performance` (region × country × month).",
        "use_cases": [
            "Which region has the highest cancellation rate?",
            "How does cancellation rate trend over the year?",
            "Are cancellations correlated with the Q4 return spike?",
        ],
        "related": ["Return Rate", "Order Count", "Region & Country Hierarchy"],
        "sources": ["mv_regional_orders", "gold_regional_performance"],
    },
    {
        "name": "DQ Quarantine", "domain": f"{PARENT_TAG}/returns_quality",
        "synonyms": ["data quality quarantine", "quarantine table", "bad records", "data quality"],
        "definition": (
            "The quarantine layer capturing 139 records that failed critical data-quality "
            "checks — the source of truth for data-health questions. Split per entity into "
            "`<source_table>_quarantine` tables, with a combined `silver_dq_quarantine` roll-up."
        ),
        "calculation": (
            "Group `silver_dq_quarantine` by `source_table, dq_rule, severity`. Contents: "
            "~52 NULL invoice totals (dropped), 3 duplicate customer emails, ~43 failed "
            "payments, and 41 faulty_product return flags — **139** total. For per-entity "
            "triage, query e.g. `bronze_customers_quarantine` or `bronze_invoices_quarantine`."
        ),
        "lives_in": (
            "Per-entity: `bronze_customers_quarantine`, `bronze_invoices_quarantine`, … (8 tables). "
            "Combined roll-up: `silver_dq_quarantine`. All share the same 7-column schema."
        ),
        "benchmark": "A non-zero row count is a data-health alert; the seeded demo total is 139.",
        "use_cases": [
            "Show the data-quality quarantine summary — how many records failed each rule?",
            "How many invoices were dropped for NULL totals?",
            "Which data-quality rule caught the most records?",
        ],
        "related": ["Return Reason", "Faulty Batch (FAULT-PHON-*)", "Medallion Layers"],
        "sources": ["silver_dq_quarantine"],
    },

    # ── Cross-domain (parent Online Retail) ───────────────────────────────────
    {
        "name": "Region & Country Hierarchy", "domain": PARENT_TAG,
        "synonyms": ["regions", "geography", "super region", "region hierarchy", "countries"],
        "definition": "7 sales regions → 65 countries, rolled up into 3 super-regions (Americas, EMEA, Asia Pacific).",
        "calculation": (
            "Use the `Region` and `Super Region` dimensions in `metrics_sales_kpis` / "
            "`mv_regional_orders`. Region IDs: REG-001 AMER-North, REG-002 AMER-South, "
            "REG-003 EMEA-West, REG-004 EMEA-East, REG-005 APAC-East, REG-006 APAC-South, "
            "REG-007 MENA."
        ),
        "lives_in": "`silver_dim_geography` (country → region → super-region); joined into all regional grains.",
        "benchmark": "APAC-East (REG-005) is the region of the Q4-2025 faulty-batch return spike.",
        "use_cases": [
            "Show revenue by super-region.",
            "Which countries make up APAC-East?",
            "Compare return rate across all 7 regions.",
        ],
        "related": ["Gross Revenue", "Return Rate", "Q4 2025 Return Spike"],
        "sources": ["silver_dim_geography", "mv_regional_orders"],
    },
    {
        "name": "Product Category Taxonomy", "domain": PARENT_TAG,
        "synonyms": ["category", "subcategory", "product hierarchy", "product taxonomy"],
        "definition": "173 products organised into 12 categories and 50 subcategories (e.g. Electronics → Smartphones).",
        "calculation": "Use the `Category` and `Subcategory` dimensions in `metrics_product_kpis` / `mv_category_revenue`.",
        "lives_in": "`silver_dim_products` (flattened hierarchy); `mv_category_revenue` (sales) and `metrics_product_kpis` (returns).",
        "use_cases": [
            "What were the top 5 categories by revenue in Q4 2025?",
            "Which subcategory has the highest return rate?",
            "Break down units sold by category.",
        ],
        "related": ["Units Sold", "Return Rate", "Gross Revenue"],
        "sources": ["silver_dim_products", "mv_category_revenue"],
    },
    {
        "name": "Fiscal Calendar & Time Grain", "domain": PARENT_TAG,
        "synonyms": ["fiscal year", "quarter", "time grain", "date", "month week quarter"],
        "definition": (
            "Fiscal year = calendar year. The dataset spans 24 months (Sep 2024 - Sep 2026). "
            "Analyse by day, week, month, or quarter."
        ),
        "calculation": (
            "Time dimensions in `metrics_sales_kpis`: `Sale Date`, `Sale Week` "
            "(`DATE_TRUNC('WEEK', …)`), `Sale Month`, `Sale Quarter`. Returns use "
            "`Return Week` / `Return Month` in `metrics_product_kpis`."
        ),
        "lives_in": "All metric views expose the appropriate time dimension; `silver_dim_date` is the spine.",
        "benchmark": "Q4 (Oct-Dec) is the seasonal peak. Story timeline: Q3 2025 faulty ship → Q4 2025 spike → Q1 2026 stabilised.",
        "use_cases": [
            "Show the weekly revenue trend for 2025.",
            "Compare this quarter to the same quarter last year.",
            "What is the monthly return trend since Q3 2025?",
        ],
        "related": ["Q4 Seasonal Peak", "Q4 2025 Return Spike", "Gross Revenue"],
        "sources": ["metrics_sales_kpis", "gold_daily_revenue"],
    },
    {
        "name": "PII Masking & GDPR", "domain": PARENT_TAG,
        "synonyms": ["PII", "column mask", "data masking", "GDPR", "privacy"],
        "definition": "Unity Catalog column masks on `silver_dim_customers` so only the catalog owner sees raw PII.",
        "calculation": (
            "Mask functions: `full_name` → first initial, `email` → ***@domain, `phone` → "
            "***-***-XXXX, `date_of_birth` → year only. Non-owners see masked values "
            "transparently at query time."
        ),
        "lives_in": "`silver_dim_customers` (masked). Unmasked PII exists only in the owner-restricted bronze layer.",
        "benchmark": "GDPR data-minimisation (Art. 4(1)). Metric views and gold tables contain NO PII.",
        "use_cases": [
            "Which customer columns are masked?",
            "Where does unmasked PII live and who can see it?",
            "Is the semantic layer free of PII?",
        ],
        "related": ["Medallion Layers", "Customer Type (B2C / B2B)"],
        "sources": ["silver_dim_customers"],
    },
    {
        "name": "Medallion Layers", "domain": PARENT_TAG,
        "synonyms": ["medallion", "bronze silver gold", "data layers", "semantic layer"],
        "definition": (
            "The governed data flow: bronze (raw ingest) → silver (cleansed, PII masked, "
            "DQ-validated) → gold (aggregated, no PII) → metrics (certified semantic layer)."
        ),
        "calculation": (
            "For Genie/analytics, query the **metrics** schema first (metric views + `mv_*`). "
            "Fall back to **gold** only when the metrics layer lacks the cut. Avoid silver "
            "for ad-hoc questions."
        ),
        "lives_in": "Schemas: online_retail_bronze / _silver / _gold / _metrics.",
        "use_cases": [
            "Which tables should I use for revenue questions?",
            "What is the certified semantic layer?",
            "Where are the data-quality checks applied?",
        ],
        "related": ["DQ Quarantine", "PII Masking & GDPR", "Gross Revenue"],
        "sources": ["metrics_sales_kpis", "silver_dq_quarantine"],
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

# ── 4. Build the Pages bulk-import Markdown (the business glossary) ───────────
print("\n4. Generating Pages bulk-import file (business glossary)...")


def _fqn(short: str) -> str:
    """Best-effort catalog.schema.table for a short table name, for @-tagging."""
    prefix = short.split("_", 1)[0]  # bronze/silver/gold/mv/metrics
    schema = {
        "bronze": "online_retail_bronze", "silver": "online_retail_silver",
        "gold": "online_retail_gold", "mv": "online_retail_metrics",
        "metrics": "online_retail_metrics",
    }.get(prefix, "online_retail_gold")
    return f"{CATALOG}.{schema}.{short}"


def _page_md(page: dict) -> str:
    """Render one glossary Page. `page` is a dict with the curated-glossary
    fields; every optional section is skipped cleanly when absent so the
    same renderer serves both concept Pages and auto-generated table Pages."""
    dom = TAG_TO_TITLE.get(page["domain"], page["domain"])
    lines = [
        f"### Page: {page['name']}", "",
        f"- **Domain:** {dom}",
        f"- **Certified owner:** {OWNER_USER}",
    ]
    if page.get("synonyms"):
        lines.append(f"- **Also known as:** {', '.join(page['synonyms'])}")
    lines += [f"- **Business definition:** {page['definition']}", ""]

    if page.get("calculation"):
        lines += ["**How it's calculated**", "", page["calculation"], ""]
    if page.get("lives_in"):
        lines += ["**Where it lives / how to query**", "", page["lives_in"], ""]
    if page.get("benchmark"):
        lines += ["**Benchmarks & thresholds**", "", page["benchmark"], ""]
    if page.get("use_cases"):
        lines += ["**Use it to answer** _(natural-language questions Genie maps here)_", ""]
        lines += [f"- {q}" for q in page["use_cases"]]
        lines.append("")
    if page.get("related"):
        lines.append("**Related terms:** " + ", ".join(page["related"]))
    if page.get("sources"):
        lines.append("**Backing assets:** " + ", ".join(f"`{_fqn(s)}`" for s in page["sources"]))
    lines.append("")
    return "\n".join(lines)


md = [
    "# NexusRetail — Genie Ontology Pages (Business Glossary, bulk import)",
    "",
    "Import into **Discover ▸ Pages ▸ Create page ▸ Genie Code ▸ Bulk import pages**.",
    "Attach this file as a source; Genie Code drafts one Page per section below.",
    "Review, then Publish. Once published, these Pages become authoritative context",
    "Genie One prioritises and cites — each term maps to the exact metric view /",
    "`MEASURE()` / column to answer the natural-language questions it lists.",
    "",
    "Definitions of the auto-generated table Pages come from the governed comments and",
    "domain tags applied by `governance/run_governance.py`; the concept Pages are the",
    "curated business glossary maintained in `setup/03_domains_setup.py`.",
    "",
    f"_Catalog:_ `{CATALOG}`  ·  _Owner:_ {OWNER_USER}  ·  _Domains:_ "
    + ", ".join(TAG_TO_TITLE.values()),
    "",
    "---", "",
    "## Concept Pages (business glossary)", "",
]

# Emit concept Pages grouped by (sub)domain so the glossary reads by business area.
_concept_order = [
    f"{PARENT_TAG}/sales_performance",
    f"{PARENT_TAG}/customer_analytics",
    f"{PARENT_TAG}/returns_quality",
    PARENT_TAG,
]
_by_dom_concept = {}
for p in CURATED_PAGES:
    _by_dom_concept.setdefault(p["domain"], []).append(p)

for dom_tag in _concept_order:
    pages = _by_dom_concept.get(dom_tag, [])
    if not pages:
        continue
    md += [f"### {TAG_TO_TITLE.get(dom_tag, 'Online Retail')}", ""]
    for p in pages:
        md.append(_page_md(p))

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
        md.append(_page_md({
            "name": table,
            "domain": tag if tag != "online_retail" else PARENT_TAG,
            "definition": comment.split(". ")[0][:200],
            "lives_in": comment,
            "sources": [table],
        }))

pages_md = "\n".join(md)
print(f"  ✓  built {len(CURATED_PAGES)} concept (glossary) pages + {len(comments)} table pages")

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
            f"COMMENT 'Generated Genie Ontology Pages (business glossary) for Discover bulk import.'")
    w.files.upload(OUT_FILE, io.BytesIO(pages_md.encode("utf-8")), overwrite=True)
    print(f"  ✓  wrote {OUT_FILE}")
    saved = True
except Exception as e:
    print(f"  ⚠  could not write to volume: {str(e)[:160]}")
    print("      The full Pages Markdown is printed below — copy it into a .md file.")

print(f"\n{'='*64}")
print("Domains + Pages setup complete.")
print(f"  Domains  : parent 'Online Retail' + {len(SUBDOMAINS)} subdomains")
print(f"  Pages    : {len(CURATED_PAGES)} concept (glossary) + {len(comments)} table (bulk-import file)")
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
