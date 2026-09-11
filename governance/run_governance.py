# Databricks notebook source
# NexusRetail Analytics — Governance Setup
# Applies comments, tags, PII masks, and grants across all 5 schemas.
#
# Run via bundle job:
#   databricks bundle run nexus_retail_governance -t dev
#
# Or run locally with env vars:
#   CATALOG=gsethi WAREHOUSE_ID=abc123 OWNER_USER=me@co.com python3 governance/run_governance.py

# COMMAND ----------
import os, time
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState

# Reads from DAB job base_parameters when run as a notebook job.
# Falls back to environment variables for local execution.
def _get(param: str, default: str = "") -> str:
    try:
        return dbutils.widgets.get(param)
    except Exception:
        return os.environ.get(param.upper(), default)

dbutils.widgets.text("catalog",      "your_catalog_name")
dbutils.widgets.text("warehouse_id", "your_warehouse_id")
dbutils.widgets.text("owner_user",   "your.email@company.com")

CATALOG      = _get("catalog")
WAREHOUSE_ID = _get("warehouse_id")
OWNER_USER   = _get("owner_user")

if any("your_" in v or "your." in v for v in [CATALOG, WAREHOUSE_ID, OWNER_USER]):
    raise ValueError(
        "Set catalog, warehouse_id, and owner_user — either via job base_parameters "
        "or env vars: CATALOG=x WAREHOUSE_ID=y OWNER_USER=z python3 run_governance.py"
    )

w = WorkspaceClient()

# ── helpers ──────────────────────────────────────────────────────────────────

def sql(label: str, stmt: str, allow_fail: bool = False) -> bool:
    """Execute a SQL statement and print the result."""
    try:
        r = w.statement_execution.execute_statement(
            warehouse_id=WAREHOUSE_ID,
            statement=stmt.strip(),
            wait_timeout="0s",
        )
        sid = r.statement_id
        for _ in range(30):
            time.sleep(2)
            r2 = w.statement_execution.get_statement(sid)
            st = r2.status.state
            if st in (StatementState.SUCCEEDED, StatementState.CLOSED):
                print(f"  ✓  {label}")
                return True
            if st in (StatementState.FAILED, StatementState.CANCELED):
                err = r2.status.error.message[:150] if r2.status.error else "?"
                icon = "⚠" if allow_fail else "✗"
                print(f"  {icon}  {label}  {err}")
                return False
        print(f"  ⏰  {label}  timeout")
        return False
    except Exception as e:
        icon = "⚠" if allow_fail else "✗"
        print(f"  {icon}  {label}  {str(e)[:120]}")
        return False


def apply_tags(obj_type: str, full_name: str, tags: dict, allow_fail: bool = True):
    """Apply UC tags to a securable object."""
    tag_str = ",".join(f"'{k}'='{v}'" for k, v in tags.items())
    sql(f"tag {full_name}", f"ALTER {obj_type} {full_name} SET TAGS ({tag_str})", allow_fail)


def col_comment(table: str, col: str, comment: str):
    """Apply a comment to a table column (Streaming Tables only, not MVs)."""
    sql(f"col comment {table}.{col}",
        f"ALTER TABLE {table} ALTER COLUMN {col} COMMENT '{comment}'",
        allow_fail=True)


def col_tag(table: str, col: str, tags: dict):
    """Apply tags to a column."""
    tag_str = ",".join(f"'{k}'='{v}'" for k, v in tags.items())
    sql(f"col tag {table}.{col}",
        f"ALTER TABLE {table} ALTER COLUMN {col} SET TAGS ({tag_str})",
        allow_fail=True)

# ── metadata config ───────────────────────────────────────────────────────────
# Structure: {full_table_name: {comment, tags, columns: {col: comment}}}
# Note: column comments/tags only apply to Streaming Tables, not MVs/Views.

C = CATALOG  # shorthand

CATALOG_COMMENT = f"""
NexusRetail Analytics Platform — primary demo catalog.
Contains domain schemas for agriculture ops and the NexusRetail online retail demo.
Online retail schemas: online_retail_raw (source), online_retail_bronze (ingest),
online_retail_silver (cleansed, PII masked), online_retail_gold (aggregated),
online_retail_metrics (semantic layer).
Owner: {OWNER_USER}
""".strip().replace("\n", " ")

SCHEMA_COMMENTS = {
    f"{C}.online_retail_raw":
        "NexusRetail raw source data — 20 Parquet tables in UC Volume /raw_data/. "
        "Landing zone for SDP Auto Loader bronze ingestion. "
        "Contains unmasked PII in customer tables. Owner: data_engineering.",
    f"{C}.online_retail_bronze":
        "NexusRetail bronze layer — 18 Auto Loader streaming tables from UC Volume Parquet files. "
        "Raw fidelity preserved, schema enforced, metadata columns added (_ingestion_time, _source_file). "
        "PII present in bronze_customers, bronze_customer_demographics, bronze_customer_addresses. "
        "Quality tier: bronze. Owner: data_engineering.",
    f"{C}.online_retail_silver":
        "NexusRetail silver layer — cleansed, DQX-style validated streaming tables. "
        "PII masked via UC column mask functions on silver_dim_customers. "
        "Only the catalog owner sees raw PII values. "
        "silver_dq_quarantine captures 139 records failing critical checks. "
        "Quality tier: silver. Regulatory: GDPR-aligned column masks applied.",
    f"{C}.online_retail_gold":
        "NexusRetail gold layer — 6 business-ready Materialized View aggregations. "
        "No PII — customer data aggregated by segment/bracket only. "
        "Tables: category_sales, customer_segment_sales, regional_performance, "
        "customer_lifetime_value, return_analysis, daily_revenue. "
        "Quality tier: gold. Owner: analytics.",
    f"{C}.online_retail_metrics":
        "NexusRetail semantic/metrics layer — 3 UC Materialized Views and 3 Metric Views "
        "(WITH METRICS LANGUAGE YAML). Genie One data sources. No PII. "
        "Metric views: metrics_sales_kpis, metrics_customer_kpis, metrics_product_kpis. "
        "Quality tier: gold. Owner: analytics. Genie domain: Online Retail Analytics.",
}

# ── Schema-level tags ────────────────────────────────────────────────────────
# Applied to the schema securable so Genie domain classification and
# Catalog Explorer browsing pick up the right groupings automatically.

SCHEMA_TAGS = {
    f"`{C}`.online_retail_raw": {
        "quality_tier":  "raw",
        "data_layer":    "raw",
        "data_domain":   "online_retail",
        "data_product":  "nexus_retail",
        "contains_pii":  "true",
        "owner":         "data_engineering",
        "genie_ready":   "false",
    },
    f"`{C}`.online_retail_bronze": {
        "quality_tier":  "bronze",
        "data_layer":    "bronze",
        "data_domain":   "online_retail",
        "data_product":  "nexus_retail",
        "contains_pii":  "true",
        "owner":         "data_engineering",
        "genie_ready":   "false",
    },
    f"`{C}`.online_retail_silver": {
        "quality_tier":  "silver",
        "data_layer":    "silver",
        "data_domain":   "online_retail",
        "data_product":  "nexus_retail",
        "contains_pii":  "true",
        "pii_masked":    "true",
        "regulatory":    "gdpr",
        "owner":         "data_engineering",
        "genie_ready":   "false",
    },
    f"`{C}`.online_retail_gold": {
        "quality_tier":  "gold",
        "data_layer":    "gold",
        "data_domain":   "online_retail",
        "data_product":  "nexus_retail",
        "contains_pii":  "false",
        "owner":         "analytics",
        "genie_ready":   "true",
    },
    f"`{C}`.online_retail_metrics": {
        "quality_tier":  "gold",
        "data_layer":    "semantic",
        "data_domain":   "online_retail",
        "data_product":  "nexus_retail",
        "contains_pii":  "false",
        "owner":         "analytics",
        "genie_ready":   "true",
        "semantic_layer":"true",
    },
}

TABLE_METADATA = {
    # ── BRONZE — reference tables ────────────────────────────────────────────
    f"`{C}`.online_retail_bronze.bronze_ref_regions": {
        "comment":
            "Reference table: 7 global sales regions with super-region grouping and primary currency. "
            "Grain: one row per region. Static — no updates expected.",
        "tags": {"quality_tier": "bronze", "domain": "geography", "data_product": "nexus_retail",
                 "owner": "data_engineering", "table_type": "dimension", "genie_domain": "all"},
    },
    f"`{C}`.online_retail_bronze.bronze_ref_countries": {
        "comment":
            "Reference table: 65 countries with ISO codes, region FK, currency, and language. "
            "Covers all 7 sales regions. Grain: one row per country code.",
        "tags": {"quality_tier": "bronze", "domain": "geography", "data_product": "nexus_retail",
                 "owner": "data_engineering", "table_type": "dimension", "genie_domain": "all"},
    },
    f"`{C}`.online_retail_bronze.bronze_product_categories": {
        "comment":
            "Reference table: 12 top-level product categories (Electronics, Apparel, Home & Living, etc.). "
            "avg_return_rate is the category-level historical return rate benchmark. "
            "Grain: one row per category.",
        "tags": {"quality_tier": "bronze", "domain": "product", "data_product": "nexus_retail",
                 "owner": "data_engineering", "table_type": "dimension",
                 "genie_domain": "sales_performance"},
    },
    f"`{C}`.online_retail_bronze.bronze_product_subcategories": {
        "comment":
            "Reference table: 50 subcategories across 12 categories (4-5 per category). "
            "SUB-0101 (Smartphones) contains the 8 FAULT-PHON-* defective products. "
            "Grain: one row per subcategory.",
        "tags": {"quality_tier": "bronze", "domain": "product", "data_product": "nexus_retail",
                 "owner": "data_engineering", "table_type": "dimension",
                 "genie_domain": "sales_performance"},
    },
    f"`{C}`.online_retail_bronze.bronze_product_pricing": {
        "comment":
            "Product price history. Each product has 1-3 records with effective_from/effective_to dates. "
            "Used in silver_dim_products to derive current_price. "
            "Grain: one row per (product, effective date range).",
        "tags": {"quality_tier": "bronze", "domain": "product", "data_product": "nexus_retail",
                 "owner": "data_engineering", "table_type": "fact",
                 "genie_domain": "sales_performance"},
    },
    f"`{C}`.online_retail_bronze.bronze_return_items": {
        "comment":
            "Raw return line items: 1-2 items per return request with condition assessment "
            "(unopened/opened/damaged/defective). ~146 rows. "
            "Used in silver_fact_returns and gold_return_analysis for product-level return rates. "
            "Grain: one row per (return, product).",
        "tags": {"quality_tier": "bronze", "domain": "transaction", "data_product": "nexus_retail",
                 "owner": "data_engineering", "table_type": "fact",
                 "genie_domain": "returns_quality"},
    },
    # ── BRONZE — core tables ─────────────────────────────────────────────────
    f"`{C}`.online_retail_bronze.bronze_orders": {
        "comment":
            "Raw order headers ingested via Auto Loader from UC Volume. "
            "1,200 orders over 24 months with Q4 seasonal spike (Oct-Dec ~40% higher volume). "
            "Statuses: delivered (65%), shipped (15%), confirmed (10%), cancelled (8%), pending (2%). "
            "order_total computed from order_items sum. Grain: one row per order.",
        "tags": {"quality_tier": "bronze", "domain": "transaction", "data_product": "nexus_retail",
                 "owner": "data_engineering", "pii": "false"},
        "columns": {
            "order_id":          "Unique order identifier in ORD-XXXXXXX format. Primary key. Never null (expect_or_fail).",
            "customer_idx":      "Integer FK to bronze_customers.customer_idx. Used to derive customer_id as CUST-XXXXXX.",
            "order_date":        "Timestamp when the order was placed. Seasonal spike visible in Oct-Dec each year.",
            "estimated_delivery":"Expected delivery date. NULL for cancelled orders.",
            "channel":           "Sales channel: web (55%), mobile (35%), partner_api (10%).",
            "status":            "Order lifecycle status. Validated by SDP expect decorator.",
            "order_total":       "Sum of all order line items after discounts. Validated against computed_total in silver.",
        },
    },
    f"`{C}`.online_retail_bronze.bronze_customers": {
        "comment":
            "Raw customer master with unmasked PII. 600 customers: 500 B2C individuals and 100 B2B companies. "
            "PII columns (full_name, email, phone, date_of_birth) are unmasked at this layer. "
            "Column masks are applied at silver_dim_customers — access to this table requires the owner role. "
            "3 intentional duplicate emails for DQX demo (duplicate.test@example.com). "
            "Grain: one row per customer.",
        "tags": {"quality_tier": "bronze", "domain": "customer", "contains_pii": "true",
                 "pii_classification": "direct", "regulatory": "gdpr", "data_product": "nexus_retail",
                 "owner": "data_engineering", "genie_domain": "sales_performance", "table_type": "fact"},
        "columns": {
            "customer_id":    "Business key in CUST-XXXXXX format. Primary key.",
            "full_name":      "PII:direct — customer full name. Masked at silver layer: shows first initial only for non-owner.",
            "email":          "PII:direct — customer email address. Masked at silver. 3 duplicates injected for DQX uniqueness demo.",
            "phone":          "PII:direct — customer phone number. Masked at silver: shows ***-***-XXXX for non-owner.",
            "date_of_birth":  "PII:direct — customer date of birth. Masked at silver: truncated to year-only for non-owner. GDPR Art.4(1).",
            "customer_type":  "Account type: B2C (individual consumer) or B2B (business account).",
            "region_id":      "FK to bronze_ref_regions.region_id. Derived from customer primary address country.",
            "country_code":   "ISO 3166-1 alpha-2 country code. FK to bronze_ref_countries.",
            "is_active":      "TRUE for active customers. FALSE for churned or closed accounts.",
        },
    },
    f"`{C}`.online_retail_bronze.bronze_invoices": {
        "comment":
            "Raw invoices from confirmed, shipped, and delivered orders — one invoice per order. "
            "~52 invoices intentionally have NULL invoice_total to demonstrate DQX completeness checks. "
            "These NULL records are dropped at silver_fact_invoices via @dp.expect_or_drop and captured "
            "in silver_dq_quarantine with rule: invoice_total_null. Grain: one row per invoice.",
        "tags": {"quality_tier": "bronze", "domain": "transaction", "data_product": "nexus_retail",
                 "dq_known_issue": "null_totals_intentional_demo", "owner": "data_engineering", "genie_domain": "sales_performance", "table_type": "fact"},
        "columns": {
            "invoice_id":      "Unique invoice identifier in INV-XXXXXXX format. Primary key.",
            "invoice_number":  "Human-readable invoice reference in NR-YYYY-XXXXXX format. Should be globally unique.",
            "order_id":        "FK to bronze_orders.order_id. One invoice per order.",
            "issue_date":      "Date the invoice was generated (order_date + 1 day).",
            "due_date":        "Payment due date (issue_date + 30 days). Used for overdue detection in silver.",
            "invoice_status":  "Lifecycle: paid, pending, overdue, cancelled.",
            "invoice_total":   "Invoice total in USD. ~52 records are intentionally NULL to demonstrate DQX DROP behaviour at silver.",
            "order_total":     "Original order total from bronze_orders. Used for reconciliation check in silver.",
        },
    },
    f"`{C}`.online_retail_bronze.bronze_products": {
        "comment":
            "Raw product master for 173 products across 12 categories and 50 subcategories. "
            "faulty_batch=TRUE is applied at bronze ingest time for 8 FAULT-PHON-* SKUs (product_idx 4-11). "
            "These defective products are the root cause of the Q3 2025 return spike demo story. "
            "Current price is enriched from bronze_product_pricing in the silver dim. "
            "Grain: one row per product.",
        "tags": {"quality_tier": "bronze", "domain": "product", "data_product": "nexus_retail",
                 "contains_faulty_batch": "true", "owner": "data_engineering", "genie_domain": "sales_performance", "table_type": "dimension"},
        "columns": {
            "product_id":      "Unique product identifier in PROD-XXXXX format. Primary key.",
            "sku":             "Stock keeping unit. FAULT-PHON-00XX prefix for the 8 defective batch products.",
            "product_name":    "Human-readable product name including brand.",
            "brand":           "Product brand name (e.g., TechNova, StyleCraft).",
            "category_id":     "FK to bronze_product_categories. CAT-01 = Electronics, CAT-02 = Apparel, etc.",
            "subcategory_id":  "FK to bronze_product_subcategories. SUB-0101 = Smartphones (contains faulty batch).",
            "faulty_batch":    "TRUE for 8 FAULT-PHON-00XX SKUs (product_idx 4-11) from the Q3 2025 defective shipment. Root cause of Q4 2025 return spike in APAC-East.",
            "base_price":      "Original listed price in USD. Must be > 0 (validated by SDP expect).",
            "is_active":       "FALSE for discontinued products.",
        },
    },
    f"`{C}`.online_retail_bronze.bronze_returns": {
        "comment":
            "Raw return requests. 120 total returns; 78 (65%) occurred in Q4 2025 — the main anomaly signal. "
            "41 returns have reason_code=faulty_product; these correlate with FAULT-PHON-* SKUs. "
            "Returns are validated at silver_fact_returns with return_reason_code expect decorator. "
            "Grain: one row per return request.",
        "tags": {"quality_tier": "bronze", "domain": "transaction", "data_product": "nexus_retail",
                 "contains_anomaly": "q4_2025_return_spike", "owner": "data_engineering", "genie_domain": "returns_quality", "table_type": "fact"},
        "columns": {
            "return_id":           "Unique return identifier in RET-XXXXXX format. Primary key.",
            "order_id":            "FK to bronze_orders. The order being returned.",
            "return_date":         "Date the return was initiated. Q4 2025 (Oct-Dec) shows anomalous spike of 78 returns.",
            "return_reason_code":  "Reason category: faulty_product | wrong_item | changed_mind | damaged_in_transit | not_as_described.",
            "return_status":       "Processing status: approved | pending | rejected | completed.",
            "refund_amount":       "USD amount refunded. NULL if return not yet approved.",
            "is_faulty_order":     "TRUE if the originating order contained any FAULT-PHON-* products.",
        },
    },
    f"`{C}`.online_retail_bronze.bronze_payments": {
        "comment":
            "Raw payment records — one per order. "
            "~43 records have payment_status=failed (approx. 3% failure rate), intentionally embedded "
            "for DQX warn demo. Failed payments are quarantined in silver_dq_quarantine with rule: failed_payment. "
            "Grain: one row per payment.",
        "tags": {"quality_tier": "bronze", "domain": "transaction", "data_product": "nexus_retail",
                 "dq_known_issue": "failed_payments_intentional_demo", "owner": "data_engineering", "genie_domain": "sales_performance", "table_type": "fact"},
        "columns": {
            "payment_id":      "Unique payment identifier in PAY-XXXXXXX format. Primary key.",
            "order_id":        "FK to bronze_orders.",
            "payment_method":  "Method used: credit_card | debit_card | paypal | bank_transfer | buy_now_pay_later.",
            "payment_status":  "Outcome: completed | pending | failed | refunded. ~43 records are failed (DQX warn demo).",
            "currency_code":   "ISO 4217 currency code (USD, EUR, CAD). Derived from customer region.",
            "gateway_ref":     "Payment gateway transaction reference (GW- + 12 hex chars).",
        },
    },
    f"`{C}`.online_retail_bronze.bronze_order_items": {
        "comment":
            "Raw order line items. 2-4 items per order. Each row is one product on one order. "
            "unit_price is the price at time of purchase (may differ from current product price). "
            "discount_pct ranges from 0-20% based on promotional activity. "
            "Grain: one row per (order, product) combination.",
        "tags": {"quality_tier": "bronze", "domain": "transaction", "data_product": "nexus_retail",
                 "owner": "data_engineering", "genie_domain": "sales_performance", "table_type": "fact"},
        "columns": {
            "line_id":      "Line item key in ORD-XXXXXXX-N format. Primary key.",
            "order_id":     "FK to bronze_orders.",
            "product_id":   "FK to bronze_products.",
            "quantity":     "Units ordered. Must be 1-999 (validated). Typical range 1-5.",
            "unit_price":   "Price per unit at time of order in USD. Must be > 0.",
            "discount_pct": "Promotional discount percentage applied (0-20%).",
            "line_total":   "quantity * unit_price * (1 - discount_pct/100). Summed to compute order_total.",
        },
    },
    f"`{C}`.online_retail_bronze.bronze_invoice_line_items": {
        "comment":
            "Raw invoice line items with tax calculation. 2-3 items per invoice. "
            "Tax rates: Electronics 10%, Food 0%, all other categories 8%. "
            "extended_price = line_total + tax_amount. "
            "Grain: one row per (invoice, order line item) combination.",
        "tags": {"quality_tier": "bronze", "domain": "transaction", "data_product": "nexus_retail",
                 "owner": "data_engineering", "genie_domain": "sales_performance", "table_type": "fact"},
        "columns": {
            "inv_line_id":    "Line item key combining invoice_id and line_id. Primary key.",
            "invoice_id":     "FK to bronze_invoices.",
            "product_id":     "FK to bronze_products.",
            "tax_rate":       "Applied tax rate: 0.10 (electronics), 0.00 (food), 0.08 (all others).",
            "tax_amount":     "Tax in USD = line_total * tax_rate.",
            "extended_price": "Total including tax = line_total + tax_amount.",
        },
    },
    f"`{C}`.online_retail_bronze.bronze_customer_demographics": {
        "comment":
            "Customer demographic profile data. One row per customer. "
            "annual_income_usd is PII-sensitive (quasi-identifier). "
            "nps_score (1-10) validated by SDP expect decorator. "
            "loyalty_tier drives CLV segmentation in gold_customer_lifetime_value. "
            "Grain: one row per customer.",
        "tags": {"quality_tier": "bronze", "domain": "customer", "contains_pii": "true",
                 "pii_classification": "quasi", "data_product": "nexus_retail", "owner": "data_engineering", "genie_domain": "customer_analytics", "table_type": "dimension"},
        "columns": {
            "customer_id":         "FK to bronze_customers.customer_id.",
            "age_bracket":         "Age group: 18-24 | 25-34 | 35-44 | 45-54 | 55+.",
            "income_bracket":      "Salary band: <$30K | $30K-$60K | $60K-$100K | $100K-$200K | $200K+.",
            "loyalty_tier":        "Program tier: Bronze (40%) | Silver (30%) | Gold (20%) | Platinum (10%).",
            "acquisition_channel": "How the customer joined: organic_search | paid_search | social_media | referral | email_campaign.",
            "annual_income_usd":   "PII:quasi — exact annual income in USD. Treated as sensitive financial data.",
            "nps_score":           "Net Promoter Score 1-10. Validated: must be in range 1-10.",
        },
    },
    f"`{C}`.online_retail_bronze.bronze_customer_addresses": {
        "comment":
            "Customer shipping and billing addresses. 1-2 addresses per customer (~800 total). "
            "address_line1 and postcode are PII. is_primary=TRUE for billing address. "
            "country_code FK to bronze_ref_countries. "
            "Grain: one row per address.",
        "tags": {"quality_tier": "bronze", "domain": "customer", "contains_pii": "true",
                 "pii_classification": "direct", "data_product": "nexus_retail", "owner": "data_engineering", "genie_domain": "customer_analytics", "table_type": "dimension"},
        "columns": {
            "customer_id":   "FK to bronze_customers.",
            "address_type":  "billing or shipping.",
            "address_line1": "PII:direct — street address. Redacted for non-owner at silver.",
            "city":          "City name.",
            "postcode":      "PII:direct — postal/ZIP code.",
            "country_code":  "ISO 3166-1 alpha-2 FK to bronze_ref_countries.",
            "is_primary":    "TRUE for the primary billing address.",
        },
    },
    f"`{C}`.online_retail_bronze.bronze_product_reviews": {
        "comment":
            "Customer product reviews with star ratings. 2,000 reviews from delivered orders. "
            "FAULT-PHON-* products (faulty_batch=TRUE) show a 1-2 star spike from Q4 2025 onward, "
            "corroborating the return rate anomaly visible in gold_return_analysis. "
            "Grain: one row per (order, product) review.",
        "tags": {"quality_tier": "bronze", "domain": "product", "data_product": "nexus_retail",
                 "owner": "data_engineering", "genie_domain": "returns_quality", "table_type": "fact"},
        "columns": {
            "review_id":         "Unique review identifier in REV-XXXXXXX format. Primary key.",
            "product_id":        "FK to bronze_products.",
            "rating":            "Star rating 1-5. FAULT-PHON-* products spike at 1-2 stars from Q4 2025.",
            "review_text":       "Free-text review content.",
            "verified_purchase": "TRUE — all reviews in this dataset are from confirmed deliveries.",
            "helpful_votes":     "Number of helpful votes. Faulty product reviews tend to have higher vote counts.",
            "is_faulty_product": "TRUE if the reviewed product is a FAULT-PHON-* defective SKU.",
        },
    },
    f"`{C}`.online_retail_bronze.bronze_customer_support_tickets": {
        "comment":
            "Customer support ticket log. 1,500 tickets over 24 months. "
            "Ticket volume spikes 3x in Q4 2025 (Oct 2025 – Jan 2026) due to faulty batch complaints. "
            "product_defect tickets rise from 10% to 40% during the spike period. "
            "Grain: one row per ticket.",
        "tags": {"quality_tier": "bronze", "domain": "customer", "data_product": "nexus_retail",
                 "contains_anomaly": "q4_2025_ticket_spike", "owner": "data_engineering", "genie_domain": "returns_quality", "table_type": "fact"},
        "columns": {
            "ticket_id":            "Unique ticket identifier in TKT-XXXXXXX format. Primary key.",
            "customer_id":          "FK to bronze_customers.",
            "order_id":             "Optional FK to bronze_orders. Set for ~30% of tickets.",
            "category":             "Issue type: product_defect | delivery_issue | billing_query | general_enquiry | return_request.",
            "priority":             "Severity: low | medium | high | critical. Escalated during Q4 2025 spike.",
            "status":               "Resolution status: open | in_progress | resolved | closed.",
            "is_faulty_escalation": "TRUE for product_defect tickets raised during the Q4 2025 faulty batch period.",
        },
    },

    # ── SILVER ────────────────────────────────────────────────────────────────
    f"`{C}`.online_retail_silver.silver_dim_date": {
        "comment":
            "Date spine dimension covering Sep 2024 – Dec 2027. "
            "Columns: date_key (YYYYMMDD int), calendar_date, year, quarter, month, "
            "week_of_year, day_of_week, is_weekend, is_q4 (Oct–Dec), fiscal_quarter (FQ1-FQ4). "
            "Join on calendar_date to add time attributes to any fact table. "
            "Grain: one row per calendar day.",
        "tags": {"quality_tier": "silver", "domain": "reference", "data_product": "nexus_retail",
                 "owner": "data_engineering", "table_type": "dimension", "genie_domain": "all"},
    },
    f"`{C}`.online_retail_silver.silver_dim_geography": {
        "comment":
            "Denormalised country → region geography dimension. "
            "Columns: country_code, country_name, region_id, region_name, super_region, "
            "regional_currency, country_currency, primary_language, continent_code. "
            "Used as FK lookup from silver_fact_orders and silver_fact_returns. "
            "Grain: one row per country.",
        "tags": {"quality_tier": "silver", "domain": "geography", "data_product": "nexus_retail",
                 "owner": "data_engineering", "table_type": "dimension", "genie_domain": "all"},
    },
    f"`{C}`.online_retail_silver.silver_dim_customers": {
        "comment":
            "Cleansed SCD-1 customer dimension enriched with demographics. "
            "PII column masks applied: full_name → first initial; email → ***@domain; "
            "phone → ***-***-XXXX; date_of_birth → year only. "
            "Only the catalog owner sees raw values. 3 duplicate emails quarantined. "
            "Regulatory: GDPR data minimisation applied via column masks. "
            "Grain: one row per customer (current state, SCD-1).",
        "tags": {"quality_tier": "silver", "domain": "customer", "contains_pii": "true",
                 "pii_masked": "true", "regulatory": "gdpr", "data_product": "nexus_retail",
                 "scd_type": "1", "owner": "data_engineering", "genie_domain": "customer_analytics", "table_type": "dimension"},
        "columns": {
            "customer_id":         "Business key CUST-XXXXXX. Primary key.",
            "full_name":           "PII:direct — Masked for non-owner: first initial + ***. GDPR Art.4(1).",
            "email":               "PII:direct — Masked: ***@domain.com. 3 duplicates quarantined by DQX.",
            "phone":               "PII:direct — Masked: ***-***-XXXX.",
            "date_of_birth":       "PII:direct — Masked to Jan 1 of birth year for non-owner. GDPR minimisation.",
            "customer_type":       "B2C or B2B.",
            "region_id":           "FK to silver_dim_geography.region_id.",
            "country_code":        "ISO 3166-1 alpha-2 FK to silver_dim_geography.",
            "age_bracket":         "Derived from date_of_birth: 18-24 | 25-34 | 35-44 | 45-54 | 55+.",
            "income_bracket":      "Salary band label: <$30K | $30K-$60K | $60K-$100K | $100K-$200K | $200K+.",
            "annual_income_usd":   "PII:quasi — annual income in USD. Sensitive financial attribute.",
            "loyalty_tier":        "Loyalty program tier: Bronze | Silver | Gold | Platinum.",
            "acquisition_channel": "Customer acquisition source: organic_search | paid_search | social_media | referral | email_campaign.",
            "nps_score":           "Net Promoter Score 1-10.",
            "__START_AT":          "SCD-1 sequence number (Unix ms). Tracks most recent change timestamp.",
        },
    },
    f"`{C}`.online_retail_silver.silver_dim_products": {
        "comment":
            "Enriched product dimension with flattened category + subcategory hierarchy and current price. "
            "faulty_batch=TRUE for 8 FAULT-PHON-* SKUs (product_idx 4-11). "
            "current_price is the most recent effective price from bronze_product_pricing. "
            "avg_return_rate is the category-level historical return rate benchmark. "
            "Grain: one row per product (streaming, latest state).",
        "tags": {"quality_tier": "silver", "domain": "product", "contains_faulty_batch": "true",
                 "data_product": "nexus_retail", "owner": "data_engineering", "genie_domain": "sales_performance", "table_type": "dimension"},
        "columns": {
            "product_id":      "Business key PROD-XXXXX. Primary key.",
            "sku":             "Stock keeping unit. FAULT-PHON-00XX for the 8 defective products.",
            "category_id":     "FK to category dimension. CAT-01 = Electronics.",
            "category_name":   "Human-readable category: Electronics | Apparel | Home & Living | etc.",
            "subcategory_id":  "FK to subcategory. SUB-0101 = Smartphones (contains all faulty batch items).",
            "subcategory_name":"Human-readable subcategory name.",
            "current_price":   "Most recent effective price in USD from product pricing history.",
            "base_price":      "Original listed price at product creation.",
            "faulty_batch":    "TRUE for 8 FAULT-PHON-* products. Root cause of Q3 2025 defect batch and Q4 2025 return spike.",
            "avg_return_rate": "Category-level historical return rate benchmark. Electronics = 0.08 (8%).",
        },
    },
    f"`{C}`.online_retail_silver.silver_fact_orders": {
        "comment":
            "Cleansed order facts enriched with customer geography. "
            "order_total validated against computed_total (sum of order_items) with ±$0.01 tolerance. "
            "Records with NULL customer or invalid status are dropped via @dp.expect_or_drop. "
            "region_id and region_name derived from customer billing country. "
            "Grain: one row per order.",
        "tags": {"quality_tier": "silver", "domain": "transaction", "data_product": "nexus_retail",
                 "owner": "data_engineering", "genie_domain": "sales_performance", "table_type": "fact"},
        "columns": {
            "order_id":       "Business key ORD-XXXXXXX. Primary key.",
            "customer_id":    "FK to silver_dim_customers.",
            "region_id":      "FK to silver_dim_geography derived from customer billing country.",
            "region_name":    "Human-readable region: AMER-North | EMEA-West | APAC-East | etc.",
            "super_region":   "High-level grouping: Americas | EMEA | Asia Pacific.",
            "order_total":    "Invoice total in USD. Validated against computed_total.",
            "computed_total": "SDP-computed total = sum(order_items.line_total). Used for reconciliation.",
            "item_count":     "Number of line items on this order.",
            "loyalty_tier":   "Customer loyalty tier at time of order. Denormalised for analytics performance.",
            "age_bracket":    "Customer age bracket at time of order. Denormalised for analytics performance.",
        },
    },
    f"`{C}`.online_retail_silver.silver_fact_invoices": {
        "comment":
            "Validated invoices. ~52 NULL invoice_total records from bronze are DROPPED here "
            "(captured in silver_dq_quarantine as rule: invoice_total_null). "
            "is_overdue flag = due_date < today AND status = pending. "
            "total_variance = invoice_total - order_total. Should be ≤ $0.01 when total_reconciled = TRUE. "
            "Grain: one row per invoice (with valid total only).",
        "tags": {"quality_tier": "silver", "domain": "transaction", "dq_note": "null_totals_dropped",
                 "data_product": "nexus_retail", "owner": "data_engineering", "genie_domain": "sales_performance", "table_type": "fact"},
        "columns": {
            "invoice_id":         "Business key INV-XXXXXXX. Primary key.",
            "invoice_number":     "Human-readable NR-YYYY-XXXXXX reference.",
            "customer_id":        "FK to silver_dim_customers (via order).",
            "invoice_total":      "Invoice total in USD. NULL records from bronze are excluded (dropped by SDP expect_or_drop).",
            "order_total":        "Corresponding order total for reconciliation comparison.",
            "total_variance":     "invoice_total minus order_total. Non-zero values indicate reconciliation gap.",
            "total_reconciled":   "TRUE when abs(total_variance) <= $0.01. Key AR quality metric.",
            "is_overdue":         "TRUE if due_date < today AND invoice_status = pending. Triggers AR alerts.",
        },
    },
    f"`{C}`.online_retail_silver.silver_fact_returns": {
        "comment":
            "Enriched return facts linked to orders, products, and the faulty_batch flag. "
            "41 returns have return_reason_code=faulty_product. "
            "faulty_batch_involved=TRUE for returns that included FAULT-PHON-* SKUs. "
            "return_category groups reason codes: quality_issue | fulfilment_error | customer_preference. "
            "Grain: one row per return request.",
        "tags": {"quality_tier": "silver", "domain": "transaction", "contains_anomaly": "q4_2025_return_spike",
                 "data_product": "nexus_retail", "owner": "data_engineering", "genie_domain": "returns_quality", "table_type": "fact"},
        "columns": {
            "return_id":              "Business key RET-XXXXXX. Primary key.",
            "order_id":               "FK to silver_fact_orders.",
            "region_id":              "FK to silver_dim_geography from originating order.",
            "return_reason_code":     "Customer-stated reason: faulty_product | wrong_item | changed_mind | damaged_in_transit | not_as_described.",
            "return_category":        "Derived grouping: quality_issue (faulty_product) | fulfilment_error (wrong_item, not_as_described) | customer_preference (changed_mind).",
            "faulty_batch_involved":  "TRUE for returns that included one or more FAULT-PHON-* defective products.",
            "returned_product_ids":   "Array of product_ids included in this return.",
            "returned_quantity":      "Total units returned across all items.",
        },
    },
    f"`{C}`.online_retail_silver.silver_dq_quarantine": {
        "comment":
            "DQX-style quarantine MV — captures all records failing critical data quality checks. "
            "Current contents: ~52 NULL invoice totals, 3 duplicate customer emails, "
            "~43 failed payments, 41 faulty_product return flags. Total: 139 records. "
            "Row count > 0 should trigger a data quality alert. "
            "Monitor this table after every pipeline run as a data health KPI.",
        "tags": {"quality_tier": "silver", "domain": "data_quality", "alert_on": "nonzero_row_count",
                 "data_product": "nexus_retail", "owner": "data_engineering", "genie_domain": "all", "table_type": "quality"},
    },

    # ── GOLD ─────────────────────────────────────────────────────────────────
    f"`{C}`.online_retail_gold.gold_category_sales": {
        "comment":
            "Category-level revenue aggregation by subcategory, region, and month. "
            "All dimension columns retained for full dashboard slice-and-dice flexibility. "
            "return_rate_pct = return_count / order_count * 100. "
            "Electronics shows anomalous return_rate_pct spike in Q4 2025 due to FAULT-PHON-* batch. "
            "Grain: category × subcategory × brand × region × month.",
        "tags": {"quality_tier": "gold", "domain": "product", "owner": "analytics",
                 "grain": "category_subcategory_region_month", "data_product": "nexus_retail"},
    },
    f"`{C}`.online_retail_gold.gold_customer_segment_sales": {
        "comment":
            "Customer demographic segment revenue breakdown. "
            "Dimensions: age_bracket × income_bracket × loyalty_tier × acquisition_channel × region × month. "
            "repeat_purchase_rate_pct = distinct returning customers / distinct total customers * 100. "
            "revenue_per_customer is a CLV proxy metric segmented by demographics. "
            "Grain: full demographic combination × region × month.",
        "tags": {"quality_tier": "gold", "domain": "customer", "owner": "analytics",
                 "grain": "segment_region_month", "data_product": "nexus_retail"},
    },
    f"`{C}`.online_retail_gold.gold_regional_performance": {
        "comment":
            "Regional performance dashboard: revenue, orders, returns, net revenue by country and month. "
            "APAC-East (REG-005) shows elevated return_rate_pct in Q4 2025 (faulty batch impact). "
            "net_revenue = gross_revenue - refund_total for the same period. "
            "Grain: region × country × month.",
        "tags": {"quality_tier": "gold", "domain": "geography", "owner": "analytics",
                 "key_story": "apac_east_return_spike", "grain": "region_country_month",
                 "data_product": "nexus_retail"},
    },
    f"`{C}`.online_retail_gold.gold_customer_lifetime_value": {
        "comment":
            "Customer-level CLV summary — one row per customer with full purchase history aggregated. "
            "clv_segment: High (total_revenue >= $1000 or Platinum) | Medium ($200-999) | Low (<$200) | Churned (180+ days inactive). "
            "orders_per_30_days measures purchase frequency normalised to a 30-day window. "
            "customer_return_rate_pct = returns / orders * 100 for this specific customer. "
            "Grain: customer_id.",
        "tags": {"quality_tier": "gold", "domain": "customer", "owner": "analytics",
                 "grain": "customer_id", "data_product": "nexus_retail"},
    },
    f"`{C}`.online_retail_gold.gold_return_analysis": {
        "comment":
            "Return analysis by product × reason code × week. "
            "faulty_batch=TRUE products (FAULT-PHON-*) show return_rate_pct > 40% in Q4 2025 "
            "versus 4-8% for normal products — the key data quality anomaly signal. "
            "avg_days_to_return measures lag between order and return initiation. "
            "Grain: product_id × return_reason_code × return_week.",
        "tags": {"quality_tier": "gold", "domain": "transaction", "owner": "analytics",
                 "key_story": "faulty_batch_return_anomaly", "grain": "product_reason_week",
                 "data_product": "nexus_retail"},
    },
    f"`{C}`.online_retail_gold.gold_daily_revenue": {
        "comment":
            "Daily revenue by channel and region — backbone for the AI/BI time-series dashboard. "
            "new_customers = customers placing their first-ever order on this date. "
            "returning_customers = customers who have ordered before this date. "
            "Q4 seasonal spike (Oct-Dec) is clearly visible: ~40-50% higher order volumes. "
            "Grain: date × channel × region_id.",
        "tags": {"quality_tier": "gold", "domain": "transaction", "owner": "analytics",
                 "grain": "date_channel_region", "data_product": "nexus_retail"},
    },

    # ── METRICS ──────────────────────────────────────────────────────────────
    f"`{C}`.online_retail_metrics.mv_category_revenue": {
        "comment":
            "Pre-aggregated category revenue Materialized View. "
            "Source: gold_category_sales — aggregated by category × subcategory × region × month. "
            "Refreshed on every pipeline run. Used as primary Genie data source for Sales Performance page. "
            "contains_faulty_products flag surfaces Electronics anomaly in Q4 2025.",
        "tags": {"quality_tier": "gold", "domain": "product", "genie_page": "sales_performance",
                 "data_product": "nexus_retail", "owner": "analytics", "genie_domain": "sales_performance", "table_type": "fact"},
    },
    f"`{C}`.online_retail_metrics.mv_customer_demo_sales": {
        "comment":
            "Pre-aggregated customer demographic sales Materialized View. "
            "Source: gold_customer_segment_sales — aggregated by age_bracket × income_bracket × loyalty_tier × region × month. "
            "Genie data source for Customer Analytics page. "
            "avg_repeat_rate_pct is the key engagement metric per segment.",
        "tags": {"quality_tier": "gold", "domain": "customer", "genie_page": "customer_analytics",
                 "data_product": "nexus_retail", "owner": "analytics", "genie_domain": "customer_analytics", "table_type": "fact"},
    },
    f"`{C}`.online_retail_metrics.mv_regional_orders": {
        "comment":
            "Pre-aggregated regional order and return performance Materialized View. "
            "Source: gold_regional_performance. Shows APAC-East Q4 2025 return spike. "
            "Genie data source for Returns and Quality page. "
            "return_rate_pct and cancellation_rate_pct are the key quality KPIs.",
        "tags": {"quality_tier": "gold", "domain": "geography", "genie_page": "returns_quality",
                 "key_story": "apac_east_return_spike", "data_product": "nexus_retail", "owner": "analytics", "genie_domain": "returns_quality", "table_type": "fact"},
    },
    f"`{C}`.online_retail_metrics.metrics_sales_kpis": {
        "comment":
            "Sales KPI Metric View (WITH METRICS LANGUAGE YAML). "
            "Source: gold_daily_revenue. Dimensions: Sale Month, Sale Quarter, Channel, Region, Super Region. "
            "Measures: Gross Revenue, Order Count, Avg Order Value, Unique Customers, New Customers, Returning Customers. "
            "Query using MEASURE() function. Genie: Sales Performance page.",
        "tags": {"quality_tier": "gold", "domain": "transaction", "semantic_layer": "metric_view",
                 "genie_page": "sales_performance", "data_product": "nexus_retail", "owner": "analytics", "genie_domain": "sales_performance", "table_type": "semantic"},
    },
    f"`{C}`.online_retail_metrics.metrics_customer_kpis": {
        "comment":
            "Customer KPI Metric View (WITH METRICS LANGUAGE YAML). "
            "Source: mv_customer_demo_sales. Dimensions: Age Bracket, Income Bracket, Loyalty Tier, Acquisition Channel, Region, Month. "
            "Measures: Revenue, Customer Count, Avg Order Value, Repeat Purchase Rate. "
            "Query using MEASURE() function. Genie: Customer Analytics page.",
        "tags": {"quality_tier": "gold", "domain": "customer", "semantic_layer": "metric_view",
                 "genie_page": "customer_analytics", "data_product": "nexus_retail", "owner": "analytics", "genie_domain": "customer_analytics", "table_type": "semantic"},
    },
    f"`{C}`.online_retail_metrics.metrics_product_kpis": {
        "comment":
            "Product/Returns KPI Metric View (WITH METRICS LANGUAGE YAML). "
            "Source: gold_return_analysis. Dimensions: Category, Subcategory, Return Reason, Faulty Batch, Return Month. "
            "Measures: Return Count, Total Refund, Return Rate. "
            "FAULT-PHON-* products show Return Rate > 40%% in Q4 2025 (normal = 4-8%%). "
            "Query using MEASURE() function. Genie: Returns and Quality page.",
        "tags": {"quality_tier": "gold", "domain": "product", "semantic_layer": "metric_view",
                 "genie_page": "returns_quality", "key_story": "faulty_batch_anomaly",
                 "data_product": "nexus_retail", "owner": "analytics", "genie_domain": "returns_quality", "table_type": "semantic"},
    },
}

# ── Column-level tags for PII columns ─────────────────────────────────────────
PII_COLUMN_TAGS = {
    f"`{C}`.online_retail_bronze.bronze_customers": {
        "full_name":    {"pii": "direct", "pii_type": "full_name",    "regulatory": "gdpr"},
        "email":        {"pii": "direct", "pii_type": "email",        "regulatory": "gdpr"},
        "phone":        {"pii": "direct", "pii_type": "phone",        "regulatory": "gdpr"},
        "date_of_birth":{"pii": "direct", "pii_type": "date_of_birth","regulatory": "gdpr"},
    },
    f"`{C}`.online_retail_bronze.bronze_customer_addresses": {
        "address_line1":{"pii": "direct", "pii_type": "address",      "regulatory": "gdpr"},
        "postcode":     {"pii": "direct", "pii_type": "postcode",     "regulatory": "gdpr"},
    },
    f"`{C}`.online_retail_bronze.bronze_customer_demographics": {
        "annual_income_usd":{"pii": "quasi", "pii_type": "financial", "regulatory": "gdpr"},
    },
    f"`{C}`.online_retail_silver.silver_dim_customers": {
        "full_name":         {"pii": "direct",   "pii_type": "full_name",    "mask_applied": "true", "regulatory": "gdpr"},
        "email":             {"pii": "direct",   "pii_type": "email",        "mask_applied": "true", "regulatory": "gdpr"},
        "phone":             {"pii": "direct",   "pii_type": "phone",        "mask_applied": "true", "regulatory": "gdpr"},
        "date_of_birth":     {"pii": "direct",   "pii_type": "date_of_birth","mask_applied": "true", "regulatory": "gdpr"},
        "annual_income_usd": {"pii": "quasi",    "pii_type": "financial",    "mask_applied": "false","regulatory": "gdpr"},
    },
}

# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    print(f"\n{'='*60}")
    print(f"NexusRetail Governance Setup")
    print(f"  Catalog   : {CATALOG}")
    print(f"  Warehouse : {WAREHOUSE_ID}")
    print(f"  Owner     : {OWNER_USER}")
    print(f"{'='*60}\n")

    # 1. PII column mask functions
    print("1. PII column mask functions...")
    sql("mask_full_name", f"""
CREATE OR REPLACE FUNCTION `{C}`.online_retail_silver.mask_full_name(raw_name STRING)
  RETURNS STRING
  COMMENT 'PII mask: first initial + *** for non-owner. Full name for {OWNER_USER} only. GDPR Art.4(1).'
  RETURN CASE WHEN current_user() = '{OWNER_USER}' THEN raw_name
              ELSE CONCAT(LEFT(raw_name,1),'***') END
""")
    sql("mask_email", f"""
CREATE OR REPLACE FUNCTION `{C}`.online_retail_silver.mask_email(raw_email STRING)
  RETURNS STRING
  COMMENT 'PII mask: ***@domain.com for non-owner. Full email for {OWNER_USER} only.'
  RETURN CASE WHEN current_user() = '{OWNER_USER}' THEN raw_email
              ELSE CONCAT('***@', ELEMENT_AT(SPLIT(raw_email,'@'),2)) END
""")
    sql("mask_phone", f"""
CREATE OR REPLACE FUNCTION `{C}`.online_retail_silver.mask_phone(raw_phone STRING)
  RETURNS STRING
  COMMENT 'PII mask: ***-***-XXXX for non-owner. Full number for {OWNER_USER} only.'
  RETURN CASE WHEN current_user() = '{OWNER_USER}' THEN raw_phone
              ELSE CONCAT('***-***-',RIGHT(raw_phone,4)) END
""")
    sql("mask_date_of_birth", f"""
CREATE OR REPLACE FUNCTION `{C}`.online_retail_silver.mask_date_of_birth(dob DATE)
  RETURNS DATE
  COMMENT 'PII mask: truncates to Jan 1 of birth year for non-owner. GDPR data minimisation.'
  RETURN CASE WHEN current_user() = '{OWNER_USER}' THEN dob
              ELSE DATE_TRUNC('YEAR',dob) END
""")

    # 2. Apply column masks to silver_dim_customers
    print("\n2. Applying column masks...")
    for col, fn in [
        ("full_name",     "mask_full_name"),
        ("email",         "mask_email"),
        ("phone",         "mask_phone"),
        ("date_of_birth", "mask_date_of_birth"),
    ]:
        sql(f"mask {col}", f"""
ALTER TABLE `{C}`.online_retail_silver.silver_dim_customers
  ALTER COLUMN {col} SET MASK `{C}`.online_retail_silver.{fn}
""", allow_fail=True)

    # 3. Create metrics schema objects (UC MVs + Metric Views)
    #    Must run BEFORE table comments/tags — can't tag tables that don't exist yet.
    print("\n3. Creating online_retail_metrics objects...")

    sql("mv_category_revenue", f"""
CREATE OR REPLACE MATERIALIZED VIEW `{C}`.online_retail_metrics.mv_category_revenue
COMMENT 'Pre-aggregated category revenue with return rates. Refreshed on pipeline run.
Source for Genie Sales Performance page. Grain: category x subcategory x region x month.'
TBLPROPERTIES ('quality_tier'='gold','domain'='product','genie_page'='sales_performance','data_product'='nexus_retail')
AS
SELECT category_id, category_name, subcategory_name, region_name, super_region, sale_month,
  SUM(order_count) AS order_count, SUM(units_sold) AS units_sold,
  SUM(gross_revenue) AS gross_revenue, SUM(net_revenue) AS net_revenue,
  SUM(total_discount) AS total_discount, SUM(return_count) AS return_count,
  SUM(refund_total) AS refund_total,
  ROUND(AVG(return_rate_pct),2) AS avg_return_rate_pct,
  MAX(contains_faulty_products) AS contains_faulty_products
FROM `{C}`.online_retail_gold.gold_category_sales
GROUP BY category_id, category_name, subcategory_name, region_name, super_region, sale_month
""")

    sql("mv_customer_demo_sales", f"""
CREATE OR REPLACE MATERIALIZED VIEW `{C}`.online_retail_metrics.mv_customer_demo_sales
COMMENT 'Pre-aggregated customer demographic sales. Refreshed on pipeline run.
Source for Genie Customer Analytics page. Grain: age_bracket x income_bracket x loyalty_tier x region x month.'
TBLPROPERTIES ('quality_tier'='gold','domain'='customer','genie_page'='customer_analytics','data_product'='nexus_retail')
AS
SELECT age_bracket, income_bracket, loyalty_tier, acquisition_channel, customer_type,
  region_name, super_region, sale_month,
  SUM(unique_customers) AS unique_customers, SUM(order_count) AS order_count,
  SUM(revenue) AS revenue, ROUND(AVG(avg_order_value),2) AS avg_order_value,
  SUM(repeat_customers) AS repeat_customers,
  ROUND(AVG(repeat_purchase_rate_pct),2) AS avg_repeat_rate_pct,
  ROUND(AVG(revenue_per_customer),2) AS avg_revenue_per_customer
FROM `{C}`.online_retail_gold.gold_customer_segment_sales
GROUP BY age_bracket, income_bracket, loyalty_tier, acquisition_channel, customer_type,
         region_name, super_region, sale_month
""")

    sql("mv_regional_orders", f"""
CREATE OR REPLACE MATERIALIZED VIEW `{C}`.online_retail_metrics.mv_regional_orders
COMMENT 'Pre-aggregated regional order and return performance. Refreshed on pipeline run.
Shows APAC-East Q4 2025 return spike. Source for Genie Returns and Quality page.
Grain: region x country x month.'
TBLPROPERTIES ('quality_tier'='gold','domain'='geography','genie_page'='returns_quality',
               'key_story'='apac_east_return_spike','data_product'='nexus_retail')
AS
SELECT region_id, region_name, super_region, country_code, country_name, regional_currency, sale_month,
  SUM(order_count) AS order_count, SUM(unique_customers) AS unique_customers,
  SUM(gross_revenue) AS gross_revenue, SUM(return_count) AS return_count,
  SUM(refund_total) AS refund_total, SUM(net_revenue) AS net_revenue,
  ROUND(AVG(return_rate_pct),2) AS return_rate_pct,
  ROUND(AVG(avg_order_value),2) AS avg_order_value,
  SUM(cancelled_orders) AS cancelled_orders,
  ROUND(AVG(cancellation_rate_pct),2) AS cancellation_rate_pct
FROM `{C}`.online_retail_gold.gold_regional_performance
GROUP BY region_id, region_name, super_region, country_code, country_name, regional_currency, sale_month
""")

    # Metric Views need async execution (timeout > 50s not allowed by sync API)
    import time as _time
    from databricks.sdk.service.sql import StatementState as _SS

    def _sql_async(label: str, stmt: str):
        try:
            r = w.statement_execution.execute_statement(
                warehouse_id=WAREHOUSE_ID, statement=stmt.strip(), wait_timeout="0s")
            sid = r.statement_id
            for _ in range(30):
                _time.sleep(3)
                r2 = w.statement_execution.get_statement(sid)
                st = r2.status.state
                if st in (_SS.SUCCEEDED, _SS.CLOSED):
                    print(f"  ✓  {label}"); return True
                if st in (_SS.FAILED, _SS.CANCELED):
                    err = r2.status.error.message[:200] if r2.status.error else "?"
                    print(f"  ✗  {label}  {err}"); return False
            print(f"  ⏰  {label}  timeout"); return False
        except Exception as e:
            print(f"  ✗  {label}  {str(e)[:120]}"); return False

    _sql_async("metrics_sales_kpis", f"""
CREATE OR REPLACE VIEW `{C}`.online_retail_metrics.metrics_sales_kpis
  WITH METRICS LANGUAGE YAML
  COMMENT 'NexusRetail Sales KPIs. Dimensions: Sale Month, Sale Quarter, Channel, Region, Super Region.
Measures: Gross Revenue, Order Count, Avg Order Value, Unique Customers, New Customers, Returning Customers.
Use MEASURE() function. Genie: Sales Performance page.'
AS $$
  version: 1.1
  source: {C}.online_retail_gold.gold_daily_revenue
  dimensions:
    - name: Sale Month
      expr: DATE_TRUNC('MONTH', sale_date)
      comment: "Month of sale — primary time dimension"
    - name: Sale Quarter
      expr: DATE_TRUNC('QUARTER', sale_date)
      comment: "Quarter. Q4 Oct-Dec is the seasonal peak"
    - name: Channel
      expr: channel
      comment: "web | mobile | partner_api"
    - name: Region
      expr: region_name
      comment: "7 global sales regions"
    - name: Super Region
      expr: super_region
      comment: "Americas | EMEA | Asia Pacific"
  measures:
    - name: Gross Revenue
      expr: SUM(gross_revenue)
      comment: "Total revenue. Primary measure."
    - name: Order Count
      expr: SUM(order_count)
    - name: Avg Order Value
      expr: SUM(gross_revenue) / NULLIF(SUM(order_count), 0)
    - name: Unique Customers
      expr: SUM(unique_customers)
    - name: New Customers
      expr: SUM(new_customers)
      comment: "Customers placing their first-ever order"
    - name: Returning Customers
      expr: SUM(returning_customers)
$$""")

    _sql_async("metrics_customer_kpis", f"""
CREATE OR REPLACE VIEW `{C}`.online_retail_metrics.metrics_customer_kpis
  WITH METRICS LANGUAGE YAML
  COMMENT 'NexusRetail Customer KPIs. Dimensions: Age Bracket, Income Bracket, Loyalty Tier, Acquisition Channel, Region, Month.
Measures: Revenue, Customer Count, Avg Order Value, Repeat Purchase Rate.
Use MEASURE() function. Genie: Customer Analytics page.'
AS $$
  version: 1.1
  source: {C}.online_retail_metrics.mv_customer_demo_sales
  dimensions:
    - name: Age Bracket
      expr: age_bracket
      comment: "18-24 | 25-34 | 35-44 | 45-54 | 55+"
    - name: Income Bracket
      expr: income_bracket
    - name: Loyalty Tier
      expr: loyalty_tier
      comment: "Bronze | Silver | Gold | Platinum"
    - name: Acquisition Channel
      expr: acquisition_channel
    - name: Region
      expr: region_name
    - name: Month
      expr: sale_month
  measures:
    - name: Revenue
      expr: SUM(revenue)
    - name: Customer Count
      expr: SUM(unique_customers)
    - name: Avg Order Value
      expr: SUM(revenue) / NULLIF(SUM(order_count), 0)
    - name: Repeat Purchase Rate
      expr: AVG(avg_repeat_rate_pct)
      comment: "% customers with more than one order"
$$""")

    _sql_async("metrics_product_kpis", f"""
CREATE OR REPLACE VIEW `{C}`.online_retail_metrics.metrics_product_kpis
  WITH METRICS LANGUAGE YAML
  COMMENT 'NexusRetail Product/Returns KPIs. FAULT-PHON-* return_rate exceeds 40%% in Q4 2025.
Dimensions: Category, Subcategory, Return Reason, Faulty Batch, Return Month.
Measures: Return Count, Total Refund, Return Rate.
Use MEASURE() function. Genie: Returns and Quality page.'
AS $$
  version: 1.1
  source: {C}.online_retail_gold.gold_return_analysis
  dimensions:
    - name: Category
      expr: category_name
    - name: Subcategory
      expr: subcategory_name
    - name: Return Reason
      expr: return_reason_code
      comment: "faulty_product | wrong_item | changed_mind | damaged_in_transit"
    - name: Faulty Batch
      expr: "CASE WHEN faulty_batch THEN 'Defective (FAULT-PHON-*)' ELSE 'Normal' END"
      comment: "TRUE for 8 defective SKUs. Filter here to isolate the Q3 2025 root cause."
    - name: Return Month
      expr: DATE_TRUNC('MONTH', return_week)
  measures:
    - name: Return Count
      expr: SUM(return_count)
    - name: Total Refund
      expr: SUM(total_refund_amount)
    - name: Return Rate
      expr: AVG(return_rate_pct)
      comment: "Normal 4-8%%. FAULT-* shows >40%%. Alert threshold 25%%."
$$""")

    # 4. Catalog comment + schema comments + schema tags
    print("\n4. Catalog, schema comments and schema tags...")
    sql("catalog comment",
        f"COMMENT ON CATALOG `{C}` IS '{CATALOG_COMMENT}'", allow_fail=True)

    for schema_fqn, comment in SCHEMA_COMMENTS.items():
        sql(f"schema comment {schema_fqn.split('.')[-1]}",
            f"COMMENT ON SCHEMA {schema_fqn} IS '{comment}'", allow_fail=True)

    for schema_fqn, tags in SCHEMA_TAGS.items():
        apply_tags("SCHEMA", schema_fqn, tags)

    # 5. Table comments, tags, column comments
    print("\n5. Table comments and tags...")
    # Determine which tables are MVs/Views (can't do ALTER COLUMN on them)
    MV_TYPES = {"MATERIALIZED_VIEW", "VIEW", "METRIC_VIEW"}

    for full_name, meta in TABLE_METADATA.items():
        table_comment = meta.get("comment", "").replace("'", "\\'")
        # Apply table comment
        sql(f"comment {full_name.split('.')[-1].strip('`')}",
            f"COMMENT ON TABLE {full_name} IS '{table_comment}'", allow_fail=True)

        # Apply table tags
        if meta.get("tags"):
            apply_tags("TABLE", full_name, meta["tags"])

        # Apply column comments (Streaming Tables only)
        schema = full_name.split(".")[1].strip("`")
        is_mv = (schema in ("online_retail_gold", "online_retail_metrics"))
        if not is_mv and meta.get("columns"):
            for col_name, col_comment_text in meta["columns"].items():
                comment_escaped = col_comment_text.replace("'", "\\'")
                col_comment(full_name, col_name, comment_escaped)

    # 6. PII column tags
    print("\n6. PII column tags...")
    for table, columns in PII_COLUMN_TAGS.items():
        for col_name, tags in columns.items():
            col_tag(table, col_name, tags)

    # 7. Grants
    print("\n7. Grants...")
    for stmt in [
        f"GRANT USE CATALOG ON CATALOG `{C}` TO `{OWNER_USER}`",
        f"GRANT USE SCHEMA ON SCHEMA `{C}`.online_retail_bronze TO `{OWNER_USER}`",
        f"GRANT USE SCHEMA ON SCHEMA `{C}`.online_retail_silver TO `{OWNER_USER}`",
        f"GRANT USE SCHEMA ON SCHEMA `{C}`.online_retail_gold TO `{OWNER_USER}`",
        f"GRANT USE SCHEMA ON SCHEMA `{C}`.online_retail_metrics TO `{OWNER_USER}`",
        f"GRANT SELECT ON SCHEMA `{C}`.online_retail_silver TO `{OWNER_USER}`",
        f"GRANT SELECT ON SCHEMA `{C}`.online_retail_gold TO `{OWNER_USER}`",
        f"GRANT SELECT ON SCHEMA `{C}`.online_retail_metrics TO `{OWNER_USER}`",
    ]:
        sql(f"grant {stmt.split('ON')[1].strip()[:50]}", stmt, allow_fail=True)

    print(f"\n{'='*60}")
    print("Governance setup complete.")
    print(f"  Catalog : https://YOUR-WORKSPACE/#explore/data/{C}")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
