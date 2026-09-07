"""
NexusRetail Governance Setup — runs all DDL statements via Databricks SDK

Usage:
  python3 governance/run_governance.py

Configuration (set these before running):
  WAREHOUSE  — SQL warehouse ID (from SQL Warehouses → Connection Details)
  CATALOG    — UC catalog name (must already exist)
  OWNER_USER — email of the user who gets unmasked PII access

The script uses your active Databricks CLI profile (or DATABRICKS_HOST +
DATABRICKS_TOKEN env vars) — same credentials as `databricks bundle deploy`.
"""
import os
from databricks.sdk import WorkspaceClient

# ── Configure for your workspace ───────────────────────────────────────────
WAREHOUSE  = os.environ.get("WAREHOUSE_ID",  "your_warehouse_id")   # <-- set this
CATALOG    = os.environ.get("CATALOG",        "your_catalog_name")   # <-- set this
OWNER_USER = os.environ.get("OWNER_USER",     "your.email@company.com")  # <-- set this
# ───────────────────────────────────────────────────────────────────────────
# Or pass as environment variables:
#   WAREHOUSE_ID=abc123 CATALOG=my_catalog OWNER_USER=me@co.com python3 run_governance.py

if "your_" in WAREHOUSE or "your_" in CATALOG or "your." in OWNER_USER:
    print("⚠  Update WAREHOUSE, CATALOG, and OWNER_USER at the top of this script")
    print("   or pass them as environment variables:")
    print("   WAREHOUSE_ID=abc CATALOG=my_cat OWNER_USER=me@co.com python3 run_governance.py")
    raise SystemExit(1)

_w = WorkspaceClient()

def run(label: str, sql: str, allow_fail: bool = False) -> bool:
    try:
        resp  = _w.statement_execution.execute_statement(
            warehouse_id=WAREHOUSE, statement=sql.strip(), wait_timeout="30s")
        state = str(resp.status.state) if resp.status else "UNKNOWN"
        err   = resp.status.error.message[:120] if resp.status and resp.status.error else ""
        ok    = "SUCCEEDED" in state or "CLOSED" in state
    except Exception as e:
        state, err, ok = "EXC", str(e)[:120], False

    icon = "✓" if ok else ("⚠" if allow_fail else "✗")
    if not ok: print(f"  {icon}  {label}  [{state}] {err}")
    else:      print(f"  {icon}  {label}")
    return ok

print("\n═══ NexusRetail Governance Setup ═══\n")

# ── Section 1: PII Column Mask Functions ──────────────────────────────────────
print("1. Creating PII column mask functions...")

run("mask_full_name", f"""
CREATE OR REPLACE FUNCTION `{CATALOG}`.online_retail_silver.mask_full_name(raw_name STRING)
  RETURNS STRING
  COMMENT 'PII mask: first initial only for non-owner. Full value for {OWNER_USER}.'
  RETURN CASE WHEN current_user() = '{OWNER_USER}' THEN raw_name
              ELSE CONCAT(LEFT(raw_name,1),'***') END
""")

run("mask_email", f"""
CREATE OR REPLACE FUNCTION `{CATALOG}`.online_retail_silver.mask_email(raw_email STRING)
  RETURNS STRING
  COMMENT 'PII mask: ***@domain.com for non-owner. Full email for {OWNER_USER}.'
  RETURN CASE WHEN current_user() = '{OWNER_USER}' THEN raw_email
              ELSE CONCAT('***@', ELEMENT_AT(SPLIT(raw_email,'@'),2)) END
""")

run("mask_phone", f"""
CREATE OR REPLACE FUNCTION `{CATALOG}`.online_retail_silver.mask_phone(raw_phone STRING)
  RETURNS STRING
  COMMENT 'PII mask: ***-***-XXXX for non-owner. Full number for {OWNER_USER}.'
  RETURN CASE WHEN current_user() = '{OWNER_USER}' THEN raw_phone
              ELSE CONCAT('***-***-', RIGHT(raw_phone,4)) END
""")

run("mask_date_of_birth", f"""
CREATE OR REPLACE FUNCTION `{CATALOG}`.online_retail_silver.mask_date_of_birth(dob DATE)
  RETURNS DATE
  COMMENT 'PII mask: truncate to Jan 1 of birth year for non-owner. Full DOB for {OWNER_USER}.'
  RETURN CASE WHEN current_user() = '{OWNER_USER}' THEN dob
              ELSE DATE_TRUNC('YEAR', dob) END
""")

# ── Section 2: Apply Column Masks ─────────────────────────────────────────────
print("\n2. Applying column masks to silver_dim_customers...")

for col, fn in [
    ("full_name",     "mask_full_name(full_name)"),
    ("email",         "mask_email(email)"),
    ("phone",         "mask_phone(phone)"),
    ("date_of_birth", "mask_date_of_birth(date_of_birth)"),
]:
    run(f"mask {col}", f"""
ALTER TABLE `{CATALOG}`.online_retail_silver.silver_dim_customers
  ALTER COLUMN {col} SET MASK `{CATALOG}`.online_retail_silver.{fn.split('(')[0]}
  USING COLUMNS ({col})
    """, allow_fail=True)

# ── Section 3: UC Tags ────────────────────────────────────────────────────────
print("\n3. Applying UC tags...")

tag_statements = [
    ("bronze_customers pii tags",
     f"ALTER TABLE `{CATALOG}`.online_retail_bronze.bronze_customers SET TAGS ('quality_tier'='bronze','domain'='customer','contains_pii'='true','pii_class'='direct','regulatory'='gdpr','data_product'='nexus_retail')"),
    ("bronze_customers full_name column tag",
     f"ALTER TABLE `{CATALOG}`.online_retail_bronze.bronze_customers ALTER COLUMN full_name SET TAGS ('pii'='full_name','pii_class'='direct')"),
    ("bronze_customers email column tag",
     f"ALTER TABLE `{CATALOG}`.online_retail_bronze.bronze_customers ALTER COLUMN email SET TAGS ('pii'='email','pii_class'='direct')"),
    ("bronze_customers phone column tag",
     f"ALTER TABLE `{CATALOG}`.online_retail_bronze.bronze_customers ALTER COLUMN phone SET TAGS ('pii'='phone','pii_class'='direct')"),
    ("bronze_customers dob column tag",
     f"ALTER TABLE `{CATALOG}`.online_retail_bronze.bronze_customers ALTER COLUMN date_of_birth SET TAGS ('pii'='date_of_birth','pii_class'='direct')"),
    ("bronze_orders tags",
     f"ALTER TABLE `{CATALOG}`.online_retail_bronze.bronze_orders SET TAGS ('quality_tier'='bronze','domain'='transaction','data_product'='nexus_retail')"),
    ("bronze_invoices tags",
     f"ALTER TABLE `{CATALOG}`.online_retail_bronze.bronze_invoices SET TAGS ('quality_tier'='bronze','domain'='transaction','dq_known_issue'='null_totals_intentional')"),
    ("bronze_products tags",
     f"ALTER TABLE `{CATALOG}`.online_retail_bronze.bronze_products SET TAGS ('quality_tier'='bronze','domain'='product','contains_faulty_batch'='true')"),
    ("bronze_products faulty_batch column tag",
     f"ALTER TABLE `{CATALOG}`.online_retail_bronze.bronze_products ALTER COLUMN faulty_batch SET TAGS ('governance'='quality_indicator','story'='q3_2025_defect_batch')"),
    ("bronze_returns tags",
     f"ALTER TABLE `{CATALOG}`.online_retail_bronze.bronze_returns SET TAGS ('quality_tier'='bronze','domain'='transaction','contains_anomaly'='q4_2025_return_spike')"),
    ("silver_dim_customers tags",
     f"ALTER TABLE `{CATALOG}`.online_retail_silver.silver_dim_customers SET TAGS ('quality_tier'='silver','domain'='customer','contains_pii'='true','pii_class'='direct','regulatory'='gdpr','pii_masked'='true','data_product'='nexus_retail')"),
    ("silver_fact_orders tags",
     f"ALTER TABLE `{CATALOG}`.online_retail_silver.silver_fact_orders SET TAGS ('quality_tier'='silver','domain'='transaction','data_product'='nexus_retail')"),
    ("silver_fact_invoices tags",
     f"ALTER TABLE `{CATALOG}`.online_retail_silver.silver_fact_invoices SET TAGS ('quality_tier'='silver','domain'='transaction','dq_note'='null_totals_dropped')"),
    ("silver_fact_returns tags",
     f"ALTER TABLE `{CATALOG}`.online_retail_silver.silver_fact_returns SET TAGS ('quality_tier'='silver','domain'='transaction','contains_anomaly'='q4_2025_return_spike')"),
    ("silver_dq_quarantine tags",
     f"ALTER TABLE `{CATALOG}`.online_retail_silver.silver_dq_quarantine SET TAGS ('quality_tier'='silver','domain'='data_quality','alert_on'='nonzero_row_count')"),
    ("silver_dim_products tags",
     f"ALTER TABLE `{CATALOG}`.online_retail_silver.silver_dim_products SET TAGS ('quality_tier'='silver','domain'='product','contains_faulty_batch'='true')"),
    ("gold_category_sales tags",
     f"ALTER TABLE `{CATALOG}`.online_retail_gold.gold_category_sales SET TAGS ('quality_tier'='gold','domain'='product','owner'='analytics','grain'='category_subcategory_region_month')"),
    ("gold_customer_segment_sales tags",
     f"ALTER TABLE `{CATALOG}`.online_retail_gold.gold_customer_segment_sales SET TAGS ('quality_tier'='gold','domain'='customer','owner'='analytics')"),
    ("gold_regional_performance tags",
     f"ALTER TABLE `{CATALOG}`.online_retail_gold.gold_regional_performance SET TAGS ('quality_tier'='gold','domain'='geography','key_story'='apac_east_return_spike')"),
    ("gold_customer_lifetime_value tags",
     f"ALTER TABLE `{CATALOG}`.online_retail_gold.gold_customer_lifetime_value SET TAGS ('quality_tier'='gold','domain'='customer','grain'='customer_id')"),
    ("gold_return_analysis tags",
     f"ALTER TABLE `{CATALOG}`.online_retail_gold.gold_return_analysis SET TAGS ('quality_tier'='gold','domain'='transaction','key_story'='faulty_batch_return_anomaly')"),
    ("gold_daily_revenue tags",
     f"ALTER TABLE `{CATALOG}`.online_retail_gold.gold_daily_revenue SET TAGS ('quality_tier'='gold','domain'='transaction','grain'='date_channel_region')"),
]

for label, sql in tag_statements:
    run(label, sql, allow_fail=True)

# ── Section 4: Table & Column Comments ───────────────────────────────────────
print("\n4. Setting table and column comments...")

comments = [
    ("silver_dim_customers full_name comment",
     f"ALTER TABLE `{CATALOG}`.online_retail_silver.silver_dim_customers ALTER COLUMN full_name COMMENT 'PII:direct — Masked for non-owner roles: shows first initial only. GDPR Art.4(1).'"),
    ("silver_dim_customers email comment",
     f"ALTER TABLE `{CATALOG}`.online_retail_silver.silver_dim_customers ALTER COLUMN email COMMENT 'PII:direct — Masked: ***@domain.com for non-owner. 3 duplicates captured in silver_dq_quarantine.'"),
    ("silver_dim_customers phone comment",
     f"ALTER TABLE `{CATALOG}`.online_retail_silver.silver_dim_customers ALTER COLUMN phone COMMENT 'PII:direct — Masked: ***-***-XXXX for non-owner.'"),
    ("silver_dim_customers dob comment",
     f"ALTER TABLE `{CATALOG}`.online_retail_silver.silver_dim_customers ALTER COLUMN date_of_birth COMMENT 'PII:direct — Masked to year (Jan 1) for non-owner. GDPR data minimisation.'"),
    ("silver_dim_products faulty_batch comment",
     f"ALTER TABLE `{CATALOG}`.online_retail_silver.silver_dim_products ALTER COLUMN faulty_batch COMMENT 'TRUE for 8 FAULT-PHON-* products (product_idx 4-11) from Q3 2025 defective batch. Root cause of Q4 2025 return spike in APAC-East.'"),
    ("silver_fact_invoices invoice_total comment",
     f"ALTER TABLE `{CATALOG}`.online_retail_silver.silver_fact_invoices ALTER COLUMN invoice_total COMMENT 'Invoice total USD. ~52 NULL records from bronze dropped by expect_or_drop at silver ingestion (visible in silver_dq_quarantine).'"),
    ("gold_return_analysis faulty_batch comment",
     f"ALTER TABLE `{CATALOG}`.online_retail_gold.gold_return_analysis ALTER COLUMN faulty_batch COMMENT 'TRUE for 8 FAULT-PHON-* SKUs. Filter on this to isolate Q3 2025 defect batch story. Return rate >40% vs 4-8% normal.'"),
    ("gold_regional_performance return_rate comment",
     f"ALTER TABLE `{CATALOG}`.online_retail_gold.gold_regional_performance ALTER COLUMN return_rate_pct COMMENT 'Returns/orders %. APAC-East (REG-005) shows elevated rate in Q4 2025 from faulty batch exposure.'"),
    ("gold_customer_lifetime_value clv_segment comment",
     f"ALTER TABLE `{CATALOG}`.online_retail_gold.gold_customer_lifetime_value ALTER COLUMN clv_segment COMMENT 'CLV tier: High (revenue>=\\$1000 or Platinum) | Medium (\\$200-999) | Low (<\\$200) | Churned (180+ days inactive).'"),
]

for label, sql in comments:
    run(label, sql, allow_fail=True)

# ── Section 5: Schema Comments ────────────────────────────────────────────────
print("\n5. Setting schema comments...")

schema_comments = [
    ("online_retail_raw comment",    f"COMMENT ON SCHEMA `{CATALOG}`.online_retail_raw IS 'NexusRetail raw source data — 20 Parquet tables in UC Volume /raw_data/. Landing zone for SDP Auto Loader bronze ingestion. Contains unmasked PII.'"),
    ("online_retail_bronze comment", f"COMMENT ON SCHEMA `{CATALOG}`.online_retail_bronze IS 'NexusRetail bronze — Auto Loader streaming tables from UC Volume. Raw fidelity, schema enforced, PK expectations. PII present in customer tables.'"),
    ("online_retail_silver comment", f"COMMENT ON SCHEMA `{CATALOG}`.online_retail_silver IS 'NexusRetail silver — cleansed, validated streaming tables. PII masked via UC column masks. Quarantine table captures DQ violations. Quality tier: silver.'"),
    ("online_retail_gold comment",   f"COMMENT ON SCHEMA `{CATALOG}`.online_retail_gold IS 'NexusRetail gold — business-ready materialized view aggregations. No PII. 6 tables: category sales, customer segments, regional performance, CLV, return analysis, daily revenue.'"),
]
for label, sql in schema_comments:
    run(label, sql, allow_fail=True)

print("\n═══ Governance setup complete ═══")
print("""
Pipeline URL: https://e2-demo-field-eng.cloud.databricks.com/#joblist/pipelines/d8de9ffb-3723-4d56-a65c-e3d183c7caf8
Catalog:      https://e2-demo-field-eng.cloud.databricks.com/explore/data/gurpreet_sethi
""")
