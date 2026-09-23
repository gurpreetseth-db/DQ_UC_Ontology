# Databricks notebook source

# MAGIC %pip install databricks-labs-dqx==0.16.0

# COMMAND ----------
# NexusRetail Analytics — Silver Layer
# Pattern  : Streaming Tables + native SDP expectations + DQX-driven quarantine MV
# Schema   : gurpreet_sethi.online_retail_silver
# DQ rules : Native @dp.expect / @dp.expect_or_drop decorators (enforcement) PLUS
#            databricks-labs-dqx applied from dqx_rules/silver_rules.yaml, which
#            drives the per-entity <source_table>_quarantine tables, the combined
#            silver_dq_quarantine roll-up, and silver_dq_summary.
# PII      : Column masks applied via MASK functions (governance/01_column_masks.sql)
#
# Quarantine: one Materialized View PER ENTITY — bronze_customers_quarantine,
#             bronze_orders_quarantine, bronze_invoices_quarantine, … (8 total) —
#             each holding only that entity's failing/flagged DQX records.
#             silver_dq_quarantine is a UNION roll-up over them; silver_dq_summary
#             aggregates counts. Run governance/01_column_masks.sql after first
#             pipeline run.

# COMMAND ----------
from pyspark import pipelines as dp
from pyspark.sql import functions as F, types as T
from pyspark.sql import Window
from databricks.labs.dqx.engine import DQEngine
from databricks.sdk import WorkspaceClient
import yaml

CATALOG = spark.conf.get("catalog", "gurpreet_sethi")
BRZ     = f"`{CATALOG}`.online_retail_bronze"
SLV     = f"`{CATALOG}`.online_retail_silver"
RAW_VOL = f"/Volumes/{CATALOG}/online_retail_raw/raw_data"

# ─────────────────────────────────────────────────────────────────────────────
# DQX HELPERS  (drives the silver_dq_quarantine table below)
# Rules live in dqx_rules/silver_rules.yaml (native DQX metadata, grouped by
# entity). The bundle syncs that file to the workspace and passes its path via
# the pipeline configuration key 'dqx.rules_path' (see databricks.yml).
# ─────────────────────────────────────────────────────────────────────────────
DQX_RULES_PATH = spark.conf.get("dqx.rules_path", "")

# Primary key per entity → becomes the quarantine 'record_key'.
DQX_KEY = {
    "customers":   "customer_id",
    "orders":      "order_id",
    "invoices":    "invoice_id",
    "products":    "product_id",
    "order_items": "line_id",
    "returns":     "return_id",
    "payments":    "payment_id",
    "reviews":     "review_id",
}

# Bronze table backing each entity's checks (defaults to bronze_<entity>).
DQX_TABLE = {
    "reviews": "bronze_product_reviews",
}


def _load_dqx_checks() -> dict:
    """Load native DQX checks grouped by entity from the synced YAML."""
    if not DQX_RULES_PATH:
        raise ValueError(
            "Pipeline conf 'dqx.rules_path' is not set — add it under the "
            "pipeline 'configuration' in databricks.yml.")
    with open(DQX_RULES_PATH) as f:
        return yaml.safe_load(f)


def _dqx_input(entity: str):
    """Batch bronze DataFrame shaped for an entity's checks.

    Batch (spark.read) so dataset-level checks like is_unique work. Handles the
    three column mismatches: customers needs demographics (loyalty_tier/nps_score),
    orders needs computed_total from line items, reviews lives in a differently
    named table.
    """
    if entity == "customers":
        cust = spark.read.table(f"{BRZ}.bronze_customers")
        # Demographics is a per-customer dimension — dedupe on customer_id so the
        # left join stays 1:1 and customer checks aren't double-counted.
        demo = (spark.read.table(f"{BRZ}.bronze_customer_demographics")
                .select("customer_id", "loyalty_tier", "nps_score")
                .dropDuplicates(["customer_id"]))
        return cust.join(demo, "customer_id", "left")
    if entity == "orders":
        orders = spark.read.table(f"{BRZ}.bronze_orders")
        items = (spark.read.table(f"{BRZ}.bronze_order_items")
                 .groupBy("order_id")
                 .agg(F.round(F.sum("line_total"), 2).alias("computed_total")))
        return orders.join(items, "order_id", "left")
    return spark.read.table(f"{BRZ}.{DQX_TABLE.get(entity, f'bronze_{entity}')}")


# Fixed 7-column quarantine schema — shared by every <source_table>_quarantine
# table AND the combined silver_dq_quarantine roll-up, so downstream consumers
# (Genie, governance, dashboards, alerts) can rely on one stable contract.
QUARANTINE_SCHEMA = T.StructType([
    T.StructField("source_table",   T.StringType()),
    T.StructField("dq_rule",        T.StringType()),
    T.StructField("severity",       T.StringType()),
    T.StructField("record_key",     T.StringType()),
    T.StructField("detail",         T.StringType()),
    T.StructField("context",        T.StringType()),
    T.StructField("quarantined_at", T.TimestampType()),
])

# Per-entity quarantine table names: <source_table>_quarantine, e.g.
# bronze_customers_quarantine, bronze_invoices_quarantine. Enumerated so the
# combined roll-up (and any downstream tooling) can list them deterministically.
QUARANTINE_TABLES = {
    entity: f"{DQX_TABLE.get(entity, f'bronze_{entity}')}_quarantine"
    for entity in DQX_KEY
}


def _dqx_quarantine_rows(annotated, source_table: str, key_col: str, arr_col: str, severity: str):
    """Explode a DQX result column (_errors|_warnings) into the fixed 7-column
    quarantine schema. `arr_col` is an array<struct> — explode() keeps only rows
    that actually failed at least one check. `source_table` is stamped verbatim so
    each row is traceable to its bronze origin (e.g. bronze_product_reviews)."""
    return (
        annotated
        .select(
            F.col(key_col).cast("string").alias("record_key"),
            F.explode(F.col(arr_col)).alias("issue"),
        )
        .select(
            F.lit(source_table).alias("source_table"),
            F.col("issue.name").alias("dq_rule"),
            F.lit(severity).alias("severity"),
            F.col("record_key"),
            F.col("issue.message").alias("detail"),
            F.concat_ws(", ", F.col("issue.columns")).alias("context"),
            F.current_timestamp().alias("quarantined_at"),
        )
    )


def _entity_quarantine_df(entity: str, key_col: str, source_table: str):
    """Run one entity's DQX checks and return the failing/flagged rows in the
    quarantine schema (errors + warnings). Returns an empty, correctly-typed
    frame when the entity has no checks in the YAML — so the table still exists
    with 0 rows (a clean data-health signal) rather than failing the pipeline."""
    checks = _load_dqx_checks()
    entity_checks = checks.get(entity)
    if not entity_checks:
        return spark.createDataFrame([], QUARANTINE_SCHEMA)
    engine = DQEngine(WorkspaceClient())
    annotated = engine.apply_checks_by_metadata(_dqx_input(entity), entity_checks)
    errors   = _dqx_quarantine_rows(annotated, source_table, key_col, "_errors",   "error")
    warnings = _dqx_quarantine_rows(annotated, source_table, key_col, "_warnings", "warn")
    return errors.unionByName(warnings)


def _make_entity_quarantine(entity: str, key_col: str):
    """Factory: register ONE Materialized View per entity, named
    <source_table>_quarantine (e.g. bronze_customers_quarantine). Each table holds
    only the records that failed or were flagged by that entity's DQX checks, in
    the shared 7-column quarantine schema. A non-zero row count is a per-entity
    data-health alert. Defined via a factory so each closure binds its own entity."""
    source_table = DQX_TABLE.get(entity, f"bronze_{entity}")
    table_name   = f"{source_table}_quarantine"

    @dp.materialized_view(
        name=f"{SLV}.{table_name}",
        comment=(
            f"DQX quarantine for {source_table}: every record failing or flagged by a "
            f"databricks-labs-dqx check for the '{entity}' entity (rules defined in "
            f"dqx_rules/silver_rules.yaml). Fixed 7-column schema: source_table, dq_rule, "
            f"severity (error|warn), record_key ({key_col}), detail, context, quarantined_at. "
            f"A non-zero row count is a data-health alert for {source_table}."
        ),
        table_properties={
            "quality": "silver",
            "domain": "data_quality",
            "data_product": "nexus_retail",
            "quarantine_entity": entity,
            "quarantine_source": source_table,
            "alert_on": "nonzero_row_count",
            "dq_engine": "databricks-labs-dqx",
        },
        cluster_by=["dq_rule", "severity"],
    )
    def _entity_quarantine():
        return _entity_quarantine_df(entity, key_col, source_table)

    # Unique function identity per registered dataset (name= drives the table name,
    # but keep __name__ distinct to avoid confusing the pipeline graph).
    _entity_quarantine.__name__ = table_name
    return _entity_quarantine


# Register the per-entity quarantine tables (one Materialized View per entity).
for _entity, _key_col in DQX_KEY.items():
    _make_entity_quarantine(_entity, _key_col)

# ─────────────────────────────────────────────────────────────────────────────
# DQ QUARANTINE  (Materialized View — combined roll-up over per-entity tables)
# This is now a UNION of the per-entity <source_table>_quarantine tables above,
# NOT a re-computation — DQX runs once per entity, and this view stitches the
# results into one cross-entity table. Covers all 8 bronze entities (customers,
# orders, invoices, products, order_items, returns, payments, reviews). The
# seeded demo issues surface as real DQX results:
#   • ~52 invoices with NULL invoice_total   → rule invoice_total_not_null (error)
#   • 3 customers sharing an email           → rule email_unique          (warn)
#   • ~43 payments with status='failed'      → rule failed_payment_flag   (warn)
# Schema is intentionally stable (7 columns) — Genie, Domains and governance
# reference this table by name + schema. Per-entity detail lives in the
# <source_table>_quarantine tables (e.g. bronze_customers_quarantine).
# ─────────────────────────────────────────────────────────────────────────────

@dp.materialized_view(
    name=f"{SLV}.silver_dq_quarantine",
    comment="Combined DQX quarantine roll-up: a UNION of the per-entity "
            "<source_table>_quarantine tables (bronze_customers_quarantine, "
            "bronze_invoices_quarantine, …) across all 8 bronze entities. "
            "Columns: source_table, dq_rule (DQX check name), severity (error|warn), "
            "record_key (entity PK), detail (DQX message), context (columns involved), "
            "quarantined_at. Seeded demo issues surface as real results: ~52 "
            "invoice_total_not_null (error), 3 email_unique (warn), ~43 failed_payment_flag "
            "(warn). Row count > 0 should trigger an alert. For per-entity triage, query the "
            "individual <source_table>_quarantine tables. Monitor this table.",
    table_properties={
        "quality": "silver",
        "domain": "data_quality",
        "data_product": "nexus_retail",
        "alert_on": "nonzero_row_count",
        "dq_engine": "databricks-labs-dqx",
    },
    cluster_by=["source_table", "dq_rule"],
)
def silver_dq_quarantine():
    frames = [spark.read.table(f"{SLV}.{table}") for table in QUARANTINE_TABLES.values()]
    result = frames[0]
    for frame in frames[1:]:
        result = result.unionByName(frame)
    return result


# ─────────────────────────────────────────────────────────────────────────────
# DQ SUMMARY  (Materialized View — dashboard/alert-friendly rollup)
# One row per (source_table, dq_rule, severity) with the count of failing records.
# ─────────────────────────────────────────────────────────────────────────────

@dp.materialized_view(
    name=f"{SLV}.silver_dq_summary",
    comment="DQX quality rollup: failing-record counts per (source_table, dq_rule, severity) "
            "over silver_dq_quarantine. Drives DQ dashboards/alerts.",
    table_properties={
        "quality": "silver",
        "domain": "data_quality",
        "data_product": "nexus_retail",
    },
)
def silver_dq_summary():
    return (
        spark.read.table(f"{SLV}.silver_dq_quarantine")
        .groupBy("source_table", "dq_rule", "severity")
        .agg(
            F.count("*").alias("failing_records"),
            F.max("quarantined_at").alias("last_evaluated_at"),
        )
    )


# ─────────────────────────────────────────────────────────────────────────────
# DATE DIMENSION
# ─────────────────────────────────────────────────────────────────────────────

@dp.materialized_view(
    name=f"{SLV}.silver_dim_date",
    comment="Date dimension Sep 2024 – Dec 2027. "
            "Columns: date_key (YYYYMMDD int), calendar_date, year, quarter, month, "
            "week_of_year, day_of_week, is_weekend, is_q4, fiscal_quarter.",
    table_properties={"quality": "silver", "domain": "reference", "data_product": "nexus_retail"},
    cluster_by=["calendar_date"],
)
def silver_dim_date():
    return spark.sql("""
        SELECT
            CAST(DATE_FORMAT(d, 'yyyyMMdd') AS INT)  AS date_key,
            d                                         AS calendar_date,
            YEAR(d)   AS year,
            QUARTER(d) AS quarter,
            MONTH(d)  AS month,
            WEEKOFYEAR(d) AS week_of_year,
            DAYOFWEEK(d)  AS day_of_week,
            CASE WHEN DAYOFWEEK(d) IN (1,7) THEN TRUE ELSE FALSE END AS is_weekend,
            CASE WHEN MONTH(d) IN (10,11,12) THEN TRUE ELSE FALSE END AS is_q4,
            CASE
                WHEN MONTH(d) BETWEEN 1  AND 3  THEN 'FQ1'
                WHEN MONTH(d) BETWEEN 4  AND 6  THEN 'FQ2'
                WHEN MONTH(d) BETWEEN 7  AND 9  THEN 'FQ3'
                ELSE 'FQ4'
            END AS fiscal_quarter
        FROM (
            SELECT EXPLODE(
                SEQUENCE(DATE '2024-09-01', DATE '2027-12-31', INTERVAL 1 DAY)
            ) AS d
        )
    """)


# ─────────────────────────────────────────────────────────────────────────────
# GEOGRAPHY DIMENSION
# ─────────────────────────────────────────────────────────────────────────────

@dp.materialized_view(
    name=f"{SLV}.silver_dim_geography",
    comment="Denormalised country → region hierarchy. "
            "APAC-East (REG-005): key region for the Q3 2025 return spike demo.",
    table_properties={"quality": "silver", "domain": "geography", "data_product": "nexus_retail"},
    cluster_by=["region_id", "country_code"],
)
def silver_dim_geography():
    return spark.sql(f"""
        SELECT
            c.country_code,
            c.country_name,
            c.region_id,
            r.region_name,
            r.super_region,
            r.primary_currency   AS regional_currency,
            c.currency_code      AS country_currency,
            c.primary_language,
            CASE r.super_region
                WHEN 'Americas'     THEN 'AMER'
                WHEN 'EMEA'         THEN 'EMEA'
                WHEN 'Asia Pacific' THEN 'APAC'
                ELSE 'OTHER'
            END AS continent_code
        FROM {BRZ}.bronze_ref_countries  c
        JOIN {BRZ}.bronze_ref_regions    r ON c.region_id = r.region_id
    """)


# ─────────────────────────────────────────────────────────────────────────────
# PRODUCT DIMENSION  (flattened hierarchy + current price + faulty_batch flag)
# ─────────────────────────────────────────────────────────────────────────────

@dp.table(
    name=f"{SLV}.silver_dim_products",
    comment="Enriched product dimension. Flattened category + subcategory. "
            "current_price = most recent effective pricing record. "
            "faulty_batch=TRUE for 8 FAULT-PHON-* SKUs (product_idx 4-11) — "
            "root cause of Q3 2025 return spike in APAC-East.",
    table_properties={
        "quality": "silver",
        "domain": "product",
        "data_product": "nexus_retail",
        "delta.enableRowTracking": "true",
    },
    cluster_by=["category_id", "subcategory_id"],
)
@dp.expect_or_drop("product_id not null", "product_id IS NOT NULL")
@dp.expect("base_price positive",         "base_price > 0")
@dp.expect("sku not null",                "sku IS NOT NULL")
def silver_dim_products():
    bronze_products  = spark.readStream.table(f"{BRZ}.bronze_products")
    bronze_subs      = spark.read.table(f"{BRZ}.bronze_product_subcategories")
    bronze_cats      = spark.read.table(f"{BRZ}.bronze_product_categories")
    bronze_pricing   = spark.read.table(f"{BRZ}.bronze_product_pricing")

    current_price = (
        bronze_pricing
        .withColumn("rank", F.row_number().over(
            Window.partitionBy("product_id").orderBy(F.col("effective_from").desc())))
        .filter(F.col("rank") == 1)
        .select("product_id", F.col("price").alias("current_price"))
    )
    return (
        bronze_products
        .join(bronze_subs.select("subcategory_id","subcategory_name"), "subcategory_id", "left")
        .join(bronze_cats.select("category_id","category_name","avg_return_rate"), "category_id", "left")
        .join(current_price, "product_id", "left")
        .select(
            "product_id", "sku", "product_name", "brand",
            "category_id", "category_name",
            "subcategory_id", "subcategory_name",
            F.coalesce("current_price", "base_price").alias("current_price"),
            "base_price", "weight_kg", "faulty_batch", "is_active", "avg_return_rate",
            "_ingestion_time", "_source_file",
        )
    )


# ─────────────────────────────────────────────────────────────────────────────
# CUSTOMER DIMENSION  (SCD-1 Auto CDC + PII column placeholders)
# Column masks applied POST-pipeline via governance/01_column_masks.sql
# ─────────────────────────────────────────────────────────────────────────────

dp.create_streaming_table(
    name=f"{SLV}.silver_dim_customers",
    comment="Cleansed SCD-1 customer dimension enriched with demographics. "
            "PII columns (full_name, email, phone, date_of_birth, annual_income_usd) "
            "are masked via UC column mask functions — run governance/01_column_masks.sql. "
            "Only gurpreet.sethi@databricks.com sees raw values. "
            "3 duplicate emails (duplicate.test@example.com) visible in quarantine.",
    table_properties={
        "quality": "silver",
        "domain": "customer",
        "contains_pii": "true",
        "pii_classification": "direct",
        "regulatory": "gdpr",
        "data_product": "nexus_retail",
        "delta.enableRowTracking": "true",
    },
)

@dp.append_flow(target=f"{SLV}.silver_dim_customers")
def silver_dim_customers_source():
    bronze_cust = spark.readStream.table(f"{BRZ}.bronze_customers")
    bronze_demo = spark.read.table(f"{BRZ}.bronze_customer_demographics")
    return (
        bronze_cust
        .join(bronze_demo.select(
            "customer_id","age_bracket","income_bracket",
            "loyalty_tier","acquisition_channel","annual_income_usd","nps_score"),
            "customer_id", "left")
        # Warn on duplicate email — quarantined separately, kept here for analysis
        .select(
            "customer_id", "full_name", "email", "phone",
            F.col("date_of_birth").cast(T.DateType()),
            "customer_type", "region_id", "country_code",
            "age_bracket", "income_bracket", "annual_income_usd",
            "loyalty_tier", "acquisition_channel", "nps_score",
            "is_active", "created_at",
            F.unix_timestamp().alias("__START_AT"),
        )
    )


# ─────────────────────────────────────────────────────────────────────────────
# FACT ORDERS  (enriched with customer + geography, DQ expectations)
# ─────────────────────────────────────────────────────────────────────────────

@dp.table(
    name=f"{SLV}.silver_fact_orders",
    comment="Cleansed order facts with customer region and geography enrichment. "
            "order_total validated against computed_total (sum of line items ±$0.01). "
            "Records with NULL customer or invalid status are dropped.",
    table_properties={
        "quality": "silver",
        "domain": "transaction",
        "data_product": "nexus_retail",
        "delta.enableRowTracking": "true",
    },
    cluster_by=["order_date", "region_id"],
)
@dp.expect_or_drop("order_id not null",   "order_id IS NOT NULL")
@dp.expect_or_drop("customer_id present", "customer_id IS NOT NULL")
@dp.expect("order_total positive",        "order_total > 0")
@dp.expect("order_date in range",
           "order_date >= DATE '2024-01-01' AND order_date <= current_date()")
@dp.expect("valid status",
           "status IN ('delivered','shipped','confirmed','cancelled','pending')")
@dp.expect("total matches items",         "ABS(order_total - computed_total) <= 0.01")
def silver_fact_orders():
    bronze_orders = spark.readStream.table(f"{BRZ}.bronze_orders")
    bronze_items  = spark.read.table(f"{BRZ}.bronze_order_items")
    cust_dim      = spark.read.table(f"{SLV}.silver_dim_customers")
    geo_dim       = spark.read.table(f"{SLV}.silver_dim_geography")

    item_totals = (
        bronze_items
        .groupBy("order_id")
        .agg(
            F.round(F.sum("line_total"), 2).alias("computed_total"),
            F.count("*").alias("item_count"),
        )
    )
    customer_geo = (
        cust_dim.select("customer_id","region_id","country_code","loyalty_tier","age_bracket")
        .join(geo_dim.select("country_code","region_name","super_region"), "country_code", "left")
    )
    return (
        bronze_orders
        .withColumn("customer_id",
            F.concat(F.lit("CUST-"), F.lpad(F.col("customer_idx").cast("string"), 6, "0")))
        .join(item_totals, "order_id", "left")
        .join(customer_geo, "customer_id", "left")
        .select(
            "order_id", "customer_id", "region_id", "region_name", "super_region",
            "country_code", "order_date", "estimated_delivery", "channel", "status",
            "order_total", "computed_total", "item_count",
            "loyalty_tier", "age_bracket",
            "_ingestion_time",
        )
    )


# ─────────────────────────────────────────────────────────────────────────────
# FACT INVOICES  (DQ: NULL totals DROPPED — key DQX demo check)
# ─────────────────────────────────────────────────────────────────────────────

@dp.table(
    name=f"{SLV}.silver_fact_invoices",
    comment="Validated invoices. ~52 NULL invoice_total records DROPPED at silver "
            "(they appear in silver_dq_quarantine with rule 'invoice_total_null'). "
            "is_overdue flag computed. total_variance tracks reconciliation quality.",
    table_properties={
        "quality": "silver",
        "domain": "transaction",
        "data_product": "nexus_retail",
        "delta.enableRowTracking": "true",
    },
    cluster_by=["invoice_id", "issue_date"],
)
@dp.expect_or_drop("invoice_id not null",    "invoice_id IS NOT NULL")
@dp.expect_or_drop("order_id not null",      "order_id IS NOT NULL")
@dp.expect_or_drop("invoice_total not null", "invoice_total IS NOT NULL")
@dp.expect("due_date after issue",           "due_date > issue_date")
def silver_fact_invoices():
    bronze_inv    = spark.readStream.table(f"{BRZ}.bronze_invoices")
    silver_orders = spark.read.table(f"{SLV}.silver_fact_orders")

    # Alias order_total from silver_fact_orders to avoid ambiguity with bronze_invoices.order_total
    order_ref = silver_orders.select(
        "order_id",
        "customer_id",
        "region_id",
        F.col("order_total").alias("order_confirmed_total"),
    )
    return (
        bronze_inv
        .join(order_ref, "order_id", "left")
        .withColumn("is_overdue",
            (F.col("due_date") < F.current_date()) & (F.col("invoice_status") == "pending"))
        .withColumn("total_variance",
            F.round(F.col("invoice_total") - F.coalesce("order_confirmed_total", F.lit(0)), 2))
        .withColumn("total_reconciled",
            F.abs(F.col("total_variance")) <= 0.01)
        .select(
            "invoice_id", "invoice_number", "order_id", "customer_id", "region_id",
            "issue_date", "due_date", "invoice_status",
            "invoice_total",
            F.col("order_confirmed_total").alias("order_total"),
            "total_variance", "total_reconciled",
            "is_overdue", "_ingestion_time",
        )
    )


# ─────────────────────────────────────────────────────────────────────────────
# FACT RETURNS  (enriched with product + faulty_batch context)
# ─────────────────────────────────────────────────────────────────────────────

@dp.table(
    name=f"{SLV}.silver_fact_returns",
    comment="Enriched return facts. 41 returns with reason_code='faulty_product' + "
            "78 of 120 total returns occurred in Q4 2025 (seasonal spike / faulty batch). "
            "faulty_batch_involved=TRUE when returned items include FAULT-PHON-* products.",
    table_properties={
        "quality": "silver",
        "domain": "transaction",
        "data_product": "nexus_retail",
        "delta.enableRowTracking": "true",
    },
    cluster_by=["return_date", "return_reason_code"],
)
@dp.expect_or_drop("return_id not null",  "return_id IS NOT NULL")
@dp.expect_or_drop("order_id not null",   "order_id IS NOT NULL")
@dp.expect("valid reason code",
           "return_reason_code IN ('faulty_product','wrong_item','changed_mind',"
           "'damaged_in_transit','not_as_described')")
def silver_fact_returns():
    bronze_ret      = spark.readStream.table(f"{BRZ}.bronze_returns")
    silver_orders   = spark.read.table(f"{SLV}.silver_fact_orders")
    silver_prods    = spark.read.table(f"{SLV}.silver_dim_products")

    # Read return_items from volume (parquet, not yet in bronze pipeline)
    ret_items = (
        spark.read.parquet(f"{RAW_VOL}/return_items")
        .join(silver_prods.select("product_id","faulty_batch","category_name"), "product_id", "left")
        .groupBy("return_id")
        .agg(
            F.collect_set("product_id").alias("returned_product_ids"),
            F.sum("quantity").alias("returned_quantity"),
            F.count("*").alias("returned_item_count"),
            F.max(F.col("faulty_batch").cast("int")).cast("boolean").alias("faulty_batch_involved"),
        )
    )

    return (
        bronze_ret
        .join(silver_orders.select("order_id","customer_id","region_id","region_name","country_code"),
               "order_id", "left")
        .join(ret_items, "return_id", "left")
        .withColumn("return_category",
            F.when(F.col("return_reason_code") == "faulty_product",    "quality_issue")
             .when(F.col("return_reason_code").isin("wrong_item","not_as_described"), "fulfilment_error")
             .otherwise("customer_preference"))
        .select(
            "return_id", "order_id", "customer_id", "region_id", "region_name",
            "return_date", "return_reason_code", "return_category",
            "return_status", "refund_amount",
            F.coalesce("faulty_batch_involved", F.lit(False)).alias("faulty_batch_involved"),
            "returned_product_ids", "returned_quantity", "returned_item_count",
            "_ingestion_time",
        )
    )
