# Databricks notebook source
# NexusRetail Analytics — Silver Layer
# Pattern  : Streaming Tables + native SDP expectations + explicit quarantine MV
# Schema   : gurpreet_sethi.online_retail_silver
# DQ rules : Native @dp.expect / @dp.expect_or_drop decorators
#            (rules documented in dqx_rules/silver_rules.yaml for DQX tooling)
# PII      : Column masks applied via MASK functions (governance/01_column_masks.sql)
#
# Quarantine: silver_dq_quarantine MV collects records failing critical checks
#             from bronze. Run governance/01_column_masks.sql after first pipeline run.

# COMMAND ----------
from pyspark import pipelines as dp
from pyspark.sql import functions as F, types as T
from pyspark.sql import Window

CATALOG = spark.conf.get("catalog", "gurpreet_sethi")
BRZ     = f"`{CATALOG}`.online_retail_bronze"
SLV     = f"`{CATALOG}`.online_retail_silver"
RAW_VOL = f"/Volumes/{CATALOG}/online_retail_raw/raw_data"

# ─────────────────────────────────────────────────────────────────────────────
# DQ QUARANTINE  (Materialized View — explicit bad-record capture)
# Shows records failing the critical data quality checks DQX would catch:
#   • ~52 invoices with NULL total_amount
#   • 3 duplicate customer emails
#   • ~43 failed payments
# ─────────────────────────────────────────────────────────────────────────────

@dp.materialized_view(
    name=f"{SLV}.silver_dq_quarantine",
    comment="DQX-style quarantine: records from bronze that fail critical data quality checks. "
            "Three known issues intentionally embedded in raw data for demo: "
            "(1) ~52 invoices with NULL invoice_total — completeness violation; "
            "(2) 3 customers sharing email 'duplicate.test@example.com' — uniqueness violation; "
            "(3) ~43 payments with status='failed' — flagged for review. "
            "Row count > 0 here should trigger an alert. Monitor this table.",
    table_properties={
        "quality": "silver",
        "domain": "data_quality",
        "data_product": "nexus_retail",
        "alert_on": "nonzero_row_count",
    },
    cluster_by=["source_table", "dq_rule"],
)
def silver_dq_quarantine():
    return spark.sql(f"""
        -- Completeness: NULL invoice totals (should never be null)
        SELECT
            'bronze_invoices'       AS source_table,
            'invoice_total_null'    AS dq_rule,
            'error'                 AS severity,
            invoice_id              AS record_key,
            CAST(order_id AS STRING) AS detail,
            CAST(issue_date AS STRING) AS context,
            current_timestamp()    AS quarantined_at
        FROM {BRZ}.bronze_invoices
        WHERE invoice_total IS NULL

        UNION ALL

        -- Uniqueness: duplicate customer emails
        SELECT
            'bronze_customers'      AS source_table,
            'duplicate_email'       AS dq_rule,
            'warn'                  AS severity,
            customer_id             AS record_key,
            email                   AS detail,
            customer_type           AS context,
            current_timestamp()
        FROM {BRZ}.bronze_customers
        WHERE email = 'duplicate.test@example.com'

        UNION ALL

        -- Validity: failed payments (3% rate, flagged for AR review)
        SELECT
            'bronze_payments'       AS source_table,
            'failed_payment'        AS dq_rule,
            'warn'                  AS severity,
            payment_id              AS record_key,
            order_id                AS detail,
            currency_code           AS context,
            current_timestamp()
        FROM {BRZ}.bronze_payments
        WHERE payment_status = 'failed'

        UNION ALL

        -- Statistical: return rate anomaly — products with >25% return rate
        -- (placeholder for the faulty batch products that will emerge post-Q3 2025)
        SELECT
            'bronze_returns'        AS source_table,
            'high_return_rate_product' AS dq_rule,
            'warn'                  AS severity,
            return_id               AS record_key,
            order_id                AS detail,
            return_reason_code      AS context,
            current_timestamp()
        FROM {BRZ}.bronze_returns
        WHERE return_reason_code = 'faulty_product'
    """)


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
