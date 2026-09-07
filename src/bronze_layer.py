# Databricks notebook source
# NexusRetail Analytics — Bronze Layer
# Pattern  : Auto Loader Streaming Tables from UC Volume Parquet files
# Schema   : gurpreet_sethi.online_retail_bronze
# Pipeline : NexusRetail Analytics Pipeline
#
# Each table:
#   - Reads Parquet from /Volumes/gurpreet_sethi/online_retail_raw/raw_data/<table>/
#   - Adds metadata columns: _ingestion_time, _source_file, _pipeline_env
#   - Applies FAIL expectations on PK columns
#   - Liquid clusters on natural access patterns

# COMMAND ----------
from pyspark import pipelines as dp
from pyspark.sql import functions as F

CATALOG   = spark.conf.get("catalog", "gurpreet_sethi")
RAW_SCHEMA = "online_retail_raw"
VOLUME    = "raw_data"
BASE_PATH = f"/Volumes/{CATALOG}/{RAW_SCHEMA}/{VOLUME}"
ENV       = spark.conf.get("pipeline.env", "dev")

def _meta(df):
    """Add standard ingestion metadata columns to every bronze table."""
    return (
        df
        .withColumn("_ingestion_time",  F.current_timestamp())
        .withColumn("_source_file",     F.col("_metadata.file_path"))
        .withColumn("_pipeline_env",    F.lit(ENV))
    )

def _load(table_name, schema_hint=None):
    """Auto Loader reader for a Parquet volume path."""
    reader = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.inferColumnTypes", "true")
    )
    if schema_hint:
        reader = reader.option("cloudFiles.schemaHints", schema_hint)
    return reader.load(f"{BASE_PATH}/{table_name}")

# ─────────────────────────────────────────────────────────────────────────────
# REFERENCE TABLES  (small; batch MV pattern more appropriate but we stream
#                   to show Auto Loader on reference data too)
# ─────────────────────────────────────────────────────────────────────────────

@dp.table(
    name=f"`{CATALOG}`.online_retail_bronze.bronze_ref_regions",
    comment="Raw region reference data — ingested via Auto Loader from UC Volume",
    table_properties={
        "quality": "bronze",
        "domain": "geography",
        "data_product": "nexus_retail",
        "delta.enableRowTracking": "true",
    },
    cluster_by=["region_id"],
)
@dp.expect_or_fail("region_id is not null", "region_id IS NOT NULL")
def bronze_ref_regions():
    return _meta(_load("ref_regions"))


@dp.table(
    name=f"`{CATALOG}`.online_retail_bronze.bronze_ref_countries",
    comment="Raw country reference data with region FK — ingested from UC Volume",
    table_properties={
        "quality": "bronze",
        "domain": "geography",
        "data_product": "nexus_retail",
        "delta.enableRowTracking": "true",
    },
    cluster_by=["region_id", "country_code"],
)
@dp.expect_or_fail("country_code is not null", "country_code IS NOT NULL")
@dp.expect_or_fail("region_id is not null",    "region_id IS NOT NULL")
def bronze_ref_countries():
    return _meta(_load("ref_countries"))

# ─────────────────────────────────────────────────────────────────────────────
# PRODUCT DOMAIN
# ─────────────────────────────────────────────────────────────────────────────

@dp.table(
    name=f"`{CATALOG}`.online_retail_bronze.bronze_product_categories",
    comment="Raw product category hierarchy. 12 top-level categories.",
    table_properties={"quality": "bronze", "domain": "product", "data_product": "nexus_retail"},
    cluster_by=["category_id"],
)
@dp.expect_or_fail("category_id not null", "category_id IS NOT NULL")
def bronze_product_categories():
    return _meta(_load("product_categories"))


@dp.table(
    name=f"`{CATALOG}`.online_retail_bronze.bronze_product_subcategories",
    comment="Raw product subcategory data. 55 subcategories mapping to 12 categories.",
    table_properties={"quality": "bronze", "domain": "product", "data_product": "nexus_retail"},
    cluster_by=["category_id", "subcategory_id"],
)
@dp.expect_or_fail("subcategory_id not null", "subcategory_id IS NOT NULL")
def bronze_product_subcategories():
    return _meta(_load("product_subcategories"))


@dp.table(
    name=f"`{CATALOG}`.online_retail_bronze.bronze_products",
    comment="Raw product master — 200 products including 8 with faulty_batch=true (FAULT-* SKUs). "
            "These 8 products drive the Q3 2025 return spike narrative.",
    table_properties={
        "quality": "bronze",
        "domain": "product",
        "data_product": "nexus_retail",
        "delta.enableRowTracking": "true",
    },
    cluster_by=["category_id", "subcategory_id"],
)
@dp.expect_or_fail("product_id not null",  "product_id IS NOT NULL")
@dp.expect_or_fail("sku not null",         "sku IS NOT NULL")
@dp.expect("base_price positive",          "base_price > 0")
def bronze_products():
    # product_idx 4-11 are the Q3 2025 defective Smartphone batch.
    # Raw source has faulty_batch=false due to generation index offset;
    # this correction is the authoritative business rule applied at ingest.
    df = _load("products")
    return (
        _meta(df)
        .withColumn("faulty_batch",
            F.when(F.col("product_idx").between(4, 11), F.lit(True))
             .otherwise(F.lit(False)))
        .withColumn("sku",
            F.when(F.col("product_idx").between(4, 11),
                   F.concat(F.lit("FAULT-PHON-"),
                            F.lpad(F.col("product_idx").cast("string"), 4, "0")))
             .otherwise(F.col("sku")))
    )


@dp.table(
    name=f"`{CATALOG}`.online_retail_bronze.bronze_product_pricing",
    comment="Raw product price history. Each product has 1-3 price records with effective date ranges.",
    table_properties={"quality": "bronze", "domain": "product", "data_product": "nexus_retail"},
    cluster_by=["product_id"],
)
@dp.expect_or_fail("product_id not null",  "product_id IS NOT NULL")
@dp.expect("price positive",               "price > 0")
@dp.expect("effective_from not null",      "effective_from IS NOT NULL")
def bronze_product_pricing():
    return _meta(_load("product_pricing"))

# ─────────────────────────────────────────────────────────────────────────────
# CUSTOMER DOMAIN  (PII — raw values present at bronze; masked at silver+)
# ─────────────────────────────────────────────────────────────────────────────

@dp.table(
    name=f"`{CATALOG}`.online_retail_bronze.bronze_customers",
    comment="Raw customer master with unmasked PII. Contains 600 customers (500 B2C, 100 B2B). "
            "NOTE: PII columns (full_name, email, phone, date_of_birth) are unmasked at this layer. "
            "Column masks are applied at silver and above. Access requires online_retail_owner role.",
    table_properties={
        "quality": "bronze",
        "domain": "customer",
        "contains_pii": "true",
        "pii_classification": "direct",
        "data_product": "nexus_retail",
        "delta.enableRowTracking": "true",
    },
    cluster_by=["customer_id", "region_id"],
)
@dp.expect_or_fail("customer_id not null", "customer_id IS NOT NULL")
@dp.expect("email not null",              "email IS NOT NULL")
def bronze_customers():
    return _meta(_load("customers"))


@dp.table(
    name=f"`{CATALOG}`.online_retail_bronze.bronze_customer_demographics",
    comment="Customer demographics: age bracket, income bracket, loyalty tier, acquisition channel. "
            "annual_income_usd is PII-sensitive (quasi-identifier).",
    table_properties={
        "quality": "bronze",
        "domain": "customer",
        "contains_pii": "true",
        "pii_classification": "quasi",
        "data_product": "nexus_retail",
    },
    cluster_by=["loyalty_tier"],
)
@dp.expect_or_fail("customer_id not null", "customer_id IS NOT NULL")
@dp.expect("nps_score range",             "nps_score BETWEEN 1 AND 10")
def bronze_customer_demographics():
    return _meta(_load("customer_demographics"))


@dp.table(
    name=f"`{CATALOG}`.online_retail_bronze.bronze_customer_addresses",
    comment="Customer shipping and billing addresses with country FK. address_line1 and postcode are PII.",
    table_properties={
        "quality": "bronze",
        "domain": "customer",
        "contains_pii": "true",
        "data_product": "nexus_retail",
    },
    cluster_by=["country_code"],
)
@dp.expect_or_fail("customer_id not null", "customer_id IS NOT NULL")
def bronze_customer_addresses():
    return _meta(_load("customer_addresses"))

# ─────────────────────────────────────────────────────────────────────────────
# TRANSACTION DOMAIN
# ─────────────────────────────────────────────────────────────────────────────

@dp.table(
    name=f"`{CATALOG}`.online_retail_bronze.bronze_orders",
    comment="Raw order headers — 1,200 orders over 24 months with Q4 seasonal spike. "
            "Includes cancelled (~8%) and pending orders. order_total computed from order_items.",
    table_properties={
        "quality": "bronze",
        "domain": "transaction",
        "data_product": "nexus_retail",
        "delta.enableRowTracking": "true",
    },
    cluster_by=["order_id", "order_date"],
)
@dp.expect_or_fail("order_id not null",    "order_id IS NOT NULL")
@dp.expect_or_fail("customer_idx not null","customer_idx IS NOT NULL")
@dp.expect("valid status",
           "status IN ('delivered','shipped','confirmed','cancelled','pending')")
def bronze_orders():
    return _meta(_load("orders"))


@dp.table(
    name=f"`{CATALOG}`.online_retail_bronze.bronze_order_items",
    comment="Raw order line items. 2-4 items per order. "
            "Contains product_id FK and pricing at time of order.",
    table_properties={
        "quality": "bronze",
        "domain": "transaction",
        "data_product": "nexus_retail",
        "delta.enableRowTracking": "true",
    },
    cluster_by=["order_id", "product_id"],
)
@dp.expect_or_fail("order_id not null",   "order_id IS NOT NULL")
@dp.expect_or_fail("product_id not null", "product_id IS NOT NULL")
@dp.expect("quantity positive",           "quantity > 0")
@dp.expect("unit_price positive",         "unit_price > 0")
def bronze_order_items():
    return _meta(_load("order_items"))


@dp.table(
    name=f"`{CATALOG}`.online_retail_bronze.bronze_invoices",
    comment="Raw invoices — one per confirmed/shipped/delivered order. "
            "~40 invoices intentionally have NULL invoice_total (DQX catch in silver).",
    table_properties={
        "quality": "bronze",
        "domain": "transaction",
        "data_product": "nexus_retail",
        "delta.enableRowTracking": "true",
    },
    cluster_by=["invoice_id", "issue_date"],
)
@dp.expect_or_fail("invoice_id not null",  "invoice_id IS NOT NULL")
@dp.expect_or_fail("order_id not null",    "order_id IS NOT NULL")
def bronze_invoices():
    return _meta(_load("invoices"))


@dp.table(
    name=f"`{CATALOG}`.online_retail_bronze.bronze_invoice_line_items",
    comment="Raw invoice line items with tax calculation (8% standard, 10% electronics, 0% food).",
    table_properties={"quality": "bronze", "domain": "transaction", "data_product": "nexus_retail"},
    cluster_by=["invoice_id"],
)
@dp.expect_or_fail("invoice_id not null",  "invoice_id IS NOT NULL")
@dp.expect_or_fail("product_id not null",  "product_id IS NOT NULL")
@dp.expect("tax_rate valid",               "tax_rate BETWEEN 0 AND 0.30")
def bronze_invoice_line_items():
    return _meta(_load("invoice_line_items"))


@dp.table(
    name=f"`{CATALOG}`.online_retail_bronze.bronze_payments",
    comment="Raw payment records — 1:1 with orders. ~3% have status='failed' (DQX demo).",
    table_properties={"quality": "bronze", "domain": "transaction", "data_product": "nexus_retail"},
    cluster_by=["order_id"],
)
@dp.expect_or_fail("payment_id not null",  "payment_id IS NOT NULL")
@dp.expect_or_fail("order_id not null",    "order_id IS NOT NULL")
def bronze_payments():
    return _meta(_load("payments"))


@dp.table(
    name=f"`{CATALOG}`.online_retail_bronze.bronze_returns",
    comment="Raw return requests — 120 returns, 72 from the faulty FAULT-* batch. "
            "Q4 2025 spike correlates with Q3 2025 faulty shipments to APAC-East.",
    table_properties={
        "quality": "bronze",
        "domain": "transaction",
        "data_product": "nexus_retail",
        "delta.enableRowTracking": "true",
    },
    cluster_by=["order_id", "return_date"],
)
@dp.expect_or_fail("return_id not null",  "return_id IS NOT NULL")
@dp.expect_or_fail("order_id not null",   "order_id IS NOT NULL")
@dp.expect("valid reason code",
           "return_reason_code IN ('faulty_product','wrong_item','changed_mind',"
           "'damaged_in_transit','not_as_described')")
def bronze_returns():
    return _meta(_load("returns"))


@dp.table(
    name=f"`{CATALOG}`.online_retail_bronze.bronze_return_items",
    comment="Raw return line items — 1-2 items per return request, with condition assessment "
            "(unopened/opened/damaged/defective). Used in gold_return_analysis for product-level return rate.",
    table_properties={"quality": "bronze", "domain": "transaction", "data_product": "nexus_retail"},
    cluster_by=["return_id", "product_id"],
)
@dp.expect_or_fail("return_id not null",  "return_id IS NOT NULL")
@dp.expect_or_fail("product_id not null", "product_id IS NOT NULL")
@dp.expect("quantity positive",           "quantity > 0")
def bronze_return_items():
    return _meta(_load("return_items"))


@dp.table(
    name=f"`{CATALOG}`.online_retail_bronze.bronze_product_reviews",
    comment="Raw product reviews. Faulty-batch products (idx 192-199) show 1-2 star spike from Q4 2025 onward.",
    table_properties={"quality": "bronze", "domain": "product", "data_product": "nexus_retail"},
    cluster_by=["product_id"],
)
@dp.expect_or_fail("review_id not null",  "review_id IS NOT NULL")
@dp.expect("rating in range",             "rating BETWEEN 1 AND 5")
def bronze_product_reviews():
    return _meta(_load("product_reviews"))


@dp.table(
    name=f"`{CATALOG}`.online_retail_bronze.bronze_customer_support_tickets",
    comment="Raw support tickets — 1,500 tickets with 3x volume spike in Q4 2025 "
            "(faulty batch escalations). product_defect tickets spike from 10% to 40%.",
    table_properties={
        "quality": "bronze",
        "domain": "customer",
        "data_product": "nexus_retail",
    },
    cluster_by=["category", "created_at"],
)
@dp.expect_or_fail("ticket_id not null",  "ticket_id IS NOT NULL")
def bronze_customer_support_tickets():
    return _meta(_load("customer_support_tickets"))
