# Databricks notebook source
# NexusRetail Analytics — Gold Layer
# Pattern  : Materialized Views (batch aggregation from silver streaming tables)
# Schema   : gurpreet_sethi.online_retail_gold
# Notes    :
#   - All gold MVs use CLUSTER BY AUTO for adaptive layout
#   - delta.enableRowTracking on silver sources enables incremental MV refresh on serverless
#   - Key business dimensions retained at grain to support all dashboard slices

# COMMAND ----------
from pyspark import pipelines as dp

CATALOG = spark.conf.get("catalog", "gurpreet_sethi")
SLV     = f"`{CATALOG}`.online_retail_silver"

# ─────────────────────────────────────────────────────────────────────────────
# 1. CATEGORY SALES  (product_category × subcategory × month × region)
# ─────────────────────────────────────────────────────────────────────────────

@dp.materialized_view(
    name=f"`{CATALOG}`.online_retail_gold.gold_category_sales",
    comment="Category-level sales aggregation by subcategory, region, and month. "
            "Retains all dimension columns needed for dashboard slicing. "
            "Includes return_rate_pct to surface the faulty-batch Electronics anomaly. "
            "Grain: category × subcategory × region × sale_month.",
    table_properties={
        "quality": "gold",
        "domain": "product",
        "data_product": "nexus_retail",
        "owner": "analytics",
        "delta.enableRowTracking": "true",
    },
    cluster_by_auto=True,
)
def gold_category_sales():
    return spark.sql(f"""
        WITH order_product AS (
            SELECT
                oi.order_id,
                oi.product_id,
                oi.quantity,
                oi.unit_price,
                oi.discount_pct,
                oi.line_total,
                o.order_date,
                o.status,
                o.region_id,
                o.region_name,
                o.super_region,
                p.category_id,
                p.category_name,
                p.subcategory_id,
                p.subcategory_name,
                p.brand,
                p.faulty_batch,
                DATE_TRUNC('MONTH', o.order_date) AS sale_month
            FROM {SLV}.silver_fact_orders          o
            JOIN `{CATALOG}`.online_retail_bronze.bronze_order_items  oi ON o.order_id = oi.order_id
            JOIN {SLV}.silver_dim_products           p  ON oi.product_id = p.product_id
            WHERE o.status IN ('delivered','shipped')
        ),
        returns_agg AS (
            SELECT
                p.category_id,
                p.subcategory_id,
                o.region_id,
                DATE_TRUNC('MONTH', r.return_date)    AS return_month,
                COUNT(*)                               AS return_count,
                SUM(COALESCE(r.refund_amount, 0))      AS refund_total
            FROM {SLV}.silver_fact_returns   r
            JOIN {SLV}.silver_fact_orders    o ON r.order_id = o.order_id
            JOIN `{CATALOG}`.online_retail_bronze.bronze_order_items oi ON o.order_id = oi.order_id
            JOIN {SLV}.silver_dim_products   p  ON oi.product_id = p.product_id
            GROUP BY 1,2,3,4
        )
        SELECT
            op.category_id,
            op.category_name,
            op.subcategory_id,
            op.subcategory_name,
            op.brand,
            op.region_id,
            op.region_name,
            op.super_region,
            op.sale_month,
            op.faulty_batch                                        AS contains_faulty_products,
            COUNT(DISTINCT op.order_id)                            AS order_count,
            SUM(op.quantity)                                       AS units_sold,
            ROUND(SUM(op.line_total), 2)                           AS gross_revenue,
            ROUND(SUM(op.line_total * (1 - op.discount_pct/100)), 2) AS net_revenue,
            ROUND(SUM(op.unit_price * op.quantity * op.discount_pct/100), 2) AS total_discount,
            ROUND(SUM(op.line_total) / NULLIF(COUNT(DISTINCT op.order_id), 0), 2) AS avg_order_value,
            COALESCE(r.return_count, 0)                            AS return_count,
            COALESCE(r.refund_total, 0)                            AS refund_total,
            ROUND(
                100.0 * COALESCE(r.return_count, 0)
                / NULLIF(COUNT(DISTINCT op.order_id), 0), 2
            )                                                      AS return_rate_pct
        FROM order_product op
        LEFT JOIN returns_agg r
            ON  op.category_id    = r.category_id
            AND op.subcategory_id = r.subcategory_id
            AND op.region_id      = r.region_id
            AND op.sale_month     = r.return_month
        GROUP BY
            op.category_id, op.category_name,
            op.subcategory_id, op.subcategory_name,
            op.brand, op.region_id, op.region_name, op.super_region,
            op.sale_month, op.faulty_batch,
            r.return_count, r.refund_total
    """)

# ─────────────────────────────────────────────────────────────────────────────
# 2. CUSTOMER SEGMENT SALES  (age_bracket × income_bracket × loyalty_tier × region × month)
# ─────────────────────────────────────────────────────────────────────────────

@dp.materialized_view(
    name=f"`{CATALOG}`.online_retail_gold.gold_customer_segment_sales",
    comment="Customer segment sales: demographic slice × region × month. "
            "Key dimensions: age_bracket, income_bracket, loyalty_tier, acquisition_channel. "
            "Includes repeat_purchase_rate, avg_order_value per segment. "
            "Grain: segment combo × region × month.",
    table_properties={
        "quality": "gold",
        "domain": "customer",
        "data_product": "nexus_retail",
        "owner": "analytics",
        "delta.enableRowTracking": "true",
    },
    cluster_by_auto=True,
)
def gold_customer_segment_sales():
    return spark.sql(f"""
        WITH customer_orders AS (
            SELECT
                o.order_id,
                o.customer_id,
                o.order_total,
                o.order_date,
                o.region_id,
                o.region_name,
                o.super_region,
                o.loyalty_tier,
                o.age_bracket,
                c.income_bracket,
                c.acquisition_channel,
                c.customer_type,
                c.country_code,
                DATE_TRUNC('MONTH', o.order_date) AS sale_month,
                ROW_NUMBER() OVER (PARTITION BY o.customer_id ORDER BY o.order_date) AS order_sequence
            FROM {SLV}.silver_fact_orders    o
            JOIN {SLV}.silver_dim_customers  c ON o.customer_id = c.customer_id
            WHERE o.status IN ('delivered','shipped')
        ),
        return_rate AS (
            SELECT
                c.customer_id,
                COUNT(r.return_id) AS return_count
            FROM {SLV}.silver_dim_customers c
            LEFT JOIN {SLV}.silver_fact_orders  o ON c.customer_id = o.customer_id
            LEFT JOIN {SLV}.silver_fact_returns r ON o.order_id = r.order_id
            GROUP BY 1
        )
        SELECT
            co.age_bracket,
            co.income_bracket,
            co.loyalty_tier,
            co.acquisition_channel,
            co.customer_type,
            co.region_id,
            co.region_name,
            co.super_region,
            co.sale_month,
            COUNT(DISTINCT co.customer_id)                           AS unique_customers,
            COUNT(DISTINCT co.order_id)                              AS order_count,
            ROUND(SUM(co.order_total), 2)                            AS revenue,
            ROUND(AVG(co.order_total), 2)                            AS avg_order_value,
            COUNT(DISTINCT CASE WHEN co.order_sequence > 1 THEN co.customer_id END)
                                                                     AS repeat_customers,
            ROUND(
                100.0 * COUNT(DISTINCT CASE WHEN co.order_sequence > 1 THEN co.customer_id END)
                / NULLIF(COUNT(DISTINCT co.customer_id), 0), 2
            )                                                        AS repeat_purchase_rate_pct,
            ROUND(SUM(co.order_total) / NULLIF(COUNT(DISTINCT co.customer_id), 0), 2) AS revenue_per_customer
        FROM customer_orders co
        GROUP BY
            co.age_bracket, co.income_bracket, co.loyalty_tier,
            co.acquisition_channel, co.customer_type,
            co.region_id, co.region_name, co.super_region, co.sale_month
    """)

# ─────────────────────────────────────────────────────────────────────────────
# 3. REGIONAL PERFORMANCE  (region × country × month)
# ─────────────────────────────────────────────────────────────────────────────

@dp.materialized_view(
    name=f"`{CATALOG}`.online_retail_gold.gold_regional_performance",
    comment="Regional performance dashboard view: revenue, orders, returns, net revenue by region × country × month. "
            "APAC-East (REG-005) shows elevated return_rate_pct in Q4 2025 (faulty batch impact). "
            "Grain: region × country × month.",
    table_properties={
        "quality": "gold",
        "domain": "geography",
        "data_product": "nexus_retail",
        "owner": "analytics",
        "delta.enableRowTracking": "true",
    },
    cluster_by_auto=True,
)
def gold_regional_performance():
    return spark.sql(f"""
        SELECT
            o.region_id,
            o.region_name,
            o.super_region,
            o.country_code,
            g.country_name,
            g.regional_currency,
            DATE_TRUNC('MONTH', o.order_date)             AS sale_month,
            COUNT(DISTINCT o.order_id)                     AS order_count,
            COUNT(DISTINCT o.customer_id)                  AS unique_customers,
            ROUND(SUM(o.order_total), 2)                   AS gross_revenue,
            COUNT(DISTINCT r.return_id)                    AS return_count,
            ROUND(SUM(COALESCE(r.refund_amount, 0)), 2)    AS refund_total,
            ROUND(
                SUM(o.order_total) - SUM(COALESCE(r.refund_amount, 0)), 2
            )                                              AS net_revenue,
            ROUND(
                100.0 * COUNT(DISTINCT r.return_id)
                / NULLIF(COUNT(DISTINCT o.order_id), 0), 2
            )                                              AS return_rate_pct,
            ROUND(AVG(o.order_total), 2)                   AS avg_order_value,
            COUNT(DISTINCT CASE WHEN o.status = 'cancelled' THEN o.order_id END) AS cancelled_orders,
            ROUND(
                100.0 * COUNT(DISTINCT CASE WHEN o.status = 'cancelled' THEN o.order_id END)
                / NULLIF(COUNT(DISTINCT o.order_id), 0), 2
            )                                              AS cancellation_rate_pct
        FROM {SLV}.silver_fact_orders       o
        JOIN {SLV}.silver_dim_geography     g ON o.country_code = g.country_code
        LEFT JOIN {SLV}.silver_fact_returns r ON o.order_id = r.order_id
        GROUP BY
            o.region_id, o.region_name, o.super_region,
            o.country_code, g.country_name, g.regional_currency,
            DATE_TRUNC('MONTH', o.order_date)
    """)

# ─────────────────────────────────────────────────────────────────────────────
# 4. CUSTOMER LIFETIME VALUE  (full customer history, one row per customer)
# ─────────────────────────────────────────────────────────────────────────────

@dp.materialized_view(
    name=f"`{CATALOG}`.online_retail_gold.gold_customer_lifetime_value",
    comment="Customer-level CLV summary. One row per customer. "
            "Includes: total_revenue, order_frequency, avg_order_value, days_since_last_order, "
            "clv_segment (High/Medium/Low/Churned), return_rate. "
            "Grain: customer_id.",
    table_properties={
        "quality": "gold",
        "domain": "customer",
        "data_product": "nexus_retail",
        "owner": "analytics",
        "delta.enableRowTracking": "true",
    },
    cluster_by_auto=True,
)
def gold_customer_lifetime_value():
    return spark.sql(f"""
        WITH customer_metrics AS (
            SELECT
                o.customer_id,
                c.loyalty_tier,
                c.age_bracket,
                c.income_bracket,
                c.region_id,
                c.country_code,
                c.customer_type,
                MIN(o.order_date)                          AS first_order_date,
                MAX(o.order_date)                          AS last_order_date,
                COUNT(DISTINCT o.order_id)                 AS total_orders,
                ROUND(SUM(o.order_total), 2)               AS total_revenue,
                ROUND(AVG(o.order_total), 2)               AS avg_order_value,
                DATEDIFF(CURRENT_DATE(), MAX(o.order_date)) AS days_since_last_order,
                COUNT(DISTINCT r.return_id)                AS total_returns
            FROM {SLV}.silver_fact_orders    o
            JOIN {SLV}.silver_dim_customers  c ON o.customer_id = c.customer_id
            LEFT JOIN {SLV}.silver_fact_returns r ON o.order_id = r.order_id
            WHERE o.status IN ('delivered','shipped','confirmed')
            GROUP BY
                o.customer_id, c.loyalty_tier, c.age_bracket, c.income_bracket,
                c.region_id, c.country_code, c.customer_type
        )
        SELECT
            customer_id,
            loyalty_tier,
            age_bracket,
            income_bracket,
            region_id,
            country_code,
            customer_type,
            first_order_date,
            last_order_date,
            total_orders,
            total_revenue,
            avg_order_value,
            days_since_last_order,
            total_returns,
            ROUND(100.0 * total_returns / NULLIF(total_orders, 0), 2) AS customer_return_rate_pct,
            -- CLV segment: High (>$1000 total OR Platinum), Medium ($200-$999),
            --              Low (<$200), Churned (no order in 180+ days)
            CASE
                WHEN days_since_last_order > 180     THEN 'Churned'
                WHEN total_revenue >= 1000
                  OR loyalty_tier = 'Platinum'       THEN 'High'
                WHEN total_revenue >= 200             THEN 'Medium'
                ELSE 'Low'
            END AS clv_segment,
            -- Order frequency: orders per 30-day period active
            ROUND(
                total_orders * 30.0
                / NULLIF(DATEDIFF(last_order_date, first_order_date) + 1, 0), 2
            ) AS orders_per_30_days
        FROM customer_metrics
    """)

# ─────────────────────────────────────────────────────────────────────────────
# 5. RETURN ANALYSIS  (product × reason × week — faulty batch anomaly visible)
# ─────────────────────────────────────────────────────────────────────────────

@dp.materialized_view(
    name=f"`{CATALOG}`.online_retail_gold.gold_return_analysis",
    comment="Return analysis by product × reason × week. "
            "The faulty_batch=TRUE electronics products show return_rate_pct > 40% in Q4 2025. "
            "DQ anomaly detectable: return_count spikes for FAULT-* SKUs. "
            "Grain: product_id × return_reason_code × return_week.",
    table_properties={
        "quality": "gold",
        "domain": "transaction",
        "data_product": "nexus_retail",
        "owner": "analytics",
        "delta.enableRowTracking": "true",
    },
    cluster_by_auto=True,
)
def gold_return_analysis():
    return spark.sql(f"""
        WITH product_sales AS (
            SELECT
                oi.product_id,
                COUNT(DISTINCT oi.order_id) AS orders_with_product,
                SUM(oi.quantity)             AS units_sold
            FROM `{CATALOG}`.online_retail_bronze.bronze_order_items oi
            JOIN {SLV}.silver_fact_orders  o ON oi.order_id = o.order_id
            WHERE o.status IN ('delivered','shipped')
            GROUP BY oi.product_id
        )
        SELECT
            p.product_id,
            p.sku,
            p.product_name,
            p.category_id,
            p.category_name,
            p.subcategory_name,
            p.brand,
            p.faulty_batch,
            r.return_reason_code,
            r.return_category,
            r.return_status,
            DATE_TRUNC('WEEK', r.return_date)              AS return_week,
            COUNT(DISTINCT r.return_id)                    AS return_count,
            ROUND(SUM(COALESCE(r.refund_amount, 0)), 2)    AS total_refund_amount,
            SUM(COALESCE(ri.quantity, 0))                  AS units_returned,
            ps.orders_with_product                         AS total_orders,
            ps.units_sold                                  AS total_units_sold,
            ROUND(
                100.0 * COUNT(DISTINCT r.return_id)
                / NULLIF(ps.orders_with_product, 0), 2
            )                                              AS return_rate_pct,
            ROUND(AVG(
                DATEDIFF(r.return_date,
                         CAST(o.order_date AS DATE))
            ), 1)                                          AS avg_days_to_return
        FROM {SLV}.silver_fact_returns   r
        JOIN {SLV}.silver_fact_orders    o  ON r.order_id = o.order_id
        JOIN `{CATALOG}`.online_retail_bronze.bronze_return_items ri ON r.return_id = ri.return_id
        JOIN {SLV}.silver_dim_products   p  ON ri.product_id = p.product_id
        JOIN product_sales              ps  ON p.product_id = ps.product_id
        GROUP BY
            p.product_id, p.sku, p.product_name, p.category_id, p.category_name,
            p.subcategory_name, p.brand, p.faulty_batch,
            r.return_reason_code, r.return_category, r.return_status,
            DATE_TRUNC('WEEK', r.return_date),
            ps.orders_with_product, ps.units_sold
    """)

# ─────────────────────────────────────────────────────────────────────────────
# 6. DAILY REVENUE  (day × channel × region — AI/BI dashboard backbone)
# ─────────────────────────────────────────────────────────────────────────────

@dp.materialized_view(
    name=f"`{CATALOG}`.online_retail_gold.gold_daily_revenue",
    comment="Daily revenue by channel and region. Backbone for AI/BI time-series dashboard. "
            "Includes new vs returning customer breakdown. "
            "Grain: sale_date × channel × region_id.",
    table_properties={
        "quality": "gold",
        "domain": "transaction",
        "data_product": "nexus_retail",
        "owner": "analytics",
        "delta.enableRowTracking": "true",
    },
    cluster_by=["sale_date", "region_id"],
)
def gold_daily_revenue():
    return spark.sql(f"""
        WITH customer_first_order AS (
            SELECT customer_id, MIN(CAST(order_date AS DATE)) AS first_order_date
            FROM {SLV}.silver_fact_orders
            GROUP BY customer_id
        )
        SELECT
            CAST(o.order_date AS DATE)                AS sale_date,
            o.channel,
            o.region_id,
            o.region_name,
            o.super_region,
            COUNT(DISTINCT o.order_id)                AS order_count,
            ROUND(SUM(o.order_total), 2)              AS gross_revenue,
            COUNT(DISTINCT o.customer_id)             AS unique_customers,
            COUNT(DISTINCT CASE
                WHEN CAST(o.order_date AS DATE) = fo.first_order_date
                THEN o.customer_id END)               AS new_customers,
            COUNT(DISTINCT CASE
                WHEN CAST(o.order_date AS DATE) > fo.first_order_date
                THEN o.customer_id END)               AS returning_customers,
            ROUND(SUM(o.order_total) / NULLIF(COUNT(DISTINCT o.order_id), 0), 2) AS avg_order_value
        FROM {SLV}.silver_fact_orders    o
        JOIN customer_first_order        fo ON o.customer_id = fo.customer_id
        WHERE o.status IN ('delivered','shipped','confirmed')
        GROUP BY
            CAST(o.order_date AS DATE), o.channel,
            o.region_id, o.region_name, o.super_region
    """)
