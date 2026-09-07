-- =============================================================================
-- NexusRetail Analytics — Metric Views (WITH METRICS LANGUAGE YAML)
-- + UC Materialized Views in online_retail_metrics schema
-- Workspace : e2-demo-field-eng | Catalog: gurpreet_sethi
-- Requires  : DBR 17.2+  (YAML v1.1); serverless SQL warehouse
-- =============================================================================

USE CATALOG gurpreet_sethi;

-- Ensure metrics schema exists
CREATE SCHEMA IF NOT EXISTS online_retail_metrics
  COMMENT 'NexusRetail semantic / metrics layer — UC Materialized Views and Metric Views.
Genie One data source. No PII. All metrics are pre-governed and documented.
Domain: Online Retail | Owner: analytics | Genie domain: Online Retail Analytics';

-- =============================================================================
-- UC MATERIALIZED VIEWS  (pre-computed for Genie performance)
-- =============================================================================

-- 1. Category Revenue — Genie "Sales Performance" page source
CREATE OR REPLACE MATERIALIZED VIEW online_retail_metrics.mv_category_revenue
  COMMENT 'Pre-computed category revenue with return rates. Refreshed on pipeline run.
Used by Genie Sales Performance page and AI/BI dashboard Category Revenue tile.
Grain: category × subcategory × region × month.'
  TBLPROPERTIES (
    'quality_tier' = 'gold',
    'domain' = 'product',
    'genie_page' = 'sales_performance',
    'data_product' = 'nexus_retail'
  )
AS
SELECT
  category_id,
  category_name,
  subcategory_name,
  region_name,
  super_region,
  sale_month,
  SUM(order_count)    AS order_count,
  SUM(units_sold)     AS units_sold,
  SUM(gross_revenue)  AS gross_revenue,
  SUM(net_revenue)    AS net_revenue,
  SUM(total_discount) AS total_discount,
  SUM(return_count)   AS return_count,
  SUM(refund_total)   AS refund_total,
  ROUND(AVG(return_rate_pct), 2) AS avg_return_rate_pct,
  MAX(contains_faulty_products)  AS contains_faulty_products
FROM online_retail_gold.gold_category_sales
GROUP BY
  category_id, category_name, subcategory_name,
  region_name, super_region, sale_month;


-- 2. Customer Demographics Sales — Genie "Customer Analytics" page source
CREATE OR REPLACE MATERIALIZED VIEW online_retail_metrics.mv_customer_demo_sales
  COMMENT 'Customer demographic segment sales by age bracket, income, loyalty tier, and region.
Used by Genie Customer Analytics page.
Grain: age_bracket × income_bracket × loyalty_tier × region × month.'
  TBLPROPERTIES (
    'quality_tier' = 'gold',
    'domain' = 'customer',
    'genie_page' = 'customer_analytics',
    'data_product' = 'nexus_retail'
  )
AS
SELECT
  age_bracket,
  income_bracket,
  loyalty_tier,
  acquisition_channel,
  customer_type,
  region_name,
  super_region,
  sale_month,
  SUM(unique_customers)          AS unique_customers,
  SUM(order_count)               AS order_count,
  SUM(revenue)                   AS revenue,
  ROUND(AVG(avg_order_value), 2) AS avg_order_value,
  SUM(repeat_customers)          AS repeat_customers,
  ROUND(AVG(repeat_purchase_rate_pct), 2) AS avg_repeat_rate_pct,
  ROUND(AVG(revenue_per_customer), 2)     AS avg_revenue_per_customer
FROM online_retail_gold.gold_customer_segment_sales
GROUP BY
  age_bracket, income_bracket, loyalty_tier,
  acquisition_channel, customer_type,
  region_name, super_region, sale_month;


-- 3. Regional Orders & Returns — Genie "Returns & Quality" page source
CREATE OR REPLACE MATERIALIZED VIEW online_retail_metrics.mv_regional_orders
  COMMENT 'Regional order and return performance. Shows the APAC-East Q4 2025 return spike.
Used by Genie Returns & Quality page and AI/BI Regional Performance tile.
Grain: region × country × month.'
  TBLPROPERTIES (
    'quality_tier' = 'gold',
    'domain' = 'geography',
    'genie_page' = 'returns_quality',
    'data_product' = 'nexus_retail',
    'key_story' = 'apac_east_return_spike'
  )
AS
SELECT
  region_id,
  region_name,
  super_region,
  country_code,
  country_name,
  regional_currency,
  sale_month,
  SUM(order_count)             AS order_count,
  SUM(unique_customers)        AS unique_customers,
  SUM(gross_revenue)           AS gross_revenue,
  SUM(return_count)            AS return_count,
  SUM(refund_total)            AS refund_total,
  SUM(net_revenue)             AS net_revenue,
  ROUND(AVG(return_rate_pct), 2)       AS return_rate_pct,
  ROUND(AVG(avg_order_value), 2)       AS avg_order_value,
  SUM(cancelled_orders)                AS cancelled_orders,
  ROUND(AVG(cancellation_rate_pct), 2) AS cancellation_rate_pct
FROM online_retail_gold.gold_regional_performance
GROUP BY
  region_id, region_name, super_region,
  country_code, country_name, regional_currency, sale_month;


-- =============================================================================
-- METRIC VIEWS  (WITH METRICS LANGUAGE YAML — governed semantic layer)
-- Requires DBR 17.2+ / serverless SQL warehouse
-- =============================================================================

-- 1. Sales KPIs — flexible slicing over daily revenue
CREATE OR REPLACE VIEW online_retail_metrics.metrics_sales_kpis
  WITH METRICS LANGUAGE YAML
  COMMENT 'NexusRetail sales KPIs: flexible slice over revenue, orders, avg order value.
Dimensions: Sale Month, Sale Week, Region, Country, Channel, Category.
Measures: Gross Revenue, Net Revenue, Order Count, Avg Order Value, New Customer Count.
Use with MEASURE() function. Genie data source: Sales Performance page.'
AS $$
  version: 1.1
  source: gurpreet_sethi.online_retail_gold.gold_daily_revenue
  comment: "NexusRetail Sales KPIs — daily revenue grain, slice by any dimension"
  dimensions:
    - name: Sale Date
      expr: sale_date
      comment: "Calendar date of the sale"
    - name: Sale Month
      expr: DATE_TRUNC('MONTH', sale_date)
      comment: "Month of sale — primary time dimension for trend analysis"
    - name: Sale Week
      expr: DATE_TRUNC('WEEK', sale_date)
      comment: "Week commencing date — use for weekly trend analysis"
    - name: Sale Quarter
      expr: DATE_TRUNC('QUARTER', sale_date)
      comment: "Quarter — Q4 (Oct-Dec) shows seasonal revenue spike"
    - name: Channel
      expr: channel
      comment: "Order channel: web | mobile | partner_api"
    - name: Region
      expr: region_name
      comment: "Sales region name (7 global regions)"
    - name: Super Region
      expr: super_region
      comment: "High-level grouping: Americas | EMEA | Asia Pacific"
  measures:
    - name: Gross Revenue
      expr: SUM(gross_revenue)
      comment: "Total revenue before any deductions. Primary revenue measure."
    - name: Order Count
      expr: SUM(order_count)
      comment: "Number of orders placed and confirmed/shipped/delivered"
    - name: Avg Order Value
      expr: SUM(gross_revenue) / NULLIF(SUM(order_count), 0)
      comment: "Average revenue per order. Benchmark: $85-$120 for most regions."
    - name: Unique Customers
      expr: SUM(unique_customers)
      comment: "Distinct customers who placed at least one order in the period"
    - name: New Customer Count
      expr: SUM(new_customers)
      comment: "Customers placing their first ever order in the period"
    - name: Returning Customer Count
      expr: SUM(returning_customers)
      comment: "Customers who have ordered before the current period"
    - name: Revenue per Customer
      expr: SUM(gross_revenue) / NULLIF(SUM(unique_customers), 0)
      comment: "Revenue per unique customer — a proxy for engagement depth"
$$;


-- 2. Customer KPIs — segment performance analysis
CREATE OR REPLACE VIEW online_retail_metrics.metrics_customer_kpis
  WITH METRICS LANGUAGE YAML
  COMMENT 'NexusRetail Customer KPIs: slice revenue and behaviour by demographic dimensions.
Dimensions: Age Bracket, Income Bracket, Loyalty Tier, Acquisition Channel, Region.
Measures: Revenue, Repeat Rate, Avg Order Value, Revenue per Customer.
Genie data source: Customer Analytics page.'
AS $$
  version: 1.1
  source: gurpreet_sethi.online_retail_metrics.mv_customer_demo_sales
  comment: "NexusRetail Customer KPIs — slice by demographic dimension"
  dimensions:
    - name: Age Bracket
      expr: age_bracket
      comment: "Customer age group: 18-24 | 25-34 | 35-44 | 45-54 | 55+"
    - name: Income Bracket
      expr: income_bracket
      comment: "Annual income band: <$30K | $30K-$60K | $60K-$100K | $100K-$200K | $200K+"
    - name: Loyalty Tier
      expr: loyalty_tier
      comment: "NexusRetail loyalty tier: Bronze | Silver | Gold | Platinum"
    - name: Acquisition Channel
      expr: acquisition_channel
      comment: "Customer acquisition source: organic_search | paid_search | social_media | referral | email_campaign"
    - name: Customer Type
      expr: customer_type
      comment: "B2C (individual) or B2B (business account)"
    - name: Region
      expr: region_name
      comment: "Customer billing region"
    - name: Month
      expr: sale_month
      comment: "Analysis month — use DATE_TRUNC('MONTH', ...) for aggregation"
  measures:
    - name: Revenue
      expr: SUM(revenue)
      comment: "Total revenue from this customer segment"
    - name: Customer Count
      expr: SUM(unique_customers)
      comment: "Distinct customers in this segment for the period"
    - name: Order Count
      expr: SUM(order_count)
      comment: "Total orders placed by this segment"
    - name: Avg Order Value
      expr: SUM(revenue) / NULLIF(SUM(order_count), 0)
      comment: "Revenue per order for this segment"
    - name: Repeat Purchase Rate
      expr: AVG(avg_repeat_rate_pct)
      comment: "% of customers who have placed more than one order. Higher is better."
    - name: Revenue per Customer
      expr: AVG(avg_revenue_per_customer)
      comment: "Revenue per unique customer — key CLV proxy"
$$;


-- 3. Product KPIs — return analysis and quality metrics
CREATE OR REPLACE VIEW online_retail_metrics.metrics_product_kpis
  WITH METRICS LANGUAGE YAML
  COMMENT 'NexusRetail Product KPIs: return rate, revenue, units by category dimension.
Key story: Electronics return_rate_pct spikes Q4 2025 (faulty batch FAULT-* SKUs).
Dimensions: Category, Subcategory, Return Reason, Faulty Batch, Month.
Genie data source: Returns & Quality page.'
AS $$
  version: 1.1
  source: gurpreet_sethi.online_retail_gold.gold_return_analysis
  comment: "NexusRetail Product KPIs — return analysis, quality signals"
  dimensions:
    - name: Category
      expr: category_name
      comment: "Product category (Electronics, Apparel, etc.)"
    - name: Subcategory
      expr: subcategory_name
      comment: "Product subcategory (Smartphones, Laptops, etc.)"
    - name: Brand
      expr: brand
      comment: "Product brand"
    - name: Return Reason
      expr: return_reason_code
      comment: "Why item was returned: faulty_product | wrong_item | changed_mind | damaged_in_transit | not_as_described"
    - name: Faulty Batch
      expr: CASE WHEN faulty_batch THEN 'Faulty Batch (FAULT-*)' ELSE 'Normal Product' END
      comment: "Whether product is from the Q3 2025 defective batch. TRUE = FAULT-* SKUs."
    - name: Return Week
      expr: return_week
      comment: "Week the return was initiated"
    - name: Return Month
      expr: DATE_TRUNC('MONTH', return_week)
      comment: "Month of return — Q4 2025 shows dramatic spike for faulty products"
  measures:
    - name: Return Count
      expr: SUM(return_count)
      comment: "Number of return requests"
    - name: Total Refund Amount
      expr: SUM(total_refund_amount)
      comment: "Total value refunded to customers"
    - name: Units Returned
      expr: SUM(units_returned)
      comment: "Physical units returned"
    - name: Return Rate
      expr: AVG(return_rate_pct)
      comment: "Return rate %. FAULT-* products: >40%. Normal: 4-8%. Threshold alert at >25%."
    - name: Avg Days to Return
      expr: AVG(avg_days_to_return)
      comment: "Average days between order and return request. Longer = harder-to-detect defects."
    - name: Net Revenue After Returns
      expr: SUM(total_orders) * AVG(avg_days_to_return) - SUM(total_refund_amount)
      comment: "Estimated revenue impact net of refunds for this product × reason combination"
$$;


-- =============================================================================
-- GRANTS on metrics schema
-- =============================================================================

GRANT USE SCHEMA ON SCHEMA online_retail_metrics TO `gurpreet.sethi@databricks.com`;
GRANT SELECT    ON SCHEMA online_retail_metrics TO `gurpreet.sethi@databricks.com`;

-- Analytics team (uncomment when group exists)
-- GRANT USE SCHEMA ON SCHEMA online_retail_metrics TO `analytics-team`;
-- GRANT SELECT    ON SCHEMA online_retail_metrics TO `analytics-team`;

-- Genie service principal (uncomment when Genie is configured)
-- GRANT USE SCHEMA ON SCHEMA online_retail_metrics TO `genie-service-principal`;
-- GRANT SELECT    ON SCHEMA online_retail_metrics TO `genie-service-principal`;

-- =============================================================================
-- TAGS on metrics objects
-- =============================================================================

ALTER VIEW online_retail_metrics.metrics_sales_kpis
  SET TAGS ('quality_tier' = 'gold', 'domain' = 'transaction',
            'genie_page' = 'sales_performance', 'semantic_layer' = 'metric_view');

ALTER VIEW online_retail_metrics.metrics_customer_kpis
  SET TAGS ('quality_tier' = 'gold', 'domain' = 'customer',
            'genie_page' = 'customer_analytics', 'semantic_layer' = 'metric_view');

ALTER VIEW online_retail_metrics.metrics_product_kpis
  SET TAGS ('quality_tier' = 'gold', 'domain' = 'product',
            'genie_page' = 'returns_quality', 'semantic_layer' = 'metric_view',
            'key_story' = 'faulty_batch_anomaly');
