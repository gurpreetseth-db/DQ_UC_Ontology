-- =============================================================================
-- NexusRetail Analytics — UC Tags, Column Comments, and Table Metadata
-- Run AFTER pipeline has produced silver + gold tables.
-- =============================================================================

USE CATALOG gurpreet_sethi;

-- ---------------------------------------------------------------------------
-- CREATE CUSTOM UC TAGS (if not already defined at account level)
-- ---------------------------------------------------------------------------
-- Note: tag creation requires Catalog Admin or Metastore Admin.
-- These tag key–value pairs are applied to securables below.

-- ---------------------------------------------------------------------------
-- BRONZE LAYER — Table-level tags
-- ---------------------------------------------------------------------------

-- customers
ALTER TABLE online_retail_bronze.bronze_customers
  SET TAGS (
    'quality_tier'   = 'bronze',
    'domain'         = 'customer',
    'contains_pii'   = 'true',
    'pii_class'      = 'direct',
    'regulatory'     = 'gdpr',
    'data_product'   = 'nexus_retail',
    'owner'          = 'data_engineering',
    'update_freq'    = 'pipeline_triggered'
  );

ALTER TABLE online_retail_bronze.bronze_customers
  ALTER COLUMN full_name    SET TAGS ('pii' = 'full_name',    'pii_class' = 'direct');
ALTER TABLE online_retail_bronze.bronze_customers
  ALTER COLUMN email        SET TAGS ('pii' = 'email',        'pii_class' = 'direct');
ALTER TABLE online_retail_bronze.bronze_customers
  ALTER COLUMN phone        SET TAGS ('pii' = 'phone',        'pii_class' = 'direct');
ALTER TABLE online_retail_bronze.bronze_customers
  ALTER COLUMN date_of_birth SET TAGS ('pii' = 'date_of_birth', 'pii_class' = 'direct');

-- orders
ALTER TABLE online_retail_bronze.bronze_orders
  SET TAGS (
    'quality_tier' = 'bronze', 'domain' = 'transaction',
    'data_product' = 'nexus_retail', 'owner' = 'data_engineering'
  );

-- invoices
ALTER TABLE online_retail_bronze.bronze_invoices
  SET TAGS (
    'quality_tier' = 'bronze', 'domain' = 'transaction',
    'data_product' = 'nexus_retail', 'owner' = 'data_engineering',
    'dq_known_issue' = 'approx_40_null_totals_intentional_for_demo'
  );

-- products
ALTER TABLE online_retail_bronze.bronze_products
  SET TAGS (
    'quality_tier' = 'bronze', 'domain' = 'product',
    'data_product' = 'nexus_retail', 'owner' = 'data_engineering',
    'contains_faulty_batch' = 'true'
  );
ALTER TABLE online_retail_bronze.bronze_products
  ALTER COLUMN faulty_batch SET TAGS ('governance' = 'quality_indicator', 'story' = 'q3_2025_defect_batch');

-- returns
ALTER TABLE online_retail_bronze.bronze_returns
  SET TAGS (
    'quality_tier' = 'bronze', 'domain' = 'transaction',
    'data_product' = 'nexus_retail', 'contains_anomaly' = 'q4_2025_return_spike'
  );

-- ---------------------------------------------------------------------------
-- SILVER LAYER — Table + Column comments + tags
-- ---------------------------------------------------------------------------

ALTER TABLE online_retail_silver.silver_dim_customers
  SET TAGS (
    'quality_tier' = 'silver', 'domain' = 'customer',
    'contains_pii' = 'true',  'pii_class' = 'direct',
    'regulatory'   = 'gdpr',  'data_product' = 'nexus_retail',
    'owner' = 'data_engineering', 'pii_masked' = 'true',
    'scd_type' = '1'
  );

-- Column-level comments on silver_dim_customers
ALTER TABLE online_retail_silver.silver_dim_customers
  ALTER COLUMN full_name     COMMENT 'PII:direct — Masked for non-owner roles: shows first initial only. GDPR Art.4(1).';
ALTER TABLE online_retail_silver.silver_dim_customers
  ALTER COLUMN email         COMMENT 'PII:direct — Masked for non-owner roles: shows ***@domain.com. 3 duplicates quarantined by DQX.';
ALTER TABLE online_retail_silver.silver_dim_customers
  ALTER COLUMN phone         COMMENT 'PII:direct — Masked for non-owner roles: shows ***-***-XXXX.';
ALTER TABLE online_retail_silver.silver_dim_customers
  ALTER COLUMN date_of_birth COMMENT 'PII:direct — Masked to year (Jan 1) for non-owner. GDPR data minimisation principle.';
ALTER TABLE online_retail_silver.silver_dim_customers
  ALTER COLUMN annual_income_usd COMMENT 'PII:sensitive_financial — Annual income in USD. Non-owner sees income_bracket label instead.';
ALTER TABLE online_retail_silver.silver_dim_customers
  ALTER COLUMN loyalty_tier  COMMENT 'Loyalty classification: Bronze / Silver / Gold / Platinum. Based on purchase history.';
ALTER TABLE online_retail_silver.silver_dim_customers
  ALTER COLUMN age_bracket   COMMENT 'Derived from date_of_birth: 18-24 | 25-34 | 35-44 | 45-54 | 55+';
ALTER TABLE online_retail_silver.silver_dim_customers
  ALTER COLUMN acquisition_channel COMMENT 'How the customer was acquired: organic_search | paid_search | social_media | referral | email_campaign';

-- silver_fact_orders tags
ALTER TABLE online_retail_silver.silver_fact_orders
  SET TAGS (
    'quality_tier' = 'silver', 'domain' = 'transaction',
    'data_product' = 'nexus_retail', 'owner' = 'data_engineering'
  );
ALTER TABLE online_retail_silver.silver_fact_orders
  ALTER COLUMN order_total  COMMENT 'Order total in USD. Validated against computed_total (sum of line items ±$0.01).';
ALTER TABLE online_retail_silver.silver_fact_orders
  ALTER COLUMN computed_total COMMENT 'DQX-computed total from order_items. Should match order_total ±$0.01.';
ALTER TABLE online_retail_silver.silver_fact_orders
  ALTER COLUMN region_id    COMMENT 'FK to silver_dim_geography. Derived from customer billing country.';

-- silver_fact_invoices tags + comments
ALTER TABLE online_retail_silver.silver_fact_invoices
  SET TAGS (
    'quality_tier' = 'silver', 'domain' = 'transaction',
    'data_product' = 'nexus_retail', 'dq_note' = 'null_totals_dropped_by_dqx'
  );
ALTER TABLE online_retail_silver.silver_fact_invoices
  ALTER COLUMN invoice_total  COMMENT 'Invoice total in USD. NULL records from bronze dropped by DQX at silver ingestion.';
ALTER TABLE online_retail_silver.silver_fact_invoices
  ALTER COLUMN total_variance COMMENT 'Difference: invoice_total minus order_total. Should be ≤ $0.01 for reconciled invoices.';
ALTER TABLE online_retail_silver.silver_fact_invoices
  ALTER COLUMN is_overdue     COMMENT 'TRUE if due_date < today AND invoice_status = pending. Triggers AR alerts.';

-- silver_fact_returns tags + comments
ALTER TABLE online_retail_silver.silver_fact_returns
  SET TAGS (
    'quality_tier' = 'silver', 'domain' = 'transaction',
    'data_product' = 'nexus_retail', 'contains_anomaly' = 'q4_2025_return_spike'
  );
ALTER TABLE online_retail_silver.silver_fact_returns
  ALTER COLUMN contains_faulty_product COMMENT 'TRUE for the 72 returns tracing to FAULT-* batch SKUs. Key signal for product quality analysis.';
ALTER TABLE online_retail_silver.silver_fact_returns
  ALTER COLUMN return_category COMMENT 'Derived grouping: quality_issue | fulfilment_error | customer_preference';

-- DQX quarantine tags + comments
ALTER TABLE online_retail_silver.silver_dq_quarantine
  SET TAGS (
    'quality_tier' = 'silver', 'domain' = 'data_quality',
    'data_product' = 'nexus_retail', 'alert_on' = 'nonzero_row_count'
  );

-- silver_dim_products tags
ALTER TABLE online_retail_silver.silver_dim_products
  SET TAGS (
    'quality_tier' = 'silver', 'domain' = 'product',
    'data_product' = 'nexus_retail', 'contains_faulty_batch' = 'true'
  );
ALTER TABLE online_retail_silver.silver_dim_products
  ALTER COLUMN faulty_batch COMMENT 'TRUE for 8 electronics SKUs (FAULT-*) that caused Q3 2025 return spike in APAC-East. Root cause of quality investigation.';
ALTER TABLE online_retail_silver.silver_dim_products
  ALTER COLUMN current_price COMMENT 'Most recent price from product_pricing (highest effective_from date). Used for revenue calculations.';

-- ---------------------------------------------------------------------------
-- GOLD LAYER — Table-level tags + column comments
-- ---------------------------------------------------------------------------

ALTER TABLE online_retail_gold.gold_category_sales
  SET TAGS (
    'quality_tier' = 'gold', 'domain' = 'product',
    'data_product' = 'nexus_retail', 'owner' = 'analytics',
    'grain' = 'category_subcategory_region_month'
  );
ALTER TABLE online_retail_gold.gold_category_sales
  ALTER COLUMN return_rate_pct COMMENT 'Return rate % = returns / orders. Electronics shows anomalous spike in Q4 2025 due to faulty batch.';
ALTER TABLE online_retail_gold.gold_category_sales
  ALTER COLUMN net_revenue     COMMENT 'Gross revenue minus discounts. Does NOT deduct refunds (see refund_total separately).';
ALTER TABLE online_retail_gold.gold_category_sales
  ALTER COLUMN contains_faulty_products COMMENT 'TRUE when the subcategory has any faulty_batch=TRUE products. Useful for root-cause filtering.';

ALTER TABLE online_retail_gold.gold_customer_segment_sales
  SET TAGS (
    'quality_tier' = 'gold', 'domain' = 'customer',
    'data_product' = 'nexus_retail', 'owner' = 'analytics',
    'grain' = 'segment_region_month'
  );
ALTER TABLE online_retail_gold.gold_customer_segment_sales
  ALTER COLUMN repeat_purchase_rate_pct COMMENT 'Percentage of customers in segment who have made more than one order.';
ALTER TABLE online_retail_gold.gold_customer_segment_sales
  ALTER COLUMN revenue_per_customer COMMENT 'Total segment revenue divided by unique customer count. Key CLV proxy metric.';

ALTER TABLE online_retail_gold.gold_regional_performance
  SET TAGS (
    'quality_tier' = 'gold', 'domain' = 'geography',
    'data_product' = 'nexus_retail', 'owner' = 'analytics',
    'grain' = 'region_country_month', 'key_story' = 'apac_east_return_spike'
  );
ALTER TABLE online_retail_gold.gold_regional_performance
  ALTER COLUMN return_rate_pct COMMENT 'Returns / orders %. APAC-East (REG-005) peaks >40% in Q4 2025 due to faulty batch exposure.';
ALTER TABLE online_retail_gold.gold_regional_performance
  ALTER COLUMN net_revenue     COMMENT 'Gross revenue minus refunds for the period. Most conservative revenue figure.';

ALTER TABLE online_retail_gold.gold_customer_lifetime_value
  SET TAGS (
    'quality_tier' = 'gold', 'domain' = 'customer',
    'data_product' = 'nexus_retail', 'owner' = 'analytics',
    'grain' = 'customer_id'
  );
ALTER TABLE online_retail_gold.gold_customer_lifetime_value
  ALTER COLUMN clv_segment COMMENT 'CLV tier: High (revenue≥$1000 or Platinum) | Medium ($200-$999) | Low (<$200) | Churned (180+ days inactive).';
ALTER TABLE online_retail_gold.gold_customer_lifetime_value
  ALTER COLUMN orders_per_30_days COMMENT 'Order frequency normalised to 30-day period. Useful for churn prediction and engagement scoring.';

ALTER TABLE online_retail_gold.gold_return_analysis
  SET TAGS (
    'quality_tier' = 'gold', 'domain' = 'transaction',
    'data_product' = 'nexus_retail', 'owner' = 'analytics',
    'grain' = 'product_reason_week', 'key_story' = 'faulty_batch_return_anomaly'
  );
ALTER TABLE online_retail_gold.gold_return_analysis
  ALTER COLUMN faulty_batch    COMMENT 'TRUE for the 8 FAULT-* SKUs. Filter on this to isolate the Q3 2025 defect batch story.';
ALTER TABLE online_retail_gold.gold_return_analysis
  ALTER COLUMN return_rate_pct COMMENT 'Return rate for this product × reason × week. FAULT-* products show >40% vs 5-8% normal.';

ALTER TABLE online_retail_gold.gold_daily_revenue
  SET TAGS (
    'quality_tier' = 'gold', 'domain' = 'transaction',
    'data_product' = 'nexus_retail', 'owner' = 'analytics',
    'grain' = 'date_channel_region'
  );
ALTER TABLE online_retail_gold.gold_daily_revenue
  ALTER COLUMN new_customers       COMMENT 'Customers placing their first ever order on this date.';
ALTER TABLE online_retail_gold.gold_daily_revenue
  ALTER COLUMN returning_customers COMMENT 'Customers who have ordered before this date.';
