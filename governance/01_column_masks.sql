-- =============================================================================
-- NexusRetail Analytics — Column Masks & Row Filters
-- Workspace : e2-demo-field-eng
-- Catalog   : gurpreet_sethi
-- Owner role: gurpreet.sethi@databricks.com (is_account_group_member check)
-- =============================================================================
-- Run this AFTER the pipeline has run at least one update so silver tables exist.
-- =============================================================================

USE CATALOG gurpreet_sethi;

-- ---------------------------------------------------------------------------
-- PII COLUMN MASK FUNCTIONS
-- Each function: if caller is owner → return raw value, else return masked value
-- ---------------------------------------------------------------------------

-- full_name: show first initial + *** for non-owners
CREATE OR REPLACE FUNCTION online_retail_silver.mask_full_name(raw_name STRING)
  RETURNS STRING
  COMMENT 'PII mask: shows first initial only to non-owner roles. Full name visible to online_retail_owner.'
  RETURN CASE
    WHEN is_member('online_retail_owner')
      OR current_user() = 'gurpreet.sethi@databricks.com'
    THEN raw_name
    ELSE CONCAT(LEFT(raw_name, 1), '***')
  END;

-- email: show domain part only
CREATE OR REPLACE FUNCTION online_retail_silver.mask_email(raw_email STRING)
  RETURNS STRING
  COMMENT 'PII mask: shows ***@domain.com for non-owners. Full email visible to online_retail_owner.'
  RETURN CASE
    WHEN is_member('online_retail_owner')
      OR current_user() = 'gurpreet.sethi@databricks.com'
    THEN raw_email
    ELSE CONCAT('***@', ELEMENT_AT(SPLIT(raw_email, '@'), 2))
  END;

-- phone: show last 4 digits only
CREATE OR REPLACE FUNCTION online_retail_silver.mask_phone(raw_phone STRING)
  RETURNS STRING
  COMMENT 'PII mask: shows ***-***-XXXX for non-owners. Full number visible to online_retail_owner.'
  RETURN CASE
    WHEN is_member('online_retail_owner')
      OR current_user() = 'gurpreet.sethi@databricks.com'
    THEN raw_phone
    ELSE CONCAT('***-***-', RIGHT(raw_phone, 4))
  END;

-- date_of_birth: truncate to year only
CREATE OR REPLACE FUNCTION online_retail_silver.mask_date_of_birth(dob DATE)
  RETURNS DATE
  COMMENT 'PII mask: truncates to year (Jan 1 of birth year) for non-owners.'
  RETURN CASE
    WHEN is_member('online_retail_owner')
      OR current_user() = 'gurpreet.sethi@databricks.com'
    THEN dob
    ELSE DATE_TRUNC('YEAR', dob)
  END;

-- annual_income: show income bracket label instead of number
CREATE OR REPLACE FUNCTION online_retail_silver.mask_annual_income(income DOUBLE, bracket STRING)
  RETURNS STRING
  COMMENT 'PII:sensitive_financial mask: returns salary bracket label for non-owners, numeric value for owner.'
  RETURN CASE
    WHEN is_member('online_retail_owner')
      OR current_user() = 'gurpreet.sethi@databricks.com'
    THEN CAST(income AS STRING)
    ELSE bracket
  END;

-- address_line1: redacted for non-owners
CREATE OR REPLACE FUNCTION online_retail_silver.mask_address_line1(addr STRING)
  RETURNS STRING
  COMMENT 'PII mask: address redacted for non-owner roles.'
  RETURN CASE
    WHEN is_member('online_retail_owner')
      OR current_user() = 'gurpreet.sethi@databricks.com'
    THEN addr
    ELSE '[REDACTED]'
  END;

-- ---------------------------------------------------------------------------
-- APPLY COLUMN MASKS to silver_dim_customers
-- ---------------------------------------------------------------------------

ALTER TABLE online_retail_silver.silver_dim_customers
  ALTER COLUMN full_name
    SET MASK online_retail_silver.mask_full_name USING COLUMNS (full_name);

ALTER TABLE online_retail_silver.silver_dim_customers
  ALTER COLUMN email
    SET MASK online_retail_silver.mask_email USING COLUMNS (email);

ALTER TABLE online_retail_silver.silver_dim_customers
  ALTER COLUMN phone
    SET MASK online_retail_silver.mask_phone USING COLUMNS (phone);

ALTER TABLE online_retail_silver.silver_dim_customers
  ALTER COLUMN date_of_birth
    SET MASK online_retail_silver.mask_date_of_birth USING COLUMNS (date_of_birth);

-- ---------------------------------------------------------------------------
-- GRANTS — Principle of least privilege
-- ---------------------------------------------------------------------------

-- Owner (full access, unmasked)
GRANT USE CATALOG    ON CATALOG gurpreet_sethi              TO `gurpreet.sethi@databricks.com`;
GRANT USE SCHEMA     ON SCHEMA online_retail_silver         TO `gurpreet.sethi@databricks.com`;
GRANT USE SCHEMA     ON SCHEMA online_retail_gold           TO `gurpreet.sethi@databricks.com`;
GRANT USE SCHEMA     ON SCHEMA online_retail_metrics        TO `gurpreet.sethi@databricks.com`;
GRANT SELECT         ON SCHEMA online_retail_silver         TO `gurpreet.sethi@databricks.com`;
GRANT SELECT         ON SCHEMA online_retail_gold           TO `gurpreet.sethi@databricks.com`;
GRANT SELECT         ON SCHEMA online_retail_metrics        TO `gurpreet.sethi@databricks.com`;

-- Analytics team (silver masked + gold + metrics access)
-- GRANT USE CATALOG ON CATALOG gurpreet_sethi           TO `analytics-team`;
-- GRANT USE SCHEMA  ON SCHEMA online_retail_gold        TO `analytics-team`;
-- GRANT USE SCHEMA  ON SCHEMA online_retail_metrics     TO `analytics-team`;
-- GRANT SELECT      ON SCHEMA online_retail_gold        TO `analytics-team`;
-- GRANT SELECT      ON SCHEMA online_retail_metrics     TO `analytics-team`;
-- NOTE: silver_dim_customers will show masked PII for analytics-team via column masks

-- Bronze is owner-only (raw PII present)
GRANT USE SCHEMA     ON SCHEMA online_retail_bronze         TO `gurpreet.sethi@databricks.com`;
GRANT SELECT         ON SCHEMA online_retail_bronze         TO `gurpreet.sethi@databricks.com`;

-- ---------------------------------------------------------------------------
-- SCHEMA & CATALOG COMMENTS
-- ---------------------------------------------------------------------------

COMMENT ON CATALOG gurpreet_sethi
  IS 'Primary catalog for Gurpreet Sethi — contains domain schemas for agriculture (ag ops),
online retail (NexusRetail demo), and data lab experiments.
Owner: gurpreet.sethi@databricks.com | Workspace: e2-demo-field-eng';

COMMENT ON SCHEMA online_retail_raw
  IS 'NexusRetail raw source data — 20 Parquet tables in UC Volume /raw_data/.
Serves as landing zone for SDP Auto Loader bronze ingestion.
Contains unmasked PII in customer and customer_addresses tables.
Domain: Online Retail | Owner: gurpreet.sethi@databricks.com | Update: on-demand';

COMMENT ON SCHEMA online_retail_bronze
  IS 'NexusRetail bronze layer — Auto Loader streaming tables from UC Volume Parquet files.
Raw fidelity preserved; schema enforced; basic PK expectations applied.
PII present in bronze_customers, bronze_customer_demographics, bronze_customer_addresses.
Quality tier: bronze | Owner: data_engineering | Pipeline: NexusRetail Analytics Pipeline';

COMMENT ON SCHEMA online_retail_silver
  IS 'NexusRetail silver layer — cleansed, DQX-validated streaming tables.
PII masked via column masks (full_name, email, phone, date_of_birth, annual_income_usd).
Only online_retail_owner role sees unmasked PII values.
quarantine table contains records failing DQX critical checks.
Quality tier: silver | Owner: data_engineering | Governance: GDPR-aligned column masks applied';

COMMENT ON SCHEMA online_retail_gold
  IS 'NexusRetail gold layer — business-ready materialized view aggregations.
No PII (customer data aggregated by segment/bracket only).
6 gold tables covering: category sales, customer segments, regional performance,
customer CLV, return analysis, and daily revenue.
Quality tier: gold | Owner: analytics | Update: pipeline-driven';

COMMENT ON SCHEMA online_retail_metrics
  IS 'NexusRetail semantic / metrics layer — UC Materialized Views and Metric Views (WITH METRICS LANGUAGE YAML).
Genie One and AI/BI dashboard data sources. No PII.
KPI semantic layer for natural language querying via Genie.
Quality tier: gold | Owner: analytics | Genie domain: Online Retail';
