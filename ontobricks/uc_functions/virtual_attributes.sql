-- =============================================================================
-- NexusRetail — Unity Catalog functions for OntoBricks MCP tools
--
-- WHAT THIS IS
--   OntoBricks lets an ontology CLASS declare two kinds of live, on-demand
--   Unity Catalog functions, both reachable over MCP:
--     • VIRTUAL ATTRIBUTES — computed values NOT stored in the graph, fetched
--       via the `compute_virtual_attributes` MCP tool.
--     • ACTIONS — functions invoked on one entity via the `invoke_entity_action`
--       MCP tool (called with exactly one argument: the entity's ID).
--
--   These turn the graph from a static picture into something an AI agent can
--   both read AND compute against — the Tier C "agentic loop" payload.
--
-- HOW TO WIRE UP
--   1. Run this file (SQL warehouse) to create the functions in
--      ${var.catalog}.online_retail_metrics.
--   2. In OntoBricks: Ontology > class > Actions / Virtual Attributes, declare
--      the function on the matching class (Customer / Product).
--   3. In Domain Information > Global, ensure API/MCP is enabled and the
--      `compute_virtual_attributes` / `invoke_entity_action` tools are allowed
--      in the per-domain MCP policy.
--
-- SAFE BY DESIGN
--   Every function is READ-ONLY (SQL SELECT). No writes, no side effects — safe
--   to expose to an LLM agent. The graph triples remain the source of truth;
--   these just compute over the underlying silver tables on demand.
-- =============================================================================

USE CATALOG ${catalog};
USE SCHEMA online_retail_metrics;

-- -----------------------------------------------------------------------------
-- VIRTUAL ATTRIBUTE — Customer.faultyBatchExposureCount(customer_id)
-- How many of a customer's orders contained a FAULT-PHON-* product.
-- The core "who was exposed to the defect?" primitive.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION customer_faulty_batch_exposure(p_customer_id STRING)
RETURNS INT
COMMENT 'Virtual attribute: count of this customer''s orders that contained a faulty-batch (FAULT-PHON-*) product. Exposed on the Customer class via OntoBricks compute_virtual_attributes.'
RETURN (
  SELECT COUNT(DISTINCT o.order_id)
  FROM online_retail_silver.silver_fact_orders o
  JOIN online_retail_bronze.bronze_order_items oi ON oi.order_id = o.order_id
  JOIN online_retail_silver.silver_dim_products p ON p.product_id = oi.product_id
  WHERE o.customer_id = p_customer_id
    AND p.faulty_batch = true
);

-- -----------------------------------------------------------------------------
-- VIRTUAL ATTRIBUTE — Customer.returnRiskScore(customer_id)
-- A simple, explainable 0–100 risk score blending faulty-batch exposure and
-- the customer's realised return behaviour. Not stored — computed live.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION customer_return_risk_score(p_customer_id STRING)
RETURNS INT
COMMENT 'Virtual attribute: explainable 0-100 return-risk score = 60 * (has faulty-batch exposure) + 40 * (realised return rate). Exposed on the Customer class via OntoBricks compute_virtual_attributes.'
RETURN (
  WITH orders AS (
    SELECT COUNT(*) AS n_orders
    FROM online_retail_silver.silver_fact_orders
    WHERE customer_id = p_customer_id
  ),
  rets AS (
    SELECT COUNT(*) AS n_returns
    FROM online_retail_silver.silver_fact_returns
    WHERE customer_id = p_customer_id
  ),
  exposure AS (
    SELECT online_retail_metrics.customer_faulty_batch_exposure(p_customer_id) AS n_exposed
  )
  SELECT CAST(
    LEAST(100,
      60 * CASE WHEN (SELECT n_exposed FROM exposure) > 0 THEN 1 ELSE 0 END
      + 40 * COALESCE((SELECT n_returns FROM rets) / NULLIF((SELECT n_orders FROM orders), 0), 0)
    ) AS INT)
);

-- -----------------------------------------------------------------------------
-- ACTION — Product.affectedCustomerCohort(product_id)
-- Given a (faulty) product, return the cohort of customers who bought it, with
-- the explaining columns (order, region, whether they already returned).
-- Invoked on a Product entity via OntoBricks invoke_entity_action.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION product_affected_cohort(p_product_id STRING)
RETURNS TABLE (
  customer_id   STRING,
  order_id      STRING,
  region_name   STRING,
  order_date    DATE,
  already_returned BOOLEAN
)
COMMENT 'Action: explainable cohort of customers exposed to a given product, with region and whether they already returned it. Invoked on the Product class via OntoBricks invoke_entity_action.'
RETURN
  SELECT
    o.customer_id,
    o.order_id,
    o.region_name,
    o.order_date,
    (r.return_id IS NOT NULL) AS already_returned
  FROM online_retail_silver.silver_fact_orders o
  JOIN online_retail_bronze.bronze_order_items oi ON oi.order_id = o.order_id
  LEFT JOIN online_retail_silver.silver_fact_returns r
    ON r.order_id = o.order_id
   AND ARRAY_CONTAINS(r.returned_product_ids, p_product_id)
  WHERE oi.product_id = p_product_id;

-- -----------------------------------------------------------------------------
-- ACTION — Region.faultyBatchImpact(region_name)
-- Region-level rollup of the faulty-batch blast radius: exposed orders,
-- realised returns, refund exposure. Powers "which region was hit hardest?"
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION region_faulty_batch_impact(p_region_name STRING)
RETURNS TABLE (
  region_name        STRING,
  exposed_orders     BIGINT,
  faulty_returns     BIGINT,
  refund_exposure_usd DECIMAL(18,2)
)
COMMENT 'Action: faulty-batch impact rollup for a region — exposed orders, faulty returns, and refund exposure. Invoked on the Region class via OntoBricks invoke_entity_action.'
RETURN
  WITH exposed AS (
    SELECT DISTINCT o.order_id, o.region_name
    FROM online_retail_silver.silver_fact_orders o
    JOIN online_retail_bronze.bronze_order_items oi ON oi.order_id = o.order_id
    JOIN online_retail_silver.silver_dim_products p ON p.product_id = oi.product_id
    WHERE p.faulty_batch = true AND o.region_name = p_region_name
  )
  SELECT
    p_region_name AS region_name,
    (SELECT COUNT(*) FROM exposed) AS exposed_orders,
    COUNT(r.return_id) AS faulty_returns,
    COALESCE(SUM(r.refund_amount), 0) AS refund_exposure_usd
  FROM online_retail_silver.silver_fact_returns r
  WHERE r.region_name = p_region_name
    AND r.faulty_batch_involved = true;
