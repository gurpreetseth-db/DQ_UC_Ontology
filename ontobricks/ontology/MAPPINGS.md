# NexusRetail Ontology → Data Mapping Crib

This is the authoritative map from every ontology class and property in
[`nexusretail.ttl`](./nexusretail.ttl) to the Unity Catalog table + column that
supplies it. Use it two ways:

1. **Guide OntoBricks Auto-Map** — after importing the ontology and importing UC
   metadata, run *Mapping → Auto-Map*, then reconcile the LLM's suggestions
   against this table so the materialised graph is deterministic and matches the
   Genie concept Pages.
2. **Author manual R2RML** — if you prefer full control, each row below is one
   R2RML `TriplesMap` (subject) or `PredicateObjectMap` (property).

All tables live in catalog `${var.catalog}` (your `databricks.local.yml`), schema
`online_retail_silver` unless noted. Silver is the correct grain: one row = one
entity instance = clean triples. Gold/metrics stay with Genie + metric views.

---

## Subject maps (classes → tables + IRI templates)

| Class | Source table | Row → instance IRI (subject) |
|---|---|---|
| `nr:Customer` | `silver_dim_customers` | `…/Customer/{customer_id}` |
| `nr:Order` | `silver_fact_orders` | `…/Order/{order_id}` |
| `nr:Product` | `silver_dim_products` | `…/Product/{product_id}` |
| `nr:Invoice` | `silver_fact_invoices` | `…/Invoice/{invoice_id}` |
| `nr:Return` | `silver_fact_returns` | `…/Return/{return_id}` |
| `nr:Category` | `silver_dim_products` (distinct `category_id`) | `…/Category/{category_id}` |
| `nr:Subcategory` | `silver_dim_products` (distinct `subcategory_id`) | `…/Subcategory/{subcategory_id}` |
| `nr:Region` | `silver_dim_geography` (distinct `region_name`) | `…/Region/{region_name}` |
| `nr:Country` | `silver_dim_geography` (distinct `country_code`) | `…/Country/{country_code}` |
| `nr:FaultyBatch` | *constant* (one instance) | `…/FaultyBatch/FAULT-PHON-2025Q3` |

---

## Datatype properties (attributes → columns)

### Customer — `silver_dim_customers`
| Property | Column |
|---|---|
| `nr:customerId` | `customer_id` |
| `nr:customerType` | `customer_type` |
| `nr:loyaltyTier` | `loyalty_tier` |
| `nr:ageBracket` | `age_bracket` |
| `nr:acquisitionChannel` | `acquisition_channel` |
| `nr:npsScore` | `nps_score` |
| `nr:customerActive` | `is_active` |

> PII columns (`full_name`, `email`, `phone`, `date_of_birth`, `annual_income_usd`)
> are intentionally **not** mapped into the graph — they are masked by UC column
> masks and the graph is meant to be broadly explorable. Keep them out of triples.

### Order — `silver_fact_orders`
| Property | Column |
|---|---|
| `nr:orderId` | `order_id` |
| `nr:orderDate` | `order_date` |
| `nr:channel` | `channel` |
| `nr:orderStatus` | `status` |
| `nr:orderTotal` | `order_total` |
| `nr:itemCount` | `item_count` |

### Product — `silver_dim_products`
| Property | Column |
|---|---|
| `nr:productId` | `product_id` |
| `nr:sku` | `sku` |
| `nr:productName` | `product_name` |
| `nr:brand` | `brand` |
| `nr:currentPrice` | `current_price` |
| `nr:faultyBatch` | `faulty_batch` |
| `nr:productReturnRate` | `avg_return_rate` |

### Invoice — `silver_fact_invoices`
| Property | Column |
|---|---|
| `nr:invoiceId` | `invoice_id` |
| `nr:invoiceNumber` | `invoice_number` |
| `nr:issueDate` | `issue_date` |
| `nr:dueDate` | `due_date` |
| `nr:invoiceStatus` | `invoice_status` |
| `nr:invoiceTotal` | `invoice_total` |
| `nr:isOverdue` | `is_overdue` |
| `nr:totalReconciled` | `total_reconciled` |

### Return — `silver_fact_returns`
| Property | Column |
|---|---|
| `nr:returnId` | `return_id` |
| `nr:returnDate` | `return_date` |
| `nr:returnReasonCode` | `return_reason_code` |
| `nr:returnCategory` | `return_category` |
| `nr:returnStatus` | `return_status` |
| `nr:refundAmount` | `refund_amount` |
| `nr:faultyBatchInvolved` | `faulty_batch_involved` |

### Category / Subcategory / Region / Country / FaultyBatch
| Property | Source column |
|---|---|
| `nr:categoryName` | `silver_dim_products.category_name` |
| `nr:categoryReturnRate` | `silver_dim_products.avg_return_rate` |
| `nr:subcategoryName` | `silver_dim_products.subcategory_name` |
| `nr:regionName` | `silver_dim_geography.region_name` |
| `nr:superRegion` | `silver_dim_geography.super_region` |
| `nr:countryCode` | `silver_dim_geography.country_code` |
| `nr:batchId` | constant `FAULT-PHON-2025Q3` |
| `nr:batchDefectType` | constant `smartphone_defect` |

---

## Object properties (relationships → join keys)

| Relationship | From → To | Join / rule |
|---|---|---|
| `nr:placedOrder` / `nr:placedBy` | Customer ↔ Order | `silver_fact_orders.customer_id = silver_dim_customers.customer_id` |
| `nr:containsProduct` | Order → Product | via `online_retail_bronze.bronze_order_items` (`order_id`, `product_id`) — the line-item bridge |
| `nr:billedByInvoice` / `nr:invoicesOrder` | Order ↔ Invoice | `silver_fact_invoices.order_id = silver_fact_orders.order_id` |
| `nr:hasReturn` / `nr:returnsOrder` | Order ↔ Return | `silver_fact_returns.order_id = silver_fact_orders.order_id` |
| `nr:returnsProduct` | Return → Product | explode `silver_fact_returns.returned_product_ids` (array) → `product_id` |
| `nr:inCategory` | Product → Category | `silver_dim_products.category_id` |
| `nr:inSubcategory` | Product → Subcategory | `silver_dim_products.subcategory_id` |
| `nr:subcategoryOf` | Subcategory → Category | `silver_dim_products` (`subcategory_id` → `category_id`) |
| `nr:memberOfBatch` | Product → FaultyBatch | **filter** `silver_dim_products.faulty_batch = true` → constant `FAULT-PHON-2025Q3`. Only these 8 rows get the edge. |
| `nr:customerLocatedIn` | Customer → Region | `silver_dim_customers.country_code` → `silver_dim_geography.region_name` |
| `nr:orderShippedToRegion` | Order → Region | `silver_fact_orders.region_name` |
| `nr:countryInRegion` | Country → Region | `silver_dim_geography` (`country_code` → `region_name`) |

---

## The narrative traversal (why this modelling matters)

The Q4-2025 return-spike story becomes a single explainable path the graph can
walk and an AI agent can narrate:

```
FaultyBatch(FAULT-PHON-2025Q3)
   ▲ memberOfBatch
Product(FAULT-PHON-*)  ◄──returnsProduct── Return  ──returnsOrder──►  Order
   ▲ containsProduct                                                   │ placedBy
   └──────────────────────── Order ─────────────────────────►  Customer ──customerLocatedIn──► Region(APAC-East)
```

- **N-hop expansion** from the `FAULT-PHON-2025Q3` node surfaces the entire blast
  radius (products → orders → customers → region) in one query.
- **Cohort discovery** returns the impacted customer set *with the path that
  explains why* each customer is in it.
- The **SHACL shapes** in `../shacl/faulty_batch_shapes.ttl` validate the
  narrative's integrity (e.g. a `faulty_product` return must trace to a
  batch-member product).
