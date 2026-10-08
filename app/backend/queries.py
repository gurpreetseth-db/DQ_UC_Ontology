"""Every SQL statement the Support Console runs, in one place.

Filters are composed from a fixed allow-list of SQL fragments; only the values
are bound as named parameters. Each table has different column names for the
same concept (e.g. `region_name` vs the metric view's `Region`), so each query
declares which filter columns it supports via a `cols` mapping.
"""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from typing import Any, Callable

from .config import gold, metrics
from .db import query


@dataclass
class Filters:
    region: str | None = None        # region_name, e.g. EMEA-West
    super_region: str | None = None  # e.g. EMEA
    category: str | None = None      # category_name
    channel: str | None = None       # web | mobile | partner_api
    start_date: str | None = None    # YYYY-MM-DD inclusive
    end_date: str | None = None      # YYYY-MM-DD inclusive


def _parallel(**jobs: Callable[[], Any]) -> dict[str, Any]:
    """Run independent statements concurrently — a page's queries each take ~1s on the warehouse."""
    with ThreadPoolExecutor(max_workers=len(jobs)) as pool:
        futures = {k: pool.submit(fn) for k, fn in jobs.items()}
        return {k: fut.result() for k, fut in futures.items()}


def _where(f: Filters, cols: dict[str, str], month_grain: bool = False) -> tuple[str, dict]:
    """Build a WHERE clause for the filters this table supports.

    cols maps a Filters field → the (already quoted) column it applies to; a
    special "date" key names the date/timestamp column. On month-grain tables
    the start date is truncated to the month so a mid-month start still
    includes that month.
    """
    clauses, params = [], {}
    for field in ("region", "super_region", "category", "channel"):
        val = getattr(f, field)
        if val and field in cols:
            clauses.append(f"{cols[field]} = :{field}")
            params[field] = val
    if "date" in cols:
        if f.start_date:
            start = "DATE_TRUNC('MONTH', CAST(:start_date AS DATE))" if month_grain else "CAST(:start_date AS DATE)"
            clauses.append(f"CAST({cols['date']} AS DATE) >= {start}")
            params["start_date"] = f.start_date
        if f.end_date:
            clauses.append(f"CAST({cols['date']} AS DATE) <= CAST(:end_date AS DATE)")
            params["end_date"] = f.end_date
    return ("WHERE " + " AND ".join(clauses)) if clauses else "", params


def _order_where(f: Filters, extra: list[str] | None = None) -> tuple[str, dict]:
    """WHERE for gold_order_details; category filters via the order's lines."""
    where, params = _where(f, {"region": "o.region_name", "super_region": "o.super_region",
                               "channel": "o.channel", "date": "o.order_date"})
    clauses = [where[len("WHERE "):]] if where else []
    if f.category:
        clauses.append(f"o.order_id IN (SELECT order_id FROM {gold('gold_order_lines')} "
                       f"WHERE category_name = :category)")
        params["category"] = f.category
    clauses += extra or []
    return ("WHERE " + " AND ".join(clauses)) if clauses else "", params


# ── Filter options ──────────────────────────────────────────────────────────

def filter_options() -> dict[str, Any]:
    regions = query(f"SELECT DISTINCT region_name, super_region FROM {gold('gold_regional_performance')} "
                    f"ORDER BY super_region, region_name")
    categories = query(f"SELECT DISTINCT category_name FROM {gold('gold_product_catalog')} ORDER BY 1")
    meta = query(f"SELECT MIN(order_date) AS min_date, MAX(order_date) AS max_date, "
                 f"ARRAY_JOIN(ARRAY_SORT(COLLECT_SET(channel)), ',') AS channels, "
                 f"ARRAY_JOIN(ARRAY_SORT(COLLECT_SET(order_status)), ',') AS statuses "
                 f"FROM {gold('gold_order_details')}")[0]
    return {
        "regions": regions,
        "super_regions": sorted({r["super_region"] for r in regions}),
        "categories": [c["category_name"] for c in categories],
        "channels": (meta["channels"] or "").split(","),
        "statuses": (meta["statuses"] or "").split(","),
        "min_date": meta["min_date"],
        "max_date": meta["max_date"],
    }


# ── Overview ────────────────────────────────────────────────────────────────

def overview(f: Filters) -> dict[str, Any]:
    where, p = _order_where(f)
    src = f"{gold('gold_order_details')} o {where}"
    kpis_sql = f"""
        SELECT
          COUNT(*)                                                        AS orders,
          ROUND(SUM(CASE WHEN order_status <> 'cancelled' THEN order_total END), 2) AS revenue,
          ROUND(AVG(CASE WHEN order_status <> 'cancelled' THEN order_total END), 2) AS avg_order_value,
          COUNT(DISTINCT customer_id)                                     AS customers,
          ROUND(100.0 * COUNT_IF(has_return) / NULLIF(COUNT(*), 0), 2)    AS return_rate_pct,
          ROUND(100.0 * COUNT_IF(order_status = 'cancelled') / NULLIF(COUNT(*), 0), 2) AS cancellation_rate_pct,
          COUNT_IF(order_status IN ('pending', 'confirmed'))              AS open_orders,
          COUNT_IF(invoice_overdue)                                       AS overdue_invoices,
          ROUND(COALESCE(SUM(refund_amount), 0), 2)                       AS refunds
        FROM {src}"""
    trend_sql = f"""
        SELECT DATE_TRUNC('MONTH', order_date) AS month,
               COUNT(*) AS orders,
               ROUND(SUM(CASE WHEN order_status <> 'cancelled' THEN order_total END), 2) AS revenue
        FROM {src} GROUP BY 1 ORDER BY 1"""
    status_sql = f"SELECT order_status, COUNT(*) AS orders FROM {src} GROUP BY 1 ORDER BY 2 DESC"
    res = _parallel(kpis=lambda: query(kpis_sql, p), trend=lambda: query(trend_sql, p),
                    by_status=lambda: query(status_sql, p))
    res["kpis"] = res["kpis"][0]
    return res


# ── Orders ──────────────────────────────────────────────────────────────────

ORDER_COLS = ("order_id, customer_id, order_date, estimated_delivery, channel, order_status, "
              "region_name, super_region, country_code, item_count, order_total, invoice_number, "
              "invoice_status, invoice_overdue, has_return, return_status, refund_amount")


def list_orders(f: Filters, status: str | None, search: str | None,
                limit: int, offset: int) -> dict[str, Any]:
    extra, extra_p = [], {}
    if status:
        extra.append("o.order_status = :status")
        extra_p["status"] = status
    if search:
        extra.append("(o.order_id ILIKE :search OR o.customer_id ILIKE :search OR o.invoice_number ILIKE :search)")
        extra_p["search"] = f"%{search.strip()}%"
    where, p = _order_where(f, extra)
    p.update(extra_p)
    total = query(f"SELECT COUNT(*) AS n FROM {gold('gold_order_details')} o {where}", p)[0]["n"]
    rows = query(f"""
        SELECT {ORDER_COLS} FROM {gold('gold_order_details')} o {where}
        ORDER BY order_ts DESC, order_id
        LIMIT {int(limit)} OFFSET {int(offset)}""", p)
    return {"total": total, "rows": rows}


def order_detail(order_id: str) -> dict[str, Any] | None:
    head = query(f"SELECT * FROM {gold('gold_order_details')} WHERE order_id = :order_id",
                 {"order_id": order_id})
    if not head:
        return None
    lines = query(f"""
        SELECT line_id, product_id, sku, product_name, brand, category_name, subcategory_name,
               faulty_batch, quantity, unit_price, discount_pct, line_total
        FROM {gold('gold_order_lines')} WHERE order_id = :order_id ORDER BY line_id""",
                  {"order_id": order_id})
    return {"order": head[0], "lines": lines}


# ── Customers ───────────────────────────────────────────────────────────────

def search_customers(search: str) -> list[dict]:
    # Driven off orders (not CLV) so customers with only pending/cancelled orders still match.
    return query(f"""
        SELECT o.customer_id,
               COUNT(*) AS orders,
               MAX(o.order_date) AS last_order_date,
               MAX(o.region_name) AS region_name,
               MAX(c.clv_segment) AS clv_segment,
               MAX(c.loyalty_tier) AS loyalty_tier
        FROM {gold('gold_order_details')} o
        LEFT JOIN {gold('gold_customer_lifetime_value')} c ON o.customer_id = c.customer_id
        WHERE o.customer_id ILIKE :search
        GROUP BY o.customer_id ORDER BY o.customer_id LIMIT 25""",
                 {"search": f"%{search.strip()}%"})


def customer_detail(customer_id: str) -> dict[str, Any] | None:
    p = {"customer_id": customer_id}
    res = _parallel(
        orders=lambda: query(f"""SELECT {ORDER_COLS} FROM {gold('gold_order_details')}
                                 WHERE customer_id = :customer_id ORDER BY order_ts DESC""", p),
        profile=lambda: query(f"SELECT * FROM {gold('gold_customer_lifetime_value')} "
                              f"WHERE customer_id = :customer_id", p),
        top_categories=lambda: query(f"""
            SELECT category_name, SUM(quantity) AS units, ROUND(SUM(line_total), 2) AS spend
            FROM {gold('gold_order_lines')} WHERE customer_id = :customer_id
            GROUP BY 1 ORDER BY spend DESC LIMIT 5""", p),
    )
    if not res["orders"]:
        return None
    # CLV only covers customers with a delivered/shipped/confirmed order.
    res["profile"] = res["profile"][0] if res["profile"] else None
    return res


# ── Products ────────────────────────────────────────────────────────────────

def list_products(search: str | None, category: str | None, faulty_only: bool,
                  sort: str) -> list[dict]:
    clauses, p = [], {}
    if search:
        clauses.append("(product_name ILIKE :search OR sku ILIKE :search OR product_id ILIKE :search "
                       "OR brand ILIKE :search)")
        p["search"] = f"%{search.strip()}%"
    if category:
        clauses.append("category_name = :category")
        p["category"] = category
    if faulty_only:
        clauses.append("faulty_batch")
    where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
    order_by = {"revenue": "gross_revenue DESC", "returns": "return_rate_pct DESC NULLS LAST",
                "name": "product_name"}.get(sort, "gross_revenue DESC")
    return query(f"""
        SELECT product_id, sku, product_name, brand, category_name, subcategory_name,
               current_price, faulty_batch, is_active, units_sold, gross_revenue,
               return_count, return_rate_pct
        FROM {gold('gold_product_catalog')} {where} ORDER BY {order_by} LIMIT 500""", p)


def product_detail(product_id: str) -> dict[str, Any] | None:
    p = {"product_id": product_id}
    res = _parallel(
        product=lambda: query(f"SELECT * FROM {gold('gold_product_catalog')} "
                              f"WHERE product_id = :product_id", p),
        return_reasons=lambda: query(f"""
            SELECT return_reason_code, return_category, SUM(return_count) AS returns,
                   ROUND(SUM(total_refund_amount), 2) AS refunds,
                   ROUND(AVG(avg_days_to_return), 1) AS avg_days
            FROM {gold('gold_return_analysis')} WHERE product_id = :product_id
            GROUP BY 1, 2 ORDER BY returns DESC""", p),
        monthly=lambda: query(f"""
            SELECT DATE_TRUNC('MONTH', order_date) AS month, SUM(quantity) AS units,
                   ROUND(SUM(line_total), 2) AS revenue
            FROM {gold('gold_order_lines')} WHERE product_id = :product_id
            GROUP BY 1 ORDER BY 1""", p),
    )
    if not res["product"]:
        return None
    res["product"] = res["product"][0]
    return res


# ── Sales & revenue (semantic layer) ────────────────────────────────────────

def sales(f: Filters) -> dict[str, Any]:
    # Category × region × month — mv_category_revenue (metrics schema)
    cw, cp = _where(f, {"region": "region_name", "super_region": "super_region",
                        "category": "category_name", "date": "sale_month"}, month_grain=True)
    by_category_sql = f"""
        SELECT category_name, SUM(order_count) AS orders, SUM(units_sold) AS units,
               ROUND(SUM(gross_revenue), 2) AS gross_revenue, ROUND(SUM(net_revenue), 2) AS net_revenue,
               ROUND(SUM(total_discount), 2) AS discounts, ROUND(SUM(refund_total), 2) AS refunds,
               SUM(return_count) AS returns
        FROM {metrics('mv_category_revenue')} {cw}
        GROUP BY 1 ORDER BY gross_revenue DESC"""
    matrix_sql = f"""
        SELECT category_name, region_name, ROUND(SUM(gross_revenue), 2) AS gross_revenue
        FROM {metrics('mv_category_revenue')} {cw}
        GROUP BY 1, 2"""
    # Region × month — mv_regional_orders (no category/channel dimension)
    rw, rp = _where(f, {"region": "region_name", "super_region": "super_region",
                        "date": "sale_month"}, month_grain=True)
    by_region_sql = f"""
        SELECT region_name, super_region, SUM(order_count) AS orders,
               ROUND(SUM(gross_revenue), 2) AS gross_revenue, ROUND(SUM(net_revenue), 2) AS net_revenue,
               ROUND(SUM(refund_total), 2) AS refunds, SUM(cancelled_orders) AS cancelled_orders,
               ROUND(SUM(gross_revenue) / NULLIF(SUM(order_count), 0), 2) AS avg_order_value
        FROM {metrics('mv_regional_orders')} {rw}
        GROUP BY 1, 2 ORDER BY gross_revenue DESC"""
    # Monthly trend by channel — certified metric view, measures via MEASURE()
    mw, mp = _where(f, {"region": "`Region`", "super_region": "`Super Region`",
                        "channel": "`Channel`", "date": "`Sale Month`"}, month_grain=True)
    trend_sql = f"""
        SELECT `Sale Month` AS month, `Channel` AS channel,
               ROUND(MEASURE(`Gross Revenue`), 2) AS gross_revenue,
               MEASURE(`Order Count`) AS orders
        FROM {metrics('metrics_sales_kpis')} {mw}
        GROUP BY ALL ORDER BY 1, 2"""
    res = _parallel(by_category=lambda: query(by_category_sql, cp),
                    matrix=lambda: query(matrix_sql, cp),
                    by_region=lambda: query(by_region_sql, rp),
                    trend=lambda: query(trend_sql, mp))
    return {**res, "notes": {
                "by_category": "Category filter applies; channel does not (mv_category_revenue has no channel).",
                "by_region": "Region/date filters apply; category and channel do not (mv_regional_orders).",
                "trend": "From the certified metric view metrics_sales_kpis; category filter does not apply.",
            }}
