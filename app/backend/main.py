"""NexusRetail Support Console backend: FastAPI JSON API + static SPA hosting.

Reads only the configured gold and metrics schemas and proxies
the NexusRetail Analytics Genie space.
"""
from __future__ import annotations

import logging
import pathlib

from fastapi import Depends, FastAPI, HTTPException, Query, Request
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field
from starlette.exceptions import HTTPException as StarletteHTTPException

from . import config, genie, queries
from .queries import Filters

log = logging.getLogger("support-console")
app = FastAPI(title="NexusRetail Support Console", version="0.1.0")

DATE_RE = r"^\d{4}-\d{2}-\d{2}$"


def filters(
    region: str | None = None,
    super_region: str | None = None,
    category: str | None = None,
    channel: str | None = None,
    start_date: str | None = Query(None, pattern=DATE_RE),
    end_date: str | None = Query(None, pattern=DATE_RE),
) -> Filters:
    return Filters(region or None, super_region or None, category or None, channel or None,
                   start_date or None, end_date or None)


class GenieRequest(BaseModel):
    message: str = Field(min_length=1, max_length=2000)
    conversation_id: str | None = None


@app.exception_handler(RuntimeError)
def runtime_error(_: Request, exc: RuntimeError):
    log.exception("backend error")
    return JSONResponse(status_code=502, content={"detail": str(exc)})


# ── Health / identity ───────────────────────────────────────────────────────

@app.get("/api/health")
def health():
    return {"status": "ok", "catalog": config.CATALOG, "gold_schema": config.GOLD_SCHEMA,
            "metrics_schema": config.METRICS_SCHEMA, "warehouse_configured": bool(config.WAREHOUSE_ID),
            "genie_configured": genie.is_configured()}


@app.get("/api/whoami")
def whoami(request: Request):
    return {"email": request.headers.get("x-forwarded-email") or "local-dev",
            "user": request.headers.get("x-forwarded-preferred-username")}


# ── Data ────────────────────────────────────────────────────────────────────

@app.get("/api/filters")
def get_filters():
    return queries.filter_options()


@app.get("/api/overview")
def get_overview(f: Filters = Depends(filters)):
    return queries.overview(f)


@app.get("/api/orders")
def get_orders(f: Filters = Depends(filters), status: str | None = None, q: str | None = None,
               limit: int = Query(50, ge=1, le=500), offset: int = Query(0, ge=0)):
    return queries.list_orders(f, status or None, q or None, limit, offset)


@app.get("/api/orders/{order_id}")
def get_order(order_id: str):
    res = queries.order_detail(order_id)
    if not res:
        raise HTTPException(404, f"Order {order_id} not found")
    return res


@app.get("/api/customers")
def get_customers(q: str = Query(..., min_length=1)):
    return queries.search_customers(q)


@app.get("/api/customers/{customer_id}")
def get_customer(customer_id: str):
    res = queries.customer_detail(customer_id)
    if not res:
        raise HTTPException(404, f"Customer {customer_id} has no orders")
    return res


@app.get("/api/products")
def get_products(q: str | None = None, category: str | None = None, faulty_only: bool = False,
                 sort: str = "revenue"):
    return queries.list_products(q or None, category or None, faulty_only, sort)


@app.get("/api/products/{product_id}")
def get_product(product_id: str):
    res = queries.product_detail(product_id)
    if not res:
        raise HTTPException(404, f"Product {product_id} not found")
    return res


@app.get("/api/sales")
def get_sales(f: Filters = Depends(filters)):
    return queries.sales(f)


@app.post("/api/genie/ask")
def genie_ask(req: GenieRequest):
    try:
        return genie.ask(req.message, req.conversation_id)
    except Exception as exc:  # surface Genie/permission errors to the chat panel
        log.exception("genie error")
        raise HTTPException(502, f"Genie request failed: {exc}") from exc


# ── SPA ─────────────────────────────────────────────────────────────────────

class SPAStaticFiles(StaticFiles):
    """Serve index.html for unknown non-API paths so client-side routes deep-link."""

    async def get_response(self, path, scope):
        try:
            return await super().get_response(path, scope)
        except StarletteHTTPException as exc:
            if exc.status_code == 404 and not path.startswith("api"):
                return await super().get_response("index.html", scope)
            raise


_DIST = pathlib.Path(__file__).resolve().parent.parent / "frontend" / "dist"
if _DIST.exists():
    app.mount("/", SPAStaticFiles(directory=str(_DIST), html=True), name="spa")
