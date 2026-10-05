"""SQL access via the Databricks SQL Statement Execution API.

Uses databricks-sdk WorkspaceClient. In the Databricks Apps runtime the app's
service principal credentials are injected; locally we fall back to a CLI profile.
All user input is bound as named parameters (:name) — never string-formatted.
"""
from __future__ import annotations

import os
from functools import lru_cache
from typing import Any

from . import config


@lru_cache(maxsize=1)
def client():
    from databricks.sdk import WorkspaceClient
    if os.getenv("DATABRICKS_CLIENT_ID"):
        return WorkspaceClient()
    return WorkspaceClient(profile=config.PROFILE)


def query(sql: str, params: dict[str, Any] | None = None) -> list[dict]:
    """Run a SQL statement and return rows as dicts keyed by column name."""
    from databricks.sdk.service.sql import StatementParameterListItem

    if not config.WAREHOUSE_ID:
        raise RuntimeError("DATABRICKS_WAREHOUSE_ID is not set")
    ps = [StatementParameterListItem(name=k, value=None if v is None else str(v))
          for k, v in (params or {}).items()]
    resp = client().statement_execution.execute_statement(
        warehouse_id=config.WAREHOUSE_ID,
        statement=sql,
        parameters=ps or None,
        wait_timeout="50s",
    )
    state = resp.status.state.value if resp.status and resp.status.state else "UNKNOWN"
    if state != "SUCCEEDED":
        msg = resp.status.error.message if resp.status and resp.status.error else state
        raise RuntimeError(f"SQL failed: {msg}")
    if not resp.result or not resp.result.data_array:
        return []
    cols = [c.name for c in resp.manifest.schema.columns]
    types = [c.type_name.value if c.type_name else "STRING" for c in resp.manifest.schema.columns]
    return [{c: _cast(v, t) for c, v, t in zip(cols, row, types)} for row in resp.result.data_array]


_NUMERIC = {"INT", "LONG", "SHORT", "BYTE"}
_FLOAT = {"DOUBLE", "FLOAT", "DECIMAL"}


def _cast(v: Any, t: str) -> Any:
    # The Statements API returns every value as a string; restore JSON types for the UI.
    if v is None:
        return None
    if t in _NUMERIC:
        return int(v)
    if t in _FLOAT:
        return float(v)
    if t == "BOOLEAN":
        return v.lower() == "true"
    return v
