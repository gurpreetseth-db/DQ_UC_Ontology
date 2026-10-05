"""Runtime configuration for the Support Console backend (env-driven).

Every value is injected by the bundle (databricks.yml → app config env). The
defaults only exist so `uvicorn backend.main:app` works for local dev.
"""
import os

CATALOG = os.getenv("CATALOG", "gsethi")
GOLD_SCHEMA = os.getenv("GOLD_SCHEMA", "online_retail_gold")
METRICS_SCHEMA = os.getenv("METRICS_SCHEMA", "online_retail_metrics")
WAREHOUSE_ID = os.getenv("DATABRICKS_WAREHOUSE_ID", "").strip()
GENIE_SPACE_ID = os.getenv("GENIE_SPACE_ID", "").strip()
# Local dev falls back to this CLI profile; in Databricks Apps the SDK is auto-authed.
PROFILE = os.getenv("DATABRICKS_CONFIG_PROFILE", "Myenv")


def gold(name: str) -> str:
    return f"`{CATALOG}`.`{GOLD_SCHEMA}`.`{name}`"


def metrics(name: str) -> str:
    return f"`{CATALOG}`.`{METRICS_SCHEMA}`.`{name}`"
