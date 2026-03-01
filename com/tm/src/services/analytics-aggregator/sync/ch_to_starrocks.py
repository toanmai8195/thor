"""
Sync silver_payments từ ClickHouse → StarRocks qua Stream Load API.
Chạy sau khi dbt_clickhouse hoàn thành silver layer.
"""

import os
import json
import logging
import requests
from datetime import date, timedelta
from typing import Optional

CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "clickhouse")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", "8123"))
STARROCKS_HOST = os.getenv("STARROCKS_HOST", "starrocks-fe")
STARROCKS_HTTP_PORT = int(os.getenv("STARROCKS_HTTP_PORT", "8030"))
STARROCKS_DB = os.getenv("STARROCKS_DB", "analytics")
STARROCKS_USER = os.getenv("STARROCKS_USER", "root")
STARROCKS_PASSWORD = os.getenv("STARROCKS_PASSWORD", "")

COLUMNS = ["event_id", "user_id", "amount", "status", "payment_date", "updated_at"]

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def export_from_clickhouse(sync_date: Optional[date]) -> str:
    """Xuất silver_payments từ ClickHouse dưới dạng JSON Lines."""
    cols = ", ".join(COLUMNS)
    sql = f"SELECT {cols} FROM tracking.silver_payments"
    if sync_date:
        sql += f" WHERE payment_date = '{sync_date}'"
    sql += " FORMAT JSONEachRow"

    resp = requests.get(
        f"http://{CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}/",
        params={"query": sql, "user": "default", "password": ""},
        timeout=600,
    )
    resp.raise_for_status()
    return resp.text


def load_to_starrocks(data_jsonl: str) -> int:
    """Ghi data vào StarRocks silver_payments qua Stream Load."""
    if not data_jsonl.strip():
        return 0

    jsonpaths = json.dumps([f"$.{c}" for c in COLUMNS])
    url = f"http://{STARROCKS_HOST}:{STARROCKS_HTTP_PORT}/api/{STARROCKS_DB}/silver_payments/_stream_load"

    resp = requests.put(
        url,
        data=data_jsonl.encode("utf-8"),
        headers={
            "format": "json",
            "strip_outer_array": "false",
            "jsonpaths": jsonpaths,
            "Content-Type": "application/json",
        },
        auth=(STARROCKS_USER, STARROCKS_PASSWORD),
        timeout=600,
    )
    resp.raise_for_status()
    result = resp.json()

    if result.get("Status") not in ("Success", "Publish Timeout"):
        raise RuntimeError(f"Stream Load failed: {result.get('Message')}")

    loaded = result.get("NumberLoadedRows", 0)
    logger.info(f"Loaded {loaded} rows → silver_payments")
    return loaded


def run_sync(sync_date: Optional[date] = None, **context) -> int:
    """Airflow callable: sync silver_payments CH → StarRocks."""
    if sync_date is None:
        ds = context.get("ds")
        sync_date = date.fromisoformat(ds) if ds else date.today() - timedelta(days=1)

    logger.info(f"Syncing silver_payments for date={sync_date}")
    data = export_from_clickhouse(sync_date)
    return load_to_starrocks(data)


if __name__ == "__main__":
    import sys
    d = date.fromisoformat(sys.argv[1]) if len(sys.argv) > 1 else None
    run_sync(d)
