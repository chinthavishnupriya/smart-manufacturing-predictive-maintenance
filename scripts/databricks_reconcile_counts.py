#!/usr/bin/env python3
"""Reconcile row counts for the Smart Manufacturing Bronze/Silver/Gold tables.

Uses the Databricks SQL Statement Execution API. The SQL wait-timeout value
must be the SDK enum, not the string "CONTINUE".
"""
from __future__ import annotations

import os
import sys
import time

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import ExecuteStatementRequestOnWaitTimeout

WAREHOUSE_ID = os.getenv("DATABRICKS_WAREHOUSE_ID", "aa7aac0d7e51d340")
POLL_SECONDS = float(os.getenv("DATABRICKS_POLL_SECONDS", "3"))
WAREHOUSE_TIMEOUT_SECONDS = int(os.getenv("DATABRICKS_WAREHOUSE_TIMEOUT_SECONDS", "480"))
STATEMENT_TIMEOUT_SECONDS = int(os.getenv("DATABRICKS_STATEMENT_TIMEOUT_SECONDS", "180"))

TABLES = (
    "workspace.default.sensor_bronze",
    "workspace.default.sensor_silver",
    "workspace.default.sensor_invalid_records",
    "workspace.default.machine_health",
    "workspace.default.downtime_kpis",
)

COUNT_SQL = "\nUNION ALL\n".join(
    f"SELECT '{table.rsplit('.', 1)[-1]}' AS table_name, COUNT(*) AS row_count "
    f"FROM {table}"
    for table in TABLES
) + "\nORDER BY table_name"


def normalized_state(value: object) -> str:
    """Normalize SDK enum and string states to a comparable uppercase name."""
    raw_value = getattr(value, "value", value)
    return str(raw_value).split(".")[-1].upper()


def wait_for_warehouse(client: WorkspaceClient, warehouse_id: str) -> None:
    deadline = time.monotonic() + WAREHOUSE_TIMEOUT_SECONDS
    while True:
        warehouse = client.warehouses.get(id=warehouse_id)
        state = normalized_state(warehouse.state)
        print(f"Warehouse state: {state}", flush=True)
        if state == "RUNNING":
            return
        if state in {"FAILED", "DELETED"}:
            raise RuntimeError(f"Warehouse entered terminal state {state}.")
        if time.monotonic() >= deadline:
            raise TimeoutError(
                f"Warehouse did not become RUNNING within "
                f"{WAREHOUSE_TIMEOUT_SECONDS} seconds (state={state})."
            )
        time.sleep(10)


def main() -> int:
    client = WorkspaceClient()
    started_here = False

    try:
        warehouse = client.warehouses.get(id=WAREHOUSE_ID)
        state = normalized_state(warehouse.state)
        print(f"Warehouse: {warehouse.name} ({WAREHOUSE_ID}); state={state}")

        if state == "STOPPED":
            client.warehouses.start(id=WAREHOUSE_ID)
            started_here = True

        wait_for_warehouse(client, WAREHOUSE_ID)

        statement = client.statement_execution.execute_statement(
            warehouse_id=WAREHOUSE_ID,
            statement=COUNT_SQL,
            wait_timeout="50s",
            # Required by the Databricks SDK; a raw string raises
            # AttributeError: 'str' object has no attribute 'value'.
            on_wait_timeout=ExecuteStatementRequestOnWaitTimeout.CONTINUE,
        )

        deadline = time.monotonic() + STATEMENT_TIMEOUT_SECONDS
        while True:
            status = normalized_state(statement.status.state)
            if status == "SUCCEEDED":
                break
            if status in {"FAILED", "CANCELED", "CLOSED"}:
                error = getattr(statement.status, "error", None)
                raise RuntimeError(f"SQL statement {status}: {error}")
            if time.monotonic() >= deadline:
                raise TimeoutError(
                    f"SQL statement is still {status}; not rerunning the ETL job. "
                    f"statement_id={statement.statement_id}"
                )
            time.sleep(POLL_SECONDS)
            statement = client.statement_execution.get_statement(
                statement_id=statement.statement_id
            )

        rows = getattr(getattr(statement, "result", None), "data_array", None)
        if rows is None:
            raise RuntimeError("Statement succeeded but returned no result rows.")

        print("\nDatabricks table counts:")
        observed = set()
        for table_name, row_count in rows:
            print(f"{table_name}: {row_count} rows")
            observed.add(table_name)

        expected = {table.rsplit(".", 1)[-1] for table in TABLES}
        missing = expected - observed
        if missing:
            raise RuntimeError(f"Missing count results for: {', '.join(sorted(missing))}")

        print("\nCount query completed. Counts are reported for inspection; "
              "they do not alone prove Bronze/Silver/Gold grain reconciliation.")
        return 0

    except Exception as exc:
        print(f"RECONCILIATION NOT COMPLETED: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1

    finally:
        if started_here:
            try:
                client.warehouses.stop(id=WAREHOUSE_ID)
                print("Warehouse stop requested.", flush=True)
            except Exception as exc:
                print(f"WARNING: please stop warehouse manually: {exc}", file=sys.stderr)


if __name__ == "__main__":
    raise SystemExit(main())
