#!/usr/bin/env python3
"""Reconcile Databricks Bronze/Silver/Gold table counts and machine grain.

Default mode validates the current materialized snapshot. Optional --run-id
filtering is only enabled when the caller identifies a real common run-id
column that exists in every table; no column name is assumed.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import re
import sys
import time
from typing import Any

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

IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def normalized_state(value: object) -> str:
    """Normalize SDK enum and string states to a comparable uppercase name."""
    raw_value = getattr(value, "value", value)
    return str(raw_value).split(".")[-1].upper()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--warehouse-id", default=WAREHOUSE_ID)
    parser.add_argument("--run-id", help="Optional exact run ID to filter all five tables.")
    parser.add_argument(
        "--run-id-column",
        help="Actual common column containing the run ID. Required with --run-id; must exist in every table.",
    )
    parser.add_argument(
        "--output",
        help="Optional JSON output path. Default: outputs/databricks-reconciliation-<UTC timestamp>.json",
    )
    args = parser.parse_args()
    if bool(args.run_id) != bool(args.run_id_column):
        parser.error("--run-id and --run-id-column must be supplied together.")
    if args.run_id_column and not IDENTIFIER_RE.fullmatch(args.run_id_column):
        parser.error("--run-id-column must be a plain SQL column identifier.")
    return args


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


def execute_sql(client: WorkspaceClient, warehouse_id: str, sql: str) -> list[list[Any]]:
    """Execute one statement and return its result rows, raising on errors/timeouts."""
    statement = client.statement_execution.execute_statement(
        warehouse_id=warehouse_id,
        statement=sql,
        wait_timeout="50s",
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
                f"SQL statement remains {status}; statement_id={statement.statement_id}"
            )
        time.sleep(POLL_SECONDS)
        statement = client.statement_execution.get_statement(
            statement_id=statement.statement_id
        )
    result_rows = getattr(getattr(statement, "result", None), "data_array", None)
    if result_rows is None:
        raise RuntimeError(f"SQL statement returned no rows: {sql}")
    return result_rows


def sql_literal(value: str) -> str:
    """Escape a string as a SQL string literal."""
    return "'" + value.replace("'", "''") + "'"


def run_filter_for_tables(
    client: WorkspaceClient, warehouse_id: str, run_id: str, column: str
) -> dict[str, str]:
    """Verify the selected run ID column exists in every table before filtering."""
    filters = {}
    for table in TABLES:
        rows = execute_sql(client, warehouse_id, f"SHOW COLUMNS IN {table}")
        columns = {
            str(row[0]).strip().lower()
            for row in rows
            if row and row[0] is not None
        }
        if column.lower() not in columns:
            raise RuntimeError(
                f"Cannot do run-scoped reconciliation: column {column!r} "
                f"is absent from {table}. Columns returned: {sorted(columns)}"
            )
        filters[table] = f" WHERE `{column}` = {sql_literal(run_id)}"
    return filters


def validate_counts(counts: dict[str, dict[str, int]]) -> list[dict[str, Any]]:
    """Validate subset counts and the expected one-row-per-machine Gold grain."""
    bronze = counts["sensor_bronze"]["row_count"]
    silver = counts["sensor_silver"]["row_count"]
    invalid = counts["sensor_invalid_records"]["row_count"]
    distinct_silver_machines = counts["sensor_silver"]["distinct_machine_count"]
    machine_health = counts["machine_health"]["row_count"]
    downtime_kpis = counts["downtime_kpis"]["row_count"]

    checks = [
        {
            "name": "silver_not_larger_than_bronze",
            "passed": silver <= bronze,
            "observed": {"silver_rows": silver, "bronze_rows": bronze},
        },
        {
            "name": "invalid_records_not_larger_than_bronze",
            "passed": invalid <= bronze,
            "observed": {"invalid_rows": invalid, "bronze_rows": bronze},
        },
        {
            "name": "machine_health_matches_silver_machine_grain",
            "passed": machine_health == distinct_silver_machines,
            "observed": {
                "machine_health_rows": machine_health,
                "distinct_silver_machines": distinct_silver_machines,
            },
        },
        {
            "name": "downtime_kpis_matches_silver_machine_grain",
            "passed": downtime_kpis == distinct_silver_machines,
            "observed": {
                "downtime_kpis_rows": downtime_kpis,
                "distinct_silver_machines": distinct_silver_machines,
            },
        },
        {
            "name": "machine_health_matches_downtime_kpis_row_count",
            "passed": machine_health == downtime_kpis,
            "observed": {
                "machine_health_rows": machine_health,
                "downtime_kpis_rows": downtime_kpis,
            },
        },
    ]
    return checks


def default_report_path() -> Path:
    root = Path(__file__).resolve().parents[1]
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return root / "outputs" / f"databricks-reconciliation-{stamp}.json"


def main() -> int:
    args = parse_args()
    client = WorkspaceClient()
    started_here = False
    started_at = datetime.now(timezone.utc)

    try:
        warehouse = client.warehouses.get(id=args.warehouse_id)
        state = normalized_state(warehouse.state)
        print(f"Warehouse: {warehouse.name} ({args.warehouse_id}); state={state}")
        if state == "STOPPED":
            client.warehouses.start(id=args.warehouse_id)
            started_here = True
        wait_for_warehouse(client, args.warehouse_id)

        filters = {table: "" for table in TABLES}
        if args.run_id:
            filters = run_filter_for_tables(
                client, args.warehouse_id, args.run_id, args.run_id_column
            )
            print(
                f"Using run-scoped filter: {args.run_id_column}={args.run_id!r} "
                "across all five tables."
            )
        else:
            print("Mode: current table snapshot; no ETL-run ID filter was supplied.")

        sql = "\nUNION ALL\n".join(
            f"SELECT '{table.rsplit('.', 1)[-1]}' AS table_name, "
            "COUNT(*) AS row_count, COUNT(DISTINCT machine_id) AS distinct_machine_count "
            f"FROM {table}{filters[table]}"
            for table in TABLES
        ) + "\nORDER BY table_name"

        rows = execute_sql(client, args.warehouse_id, sql)
        counts: dict[str, dict[str, int]] = {}
        print("\nDatabricks table counts:")
        for row in rows:
            table_name, row_count, distinct_machine_count = row
            counts[str(table_name)] = {
                "row_count": int(row_count),
                "distinct_machine_count": int(distinct_machine_count),
            }
            print(
                f"{table_name}: {int(row_count)} rows, "
                f"{int(distinct_machine_count)} distinct machine IDs"
            )

        expected = {table.rsplit(".", 1)[-1] for table in TABLES}
        missing = expected - set(counts)
        if missing:
            raise RuntimeError(f"Missing count results for: {', '.join(sorted(missing))}")

        checks = validate_counts(counts)
        for check in checks:
            state = "PASS" if check["passed"] else "FAIL"
            print(f"{state}: {check['name']} — {check['observed']}")

        passed = all(check["passed"] for check in checks)
        mode = "run_scoped" if args.run_id else "current_snapshot"
        output_path = Path(args.output).expanduser() if args.output else default_report_path()
        if not output_path.is_absolute():
            output_path = Path.cwd() / output_path
        report = {
            "started_at_utc": started_at.isoformat(),
            "completed_at_utc": datetime.now(timezone.utc).isoformat(),
            "warehouse_id": args.warehouse_id,
            "mode": mode,
            "run_id": args.run_id,
            "run_id_column": args.run_id_column,
            "tables": counts,
            "checks": checks,
            "reconciliation_passed": passed,
            "scope_note": (
                "Counts and machine-grain checks apply to one common ETL run because an explicit "
                "run-ID filter was applied to each table."
                if args.run_id
                else "This validates the currently materialized table snapshot and machine grain; "
                     "it does not bind rows to a specific Databricks job-run ID."
            ),
        }
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
        print(f"Report written: {output_path}")

        if not passed:
            print("RECONCILIATION FAILED: one or more machine-grain checks failed.", file=sys.stderr)
            return 2
        if not args.run_id:
            print(
                "PASS: current snapshot and machine-grain checks reconciled. "
                "Use --run-id and --run-id-column only when a real common run column exists."
            )
        else:
            print("PASS: run-scoped counts and machine-grain checks reconciled.")
        return 0

    except Exception as exc:
        print(f"RECONCILIATION NOT COMPLETED: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1

    finally:
        if started_here:
            try:
                client.warehouses.stop(id=args.warehouse_id)
                print("Warehouse stop requested.", flush=True)
            except Exception as exc:
                print(f"WARNING: please stop warehouse manually: {exc}", file=sys.stderr)


if __name__ == "__main__":
    raise SystemExit(main())
