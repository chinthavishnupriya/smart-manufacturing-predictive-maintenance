#!/usr/bin/env python3
"""Verify HBase row keys and event_count values for one unique test machine."""
import argparse
import json
import os
import sys
import happybase

def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--machine-id", required=True)
    parser.add_argument("--min-rows", type=int, default=1)
    parser.add_argument("--expected-event-count", type=int, default=1)
    parser.add_argument("--host", default=os.getenv("HBASE_HOST", "localhost"))
    parser.add_argument("--port", type=int, default=int(os.getenv("HBASE_PORT", "9090")))
    parser.add_argument("--table", default=os.getenv("HBASE_TABLE", "sensor_anomalies"))
    args = parser.parse_args()
    connection = happybase.Connection(host=args.host, port=args.port, timeout=10000)
    try:
        connection.open()
        table = connection.table(args.table)
        prefix = f"{args.machine_id}_".encode("utf-8")
        rows = []
        for row_key, columns in table.scan(row_prefix=prefix):
            raw_count = columns.get(b"metrics:event_count")
            if raw_count is None:
                raise ValueError(f"Row {row_key!r} is missing metrics:event_count")
            rows.append({"row_key": row_key.decode("utf-8", errors="replace"),
                         "event_count": int(raw_count.decode("utf-8"))})
    finally:
        connection.close()
    report = {"machine_id": args.machine_id, "row_count": len(rows),
              "event_counts": sorted({row["event_count"] for row in rows}),
              "rows": rows}
    print(json.dumps(report, indent=2))
    if len(rows) < args.min_rows:
        print(f"PENDING: expected at least {args.min_rows} rows; found {len(rows)}", file=sys.stderr)
        return 3
    wrong = [row for row in rows if row["event_count"] != args.expected_event_count]
    if wrong:
        print(f"FAIL: {len(wrong)} row(s) have event_count != {args.expected_event_count}", file=sys.stderr)
        return 1
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
