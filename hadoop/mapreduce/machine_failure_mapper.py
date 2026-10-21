#!/usr/bin/env python3
"""Hadoop Streaming mapper: emit per-sensor-row counts by machine_id.

Input CSV: machine_id,timestamp,temperature,vibration,rpm,pressure,operating_hours,failure
Output: machine_id<TAB>1<TAB>failure_flag
"""
import csv
import sys

def main() -> int:
    reader = csv.reader(sys.stdin)
    for row_number, row in enumerate(reader, start=1):
        if not row or all(not value.strip() for value in row):
            continue
        if row[0].lstrip("\ufeff").strip().lower() == "machine_id":
            continue
        if len(row) < 8:
            print(f"Malformed CSV row {row_number}: expected 8 columns, got {len(row)}", file=sys.stderr)
            return 2
        machine_id = row[0].strip()
        if not machine_id:
            print(f"Malformed CSV row {row_number}: empty machine_id", file=sys.stderr)
            return 2
        try:
            failure = int(row[7].strip())
        except ValueError:
            print(f"Malformed CSV row {row_number}: failure must be 0 or 1", file=sys.stderr)
            return 2
        if failure not in (0, 1):
            print(f"Malformed CSV row {row_number}: failure must be 0 or 1", file=sys.stderr)
            return 2
        print(f"{machine_id}\t1\t{failure}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
