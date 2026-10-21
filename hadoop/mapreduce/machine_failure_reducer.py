#!/usr/bin/env python3
"""Hadoop Streaming reducer: count records and failures per machine_id."""
import sys

def emit(machine_id: str, record_count: int, failure_count: int) -> None:
    rate = 100.0 * failure_count / record_count if record_count else 0.0
    print(f"{machine_id}\t{record_count}\t{failure_count}\t{rate:.2f}")

def main() -> int:
    current_machine = None
    record_count = 0
    failure_count = 0
    for line_number, raw in enumerate(sys.stdin, start=1):
        parts = raw.rstrip("\n").split("\t")
        if len(parts) != 3:
            print(f"Malformed mapper output line {line_number}: expected 3 tab-separated fields", file=sys.stderr)
            return 2
        machine_id, count_text, failure_text = parts
        if not machine_id:
            print(f"Malformed mapper output line {line_number}: empty machine_id", file=sys.stderr)
            return 2
        try:
            count = int(count_text)
            failure = int(failure_text)
        except ValueError:
            print(f"Malformed mapper output line {line_number}: counts must be integers", file=sys.stderr)
            return 2
        if count < 0 or failure not in (0, 1) or failure > count:
            print(f"Malformed mapper output line {line_number}: invalid count/failure value", file=sys.stderr)
            return 2
        if current_machine is not None and machine_id != current_machine:
            emit(current_machine, record_count, failure_count)
            record_count = 0
            failure_count = 0
        current_machine = machine_id
        record_count += count
        failure_count += failure
    if current_machine is not None:
        emit(current_machine, record_count, failure_count)
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
