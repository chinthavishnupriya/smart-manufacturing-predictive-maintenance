import csv
import io
import subprocess
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
MAPPER = ROOT / "hadoop" / "mapreduce" / "machine_failure_mapper.py"
REDUCER = ROOT / "hadoop" / "mapreduce" / "machine_failure_reducer.py"

def sensor_csv(rows):
    buffer = io.StringIO()
    writer = csv.writer(buffer, lineterminator="\n")
    writer.writerow(["machine_id", "timestamp", "temperature", "vibration", "rpm", "pressure", "operating_hours", "failure"])
    writer.writerows(rows)
    return buffer.getvalue()

class HadoopStreamingContractTests(unittest.TestCase):
    def test_mapper_and_reducer_aggregate_machine_failure_rates(self):
        payload = sensor_csv([
            ["M001", "2026-10-22 09:00:00", 70, 1.0, 1200, 4.1, 12, 0],
            ["M001", "2026-10-22 09:01:00", 93, 5.1, 2700, 8.5, 13, 1],
            ["M002", "2026-10-22 09:02:00", 75, 1.5, 1300, 4.0, 14, 0],
        ])
        mapped = subprocess.run([sys.executable, str(MAPPER)], input=payload, text=True, capture_output=True)
        self.assertEqual(mapped.returncode, 0, mapped.stderr)
        lines = sorted(mapped.stdout.splitlines(), key=lambda line: line.split("\t", 1)[0])
        reduced = subprocess.run([sys.executable, str(REDUCER)], input="\n".join(lines) + "\n", text=True, capture_output=True)
        self.assertEqual(reduced.returncode, 0, reduced.stderr)
        self.assertEqual(reduced.stdout.splitlines(), ["M001\t2\t1\t50.00", "M002\t1\t0\t0.00"])

    def test_mapper_rejects_invalid_failure_label(self):
        payload = sensor_csv([["M001", "2026-10-22 09:00:00", 70, 1.0, 1200, 4.1, 12, 7]])
        result = subprocess.run([sys.executable, str(MAPPER)], input=payload, text=True, capture_output=True)
        self.assertEqual(result.returncode, 2)
        self.assertIn("failure must be 0 or 1", result.stderr)

    def test_reducer_rejects_malformed_mapper_record(self):
        result = subprocess.run([sys.executable, str(REDUCER)], input="M001\tbad\n", text=True, capture_output=True)
        self.assertEqual(result.returncode, 2)
        self.assertIn("expected 3 tab-separated fields", result.stderr)

if __name__ == "__main__":
    unittest.main()
