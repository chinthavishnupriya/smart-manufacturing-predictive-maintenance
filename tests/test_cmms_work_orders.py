import tempfile
import unittest
from pathlib import Path

from cmms.work_order_service import create_work_order, list_work_orders


class CMMSWorkOrderTests(unittest.TestCase):
    def test_threshold_and_idempotency(self):
        with tempfile.TemporaryDirectory() as temp:
            db = Path(temp) / "test_work_orders.db"

            low = {
                "machine_id": "TEST-01",
                "timestamp": "2026-10-20T10:00:00",
                "failure_probability": 0.80
            }
            high = {
                **low,
                "timestamp": "2026-10-20T10:01:00",
                "failure_probability": 0.91
            }

            self.assertFalse(create_work_order(low, db)["created"])

            first = create_work_order(high, db)
            second = create_work_order(high, db)

            self.assertTrue(first["created"])
            self.assertFalse(second["created"])
            self.assertEqual(second["reason"], "DUPLICATE")
            self.assertEqual(len(list_work_orders(db)), 1)

    def test_distinct_events_create_distinct_orders(self):
        with tempfile.TemporaryDirectory() as temp:
            db = Path(temp) / "test_work_orders.db"

            for minute in ("10:01:00", "10:02:00"):
                create_work_order({
                    "machine_id": "TEST-02",
                    "timestamp": f"2026-10-20T{minute}",
                    "failure_probability": 0.95
                }, db)

            self.assertEqual(len(list_work_orders(db)), 2)


if __name__ == "__main__":
    unittest.main()
