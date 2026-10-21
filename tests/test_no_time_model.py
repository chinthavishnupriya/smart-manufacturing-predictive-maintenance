import joblib
import pandas as pd

from data_processing.feature_engineering import build_features


def test_experimental_model_ignores_timestamp_features():
    bundle = joblib.load(
        "models/predictive_maintenance_model_no_time_experimental.joblib"
    )

    assert "hour" not in bundle["features"]
    assert "day_of_week" not in bundle["features"]

    samples = pd.DataFrame([
        {
            "machine_id": "M_TEST",
            "timestamp": "2026-10-22 00:00:00",
            "temperature": 74.26,
            "vibration": 3.838,
            "rpm": 1371.31,
            "pressure": 3.496,
            "operating_hours": 514.12,
        },
        {
            "machine_id": "M_TEST",
            "timestamp": "2026-10-22 23:00:00",
            "temperature": 74.26,
            "vibration": 3.838,
            "rpm": 1371.31,
            "pressure": 3.496,
            "operating_hours": 514.12,
        },
    ])

    X = build_features(samples)[bundle["features"]]
    probabilities = bundle["model"].predict_proba(X)[:, 1]

    assert probabilities[0] == probabilities[1]
