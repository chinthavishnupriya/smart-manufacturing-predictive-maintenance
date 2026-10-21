"""Make a prediction for one sensor reading from the command line."""
from pathlib import Path
import argparse
import os
import joblib
import pandas as pd
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from data_processing.feature_engineering import build_features

DEFAULT_MODEL_PATH = ROOT / "models" / "predictive_maintenance_model_v191.joblib"
MODEL_PATH = Path(os.environ.get("PREDICT_MODEL_PATH", str(DEFAULT_MODEL_PATH)))
if not MODEL_PATH.is_absolute():
    MODEL_PATH = ROOT / MODEL_PATH

BUNDLE = joblib.load(MODEL_PATH)


def predict(payload):
    row = pd.DataFrame([payload])
    x = build_features(row)[BUNDLE["features"]]
    p = float(BUNDLE["model"].predict_proba(x)[0, 1])
    return {
        "failure_probability": round(p, 4),
        "prediction": int(p >= BUNDLE["threshold"]),
        "status": "FAILURE RISK" if p >= BUNDLE["threshold"] else "NORMAL",
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--machine-id", default="M001")
    parser.add_argument("--timestamp", default="2026-01-01 12:00:00")
    parser.add_argument("--temperature", type=float, default=65)
    parser.add_argument("--vibration", type=float, default=1.8)
    parser.add_argument("--rpm", type=float, default=1500)
    parser.add_argument("--pressure", type=float, default=4.2)
    parser.add_argument("--operating-hours", type=float, default=1000)
    args = parser.parse_args()
    print(predict(vars(args)))
