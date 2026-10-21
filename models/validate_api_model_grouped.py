"""Evaluate the API classifier with machines held out from training."""
from pathlib import Path
import json

import joblib
import pandas as pd
import sklearn
from sklearn.base import clone
from sklearn.metrics import (
    accuracy_score,
    precision_score,
    recall_score,
    f1_score,
    roc_auc_score,
    confusion_matrix,
)
from sklearn.model_selection import GroupShuffleSplit

ROOT = Path(__file__).resolve().parents[1]
import sys
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

DATA = ROOT / "data" / "processed" / "cleaned_data.csv"
MODEL_FILE = ROOT / "models" / "predictive_maintenance_model.joblib"
REPORT_FILE = ROOT / "outputs" / "classifier_api_grouped_validation.json"


def main():
    from data_processing.feature_engineering import build_features

    df = pd.read_csv(DATA)
    df = build_features(df)

    if "machine_id" not in df.columns or "failure" not in df.columns:
        raise ValueError("Dataset must contain machine_id and failure columns.")

    bundle = joblib.load(MODEL_FILE)
    features = bundle["features"]
    threshold = float(bundle["threshold"])

    splitter = GroupShuffleSplit(
        n_splits=1, test_size=0.20, random_state=42
    )
    train_idx, test_idx = next(
        splitter.split(df[features], df["failure"], groups=df["machine_id"])
    )

    # Refit a clone of the API model's estimator on the training machines.
    # The saved API model is loaded for its configuration, but is not modified.
    model = clone(bundle["model"])
    model.fit(
        df.iloc[train_idx][features],
        df.iloc[train_idx]["failure"].astype(int),
    )

    y_test = df.iloc[test_idx]["failure"].astype(int)
    probabilities = model.predict_proba(df.iloc[test_idx][features])[:, 1]
    predictions = (probabilities >= threshold).astype(int)

    cm = confusion_matrix(y_test, predictions, labels=[0, 1])
    report = {
        "validation": "machine_group_holdout",
        "purpose": "API model configuration evaluated on unseen machines",
        "scikit_learn_version": sklearn.__version__,
        "threshold": threshold,
        "train_rows": int(len(train_idx)),
        "test_rows": int(len(test_idx)),
        "train_machines": int(df.iloc[train_idx]["machine_id"].nunique()),
        "test_machines": int(df.iloc[test_idx]["machine_id"].nunique()),
        "train_failure_rows": int(df.iloc[train_idx]["failure"].sum()),
        "test_failure_rows": int(y_test.sum()),
        "accuracy": float(accuracy_score(y_test, predictions)),
        "precision": float(precision_score(y_test, predictions, zero_division=0)),
        "recall": float(recall_score(y_test, predictions, zero_division=0)),
        "f1": float(f1_score(y_test, predictions, zero_division=0)),
        "roc_auc": float(roc_auc_score(y_test, probabilities)),
        "confusion_matrix": cm.tolist(),
        "model_file_modified": False,
    }

    REPORT_FILE.parent.mkdir(parents=True, exist_ok=True)
    REPORT_FILE.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(json.dumps(report, indent=2))
    print(f"\nReport saved to: {REPORT_FILE}")


if __name__ == "__main__":
    main()
