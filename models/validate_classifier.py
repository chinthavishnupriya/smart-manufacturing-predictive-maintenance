from pathlib import Path
import json
import warnings

import joblib
import pandas as pd
import sklearn
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import (
    accuracy_score,
    precision_score,
    recall_score,
    f1_score,
    roc_auc_score,
    confusion_matrix,
)
from sklearn.model_selection import GroupShuffleSplit

import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from data_processing.feature_engineering import build_features, FEATURE_COLUMNS

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data" / "processed" / "cleaned_data.csv"
OUTPUT = ROOT / "outputs" / "classifier_grouped_validation_v191.json"
MODEL_OUTPUT = ROOT / "models" / "predictive_maintenance_model_v191.joblib"

if not DATA.exists():
    raise FileNotFoundError(f"Cleaned dataset not found: {DATA}")

df = build_features(pd.read_csv(DATA))
X = df[FEATURE_COLUMNS]
y = df["failure"].astype(int)
groups = df["machine_id"]

splitter = GroupShuffleSplit(
    n_splits=1, test_size=0.20, random_state=42
)
train_idx, test_idx = next(splitter.split(X, y, groups=groups))

X_train, X_test = X.iloc[train_idx], X.iloc[test_idx]
y_train, y_test = y.iloc[train_idx], y.iloc[test_idx]

model = RandomForestClassifier(
    n_estimators=250,
    max_depth=18,
    min_samples_leaf=2,
    class_weight="balanced",
    random_state=42,
    n_jobs=-1,
)
model.fit(X_train, y_train)

probabilities = model.predict_proba(X_test)[:, 1]
predictions = (probabilities >= 0.50).astype(int)

metrics = {
    "validation": "machine_group_holdout",
    "scikit_learn_version": sklearn.__version__,
    "train_rows": int(len(train_idx)),
    "test_rows": int(len(test_idx)),
    "train_machines": int(groups.iloc[train_idx].nunique()),
    "test_machines": int(groups.iloc[test_idx].nunique()),
    "train_failure_rows": int(y_train.sum()),
    "test_failure_rows": int(y_test.sum()),
    "accuracy": float(accuracy_score(y_test, predictions)),
    "precision": float(precision_score(y_test, predictions, zero_division=0)),
    "recall": float(recall_score(y_test, predictions, zero_division=0)),
    "f1": float(f1_score(y_test, predictions, zero_division=0)),
    "roc_auc": float(roc_auc_score(y_test, probabilities)),
    "confusion_matrix": confusion_matrix(y_test, predictions).tolist(),
}

# Save the newly trained model under a different filename.
joblib.dump(
    {"model": model, "features": FEATURE_COLUMNS, "threshold": 0.50},
    MODEL_OUTPUT,
)
OUTPUT.parent.mkdir(parents=True, exist_ok=True)
OUTPUT.write_text(json.dumps(metrics, indent=2), encoding="utf-8")

print(json.dumps(metrics, indent=2))
print(f"\nNew model saved to: {MODEL_OUTPUT}")
print(f"Validation results saved to: {OUTPUT}")
