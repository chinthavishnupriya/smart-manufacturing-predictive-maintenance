"""Train and evaluate a predictive-maintenance classifier."""
from pathlib import Path
import json
import joblib
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import (accuracy_score, classification_report, confusion_matrix, f1_score,
                             precision_score, recall_score, roc_auc_score, roc_curve)
from sklearn.model_selection import train_test_split
from data_processing.feature_engineering import build_features, FEATURE_COLUMNS

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data" / "processed" / "cleaned_data.csv"
MODEL_DIR = ROOT / "models"; OUTPUT_DIR = ROOT / "outputs"; PLOT_DIR = OUTPUT_DIR / "plots"
MODEL_FILE = MODEL_DIR / "predictive_maintenance_model.joblib"
METRICS_FILE = OUTPUT_DIR / "metrics.json"

def train():
    if not DATA.exists():
        from data_processing.clean_data import clean_data
        clean_data()
    df = pd.read_csv(DATA)
    df = build_features(df)
    X = df[FEATURE_COLUMNS]
    y = df["failure"].astype(int)
    # Stratified split keeps the rare failure class represented in both sets.
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.20, random_state=42, stratify=y)
    model = RandomForestClassifier(n_estimators=250, max_depth=18, min_samples_leaf=2,
                                   class_weight="balanced", random_state=42, n_jobs=-1)
    model.fit(X_train, y_train)
    probabilities = model.predict_proba(X_test)[:, 1]
    predictions = (probabilities >= 0.50).astype(int)
    metrics = {
        "accuracy": float(accuracy_score(y_test, predictions)),
        "precision": float(precision_score(y_test, predictions, zero_division=0)),
        "recall": float(recall_score(y_test, predictions, zero_division=0)),
        "f1": float(f1_score(y_test, predictions, zero_division=0)),
        "roc_auc": float(roc_auc_score(y_test, probabilities)),
        "test_rows": int(len(y_test)),
        "failure_rows_test": int(y_test.sum()),
        "classification_report": classification_report(y_test, predictions, output_dict=True, zero_division=0),
    }
    MODEL_DIR.mkdir(exist_ok=True); OUTPUT_DIR.mkdir(exist_ok=True); PLOT_DIR.mkdir(parents=True, exist_ok=True)
    joblib.dump({"model": model, "features": FEATURE_COLUMNS, "threshold": 0.50}, MODEL_FILE)
    METRICS_FILE.write_text(json.dumps(metrics, indent=2), encoding="utf-8")
    cm = confusion_matrix(y_test, predictions)
    plt.figure(figsize=(6,5)); sns.heatmap(cm, annot=True, fmt="d", cmap="Blues", xticklabels=["Normal","Failure"], yticklabels=["Normal","Failure"])
    plt.xlabel("Predicted"); plt.ylabel("Actual"); plt.title("Confusion Matrix"); plt.tight_layout(); plt.savefig(PLOT_DIR / "confusion_matrix.png", dpi=150); plt.close()
    fpr, tpr, _ = roc_curve(y_test, probabilities)
    plt.figure(figsize=(6,5)); plt.plot(fpr, tpr, label=f"ROC-AUC = {metrics['roc_auc']:.3f}"); plt.plot([0,1],[0,1],"--"); plt.xlabel("False Positive Rate"); plt.ylabel("True Positive Rate"); plt.title("ROC Curve"); plt.legend(); plt.tight_layout(); plt.savefig(PLOT_DIR / "roc_curve.png", dpi=150); plt.close()
    importances = pd.Series(model.feature_importances_, index=FEATURE_COLUMNS).sort_values()
    plt.figure(figsize=(8,5)); importances.plot(kind="barh"); plt.xlabel("Importance"); plt.title("Feature Importance"); plt.tight_layout(); plt.savefig(PLOT_DIR / "feature_importance.png", dpi=150); plt.close()
    print(json.dumps({k:v for k,v in metrics.items() if k not in ["classification_report"]}, indent=2))
    print(f"Model saved to: {MODEL_FILE}")

if __name__ == "__main__": train()
