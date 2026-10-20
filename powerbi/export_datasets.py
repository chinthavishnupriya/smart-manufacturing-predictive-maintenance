from pathlib import Path
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "outputs" / "powerbi"
OUT.mkdir(parents=True, exist_ok=True)

sensor_path = ROOT / "data/raw/sensor_data_100k.csv"
history_path = ROOT / "outputs/live_sensor_history.csv"

if not sensor_path.exists():
    raise FileNotFoundError(sensor_path)

sensor = pd.read_csv(sensor_path)
sensor["timestamp"] = pd.to_datetime(sensor["timestamp"], errors="coerce")
sensor = sensor.dropna(subset=["machine_id", "timestamp"])

# Dataset 1: sensor readings and known failure labels.
sensor.to_csv(OUT / "sensor_detail.csv", index=False)

# Dataset 2: machine-level health summary.
latest = (
    sensor.sort_values("timestamp")
    .groupby("machine_id", as_index=False)
    .tail(1)
    .copy()
)

machine_health = latest[
    ["machine_id", "timestamp", "temperature", "vibration",
     "rpm", "pressure", "operating_hours", "failure"]
].rename(columns={
    "timestamp": "latest_timestamp",
    "failure": "latest_failure_label"
})


# Add latest live predictions when available; fill missing machines using
# the trained model applied to their latest sensor readings.
import joblib

bundle = joblib.load(ROOT / "models" / "predictive_maintenance_model.joblib")
model = bundle["model"]
features = bundle["features"]

sensor_features = sensor.copy()
sensor_features["timestamp"] = pd.to_datetime(
    sensor_features["timestamp"], errors="coerce"
)
sensor_features["hour"] = sensor_features["timestamp"].dt.hour
sensor_features["day_of_week"] = sensor_features["timestamp"].dt.dayofweek
sensor_features["temp_vibration"] = (
    sensor_features["temperature"] * sensor_features["vibration"]
)
sensor_features["rpm_deviation"] = sensor_features["rpm"] - sensor_features["rpm"].mean()
sensor_features["pressure_deviation"] = (
    sensor_features["pressure"] - sensor_features["pressure"].mean()
)

latest_features = (
    sensor_features.sort_values("timestamp")
    .groupby("machine_id", as_index=False)
    .tail(1)
    .copy()
)

X = latest_features[features].apply(pd.to_numeric, errors="coerce")
valid = X.notna().all(axis=1)
latest_features["failure_probability"] = float("nan")
latest_features["prediction"] = float("nan")

if valid.any():
    probabilities = model.predict_proba(X.loc[valid])
    classes = list(model.classes_)
    failure_class_index = classes.index(1)
    latest_features.loc[valid, "failure_probability"] = probabilities[:, failure_class_index]
    latest_features.loc[valid, "prediction"] = (
        probabilities[:, failure_class_index] >= bundle.get("threshold", 0.5)
    ).astype(int)

latest_features["status"] = latest_features["prediction"].map(
    {0.0: "NORMAL", 1.0: "AT_RISK"}
)

model_predictions = latest_features[
    ["machine_id", "failure_probability", "prediction", "status"]
]
machine_health = machine_health.merge(
    model_predictions, on="machine_id", how="left"
)

# Preserve a live prediction only when it has a valid probability.
if history_path.exists():
    history = pd.read_csv(history_path)
    needed = {"machine_id", "failure_probability", "prediction", "status"}
    if needed.issubset(history.columns) and "timestamp" in history.columns:
        history["timestamp"] = pd.to_datetime(history["timestamp"], errors="coerce")
        latest_live = (
            history.sort_values("timestamp")
            .groupby("machine_id", as_index=False)
            .tail(1)[["machine_id", "failure_probability", "prediction", "status"]]
        )
        latest_live = latest_live.rename(columns={
            "failure_probability": "live_failure_probability",
            "prediction": "live_prediction",
            "status": "live_status",
        })
        machine_health = machine_health.merge(
            latest_live, on="machine_id", how="left"
        )
        machine_health["failure_probability"] = (
            pd.to_numeric(machine_health["live_failure_probability"], errors="coerce")
            .combine_first(pd.to_numeric(machine_health["failure_probability"], errors="coerce"))
        )
        machine_health["prediction"] = (
            pd.to_numeric(machine_health["live_prediction"], errors="coerce")
            .combine_first(pd.to_numeric(machine_health["prediction"], errors="coerce"))
        )
        machine_health["status"] = (
            machine_health["live_status"].fillna(machine_health["status"])
        )
        machine_health = machine_health.drop(
            columns=["live_failure_probability", "live_prediction", "live_status"]
        )

machine_health.to_csv(OUT / "machine_health.csv", index=False)

# Dataset 3: machine-level failure counts and a proxy MTBF estimate.
# This is an estimate based on timestamp intervals, not measured downtime.
rows = []
for machine_id, group in sensor.sort_values("timestamp").groupby("machine_id"):
    group = group.sort_values("timestamp")
    failures = int(pd.to_numeric(group["failure"], errors="coerce").fillna(0).sum())
    span_hours = (
        (group["timestamp"].max() - group["timestamp"].min()).total_seconds()
        / 3600
    )
    rows.append({
        "machine_id": machine_id,
        "observed_rows": len(group),
        "failure_events_in_dataset": failures,
        "observed_span_hours": round(span_hours, 3),
        "estimated_mtbf_hours": round(span_hours / failures, 3) if failures else None
    })

kpis = pd.DataFrame(rows)
kpis.to_csv(OUT / "downtime_kpis.csv", index=False)

print("Power BI datasets created:")
for path in sorted(OUT.glob("*.csv")):
    df = pd.read_csv(path)
    print(f"- {path.relative_to(ROOT)}: {len(df):,} rows, {len(df.columns)} columns")

print("\nNote: downtime_kpis.csv contains an estimated MTBF proxy.")
print("Actual OEE and downtime require production-time and downtime records.")
