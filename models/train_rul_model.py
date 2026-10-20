import pandas as pd
import numpy as np
import joblib

from pathlib import Path

from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score


# ============================================================
# PATHS
# ============================================================

BASE_DIR = Path(__file__).resolve().parent.parent

INPUT_FILE = (
    BASE_DIR
    / "data"
    / "processed"
    / "rul_data.csv"
)

MODEL_FILE = (
    BASE_DIR
    / "models"
    / "rul_model.joblib"
)


# ============================================================
# LOAD DATA
# ============================================================

print("=" * 60)
print("RUL REGRESSION MODEL TRAINING")
print("=" * 60)

print("\nLoading RUL dataset...")

df = pd.read_csv(INPUT_FILE)

df["timestamp"] = pd.to_datetime(df["timestamp"])

print(f"Total records: {len(df)}")


# ============================================================
# FEATURES AND TARGET
# ============================================================

features = [
    "temperature",
    "vibration",
    "rpm",
    "pressure",
    "operating_hours"
]

target = "rul_hours"


X = df[features]
y = df[target]


# ============================================================
# TIME-BASED SPLIT
# ============================================================

print("\nSplitting data chronologically...")

# Sort by time
df = df.sort_values("timestamp").reset_index(drop=True)

split_index = int(len(df) * 0.80)

train_df = df.iloc[:split_index]
test_df = df.iloc[split_index:]


X_train = train_df[features]
y_train = train_df[target]

X_test = test_df[features]
y_test = test_df[target]


print(f"Training records: {len(train_df)}")
print(f"Testing records:  {len(test_df)}")


# ============================================================
# TRAIN RANDOM FOREST REGRESSOR
# ============================================================

print("\nTraining Random Forest Regressor...")

model = RandomForestRegressor(
    n_estimators=200,
    max_depth=15,
    min_samples_leaf=2,
    random_state=42,
    n_jobs=-1
)

model.fit(
    X_train,
    y_train
)


# ============================================================
# PREDICTIONS
# ============================================================

print("Generating predictions...")

y_pred = model.predict(X_test)


# ============================================================
# EVALUATION
# ============================================================

mae = mean_absolute_error(
    y_test,
    y_pred
)

rmse = np.sqrt(
    mean_squared_error(
        y_test,
        y_pred
    )
)

r2 = r2_score(
    y_test,
    y_pred
)


print("\n" + "=" * 60)
print("RUL MODEL PERFORMANCE")
print("=" * 60)

print(f"\nMAE  : {mae:.4f} hours")
print(f"RMSE : {rmse:.4f} hours")
print(f"R²   : {r2:.4f}")


# ============================================================
# FEATURE IMPORTANCE
# ============================================================

print("\n" + "-" * 60)
print("FEATURE IMPORTANCE")
print("-" * 60)

importance = pd.DataFrame({
    "feature": features,
    "importance": model.feature_importances_
})

importance = importance.sort_values(
    "importance",
    ascending=False
)

for _, row in importance.iterrows():
    print(
        f"{row['feature']:20s} "
        f"{row['importance']:.4f}"
    )


# ============================================================
# SAVE MODEL
# ============================================================

MODEL_FILE.parent.mkdir(
    parents=True,
    exist_ok=True
)

joblib.dump(
    model,
    MODEL_FILE
)


# ============================================================
# SAMPLE PREDICTIONS
# ============================================================

print("\n" + "-" * 60)
print("SAMPLE PREDICTIONS")
print("-" * 60)

sample = test_df[
    [
        "machine_id",
        "timestamp",
        "temperature",
        "vibration",
        "rpm",
        "pressure",
        "operating_hours",
        "rul_hours"
    ]
].head(10).copy()

sample["predicted_rul_hours"] = y_pred[:10]

print(
    sample.to_string(
        index=False
    )
)


# ============================================================
# FINAL
# ============================================================

print("\n" + "=" * 60)
print("RUL MODEL TRAINING COMPLETED")
print("=" * 60)

print(f"\nModel saved to:")
print(MODEL_FILE)