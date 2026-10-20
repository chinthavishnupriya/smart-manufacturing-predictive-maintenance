import pandas as pd
import numpy as np
import joblib

from pathlib import Path

from sklearn.ensemble import (
    RandomForestRegressor,
    GradientBoostingRegressor,
    HistGradientBoostingRegressor
)

from sklearn.metrics import (
    mean_absolute_error,
    mean_squared_error,
    r2_score
)


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
print("RUL MODEL COMPARISON")
print("=" * 60)

df = pd.read_csv(INPUT_FILE)

df["timestamp"] = pd.to_datetime(df["timestamp"])

df = df.sort_values("timestamp").reset_index(drop=True)

print(f"\nTotal records: {len(df)}")


# ============================================================
# FEATURES
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

split_index = int(len(df) * 0.80)

X_train = X.iloc[:split_index]
X_test = X.iloc[split_index:]

y_train = y.iloc[:split_index]
y_test = y.iloc[split_index:]

print(f"Training records: {len(X_train)}")
print(f"Testing records:  {len(X_test)}")


# ============================================================
# MODELS
# ============================================================

models = {
    "Random Forest": RandomForestRegressor(
        n_estimators=200,
        max_depth=15,
        min_samples_leaf=2,
        random_state=42,
        n_jobs=-1
    ),

    "Gradient Boosting": GradientBoostingRegressor(
        n_estimators=200,
        learning_rate=0.05,
        max_depth=4,
        min_samples_leaf=3,
        random_state=42
    ),

    "Hist Gradient Boosting": HistGradientBoostingRegressor(
        max_iter=200,
        learning_rate=0.05,
        max_leaf_nodes=31,
        l2_regularization=0.1,
        random_state=42
    )
}


# ============================================================
# TRAIN AND EVALUATE
# ============================================================

results = []

trained_models = {}

for name, model in models.items():

    print("\n" + "-" * 60)
    print(f"Training: {name}")
    print("-" * 60)

    model.fit(X_train, y_train)

    predictions = model.predict(X_test)

    mae = mean_absolute_error(
        y_test,
        predictions
    )

    rmse = np.sqrt(
        mean_squared_error(
            y_test,
            predictions
        )
    )

    r2 = r2_score(
        y_test,
        predictions
    )

    results.append({
        "Model": name,
        "MAE": mae,
        "RMSE": rmse,
        "R2": r2
    })

    trained_models[name] = model

    print(f"MAE  : {mae:.4f} hours")
    print(f"RMSE : {rmse:.4f} hours")
    print(f"R²   : {r2:.4f}")


# ============================================================
# COMPARISON
# ============================================================

results_df = pd.DataFrame(results)

results_df = results_df.sort_values(
    by="MAE"
)

print("\n" + "=" * 60)
print("MODEL COMPARISON")
print("=" * 60)

print(
    results_df.to_string(
        index=False,
        float_format=lambda x: f"{x:.4f}"
    )
)


# ============================================================
# SELECT BEST MODEL
# ============================================================

best_model_name = results_df.iloc[0]["Model"]

best_model = trained_models[
    best_model_name
]

joblib.dump(
    best_model,
    MODEL_FILE
)


# ============================================================
# FINAL RESULT
# ============================================================

print("\n" + "=" * 60)
print("BEST RUL MODEL")
print("=" * 60)

print(f"\nSelected model: {best_model_name}")

best_row = results_df.iloc[0]

print(f"MAE  : {best_row['MAE']:.4f} hours")
print(f"RMSE : {best_row['RMSE']:.4f} hours")
print(f"R²   : {best_row['R2']:.4f}")

print("\nModel saved to:")
print(MODEL_FILE)

print("\n" + "=" * 60)
print("RUL MODEL COMPARISON COMPLETED")
print("=" * 60)