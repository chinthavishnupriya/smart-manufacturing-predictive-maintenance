
from pathlib import Path

import joblib
import numpy as np
import pandas as pd

from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error
from sklearn.model_selection import GroupShuffleSplit
from sklearn.pipeline import Pipeline
from sklearn.impute import SimpleImputer


ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data" / "raw" / "nasa_cmapss"

COLUMNS = (
    ["unit", "cycle"]
    + [f"setting_{i}" for i in range(1, 4)]
    + [f"sensor_{i}" for i in range(1, 22)]
)

FEATURES = (
    ["cycle"]
    + [f"setting_{i}" for i in range(1, 4)]
    + [f"sensor_{i}" for i in range(1, 22)]
)


def load_data(filename):
    return pd.read_csv(
        DATA / filename,
        sep=r"\s+",
        header=None,
        names=COLUMNS,
        usecols=range(26),
    )


def main():
    train = load_data("train_FD001.txt")
    test = load_data("test_FD001.txt")

    true_rul = pd.read_csv(
        DATA / "RUL_FD001.txt",
        sep=r"\s+",
        header=None,
        names=["true_rul"],
    )["true_rul"].to_numpy()

    max_cycles = train.groupby("unit")["cycle"].max()
    train["rul_cycles"] = (
        train["unit"].map(max_cycles) - train["cycle"]
    ).clip(lower=0)

    # Split by engine, so one engine cannot appear in both sets.
    splitter = GroupShuffleSplit(
        n_splits=1, test_size=0.2, random_state=42
    )
    fit_idx, val_idx = next(
        splitter.split(train[FEATURES], train["rul_cycles"], groups=train["unit"])
    )

    model = Pipeline([
        ("imputer", SimpleImputer(strategy="median")),
        ("regressor", RandomForestRegressor(
            n_estimators=150,
            min_samples_leaf=2,
            random_state=42,
            n_jobs=-1,
        )),
    ])

    model.fit(
        train.iloc[fit_idx][FEATURES],
        train.iloc[fit_idx]["rul_cycles"],
    )

    val_pred = np.maximum(
        0, model.predict(train.iloc[val_idx][FEATURES])
    )

    print("\n=== ENGINE-WISE VALIDATION ===")
    print("Validation engines:", train.iloc[val_idx]["unit"].nunique())
    print("Validation MAE (cycles):",
          round(mean_absolute_error(
              train.iloc[val_idx]["rul_cycles"], val_pred
          ), 3))
    print("Validation RMSE (cycles):",
          round(np.sqrt(mean_squared_error(
              train.iloc[val_idx]["rul_cycles"], val_pred
          )), 3))

    # Evaluate each test engine's final observed cycle.
    last_rows = (
        test.sort_values(["unit", "cycle"])
        .groupby("unit", as_index=False)
        .tail(1)
        .sort_values("unit")
    )

    if len(last_rows) != len(true_rul):
        raise ValueError(
            f"Test engines ({len(last_rows)}) do not match "
            f"reference RUL values ({len(true_rul)})."
        )

    # Refit on all training engines before the official test evaluation.
    model.fit(train[FEATURES], train["rul_cycles"])
    predictions = np.maximum(0, model.predict(last_rows[FEATURES]))

    print("\n=== OFFICIAL FD001 TEST EVALUATION ===")
    print("Test engines:", len(last_rows))
    print("MAE (cycles):",
          round(mean_absolute_error(true_rul, predictions), 3))
    print("RMSE (cycles):",
          round(np.sqrt(mean_squared_error(true_rul, predictions)), 3))

    results = pd.DataFrame({
        "unit": last_rows["unit"].to_numpy(),
        "true_rul_cycles": true_rul,
        "predicted_rul_cycles": np.round(predictions, 2),
    })

    results_path = ROOT / "data" / "processed" / "nasa_fd001_rul_results.csv"
    results_path.parent.mkdir(parents=True, exist_ok=True)
    results.to_csv(results_path, index=False)

    model_path = ROOT / "models" / "nasa_fd001_rul_model.joblib"
    joblib.dump({
        "model": model,
        "features": FEATURES,
        "target_unit": "operating_cycles",
        "dataset": "NASA C-MAPSS FD001",
    }, model_path)

    print("\nSaved separate model:", model_path)
    print("Saved test predictions:", results_path)


if __name__ == "__main__":
    main()
