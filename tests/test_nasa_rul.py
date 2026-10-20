
from pathlib import Path

import joblib
import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
RESULTS_PATH = PROJECT_ROOT / "data/processed/nasa_fd001_rul_results.csv"
MODEL_PATH = PROJECT_ROOT / "models/nasa_fd001_rul_model.joblib"


def test_nasa_rul_model_artifact_exists():
    assert MODEL_PATH.is_file()


def test_nasa_rul_results_exist():
    assert RESULTS_PATH.is_file()


def test_test_results_have_100_engines():
    df = pd.read_csv(RESULTS_PATH)
    assert len(df) == 100
    assert df["unit"].nunique() == 100


def test_rul_predictions_are_finite_and_nonnegative():
    df = pd.read_csv(RESULTS_PATH)
    predictions = df["predicted_rul_cycles"].to_numpy()

    assert np.isfinite(predictions).all()
    assert (predictions >= 0).all()


def test_reference_rul_values_are_valid():
    df = pd.read_csv(RESULTS_PATH)

    assert df["true_rul_cycles"].notna().all()
    assert (df["true_rul_cycles"] >= 0).all()


def test_model_metadata_is_correct():
    artifact = joblib.load(MODEL_PATH)

    assert artifact["target_unit"] == "operating_cycles"
    assert artifact["dataset"] == "NASA C-MAPSS FD001"
    assert len(artifact["features"]) == 25
    assert artifact["model"] is not None
