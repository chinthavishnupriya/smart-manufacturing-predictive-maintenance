"""Feature engineering shared by model training and API inference."""
from __future__ import annotations
import pandas as pd

BASE_FEATURES = ["temperature", "vibration", "rpm", "pressure", "operating_hours"]
FEATURE_COLUMNS = BASE_FEATURES + ["hour", "day_of_week", "temp_vibration", "rpm_deviation", "pressure_deviation"]


def build_features(df: pd.DataFrame) -> pd.DataFrame:
    data = df.copy()
    data["timestamp"] = pd.to_datetime(data["timestamp"], errors="coerce")
    if data["timestamp"].isna().any():
        raise ValueError("timestamp contains invalid values")
    data["hour"] = data["timestamp"].dt.hour
    data["day_of_week"] = data["timestamp"].dt.dayofweek
    data["temp_vibration"] = data["temperature"] * data["vibration"]
    data["rpm_deviation"] = (data["rpm"] - 1500).abs()
    data["pressure_deviation"] = (data["pressure"] - 4.2).abs()
    return data
