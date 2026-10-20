import joblib
import pandas as pd

from pathlib import Path


# ============================================================
# PATH
# ============================================================

BASE_DIR = Path(__file__).resolve().parent.parent

MODEL_FILE = (
    BASE_DIR
    / "models"
    / "rul_model.joblib"
)


# ============================================================
# LOAD MODEL
# ============================================================

print("=" * 60)
print("RUL PREDICTION TEST")
print("=" * 60)

print("\nLoading RUL model...")

model = joblib.load(MODEL_FILE)

print("RUL model loaded successfully.")


# ============================================================
# TEST SENSOR DATA
# ============================================================

sensor_data = pd.DataFrame([
    {
        "temperature": 75.0,
        "vibration": 4.5,
        "rpm": 1450.0,
        "pressure": 4.0,
        "operating_hours": 600.0
    },
    {
        "temperature": 105.0,
        "vibration": 7.5,
        "rpm": 3000.0,
        "pressure": 10.0,
        "operating_hours": 5000.0
    }
])


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


# ============================================================
# PREDICT
# ============================================================

predictions = model.predict(
    sensor_data[features]
)


# ============================================================
# DISPLAY
# ============================================================

print("\n" + "-" * 60)
print("RUL PREDICTIONS")
print("-" * 60)

for index, prediction in enumerate(predictions, start=1):

    print(
        f"Test Sensor {index}: "
        f"Predicted RUL = {prediction:.2f} hours "
        f"({prediction * 60:.1f} minutes)"
    )


print("\n" + "=" * 60)
print("RUL PREDICTION TEST COMPLETED")
print("=" * 60)