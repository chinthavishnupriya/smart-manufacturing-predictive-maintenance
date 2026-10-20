"""Clean and validate the raw predictive-maintenance sensor dataset."""
from pathlib import Path
import pandas as pd
PROJECT_ROOT = Path(__file__).resolve().parents[1]
RAW_FILE = PROJECT_ROOT / "data" / "raw" / "sensor_data_100k.csv"
PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"
OUTPUT_FILE = PROCESSED_DIR / "cleaned_data.csv"
REQUIRED_COLUMNS = ["machine_id","timestamp","temperature","vibration","rpm","pressure","operating_hours","failure"]
NUMERIC_COLUMNS = ["temperature","vibration","rpm","pressure","operating_hours","failure"]

def clean_data() -> pd.DataFrame:
    if not RAW_FILE.exists(): raise FileNotFoundError(f"Raw dataset not found: {RAW_FILE}")
    df = pd.read_csv(RAW_FILE)
    original_rows = len(df)
    missing_columns = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing_columns: raise ValueError(f"Missing required columns: {missing_columns}")
    df = df[REQUIRED_COLUMNS].copy()
    duplicates = int(df.duplicated().sum())
    df = df.drop_duplicates().copy()
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    invalid_timestamps = int(df["timestamp"].isna().sum())
    for c in NUMERIC_COLUMNS: df[c] = pd.to_numeric(df[c], errors="coerce")
    invalid_before = int(df.isna().sum().sum())
    df = df.dropna(subset=NUMERIC_COLUMNS + ["machine_id"]).copy()
    df["machine_id"] = df["machine_id"].astype(str).str.strip().str.upper()
    df["failure"] = df["failure"].astype(int)
    if not set(df["failure"].unique()).issubset({0,1}): raise ValueError("failure must contain only 0 and 1")
    df = df.sort_values(["machine_id","timestamp"]).reset_index(drop=True)
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    df.to_csv(OUTPUT_FILE, index=False)
    print(f"Original rows: {original_rows:,}\nDuplicates removed: {duplicates:,}\nInvalid timestamps: {invalid_timestamps:,}\nRows with invalid/missing required values: {invalid_before:,}\nFinal rows: {len(df):,}\nSaved: {OUTPUT_FILE}")
    return df
if __name__ == "__main__": clean_data()
