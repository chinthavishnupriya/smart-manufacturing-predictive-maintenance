import pandas as pd
import numpy as np
from pathlib import Path


# ============================================================
# PATHS
# ============================================================

BASE_DIR = Path(__file__).resolve().parent.parent

INPUT_FILE = (
    BASE_DIR
    / "data"
    / "raw"
    / "sensor_data_100k.csv"
)

OUTPUT_FILE = (
    BASE_DIR
    / "data"
    / "processed"
    / "rul_data.csv"
)


# ============================================================
# 1. LOAD DATASET
# ============================================================

print("=" * 60)
print("RUL DATASET GENERATION")
print("=" * 60)

print("\nLoading sensor dataset...")

df = pd.read_csv(INPUT_FILE)

print(f"Original records: {len(df)}")


# ============================================================
# 2. PREPARE TIMESTAMP
# ============================================================

print("\nPreparing timestamps...")

df["timestamp"] = pd.to_datetime(
    df["timestamp"],
    errors="coerce"
)

# Remove invalid timestamps
df = df.dropna(
    subset=["timestamp"]
).copy()

# Sort by machine and time
df = df.sort_values(
    ["machine_id", "timestamp"]
).reset_index(drop=True)


print(f"Records after timestamp validation: {len(df)}")


# ============================================================
# 3. FIND NEXT FAILURE FOR EACH MACHINE
# ============================================================

print("\nCalculating next failure timestamp...")

# Create an empty column
df["failure_timestamp"] = pd.NaT

# Process each machine separately
for machine_id, group in df.groupby("machine_id"):

    # Get all failure timestamps for this machine
    failure_times = (
        group.loc[
            group["failure"] == 1,
            "timestamp"
        ]
        .sort_values()
        .to_numpy()
    )

    # If this machine has no failures,
    # it cannot be used for RUL calculation.
    if len(failure_times) == 0:
        continue

    # Current timestamps
    timestamps = group["timestamp"].to_numpy()

    # Find the first failure timestamp
    # that occurs at or after each sensor reading.
    positions = np.searchsorted(
        failure_times,
        timestamps,
        side="left"
    )

    # Identify timestamps that have a future failure
    valid = positions < len(failure_times)

    # Create empty failure timestamp array
    failure_values = np.full(
        len(group),
        np.datetime64("NaT"),
        dtype="datetime64[ns]"
    )

    # Assign next failure timestamps
    failure_values[valid] = (
        failure_times[positions[valid]]
    )

    # Store them back into the main DataFrame
    df.loc[
        group.index,
        "failure_timestamp"
    ] = failure_values


# ============================================================
# 4. CALCULATE RUL
# ============================================================

print("Calculating RUL in hours...")

df["rul_hours"] = (
    df["failure_timestamp"]
    - df["timestamp"]
).dt.total_seconds() / 3600


# ============================================================
# 5. REMOVE RECORDS WITHOUT FUTURE FAILURE
# ============================================================

before = len(df)

df = df.dropna(
    subset=["rul_hours"]
).copy()

after = len(df)

print(f"\nRecords before RUL filtering: {before}")
print(f"Records with valid RUL:      {after}")
print(f"Records removed:             {before - after}")


# ============================================================
# 6. REMOVE HELPER COLUMN
# ============================================================

df = df.drop(
    columns=["failure_timestamp"]
)


# ============================================================
# 7. VALIDATE RUL VALUES
# ============================================================

print("\nValidating RUL values...")

# RUL should never be negative
negative_rul = (
    df["rul_hours"] < 0
).sum()

print(f"Negative RUL records: {negative_rul}")

if negative_rul > 0:
    print("WARNING: Negative RUL values detected.")
else:
    print("RUL validation passed.")


# ============================================================
# 8. CREATE OUTPUT DIRECTORY
# ============================================================

OUTPUT_FILE.parent.mkdir(
    parents=True,
    exist_ok=True
)


# ============================================================
# 9. SAVE RUL DATASET
# ============================================================

df.to_csv(
    OUTPUT_FILE,
    index=False
)


# ============================================================
# 10. DISPLAY RESULTS
# ============================================================

print("\n" + "=" * 60)
print("RUL DATASET CREATED SUCCESSFULLY")
print("=" * 60)

print(f"\nOutput file:")
print(OUTPUT_FILE)

print(f"\nTotal RUL records: {len(df)}")

print("\nColumns:")
for column in df.columns:
    print(f"  - {column}")


# ============================================================
# 11. RUL STATISTICS
# ============================================================

print("\n" + "-" * 60)
print("RUL STATISTICS")
print("-" * 60)

print(
    df["rul_hours"].describe()
)


# ============================================================
# 12. FAILURE RECORD CHECK
# ============================================================

print("\n" + "-" * 60)
print("FAILURE RECORD CHECK")
print("-" * 60)

failure_rul = df.loc[
    df["failure"] == 1,
    "rul_hours"
]

print(
    f"Failure records: {len(failure_rul)}"
)

if len(failure_rul) > 0:
    print(
        f"Minimum RUL at failure: "
        f"{failure_rul.min():.2f} hours"
    )

    print(
        f"Maximum RUL at failure: "
        f"{failure_rul.max():.2f} hours"
    )


# ============================================================
# 13. SAMPLE DATA
# ============================================================

print("\n" + "-" * 60)
print("SAMPLE RUL DATA")
print("-" * 60)

sample_columns = [
    "machine_id",
    "timestamp",
    "temperature",
    "vibration",
    "rpm",
    "pressure",
    "operating_hours",
    "failure",
    "rul_hours"
]

print(
    df[sample_columns].head(10).to_string(
        index=False
    )
)

print("\n" + "=" * 60)
print("DONE")
print("=" * 60)