# Data Processing

This folder contains scripts used to prepare the raw sensor data for analysis and machine-learning work.

## Clean the dataset

From the project root, run:

```bash
python data_processing/clean_data.py
```

The cleaned dataset will be created at:

```text
data/processed/cleaned_data.csv
```

The script:

1. Loads the 100K raw sensor dataset.
2. Validates the required columns.
3. Removes exact duplicate rows.
4. Converts timestamps to datetime.
5. Converts sensor values to numeric values.
6. Removes rows with invalid required values.
7. Normalizes machine IDs.
8. Sorts records by machine and timestamp.
9. Saves the cleaned dataset for the next project stages.
