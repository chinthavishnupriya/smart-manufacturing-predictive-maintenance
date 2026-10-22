# Smart Manufacturing Predictive Maintenance

An end-to-end student project that uses 100,000 machine sensor records to predict machine failure risk.

## Architecture

Raw CSV -> Cleaning -> Feature Engineering -> Random Forest -> Evaluation -> FastAPI -> Streamlit Dashboard
                                      ^
                                      |
                               IoT Sensor Simulator

## Dataset

The included dataset contains 100,000 records, 100 machines and these fields:
`machine_id`, `timestamp`, `temperature`, `vibration`, `rpm`, `pressure`, `operating_hours`, `failure`.

## 1. Create a virtual environment (recommended)

Windows PowerShell:
```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
```

Windows CMD:
```cmd
python -m venv .venv
.venv\Scripts\activate
```

## 2. Install dependencies

```bash
pip install -r requirements.txt
```

## 3. Run the complete ML pipeline

```bash
python run_pipeline.py
```

This creates:
- `data/processed/cleaned_data.csv`
- `models/predictive_maintenance_model.joblib`
- `outputs/metrics.json`
- `outputs/plots/confusion_matrix.png`
- `outputs/plots/roc_curve.png`
- `outputs/plots/feature_importance.png`

## 4. Test a prediction from the terminal

```bash
python models/predict.py
```

## 5. Start the API

```bash
uvicorn api.main:app --reload
```

Open the API documentation at `http://127.0.0.1:8000/docs`.

## 6. Start the dashboard

Keep the API running and open another VS Code terminal:

```bash
streamlit run dashboard/app.py
```

The dashboard shows dataset statistics, sensor charts, model metrics and a live prediction form.

## 7. Run the IoT simulator

With the API running:

```bash
python ingestion/iot_simulator/producer.py --api-url http://127.0.0.1:8000/predict --limit 20
```

Without API calls:

```bash
python ingestion/iot_simulator/producer.py --limit 20
```

## Project folders

- `data/raw` — original 100K dataset
- `data/processed` — cleaned dataset
- `data_processing` — cleaning and feature engineering
- `models` — training and prediction
- `api` — FastAPI inference service
- `dashboard` — Streamlit UI
- `ingestion/iot_simulator` — simulated sensor stream
- `outputs` — metrics and charts

## Important ML note

Failure is an imbalanced target in this dataset. Accuracy is therefore not enough to judge the model. The training script reports precision, recall, F1 and ROC-AUC as well as accuracy.

## Model version compatibility

The saved model artifacts were created with different scikit-learn versions:

- `models/predictive_maintenance_model.joblib` (FastAPI default): scikit-learn 1.8.0.
- `models/predictive_maintenance_model_v191.joblib` (terminal prediction default): scikit-learn 1.9.1.
- `models/predictive_maintenance_model_no_time_experimental.joblib` (experimental): scikit-learn 1.9.1, based on the recorded validation environment.

Use the matching scikit-learn version when loading each model. Loading a model with a different version may produce compatibility warnings or unexpected behavior. Matching versions are recommended; successful loading alone does not guarantee compatibility.

The repository's `requirements.txt` currently permits scikit-learn versions from 1.5 up to, but not including, 2.0. The dependency range does not enforce the version used to create each saved model.


## Hadoop, Sqoop and streaming resilience acceptance

Acceptance work is maintained on the `acceptance/hadoop-sqoop-resilience` branch and tracked in [PR #1](https://github.com/chinthavishnupriya/smart-manufacturing-predictive-maintenance/pull/1).

- `hadoop/mapreduce/` — Hadoop Streaming mapper/reducer and HDFS/YARN job runner. The recorded cluster job summarized 100,000 sensor rows into 100 machine rows.
- `hadoop/sqoop/` — MySQL source schema and Sqoop imports for `machine_master`, `maintenance_history`, and `work_orders`; the recorded demo imports matched source counts (5, 3, and 2).
- `kafka/resilience/` — separate three-broker Kafka stack and broker-failure/zero-loss drill; 40 acknowledged message IDs were recovered in the recorded outage test.
- `spark_streaming/resilience/` — isolated checkpoint replay, duplicate-event, and Kafka-to-HBase latency drills.
- `scripts/databricks_reconcile_counts.py` — query Bronze/Silver/Gold table counts with the configured Databricks SQL warehouse.
- `docs/resilience_acceptance.md` and `PART3_STREAMING_VALIDATION.md` — test evidence, limitations, and remaining acceptance gates.

### Current acceptance snapshot (2026-10-22)

- **Passed in recorded runs:** Hadoop MapReduce; three Sqoop demo imports with count reconciliation; the tested Kafka broker outage; duplicate payload suppression in the isolated Spark/HBase test (all 10 rows remained at `event_count=1`); warmed sequential streaming latency under the instructor's 10-second threshold (10 events, p95/max 3.336 seconds).
- **Requires one regression rerun:** Spark checkpoint recovery after the event-ID/window-aggregation code change.
- **Still limited/unverified:** burst-load latency (a prior burst-style run measured p95 18.75 seconds), Databricks run-scoped row reconciliation, and production-scale behavior of the windowed payload set.
- **ML caveat:** machine-held-out RUL evaluation was weak (R² below zero); treat RUL output as experimental, not production-ready.

A test harness is not acceptance evidence by itself. Use the logs and row counts linked in `docs/resilience_acceptance.md`, rerun checks after streaming-code changes, and do not claim universal exactly-once or production-scale guarantees from the local drills.
