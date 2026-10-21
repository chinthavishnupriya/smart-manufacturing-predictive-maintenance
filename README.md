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


## Hadoop, Sqoop and resilience acceptance

Additional acceptance modules are being prepared on the `acceptance/hadoop-sqoop-resilience` branch:

- `hadoop/mapreduce/` — Hadoop Streaming mapper/reducer, local tests, and an HDFS/YARN job runner.
- `hadoop/sqoop/` — demo MySQL source schema and Sqoop imports for machine master, maintenance history and work orders.
- `kafka/resilience/` — separate three-broker Kafka Compose stack and broker-failure/zero-loss drill.
- `spark_streaming/resilience/` — checkpoint restart, duplicate-event and HBase idempotency drill.
- `docs/resilience_acceptance.md` — commands, scope, prerequisites, and acceptance evidence checklist.

These scripts do not by themselves mark the operational requirements as passed. Run the drills against the required services and retain their logs/count comparisons before claiming acceptance.
