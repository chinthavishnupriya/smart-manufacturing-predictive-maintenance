
from datetime import timedelta
import time

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from databricks.sdk import WorkspaceClient


# --------------------------------------------------
# DATABRICKS CONFIGURATION
# --------------------------------------------------

DATABRICKS_HOST = (
    "https://dbc-bdad902e-fcf6.cloud.databricks.com"
)

DATABRICKS_JOB_ID = 74232591887481

POLL_INTERVAL_SECONDS = 30
MAX_WAIT_SECONDS = 2 * 60 * 60


# --------------------------------------------------
# DATBRICKS PIPELINE EXECUTION
# --------------------------------------------------

def run_databricks_pipeline():

    # Authenticate using the existing Databricks CLI login.
    client = WorkspaceClient(
        host=DATABRICKS_HOST,
        auth_type="databricks-cli",
    )

    # Trigger the existing Bronze -> Silver -> Gold job.
    response = client.jobs.run_now(
        job_id=DATABRICKS_JOB_ID
    )

    run_id = response.run_id

    if run_id is None:
        raise RuntimeError(
            "Databricks did not return a run ID."
        )

    print(
        f"Started Databricks job: {DATABRICKS_JOB_ID}"
    )
    print(f"Databricks run ID: {run_id}")

    deadline = time.monotonic() + MAX_WAIT_SECONDS

    # Monitor the Databricks job until it finishes.
    while time.monotonic() < deadline:

        run = client.jobs.get_run(
            run_id=run_id
        )

        state = run.state

        lifecycle = (
            state.life_cycle_state if state else None
        )

        result = (
            state.result_state if state else None
        )

        message = (
            state.state_message if state else ""
        )

        # SDK states can be enum objects.
        lifecycle_value = getattr(
            lifecycle, "value", lifecycle
        )

        result_value = getattr(
            result, "value", result
        )

        print(
            f"Run ID: {run_id} | "
            f"Lifecycle: {lifecycle_value} | "
            f"Result: {result_value} | "
            f"Message: {message}"
        )

        # Handle completed or unsuccessful runs.
        if lifecycle_value in (
            "TERMINATED",
            "SKIPPED",
            "INTERNAL_ERROR",
        ):

            if (
                lifecycle_value == "TERMINATED"
                and result_value == "SUCCESS"
            ):
                print(
                    "Bronze -> Silver -> Gold "
                    "pipeline succeeded."
                )
                return

            raise RuntimeError(
                "Databricks job failed. "
                f"Run ID: {run_id}; "
                f"Lifecycle: {lifecycle_value}; "
                f"Result: {result_value}; "
                f"Message: {message}"
            )

        # Continue polling while the job is running.
        time.sleep(POLL_INTERVAL_SECONDS)

    # Timeout: do not automatically launch another run.
    raise TimeoutError(
        f"Timed out waiting for Databricks run {run_id}. "
        "Check the Databricks job status before retrying."
    )


# --------------------------------------------------
# AIRFLOW DAG DEFINITION
# --------------------------------------------------

with DAG(
    dag_id="smart_manufacturing_hourly",

    description=(
        "Hourly orchestration of the Databricks "
        "Bronze-Silver-Gold ETL pipeline"
    ),

    # Run at the start of every hour.
    schedule="0 * * * *",

    start_date=pendulum.datetime(
        2026, 10, 1, tz="Asia/Kolkata"
    ),

    catchup=False,

    # Prevent overlapping scheduled DAG runs.
    max_active_runs=1,

    tags=[
        "smart-manufacturing",
        "databricks",
        "etl",
    ],

) as dag:

    run_pipeline = PythonOperator(
        task_id="run_bronze_silver_gold_pipeline",

        python_callable=run_databricks_pipeline,

        retries=0,

        execution_timeout=timedelta(
            hours=3
        ),
    )
