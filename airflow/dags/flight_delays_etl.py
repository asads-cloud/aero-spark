# isort: skip_file
from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.sensors.s3 import S3PrefixSensor
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator

# Optional GE (guarded by variable)
try:
    from great_expectations_provider.operators.great_expectations import (
        GreatExpectationsOperator,  # type: ignore
    )

    GE_AVAILABLE = True
except Exception:
    GE_AVAILABLE = False


def _get_var(name: str, default: str | None = None) -> str:
    v = Variable.get(name, default_var=default)
    if v is None:
        raise ValueError(f"Airflow Variable {name} is required")
    return v


with DAG(
    dag_id="flight_delays_etl",
    description="Aero Spark: Daily ETL for flight delays",
    schedule="0 2 * * *",  # 02:00 UTC daily
    start_date=datetime(2025, 10, 1),
    catchup=False,
    dagrun_timeout=timedelta(hours=2),
    max_active_runs=1,
    tags=["aero-spark", "phase3", "mwaa"],
) as dag:
    # ---- Config from Airflow Variables ----
    DATA_BUCKET = _get_var("DATA_BUCKET")  # e.g. aero-spark-data-uc3pslda
    RAW_PREFIX_ROOT = "raw/flight_delays"
    PROCESSED_PREFIX = "processed/flight_delays"
    ENABLE_GE = Variable.get("ENABLE_GE", default_var="false").lower() == "true"

    # Databricks integration
    DATABRICKS_JOB_ID = _get_var("DATABRICKS_JOB_ID", default=None)
    if not DATABRICKS_JOB_ID:
        raise ValueError(
            "Set Airflow Variable DATABRICKS_JOB_ID to your Databricks job id."
        )

    # ---- Templates for this run date (Airflow supplies ds=YYYY-MM-DD) ----
    raw_ingest_prefix_tmpl = f"{RAW_PREFIX_ROOT}/" + "ingest_date={{ ds }}/"
    processed_prefix_tmpl = f"{PROCESSED_PREFIX}/" + "run_date={{ ds }}/"

    # 1) Wait for raw files for {{ ds }}
    wait_raw = S3PrefixSensor(
        task_id="wait_raw",
        bucket_name=DATA_BUCKET,
        prefix=raw_ingest_prefix_tmpl,
        aws_conn_id="aws_default",
        poke_interval=60,  # seconds
        timeout=60 * 60,  # 1 hour
        mode="reschedule",
        soft_fail=False,
    )

    # 2) (Optional) Great Expectations validation
    if ENABLE_GE and GE_AVAILABLE:
        ge_validate = GreatExpectationsOperator(
            task_id="ge_validate_raw",
            data_context_root_dir="/usr/local/airflow/dags/great_expectations",
            checkpoint_name="flight_delays_raw_checkpoint",
            fail_task_on_validation_failure=True,
            return_json_dict=True,
        )
    else:
        ge_validate = EmptyOperator(task_id="ge_validate_skipped")

    # 3) Trigger Databricks job run for {{ ds }}
    databricks_run = DatabricksRunNowOperator(
        task_id="transform_with_spark",
        databricks_conn_id="databricks_default",
        json={
            "job_id": int(DATABRICKS_JOB_ID),
            "job_parameters": {
                "input_path": "s3://" + DATA_BUCKET + "/" + raw_ingest_prefix_tmpl,
                "output_path": "s3://" + DATA_BUCKET + "/" + PROCESSED_PREFIX + "/",
                "run_date": "{{ ds }}",
            },
        },
    )

    # 4) Post-run verification — confirm processed keys exist
    def _verify_processed(ds: str, **_):
        """Check that at least one Parquet file landed under processed/ for this ds."""
        hook = S3Hook(aws_conn_id="aws_default")
        prefix = processed_prefix_tmpl.replace("{{ ds }}", ds)
        keys = hook.list_keys(bucket_name=DATA_BUCKET, prefix=prefix) or []
        if not keys:
            raise ValueError(f"No processed objects under s3://{DATA_BUCKET}/{prefix}")
        if not any(k.endswith(".parquet") for k in keys):
            raise ValueError(
                f"Processed prefix has no .parquet files: s3://{DATA_BUCKET}/{prefix}"
            )

    verify_processed = PythonOperator(
        task_id="verify_processed",
        python_callable=_verify_processed,
    )
    verify_processed.trigger_rule = TriggerRule.ALL_SUCCESS

    # ---- Orchestration ----
    wait_raw >> ge_validate >> databricks_run >> verify_processed
