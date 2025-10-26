from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG

# No operators yet; this just validates MWAA can parse the DAG.
with DAG(
    dag_id="flight_delays_etl",
    description="Aero Spark: Daily ETL for flight delays",
    schedule="0 2 * * *",  # 02:00 UTC daily
    start_date=datetime(2025, 10, 1),
    catchup=False,
    dagrun_timeout=timedelta(hours=2),
    tags=["aero-spark", "phase3", "mwaa"],
) as dag:
    pass
