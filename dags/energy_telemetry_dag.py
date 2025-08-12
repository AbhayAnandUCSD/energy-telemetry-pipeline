from __future__ import annotations

import os
import subprocess
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator

# Prefer SparkSubmitOperator; fall back to BashOperator if provider not installed
try:
    from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator  # type: ignore
    HAS_SPARK_PROVIDER = True
except Exception:  # pragma: no cover - import-time detection only
    from airflow.operators.bash import BashOperator  # type: ignore

    SparkSubmitOperator = None  # type: ignore
    HAS_SPARK_PROVIDER = False


DEFAULT_ARGS = {
    "owner": "data-eng",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Resolve project paths
DAGS_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = Path(os.environ.get("ENERGY_PIPELINE_ROOT", DAGS_DIR.parent))
SCRIPTS_DIR = PROJECT_ROOT / "scripts"
SPARK_JOBS_DIR = PROJECT_ROOT / "spark_jobs"


def run_generator(start: str, rows_per_device: int = 600) -> None:
    script = str(SCRIPTS_DIR / "generate_data.py")
    cmd = [
        "python",
        script,
        "--start",
        start,
        "--days",
        "1",
        "--rows-per-device",
        str(rows_per_device),
    ]
    subprocess.run(cmd, check=True)


with DAG(
    dag_id="energy_telemetry_daily",
    default_args=DEFAULT_ARGS,
    description="Daily energy telemetry pipeline: generate -> transform (Silver/Gold)",
    schedule_interval="0 2 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["energy", "spark", "bronze-silver-gold"],
) as dag:
    generate_raw = PythonOperator(
        task_id="generate_raw",
        python_callable=run_generator,
        op_kwargs={"start": "{{ ds }}", "rows_per_device": 600},
    )

    spark_script = str(SPARK_JOBS_DIR / "transform_telemetry.py")

    if HAS_SPARK_PROVIDER:
        spark_transform = SparkSubmitOperator(
            task_id="spark_transform",
            application=spark_script,
            conn_id="spark_default",
            application_args=["--process_date", "{{ ds }}"],
        )
    else:
        from airflow.operators.bash import BashOperator  # type: ignore

        spark_transform = BashOperator(
            task_id="spark_transform",
            bash_command="spark-submit {{ params.script }} --process_date {{ ds }}",
            params={"script": spark_script},
        )

    generate_raw >> spark_transform
