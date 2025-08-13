# energy-telemetry-pipeline

A portfolio-ready, end-to-end data engineering project that simulates energy device telemetry and processes it through a Bronze/Silver/Gold lakehouse pattern using PySpark, with daily orchestration in Airflow. This mirrors real-world telemetry workloads at EV/energy companies (think Tesla Megapack/Powerwall style), with idempotent partitioned writes and data-quality metrics for interview discussion.

## Architecture
- **Bronze (raw)**: JSON Lines generated per device per site per day
  - Path: `data/raw/YYYY/MM/DD/<site_id>/<device_id>.jsonl`
- **Silver (cleaned/enriched)**: Parquet partitioned by `event_date` and `site_id`
  - Cleaning: duplicate key drop, cap negative power to 0, bounds checks for `soc_percent` and `temperature_c`
  - Enrichment: broadcast join to `data/reference/devices.csv` for `capacity_kwh`
  - Write: `mode=overwrite` with `repartition("event_date","site_id")` to mitigate small files
- **Gold (KPIs)**: Daily site-level aggregates in Parquet
  - Metrics: `energy_wh_total`, `peak_power_w`, `avg_temp_c`, `uptime_percent`, `anomaly_count`
- **Quality metrics**: JSON summary per day under `data/metrics/quality_<date>.json`
- **Idempotency**: Partition overwrite + deterministic inputs → safe re-runs for the same date

## Quickstart
Choose one of the following:

- Minimal (Spark only):
```bash
python -m venv .venv
source .venv/bin/activate
pip install pyspark==3.5.1 pyarrow==16.1.0 pandas==2.2.2
```
- Full (includes Airflow):
```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt  # use --no-cache-dir if low disk space
```

Generate 1 day of telemetry (1,440 rows/device/day):
```bash
python scripts/generate_data.py --start 2025-08-01 --days 1 --rows-per-device 1440
```

Run the Spark transform for that date:
```bash
spark-submit spark_jobs/transform_telemetry.py --process_date 2025-08-01
```

Results:
- Silver Parquet: `data/silver/event_date=2025-08-01/site_id=.../`
- Gold Parquet: `data/gold/event_date=2025-08-01/site_id=.../`
- Metrics JSON: `data/metrics/quality_2025-08-01.json`

Re-running the same date will overwrite the same partitions (idempotent, no duplication).

## Validate outputs
List partitions and show metrics:
```bash
ls -R data/silver | sed -n '1,80p'
ls -R data/gold   | sed -n '1,80p'
cat data/metrics/quality_2025-08-01.json
```

## Airflow
Copy the DAG to your Airflow `dags` directory (or set `AIRFLOW_HOME` and symlink):
```bash
export AIRFLOW_HOME=~/airflow
mkdir -p "$AIRFLOW_HOME/dags"
cp -r dags/energy_telemetry_dag.py "$AIRFLOW_HOME/dags/"
# Let the DAG find this repo reliably (optional but recommended)
export ENERGY_PIPELINE_ROOT=$(pwd)
```

Make sure the `spark_default` connection exists (UI → Admin → Connections) or set via env. The DAG prefers `SparkSubmitOperator` from `apache-airflow-providers-apache-spark`; if not installed, it falls back to a `BashOperator` running `spark-submit`.

DAG details:
- `energy_telemetry_daily`, schedule `0 2 * * *` (02:00 daily), `catchup=False`
- Tasks:
  - `generate_raw`: calls `scripts/generate_data.py --start {{ ds }} --days 1 --rows-per-device 600`
  - `spark_transform`: runs `spark_jobs/transform_telemetry.py --process_date {{ ds }}`
- Dependency: `generate_raw >> spark_transform`

## Talking points
- **Broadcast join**: Small dimension (`devices.csv`) broadcast to all executors for efficient enrichment
- **Partition strategy**: `event_date`, `site_id` supports pruning, scalable multi-tenant sites
- **Small-files mitigation**: `repartition("event_date","site_id")` and dynamic partition overwrite
- **Data quality**: metrics JSON (nulls, duplicates); `sql/quality_checks.sql` provides warehouse-side queries
- **Idempotency**: overwrite partitions → safe reprocessing for late-arriving or corrected data
- **Extensibility**: swap local FS for S3/MinIO, add streaming source, or integrate Great Expectations

## Troubleshooting
- Low disk space (Airflow has many dependencies):
  - Use the minimal install (`pyspark`, `pyarrow`, `pandas`) to run Spark locally
  - Or `pip install --no-cache-dir -r requirements.txt`, and clean caches: `pip cache purge`, `brew cleanup`
- Java/Spark:
  - Spark 3.5+ requires Java 8–21; macOS ARM64 works out of the box with the embedded Hadoop client
- Timestamp parsing:
  - Generator writes ISO timestamps with `Z`; transform uses an explicit pattern to parse reliably

## Repo layout
```
energy-telemetry-pipeline/
├── README.md
├── requirements.txt
├── dags/
│   └── energy_telemetry_dag.py
├── scripts/
│   └── generate_data.py
├── spark_jobs/
│   └── transform_telemetry.py
├── data/
│   ├── reference/
│   │   └── devices.csv
│   ├── raw/
│   ├── silver/
│   ├── gold/
│   └── metrics/
└── sql/
    └── quality_checks.sql
```

## Example SQL quality checks
See `sql/quality_checks.sql` for null counts, bounds checks, and duplicate detection.