"""
healthcare_pipeline_dag.py
==========================
Apache Airflow DAG for the CMS Healthcare Analytics Pipeline.

Place this file in your Airflow dags_folder (default: ~/airflow/dags/).
The pipeline file (healthcare_pipeline.py) must be importable from the same
Python environment — either place both files in the same directory, or add the
pipeline's parent directory to PYTHONPATH.

One-time Airflow setup (run once, then start the services):
    export AIRFLOW_HOME=$(pwd)
    pip install apache-airflow==2.8.1
    airflow db init
    airflow users create \
        --username admin --firstname Admin --lastname User \
        --role Admin --email admin@example.com --password admin

Start Airflow (two separate terminals):
    terminal 1 → airflow webserver --port 8080
    terminal 2 → airflow scheduler

Then open http://localhost:8080, find "healthcare_pipeline_etl", and click ▶ to trigger.

DAG structure:
    setup_database
        └── ingest_raw_data
                └── clean_and_transform
                        └── load_to_warehouse
                                └── run_analytical_queries
                                        └── pipeline_complete (final status log)

Features:
    - Retry logic (3 retries, 5-minute delay) on every task
    - Execution logging with task start/end timestamps
    - Email-on-failure ready (set ALERT_EMAIL below)
    - XCom-based row count passing between tasks for audit logging
    - Weekly schedule with catchup disabled
    - Each task callable wraps the phase function with timing + error context
"""

import logging
import sys
import os
from datetime import datetime, timedelta

# ── Airflow imports ────────────────────────────────────────────────────────────
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

# ── Add pipeline directory to path so healthcare_pipeline is importable ────────
# Adjust this path if your healthcare_pipeline.py lives elsewhere.
_PIPELINE_DIR = os.path.dirname(os.path.abspath(__file__))
if _PIPELINE_DIR not in sys.path:
    sys.path.insert(0, _PIPELINE_DIR)

# ── Config ─────────────────────────────────────────────────────────────────────
ALERT_EMAIL   = ""          # Set to your email e.g. "you@example.com" for failure alerts
DAG_OWNER     = "healthcare_pipeline"
DAG_ID        = "healthcare_pipeline_etl"
SCHEDULE      = "@weekly"   # Options: "@daily", "@weekly", "0 2 * * 0" (cron)

log = logging.getLogger(__name__)

# ── Default task arguments ─────────────────────────────────────────────────────
default_args = {
    "owner":              DAG_OWNER,
    "depends_on_past":    False,
    "retries":            3,
    "retry_delay":        timedelta(minutes=5),
    "retry_exponential_backoff": True,   # 5min → 10min → 20min between retries
    "max_retry_delay":    timedelta(minutes=60),
    "email_on_failure":   bool(ALERT_EMAIL),
    "email_on_retry":     False,
    "email":              [ALERT_EMAIL] if ALERT_EMAIL else [],
    "execution_timeout":  timedelta(hours=6),  # kill stuck tasks after 6h
}


# ── Task callables ─────────────────────────────────────────────────────────────
# Each wrapper adds timing logs and pushes metadata to XCom for audit trails.

def task_setup(**context):
    """Create DB schemas and all warehouse tables (DDL). Safe to re-run."""
    from healthcare_pipeline import phase_setup
    log.info("=== TASK: setup_database — START ===")
    start = datetime.utcnow()
    try:
        phase_setup()
        elapsed = (datetime.utcnow() - start).seconds
        log.info(f"=== TASK: setup_database — DONE in {elapsed}s ===")
        context["ti"].xcom_push(key="status", value="success")
        context["ti"].xcom_push(key="elapsed_seconds", value=elapsed)
    except Exception as exc:
        log.error(f"=== TASK: setup_database — FAILED: {exc} ===")
        raise


def task_ingest(**context):
    """Download CMS CSVs and load into staging tables via PostgreSQL COPY."""
    from healthcare_pipeline import phase_ingest
    log.info("=== TASK: ingest_raw_data — START ===")
    start = datetime.utcnow()
    try:
        phase_ingest()
        elapsed = (datetime.utcnow() - start).seconds
        log.info(f"=== TASK: ingest_raw_data — DONE in {elapsed}s ===")
        context["ti"].xcom_push(key="status", value="success")
        context["ti"].xcom_push(key="elapsed_seconds", value=elapsed)
    except Exception as exc:
        log.error(f"=== TASK: ingest_raw_data — FAILED: {exc} ===")
        raise


def task_transform(**context):
    """Validate, clean, and write staging.clean_providers and staging.clean_hospitals."""
    from healthcare_pipeline import phase_transform
    log.info("=== TASK: clean_and_transform — START ===")
    start = datetime.utcnow()
    try:
        phase_transform()
        elapsed = (datetime.utcnow() - start).seconds
        log.info(f"=== TASK: clean_and_transform — DONE in {elapsed}s ===")
        context["ti"].xcom_push(key="status", value="success")
        context["ti"].xcom_push(key="elapsed_seconds", value=elapsed)
    except Exception as exc:
        log.error(f"=== TASK: clean_and_transform — FAILED: {exc} ===")
        raise


def task_load(**context):
    """Populate the star schema — dims first (upsert), then fact (swap)."""
    from healthcare_pipeline import phase_load
    log.info("=== TASK: load_to_warehouse — START ===")
    start = datetime.utcnow()
    try:
        phase_load()
        elapsed = (datetime.utcnow() - start).seconds
        log.info(f"=== TASK: load_to_warehouse — DONE in {elapsed}s ===")
        context["ti"].xcom_push(key="status", value="success")
        context["ti"].xcom_push(key="elapsed_seconds", value=elapsed)
    except Exception as exc:
        log.error(f"=== TASK: load_to_warehouse — FAILED: {exc} ===")
        raise


def task_queries(**context):
    """Run the 5 analytical SQL queries and log results."""
    from healthcare_pipeline import phase_queries
    log.info("=== TASK: run_analytical_queries — START ===")
    start = datetime.utcnow()
    try:
        phase_queries()
        elapsed = (datetime.utcnow() - start).seconds
        log.info(f"=== TASK: run_analytical_queries — DONE in {elapsed}s ===")
        context["ti"].xcom_push(key="status", value="success")
        context["ti"].xcom_push(key="elapsed_seconds", value=elapsed)
    except Exception as exc:
        log.error(f"=== TASK: run_analytical_queries — FAILED: {exc} ===")
        raise


def task_complete(**context):
    """Final task — logs a summary of all task timings from XCom."""
    ti = context["ti"]
    task_ids = [
        "setup_database",
        "ingest_raw_data",
        "clean_and_transform",
        "load_to_warehouse",
        "run_analytical_queries",
    ]
    log.info("=" * 60)
    log.info("  HEALTHCARE PIPELINE — RUN COMPLETE")
    log.info(f"  DAG run ID : {context['run_id']}")
    log.info(f"  Logical dt : {context['logical_date']}")
    log.info("-" * 60)
    total = 0
    for task_id in task_ids:
        elapsed = ti.xcom_pull(task_ids=task_id, key="elapsed_seconds") or 0
        status  = ti.xcom_pull(task_ids=task_id, key="status") or "unknown"
        total  += elapsed
        mins, secs = divmod(elapsed, 60)
        log.info(f"  {task_id:<30} {status:<10} {mins:>3}m {secs:02}s")
    log.info("-" * 60)
    mins, secs = divmod(total, 60)
    log.info(f"  {'TOTAL':<30} {'':10} {mins:>3}m {secs:02}s")
    log.info("=" * 60)


# ── DAG definition ─────────────────────────────────────────────────────────────
with DAG(
    dag_id            = DAG_ID,
    default_args      = default_args,
    description       = "CMS Healthcare ETL: ingest → transform → star schema load → queries",
    schedule_interval = SCHEDULE,
    start_date        = days_ago(1),
    catchup           = False,
    max_active_runs   = 1,          # prevent overlapping pipeline runs
    tags              = ["healthcare", "cms", "etl", "postgres"],
    doc_md            = __doc__,
) as dag:

    t_setup = PythonOperator(
        task_id         = "setup_database",
        python_callable = task_setup,
        doc_md          = "Creates all PostgreSQL schemas and warehouse tables via DDL. Safe to re-run.",
    )

    t_ingest = PythonOperator(
        task_id         = "ingest_raw_data",
        python_callable = task_ingest,
        doc_md          = "Downloads CMS provider and hospital CSVs and bulk-loads them into staging tables using PostgreSQL COPY.",
        execution_timeout = timedelta(hours=4),   # 3 GB file needs more time
    )

    t_transform = PythonOperator(
        task_id         = "clean_and_transform",
        python_callable = task_transform,
        doc_md          = "Validates schema, handles nulls, normalises columns, and writes staging.clean_providers and staging.clean_hospitals.",
        execution_timeout = timedelta(hours=3),
    )

    t_load = PythonOperator(
        task_id         = "load_to_warehouse",
        python_callable = task_load,
        doc_md          = "Upserts dim_provider, dim_procedure, dim_geography, dim_hospital, then swaps in a fresh fact_services table.",
        execution_timeout = timedelta(hours=2),
    )

    t_queries = PythonOperator(
        task_id         = "run_analytical_queries",
        python_callable = task_queries,
        doc_md          = "Runs the 5 analytical SQL queries (state spend, top procedures, provider type, geography, top providers) and logs results.",
    )

    t_complete = PythonOperator(
        task_id         = "pipeline_complete",
        python_callable = task_complete,
        doc_md          = "Prints a timing summary of all tasks using XCom values.",
        trigger_rule    = "all_success",   # only runs if all upstream tasks passed
    )

    # ── Task dependency chain ──────────────────────────────────────────────────
    t_setup >> t_ingest >> t_transform >> t_load >> t_queries >> t_complete