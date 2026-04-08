from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator


PROJECT_DIR = "/mnt/c/dev/canadian-weather-data-pipeline"
PYTHON_BIN = f"{PROJECT_DIR}/airflow_venv/bin/python"
DBT_BIN = f"{PROJECT_DIR}/airflow_venv/bin/dbt"
DBT_DIR = f"{PROJECT_DIR}/dbt_weather"
DUCKDB_PATH = f"{PROJECT_DIR}/data/warehouse/weather.duckdb"

with DAG(
    dag_id="weather_pipeline",
    start_date=datetime(2026, 4, 8),
    schedule=None,
    catchup=False,
    tags=["weather", "data-engineering", "dbt", "duckdb"],
    description="Ingest weather data, load DuckDB, then run dbt build",
) as dag:

    ingest_weather_api = BashOperator(
        task_id="ingest_weather_api",
        bash_command=(
            f"cd {PROJECT_DIR} && "
            f"{PYTHON_BIN} src/ingest_weather_api.py"
        ),
    )

    load_duckdb = BashOperator(
        task_id="load_duckdb",
        bash_command=(
            f"cd {PROJECT_DIR} && "
            f"{PYTHON_BIN} src/load_duckdb.py"
        ),
    )

    dbt_build = BashOperator(
        task_id="dbt_build",
        env={"DBT_DUCKDB_PATH": DUCKDB_PATH},
        bash_command=f"cd {DBT_DIR} && {DBT_BIN} build --profiles-dir .",
    )

    ingest_weather_api >> load_duckdb >> dbt_build