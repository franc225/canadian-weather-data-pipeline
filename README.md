# Canadian Weather Data Pipeline

![Python](https://img.shields.io/badge/python-3.11-blue)
![DuckDB](https://img.shields.io/badge/database-duckdb-yellow)
![dbt](https://img.shields.io/badge/transform-dbt-orange)
![Airflow](https://img.shields.io/badge/orchestration-airflow-red)
![status](https://img.shields.io/badge/status-in%20progress-blue)

End-to-end **data engineering pipeline** that ingests weather data from a public API and builds an analytical dataset using a modern data stack.

The project demonstrates how to build a small **data platform locally** using Python, Parquet, DuckDB, dbt and Airflow.

---

# Architecture

```text
Open-Meteo API
↓
Python ingestion
↓
Parquet data lake (raw)
↓
DuckDB warehouse (raw_weather table)
↓
dbt transformations
↓
Analytics / dashboards / forecasting
```

---

# Stack

| Layer | Technology |
|-----|-----|
| Ingestion | Python |
| Data source | Open-Meteo API |
| Data lake | Parquet |
| Warehouse | DuckDB |
| Transformation | dbt |
| Orchestration | Airflow |
| Validation | Python tests |

---

# Project Structure

```text
canadian-weather-data-pipeline
│
├─ data
│ ├─ raw
│ │ └─ weather
│ │      └─ weather_hourly_YYYYMMDDTHHMMSS.parquet
│ └─ warehouse
│     └─ weather.duckdb
│
├─ src
│ ├─ config.py
│ ├─ ingest_weather_api.py
│ └─ load_duckdb.py
│
├─ test
│ └─ check_ingestion.py
│ └─ check_duckdb_load.py
│
├─ notebooks
│
├─ requirements.txt
└─ README.md
```

---

# Data Source

Weather data is retrieved from:

**Open-Meteo API**

https://open-meteo.com/

The pipeline currently collects hourly data for several Canadian cities including:

- Montreal
- Quebec City
- Toronto

Variables collected:

- temperature
- humidity
- precipitation
- wind speed
- surface pressure
- weather code

---

# Example Pipeline Run

Step 1 — API ingestion

Running the ingestion script generates a raw dataset stored in Parquet.

Example file:

data/raw/weather/weather_hourly_20260408T142500Z.parquet

Example rows:

city	time	temperature	precipitation
Montreal	2026-04-08 10:00	4.5	0.0
Montreal	2026-04-08 11:00	5.1	0.0

Step 2 — Load into DuckDB

The raw Parquet files are loaded into a local DuckDB database.

Database file:

data/warehouse/weather.duckdb

Main table created:

raw_weather

Example query:

SELECT city, time, temperature_2m, precipitation
FROM raw_weather
ORDER BY city, time
LIMIT 10;

---

# Tests

The project includes validation tests.

Example tests:

test/check_ingestion.py
test/check_duckdb_load.py

Tests verify:

API response structure
parquet dataset creation
successful DuckDB load
dataset schema integrity

---

# Roadmap

Roadmap

Planned improvements:

- staging layer for weather data
- dbt transformations
- dimensional model (star schema)
- Airflow orchestration
- weather analytics dashboard
- forecasting model