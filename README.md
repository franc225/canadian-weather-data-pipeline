# Canadian Weather Data Pipeline

![Python](https://img.shields.io/badge/python-3.11-blue)
![DuckDB](https://img.shields.io/badge/database-duckdb-yellow)
![dbt](https://img.shields.io/badge/transform-dbt-orange)
![Airflow](https://img.shields.io/badge/orchestration-airflow-red)

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
DuckDB warehouse
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
│ └─ warehouse
│
├─ src
│ ├─ config.py
│ ├─ ingest_weather_api.py
│ └─ load_duckdb.py
│
├─ test
│ └─ check_ingestion.py
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

Running the ingestion script generates a Parquet dataset:

data/raw/weather/weather_hourly_20260408T142500Z.parquet

Example data:

| city | time | temperature | precipitation |
|----|----|----|----|
| Montreal | 2026-04-08 10:00 | 4.5 | 0.0 |
| Montreal | 2026-04-08 11:00 | 5.1 | 0.0 |

---

# Tests

The project includes validation tests for ingestion.

Example:

test/check_ingestion.py

Tests verify:

- API response structure
- dataset creation
- parquet schema integrity

---

# Roadmap

Planned improvements:

- DuckDB warehouse layer
- dbt transformations
- Airflow orchestration
- Weather analytics dashboard
- Forecasting model