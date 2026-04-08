from __future__ import annotations

from pathlib import Path
import duckdb


DB_PATH = Path("data/warehouse/weather.duckdb")
RAW_PARQUET_GLOB = "data/raw/weather/*.parquet"


def main() -> None:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)

    conn = duckdb.connect(str(DB_PATH))

    try:
        parquet_files = list(Path("data/raw/weather").glob("*.parquet"))
        if not parquet_files:
            raise FileNotFoundError(
                "Aucun fichier parquet trouvé dans data/raw/weather"
            )

        conn.execute(
            f"""
            CREATE OR REPLACE TABLE raw_weather AS
            SELECT *
            FROM read_parquet('{RAW_PARQUET_GLOB}', union_by_name = true)
            """
        )

        row_count = conn.execute(
            "SELECT COUNT(*) FROM raw_weather"
        ).fetchone()[0]

        min_time, max_time = conn.execute(
            """
            SELECT MIN(time), MAX(time)
            FROM raw_weather
            """
        ).fetchone()

        city_count = conn.execute(
            "SELECT COUNT(DISTINCT city) FROM raw_weather"
        ).fetchone()[0]

        print("Chargement DuckDB terminé avec succès.")
        print(f"Base créée : {DB_PATH}")
        print(f"Lignes chargées : {row_count}")
        print(f"Villes distinctes : {city_count}")
        print(f"Période couverte : {min_time} à {max_time}")

    finally:
        conn.close()


if __name__ == "__main__":
    main()