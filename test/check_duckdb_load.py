from __future__ import annotations

from pathlib import Path
import duckdb


DB_PATH = Path("data/warehouse/weather.duckdb")


def main() -> None:
    if not DB_PATH.exists():
        raise FileNotFoundError(f"Base introuvable : {DB_PATH}")

    conn = duckdb.connect(str(DB_PATH))

    try:
        tables = conn.execute("SHOW TABLES").fetchall()
        print("Tables :", tables)

        preview = conn.execute(
            """
            SELECT city, time, temperature_2m, precipitation
            FROM raw_weather
            ORDER BY city, time
            LIMIT 10
            """
        ).fetchdf()

        print(preview)

    finally:
        conn.close()


if __name__ == "__main__":
    main()