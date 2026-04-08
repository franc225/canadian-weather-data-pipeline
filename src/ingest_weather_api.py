from __future__ import annotations

from datetime import datetime, UTC
from pathlib import Path
from typing import Any

import pandas as pd
import requests

from config import BASE_URL, CITIES, FORECAST_DAYS, HOURLY_VARIABLES, TIMEZONE


def fetch_weather(city: dict[str, Any]) -> pd.DataFrame:
    """
    Fetch hourly weather forecast data for one city from Open-Meteo
    and return a normalized pandas DataFrame.
    """
    params = {
        "latitude": city["latitude"],
        "longitude": city["longitude"],
        "hourly": ",".join(HOURLY_VARIABLES),
        "forecast_days": FORECAST_DAYS,
        "timezone": TIMEZONE,
    }

    response = requests.get(BASE_URL, params=params, timeout=30)
    response.raise_for_status()
    payload = response.json()

    hourly = payload.get("hourly")
    if not hourly:
        raise ValueError(f"No hourly data returned for {city['city']}")

    df = pd.DataFrame(hourly)

    # Ajout des métadonnées ville
    df["city_id"] = city["city_id"]
    df["city"] = city["city"]
    df["province"] = city["province"]
    df["latitude"] = city["latitude"]
    df["longitude"] = city["longitude"]

    # Horodatage technique d'ingestion
    ingested_at_utc = datetime.now(UTC).replace(microsecond=0).isoformat()
    df["ingested_at_utc"] = ingested_at_utc

    # Conversion de la colonne temps
    df["time"] = pd.to_datetime(df["time"], errors="coerce")

    # Colonnes utiles pour partition logique
    df["date"] = df["time"].dt.date.astype(str)

    # Réorganisation des colonnes
    ordered_columns = [
        "city_id",
        "city",
        "province",
        "latitude",
        "longitude",
        "time",
        "date",
        "temperature_2m",
        "relative_humidity_2m",
        "precipitation",
        "surface_pressure",
        "wind_speed_10m",
        "weather_code",
        "ingested_at_utc",
    ]

    return df[ordered_columns]


def save_parquet(df: pd.DataFrame, output_dir: Path) -> Path:
    """
    Save one parquet file per pipeline run.
    """
    output_dir.mkdir(parents=True, exist_ok=True)

    run_ts = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    output_path = output_dir / f"weather_hourly_{run_ts}.parquet"

    df.to_parquet(output_path, index=False)
    return output_path


def main() -> None:
    frames: list[pd.DataFrame] = []

    for city in CITIES:
        print(f"Fetching weather data for {city['city']}...")
        city_df = fetch_weather(city)
        frames.append(city_df)

    final_df = pd.concat(frames, ignore_index=True)

    output_path = save_parquet(final_df, Path("data/raw/weather"))

    print(f"Ingestion completed successfully.")
    print(f"Rows written: {len(final_df)}")
    print(f"File created: {output_path}")


if __name__ == "__main__":
    main()