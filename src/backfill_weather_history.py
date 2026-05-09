from __future__ import annotations

import argparse
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import requests

from config import CITIES, HOURLY_VARIABLES

BASE_URL = "https://archive-api.open-meteo.com/v1/archive"


def fetch_historical_weather(
    city: dict[str, Any],
    start_date: str,
    end_date: str,
) -> pd.DataFrame:
    params = {
        "latitude": city["latitude"],
        "longitude": city["longitude"],
        "start_date": start_date,
        "end_date": end_date,
        "hourly": ",".join(HOURLY_VARIABLES),
        "timezone": "auto",
    }

    response = requests.get(BASE_URL, params=params, timeout=60)
    response.raise_for_status()
    payload = response.json()

    hourly = payload.get("hourly")
    if not hourly:
        raise ValueError(f"No historical hourly data returned for {city['city']}")

    df = pd.DataFrame(hourly)

    df["city_id"] = city["city_id"]
    df["city"] = city["city"]
    df["province"] = city["province"]
    df["latitude"] = city["latitude"]
    df["longitude"] = city["longitude"]

    df["ingested_at_utc"] = datetime.now(timezone.utc).replace(microsecond=0).isoformat()

    df["time"] = pd.to_datetime(df["time"], errors="coerce")
    df["date"] = df["time"].dt.date.astype(str)

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


def save_parquet(df: pd.DataFrame, start_date: str, end_date: str, output_dir: Path) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)

    run_ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    output_path = output_dir / f"weather_history_{start_date}_to_{end_date}_{run_ts}.parquet"

    df.to_parquet(output_path, index=False)
    return output_path


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill historical weather data from Open-Meteo.")
    parser.add_argument("--start-date", required=True, help="Start date, format YYYY-MM-DD")
    parser.add_argument("--end-date", required=True, help="End date, format YYYY-MM-DD")

    args = parser.parse_args()

    frames: list[pd.DataFrame] = []

    for city in CITIES:
        print(f"Fetching historical weather data for {city['city']}...")
        city_df = fetch_historical_weather(
            city=city,
            start_date=args.start_date,
            end_date=args.end_date,
        )
        frames.append(city_df)

    final_df = pd.concat(frames, ignore_index=True)

    output_path = save_parquet(
        df=final_df,
        start_date=args.start_date,
        end_date=args.end_date,
        output_dir=Path("data/raw/weather"),
    )

    print("Historical backfill completed successfully.")
    print(f"Rows written: {len(final_df)}")
    print(f"File created: {output_path}")


if __name__ == "__main__":
    main()