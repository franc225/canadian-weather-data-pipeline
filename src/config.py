from __future__ import annotations

CITIES = [
    {"city_id": 1, "city": "Montreal", "province": "QC", "latitude": 45.5017, "longitude": -73.5673},
    {"city_id": 2, "city": "Quebec City", "province": "QC", "latitude": 46.8139, "longitude": -71.2080},
    {"city_id": 3, "city": "Toronto", "province": "ON", "latitude": 43.6532, "longitude": -79.3832},
]

HOURLY_VARIABLES = [
    "temperature_2m",
    "relative_humidity_2m",
    "precipitation",
    "surface_pressure",
    "wind_speed_10m",
    "weather_code",
]

TIMEZONE = "auto"
FORECAST_DAYS = 3
BASE_URL = "https://api.open-meteo.com/v1/forecast"