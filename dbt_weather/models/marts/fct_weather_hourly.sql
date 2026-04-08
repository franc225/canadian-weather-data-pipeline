select
    weather_key,
    city_id,
    observed_date as date_key,
    observed_at,
    temperature_c,
    relative_humidity_pct,
    precipitation_mm,
    surface_pressure_hpa,
    wind_speed_kmh,
    weather_code,
    ingested_at_utc
from {{ ref('stg_weather_hourly') }}