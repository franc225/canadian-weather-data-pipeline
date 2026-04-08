select distinct
    city_id,
    city,
    province,
    latitude,
    longitude
from {{ ref('stg_weather_hourly') }}