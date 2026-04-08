select distinct
    observed_date as date_key,
    year(observed_date) as year_number,
    month(observed_date) as month_number,
    day(observed_date) as day_number
from {{ ref('stg_weather_hourly') }}