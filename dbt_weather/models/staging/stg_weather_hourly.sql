with source as (

    select *
    from {{ source('raw', 'raw_weather') }}

),

renamed as (

    select
        cast(city_id as integer) as city_id,
        cast(city as varchar) as city,
        cast(province as varchar) as province,
        cast(latitude as double) as latitude,
        cast(longitude as double) as longitude,
        cast(time as timestamp) as observed_at,
        cast(date as date) as observed_date,
        cast(temperature_2m as double) as temperature_c,
        cast(relative_humidity_2m as integer) as relative_humidity_pct,
        cast(precipitation as double) as precipitation_mm,
        cast(surface_pressure as double) as surface_pressure_hpa,
        cast(wind_speed_10m as double) as wind_speed_kmh,
        cast(weather_code as integer) as weather_code,
        cast(ingested_at_utc as timestamp) as ingested_at_utc,
        city || '|' || cast(time as varchar) as weather_key
    from source

),

deduplicated as (

    select *
    from renamed
    qualify row_number() over (
        partition by weather_key
        order by ingested_at_utc desc
    ) = 1

)

select *
from deduplicated