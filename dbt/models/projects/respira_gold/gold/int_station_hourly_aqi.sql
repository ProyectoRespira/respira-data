{{ config(materialized='view') }}

with base as (
  select * from {{ ref('int_station_hourly_calibrated') }}
),

rolling as (
  select
    *,
    avg(pm2_5) over w24 as pm2_5_avg_24h,
    avg(pm10) over w24 as pm10_avg_24h
  from base
  window w24 as (
    partition by station_id
    order by date_localtime
    rows between 23 preceding and current row
  )
),

aqi as (
  select
    station_id,
    station_code,
    date_localtime,
    pm_calibrated,
    pm1,
    pm2_5,
    pm10,
    temperature_c,
    humidity,
    pressure,
    wind_speed,
    wind_dir,
    -- AQI for particulate matter is derived from the rolling 24-hour concentration.
    {{ aqi_pm25('pm2_5_avg_24h') }} as aqi_pm2_5,
    {{ aqi_pm10('pm10_avg_24h') }} as aqi_pm10
  from rolling
),

final as (
  select
    *,
    {{ aqi_level('greatest(coalesce(aqi_pm2_5, -1), coalesce(aqi_pm10, -1))') }} as aqi_level
  from aqi
)

select * from final
