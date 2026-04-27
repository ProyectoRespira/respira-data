{{ config(materialized='view') }}

with base as (
  select * from {{ ref('int_station_hourly_calibrated') }}
),

aqi as (
  select
    *,
    {{ aqi_pm25('pm2_5') }} as aqi_pm2_5,
    {{ aqi_pm10('pm10') }} as aqi_pm10
  from base
),

final as (
  select
    *,
    {{ aqi_level('greatest(coalesce(aqi_pm2_5, -1), coalesce(aqi_pm10, -1))') }} as aqi_level
  from aqi
)

select * from final
