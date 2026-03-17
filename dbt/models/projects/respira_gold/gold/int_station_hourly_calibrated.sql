{{ config(materialized='view') }}

with base as (
  select * from {{ ref('int_station_hourly_wide') }}
),

factors as (
  select * from {{ ref('int_calibration_factors') }}
)

select
  b.station_id,
  b.station_code,
  b.date_localtime,
  (f.calibration_factor is not null) as pm_calibrated,
  case when f.calibration_factor is not null then b.pm1 * f.calibration_factor else b.pm1 end as pm1,
  case when f.calibration_factor is not null then b.pm2_5 * f.calibration_factor else b.pm2_5 end as pm2_5,
  case when f.calibration_factor is not null then b.pm10 * f.calibration_factor else b.pm10 end as pm10,
  b.temperature_c,
  b.humidity,
  b.pressure,
  b.wind_speed,
  b.wind_dir
from base b
left join lateral (
  select calibration_factor
  from factors f
  where f.station_id = b.station_id
    and b.date_localtime >= f.date_start_use
    and (f.date_end_use is null or b.date_localtime < f.date_end_use)
  order by f.date_start_use desc
  limit 1
) f on true
