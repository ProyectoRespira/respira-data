{{ config(
  materialized='incremental',
  unique_key=['station_id','date_localtime'],
  incremental_strategy='merge'
) }}

with base as (
  select
    m.gold_station_id as station_id,
    a.date_localtime,
    a.pm_calibrated,
    a.pm1,
    a.pm2_5,
    a.pm10,
    a.aqi_pm2_5,
    a.aqi_pm10,
    a.aqi_level
  from {{ ref('int_station_hourly_aqi') }} a
  join {{ ref('int_station_id_map') }} m
    on m.core_station_id = a.station_id

  {% if is_incremental() %}
  where a.date_localtime >= (
    select coalesce(max(date_localtime), '1970-01-01'::timestamptz) from {{ this }}
  ) - interval '2 days'
  {% endif %}
),

stats as (
  select
    base.*,

    avg(pm2_5) over w6 as pm2_5_avg_6h,
    max(pm2_5) over w6 as pm2_5_max_6h,
    stddev_pop(pm2_5) over w6 as pm2_5_std_6h,
    (
      avg(power(pm2_5, 3)) over w6
      - 3 * avg(pm2_5) over w6 * avg(power(pm2_5, 2)) over w6
      + 2 * power(avg(pm2_5) over w6, 3)
    ) / nullif(power(stddev_pop(pm2_5) over w6, 3), 0) as pm2_5_skew_6h,

    max(aqi_pm2_5) over w24 as aqi_pm2_5_max_24h,
    stddev_pop(aqi_pm2_5) over w24 as aqi_pm2_5_std_24h,
    (
      avg(power(aqi_pm2_5, 3)) over w24
      - 3 * avg(aqi_pm2_5) over w24 * avg(power(aqi_pm2_5, 2)) over w24
      + 2 * power(avg(aqi_pm2_5) over w24, 3)
    ) / nullif(power(stddev_pop(aqi_pm2_5) over w24, 3), 0) as aqi_pm2_5_skew_24h

  from base
  window
    w6 as (
      partition by station_id
      order by date_localtime
      range between interval '5 hours' preceding and current row
    ),
    w24 as (
      partition by station_id
      order by date_localtime
      range between interval '23 hours' preceding and current row
    )
)

select
  {{ surrogate_key_bigint(["station_id", "date_localtime"]) }} as id,
  station_id,
  null::bigint as airnow_id,
  date_localtime,
  pm_calibrated,
  pm1,
  pm2_5,
  pm10,
  pm2_5_avg_6h,
  pm2_5_max_6h,
  pm2_5_skew_6h,
  pm2_5_std_6h,
  aqi_pm2_5,
  aqi_pm10,
  aqi_level,
  aqi_pm2_5_max_24h,
  aqi_pm2_5_skew_24h,
  aqi_pm2_5_std_24h
from stats
