{{ config(
  materialized='incremental',
  unique_key=['region_id','date_utc'],
  incremental_strategy='merge'
) }}

with base as (
  select
    s.region_id,
    r.date_localtime as date_utc,
    r.pm2_5,
    r.aqi_pm2_5,
    r.aqi_level
  from {{ ref('station_readings_gold') }} r
  join {{ ref('stations') }} s
    on s.id = r.station_id

  {% if is_incremental() %}
  where r.date_localtime >= (
    select coalesce(max(date_utc), '1970-01-01'::timestamptz) from {{ this }}
  ) - interval '2 days'
  {% endif %}
),

agg as (
  select
    region_id,
    date_utc,
    avg(pm2_5) as pm2_5_region_avg,
    max(pm2_5) as pm2_5_region_max,
    stddev_pop(pm2_5) as pm2_5_region_std,
    avg(power(pm2_5, 2)) as pm2_5_m2,
    avg(power(pm2_5, 3)) as pm2_5_m3,

    avg(aqi_pm2_5) as aqi_region_avg,
    max(aqi_pm2_5) as aqi_region_max,
    stddev_pop(aqi_pm2_5) as aqi_region_std,
    avg(power(aqi_pm2_5, 2)) as aqi_m2,
    avg(power(aqi_pm2_5, 3)) as aqi_m3,

    max(aqi_level) as level_region_max
  from base
  group by 1,2
)

select
  {{ surrogate_key_bigint(["region_id", "date_utc"]) }} as id,
  region_id,
  date_utc,
  pm2_5_region_avg,
  pm2_5_region_max,
  (pm2_5_m3 - 3 * pm2_5_region_avg * pm2_5_m2 + 2 * power(pm2_5_region_avg, 3))
    / nullif(power(pm2_5_region_std, 3), 0) as pm2_5_region_skew,
  pm2_5_region_std,
  aqi_region_avg,
  aqi_region_max,
  (aqi_m3 - 3 * aqi_region_avg * aqi_m2 + 2 * power(aqi_region_avg, 3))
    / nullif(power(aqi_region_std, 3), 0) as aqi_region_skew,
  aqi_region_std,
  level_region_max
from agg
