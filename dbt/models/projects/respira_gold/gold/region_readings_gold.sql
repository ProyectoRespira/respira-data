{{ config(
  materialized='incremental',
  unique_key=['region_id','date_utc'],
  incremental_strategy='merge'
) }}

{%- set region_station_freshness_hours = var('region_station_freshness_hours', 6) -%}

with station_regions as (
  select
    id as station_id,
    region_id
  from {{ ref('stations') }}
  where region_id is not null
    and is_station_on
),

target_hours as (
  select distinct
    sr_map.region_id,
    sr.date_localtime as date_utc
  from {{ ref('station_readings_gold') }} sr
  join station_regions sr_map
    on sr_map.station_id = sr.station_id

  {% if is_incremental() %}
  where sr.date_localtime >= (
    select coalesce(max(date_utc), '1970-01-01'::timestamptz) from {{ this }}
  ) - interval '2 days'
  {% endif %}
),

base as (
  -- For each region-hour, carry forward each station's latest reading within a bounded freshness window.
  select
    th.region_id,
    th.date_utc,
    latest.pm2_5,
    latest.aqi_pm2_5,
    latest.aqi_level
  from target_hours th
  join station_regions sr_map
    on sr_map.region_id = th.region_id
  join lateral (
    select
      sr.pm2_5,
      sr.aqi_pm2_5,
      sr.aqi_level
    from {{ ref('station_readings_gold') }} sr
    where sr.station_id = sr_map.station_id
      and sr.date_localtime <= th.date_utc
      and sr.date_localtime >= th.date_utc - interval '{{ region_station_freshness_hours }} hours'
    order by sr.date_localtime desc
    limit 1
  ) latest
    on true
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
  coalesce(
    (pm2_5_m3 - 3 * pm2_5_region_avg * pm2_5_m2 + 2 * power(pm2_5_region_avg, 3))
      / nullif(power(pm2_5_region_std, 3), 0),
    0.0
  ) as pm2_5_region_skew,
  pm2_5_region_std,
  aqi_region_avg,
  aqi_region_max,
  coalesce(
    (aqi_m3 - 3 * aqi_region_avg * aqi_m2 + 2 * power(aqi_region_avg, 3))
      / nullif(power(aqi_region_std, 3), 0),
    0.0
  ) as aqi_region_skew,
  aqi_region_std,
  level_region_max
from agg
