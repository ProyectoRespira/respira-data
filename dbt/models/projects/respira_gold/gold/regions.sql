{{ config(materialized='table') }}

with regions as (
  select * from {{ ref('int_regions') }}
),

station_map as (
  select * from {{ ref('int_station_regions') }}
),

weather_map as (
  select * from {{ ref('int_weather_station_regions') }}
),

station_flags as (
  select
    m.region_code,
    bool_or(s.is_pattern_station) as has_pattern_data
  from station_map m
  join {{ ref('int_air_quality_stations') }} s
    on s.code = m.station_code
  group by 1
),

weather_flags as (
  select
    m.region_code,
    count(*) > 0 as has_weather_data
  from weather_map m
  group by 1
)

select
  {{ surrogate_key_bigint(["r.region_code"]) }} as id,
  r.name,
  r.region_code,
  r.bbox,
  coalesce(w.has_weather_data, false) as has_weather_data,
  coalesce(p.has_pattern_data, false) as has_pattern_data
from regions r
left join weather_flags w
  on w.region_code = r.region_code
left join station_flags p
  on p.region_code = r.region_code
