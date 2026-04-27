{{ config(materialized='table') }}

with stations as (
  select * from {{ ref('int_weather_stations') }}
),

station_map as (
  select * from {{ ref('int_weather_station_regions') }}
),

regions as (
  select id, region_code from {{ ref('regions') }}
)

select
  s.id,
  s.name,
  s.latitude,
  s.longitude,
  r.id as region_id
from stations s
left join station_map m
  on m.station_code = s.code
left join regions r
  on r.region_code = m.region_code
