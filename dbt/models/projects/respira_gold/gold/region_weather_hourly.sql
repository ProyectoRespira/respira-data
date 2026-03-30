{{ config(materialized='view') }}

with weather as (
  select w.*
  from {{ ref('int_station_hourly_wide') }} w
  join {{ ref('int_weather_stations') }} s
    on s.code = w.station_code
),

station_map as (
  select * from {{ ref('int_weather_station_regions') }}
),

regions as (
  select id, region_code from {{ ref('regions') }}
)

select
  r.id as region_id,
  w.date_localtime,
  avg(w.temperature_c) as temperature_c,
  avg(w.humidity) as humidity,
  avg(w.pressure) as pressure,
  avg(w.wind_speed) as wind_speed,
  avg(sin(radians(w.wind_dir))) as wind_dir_sin,
  avg(cos(radians(w.wind_dir))) as wind_dir_cos
from weather w
join station_map m
  on m.station_code = w.station_code
join regions r
  on r.region_code = m.region_code
group by 1,2
