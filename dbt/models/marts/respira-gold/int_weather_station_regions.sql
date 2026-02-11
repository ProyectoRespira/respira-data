{{ config(materialized='view') }}

with seed as (
  select station_code::text as station_code, region_code::text as region_code
  from {{ ref('station_region_seed') }}
),

weather as (
  select code, latitude, longitude
  from {{ ref('int_weather_stations') }}
),

airq as (
  select code, latitude, longitude
  from {{ ref('int_air_quality_stations') }}
)

select
  w.code as station_code,
  coalesce(seed.region_code, '{{ var('default_region_code', 'GRAN_ASUNCION') }}') as region_code
from weather w
left join seed
  on seed.station_code = w.code
left join lateral (
  select a.code as region_code
  from airq a
  order by ((a.latitude - w.latitude) * (a.latitude - w.latitude))
         + ((a.longitude - w.longitude) * (a.longitude - w.longitude))
  limit 1
) nearest on true
