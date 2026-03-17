{{ config(materialized='view') }}

with seed as (
  select station_code::text as station_code, region_code::text as region_code
  from {{ ref('station_region_seed') }}
),

stations as (
  select code as station_code
  from {{ ref('int_air_quality_stations') }}
)

select
  s.station_code,
  coalesce(seed.region_code, '{{ var('default_region_code', 'GRAN_ASUNCION') }}') as region_code
from stations s
left join seed
  on seed.station_code = s.station_code
