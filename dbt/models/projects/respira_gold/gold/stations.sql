{{ config(materialized='table') }}

with stations as (
  select * from {{ ref('int_air_quality_stations') }}
),

station_id_map as (
  select * from {{ ref('int_station_id_map') }}
),

station_map as (
  select * from {{ ref('int_station_regions') }}
),

regions as (
  select id, region_code from {{ ref('regions') }}
)

select
  sm.project_station_id as id,
  case
    when lower(coalesce(s.properties->>'source', '')) = 'fiuna' then 'FIUNA: ' || s.name
    when lower(coalesce(s.properties->>'source', '')) = 'airelibre' then 'AireLibre: ' || s.name
    else s.name
  end as name,
  s.latitude,
  s.longitude,
  r.id as region_id,
  (s.status = 'active') as is_station_on,
  coalesce(s.is_pattern_station, false) as is_pattern_station
from stations s
join station_id_map sm
  on sm.core_station_id = s.id
left join station_map m
  on m.station_code = s.code
left join regions r
  on r.region_code = m.region_code
