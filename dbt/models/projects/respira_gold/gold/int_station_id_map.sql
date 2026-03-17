{{ config(materialized='view') }}

with stations as (
  select
    id as core_station_id,
    code as station_code
  from {{ ref('int_air_quality_stations') }}
)

select
  core_station_id,
  station_code,
  row_number() over (
    order by station_code, core_station_id
  )::bigint as project_station_id
from stations
