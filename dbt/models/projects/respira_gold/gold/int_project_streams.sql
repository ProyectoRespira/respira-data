{{ config(materialized='view') }}

with project_data_sources as (
  select data_source_id
  from {{ ref('int_project_data_sources') }}
)

select distinct
  s.id,
  s.station_id,
  s.variable_id,
  s.data_source_id,
  s.code,
  s.name
from {{ ref('dim_streams') }} s
join project_data_sources pds
  on pds.data_source_id = s.data_source_id
