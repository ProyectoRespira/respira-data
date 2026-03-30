{{ config(materialized='view') }}

with project_streams as (
  select distinct station_id
  from {{ ref('int_project_streams') }}
)

select distinct
  st.*
from {{ ref('dim_stations') }} st
join project_streams ps
  on ps.station_id = st.id
