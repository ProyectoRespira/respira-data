{{ config(materialized='view') }}

with project_streams as (
  select distinct station_id
  from {{ ref('int_project_streams') }}
),

status_overrides as (
  select station_code
  from {{ ref('int_station_status_overrides') }}
),

project_stations as (
  select distinct
    st.*
  from {{ ref('dim_stations') }} st
  join project_streams ps
    on ps.station_id = st.id
)

select
  st.id,
  st.code,
  st.name,
  st.description,
  st.latitude,
  st.longitude,
  st.elevation_m,
  case
    when so.station_code is not null then 'inactive'
    else st.status
  end as status,
  st.properties,
  st.is_pattern_station,
  st.created_at,
  st.updated_at
from project_stations st
left join status_overrides so
  on so.station_code = st.code
