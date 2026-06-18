{{ config(materialized='view') }}

with project_streams as (
  select distinct station_id
  from {{ ref('int_project_streams') }}
),

status_overrides as (
  select
    station_code::text as station_code,
    lower(status::text) as status
  from {{ ref('station_status_seed') }}
  where nullif(station_code::text, '') is not null
    and nullif(status::text, '') is not null
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
  coalesce(so.status, st.status) as status,
  st.properties,
  st.is_pattern_station,
  st.created_at,
  st.updated_at
from project_stations st
left join status_overrides so
  on so.station_code = st.code
