{{ config(materialized='view') }}

with base as (
  select
    m.timestamp,
    date_trunc('hour', m.timestamp) as date_hour,
    m.value_parsed,
    project_streams.station_id,
    st.code as station_code,
    dv.code as variable_code,
    ds.name as data_source_name
  from {{ ref('fct_measurements_silver') }} m
  join {{ ref('int_project_streams') }} project_streams
    on project_streams.id = m.stream_id
  join {{ ref('dim_stations') }} st
    on st.id = project_streams.station_id
  join {{ ref('dim_variables') }} dv
    on dv.id = project_streams.variable_id
  join {{ ref('dim_data_sources') }} ds
    on ds.id = project_streams.data_source_id
  where dv.code in (
    'pm1',
    'pm25',
    'pm10',
    'temperature_c',
    'hum',
    'pres',
    'wspd',
    'wdir'
  )
)

select
  station_id,
  station_code,
  variable_code,
  data_source_name,
  date_hour,
  avg(value_parsed) as value_hourly
from base
group by 1,2,3,4,5
