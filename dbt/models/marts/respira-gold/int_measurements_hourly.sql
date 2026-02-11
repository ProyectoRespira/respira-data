{{ config(materialized='view') }}

with base as (
  select
    m.timestamp,
    date_trunc('hour', m.timestamp) as date_hour,
    m.value_parsed,
    s.station_id,
    st.code as station_code,
    dv.code as variable_code,
    ds.name as data_source_name
  from {{ ref('fct_measurements_silver') }} m
  join {{ ref('dim_streams') }} s
    on s.id = m.stream_id
  join {{ ref('dim_stations') }} st
    on st.id = s.station_id
  join {{ ref('dim_variables') }} dv
    on dv.id = s.variable_id
  join {{ ref('dim_data_sources') }} ds
    on ds.id = s.data_source_id
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
