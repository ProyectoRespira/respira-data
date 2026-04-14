{{ config(
  materialized='incremental',
  unique_key='code',
  incremental_strategy='merge',
  indexes=[
    {'columns': ['code'], 'unique': true},
    {'columns': ['extracted_at']}
  ]
) }}

with source_rows as (

  select
    m.extracted_at,
    m.station_code,
    m.variable_code,
    m.data_source_name
  from {{ ref('int_measurements_long') }} m

  {% if is_incremental() %}
  where m.extracted_at >= (
    select coalesce(max(extracted_at), '1970-01-01'::timestamptz)
    from {{ this }}
  )
  {% endif %}

),

grouped as (

  select
    max(extracted_at) as extracted_at,
    station_code,
    variable_code,
    data_source_name
  from source_rows
  group by 2, 3, 4

)

select
  extracted_at,
  station_code,
  variable_code,
  data_source_name,
  (station_code || '_' || variable_code || '_' || data_source_name) as code,
  (variable_code || ' at ' || station_code) as name
from grouped
