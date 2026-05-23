{{ config(
  materialized='incremental',
  unique_key='source_station_id',
  incremental_strategy='merge',
  on_schema_change='sync_all_columns',
  full_refresh=false,
  indexes=[
    {'columns': ['source_station_id'], 'unique': true},
    {'columns': ['station_code']},
    {'columns': ['extracted_at']}
  ]
) }}

with current_source_rows as (

  select *
  from {{ ref('stg_mades_open_stations') }}

)

select
  _airbyte_raw_id,
  extracted_at,
  _airbyte_metadata,
  _airbyte_generation_id,
  data_source_name,
  source_station_id,
  source_station_code,
  station_code,
  station_name,
  station_city,
  station_type,
  latitude,
  longitude,
  is_active,
  is_collecting_data,
  is_under_maintenance,
  raw_payload
from current_source_rows
