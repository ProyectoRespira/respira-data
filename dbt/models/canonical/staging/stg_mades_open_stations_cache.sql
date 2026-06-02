{% set stations_relation = source('airbyte', 'MADES_Open_stations') %}
{% set stations_meta_col = first_existing_column(stations_relation, ['_airbyte_metadata', '_airbyte_meta']) %}

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

with src as (

  select *
  from {{ stations_relation }}

),

typed as (

  select
    _airbyte_raw_id,
    _airbyte_extracted_at as extracted_at,
    {% if stations_meta_col %}
    {{ stations_meta_col }} as _airbyte_metadata,
    {% else %}
    null::jsonb as _airbyte_metadata,
    {% endif %}
    _airbyte_generation_id,

    'mades_open_airbyte' as data_source_name,

    id::text as source_station_id,
    nullif(station_code::text, '') as source_station_code,

    (
      'mades_open_' ||
      coalesce(
        nullif(
          regexp_replace(lower(station_code::text), '[^a-z0-9]+', '_', 'g'),
          ''
        ),
        nullif(
          regexp_replace(lower(id::text), '[^a-z0-9]+', '_', 'g'),
          ''
        )
      )
    ) as station_code,

    nullif(name::text, '') as station_name,
    nullif(city::text, '') as station_city,
    nullif(type::text, '') as station_type,

    nullif(latitude::text, '')::double precision as latitude,
    nullif(longitude::text, '')::double precision as longitude,

    coalesce(is_active::boolean, false) as is_active,
    coalesce(is_collecting_data::boolean, false) as is_collecting_data,
    coalesce(is_under_maintenance::boolean, false) as is_under_maintenance,

    to_jsonb(src) as raw_payload

  from src

),

deduped as (

  select *
  from (
    select
      *,
      row_number() over (
        partition by source_station_id
        order by extracted_at desc, _airbyte_raw_id desc
      ) as rn
    from typed
  ) ranked
  where rn = 1

),

current_source_rows as (

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
  from deduped

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
