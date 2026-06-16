{% set stations_relation = source('airbyte', 'Respira_measures_current_list') %}
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

    'respira_airbyte' as data_source_name,

    "locationId"::text as source_station_id,
    ('respira_' || "locationId"::text) as station_code,

    nullif(
      regexp_replace(trim("locationName"::text), '^Respira:\\s*', '', 'i'),
      ''
    ) as station_name,

    nullif(trim("locationType"::text), '') as location_type,
    nullif(latitude::text, '')::double precision as latitude,
    nullif(longitude::text, '')::double precision as longitude,

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

)

select
  _airbyte_raw_id,
  extracted_at,
  _airbyte_metadata,
  _airbyte_generation_id,
  data_source_name,
  source_station_id,
  station_code,
  station_name,
  location_type,
  latitude,
  longitude,
  raw_payload
from deduped
