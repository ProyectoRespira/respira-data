{#
  Respira and Vigilancia del Aire (VA) sensors both come from AirGradient and
  share one logical source: same data_source_name, station_code namespace and
  incremental checkpoint. union_relations aligns columns by name, not position.

  Because the checkpoint is shared, the VA Airbyte sync must not overlap the
  Respira one: rows extracted earlier but committed after a newer Respira
  batch would fall behind max(extracted_at) and never be picked up.
#}
{% set respira_relation = source('airbyte', 'Respira_measures_past') %}
{% set va_relation = source('airbyte', 'va_respira_measures_past') %}
{% set relations = [respira_relation, va_relation] %}

with src as (

  {{ dbt_utils.union_relations(
    relations=relations,
    source_column_name=none
  ) }}

),

typed as (

  select
    _airbyte_raw_id,
    _airbyte_extracted_at as extracted_at,
    {{ coalesce_existing_columns(relations, ['_airbyte_meta', '_airbyte_metadata'], 'null::jsonb') }} as _airbyte_meta,
    _airbyte_generation_id,

    'respira_airbyte' as data_source_name,

    "locationId"::text as source_station_id,
    'respira_' || "locationId"::text as station_code,
    nullif(
      regexp_replace(trim("locationName"::text), '^Respira:\\s*', '', 'i'),
      ''
    ) as station_name,

    null::bigint as cursor_id,

    "timestamp"::text as measured_at_raw,
    nullif("timestamp"::text, '')::timestamptz as measured_at_parsed,

    (
      nullif("timestamp"::text, '')::timestamptz is not null
      and nullif("timestamp"::text, '')::timestamptz >= '2018-01-01'::timestamptz
    ) as is_measured_at_valid,

    coalesce(
      nullif(pm01_corrected::text, '')::numeric,
      nullif(pm01::text, '')::numeric
    ) as pm1,

    coalesce(
      nullif(pm02_corrected::text, '')::numeric,
      nullif(pm02::text, '')::numeric
    ) as pm25,

    coalesce(
      nullif(pm10_corrected::text, '')::numeric,
      nullif(pm10::text, '')::numeric
    ) as pm10,

    coalesce(
      nullif(atmp_corrected::text, '')::numeric,
      nullif(atmp::text, '')::numeric
    ) as temperature_c,

    coalesce(
      nullif(rhum_corrected::text, '')::numeric,
      nullif(rhum::text, '')::numeric
    ) as hum,

    nullif(model::text, '') as sensor_model,
    nullif(serialno::text, '') as serial_number,
    nullif("firmwareVersion"::text, '') as firmware_version,

    to_jsonb(src) as raw_payload

  from src

)

select * from typed
