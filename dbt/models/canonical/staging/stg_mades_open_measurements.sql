{% set mades_pm10_relation = source('airbyte', 'MADES_Open_pm10_data') %}
{% set mades_pm25_relation = source('airbyte', 'MADES_Open_pm25_data') %}
{% set measurements_meta_col = first_existing_column(mades_pm10_relation, ['_airbyte_metadata', '_airbyte_meta']) %}

with src as (

  select 'MADES_Open_pm10_data' as _source_table, * from {{ mades_pm10_relation }}
  union all
  select 'MADES_Open_pm25_data' as _source_table, * from {{ mades_pm25_relation }}

),

stations as (
  select *
  from {{ ref('stg_mades_open_stations_cache') }}
),

typed as (

  select
    (_source_table || ':' || src._airbyte_raw_id::text) as source_row_id,
    src._airbyte_raw_id,
    src._airbyte_extracted_at as extracted_at,
    {% if measurements_meta_col %}
    src.{{ measurements_meta_col }} as _airbyte_metadata,
    {% else %}
    null::jsonb as _airbyte_metadata,
    {% endif %}
    src._airbyte_generation_id,
    src._source_table,

    'mades_open_airbyte' as data_source_name,

    src.station_id::text as source_station_id,
    stations.source_station_code,
    coalesce(
      stations.station_code,
      'mades_open_' || nullif(
        regexp_replace(lower(src.station_id::text), '[^a-z0-9]+', '_', 'g'),
        ''
      )
    ) as station_code,

    src.date_time::text as measured_at_raw,
    nullif(src.date_time::text, '')::timestamptz as measured_at_parsed,

    (
      nullif(src.date_time::text, '')::timestamptz is not null
      and nullif(src.date_time::text, '')::timestamptz >= '2018-01-01'::timestamptz
    ) as is_measured_at_valid,

    src.parameter_name::text as parameter_name_raw,

    case
      when src._source_table = 'MADES_Open_pm25_data' then nullif(src.parameter_value::text, '')::numeric
      else null::numeric
    end as pm25,

    case
      when src._source_table = 'MADES_Open_pm10_data' then nullif(src.parameter_value::text, '')::numeric
      else null::numeric
    end as pm10,

    stations.station_name,
    stations.station_city,
    stations.station_type,
    stations.latitude,
    stations.longitude,
    stations.is_active,
    stations.is_collecting_data,
    stations.is_under_maintenance,

    to_jsonb(src) as raw_payload

  from src
  left join stations
    on stations.source_station_id = src.station_id::text

)

select * from typed
