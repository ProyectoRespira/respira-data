with src as (

  select *
  from {{ source('airbyte', 'measurements') }}

),

typed as (

  select
    _airbyte_raw_id,
    _airbyte_extracted_at as extracted_at,
    _airbyte_meta,
    _airbyte_generation_id,

    'airelibre_airbyte' as data_source_name,

    ('airelibre_' || source::text) as station_code,

    null::bigint as cursor_id,

    recorded::text as measured_at_raw,
    nullif(recorded::text, '')::timestamptz as measured_at_parsed,

    (
      nullif(recorded::text, '')::timestamptz is not null
      and nullif(recorded::text, '')::timestamptz >= '2018-01-01'::timestamptz
    ) as is_measured_at_valid,

    /* keep original source for audit/debug, but not as cursor */
    source::text as source_id,

    pm1dot0::numeric as pm1,
    pm2dot5::numeric as pm25,
    pm10::numeric as pm10,

    latitude::double precision as latitude,
    longitude::double precision as longitude,

    description::text as description,
    sensor::text as sensor_model,
    version::text as firmware_version,

    to_jsonb(src) as raw_payload

  from src

)

select * from typed
