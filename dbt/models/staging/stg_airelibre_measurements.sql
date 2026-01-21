with src as (

  select *
  from {{ source('airbyte', 'Airelibre_measurements') }}

),

typed as (

  select
    _airbyte_raw_id,
    _airbyte_extracted_at,
    _airbyte_meta,
    _airbyte_generation_id,

    source::text as source_id,
    ('airelibre_' || source::text) as station_code,

    nullif(recorded::text, '')::timestamptz as measured_at,

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
