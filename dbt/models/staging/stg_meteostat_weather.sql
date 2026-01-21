with src as (

  select *
  from {{ source('airbyte', 'asu_airportasu_airport') }}

),

typed as (

  select
    _airbyte_raw_id,
    _airbyte_extracted_at,
    _airbyte_meta,
    _airbyte_generation_id,

    'asu_airport' as station_code,
    (time::timestamp at time zone 'UTC')::timestamptz as measured_at,

    temp::numeric as temperature_c,
    rhum::numeric as hum,
    pres::numeric as pres,
    prcp::numeric as prcp,
    wspd::numeric as wspd,
    wdir::numeric as wdir,
    dwpt::numeric as dwpt,
    coco::numeric as coco,

    to_jsonb(src) as raw_payload

  from src

)

select * from typed
