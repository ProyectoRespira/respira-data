with src as (

  select *
  from {{ source('airbyte', 'meteostat_asu_airport') }}

),

typed as (

  select
    _airbyte_raw_id,
    _airbyte_extracted_at as extracted_at,
    _airbyte_meta,
    _airbyte_generation_id,

    'meteostat_airbyte' as data_source_name,

    'asu_airport' as station_code,

    null::bigint as cursor_id,

    time::text as measured_at_raw,
    (time::timestamp at time zone 'UTC')::timestamptz as measured_at_parsed,

    (
      (time::timestamp at time zone 'UTC')::timestamptz >= '2018-01-01'::timestamptz
    ) as is_measured_at_valid,

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
