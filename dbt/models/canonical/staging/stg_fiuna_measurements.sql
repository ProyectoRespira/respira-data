with src as (

    select 'FIUNA_Estacion1'  as _source_table, * from {{ source('airbyte', 'FIUNA_Estacion1') }}
    union all
    select 'FIUNA_Estacion2'  as _source_table, * from {{ source('airbyte', 'FIUNA_Estacion2') }}
    union all
    select 'FIUNA_Estacion3'  as _source_table, * from {{ source('airbyte', 'FIUNA_Estacion3') }}
    union all
    select 'FIUNA_Estacion4'  as _source_table, * from {{ source('airbyte', 'FIUNA_Estacion4') }}
    union all
    select 'FIUNA_Estacion5'  as _source_table, * from {{ source('airbyte', 'FIUNA_Estacion5') }}
    union all
    select 'FIUNA_Estacion6'  as _source_table, * from {{ source('airbyte', 'FIUNA_Estacion6') }}
    union all
    select 'FIUNA_Estacion7'  as _source_table, * from {{ source('airbyte', 'FIUNA_Estacion7') }}
    union all
    select 'FIUNA_Estacion8'  as _source_table, * from {{ source('airbyte', 'FIUNA_Estacion8') }}
    union all
    select 'FIUNA_Estacion9'  as _source_table, * from {{ source('airbyte', 'FIUNA_Estacion9') }}
    union all
    select 'FIUNA_Estacion10' as _source_table, * from {{ source('airbyte', 'FIUNA_Estacion10') }}

),

typed as (

  select
    src._airbyte_raw_id,
    src._airbyte_extracted_at as extracted_at,
    src._airbyte_meta,
    src._airbyte_generation_id,
    src._source_table,

    'fiuna_airbyte' as data_source_name,

    src."ID"::bigint as cursor_id,

    /* Preserve raw date/time for auditability */
    src."FECHA"::text as fecha_raw,
    src."HORA"::text as hora_raw,
    (src."FECHA"::text || ' ' || src."HORA"::text) as measured_at_raw,

    /* FIUNA_Estacion7 -> fiuna_7 */
    ('fiuna_' || regexp_replace(lower(src._source_table), '^fiuna_estacion', '')) as station_code,

    {{ parse_fiuna_timestamp('FECHA', 'HORA') }} as measured_at_parsed,

    (
      {{ parse_fiuna_timestamp('FECHA', 'HORA') }} is not null
      and {{ parse_fiuna_timestamp('FECHA', 'HORA') }} >= '2018-01-01'::timestamptz
    ) as is_measured_at_valid,

    src."MP1"::numeric   as pm1,
    src."MP2_5"::numeric as pm25,
    src."MP10"::numeric  as pm10,

    src."BATERIA"::numeric     as battery,
    src."HUMEDAD"::numeric     as hum,
    src."PRESION"::numeric     as pres,
    src."TEMPERATURA"::numeric as temperature_c,

    to_jsonb(src) as raw_payload

  from src

)

select * from typed
