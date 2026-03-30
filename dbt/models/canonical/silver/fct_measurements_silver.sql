{{ config(
  materialized='incremental',
  unique_key=['stream_id','timestamp'],
  incremental_strategy='merge'
) }}

with m as (
  select *
  from {{ ref('int_measurements_values_silver') }}
  where measured_at_silver is not null
    and value_silver is not null
),

streams as (
  select id as stream_id, code
  from {{ ref('dim_streams') }}
),

joined as (
  select
    s.stream_id,
    m.measured_at_silver as timestamp,
    m.value_silver as value_parsed,

    -- prefer this if available in m:
    -- m._airbyte_extracted_at as ingested_at
    now()::timestamptz as ingested_at,

    m.raw_payload
  from m
  join streams s
    on s.code = (m.station_code || '_' || m.variable_code || '_' || m.data_source_name)

  {% if is_incremental() %}
  where m.measured_at_silver >= (
    select coalesce(max(timestamp), '1970-01-01'::timestamptz) from {{ this }}
  ) - interval '2 days'
  {% endif %}
),

deduped as (
  select *
  from (
    select
      *,
      row_number() over (
        partition by stream_id, timestamp
        order by ingested_at desc
      ) as rn
    from joined
  ) x
  where rn = 1
)

select
  stream_id,
  timestamp,
  value_parsed,
  ingested_at,
  raw_payload
from deduped
