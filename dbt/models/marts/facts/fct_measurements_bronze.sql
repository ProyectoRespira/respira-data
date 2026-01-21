{{ config(
  materialized='incremental',
  unique_key=['stream_id','timestamp'],
  incremental_strategy='merge'
) }}

with m as (
  select * from {{ ref('int_measurements_long') }}
),

streams as (
  select id as stream_id, code
  from {{ ref('dim_streams') }}
),

joined as (
  select
    s.stream_id,
    m.measured_at as timestamp,
    m.value_raw,
    m.value_parsed,
    now()::timestamptz as ingested_at,
    m.raw_payload
  from m
  join streams s
    on s.code = (m.station_code || '_' || m.variable_code || '_' || m.data_source_name)

  {% if is_incremental() %}
  where m.measured_at >= (
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
  value_raw,
  value_parsed,
  ingested_at,
  raw_payload
from deduped
