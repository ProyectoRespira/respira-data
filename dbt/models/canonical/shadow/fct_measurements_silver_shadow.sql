{{ config(
  materialized='incremental',
  schema='silver',
  alias='fct_measurements_silver_shadow',
  unique_key=['stream_id', 'timestamp'],
  incremental_strategy='merge',
  on_schema_change='sync_all_columns',
  indexes=[
    {'columns': ['stream_id', 'timestamp'], 'unique': true},
    {'columns': ['timestamp']},
    {'columns': ['ingested_at']},
    {'columns': ['source_row_id']}
  ]
) }}

{{ validate_measurement_shadow_scope() }}

with streams as (
  select id as stream_id, code
  from {{ ref('dim_streams') }}
),

joined as (
  select
    values.source_row_id,
    streams.stream_id,
    values.measured_at_silver as timestamp,
    values.value_silver as value_parsed,
    values.extracted_at as ingested_at
  from {{ ref('int_measurements_values_silver_shadow') }} values
  join streams
    on streams.code = (
      values.station_code || '_' || values.variable_code || '_' || values.data_source_name
    )
  where values.measured_at_silver is not null
    and values.value_silver is not null
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
  ) ranked
  where rn = 1
)

select
  source_row_id,
  stream_id,
  timestamp,
  value_parsed,
  ingested_at
from deduped
