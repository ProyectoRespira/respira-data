{{ config(
  materialized='incremental',
  unique_key=['stream_id','timestamp'],
  incremental_strategy='merge',
  indexes=[
    {'columns': ['stream_id', 'timestamp'], 'unique': true},
    {'columns': ['timestamp']},
    {'columns': ['ingested_at']},
    {'columns': ['source_row_id']}
  ]
) }}

with streams as (
  select
    id as stream_id,
    code,
    data_source_id
  from {{ ref('dim_streams') }}
),

data_sources as (
  select
    id as data_source_id,
    name as data_source_name
  from {{ ref('dim_data_sources') }}
),

existing_source_cutoffs as (
  {% if is_incremental() %}
  select
    ds.data_source_name,
    max(f.ingested_at) as max_ingested_at
  from {{ this }} f
  join streams s
    on s.stream_id = f.stream_id
  join data_sources ds
    on ds.data_source_id = s.data_source_id
  group by ds.data_source_name
  {% else %}
  select
    null::text as data_source_name,
    null::timestamptz as max_ingested_at
  where false
  {% endif %}
),

m as (
  select
    msrc.*
  from {{ ref('int_measurements_values_silver') }} msrc
  left join existing_source_cutoffs c
    on c.data_source_name = msrc.data_source_name
  where msrc.measured_at_silver is not null
    and msrc.value_silver is not null
  {% if measurement_has_process_batch_scope() %}
    and {{ measurement_process_row_predicate('msrc.data_source_name', 'msrc.measured_at_silver') }}
  {% elif is_incremental() %}
    and msrc.extracted_at >= coalesce(c.max_ingested_at, '1970-01-01'::timestamptz)
  {% endif %}
),

joined as (
  select
    m.source_row_id,
    s.stream_id,
    m.measured_at_silver as timestamp,
    m.value_silver as value_parsed,
    m.extracted_at as ingested_at
  from m
  join streams s
    on s.code = (m.station_code || '_' || m.variable_code || '_' || m.data_source_name)
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
  source_row_id,
  stream_id,
  timestamp,
  value_parsed,
  ingested_at
from deduped
