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

expected as (
  select
    (m.station_code || '_' || m.variable_code || '_' || m.data_source_name) as stream_code,
    m.measured_at_silver as timestamp,
    m.value_silver as value_parsed,
    m.extracted_at as ingested_at
  from {{ ref('int_measurements_values_silver') }} m
  where m.measured_at_silver is not null
    and m.value_silver is not null
    and {{ measurement_source_column_predicate('m.data_source_name') }}
),

actual as (
  select
    s.code as stream_code,
    f.timestamp,
    f.value_parsed,
    f.ingested_at
  from {{ ref('fct_measurements_silver') }} f
  join streams s
    on s.stream_id = f.stream_id
  join data_sources ds
    on ds.data_source_id = s.data_source_id
  where {{ measurement_source_column_predicate('ds.data_source_name') }}
),

missing as (
  select
    e.stream_code,
    e.timestamp,
    e.value_parsed,
    e.ingested_at
  from expected e
  left join actual a
    on a.stream_code = e.stream_code
   and a.timestamp = e.timestamp
   and a.value_parsed = e.value_parsed
   and a.ingested_at = e.ingested_at
  where a.stream_code is null
)

select * from missing
