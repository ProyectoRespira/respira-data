{{ config(
  materialized='incremental',
  schema='ops',
  alias='measurement_stream_state',
  unique_key=['data_source_name', 'station_code', 'variable_code'],
  incremental_strategy='merge',
  on_schema_change='fail',
  indexes=[
    {'columns': ['data_source_name', 'station_code', 'variable_code'], 'unique': true},
    {'columns': ['updated_at']},
    {'columns': ['data_source_name', 'last_measured_at_silver', 'last_extracted_at']}
  ]
) }}

-- This relation is created and initially populated by the dedicated bootstrap.
-- Refuse to recreate it through dbt because doing so would bypass bootstrap
-- coverage validation and the documented rollback procedure. A normal run
-- against a missing relation also fails because the query reads {{ this }}.
{% if flags.FULL_REFRESH %}
  {{ exceptions.raise_compiler_error(
    "ops.measurement_stream_state does not support --full-refresh; restore or bootstrap the relation instead."
  ) }}
{% endif %}

-- dbt cannot infer refs hidden behind the incremental relation guard while parsing.
-- depends_on: {{ ref('dim_streams') }}
-- depends_on: {{ ref('fct_measurements_silver') }}
-- depends_on: {{ ref('int_measurement_timestamps_silver') }}

with streams as (
  select
    streams.id as stream_id,
    data_sources.name as data_source_name,
    stations.code as station_code,
    variables.code as variable_code
  from {{ ref('dim_streams') }} streams
  join {{ ref('dim_data_sources') }} data_sources
    on data_sources.id = streams.data_source_id
  join {{ ref('dim_stations') }} stations
    on stations.id = streams.station_id
  join {{ ref('dim_variables') }} variables
    on variables.id = streams.variable_id
),

published_candidates as (
  select
    streams.data_source_name,
    streams.station_code,
    streams.variable_code,
    queue.measured_at_silver as last_measured_at_silver,
    queue.cursor_id as last_cursor_id,
    queue.extracted_at as last_extracted_at,
    queue.source_row_id as last_source_row_id,
    fact.value_parsed as last_value_silver
  from {{ ref('int_measurement_timestamps_silver') }} queue
  join streams
    on streams.data_source_name = queue.data_source_name
   and streams.station_code = queue.station_code
  join {{ ref('fct_measurements_silver') }} fact
    on fact.stream_id = streams.stream_id
   and fact.source_row_id = queue.source_row_id
   and fact.timestamp = queue.measured_at_silver
   and fact.ingested_at = queue.extracted_at
  left join {{ this }} state
    using (data_source_name, station_code, variable_code)
  where fact.value_parsed is not null
    and {{ measurement_runtime_queue_row_predicate(
      'queue.data_source_name',
      'queue.measured_at_silver',
      'queue.cleanup_eligible_at'
    ) }}
    and (
      state.data_source_name is null
      or (
        queue.measured_at_silver,
        coalesce(queue.cursor_id, -1),
        queue.extracted_at,
        queue.source_row_id
      ) > (
        state.last_measured_at_silver,
        coalesce(state.last_cursor_id, -1),
        state.last_extracted_at,
        state.last_source_row_id
      )
    )
),

latest_candidates as (
  select *
  from (
    select
      candidates.*,
      row_number() over (
        partition by data_source_name, station_code, variable_code
        order by
          last_measured_at_silver desc,
          coalesce(last_cursor_id, -1) desc,
          last_extracted_at desc,
          last_source_row_id desc
      ) as rn
    from published_candidates candidates
  ) ranked
  where rn = 1
)

select
  data_source_name,
  station_code,
  variable_code,
  last_measured_at_silver,
  last_cursor_id,
  last_extracted_at,
  last_source_row_id,
  last_value_silver,
  now() as updated_at
from latest_candidates
