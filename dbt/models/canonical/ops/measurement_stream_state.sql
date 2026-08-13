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
-- depends_on: {{ ref('int_measurements_values_silver') }}
-- depends_on: {{ ref('fct_measurements_silver') }}

with streams as (
  select id as stream_id, code
  from {{ ref('dim_streams') }}
),

published_candidates as (
  select
    values.data_source_name,
    values.station_code,
    values.variable_code,
    values.measured_at_silver as last_measured_at_silver,
    values.cursor_id as last_cursor_id,
    values.extracted_at as last_extracted_at,
    values.source_row_id as last_source_row_id,
    values.value_silver as last_value_silver
  from {{ ref('int_measurements_values_silver') }} values
  join streams
    on streams.code = (
      values.station_code || '_' || values.variable_code || '_' || values.data_source_name
    )
  join {{ ref('fct_measurements_silver') }} fact
    on fact.stream_id = streams.stream_id
   and fact.source_row_id = values.source_row_id
   and fact.timestamp = values.measured_at_silver
   and fact.ingested_at = values.extracted_at
   and fact.value_parsed is not distinct from values.value_silver
  left join {{ this }} state
    using (data_source_name, station_code, variable_code)
  where values.value_silver is not null
    and (
      state.data_source_name is null
      or (
        values.measured_at_silver,
        coalesce(values.cursor_id, -1),
        values.extracted_at,
        values.source_row_id
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
