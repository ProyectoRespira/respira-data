{{ config(
  materialized='incremental',
  alias='measurement_stream_state',
  unique_key=['data_source_name', 'station_code', 'variable_code'],
  incremental_strategy='merge',
  on_schema_change='sync_all_columns',
  indexes=[
    {'columns': ['data_source_name', 'station_code', 'variable_code'], 'unique': true},
    {'columns': ['updated_at']},
    {'columns': ['data_source_name', 'last_measured_at_silver', 'last_extracted_at']}
  ]
) }}

-- depends_on: {{ ref('fct_measurements_silver_shadow') }}

{% if is_incremental() %}
{{ validate_measurement_shadow_scope() }}

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
  from {{ ref('int_measurements_values_silver_shadow') }} values
  join streams
    on streams.code = (
      values.station_code || '_' || values.variable_code || '_' || values.data_source_name
    )
  join {{ ref('fct_measurements_silver_shadow') }} fact
    on fact.stream_id = streams.stream_id
   and fact.source_row_id = values.source_row_id
   and fact.timestamp = values.measured_at_silver
   and fact.ingested_at = values.extracted_at
   and fact.value_parsed is not distinct from values.value_silver
  where values.value_silver is not null
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
),

advanced as (
  select candidates.*
  from latest_candidates candidates
  left join {{ this }} state
    using (data_source_name, station_code, variable_code)
  where state.data_source_name is null
     or (
       candidates.last_measured_at_silver,
       coalesce(candidates.last_cursor_id, -1),
       candidates.last_extracted_at,
       candidates.last_source_row_id
     ) > (
       state.last_measured_at_silver,
       coalesce(state.last_cursor_id, -1),
       state.last_extracted_at,
       state.last_source_row_id
     )
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
from advanced

{% else %}

select
  data_source_name,
  station_code,
  variable_code,
  last_measured_at_silver,
  last_cursor_id,
  last_extracted_at,
  last_source_row_id,
  last_value_silver,
  updated_at
from {{ source('ops', 'measurement_stream_state') }}

{% endif %}
