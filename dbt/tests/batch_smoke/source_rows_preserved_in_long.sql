{%- set sources_cfg = var('measurements_sources') -%}
{%- set selected_source_names = get_selected_measurement_sources(sources_cfg) -%}

with expected as (

  {%- for source_name in selected_source_names %}
  {%- set cfg = sources_cfg[source_name] %}
  {%- set variable_count = (cfg.get('variables', {}) | length) %}

  select
    t.source_row_id,
    t.data_source_name,
    {{ variable_count }}::bigint as expected_row_count
  from {{ ref('int_measurement_timestamps_silver') }} t
  where t.data_source_name = '{{ source_name }}'
    and {{ measurement_runtime_queue_row_predicate(
      't.data_source_name',
      't.measured_at_silver',
      't.cleanup_eligible_at'
    ) }}

  {%- if not loop.last %}
  union all
  {%- endif %}

  {%- endfor %}

),

actual as (
  select
    l.source_row_id,
    l.data_source_name,
    count(*)::bigint as actual_row_count
  from {{ ref('int_measurements_long') }} l
  group by l.source_row_id, l.data_source_name
),

mismatches as (
  select
    e.source_row_id,
    e.data_source_name,
    e.expected_row_count,
    coalesce(a.actual_row_count, 0) as actual_row_count
  from expected e
  left join actual a
    on a.source_row_id = e.source_row_id
   and a.data_source_name = e.data_source_name
  where coalesce(a.actual_row_count, 0) <> e.expected_row_count
)

select * from mismatches
