{%- set sources_cfg = var('measurements_sources') -%}
{%- set selected_source_names = get_selected_measurement_sources(sources_cfg) -%}
{%- set cutover_min_coverage = var('measurement_cutover_min_long_coverage_ratio', 0.90) -%}

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

{% if measurement_batch_unmarked_only() %}

-- Historical cutover rows may outlive the raw Airbyte rows used by the inline
-- transformation. During cutover only, accept configured-variable coverage
-- from either the inline result or the retained physical legacy table.
covered_variables as (
  select
    l.source_row_id,
    l.data_source_name,
    l.variable_code
  from {{ ref('int_measurements_long') }} l
  join expected e
    on e.source_row_id = l.source_row_id
   and e.data_source_name = l.data_source_name

  union

  select
    legacy.source_row_id,
    legacy.data_source_name,
    legacy.variable_code
  from {{ source('legacy_runtime_intermediate', 'int_measurements_long') }} legacy
  join expected e
    on e.source_row_id = legacy.source_row_id
   and e.data_source_name = legacy.data_source_name
),

actual as (
  select
    source_row_id,
    data_source_name,
    count(*)::bigint as actual_row_count
  from covered_variables
  group by source_row_id, data_source_name
),

{% else %}

actual as (
  select
    l.source_row_id,
    l.data_source_name,
    count(*)::bigint as actual_row_count
  from {{ ref('int_measurements_long') }} l
  group by l.source_row_id, l.data_source_name
),

{% endif %}

row_coverage as (
  select
    e.source_row_id,
    e.data_source_name,
    e.expected_row_count,
    coalesce(a.actual_row_count, 0) as actual_row_count,
    coalesce(a.actual_row_count, 0) = e.expected_row_count as is_complete
  from expected e
  left join actual a
    on a.source_row_id = e.source_row_id
   and a.data_source_name = e.data_source_name
)

{% if measurement_batch_unmarked_only() %}

-- Cutover validates the whole bounded scope deterministically. A small number
-- of historical queue rows may have outlived every row-level transformation
-- witness, so require a high complete-expansion ratio instead of making one
-- stale row block an otherwise healthy historical cutover.
select
  count(*)::bigint as total_queue_rows,
  count(*) filter (where is_complete)::bigint as complete_queue_rows,
  count(*) filter (where not is_complete)::bigint as incomplete_queue_rows,
  (
    count(*) filter (where is_complete)::numeric
    / nullif(count(*), 0)
  ) as coverage_ratio,
  {{ cutover_min_coverage }}::numeric as required_coverage_ratio
from row_coverage
having count(*) > 0
   and (
     count(*) filter (where is_complete)::numeric
     / nullif(count(*), 0)
   ) < {{ cutover_min_coverage }}::numeric

{% else %}

select
  source_row_id,
  data_source_name,
  expected_row_count,
  actual_row_count
from row_coverage
where not is_complete

{% endif %}
