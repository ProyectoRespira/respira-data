{{ config(
  materialized='incremental',
  unique_key=['source_row_id', 'variable_code'],
  incremental_strategy='merge',
  on_schema_change='sync_all_columns',
  indexes=[
    {'columns': ['source_row_id', 'variable_code'], 'unique': true},
    {'columns': ['extracted_at']},
    {'columns': ['data_source_name', 'measured_at_silver']},
    {'columns': ['data_source_name', 'station_code', 'variable_code', 'measured_at_silver']}
  ]
) }}

with existing_source_cutoffs as (
  {% if is_incremental() and not measurement_has_process_batch_scope() %}
  select
    data_source_name,
    max(extracted_at) as max_extracted_at
  from {{ this }}
  group by data_source_name
  {% else %}
  select
    null::text as data_source_name,
    null::timestamptz as max_extracted_at
  where false
  {% endif %}
),

base_rows as (
  select
    m.*
  from {{ ref('int_measurements_long') }} m
  left join existing_source_cutoffs c
    on c.data_source_name = m.data_source_name
  where m.measured_at_silver is not null
  {% if measurement_has_process_batch_scope() %}
    and {{ measurement_process_row_predicate('m.data_source_name', 'm.measured_at_silver') }}
  {% elif is_incremental() %}
    and m.extracted_at >= coalesce(c.max_extracted_at, '1970-01-01'::timestamptz)
  {% endif %}
),

rules as (
  select
    variable_code,
    min_value::double precision as min_value,
    max_value::double precision as max_value,
    coalesce(allow_null::boolean, true) as allow_null
  from {{ ref('variable_rules') }}
),

target_rows as (
  select
    b.*,
    r.min_value,
    r.max_value,
    coalesce(r.allow_null, true) as allow_null
  from base_rows b
  left join rules r
    on r.variable_code = b.variable_code
),

stream_first_targets as (
  {% if is_incremental() %}
  select
    data_source_name,
    station_code,
    variable_code,
    measured_at_silver as first_measured_at_silver,
    coalesce(cursor_id, -1) as first_cursor_sort,
    extracted_at as first_extracted_at,
    source_row_id as first_source_row_id
  from (
    select
      t.*,
      row_number() over (
        partition by data_source_name, station_code, variable_code
        order by measured_at_silver, coalesce(cursor_id, -1), extracted_at, source_row_id
      ) as rn
    from target_rows t
  ) ranked
  where rn = 1
  {% else %}
  select
    null::text as data_source_name,
    null::text as station_code,
    null::text as variable_code,
    null::timestamptz as first_measured_at_silver,
    null::bigint as first_cursor_sort,
    null::timestamptz as first_extracted_at,
    null::text as first_source_row_id
  where false
  {% endif %}
),

anchor_rows as (
  {% if is_incremental() %}
  select
    '__anchor__:' || first_targets.data_source_name || ':' || first_targets.station_code || ':' || first_targets.variable_code as source_row_id,
    first_targets.first_measured_at_silver - interval '1 microsecond' as extracted_at,
    first_targets.data_source_name,
    first_targets.station_code,
    null::bigint as cursor_id,
    first_targets.first_measured_at_silver - interval '1 microsecond' as measured_at_parsed,
    true as is_measured_at_valid,
    first_targets.first_measured_at_silver - interval '1 microsecond' as measured_at_silver,
    false as is_time_imputed,
    null::text as time_impute_method,
    first_targets.variable_code,
    ''::text as value_raw,
    state.last_value_silver as value_parsed,
    null::double precision as min_value,
    null::double precision as max_value,
    true as allow_null,
    true as is_anchor,
    true as is_observed_value_valid,
    state.last_value_silver as observed_value_silver
  from stream_first_targets first_targets
  join {{ source('ops', 'measurement_stream_state') }} state
    on state.data_source_name = first_targets.data_source_name
   and state.station_code = first_targets.station_code
   and state.variable_code = first_targets.variable_code
   and state.last_value_silver is not null
   and (
     state.last_measured_at_silver,
     coalesce(state.last_cursor_id, -1),
     state.last_extracted_at,
     state.last_source_row_id
   ) < (
     first_targets.first_measured_at_silver,
     first_targets.first_cursor_sort,
     first_targets.first_extracted_at,
     first_targets.first_source_row_id
   )
  {% else %}
  select
    null::text as source_row_id,
    null::timestamptz as extracted_at,
    null::text as data_source_name,
    null::text as station_code,
    null::bigint as cursor_id,
    null::timestamptz as measured_at_parsed,
    null::boolean as is_measured_at_valid,
    null::timestamptz as measured_at_silver,
    null::boolean as is_time_imputed,
    null::text as time_impute_method,
    null::text as variable_code,
    null::text as value_raw,
    null::double precision as value_parsed,
    null::double precision as min_value,
    null::double precision as max_value,
    null::boolean as allow_null,
    null::boolean as is_anchor,
    null::boolean as is_observed_value_valid,
    null::double precision as observed_value_silver
  where false
  {% endif %}
),

observed_validation as (
  select
    source_row_id,
    extracted_at,
    data_source_name,
    station_code,
    cursor_id,
    measured_at_parsed,
    is_measured_at_valid,
    measured_at_silver,
    is_time_imputed,
    time_impute_method,
    variable_code,
    value_raw,
    value_parsed,
    min_value,
    max_value,
    allow_null,
    false as is_anchor,
    case
      when value_parsed is null then false
      when min_value is not null and value_parsed < min_value then false
      when max_value is not null and value_parsed > max_value then false
      else true
    end as is_observed_value_valid,

    case
      when value_parsed is null then null
      when min_value is not null and value_parsed < min_value then null
      when max_value is not null and value_parsed > max_value then null
      else value_parsed
    end as observed_value_silver
  from target_rows

  union all

  select
    source_row_id,
    extracted_at,
    data_source_name,
    station_code,
    cursor_id,
    measured_at_parsed,
    is_measured_at_valid,
    measured_at_silver,
    is_time_imputed,
    time_impute_method,
    variable_code,
    value_raw,
    value_parsed,
    min_value,
    max_value,
    allow_null,
    is_anchor,
    is_observed_value_valid,
    observed_value_silver
  from anchor_rows
),

fill_groups as (
  select
    *,
    count(observed_value_silver) over (
      partition by data_source_name, station_code, variable_code
      order by measured_at_silver, coalesce(cursor_id, -1), extracted_at, source_row_id
      rows between unbounded preceding and current row
    ) as value_fill_group
  from observed_validation
),

filled as (
  select
    *,
    max(observed_value_silver) over (
      partition by data_source_name, station_code, variable_code, value_fill_group
    ) as last_valid_value_silver
  from fill_groups
),

final as (
  select
    -- keys
    source_row_id,
    extracted_at,
    data_source_name,
    station_code,
    cursor_id,
    measured_at_parsed,
    is_measured_at_valid,
    measured_at_silver,
    is_time_imputed,
    time_impute_method,

    -- measurement identity
    variable_code,

    -- raw and parsed
    value_raw,
    value_parsed,

    -- validation result
    case
      when is_observed_value_valid then true
      when value_parsed is null and allow_null and last_valid_value_silver is not null then true
      else false
    end as is_value_valid,

    case
      when is_observed_value_valid then false
      when value_parsed is null and allow_null and last_valid_value_silver is not null then true
      else false
    end as is_value_imputed,

    case
      when is_observed_value_valid then null
      when value_parsed is null and allow_null and last_valid_value_silver is not null then 'forward_fill_last_valid'
      when value_parsed is null and allow_null then 'missing_no_prior_value'
      when value_parsed is null then 'missing_not_allowed'
      when min_value is not null and value_parsed < min_value then 'below_min_value'
      when max_value is not null and value_parsed > max_value then 'above_max_value'
      else 'invalid_value'
    end as value_impute_method,

    case
      when is_observed_value_valid then observed_value_silver
      when value_parsed is null and allow_null and last_valid_value_silver is not null
        then last_valid_value_silver
      else null
    end as value_silver

  from filled
  where not is_anchor
)

select * from final
