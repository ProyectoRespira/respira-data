with m as (
  select *
  from {{ ref('int_measurements_time_silver') }}
  where measured_at_silver is not null
),

rules as (
  select
    variable_code,
    min_value::double precision as min_value,
    max_value::double precision as max_value,
    coalesce(allow_null::boolean, true) as allow_null
  from {{ ref('variable_rules') }}
),

joined as (
  select
    m.*,
    r.min_value,
    r.max_value,
    coalesce(r.allow_null, true) as allow_null
  from m
  left join rules r
    on r.variable_code = m.variable_code
),

observed_validation as (
  select
    *,
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
  from joined
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
)

select * from final
