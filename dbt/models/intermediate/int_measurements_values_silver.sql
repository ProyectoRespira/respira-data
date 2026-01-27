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
    r.allow_null
  from m
  left join rules r
    on r.variable_code = m.variable_code
),

final as (
  select
    -- keys
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
    raw_payload,

    -- validation result
    case
      when value_parsed is null then allow_null
      when min_value is not null and value_parsed < min_value then false
      when max_value is not null and value_parsed > max_value then false
      else true
    end as is_value_valid,

    case
      when (
        case
          when value_parsed is null then allow_null
          when min_value is not null and value_parsed < min_value then false
          when max_value is not null and value_parsed > max_value then false
          else true
        end
      )
      then value_parsed
      else null
    end as value_silver

  from joined
)

select * from final
