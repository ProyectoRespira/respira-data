with m as (

  select
    data_source_name,
    station_code,
    cursor_id,
    measured_at_parsed,
    is_measured_at_valid,

    variable_code,
    value_raw,
    value_parsed,
    raw_payload
  from {{ ref('int_measurements_long') }}

),

fiuna as (

  select
    *,
    max(case when is_measured_at_valid then measured_at_parsed end)
      over (
        partition by station_code
        order by cursor_id
        rows between unbounded preceding and current row
      ) as last_valid_ts,

    max(case when is_measured_at_valid then cursor_id end)
      over (
        partition by station_code
        order by cursor_id
        rows between unbounded preceding and current row
      ) as last_valid_id
  from m
  where data_source_name = 'fiuna_airbyte'

),

fiuna_fixed as (

  select
    data_source_name,
    station_code,
    cursor_id,
    measured_at_parsed,
    is_measured_at_valid,

    case
      when is_measured_at_valid then measured_at_parsed
      when last_valid_ts is not null and last_valid_id is not null
        then last_valid_ts + ((cursor_id - last_valid_id) * interval '5 minutes')
      else null
    end as measured_at_silver,

    case
      when is_measured_at_valid then false
      when last_valid_ts is not null and last_valid_id is not null then true
      else false
    end as is_time_imputed,

    case
      when is_measured_at_valid then null
      when last_valid_ts is not null and last_valid_id is not null then 'fiuna_id_5min'
      else 'unfixable_no_anchor'
    end as time_impute_method,

    variable_code,
    value_raw,
    value_parsed,
    raw_payload
  from fiuna

),

other_sources as (

  select
    data_source_name,
    station_code,
    cursor_id,
    measured_at_parsed,
    is_measured_at_valid,

    case
      when is_measured_at_valid then measured_at_parsed
      else null
    end as measured_at_silver,

    false as is_time_imputed,

    case
      when is_measured_at_valid then null
      else 'invalid_timestamp_no_imputation'
    end as time_impute_method,

    variable_code,
    value_raw,
    value_parsed,
    raw_payload
  from m
  where data_source_name <> 'fiuna_airbyte'

)

select * from fiuna_fixed
union all
select * from other_sources
