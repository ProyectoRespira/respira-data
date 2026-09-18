with windowed as (
  select
    _airbyte_raw_id,
    measured_at_parsed,
    is_measured_at_valid,
    measured_at_silver,
    is_time_imputed,
    time_impute_method,
    cursor_id,
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
  from {{ ref('stg_fiuna_measurements_repaired') }}
)

select *
from windowed
where
  (
    is_measured_at_valid
    and (
      measured_at_silver is distinct from measured_at_parsed
      or is_time_imputed
      or time_impute_method is not null
    )
  )
  or (
    not is_measured_at_valid
    and last_valid_ts is not null
    and last_valid_id is not null
    and (
      measured_at_silver is distinct from (
        last_valid_ts + ((cursor_id - last_valid_id) * interval '5 minutes')
      )
      or not is_time_imputed
      or time_impute_method is distinct from 'fiuna_id_5min'
    )
  )
  or (
    not is_measured_at_valid
    and (last_valid_ts is null or last_valid_id is null)
    and (
      measured_at_silver is not null
      or is_time_imputed
      or time_impute_method is distinct from 'unfixable_no_anchor'
    )
  )
