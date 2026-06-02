select *
from {{ ref('int_measurement_timestamps_silver') }}
where data_source_name <> 'fiuna_airbyte'
  and (
    (coalesce(is_measured_at_valid, false) and measured_at_silver is distinct from measured_at_parsed)
    or (not coalesce(is_measured_at_valid, false) and measured_at_silver is not null)
    or is_time_imputed
  )
