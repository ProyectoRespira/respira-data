select
  f._airbyte_raw_id,
  f.measured_at_silver as staging_measured_at_silver,
  t.measured_at_silver as intermediate_measured_at_silver,
  f.is_time_imputed as staging_is_time_imputed,
  t.is_time_imputed as intermediate_is_time_imputed,
  f.time_impute_method as staging_time_impute_method,
  t.time_impute_method as intermediate_time_impute_method
from {{ ref('stg_fiuna_measurements') }} f
left join {{ ref('int_measurement_timestamps_silver') }} t
  on t.data_source_name = 'fiuna_airbyte'
 and t.source_row_id = f._airbyte_raw_id::text
where t.source_row_id is null
   or t.measured_at_silver is distinct from f.measured_at_silver
   or t.is_time_imputed is distinct from f.is_time_imputed
   or t.time_impute_method is distinct from f.time_impute_method
