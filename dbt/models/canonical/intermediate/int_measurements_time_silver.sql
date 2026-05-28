{{ config(materialized='view') }}

-- Compatibility view kept temporarily for external or ad hoc consumers.
-- TODO: Remove this model after all consumers migrate to int_measurements_long.

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
  value_parsed
from {{ ref('int_measurements_long') }}
