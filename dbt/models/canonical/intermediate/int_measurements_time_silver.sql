{{ config(
  materialized='view',
  meta={
    'compatibility_contract': true,
    'runtime_scope': 'recent_or_explicit_batch',
    'ad_hoc_cost': 'potentially_expensive'
  }
) }}

-- Compatibility view kept temporarily for external or ad hoc consumers.
-- It exposes only the current runtime scope and inlines the long transformation;
-- broad ad hoc queries may therefore be expensive.

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
