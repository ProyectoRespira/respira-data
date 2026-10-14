{{ config(
  materialized='table',
  alias='debug_int_measurements_long',
  tags=['debug_only']
) }}

{{ validate_measurement_debug_scope() }}

select *
from {{ ref('int_measurements_long') }}
