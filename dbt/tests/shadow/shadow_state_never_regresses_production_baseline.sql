select shadow.*
from {{ ref('measurement_stream_state_shadow') }} shadow
join {{ source('ops', 'measurement_stream_state') }} production
  using (data_source_name, station_code, variable_code)
where (
  shadow.last_measured_at_silver,
  coalesce(shadow.last_cursor_id, -1),
  shadow.last_extracted_at,
  shadow.last_source_row_id
) < (
  production.last_measured_at_silver,
  coalesce(production.last_cursor_id, -1),
  production.last_extracted_at,
  production.last_source_row_id
)
