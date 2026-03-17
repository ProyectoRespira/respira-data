select distinct
  m.station_code,
  m.variable_code,
  m.data_source_name,
  (m.station_code || '_' || m.variable_code || '_' || m.data_source_name) as code,
  (m.variable_code || ' at ' || m.station_code) as name
from {{ ref('int_measurements_long') }} m
