{{ config(materialized='view') }}

select
  id,
  code,
  name,
  latitude,
  longitude,
  status,
  properties,
  is_pattern_station
from {{ ref('dim_stations') }}
where coalesce(properties->>'source', '') = 'Meteostat'
