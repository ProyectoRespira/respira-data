with overridden as (
  select station_code
  from {{ ref('int_station_status_overrides') }}
)

select
  air_quality.code,
  backend_stations.id as project_station_id,
  backend_stations.is_station_on
from {{ ref('int_air_quality_stations') }} air_quality
join {{ ref('int_station_id_map') }} station_id_map
  on station_id_map.core_station_id = air_quality.id
join {{ ref('stations') }} backend_stations
  on backend_stations.id = station_id_map.project_station_id
join overridden
  on overridden.station_code = air_quality.code
where backend_stations.is_station_on
