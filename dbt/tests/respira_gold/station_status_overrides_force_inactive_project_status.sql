with overridden as (
  select station_code
  from {{ ref('int_station_status_overrides') }}
)

select
  stations.code,
  stations.status
from {{ ref('int_project_stations') }} stations
join overridden
  on overridden.station_code = stations.code
where stations.status <> 'inactive'
