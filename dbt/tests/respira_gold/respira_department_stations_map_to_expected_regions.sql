with expected as (
  select *
  from (
    values
      ('Gobernacion de Caaguazú', 'CAAGUAZU'),
      ('Cuerpo de Bomberos Voluntarios Vallemi', 'CONCEPCION'),
      ('Centro Educativo Campo Verde', 'CONCEPCION'),
      ('Colegio Nacional Santa Rosa del Aguaray', 'SAN_PEDRO')
  ) as t(station_name, region_code)
),

actual as (
  select
    stations.name as station_name,
    station_map.region_code
  from {{ ref('int_air_quality_stations') }} stations
  left join {{ ref('int_station_regions') }} station_map
    on station_map.station_code = stations.code
  where lower(coalesce(stations.properties->>'source', '')) = 'respira'
)

select
  expected.station_name,
  expected.region_code as expected_region_code,
  actual.region_code as actual_region_code
from expected
left join actual
  on actual.station_name = expected.station_name
where actual.region_code is distinct from expected.region_code
