with expected as (
  select *
  from (
    values
      ('asu_airport', 'GRAN_ASUNCION'),
      ('cde_airport', 'CIUDAD_DEL_ESTE'),
      ('posadas_airport', 'ENCARNACION')
  ) as t(station_code, region_code)
),

actual as (
  select
    weather.code as station_code,
    regions.region_code
  from {{ ref('int_weather_stations') }} weather
  left join {{ ref('weather_stations') }} project_weather
    on project_weather.id = weather.id
  left join {{ ref('regions') }} regions
    on regions.id = project_weather.region_id
  where weather.code in ('asu_airport', 'cde_airport', 'posadas_airport')
)

select
  expected.station_code,
  expected.region_code as expected_region_code,
  actual.region_code as actual_region_code
from expected
left join actual
  on actual.station_code = expected.station_code
where actual.region_code is distinct from expected.region_code
