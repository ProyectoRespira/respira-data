with expected as (
  select *
  from (
    values
      ('GRAN_ASUNCION', 'asu_airport'),
      ('CIUDAD_DEL_ESTE', 'cde_airport'),
      ('ENCARNACION', 'posadas_airport'),
      ('CAAGUAZU', 'asu_airport'),
      ('CONCEPCION', 'asu_airport'),
      ('SAN_PEDRO', 'asu_airport')
  ) as t(region_code, station_code)
),

actual as (
  select region_code, station_code
  from {{ ref('int_region_weather_stations') }}
),

region_flags as (
  select region_code, has_weather_data
  from {{ ref('regions') }}
)

select
  expected.region_code,
  expected.station_code as expected_station_code,
  actual.station_code as actual_station_code,
  region_flags.has_weather_data
from expected
left join actual
  on actual.region_code = expected.region_code
left join region_flags
  on region_flags.region_code = expected.region_code
where actual.station_code is distinct from expected.station_code
  or region_flags.has_weather_data is distinct from true
