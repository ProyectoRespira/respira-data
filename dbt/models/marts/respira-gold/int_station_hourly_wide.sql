{{ config(materialized='view') }}

select
  station_id,
  station_code,
  date_hour as date_localtime,
  avg(case when variable_code = 'pm1' then value_hourly end) as pm1,
  avg(case when variable_code = 'pm25' then value_hourly end) as pm2_5,
  avg(case when variable_code = 'pm10' then value_hourly end) as pm10,
  avg(case when variable_code = 'temperature_c' then value_hourly end) as temperature_c,
  avg(case when variable_code = 'hum' then value_hourly end) as humidity,
  avg(case when variable_code = 'pres' then value_hourly end) as pressure,
  avg(case when variable_code = 'wspd' then value_hourly end) as wind_speed,
  avg(case when variable_code = 'wdir' then value_hourly end) as wind_dir
from {{ ref('int_measurements_hourly') }}
group by 1,2,3
