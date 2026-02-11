{{ config(materialized='view') }}

with station_base as (
  select
    sr.*, 
    st.region_id
  from {{ ref('station_readings_gold') }} sr
  join {{ ref('stations') }} st
    on st.id = sr.station_id
),

region_features as (
  select * from {{ ref('region_readings_gold') }}
),

weather_features as (
  select * from {{ ref('region_weather_hourly') }}
)

select
  sb.station_id,
  sb.date_localtime as date_utc,

  sb.pm1,
  sb.pm2_5,
  sb.pm10,
  sb.pm2_5_avg_6h,
  sb.pm2_5_max_6h,
  sb.pm2_5_skew_6h,
  sb.pm2_5_std_6h,

  sb.aqi_pm2_5,
  sb.aqi_pm2_5_max_24h,
  sb.aqi_pm2_5_skew_24h,
  sb.aqi_pm2_5_std_24h,

  rf.pm2_5_region_avg,
  rf.pm2_5_region_max,
  rf.pm2_5_region_skew,
  rf.pm2_5_region_std,
  rf.aqi_region_avg,
  rf.aqi_region_max,
  rf.aqi_region_skew,
  rf.aqi_region_std,
  rf.level_region_max,

  wf.temperature_c,
  wf.humidity,
  wf.pressure,
  wf.wind_speed,
  wf.wind_dir_sin,
  wf.wind_dir_cos

from station_base sb
left join region_features rf
  on rf.region_id = sb.region_id
 and rf.date_utc = sb.date_localtime
left join weather_features wf
  on wf.region_id = sb.region_id
 and wf.date_localtime = sb.date_localtime
