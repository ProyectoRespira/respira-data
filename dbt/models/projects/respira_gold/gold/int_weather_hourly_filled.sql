{{ config(materialized='view') }}

with observed as (
  select
    w.station_id,
    w.station_code,
    w.date_localtime,
    w.temperature_c,
    w.humidity,
    w.pressure,
    w.wind_speed,
    sin(radians(w.wind_dir)) as wind_dir_sin,
    cos(radians(w.wind_dir)) as wind_dir_cos
  from {{ ref('int_station_hourly_wide') }} w
  join {{ ref('int_weather_stations') }} s
    on s.code = w.station_code
),

station_bounds as (
  select
    station_id,
    max(station_code) as station_code,
    min(date_localtime) as min_date_localtime,
    max(date_localtime) as max_date_localtime
  from observed
  group by 1
),

hourly_spine as (
  select
    b.station_id,
    b.station_code,
    gs.date_localtime
  from station_bounds b
  cross join lateral generate_series(
    b.min_date_localtime,
    b.max_date_localtime,
    interval '1 hour'
  ) as gs(date_localtime)
),

joined as (
  select
    s.station_id,
    s.station_code,
    s.date_localtime,
    o.temperature_c,
    o.humidity,
    o.pressure,
    o.wind_speed,
    o.wind_dir_sin,
    o.wind_dir_cos
  from hourly_spine s
  left join observed o
    on o.station_id = s.station_id
   and o.date_localtime = s.date_localtime
),

groups as (
  select
    *,
    count(temperature_c) over (
      partition by station_id
      order by date_localtime
      rows between unbounded preceding and current row
    ) as temperature_prev_group,
    count(temperature_c) over (
      partition by station_id
      order by date_localtime desc
      rows between unbounded preceding and current row
    ) as temperature_next_group,

    count(humidity) over (
      partition by station_id
      order by date_localtime
      rows between unbounded preceding and current row
    ) as humidity_prev_group,
    count(humidity) over (
      partition by station_id
      order by date_localtime desc
      rows between unbounded preceding and current row
    ) as humidity_next_group,

    count(pressure) over (
      partition by station_id
      order by date_localtime
      rows between unbounded preceding and current row
    ) as pressure_prev_group,
    count(pressure) over (
      partition by station_id
      order by date_localtime desc
      rows between unbounded preceding and current row
    ) as pressure_next_group,

    count(wind_speed) over (
      partition by station_id
      order by date_localtime
      rows between unbounded preceding and current row
    ) as wind_speed_prev_group,
    count(wind_speed) over (
      partition by station_id
      order by date_localtime desc
      rows between unbounded preceding and current row
    ) as wind_speed_next_group,

    count(wind_dir_sin) over (
      partition by station_id
      order by date_localtime
      rows between unbounded preceding and current row
    ) as wind_dir_sin_prev_group,
    count(wind_dir_sin) over (
      partition by station_id
      order by date_localtime desc
      rows between unbounded preceding and current row
    ) as wind_dir_sin_next_group,

    count(wind_dir_cos) over (
      partition by station_id
      order by date_localtime
      rows between unbounded preceding and current row
    ) as wind_dir_cos_prev_group,
    count(wind_dir_cos) over (
      partition by station_id
      order by date_localtime desc
      rows between unbounded preceding and current row
    ) as wind_dir_cos_next_group
  from joined
),

neighbors as (
  select
    *,
    max(temperature_c) over (partition by station_id, temperature_prev_group) as temperature_prev_value,
    min(case when temperature_c is not null then date_localtime end) over (
      partition by station_id, temperature_prev_group
    ) as temperature_prev_time,
    max(temperature_c) over (partition by station_id, temperature_next_group) as temperature_next_value,
    max(case when temperature_c is not null then date_localtime end) over (
      partition by station_id, temperature_next_group
    ) as temperature_next_time,

    max(humidity) over (partition by station_id, humidity_prev_group) as humidity_prev_value,
    min(case when humidity is not null then date_localtime end) over (
      partition by station_id, humidity_prev_group
    ) as humidity_prev_time,
    max(humidity) over (partition by station_id, humidity_next_group) as humidity_next_value,
    max(case when humidity is not null then date_localtime end) over (
      partition by station_id, humidity_next_group
    ) as humidity_next_time,

    max(pressure) over (partition by station_id, pressure_prev_group) as pressure_prev_value,
    min(case when pressure is not null then date_localtime end) over (
      partition by station_id, pressure_prev_group
    ) as pressure_prev_time,
    max(pressure) over (partition by station_id, pressure_next_group) as pressure_next_value,
    max(case when pressure is not null then date_localtime end) over (
      partition by station_id, pressure_next_group
    ) as pressure_next_time,

    max(wind_speed) over (partition by station_id, wind_speed_prev_group) as wind_speed_prev_value,
    min(case when wind_speed is not null then date_localtime end) over (
      partition by station_id, wind_speed_prev_group
    ) as wind_speed_prev_time,
    max(wind_speed) over (partition by station_id, wind_speed_next_group) as wind_speed_next_value,
    max(case when wind_speed is not null then date_localtime end) over (
      partition by station_id, wind_speed_next_group
    ) as wind_speed_next_time,

    max(wind_dir_sin) over (partition by station_id, wind_dir_sin_prev_group) as wind_dir_sin_prev_value,
    min(case when wind_dir_sin is not null then date_localtime end) over (
      partition by station_id, wind_dir_sin_prev_group
    ) as wind_dir_sin_prev_time,
    max(wind_dir_sin) over (partition by station_id, wind_dir_sin_next_group) as wind_dir_sin_next_value,
    max(case when wind_dir_sin is not null then date_localtime end) over (
      partition by station_id, wind_dir_sin_next_group
    ) as wind_dir_sin_next_time,

    max(wind_dir_cos) over (partition by station_id, wind_dir_cos_prev_group) as wind_dir_cos_prev_value,
    min(case when wind_dir_cos is not null then date_localtime end) over (
      partition by station_id, wind_dir_cos_prev_group
    ) as wind_dir_cos_prev_time,
    max(wind_dir_cos) over (partition by station_id, wind_dir_cos_next_group) as wind_dir_cos_next_value,
    max(case when wind_dir_cos is not null then date_localtime end) over (
      partition by station_id, wind_dir_cos_next_group
    ) as wind_dir_cos_next_time
  from groups
),

filled as (
  select
    station_id,
    station_code,
    date_localtime,

    case
      when temperature_c is not null then temperature_c
      when temperature_prev_value is not null
        and temperature_next_value is not null
        and temperature_next_time > temperature_prev_time
        then temperature_prev_value
          + (
            extract(epoch from (date_localtime - temperature_prev_time))
            / nullif(extract(epoch from (temperature_next_time - temperature_prev_time)), 0)
          ) * (temperature_next_value - temperature_prev_value)
      when temperature_prev_value is not null then temperature_prev_value
      when temperature_next_value is not null then temperature_next_value
      else null
    end as temperature_c,

    case
      when humidity is not null then humidity
      when humidity_prev_value is not null
        and humidity_next_value is not null
        and humidity_next_time > humidity_prev_time
        then humidity_prev_value
          + (
            extract(epoch from (date_localtime - humidity_prev_time))
            / nullif(extract(epoch from (humidity_next_time - humidity_prev_time)), 0)
          ) * (humidity_next_value - humidity_prev_value)
      when humidity_prev_value is not null then humidity_prev_value
      when humidity_next_value is not null then humidity_next_value
      else null
    end as humidity,

    case
      when pressure is not null then pressure
      when pressure_prev_value is not null
        and pressure_next_value is not null
        and pressure_next_time > pressure_prev_time
        then pressure_prev_value
          + (
            extract(epoch from (date_localtime - pressure_prev_time))
            / nullif(extract(epoch from (pressure_next_time - pressure_prev_time)), 0)
          ) * (pressure_next_value - pressure_prev_value)
      when pressure_prev_value is not null then pressure_prev_value
      when pressure_next_value is not null then pressure_next_value
      else null
    end as pressure,

    case
      when wind_speed is not null then wind_speed
      when wind_speed_prev_value is not null
        and wind_speed_next_value is not null
        and wind_speed_next_time > wind_speed_prev_time
        then wind_speed_prev_value
          + (
            extract(epoch from (date_localtime - wind_speed_prev_time))
            / nullif(extract(epoch from (wind_speed_next_time - wind_speed_prev_time)), 0)
          ) * (wind_speed_next_value - wind_speed_prev_value)
      when wind_speed_prev_value is not null then wind_speed_prev_value
      when wind_speed_next_value is not null then wind_speed_next_value
      else null
    end as wind_speed,

    case
      when wind_dir_sin is not null then wind_dir_sin
      when wind_dir_sin_prev_value is not null
        and wind_dir_sin_next_value is not null
        and wind_dir_sin_next_time > wind_dir_sin_prev_time
        then wind_dir_sin_prev_value
          + (
            extract(epoch from (date_localtime - wind_dir_sin_prev_time))
            / nullif(extract(epoch from (wind_dir_sin_next_time - wind_dir_sin_prev_time)), 0)
          ) * (wind_dir_sin_next_value - wind_dir_sin_prev_value)
      when wind_dir_sin_prev_value is not null then wind_dir_sin_prev_value
      when wind_dir_sin_next_value is not null then wind_dir_sin_next_value
      else null
    end as wind_dir_sin,

    case
      when wind_dir_cos is not null then wind_dir_cos
      when wind_dir_cos_prev_value is not null
        and wind_dir_cos_next_value is not null
        and wind_dir_cos_next_time > wind_dir_cos_prev_time
        then wind_dir_cos_prev_value
          + (
            extract(epoch from (date_localtime - wind_dir_cos_prev_time))
            / nullif(extract(epoch from (wind_dir_cos_next_time - wind_dir_cos_prev_time)), 0)
          ) * (wind_dir_cos_next_value - wind_dir_cos_prev_value)
      when wind_dir_cos_prev_value is not null then wind_dir_cos_prev_value
      when wind_dir_cos_next_value is not null then wind_dir_cos_next_value
      else null
    end as wind_dir_cos
  from neighbors
)

select * from filled
