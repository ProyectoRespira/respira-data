{{ config(materialized='view') }}

with observed as (
  select * from {{ ref('int_station_hourly_wide') }}
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
    o.pm1,
    o.pm2_5,
    o.pm10,
    o.temperature_c,
    o.humidity,
    o.pressure,
    o.wind_speed,
    o.wind_dir
  from hourly_spine s
  left join observed o
    on o.station_id = s.station_id
   and o.date_localtime = s.date_localtime
),

groups as (
  select
    *,
    count(pm1) over (
      partition by station_id
      order by date_localtime
      rows between unbounded preceding and current row
    ) as pm1_prev_group,
    count(pm1) over (
      partition by station_id
      order by date_localtime desc
      rows between unbounded preceding and current row
    ) as pm1_next_group,
    count(pm2_5) over (
      partition by station_id
      order by date_localtime
      rows between unbounded preceding and current row
    ) as pm2_5_prev_group,
    count(pm2_5) over (
      partition by station_id
      order by date_localtime desc
      rows between unbounded preceding and current row
    ) as pm2_5_next_group,
    count(pm10) over (
      partition by station_id
      order by date_localtime
      rows between unbounded preceding and current row
    ) as pm10_prev_group,
    count(pm10) over (
      partition by station_id
      order by date_localtime desc
      rows between unbounded preceding and current row
    ) as pm10_next_group
  from joined
),

neighbors as (
  select
    *,
    max(pm1) over (partition by station_id, pm1_prev_group) as pm1_prev_value,
    min(case when pm1 is not null then date_localtime end) over (
      partition by station_id, pm1_prev_group
    ) as pm1_prev_time,
    max(pm1) over (partition by station_id, pm1_next_group) as pm1_next_value,
    max(case when pm1 is not null then date_localtime end) over (
      partition by station_id, pm1_next_group
    ) as pm1_next_time,

    max(pm2_5) over (partition by station_id, pm2_5_prev_group) as pm2_5_prev_value,
    min(case when pm2_5 is not null then date_localtime end) over (
      partition by station_id, pm2_5_prev_group
    ) as pm2_5_prev_time,
    max(pm2_5) over (partition by station_id, pm2_5_next_group) as pm2_5_next_value,
    max(case when pm2_5 is not null then date_localtime end) over (
      partition by station_id, pm2_5_next_group
    ) as pm2_5_next_time,

    max(pm10) over (partition by station_id, pm10_prev_group) as pm10_prev_value,
    min(case when pm10 is not null then date_localtime end) over (
      partition by station_id, pm10_prev_group
    ) as pm10_prev_time,
    max(pm10) over (partition by station_id, pm10_next_group) as pm10_next_value,
    max(case when pm10 is not null then date_localtime end) over (
      partition by station_id, pm10_next_group
    ) as pm10_next_time
  from groups
),

filled as (
  select
    station_id,
    station_code,
    date_localtime,

    case
      when pm1 is not null then pm1
      when pm1_prev_value is not null
        and pm1_next_value is not null
        and pm1_next_time > pm1_prev_time
        then pm1_prev_value
          + (
            extract(epoch from (date_localtime - pm1_prev_time))
            / nullif(extract(epoch from (pm1_next_time - pm1_prev_time)), 0)
          ) * (pm1_next_value - pm1_prev_value)
      when pm1_prev_value is not null then pm1_prev_value
      when pm1_next_value is not null then pm1_next_value
      else null
    end as pm1,

    case
      when pm2_5 is not null then pm2_5
      when pm2_5_prev_value is not null
        and pm2_5_next_value is not null
        and pm2_5_next_time > pm2_5_prev_time
        then pm2_5_prev_value
          + (
            extract(epoch from (date_localtime - pm2_5_prev_time))
            / nullif(extract(epoch from (pm2_5_next_time - pm2_5_prev_time)), 0)
          ) * (pm2_5_next_value - pm2_5_prev_value)
      when pm2_5_prev_value is not null then pm2_5_prev_value
      when pm2_5_next_value is not null then pm2_5_next_value
      else null
    end as pm2_5,

    case
      when pm10 is not null then pm10
      when pm10_prev_value is not null
        and pm10_next_value is not null
        and pm10_next_time > pm10_prev_time
        then pm10_prev_value
          + (
            extract(epoch from (date_localtime - pm10_prev_time))
            / nullif(extract(epoch from (pm10_next_time - pm10_prev_time)), 0)
          ) * (pm10_next_value - pm10_prev_value)
      when pm10_prev_value is not null then pm10_prev_value
      when pm10_next_value is not null then pm10_next_value
      else null
    end as pm10,

    temperature_c,
    humidity,
    pressure,
    wind_speed,
    wind_dir
  from neighbors
)

select * from filled
