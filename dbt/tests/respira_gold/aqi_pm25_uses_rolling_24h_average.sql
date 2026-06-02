with sample as (
  select
    1 as station_id,
    ('2026-05-27 00:00:00+00'::timestamptz + ((seq - 1) * interval '1 hour')) as date_localtime,
    pm2_5
  from (
    values
      (1, 82.075::numeric),
      (2, 60.87::numeric),
      (3, 20.12::numeric),
      (4, 12.90333333::numeric),
      (5, 11.11833333::numeric),
      (6, 8.873333333::numeric),
      (7, 8.631666667::numeric),
      (8, 7.605::numeric),
      (9, 7.201666667::numeric),
      (10, 8.228333333::numeric),
      (11, 8.921666667::numeric),
      (12, 9.856666667::numeric),
      (13, 10.72333333::numeric),
      (14, 10.53666667::numeric),
      (15, 9.56::numeric),
      (16, 10.30333333::numeric),
      (17, 8.391666667::numeric),
      (18, 9.816666667::numeric),
      (19, 22.83333333::numeric),
      (20, 20.585::numeric),
      (21, 15.55666667::numeric),
      (22, 19.68166667::numeric),
      (23, 14.555::numeric),
      (24, 16.295::numeric)
  ) as readings(seq, pm2_5)
),

rolling as (
  select
    station_id,
    date_localtime,
    avg(pm2_5) over (
      partition by station_id
      order by date_localtime
      rows between 23 preceding and current row
    ) as pm2_5_avg_24h
  from sample
),

latest as (
  select
    date_localtime,
    round(pm2_5_avg_24h, 1) as pm2_5_avg_24h_rounded,
    {{ aqi_pm25('pm2_5_avg_24h') }} as aqi_pm2_5
  from rolling
  order by date_localtime desc
  limit 1
),

failing as (
  select *
  from latest
  where pm2_5_avg_24h_rounded <> 17.3
    or aqi_pm2_5 <> 62
)

select * from failing
