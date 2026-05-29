with cases as (
  select
    'pm25_24h_reference_value' as case_name,
    62 as expected,
    {{ aqi_pm25('17.301805555333335::numeric') }} as actual

  union all

  select
    'pm25_truncates_to_one_decimal' as case_name,
    50 as expected,
    {{ aqi_pm25('12.09::numeric') }} as actual

  union all

  select
    'pm10_truncates_to_integer' as case_name,
    50 as expected,
    {{ aqi_pm10('54.99::numeric') }} as actual
),

failing as (
  select *
  from cases
  where actual <> expected
)

select * from failing
