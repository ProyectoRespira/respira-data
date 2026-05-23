with fiuna_source_rows as (
  select
    data_source_name,
    source_row_id,
    count(*) as row_count,
    count(distinct coalesce(measured_at_silver::text, '__dbt_null__')) as distinct_silver_times
  from {{ ref('int_measurements_long') }}
  where data_source_name = 'fiuna_airbyte'
  group by 1, 2
)

select *
from fiuna_source_rows
where row_count > 1
  and distinct_silver_times > 1
