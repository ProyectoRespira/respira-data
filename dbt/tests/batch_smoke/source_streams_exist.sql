with candidates as (
  select code
  from {{ ref('int_streams_candidates') }}
  where {{ measurement_source_column_predicate('data_source_name') }}
),

missing as (
  select c.code
  from candidates c
  left join {{ ref('dim_streams') }} s
    on s.code = c.code
  where s.id is null
)

select * from missing
