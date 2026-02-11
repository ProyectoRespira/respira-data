{{ config(materialized='table', enabled=var('build_inference_tables', false)) }}

select
  null::bigint as id,
  null::timestamptz as run_date
where false
