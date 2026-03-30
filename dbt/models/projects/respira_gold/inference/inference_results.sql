{{ config(materialized='table', enabled=var('build_inference_tables', false)) }}

select
  null::bigint as id,
  null::bigint as inference_run_id,
  null::bigint as station_id,
  null::jsonb as forecast_6h,
  null::jsonb as forecast_12h,
  null::jsonb as aqi_input
where false
