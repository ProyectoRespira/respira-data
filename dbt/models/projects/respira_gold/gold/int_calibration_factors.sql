{{ config(materialized='view') }}

{% set schema_name = var('calibration_factors_schema', 'respira_gold') %}
{% set table_name = var('calibration_factors_identifier', 'calibration_factors') %}
{% set rel = adapter.get_relation(database=target.database, schema=schema_name, identifier=table_name) %}

{% if rel is not none %}
select
  station_id,
  date_start_use,
  date_end_use,
  calibration_factor
from {{ rel }}
{% else %}
select
  s.id as station_id,
  nullif(cf.date_start_use::text, '')::timestamptz as date_start_use,
  nullif(cf.date_end_use::text, '')::timestamptz as date_end_use,
  cf.calibration_factor::double precision as calibration_factor
from {{ ref('calibration_factors_seed') }} cf
join {{ ref('dim_stations') }} s
  on s.code = cf.station_code::text
{% endif %}
