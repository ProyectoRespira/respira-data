{{ config(materialized='view') }}

select *
from {{ ref('stg_fiuna_measurements_repaired') }}
