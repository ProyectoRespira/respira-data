{{ config(
  materialized='incremental',
  unique_key='code',
  incremental_strategy='merge'
) }}

with candidates as (
  select * from {{ ref('int_stations_candidates') }}
),

final as (
  select
    {{ surrogate_key_bigint(["code"]) }} as id,
    code,
    name,
    description,
    latitude,
    longitude,
    elevation_m,
    status,
    properties,
    is_pattern_station,
    now()::timestamptz as created_at,
    null::timestamptz as updated_at
  from candidates
)

select * from final
