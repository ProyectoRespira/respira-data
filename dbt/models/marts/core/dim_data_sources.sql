{{ config(
  materialized='incremental',
  unique_key='name',
  incremental_strategy='merge'
) }}

with src as (
  select
    name,
    type,
    active::boolean as active
  from {{ ref('data_sources') }}
),

final as (
  select
    {{ surrogate_key_bigint(["name"]) }} as id,
    name,
    type,
    '{}'::jsonb as connection_info,
    active,
    now()::timestamptz as created_at,
    null::timestamptz as updated_at
  from src
)

select * from final
