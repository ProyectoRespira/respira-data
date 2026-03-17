{{ config(
  materialized='incremental',
  unique_key='code',
  incremental_strategy='merge'
) }}

with src as (
  select
    short_name as code,
    name,
    type,
    country,
    properties::jsonb as properties
  from {{ ref('organizations') }}
),

final as (
  select
    {{ surrogate_key_bigint(["code"]) }} as id,
    code,
    name,
    type,
    country,
    properties,
    now()::timestamptz as created_at,
    null::timestamptz as updated_at
  from src
)

select * from final
