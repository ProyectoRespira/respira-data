{{ config(
  materialized='incremental',
  unique_key='code',
  incremental_strategy='merge'
) }}

with src as (
  select
    code,
    name,
    unit_symbol,
    data_type,
    category
  from {{ ref('variables') }}
),

final as (
  select
    {{ surrogate_key_bigint(["code"]) }} as id,
    code,
    name,
    null::text as description,
    unit_symbol,
    null::text as unit_name,
    null::text as unit_definition_uri,
    data_type,
    category,
    null::text as standard_definition_uri,
    '{}'::jsonb as properties,
    now()::timestamptz as created_at,
    null::timestamptz as updated_at
  from src
)

select * from final
