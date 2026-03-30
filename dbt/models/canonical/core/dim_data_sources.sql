{{ config(
  materialized='incremental',
  unique_key='name',
  incremental_strategy='merge'
) }}

with src as (
  select
    name,
    organization_code,
    type,
    active::boolean as active,
    properties::jsonb as properties
  from {{ ref('data_sources') }}
),

organizations as (
  select
    id as organization_id,
    code as organization_code
  from {{ ref('dim_organizations') }}
),

final as (
  select
    {{ surrogate_key_bigint(["src.name"]) }} as id,
    src.name,
    src.organization_code,
    organizations.organization_id,
    src.type,
    '{}'::jsonb as connection_info,
    src.properties,
    src.active,
    now()::timestamptz as created_at,
    null::timestamptz as updated_at
  from src
  join organizations
    on organizations.organization_code = src.organization_code
)

select * from final
