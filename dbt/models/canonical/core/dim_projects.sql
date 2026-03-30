{{ config(
  materialized='incremental',
  unique_key='code',
  incremental_strategy='merge'
) }}

with src as (
  select
    id as source_id,
    owner_organization_code,
    code,
    name,
    description,
    default_timezone,
    active::boolean as active
  from {{ ref('projects') }}
),

organizations as (
  select
    id as owner_organization_id,
    code as owner_organization_code
  from {{ ref('dim_organizations') }}
),

final as (
  select
    {{ surrogate_key_bigint(["src.code"]) }} as id,
    src.source_id,
    src.code,
    src.name,
    src.description,
    src.default_timezone,
    src.active,
    organizations.owner_organization_id,
    src.owner_organization_code,
    now()::timestamptz as created_at,
    null::timestamptz as updated_at
  from src
  join organizations
    on organizations.owner_organization_code = src.owner_organization_code
)

select * from final
