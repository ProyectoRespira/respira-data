{{ config(
  materialized='incremental',
  unique_key=['project_code', 'organization_code'],
  incremental_strategy='merge'
) }}

with src as (
  select
    project_code,
    organization_code
  from {{ ref('project_organizations') }}
),

projects as (
  select
    id as project_id,
    code as project_code
  from {{ ref('dim_projects') }}
),

organizations as (
  select
    id as organization_id,
    code as organization_code
  from {{ ref('dim_organizations') }}
),

final as (
  select
    {{ surrogate_key_bigint(["src.project_code", "src.organization_code"]) }} as id,
    projects.project_id,
    organizations.organization_id,
    src.project_code,
    src.organization_code,
    now()::timestamptz as created_at
  from src
  join projects
    on projects.project_code = src.project_code
  join organizations
    on organizations.organization_code = src.organization_code
)

select * from final
