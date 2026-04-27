{{ config(
  materialized='incremental',
  unique_key=['project_code', 'data_source_name'],
  incremental_strategy='merge'
) }}

with src as (
  select
    project_code,
    data_source_name
  from {{ ref('project_data_sources') }}
),

projects as (
  select
    id as project_id,
    code as project_code
  from {{ ref('dim_projects') }}
),

data_sources as (
  select
    id as data_source_id,
    name as data_source_name
  from {{ ref('dim_data_sources') }}
),

final as (
  select
    {{ surrogate_key_bigint(["src.project_code", "src.data_source_name"]) }} as id,
    projects.project_id,
    data_sources.data_source_id,
    src.project_code,
    src.data_source_name,
    now()::timestamptz as created_at
  from src
  join projects
    on projects.project_code = src.project_code
  join data_sources
    on data_sources.data_source_name = src.data_source_name
)

select * from final
