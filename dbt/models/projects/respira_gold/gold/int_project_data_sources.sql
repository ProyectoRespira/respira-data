{{ config(materialized='view') }}

select
  bridge.project_id,
  bridge.project_code,
  ds.id as data_source_id,
  ds.name as data_source_name,
  ds.organization_id
from {{ ref('bridge_project_data_sources') }} bridge
join {{ ref('dim_data_sources') }} ds
  on ds.id = bridge.data_source_id
where bridge.project_code = 'respira_gold'
