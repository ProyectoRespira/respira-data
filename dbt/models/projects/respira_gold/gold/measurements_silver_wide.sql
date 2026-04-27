{{ config(materialized='view') }}

{%- set var_codes = dbt_utils.get_column_values(ref('dim_variables'), 'code') -%}

with base as (

  select
    m.timestamp,
    m.value_parsed,

    st.code as station_code,
    dv.code as variable_code,
    ds.name as data_source_name,
    organizations.name as organization_name

  from {{ ref('fct_measurements_silver') }} m
  join {{ ref('int_project_streams') }} project_streams
    on project_streams.id = m.stream_id
  join {{ ref('dim_stations') }} st
    on st.id = project_streams.station_id
  join {{ ref('dim_variables') }} dv
    on dv.id = project_streams.variable_id
  join {{ ref('dim_data_sources') }} ds
    on ds.id = project_streams.data_source_id
  join {{ ref('dim_organizations') }} organizations
    on organizations.id = ds.organization_id
),

project as (
  select code as project_code
  from {{ ref('dim_projects') }}
  where code = 'respira_gold'
)

select
  p.project_code as project,
  b.station_code as station,
  b.timestamp as timestamp,

  b.data_source_name as stream,
  b.organization_name as organization,

  {%- for v in var_codes %}
  max(case when b.variable_code = '{{ v }}' then b.value_parsed end) as {{ v }}{% if not loop.last %},{% endif %}
  {%- endfor %}

from base b
cross join project p

group by
  p.project_code,
  b.station_code,
  b.timestamp,
  b.data_source_name,
  b.organization_name
