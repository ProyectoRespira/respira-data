{{ config(materialized='view') }}

{%- set var_codes = dbt_utils.get_column_values(ref('variables'), 'code') -%}

with base as (

  select
    m.timestamp,
    m.value_parsed,

    st.code as station_code,
    dv.code as variable_code,
    ds.name as data_source_name

  from {{ ref('fct_measurements_silver') }} m
  join {{ ref('dim_streams') }} s
    on s.id = m.stream_id
  join {{ ref('dim_stations') }} st
    on st.id = s.station_id
  join {{ ref('dim_variables') }} dv
    on dv.id = s.variable_id
  join {{ ref('dim_data_sources') }} ds
    on ds.id = s.data_source_id
),

project_one as (
  -- hoy tu repo parece asumir 1 solo proyecto “activo”
  select code as project_code
  from {{ ref('projects') }}
  where active::boolean is true
  order by id
  limit 1
)

select
  p.project_code as project,
  b.station_code as station,
  b.timestamp as timestamp,

  -- "stream": en tu caso, el stream agrupador para el wide es el data source
  b.data_source_name as stream,

  -- organization: mapeo directo por data_source_name (hoy no tenés esta relación modelada en dim_data_sources)
  o.name as organization,

  {%- for v in var_codes %}
  max(case when b.variable_code = '{{ v }}' then b.value_parsed end) as {{ v }}{% if not loop.last %},{% endif %}
  {%- endfor %}

from base b
cross join project_one p
left join {{ ref('organizations') }} o
  on o.short_name = case
    when b.data_source_name = 'fiuna_airbyte' then 'fiuna'
    when b.data_source_name = 'airelibre_airbyte' then 'airelibre'
    when b.data_source_name = 'meteostat_airbyte' then 'meteostat'
    else null
  end

group by
  p.project_code,
  b.station_code,
  b.timestamp,
  b.data_source_name,
  o.name
