{{ config(
  materialized='incremental',
  unique_key='code',
  incremental_strategy='merge',
  indexes=[
    {'columns': ['code'], 'unique': true}
  ]
) }}

with stations as (
  select id as station_id, code as station_code
  from {{ ref('dim_stations') }}
),
variables as (
  select id as variable_id, code as variable_code
  from {{ ref('dim_variables') }}
),
data_sources as (
  select id as data_source_id, name as data_source_name
  from {{ ref('dim_data_sources') }}
),
candidates as (
  select *
  from {{ ref('int_streams_candidates') }}

  {% if is_incremental() %}
  where code not in (
    select code
    from {{ this }}
  )
  {% endif %}
),

joined as (
  select
    c.code,
    c.name,
    s.station_id,
    v.variable_id,
    ds.data_source_id
  from candidates c
  join stations s on s.station_code = c.station_code
  join variables v on v.variable_code = c.variable_code
  join data_sources ds on ds.data_source_name = c.data_source_name
),

final as (
  select
    {{ surrogate_key_bigint(["code"]) }} as id,
    station_id,
    variable_id,
    null::bigint as device_id,
    data_source_id,
    code,
    name,
    null::text as description,
    'active' as status,
    null::integer as expected_interval_seconds,
    '{}'::jsonb as properties,
    now()::timestamptz as created_at,
    null::timestamptz as updated_at
  from joined
)

select * from final
