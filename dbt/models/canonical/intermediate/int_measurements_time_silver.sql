{{ config(
  materialized='incremental',
  unique_key=['source_row_id', 'variable_code'],
  incremental_strategy='merge',
  indexes=[
    {'columns': ['source_row_id', 'variable_code'], 'unique': true},
    {'columns': ['extracted_at']},
    {'columns': ['data_source_name', 'station_code', 'cursor_id']}
  ]
) }}

with cutoff as (

  {% if is_incremental() %}
  select coalesce(max(extracted_at), '1970-01-01'::timestamptz) as extracted_at_cutoff
  from {{ this }}
  {% else %}
  select '1970-01-01'::timestamptz as extracted_at_cutoff
  {% endif %}

),

base_rows as (

  select
    false as is_anchor,
    source_row_id,
    extracted_at,
    data_source_name,
    station_code,
    cursor_id,
    measured_at_parsed,
    is_measured_at_valid,

    variable_code,
    value_raw,
    value_parsed
  from {{ ref('int_measurements_long') }}

  {% if is_incremental() %}
  where extracted_at >= (select extracted_at_cutoff from cutoff)
  {% endif %}

),

fiuna_new_stations as (

  select distinct station_code
  from base_rows
  where data_source_name = 'fiuna_airbyte'

),

fiuna_anchor as (

  {% if is_incremental() %}
  select
    true as is_anchor,
    null::text as source_row_id,
    null::timestamptz as extracted_at,
    'fiuna_airbyte'::text as data_source_name,
    a.station_code,
    a.cursor_id,
    a.measured_at_silver as measured_at_parsed,
    true as is_measured_at_valid,

    null::text as variable_code,
    null::text as value_raw,
    null::double precision as value_parsed
  from (
    select distinct on (station_code)
      station_code,
      cursor_id,
      measured_at_silver
    from {{ this }}
    where data_source_name = 'fiuna_airbyte'
      and is_measured_at_valid
      and extracted_at < (select extracted_at_cutoff from cutoff)
    order by station_code, cursor_id desc, extracted_at desc
  ) a
  join fiuna_new_stations s
    on s.station_code = a.station_code
  {% else %}
  select
    true as is_anchor,
    null::text as source_row_id,
    null::timestamptz as extracted_at,
    'fiuna_airbyte'::text as data_source_name,
    null::text as station_code,
    null::bigint as cursor_id,
    null::timestamptz as measured_at_parsed,
    null::boolean as is_measured_at_valid,

    null::text as variable_code,
    null::text as value_raw,
    null::double precision as value_parsed
  where false
  {% endif %}

),

m as (

  select * from base_rows
  union all
  select * from fiuna_anchor

),

fiuna as (

  select
    *,
    max(case when is_measured_at_valid then measured_at_parsed end)
      over (
        partition by station_code
        order by cursor_id
        rows between unbounded preceding and current row
      ) as last_valid_ts,

    max(case when is_measured_at_valid then cursor_id end)
      over (
        partition by station_code
        order by cursor_id
        rows between unbounded preceding and current row
      ) as last_valid_id
  from m
  where data_source_name = 'fiuna_airbyte'

),

fiuna_fixed as (

  select
    source_row_id,
    extracted_at,
    data_source_name,
    station_code,
    cursor_id,
    measured_at_parsed,
    is_measured_at_valid,

    case
      when is_measured_at_valid then measured_at_parsed
      when last_valid_ts is not null and last_valid_id is not null
        then last_valid_ts + ((cursor_id - last_valid_id) * interval '5 minutes')
      else null
    end as measured_at_silver,

    case
      when is_measured_at_valid then false
      when last_valid_ts is not null and last_valid_id is not null then true
      else false
    end as is_time_imputed,

    case
      when is_measured_at_valid then null
      when last_valid_ts is not null and last_valid_id is not null then 'fiuna_id_5min'
      else 'unfixable_no_anchor'
    end as time_impute_method,

    variable_code,
    value_raw,
    value_parsed
  from fiuna
  where not is_anchor

),

other_sources as (

  select
    source_row_id,
    extracted_at,
    data_source_name,
    station_code,
    cursor_id,
    measured_at_parsed,
    is_measured_at_valid,

    case
      when is_measured_at_valid then measured_at_parsed
      else null
    end as measured_at_silver,

    false as is_time_imputed,

    case
      when is_measured_at_valid then null
      else 'invalid_timestamp_no_imputation'
    end as time_impute_method,

    variable_code,
    value_raw,
    value_parsed
  from m
  where data_source_name <> 'fiuna_airbyte'

)

select * from fiuna_fixed
union all
select * from other_sources
