{{ config(
  materialized='incremental',
  unique_key=['data_source_name', 'source_row_id'],
  incremental_strategy='merge',
  indexes=[
    {'columns': ['data_source_name', 'source_row_id'], 'unique': true},
    {'columns': ['extracted_at']},
    {'columns': ['data_source_name', 'station_code', 'cursor_id']}
  ]
) }}

{%- set sources_cfg = var('measurements_sources') -%}
{%- set ingest_batch_from = measurement_batch_extracted_at_from() -%}
{%- set selected_source_names = get_selected_measurement_sources(sources_cfg) -%}

with cutoff as (

  {% if ingest_batch_from is not none %}
  select cast({{ measurement_sql_string(ingest_batch_from) }} as timestamptz) as extracted_at_cutoff
  {% elif is_incremental() and not measurement_has_ingest_batch_scope() %}
  select coalesce(max(extracted_at), '1970-01-01'::timestamptz) as extracted_at_cutoff
  from {{ this }}
  where data_source_name = 'fiuna_airbyte'
  {% else %}
  select null::timestamptz as extracted_at_cutoff
  {% endif %}

),

source_rows as (

  {%- for source_name in selected_source_names %}
  {%- set cfg = sources_cfg[source_name] %}

    {{ measurement_timestamps_from_source(source_name, cfg) }}

    {%- if not loop.last %}
    union all
    {%- endif %}

  {%- endfor %}

),

fiuna_new_stations as (

  select distinct station_code
  from source_rows
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
    true as is_measured_at_valid
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
  join cutoff c
    on c.extracted_at_cutoff is not null
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
    null::boolean as is_measured_at_valid
  where false
  {% endif %}

),

fiuna_rows as (

  select
    false as is_anchor,
    source_row_id,
    extracted_at,
    data_source_name,
    station_code,
    cursor_id,
    measured_at_parsed,
    is_measured_at_valid
  from source_rows
  where data_source_name = 'fiuna_airbyte'

  union all

  select * from fiuna_anchor

),

fiuna_windowed as (

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
  from fiuna_rows

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
    end as time_impute_method
  from fiuna_windowed
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
    end as time_impute_method
  from source_rows
  where data_source_name <> 'fiuna_airbyte'

)

select * from fiuna_fixed
union all
select * from other_sources
