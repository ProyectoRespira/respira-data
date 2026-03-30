{{ config(materialized='view') }}

with seed as (
  select region_code::text as region_code, name::text as name, bbox::text as bbox
  from {{ ref('regions_seed') }}
),

seed_count as (
  select count(*) as cnt from seed
),

stations as (
  select code, name, latitude, longitude
  from {{ ref('int_air_quality_stations') }}
),

generated as (
  select
    s.code as region_code,
    s.name as name,
    case
      when s.latitude is null or s.longitude is null then null
      else (
        round((s.longitude - d.lon_delta)::numeric, 6)::text || ',' ||
        round((s.latitude - d.lat_delta)::numeric, 6)::text || ',' ||
        round((s.longitude + d.lon_delta)::numeric, 6)::text || ',' ||
        round((s.latitude + d.lat_delta)::numeric, 6)::text
      )
    end as bbox
  from stations s
  cross join lateral (
    select
      5.0 / 111.0 as lat_delta,
      case
        when cos(radians(s.latitude)) = 0 then 0.0
        else 5.0 / (111.0 * cos(radians(s.latitude)))
      end as lon_delta
  ) d
),

combined as (
  select
    seed.region_code,
    seed.name,
    seed.bbox
  from seed
  cross join seed_count sc
  where sc.cnt > 0

  union all

  select
    coalesce(seed.region_code, g.region_code) as region_code,
    coalesce(seed.name, g.name) as name,
    coalesce(seed.bbox, g.bbox) as bbox
  from generated g
  left join seed
    on seed.region_code = g.region_code
  cross join seed_count sc
  where sc.cnt = 0

  union all

  select
    seed.region_code,
    seed.name,
    seed.bbox
  from seed
  left join generated g
    on g.region_code = seed.region_code
  cross join seed_count sc
  where g.region_code is null and sc.cnt = 0
)

select distinct
  region_code,
  name,
  bbox
from combined
