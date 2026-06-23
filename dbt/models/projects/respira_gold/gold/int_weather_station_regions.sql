{{ config(materialized='view') }}

with seed as (
  select station_code::text as station_code, region_code::text as region_code
  from {{ ref('station_region_seed') }}
),

weather as (
  select code, latitude, longitude
  from {{ ref('int_weather_stations') }}
),

bbox_regions as (
  select
    region_code,
    split_part(bbox, ',', 1)::double precision as min_lon,
    split_part(bbox, ',', 2)::double precision as min_lat,
    split_part(bbox, ',', 3)::double precision as max_lon,
    split_part(bbox, ',', 4)::double precision as max_lat,
    abs(
      (split_part(bbox, ',', 3)::double precision - split_part(bbox, ',', 1)::double precision)
      * (split_part(bbox, ',', 4)::double precision - split_part(bbox, ',', 2)::double precision)
    ) as bbox_area
  from {{ ref('int_regions') }}
  where bbox is not null
),

bbox_matches as (
  select
    w.code as station_code,
    r.region_code,
    row_number() over (
      partition by w.code
      order by
        power(
          w.longitude - greatest(r.min_lon, least(w.longitude, r.max_lon)),
          2
        ) + power(
          w.latitude - greatest(r.min_lat, least(w.latitude, r.max_lat)),
          2
        ) asc,
        r.bbox_area asc,
        r.region_code asc
    ) as rn
  from weather w
  join bbox_regions r
    on w.latitude is not null
   and w.longitude is not null
),

bbox_assignment as (
  select
    station_code,
    region_code
  from bbox_matches
  where rn = 1
)

select
  w.code as station_code,
  coalesce(
    seed.region_code,
    bbox_assignment.region_code,
    '{{ var('default_region_code', 'GRAN_ASUNCION') }}'
  ) as region_code
from weather w
left join seed
  on seed.station_code = w.code
left join bbox_assignment
  on bbox_assignment.station_code = w.code
