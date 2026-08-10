{{ config(materialized='view') }}

with regions as (
  select
    region_code,
    case
      when bbox is null then null
      else (
        split_part(bbox, ',', 2)::double precision
        + split_part(bbox, ',', 4)::double precision
      ) / 2.0
    end as bbox_center_latitude,
    case
      when bbox is null then null
      else (
        split_part(bbox, ',', 1)::double precision
        + split_part(bbox, ',', 3)::double precision
      ) / 2.0
    end as bbox_center_longitude
  from {{ ref('int_regions') }}
),

air_quality_station_centroids as (
  select
    station_map.region_code,
    avg(stations.latitude) as latitude,
    avg(stations.longitude) as longitude
  from {{ ref('int_station_regions') }} station_map
  join {{ ref('int_air_quality_stations') }} stations
    on stations.code = station_map.station_code
  where stations.latitude is not null
    and stations.longitude is not null
  group by 1
),

region_anchors as (
  select
    regions.region_code,
    coalesce(
      station_centroids.latitude,
      regions.bbox_center_latitude
    ) as latitude,
    coalesce(
      station_centroids.longitude,
      regions.bbox_center_longitude
    ) as longitude
  from regions
  left join air_quality_station_centroids station_centroids
    on station_centroids.region_code = regions.region_code
),

weather_stations as (
  select code, latitude, longitude
  from {{ ref('int_weather_stations') }}
  where latitude is not null
    and longitude is not null
),

ranked_weather_stations as (
  select
    region_anchors.region_code,
    weather_stations.code as station_code,
    row_number() over (
      partition by region_anchors.region_code
      order by
        power(weather_stations.latitude - region_anchors.latitude, 2)
        + power(weather_stations.longitude - region_anchors.longitude, 2),
        weather_stations.code
    ) as rn
  from region_anchors
  cross join weather_stations
  where region_anchors.latitude is not null
    and region_anchors.longitude is not null
)

select
  region_code,
  station_code
from ranked_weather_stations
where rn = 1
