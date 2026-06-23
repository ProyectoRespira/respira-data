with source_stations as (
  select
    sm.project_station_id as station_id,
    lower(coalesce(s.properties->>'source', '')) as source_name
  from {{ ref('int_air_quality_stations') }} s
  join {{ ref('int_station_id_map') }} sm
    on sm.core_station_id = s.id
)

select
  st.id,
  st.name,
  ss.source_name
from {{ ref('stations') }} st
join source_stations ss
  on ss.station_id = st.id
where ss.source_name = 'respira'
  and (
    st.name not like 'Respira: %'
    or st.name ~* '^Respira:\\s*Respira:\\s*'
  )
