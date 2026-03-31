# Tables in respira_gold

## Backend contract tables

Table respira_gold.regions {
  id integer [primary key]
  name varchar
  region_code varchar [unique]
  bbox varchar
  has_weather_data bool
  has_pattern_data bool
  
}

Table respira_gold.stations {
  id integer [primary key]
  name varchar 
  latitude float
  longitude float
  region_id varchar [ref: > regions.id]
  is_station_on bool
  is_pattern_station bool
}

Table respira_gold.weather_stations {
  id integer [primary key]
  name varchar
  latitude float
  longitude float
  region_id varchar [ref: > regions.id]
}

Table respira_gold.station_readings_gold {
  id serial [primary key]
  station_id integer [ref: > stations.id]
  airnow_id integer [ref: - airnow_readings_silver.id]
  date_localtime timestamptz 
  pm_calibrated bool
  pm1 float
  pm2_5 float
  pm10 float
  pm2_5_avg_6h float
  pm2_5_max_6h float
  pm2_5_skew_6h float
  pm2_5_std_6h float
  aqi_pm2_5 float
  aqi_pm10 float
  aqi_level integer
  aqi_pm2_5_max_24h float
  aqi_pm2_5_skew_24h float
  aqi_pm2_5_std_24h float

  Indexes {
    (station_id, date_localtime) [unique]
  }

  Note {
    " Frequency for readings should be 1 hour"
  }

}

Table respira_gold.inference_runs {
  id uuid [primary key]
  flow_run_id text
  deployment text
  as_of timestamptz
  window_hours integer
  min_points integer
  model_6h_version text
  model_12h_version text
  model_6h_path text
  model_12h_path text
  started_at timestamptz
  ended_at timestamptz
  duration_s integer
  status text
  stations_total integer
  stations_success integer
  stations_skipped integer
  stations_failed integer
  error_summary text
  created_at timestamptz
}

Table respira_gold.inference_results {
  id uuid [primary key]
  inference_run_id uuid [ref: > inference_runs.id]
  station_id integer [ref: > stations.id]
  forecast_6h jsonb
  forecast_12h jsonb
  aqi_input jsonb
  created_at timestamptz
}

### Internal (not part of backend contract)
- `station_inference_features`: wide feature set for inference (station + region + weather).

## Implementation notes (dbt)

### Region generation
- If `regions_seed` is empty, each air-quality station becomes its own region.
- `station_region_seed` can override station → region assignments (and can include weather stations too).
- `bbox` format: `min_lon,min_lat,max_lon,max_lat` (degrees).

### Calibration factors
- If an external `calibration_factors` table exists (schema configurable via `calibration_factors_schema`), dbt reads from it.
- Otherwise dbt falls back to `calibration_factors_seed` (seed uses `station_code`, joined to `dim_stations`).

### Inference tables
- The dbt models for `inference_runs` and `inference_results` are still stubbed and disabled by default.
- The production-shaped tables are created and maintained by the inference pipeline SQL in [`pipelines/sql/03_inference_tables.sql`](/home/fer-dev/projects/respira/respira-data/pipelines/sql/03_inference_tables.sql).
- Backend code should treat `run_date` as an application alias over `as_of` when preserving the existing API response shape.
