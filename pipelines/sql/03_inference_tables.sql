create schema if not exists {schema_name};

create table if not exists {inference_runs_table} (
    id uuid primary key,
    flow_run_id text not null,
    deployment text null,
    as_of timestamptz not null,
    window_hours int not null,
    min_points int not null,
    model_6h_version text not null,
    model_12h_version text not null,
    model_6h_path text null,
    model_12h_path text null,
    started_at timestamptz not null,
    ended_at timestamptz null,
    duration_s int null,
    status text not null check (status in ('running', 'success', 'failed', 'cancelled')),
    stations_total int not null default 0,
    stations_success int not null default 0,
    stations_skipped int not null default 0,
    stations_failed int not null default 0,
    error_summary text null,
    created_at timestamptz not null default now()
);

create index if not exists idx_{schema_name}_inference_runs_as_of
on {inference_runs_table} (as_of);

create index if not exists idx_{schema_name}_inference_runs_status
on {inference_runs_table} (status);

create table if not exists {inference_results_table} (
    id uuid primary key,
    inference_run_id uuid not null references {inference_runs_table}(id),
    station_id bigint not null,
    forecast_6h jsonb not null,
    forecast_12h jsonb not null,
    aqi_input jsonb not null,
    created_at timestamptz not null default now(),
    unique (inference_run_id, station_id)
);

create index if not exists idx_{schema_name}_inference_results_run_id
on {inference_results_table} (inference_run_id);

create index if not exists idx_{schema_name}_inference_results_station
on {inference_results_table} (station_id);
