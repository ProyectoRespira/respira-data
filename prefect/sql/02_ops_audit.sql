create schema if not exists ops;
create schema if not exists "respira-gold";

create table if not exists ops.dbt_run_audit (
    id uuid primary key,
    flow_run_id text not null,
    deployment text null,
    target text not null,
    git_sha text null,
    command text not null,
    selector text null,
    started_at timestamptz not null,
    ended_at timestamptz not null,
    duration_s int not null,
    status text not null check (status in ('success', 'failed', 'cancelled')),
    models_passed int not null default 0,
    models_failed int not null default 0,
    tests_passed int not null default 0,
    tests_failed int not null default 0,
    error_summary text null,
    run_results_json jsonb null,
    created_at timestamptz not null default now()
);

create index if not exists idx_dbt_run_audit_started_at on ops.dbt_run_audit (started_at);
create index if not exists idx_dbt_run_audit_status on ops.dbt_run_audit (status);
create index if not exists idx_dbt_run_audit_selector on ops.dbt_run_audit (selector);

create table if not exists "respira-gold".inference_runs (
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
    status text not null check (status in ('success', 'failed', 'cancelled')),
    stations_total int not null default 0,
    stations_success int not null default 0,
    stations_skipped int not null default 0,
    stations_failed int not null default 0,
    error_summary text null,
    created_at timestamptz not null default now()
);

create index if not exists idx_inference_runs_as_of on "respira-gold".inference_runs (as_of);
create index if not exists idx_inference_runs_status on "respira-gold".inference_runs (status);

create table if not exists ops.inference_station_status (
    id uuid primary key,
    inference_run_id uuid not null references "respira-gold".inference_runs(id),
    station_id bigint not null,
    status text not null check (status in ('success', 'skipped', 'failed')),
    reason_code text null,
    reason_detail text null,
    duration_s int null,
    created_at timestamptz not null default now(),
    unique (inference_run_id, station_id)
);

create index if not exists idx_inference_station_status_run_id on ops.inference_station_status (inference_run_id);
create index if not exists idx_inference_station_status_station on ops.inference_station_status (station_id);
create index if not exists idx_inference_station_status_status on ops.inference_station_status (status);

create table if not exists "respira-gold".inference_results (
    id uuid primary key,
    inference_run_id uuid not null references "respira-gold".inference_runs(id),
    station_id bigint not null,
    as_of timestamptz not null,
    horizon_hours int not null check (horizon_hours in (6, 12)),
    model_version text not null,
    predictions_json jsonb not null,
    created_at timestamptz not null default now(),
    unique (inference_run_id, station_id, horizon_hours)
);

create index if not exists idx_inference_results_run_id on "respira-gold".inference_results (inference_run_id);
create index if not exists idx_inference_results_station on "respira-gold".inference_results (station_id);
create index if not exists idx_inference_results_as_of on "respira-gold".inference_results (as_of);
