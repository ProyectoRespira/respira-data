create schema if not exists ops;

create table if not exists ops.dbt_run_audit (
    id uuid primary key,
    flow_run_id text not null,
    deployment text null,
    target text not null,
    git_sha text null,
    project_code text null,
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
create index if not exists idx_dbt_run_audit_project_code on ops.dbt_run_audit (project_code);

create table if not exists ops.inference_station_status (
    id uuid primary key,
    project_code text not null,
    inference_run_id uuid not null,
    station_id bigint not null,
    status text not null check (status in ('success', 'skipped', 'failed')),
    reason_code text null,
    reason_detail text null,
    duration_s int null,
    created_at timestamptz not null default now(),
    unique (project_code, inference_run_id, station_id)
);

create index if not exists idx_inference_station_status_project_code on ops.inference_station_status (project_code);
create index if not exists idx_inference_station_status_run_id on ops.inference_station_status (inference_run_id);
create index if not exists idx_inference_station_status_station on ops.inference_station_status (station_id);
create index if not exists idx_inference_station_status_status on ops.inference_station_status (status);
