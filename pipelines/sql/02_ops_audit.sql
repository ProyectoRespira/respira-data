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

create table if not exists ops.measurement_stream_state (
    data_source_name text not null,
    station_code text not null,
    variable_code text not null,
    last_measured_at_silver timestamptz not null,
    last_cursor_id bigint null,
    last_extracted_at timestamptz not null,
    last_source_row_id text not null,
    last_value_silver double precision not null,
    updated_at timestamptz not null default now(),
    primary key (data_source_name, station_code, variable_code)
);

create index if not exists idx_measurement_stream_state_updated_at
    on ops.measurement_stream_state (updated_at);
create index if not exists idx_measurement_stream_state_data_source_watermark
    on ops.measurement_stream_state (
        data_source_name,
        last_measured_at_silver,
        last_extracted_at
    );

comment on table ops.measurement_stream_state is
'Latest successfully published carry-forward state for each measurement stream.';
comment on column ops.measurement_stream_state.data_source_name is
'Canonical data source name; part of the stream-state primary key.';
comment on column ops.measurement_stream_state.station_code is
'Canonical station code; part of the stream-state primary key.';
comment on column ops.measurement_stream_state.variable_code is
'Canonical variable code; part of the stream-state primary key.';
comment on column ops.measurement_stream_state.last_measured_at_silver is
'Latest published canonical measurement timestamp for the stream.';
comment on column ops.measurement_stream_state.last_cursor_id is
'Latest published source cursor when the upstream source has one; null for non-cursor sources.';
comment on column ops.measurement_stream_state.last_extracted_at is
'Latest published source extraction timestamp used in the deterministic stream watermark.';
comment on column ops.measurement_stream_state.last_source_row_id is
'Latest published source-row identifier used to break watermark ties deterministically.';
comment on column ops.measurement_stream_state.last_value_silver is
'Latest published canonical value for the stream after validation and imputation.';
comment on column ops.measurement_stream_state.updated_at is
'Warehouse timestamp for when the persisted state last changed; idempotent refreshes should leave it unchanged.';

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
