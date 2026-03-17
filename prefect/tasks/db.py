from __future__ import annotations

import logging
from pathlib import Path

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

from config.projects import ProjectConfig


PREFECT_ROOT = Path(__file__).resolve().parents[1]
OPS_AUDIT_SQL = PREFECT_ROOT / "sql" / "02_ops_audit.sql"
logger = logging.getLogger(__name__)


def get_engine(settings) -> Engine:
    dsn = settings.database_dsn() if hasattr(settings, "database_dsn") else settings.DB_DSN
    if not dsn:
        raise ValueError("Database DSN is required.")
    return create_engine(dsn, pool_pre_ping=True)


def execute_sql_file(engine: Engine, path: str) -> None:
    sql_path = Path(path)
    if not sql_path.exists():
        raise FileNotFoundError(f"SQL file not found: {sql_path}")

    sql = sql_path.read_text(encoding="utf-8")
    statements = [stmt.strip() for stmt in sql.split(";") if stmt.strip()]

    with engine.begin() as connection:
        for statement in statements:
            connection.exec_driver_sql(statement)


def execute_statements(engine: Engine, statements: list[str]) -> None:
    with engine.begin() as connection:
        for statement in statements:
            connection.exec_driver_sql(statement)


def ensure_ops_audit_tables(engine: Engine) -> None:
    try:
        execute_sql_file(engine, str(OPS_AUDIT_SQL))
    except SQLAlchemyError as exc:
        logger.warning("Unable to ensure ops audit tables: %s", exc)


def ensure_project_inference_tables(engine: Engine, project: ProjectConfig) -> None:
    schema_name = project.schema_name
    inference_runs = project.inference_runs_table
    inference_results = project.inference_results_table

    statements = [
        f"create schema if not exists {schema_name}",
        f"""
        create table if not exists {inference_runs} (
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
        )
        """,
        f"""
        create index if not exists idx_{schema_name}_inference_runs_as_of
        on {inference_runs} (as_of)
        """,
        f"""
        create index if not exists idx_{schema_name}_inference_runs_status
        on {inference_runs} (status)
        """,
        f"""
        create table if not exists {inference_results} (
            id uuid primary key,
            inference_run_id uuid not null references {inference_runs}(id),
            station_id bigint not null,
            as_of timestamptz not null,
            horizon_hours int not null check (horizon_hours in (6, 12)),
            model_version text not null,
            predictions_json jsonb not null,
            created_at timestamptz not null default now(),
            unique (inference_run_id, station_id, horizon_hours)
        )
        """,
        f"""
        create index if not exists idx_{schema_name}_inference_results_run_id
        on {inference_results} (inference_run_id)
        """,
        f"""
        create index if not exists idx_{schema_name}_inference_results_station
        on {inference_results} (station_id)
        """,
        f"""
        create index if not exists idx_{schema_name}_inference_results_as_of
        on {inference_results} (as_of)
        """,
    ]

    try:
        execute_statements(engine, statements)
    except SQLAlchemyError as exc:
        logger.warning("Unable to ensure inference tables for project %s: %s", project.project_code, exc)
