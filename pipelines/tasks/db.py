from __future__ import annotations

import logging
from pathlib import Path

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

from pipelines.config.projects import ProjectConfig

PIPELINES_ROOT = Path(__file__).resolve().parents[1]
SQL_DIR = PIPELINES_ROOT / "sql"
OPS_AUDIT_SQL = SQL_DIR / "02_ops_audit.sql"
INFERENCE_TABLES_SQL = SQL_DIR / "03_inference_tables.sql"
logger = logging.getLogger(__name__)


def get_engine(settings) -> Engine:
    dsn = (
        settings.database_dsn()
        if hasattr(settings, "database_dsn")
        else settings.DB_DSN
    )
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


def _render_sql_template(path: Path, **kwargs: str) -> list[str]:
    sql = path.read_text(encoding="utf-8")
    rendered = sql.format(**kwargs)
    return [stmt.strip() for stmt in rendered.split(";") if stmt.strip()]


def ensure_project_inference_tables(engine: Engine, project: ProjectConfig) -> None:
    statements = _render_sql_template(
        INFERENCE_TABLES_SQL,
        schema_name=project.schema_name,
        inference_runs_table=project.inference_runs_table,
        inference_results_table=project.inference_results_table,
    )

    try:
        execute_statements(engine, statements)
    except SQLAlchemyError as exc:
        logger.warning(
            "Unable to ensure inference tables for project %s: %s",
            project.project_code,
            exc,
        )
