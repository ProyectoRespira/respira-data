from __future__ import annotations

import logging
from pathlib import Path

import sqlparse
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

from pipelines.config.projects import ProjectConfig, is_safe_sql_identifier


PIPELINES_ROOT = Path(__file__).resolve().parents[1]
SQL_DIR = PIPELINES_ROOT / "sql"
OPS_AUDIT_SQL = SQL_DIR / "02_ops_audit.sql"
INFERENCE_TABLES_SQL = SQL_DIR / "03_inference_tables.sql"
logger = logging.getLogger(__name__)


def get_engine(settings) -> Engine:
    dsn = settings.database_dsn() if hasattr(settings, "database_dsn") else settings.DB_DSN
    if not dsn:
        raise ValueError("Database DSN is required.")
    return create_engine(dsn, pool_pre_ping=True)


def _split_sql(sql: str) -> list[str]:
    # sqlparse correctly handles semicolons inside string literals and comments.
    return [stmt.strip() for stmt in sqlparse.split(sql) if stmt and stmt.strip()]


def execute_sql_file(engine: Engine, path: str) -> None:
    sql_path = Path(path)
    if not sql_path.exists():
        raise FileNotFoundError(f"SQL file not found: {sql_path}")

    sql = sql_path.read_text(encoding="utf-8")
    statements = _split_sql(sql)

    with engine.begin() as connection:
        for statement in statements:
            connection.exec_driver_sql(statement.rstrip(";"))


def execute_statements(engine: Engine, statements: list[str]) -> None:
    with engine.begin() as connection:
        for statement in statements:
            connection.exec_driver_sql(statement.rstrip(";"))


def ensure_ops_audit_tables(engine: Engine) -> None:
    execute_sql_file(engine, str(OPS_AUDIT_SQL))


def _render_sql_template(path: Path, **kwargs: str) -> list[str]:
    # Callers MUST pass values that have been validated against
    # ``_SAFE_SQL_IDENTIFIER`` (see pipelines/config/projects.py). Because the
    # template substitutes bare identifiers into DDL, any unvalidated input
    # here would enable SQL injection.
    for key, value in kwargs.items():
        if not is_safe_sql_identifier(value):
            raise ValueError(
                f"Refusing to render SQL template {path.name}: unsafe identifier for {key}={value!r}"
            )
    sql = path.read_text(encoding="utf-8")
    rendered = sql.format(**kwargs)
    return _split_sql(rendered)


def ensure_project_inference_tables(engine: Engine, project: ProjectConfig) -> None:
    statements = _render_sql_template(
        INFERENCE_TABLES_SQL,
        schema_name=project.schema_name,
        inference_runs_table=project.inference_runs_table,
        inference_results_table=project.inference_results_table,
    )
    execute_statements(engine, statements)
