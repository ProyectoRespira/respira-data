from __future__ import annotations

import logging
from pathlib import Path

from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine


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


def ensure_ops_audit_tables(engine: Engine) -> None:
    try:
        execute_sql_file(engine, str(OPS_AUDIT_SQL))
    except SQLAlchemyError as exc:
        # Audit is operational metadata. If we cannot create it due to permissions,
        # data pipelines may still run and we skip audit persistence.
        logger.warning("Unable to ensure ops audit tables: %s", exc)
        return
