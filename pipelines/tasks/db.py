from __future__ import annotations

import logging
import re
from pathlib import Path

from sqlalchemy import create_engine
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.exc import SQLAlchemyError

from pipelines.config.projects import ProjectConfig

PIPELINES_ROOT = Path(__file__).resolve().parents[1]
SQL_DIR = PIPELINES_ROOT / "sql"
OPS_AUDIT_SQL = SQL_DIR / "02_ops_audit.sql"
INFERENCE_TABLES_SQL = SQL_DIR / "03_inference_tables.sql"
logger = logging.getLogger(__name__)
_DOLLAR_QUOTE_RE = re.compile(r"\$(?:[A-Za-z_][A-Za-z0-9_]*)?\$")


def get_engine(settings) -> Engine:
    dsn = (
        settings.database_dsn()
        if hasattr(settings, "database_dsn")
        else settings.DB_DSN
    )
    if not dsn:
        raise ValueError("Database DSN is required.")
    return create_engine(dsn, pool_pre_ping=True)


def _split_sql_statements(sql: str) -> list[str]:
    statements: list[str] = []
    current: list[str] = []
    in_single_quote = False
    in_double_quote = False
    in_line_comment = False
    block_comment_depth = 0
    dollar_quote: str | None = None
    index = 0

    while index < len(sql):
        char = sql[index]
        following = sql[index + 1] if index + 1 < len(sql) else ""

        if in_line_comment:
            current.append(char)
            if char == "\n":
                in_line_comment = False
            index += 1
            continue

        if block_comment_depth:
            if char == "/" and following == "*":
                current.extend((char, following))
                block_comment_depth += 1
                index += 2
                continue
            if char == "*" and following == "/":
                current.extend((char, following))
                block_comment_depth -= 1
                index += 2
                continue
            current.append(char)
            index += 1
            continue

        if dollar_quote is not None:
            if sql.startswith(dollar_quote, index):
                current.append(dollar_quote)
                index += len(dollar_quote)
                dollar_quote = None
            else:
                current.append(char)
                index += 1
            continue

        if in_single_quote:
            current.append(char)
            if char == "'":
                if following == "'":
                    current.append(following)
                    index += 2
                    continue
                in_single_quote = False
            index += 1
            continue

        if in_double_quote:
            current.append(char)
            if char == '"':
                if following == '"':
                    current.append(following)
                    index += 2
                    continue
                in_double_quote = False
            index += 1
            continue

        if char == "-" and following == "-":
            current.extend((char, following))
            in_line_comment = True
            index += 2
            continue

        if char == "/" and following == "*":
            current.extend((char, following))
            block_comment_depth = 1
            index += 2
            continue

        if char == "'":
            current.append(char)
            in_single_quote = True
            index += 1
            continue

        if char == '"':
            current.append(char)
            in_double_quote = True
            index += 1
            continue

        if char == "$":
            match = _DOLLAR_QUOTE_RE.match(sql, index)
            if match is not None:
                dollar_quote = match.group(0)
                current.append(dollar_quote)
                index = match.end()
                continue

        if char == ";":
            statement = "".join(current).strip()
            if statement:
                statements.append(statement)
            current = []
            index += 1
            continue

        current.append(char)
        index += 1

    if in_single_quote or in_double_quote or block_comment_depth or dollar_quote:
        raise ValueError("SQL file contains an unterminated quoted string or comment.")

    statement = "".join(current).strip()
    if statement:
        statements.append(statement)
    return statements


def read_sql_statements(path: str | Path) -> list[str]:
    sql_path = Path(path)
    if not sql_path.exists():
        raise FileNotFoundError(f"SQL file not found: {sql_path}")

    sql = sql_path.read_text(encoding="utf-8")
    return _split_sql_statements(sql)


def execute_sql_file_on_connection(connection: Connection, path: str | Path) -> None:
    for statement in read_sql_statements(path):
        connection.exec_driver_sql(statement)


def execute_sql_file(engine: Engine, path: str) -> None:
    statements = read_sql_statements(path)

    with engine.begin() as connection:
        for statement in statements:
            connection.exec_driver_sql(statement)


def execute_statements(engine: Engine, statements: list[str]) -> None:
    with engine.begin() as connection:
        for statement in statements:
            connection.exec_driver_sql(statement)


def ensure_ops_audit_tables(engine: Engine, *, strict: bool = False) -> None:
    try:
        execute_sql_file(engine, str(OPS_AUDIT_SQL))
    except SQLAlchemyError as exc:
        if strict:
            raise
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
