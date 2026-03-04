from __future__ import annotations

import sys
from pathlib import Path

PREFECT_ROOT = Path(__file__).resolve().parents[1]
if str(PREFECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PREFECT_ROOT))

from compat import flow, get_run_logger
from config.settings import get_settings
from tasks.db import execute_sql_file, get_engine


@flow(name="warehouse_bootstrap")
def warehouse_bootstrap() -> None:
    logger = get_run_logger()
    settings = get_settings()
    engine = get_engine(settings)

    bootstrap_files = [
        PREFECT_ROOT / "sql" / "01_schema.sql",
        PREFECT_ROOT / "sql" / "02_ops_audit.sql",
    ]

    try:
        for sql_file in bootstrap_files:
            if sql_file.name == "01_schema.sql" and not sql_file.exists():
                logger.info("Skipping optional bootstrap SQL: %s", sql_file)
                continue

            if not sql_file.exists():
                raise FileNotFoundError(f"Required bootstrap SQL file not found: {sql_file}")

            logger.info("Executing bootstrap SQL: %s", sql_file)
            execute_sql_file(engine, str(sql_file))
    finally:
        engine.dispose()


if __name__ == "__main__":
    warehouse_bootstrap()
