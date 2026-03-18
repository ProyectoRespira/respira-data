from __future__ import annotations

from pipelines.compat import flow, get_run_logger
from pipelines.config.projects import list_project_configs
from pipelines.config.settings import get_settings
from pipelines.tasks.db import ensure_ops_audit_tables, ensure_project_inference_tables, get_engine


@flow(name="warehouse_bootstrap")
def warehouse_bootstrap() -> None:
    logger = get_run_logger()
    settings = get_settings()
    engine = get_engine(settings)

    try:
        logger.info("Ensuring ops audit tables")
        ensure_ops_audit_tables(engine)

        for project in list_project_configs():
            logger.info("Ensuring inference tables for project_code=%s", project.project_code)
            ensure_project_inference_tables(engine, project)
    finally:
        engine.dispose()


if __name__ == "__main__":
    warehouse_bootstrap()
