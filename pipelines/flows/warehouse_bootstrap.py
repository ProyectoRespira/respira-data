from __future__ import annotations

from pipelines.compat import flow, get_run_logger
from pipelines.config.projects import list_project_configs
from pipelines.config.settings import get_settings
from pipelines.tasks.db import (
    ensure_ops_audit_tables,
    ensure_project_inference_tables,
    ensure_station_overrides_table,
    get_engine,
)


@flow(name="warehouse_bootstrap")
def warehouse_bootstrap() -> None:
    logger = get_run_logger()
    settings = get_settings()
    engine = get_engine(settings)

    try:
        logger.info("Ensuring ops audit tables")
        ensure_ops_audit_tables(engine, strict=True)

        for project in list_project_configs():
            logger.info(
                "Ensuring inference tables for project_code=%s", project.project_code
            )
            ensure_project_inference_tables(engine, project)

        # Read by int_station_status_overrides through the respira_webapp dbt
        # source, and written by the respira-webapp backoffice. Provisioned
        # here because this pipeline depends on it at runtime; a failure raises
        # rather than warns, so the flow cannot report success while leaving
        # the pipeline pointed at a missing relation.
        logger.info("Ensuring respira_gold.station_overrides")
        ensure_station_overrides_table(engine, backend_role=settings.BACKEND_DB_ROLE)
    finally:
        engine.dispose()


if __name__ == "__main__":
    warehouse_bootstrap()
