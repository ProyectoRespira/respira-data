from __future__ import annotations

from sqlalchemy.engine import make_url

from pipelines.compat import flow, get_run_logger
from pipelines.config.settings import get_settings
from pipelines.tasks.db import get_engine
from pipelines.tasks.measurement_stream_state import (
    MeasurementStreamStateBootstrapResult,
    bootstrap_measurement_stream_state,
)


@flow(name="measurement_stream_state_bootstrap")
def measurement_stream_state_bootstrap(
    statement_timeout_seconds: int | None = None,
) -> MeasurementStreamStateBootstrapResult:
    logger = get_run_logger()
    settings = get_settings()
    dsn = settings.database_dsn()
    database_url = make_url(dsn)
    timeout_seconds = (
        settings.MEASUREMENT_STREAM_STATE_BOOTSTRAP_TIMEOUT_S
        if statement_timeout_seconds is None
        else statement_timeout_seconds
    )

    logger.info(
        "Bootstrapping measurement stream state on host=%s port=%s database=%s",
        database_url.host,
        database_url.port,
        database_url.database,
    )

    engine = get_engine(settings)
    try:
        result = bootstrap_measurement_stream_state(
            engine,
            statement_timeout_seconds=timeout_seconds,
        )
        logger.info(
            "Measurement stream state bootstrap completed: "
            "published_streams=%s candidates=%s inserted=%s updated=%s "
            "unchanged=%s affected=%s missing=%s extra=%s mismatched=%s",
            result["published_streams"],
            result["candidates"],
            result["inserted"],
            result["updated"],
            result["unchanged"],
            result["affected"],
            result["missing"],
            result["extra"],
            result["mismatched"],
        )
        return result
    finally:
        engine.dispose()


if __name__ == "__main__":
    measurement_stream_state_bootstrap()
