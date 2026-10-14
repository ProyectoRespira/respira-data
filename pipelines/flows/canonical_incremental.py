from __future__ import annotations

import subprocess
from pathlib import Path

from sqlalchemy import inspect, text

from pipelines.compat import flow, get_flow_context, get_run_logger
from pipelines.config.selectors import (
    SELECTOR_CANONICAL_BATCH_SMOKE_TESTS,
    SELECTOR_CANONICAL_INCREMENTAL_CORE,
    SELECTOR_CANONICAL_INCREMENTAL_STATE,
    SELECTOR_CANONICAL_SILVER,
    SELECTOR_SHARED_CORE_SEED,
    SELECTOR_SHARED_CORE_SEED_TESTS,
)
from pipelines.config.settings import get_settings
from pipelines.tasks.artifacts import (
    load_run_results,
    persist_dbt_audit,
    summarize_run_results,
)
from pipelines.tasks.db import ensure_ops_audit_tables, get_engine
from pipelines.tasks.dbt_tasks import (
    dbt_deps,
    dbt_run_selector,
    dbt_seed_selector,
    dbt_test_selector,
)
from pipelines.tasks.gates import raise_if_failed
from pipelines.tasks.measurement_queue import cleanup_measurement_timestamp_queue
from pipelines.tasks.measurement_runtime import (
    acquire_measurement_publish_lock,
    release_measurement_publish_lock,
    validate_incremental_queue_workload,
)
from pipelines.tasks.notifications import notify_flow_failure

REPO_ROOT = Path(__file__).resolve().parents[2]
OPS_SCHEMA = "ops"
STREAM_STATE_TABLE = "measurement_stream_state"
SILVER_SCHEMA = "silver"
SILVER_FACT_TABLE = "fct_measurements_silver"


def _git_sha() -> str | None:
    try:
        completed = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            cwd=REPO_ROOT,
            capture_output=True,
            text=True,
            check=False,
        )
        if completed.returncode == 0:
            return completed.stdout.strip() or None
    except Exception:  # noqa: BLE001
        return None
    return None


def _summary_from_result(result) -> dict:
    run_results = (
        load_run_results(result.run_results_path) if result.run_results_path else {}
    )
    return summarize_run_results(run_results)


def _relation_has_rows(engine, schema: str, table: str) -> bool:
    with engine.connect() as connection:
        return bool(
            connection.execute(
                text(f"select exists (select 1 from {schema}.{table} limit 1)")
            ).scalar_one()
        )


def _validate_stream_state_ready(engine) -> None:
    inspector = inspect(engine)
    if not inspector.has_table(STREAM_STATE_TABLE, schema=OPS_SCHEMA):
        raise RuntimeError(
            "ops.measurement_stream_state is missing; run warehouse bootstrap and measurement stream state bootstrap before the incremental cutover."
        )

    if not inspector.has_table(SILVER_FACT_TABLE, schema=SILVER_SCHEMA):
        return

    if _relation_has_rows(
        engine, SILVER_SCHEMA, SILVER_FACT_TABLE
    ) and not _relation_has_rows(engine, OPS_SCHEMA, STREAM_STATE_TABLE):
        raise RuntimeError(
            "ops.measurement_stream_state is empty while silver.fct_measurements_silver contains history; run measurement_stream_state_bootstrap before canonical_incremental."
        )


@flow(name="canonical_incremental")
def canonical_incremental() -> None:
    logger = get_run_logger()
    settings = get_settings()
    engine = get_engine(settings)
    lock_connection = None

    ctx = get_flow_context()
    ctx.update(
        {
            "target": settings.DBT_TARGET,
            "git_sha": _git_sha(),
            "project_code": None,
            "slack_webhook_url": settings.SLACK_WEBHOOK_URL,
            "flow_name": "canonical_incremental",
        }
    )

    try:
        lock_connection = acquire_measurement_publish_lock(engine)
        ensure_ops_audit_tables(engine)
        _validate_stream_state_ready(engine)

        deps_result = dbt_deps(settings)
        deps_summary = _summary_from_result(deps_result)
        persist_dbt_audit(engine, deps_result, deps_summary, ctx)
        raise_if_failed(deps_result, "dbt deps failed")

        seed_result = dbt_seed_selector(
            settings,
            selector=SELECTOR_SHARED_CORE_SEED,
        )
        seed_summary = _summary_from_result(seed_result)
        persist_dbt_audit(engine, seed_result, seed_summary, ctx)
        raise_if_failed(seed_result, "shared core seed stage failed")

        seed_test_result = dbt_test_selector(
            settings,
            selector=SELECTOR_SHARED_CORE_SEED_TESTS,
        )
        seed_test_summary = _summary_from_result(seed_test_result)
        persist_dbt_audit(engine, seed_test_result, seed_test_summary, ctx)
        raise_if_failed(seed_test_result, "shared core seed tests failed")

        core_result = dbt_run_selector(
            settings,
            selector=SELECTOR_CANONICAL_INCREMENTAL_CORE,
        )
        core_summary = _summary_from_result(core_result)
        persist_dbt_audit(engine, core_result, core_summary, ctx)
        raise_if_failed(core_result, "canonical core stage failed")

        workloads = validate_incremental_queue_workload(engine, settings)
        logger.info(
            "Canonical incremental queue workload: queue_rows=%s expanded_rows=%s",
            sum(item.queue_rows for item in workloads),
            sum(item.expanded_rows for item in workloads),
        )

        silver_result = dbt_run_selector(settings, selector=SELECTOR_CANONICAL_SILVER)
        silver_summary = _summary_from_result(silver_result)
        persist_dbt_audit(engine, silver_result, silver_summary, ctx)
        raise_if_failed(silver_result, "canonical silver stage failed")

        state_result = dbt_run_selector(
            settings,
            selector=SELECTOR_CANONICAL_INCREMENTAL_STATE,
        )
        state_summary = _summary_from_result(state_result)
        persist_dbt_audit(engine, state_result, state_summary, ctx)
        raise_if_failed(state_result, "canonical stream state refresh failed")

        smoke_result = dbt_test_selector(
            settings,
            selector=SELECTOR_CANONICAL_BATCH_SMOKE_TESTS,
        )
        smoke_summary = _summary_from_result(smoke_result)
        persist_dbt_audit(engine, smoke_result, smoke_summary, ctx)
        raise_if_failed(smoke_result, "canonical incremental smoke tests failed")

        deleted_rows = cleanup_measurement_timestamp_queue(
            engine,
            retention_hours=settings.MEASUREMENT_TIMESTAMP_QUEUE_RETENTION_HOURS,
        )
        logger.info(
            "Cleaned %s eligible non-null timestamp queue rows outside the %s-hour "
            "retention floor; null-time rows and one checkpoint row per source remain.",
            deleted_rows,
            settings.MEASUREMENT_TIMESTAMP_QUEUE_RETENTION_HOURS,
        )

        logger.info("canonical_incremental completed successfully")
    except Exception as exc:  # noqa: BLE001
        notify_flow_failure(ctx, str(exc))
        raise
    finally:
        if lock_connection is not None:
            release_measurement_publish_lock(lock_connection)
        engine.dispose()


if __name__ == "__main__":
    canonical_incremental()
