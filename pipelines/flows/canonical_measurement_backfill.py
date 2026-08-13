from __future__ import annotations

import subprocess
from datetime import datetime, timedelta
from pathlib import Path

from pipelines.compat import flow, get_flow_context, get_run_logger
from pipelines.config.projects import list_project_configs
from pipelines.config.selectors import (
    SELECTOR_CANONICAL_BATCH_INGEST,
    SELECTOR_CANONICAL_BATCH_PAYLOAD_AUDIT,
    SELECTOR_CANONICAL_BATCH_PREP,
    SELECTOR_CANONICAL_BATCH_PROCESS,
    SELECTOR_CANONICAL_BATCH_SMOKE_TESTS,
)
from pipelines.config.settings import get_settings
from pipelines.tasks.artifacts import (
    load_run_results,
    persist_dbt_audit,
    summarize_run_results,
)
from pipelines.tasks.db import ensure_ops_audit_tables, get_engine
from pipelines.tasks.dbt_tasks import dbt_deps, dbt_run_selector, dbt_test_selector
from pipelines.tasks.gates import raise_if_failed
from pipelines.tasks.measurement_backfill import (
    MeasurementProcessBounds,
    build_measured_at_windows,
    get_measurement_process_bounds,
    load_measurement_source_registry,
    validate_measurement_sources,
)
from pipelines.tasks.measurement_queue import cleanup_measurement_timestamp_queue
from pipelines.tasks.notifications import notify_flow_failure

REPO_ROOT = Path(__file__).resolve().parents[2]


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


def _persist_result(engine, result, ctx: dict) -> dict:
    summary = _summary_from_result(result)
    persist_dbt_audit(engine, result, summary, ctx)
    return summary


def _build_ingest_vars(
    data_source_name: str,
    extracted_at_from: datetime | None,
    extracted_at_to: datetime | None,
) -> dict[str, object]:
    vars_payload: dict[str, object] = {
        "measurement_batch_data_source": data_source_name,
    }
    if extracted_at_from is not None:
        vars_payload["measurement_batch_extracted_at_from"] = extracted_at_from
    if extracted_at_to is not None:
        vars_payload["measurement_batch_extracted_at_to"] = extracted_at_to
    return vars_payload


def _build_process_vars(
    data_source_name: str,
    measured_at_from: datetime | None = None,
    measured_at_to: datetime | None = None,
    include_null_time_rows: bool = False,
) -> dict[str, object]:
    vars_payload: dict[str, object] = {
        "measurement_batch_data_source": data_source_name,
    }
    if measured_at_from is not None:
        vars_payload["measurement_batch_measured_at_from"] = measured_at_from
    if measured_at_to is not None:
        vars_payload["measurement_batch_measured_at_to"] = measured_at_to
    if include_null_time_rows:
        vars_payload["measurement_batch_include_null_time_rows"] = True
    return vars_payload


def _effective_process_bounds(
    source_bounds: MeasurementProcessBounds,
    process_measured_at_from: datetime | None,
    process_measured_at_to: datetime | None,
) -> tuple[datetime | None, datetime | None]:
    source_min = source_bounds["min_measured_at"]
    source_max = source_bounds["max_measured_at"]
    if source_min is None or source_max is None:
        return None, None

    source_max_exclusive: datetime = source_max + timedelta(microseconds=1)

    effective_from: datetime = (
        max(source_min, process_measured_at_from)
        if process_measured_at_from
        else source_min
    )
    effective_to: datetime = (
        min(source_max_exclusive, process_measured_at_to)
        if process_measured_at_to
        else source_max_exclusive
    )

    if effective_from >= effective_to:
        return None, None

    return effective_from, effective_to


def _validate_resume_queue_available(
    data_source_name: str,
    source_bounds: MeasurementProcessBounds,
    effective_from: datetime | None,
    effective_to: datetime | None,
    process_measured_at_from: datetime | None,
    process_measured_at_to: datetime | None,
) -> None:
    """Fail a resume that cannot be satisfied from the retained queue."""
    row_count = int(source_bounds.get("row_count", 0))
    null_time_row_count = int(source_bounds.get("null_time_row_count", 0))

    if row_count == 0:
        raise RuntimeError(
            "Cannot resume backfill with run_ingest=False: no rows for "
            f"data_source_name={data_source_name} remain in "
            "intermediate.int_measurement_timestamps_silver. Re-run with "
            "run_ingest=True to land the required source rows again."
        )

    requested_timed_scope = (
        process_measured_at_from is not None or process_measured_at_to is not None
    )
    timed_scope_available = effective_from is not None and effective_to is not None
    if requested_timed_scope and not timed_scope_available and null_time_row_count == 0:
        raise RuntimeError(
            "Cannot resume backfill with run_ingest=False: the requested measured-time "
            f"scope for data_source_name={data_source_name} is not present in "
            "intermediate.int_measurement_timestamps_silver. The queue may have been "
            "cleaned; re-run with run_ingest=True to land the required source rows again."
        )


@flow(name="canonical_measurement_backfill")
def canonical_measurement_backfill(
    data_sources: list[str] | None = None,
    ingest_extracted_at_from: datetime | None = None,
    ingest_extracted_at_to: datetime | None = None,
    process_measured_at_from: datetime | None = None,
    process_measured_at_to: datetime | None = None,
    process_batch_hours: int | None = None,
    run_prep: bool = True,
    run_ingest: bool = True,
    run_tests: bool = True,
    run_project_models_after: bool = False,
    include_payload_audit: bool = False,
) -> None:
    logger = get_run_logger()
    settings = get_settings()
    effective_process_batch_hours = int(
        process_batch_hours or settings.MEASUREMENT_BACKFILL_PROCESS_BATCH_HOURS
    )
    retention_hours = int(settings.MEASUREMENT_TIMESTAMP_QUEUE_RETENTION_HOURS)
    engine = get_engine(settings)
    ensure_ops_audit_tables(engine)

    ctx = get_flow_context()
    ctx.update(
        {
            "target": settings.DBT_TARGET,
            "git_sha": _git_sha(),
            "project_code": None,
            "slack_webhook_url": settings.SLACK_WEBHOOK_URL,
            "flow_name": "canonical_measurement_backfill",
        }
    )

    try:
        logger.info(
            "canonical_measurement_backfill using process_batch_hours=%s queue_retention_hours=%s",
            effective_process_batch_hours,
            retention_hours,
        )

        deps_result = dbt_deps(settings)
        _persist_result(engine, deps_result, ctx)
        raise_if_failed(deps_result, "dbt deps failed")

        if run_prep:
            prep_result = dbt_run_selector(
                settings,
                selector=SELECTOR_CANONICAL_BATCH_PREP,
            )
            _persist_result(engine, prep_result, ctx)
            raise_if_failed(prep_result, "canonical batch prep failed")
        else:
            logger.info(
                "Skipping canonical batch prep; assuming staging views and prerequisite dims already exist."
            )

        source_registry = load_measurement_source_registry(settings)
        selected_sources = validate_measurement_sources(
            settings,
            source_registry=source_registry,
            requested_sources=data_sources,
        )

        for data_source_name in selected_sources:
            logger.info("Starting backfill for data_source_name=%s", data_source_name)

            ingest_vars = _build_ingest_vars(
                data_source_name,
                ingest_extracted_at_from,
                ingest_extracted_at_to,
            )
            if run_ingest:
                ingest_result = dbt_run_selector(
                    settings,
                    selector=SELECTOR_CANONICAL_BATCH_INGEST,
                    vars_payload=ingest_vars,
                )
                _persist_result(engine, ingest_result, ctx)
                raise_if_failed(
                    ingest_result,
                    f"canonical batch ingest failed for {data_source_name}",
                )

                if include_payload_audit:
                    logger.info(
                        "Building payload audit rows for data_source_name=%s",
                        data_source_name,
                    )
                    payload_result = dbt_run_selector(
                        settings,
                        selector=SELECTOR_CANONICAL_BATCH_PAYLOAD_AUDIT,
                        vars_payload=ingest_vars,
                    )
                    _persist_result(engine, payload_result, ctx)
                    raise_if_failed(
                        payload_result,
                        f"canonical batch payload audit failed for {data_source_name}",
                    )
            else:
                logger.info(
                    "Skipping canonical batch ingest; validating that source-row timestamps remain in the processing queue and streams already exist for data_source_name=%s.",
                    data_source_name,
                )

            source_bounds = get_measurement_process_bounds(engine, data_source_name)
            effective_from, effective_to = _effective_process_bounds(
                source_bounds,
                process_measured_at_from,
                process_measured_at_to,
            )

            if not run_ingest:
                _validate_resume_queue_available(
                    data_source_name,
                    source_bounds,
                    effective_from,
                    effective_to,
                    process_measured_at_from,
                    process_measured_at_to,
                )
                logger.info(
                    "Resuming from retained timestamp queue rows for data_source_name=%s; "
                    "resume is supported only while the required queue rows remain.",
                    data_source_name,
                )

            process_windows = build_measured_at_windows(
                effective_from,
                effective_to,
                effective_process_batch_hours,
            )

            for window in process_windows:
                process_vars = _build_process_vars(
                    data_source_name,
                    measured_at_from=window["measured_at_from"],
                    measured_at_to=window["measured_at_to"],
                )
                logger.info(
                    "Processing source=%s window=[%s, %s)",
                    data_source_name,
                    window["measured_at_from"],
                    window["measured_at_to"],
                )
                process_result = dbt_run_selector(
                    settings,
                    selector=SELECTOR_CANONICAL_BATCH_PROCESS,
                    vars_payload=process_vars,
                )
                _persist_result(engine, process_result, ctx)
                raise_if_failed(
                    process_result,
                    f"canonical batch process failed for {data_source_name}",
                )

                if run_tests:
                    test_result = dbt_test_selector(
                        settings,
                        selector=SELECTOR_CANONICAL_BATCH_SMOKE_TESTS,
                        vars_payload=process_vars,
                    )
                    _persist_result(engine, test_result, ctx)
                    raise_if_failed(
                        test_result,
                        "canonical batch smoke tests failed for "
                        f"{data_source_name} window "
                        f"[{window['measured_at_from']}, {window['measured_at_to']})",
                    )
                    deleted_rows = cleanup_measurement_timestamp_queue(
                        engine,
                        retention_hours=retention_hours,
                        data_source_name=data_source_name,
                        measured_at_from=window["measured_at_from"],
                        measured_at_to=window["measured_at_to"],
                    )
                    logger.info(
                        "Cleaned %s eligible timestamp queue rows for source=%s "
                        "window=[%s, %s); rows inside the %s-hour retention floor remain.",
                        deleted_rows,
                        data_source_name,
                        window["measured_at_from"],
                        window["measured_at_to"],
                        retention_hours,
                    )
                else:
                    logger.info(
                        "Skipping timestamp queue cleanup for source=%s window=[%s, %s) "
                        "because smoke tests were disabled.",
                        data_source_name,
                        window["measured_at_from"],
                        window["measured_at_to"],
                    )

            null_time_row_count = int(source_bounds.get("null_time_row_count", 0))
            if null_time_row_count > 0:
                null_process_vars = _build_process_vars(
                    data_source_name,
                    include_null_time_rows=True,
                )
                logger.info(
                    "Processing null-time rows for source=%s count=%s",
                    data_source_name,
                    null_time_row_count,
                )
                null_process_result = dbt_run_selector(
                    settings,
                    selector=SELECTOR_CANONICAL_BATCH_PROCESS,
                    vars_payload=null_process_vars,
                )
                _persist_result(engine, null_process_result, ctx)
                raise_if_failed(
                    null_process_result,
                    f"canonical null-time process failed for {data_source_name}",
                )
                if run_tests:
                    null_test_result = dbt_test_selector(
                        settings,
                        selector=SELECTOR_CANONICAL_BATCH_SMOKE_TESTS,
                        vars_payload=null_process_vars,
                    )
                    _persist_result(engine, null_test_result, ctx)
                    raise_if_failed(
                        null_test_result,
                        f"canonical null-time smoke tests failed for {data_source_name}",
                    )
                    deleted_null_rows = cleanup_measurement_timestamp_queue(
                        engine,
                        retention_hours=retention_hours,
                        data_source_name=data_source_name,
                        include_null_time_rows=True,
                    )
                    logger.info(
                        "Cleaned %s eligible null-time timestamp queue rows for "
                        "source=%s; rows inside the %s-hour retention floor remain.",
                        deleted_null_rows,
                        data_source_name,
                        retention_hours,
                    )
                else:
                    logger.info(
                        "Preserving null-time timestamp queue rows for source=%s "
                        "because smoke tests were disabled.",
                        data_source_name,
                    )

            logger.info("Finished backfill for data_source_name=%s", data_source_name)

        if run_project_models_after:
            for project in list_project_configs():
                project_result = dbt_run_selector(
                    settings, selector=project.dbt_selector
                )
                _persist_result(engine, project_result, ctx)
                raise_if_failed(
                    project_result,
                    f"project selector failed for {project.project_code}",
                )

        logger.info("canonical_measurement_backfill completed successfully")
    except Exception as exc:  # noqa: BLE001
        notify_flow_failure(ctx, str(exc))
        raise
    finally:
        engine.dispose()


if __name__ == "__main__":
    canonical_measurement_backfill()
