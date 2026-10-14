from __future__ import annotations

import subprocess
from datetime import datetime
from pathlib import Path

from pipelines.compat import flow, get_flow_context, get_run_logger
from pipelines.config.selectors import (
    SELECTOR_CANONICAL_BATCH_PROCESS,
    SELECTOR_CANONICAL_BATCH_SMOKE_TESTS,
    SELECTOR_CANONICAL_CUTOVER_WITNESS_TESTS,
    SELECTOR_CANONICAL_INCREMENTAL_STATE,
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
    load_measurement_source_registry,
    validate_measurement_sources,
)
from pipelines.tasks.measurement_queue import cleanup_measurement_timestamp_queue
from pipelines.tasks.measurement_runtime import (
    ensure_measurement_queue_runtime_indexes,
    get_measurement_queue_index_status,
    get_unmarked_null_time_row_count,
    measurement_publish_lock,
    plan_unmarked_queue_windows,
    validate_legacy_runtime_tables,
)
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


def _is_data_test_failure(result, summary: dict) -> bool:
    return result.status != "success" and int(summary.get("tests_failed", 0)) > 0


def _process_vars(
    data_source_name: str,
    measured_at_from: datetime | None = None,
    measured_at_to: datetime | None = None,
    include_null_time_rows: bool = False,
) -> dict[str, object]:
    values: dict[str, object] = {
        "measurement_batch_data_source": data_source_name,
        "measurement_batch_unmarked_only": True,
    }
    if measured_at_from is not None:
        values["measurement_batch_measured_at_from"] = measured_at_from
    if measured_at_to is not None:
        values["measurement_batch_measured_at_to"] = measured_at_to
    if include_null_time_rows:
        values["measurement_batch_include_null_time_rows"] = True
    return values


def _repair_and_validate_window(
    settings, engine, ctx: dict, vars_payload: dict
) -> None:
    process_result = dbt_run_selector(
        settings,
        selector=SELECTOR_CANONICAL_BATCH_PROCESS,
        vars_payload=vars_payload,
    )
    _persist_result(engine, process_result, ctx)
    raise_if_failed(process_result, "canonical cutover bounded repair failed")

    smoke_result = dbt_test_selector(
        settings,
        selector=SELECTOR_CANONICAL_BATCH_SMOKE_TESTS,
        vars_payload=vars_payload,
    )
    _persist_result(engine, smoke_result, ctx)
    raise_if_failed(smoke_result, "canonical cutover post-repair smoke tests failed")


def _validate_or_repair_window(
    settings,
    engine,
    ctx: dict,
    vars_payload: dict,
    repair_failed_windows: bool,
) -> None:
    witness_vars = {
        **vars_payload,
        "measurement_cutover_witness_enabled": True,
    }
    witness_result = dbt_test_selector(
        settings,
        selector=SELECTOR_CANONICAL_CUTOVER_WITNESS_TESTS,
        vars_payload=witness_vars,
    )
    witness_summary = _persist_result(engine, witness_result, ctx)

    if witness_result.status != "success":
        if repair_failed_windows and _is_data_test_failure(
            witness_result, witness_summary
        ):
            _repair_and_validate_window(settings, engine, ctx, vars_payload)
            return
        raise_if_failed(witness_result, "canonical cutover witness tests failed")

    state_result = dbt_run_selector(
        settings,
        selector=SELECTOR_CANONICAL_INCREMENTAL_STATE,
        vars_payload=vars_payload,
    )
    _persist_result(engine, state_result, ctx)
    raise_if_failed(state_result, "canonical cutover stream-state refresh failed")

    smoke_result = dbt_test_selector(
        settings,
        selector=SELECTOR_CANONICAL_BATCH_SMOKE_TESTS,
        vars_payload=vars_payload,
    )
    smoke_summary = _persist_result(engine, smoke_result, ctx)
    if smoke_result.status == "success":
        return
    if repair_failed_windows and _is_data_test_failure(smoke_result, smoke_summary):
        _repair_and_validate_window(settings, engine, ctx, vars_payload)
        return
    raise_if_failed(smoke_result, "canonical cutover inline smoke tests failed")


@flow(name="canonical_measurement_queue_cutover")
def canonical_measurement_queue_cutover(
    mode: str = "plan",
    confirm: bool = False,
    data_sources: list[str] | None = None,
    measured_at_from: datetime | None = None,
    measured_at_to: datetime | None = None,
    batch_hours: int | None = None,
    repair_failed_windows: bool = True,
    allow_oversized_null_time_scope: bool = False,
) -> None:
    normalized_mode = mode.strip().lower()
    if normalized_mode not in {"plan", "execute"}:
        raise ValueError("mode must be 'plan' or 'execute'.")
    if normalized_mode == "execute" and not confirm:
        raise ValueError("Cutover execution requires confirm=True.")
    if measured_at_from and measured_at_to and measured_at_from >= measured_at_to:
        raise ValueError("measured_at_from must be earlier than measured_at_to.")

    logger = get_run_logger()
    settings = get_settings()
    effective_batch_hours = int(
        batch_hours or settings.MEASUREMENT_QUEUE_CUTOVER_BATCH_HOURS
    )
    max_queue_rows = int(settings.MEASUREMENT_QUEUE_CUTOVER_MAX_QUEUE_ROWS)
    max_expanded_rows = int(settings.MEASUREMENT_INCREMENTAL_MAX_EXPANDED_ROWS)
    engine = get_engine(settings)

    ctx = get_flow_context()
    ctx.update(
        {
            "target": settings.DBT_TARGET,
            "git_sha": _git_sha(),
            "project_code": None,
            "slack_webhook_url": settings.SLACK_WEBHOOK_URL,
            "flow_name": "canonical_measurement_queue_cutover",
        }
    )

    try:
        with measurement_publish_lock(engine):
            validate_legacy_runtime_tables(engine)
            registry = load_measurement_source_registry(settings)
            selected_sources = validate_measurement_sources(
                settings,
                source_registry=registry,
                requested_sources=data_sources,
            )

            if normalized_mode == "execute":
                ensure_ops_audit_tables(engine)
                logger.info(
                    "Ensuring measurement queue indexes concurrently; the first "
                    "cutover execution can remain in this step for several minutes."
                )
                ensure_measurement_queue_runtime_indexes(engine)
                logger.info("Measurement queue index preparation completed.")
                deps_result = dbt_deps(settings)
                _persist_result(engine, deps_result, ctx)
                raise_if_failed(deps_result, "dbt deps failed")

            index_status = get_measurement_queue_index_status(engine)
            logger.info("Measurement queue index readiness: %s", index_status)

            for source_name in selected_sources:
                variable_count = len(
                    (registry[source_name] or {}).get("variables") or {}
                )
                windows = plan_unmarked_queue_windows(
                    engine,
                    data_source_name=source_name,
                    variable_count=variable_count,
                    batch_hours=effective_batch_hours,
                    max_queue_rows=max_queue_rows,
                    max_expanded_rows=max_expanded_rows,
                    measured_at_from=measured_at_from,
                    measured_at_to=measured_at_to,
                )
                null_rows = get_unmarked_null_time_row_count(engine, source_name)
                logger.info(
                    "Cutover plan source=%s windows=%s timed_rows=%s expanded_rows=%s null_rows=%s",
                    source_name,
                    len(windows),
                    sum(window.queue_rows for window in windows),
                    sum(window.expanded_rows for window in windows),
                    null_rows,
                )
                for window in windows:
                    logger.info(
                        "Cutover window source=%s scope=[%s, %s) queue_rows=%s expanded_rows=%s oversized_single_timestamp=%s",
                        source_name,
                        window.measured_at_from,
                        window.measured_at_to,
                        window.queue_rows,
                        window.expanded_rows,
                        window.oversized_single_timestamp,
                    )

                if normalized_mode == "plan":
                    continue

                oversized_windows = [
                    window for window in windows if window.oversized_single_timestamp
                ]
                if oversized_windows:
                    largest = max(
                        oversized_windows,
                        key=lambda window: window.expanded_rows,
                    )
                    raise RuntimeError(
                        f"Source {source_name} has {largest.queue_rows} unmarked queue "
                        "rows at one measured timestamp, expanding to "
                        f"{largest.expanded_rows} rows. That timestamp cannot be split "
                        "into safe measured-time windows and exceeds the configured "
                        f"limits ({max_queue_rows} queue, {max_expanded_rows} expanded). "
                        "No rows in this source were processed; investigate and repair "
                        "the duplicate timestamp scope explicitly."
                    )

                for window in windows:
                    vars_payload = _process_vars(
                        source_name,
                        measured_at_from=window.measured_at_from,
                        measured_at_to=window.measured_at_to,
                    )
                    _validate_or_repair_window(
                        settings,
                        engine,
                        ctx,
                        vars_payload,
                        repair_failed_windows,
                    )
                    logger.info(
                        "Cutover cleanup starting source=%s scope=[%s, %s)",
                        source_name,
                        window.measured_at_from,
                        window.measured_at_to,
                    )
                    deleted_rows = cleanup_measurement_timestamp_queue(
                        engine,
                        retention_hours=settings.MEASUREMENT_TIMESTAMP_QUEUE_RETENTION_HOURS,
                        data_source_name=source_name,
                        measured_at_from=window.measured_at_from,
                        measured_at_to=window.measured_at_to,
                    )
                    logger.info(
                        "Cutover completed source=%s scope=[%s, %s); deleted_rows=%s",
                        source_name,
                        window.measured_at_from,
                        window.measured_at_to,
                        deleted_rows,
                    )

                if null_rows:
                    if (
                        null_rows > max_queue_rows
                        and not allow_oversized_null_time_scope
                    ):
                        raise RuntimeError(
                            f"Source {source_name} has {null_rows} unmarked null-time rows, "
                            f"above the guarded limit of {max_queue_rows}. Re-run with "
                            "allow_oversized_null_time_scope=True only after reviewing the scope."
                        )
                    null_vars = _process_vars(source_name, include_null_time_rows=True)
                    _repair_and_validate_window(settings, engine, ctx, null_vars)
                    logger.info(
                        "Cutover cleanup starting null-time scope source=%s",
                        source_name,
                    )
                    deleted_rows = cleanup_measurement_timestamp_queue(
                        engine,
                        retention_hours=settings.MEASUREMENT_TIMESTAMP_QUEUE_RETENTION_HOURS,
                        data_source_name=source_name,
                        include_null_time_rows=True,
                    )
                    logger.info(
                        "Cutover completed null-time scope source=%s; deleted_rows=%s",
                        source_name,
                        deleted_rows,
                    )

            logger.info(
                "canonical_measurement_queue_cutover %s completed", normalized_mode
            )
    except Exception as exc:  # noqa: BLE001
        notify_flow_failure(ctx, str(exc))
        raise
    finally:
        engine.dispose()


if __name__ == "__main__":
    canonical_measurement_queue_cutover()
