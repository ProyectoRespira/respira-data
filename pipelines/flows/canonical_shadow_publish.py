from __future__ import annotations

import subprocess
from datetime import datetime
from pathlib import Path

from sqlalchemy import inspect, text

from pipelines.compat import flow, get_flow_context, get_run_logger
from pipelines.config.selectors import (
    SELECTOR_CANONICAL_SHADOW_PUBLISH,
    SELECTOR_CANONICAL_SHADOW_STATE,
    SELECTOR_CANONICAL_SHADOW_TESTS,
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
from pipelines.tasks.notifications import notify_flow_failure

REPO_ROOT = Path(__file__).resolve().parents[2]
SHADOW_SCHEMA = "shadow"
SHADOW_STATE_TABLE = "measurement_stream_state"


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


def _validate_and_build_shadow_vars(
    data_source_name: str,
    extracted_at_from: datetime | None,
    extracted_at_to: datetime | None,
    measured_at_from: datetime | None,
    measured_at_to: datetime | None,
) -> tuple[str, dict[str, object]]:
    if not data_source_name or not data_source_name.strip():
        raise ValueError("data_source_name must not be empty.")

    has_measured_from = measured_at_from is not None
    has_measured_to = measured_at_to is not None
    if has_measured_from != has_measured_to:
        raise ValueError(
            "Bounded shadow replay requires both measured_at_from and measured_at_to."
        )

    vars_payload: dict[str, object] = {
        "measurement_batch_data_source": data_source_name,
    }

    if has_measured_from:
        if extracted_at_from is not None or extracted_at_to is not None:
            raise ValueError("Bounded shadow replay does not accept extraction bounds.")
        assert measured_at_from is not None
        assert measured_at_to is not None
        if measured_at_from >= measured_at_to:
            raise ValueError("measured_at_from must be earlier than measured_at_to.")
        vars_payload.update(
            {
                "measurement_shadow_anchor_mode": "prior_silver",
                "measurement_batch_measured_at_from": measured_at_from,
                "measurement_batch_measured_at_to": measured_at_to,
            }
        )
        return "prior_silver", vars_payload

    if extracted_at_from is None:
        raise ValueError("Open-ended shadow publishing requires extracted_at_from.")
    if extracted_at_to is not None and extracted_at_from >= extracted_at_to:
        raise ValueError("extracted_at_from must be earlier than extracted_at_to.")

    vars_payload.update(
        {
            "measurement_shadow_anchor_mode": "stream_state",
            "measurement_batch_extracted_at_from": extracted_at_from,
        }
    )
    if extracted_at_to is not None:
        vars_payload["measurement_batch_extracted_at_to"] = extracted_at_to
    return "stream_state", vars_payload


def _state_relation_is_ready(engine, schema: str) -> bool:
    if not inspect(engine).has_table(SHADOW_STATE_TABLE, schema=schema):
        return False
    with engine.connect() as connection:
        return bool(
            connection.execute(
                text(
                    f"select exists (select 1 from {schema}.{SHADOW_STATE_TABLE} limit 1)"
                )
            ).scalar_one()
        )


@flow(name="canonical_shadow_publish")
def canonical_shadow_publish(
    data_source_name: str,
    extracted_at_from: datetime | None = None,
    extracted_at_to: datetime | None = None,
    measured_at_from: datetime | None = None,
    measured_at_to: datetime | None = None,
    reset_shadow: bool = False,
    run_tests: bool = True,
) -> None:
    logger = get_run_logger()
    settings = get_settings()
    anchor_mode, vars_payload = _validate_and_build_shadow_vars(
        data_source_name,
        extracted_at_from,
        extracted_at_to,
        measured_at_from,
        measured_at_to,
    )

    engine = get_engine(settings)
    ctx = get_flow_context()
    ctx.update(
        {
            "target": settings.DBT_TARGET,
            "git_sha": _git_sha(),
            "project_code": None,
            "slack_webhook_url": settings.SLACK_WEBHOOK_URL,
            "flow_name": "canonical_shadow_publish",
        }
    )

    try:
        ensure_ops_audit_tables(engine)
        logger.info(
            "Starting shadow publish source=%s anchor_mode=%s reset_shadow=%s",
            data_source_name,
            anchor_mode,
            reset_shadow,
        )

        source_registry = load_measurement_source_registry(settings)
        validate_measurement_sources(
            settings,
            source_registry=source_registry,
            requested_sources=[data_source_name],
        )

        deps_result = dbt_deps(settings)
        _persist_result(engine, deps_result, ctx)
        raise_if_failed(deps_result, "dbt deps failed")

        if reset_shadow:
            if not _state_relation_is_ready(engine, "ops"):
                raise RuntimeError(
                    "ops.measurement_stream_state is missing or empty; run the production stream-state bootstrap first."
                )
            reset_result = dbt_run_selector(
                settings,
                selector=SELECTOR_CANONICAL_SHADOW_STATE,
                full_refresh=True,
            )
            _persist_result(engine, reset_result, ctx)
            raise_if_failed(reset_result, "shadow state reset failed")
        elif not _state_relation_is_ready(engine, SHADOW_SCHEMA):
            raise RuntimeError(
                "shadow.measurement_stream_state is missing or empty; rerun with reset_shadow=True."
            )

        publish_result = dbt_run_selector(
            settings,
            selector=SELECTOR_CANONICAL_SHADOW_PUBLISH,
            full_refresh=reset_shadow,
            vars_payload=vars_payload,
        )
        _persist_result(engine, publish_result, ctx)
        raise_if_failed(publish_result, "shadow silver publish failed")

        state_result = dbt_run_selector(
            settings,
            selector=SELECTOR_CANONICAL_SHADOW_STATE,
            vars_payload=vars_payload,
        )
        _persist_result(engine, state_result, ctx)
        raise_if_failed(state_result, "shadow state refresh failed")

        if run_tests:
            test_result = dbt_test_selector(
                settings,
                selector=SELECTOR_CANONICAL_SHADOW_TESTS,
                vars_payload=vars_payload,
            )
            _persist_result(engine, test_result, ctx)
            raise_if_failed(test_result, "shadow contract tests failed")

        logger.info(
            "Shadow publish completed source=%s anchor_mode=%s",
            data_source_name,
            anchor_mode,
        )
    except Exception as exc:  # noqa: BLE001
        notify_flow_failure(ctx, str(exc))
        raise
    finally:
        engine.dispose()
