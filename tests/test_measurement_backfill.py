from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from pipelines.flows.canonical_measurement_backfill import (
    _effective_process_bounds,
    _validate_resume_queue_available,
)
from pipelines.tasks.measurement_backfill import (
    _build_measured_at_windows,
    _load_measurement_source_registry,
    _validate_measurement_sources,
)


def _settings() -> SimpleNamespace:
    repo_root = Path(__file__).resolve().parents[1]
    return SimpleNamespace(DBT_PROJECT_DIR=str(repo_root / "dbt"))


def _call_flow(flow_or_fn, *args, **kwargs):
    fn = getattr(flow_or_fn, "fn", flow_or_fn)
    return fn(*args, **kwargs)


def test_load_measurement_source_registry_reads_registered_sources():
    registry = _load_measurement_source_registry(_settings())

    assert "fiuna_airbyte" in registry
    assert "airelibre_airbyte" in registry


def test_validate_measurement_sources_accepts_known_sources():
    settings = _settings()
    registry = _load_measurement_source_registry(settings)

    selected = _validate_measurement_sources(
        settings,
        source_registry=registry,
        requested_sources=["fiuna_airbyte", "meteostat_airbyte"],
    )

    assert selected == ["fiuna_airbyte", "meteostat_airbyte"]


def test_validate_measurement_sources_rejects_unknown_source():
    settings = _settings()
    registry = _load_measurement_source_registry(settings)

    with pytest.raises(ValueError, match="Unknown measurement source"):
        _validate_measurement_sources(
            settings,
            source_registry=registry,
            requested_sources=["does_not_exist"],
        )


def test_build_measured_at_windows_returns_half_open_windows():
    measured_at_from = datetime(2026, 1, 1, 0, 0, tzinfo=UTC)
    measured_at_to = measured_at_from + timedelta(hours=50)

    windows = _build_measured_at_windows(
        measured_at_from=measured_at_from,
        measured_at_to=measured_at_to,
        batch_hours=24,
    )

    assert windows == [
        {
            "measured_at_from": datetime(2026, 1, 1, 0, 0, tzinfo=UTC),
            "measured_at_to": datetime(2026, 1, 2, 0, 0, tzinfo=UTC),
        },
        {
            "measured_at_from": datetime(2026, 1, 2, 0, 0, tzinfo=UTC),
            "measured_at_to": datetime(2026, 1, 3, 0, 0, tzinfo=UTC),
        },
        {
            "measured_at_from": datetime(2026, 1, 3, 0, 0, tzinfo=UTC),
            "measured_at_to": datetime(2026, 1, 3, 2, 0, tzinfo=UTC),
        },
    ]


def test_build_measured_at_windows_rejects_non_positive_batch_hours():
    with pytest.raises(ValueError, match="greater than zero"):
        _build_measured_at_windows(
            measured_at_from=datetime(2026, 1, 1, 0, 0, tzinfo=UTC),
            measured_at_to=datetime(2026, 1, 2, 0, 0, tzinfo=UTC),
            batch_hours=0,
        )


def test_effective_process_bounds_are_clamped_to_retained_queue_rows():
    queue_from = datetime(2026, 1, 2, tzinfo=UTC)
    queue_to = datetime(2026, 1, 3, tzinfo=UTC)

    effective_from, effective_to = _effective_process_bounds(
        {
            "min_measured_at": queue_from,
            "max_measured_at": queue_to,
            "null_time_row_count": 0,
            "row_count": 2,
        },
        process_measured_at_from=datetime(2026, 1, 1, tzinfo=UTC),
        process_measured_at_to=datetime(2026, 1, 4, tzinfo=UTC),
    )

    assert effective_from == queue_from
    assert effective_to == queue_to + timedelta(microseconds=1)


def test_resume_accepts_rows_that_still_remain_in_queue():
    queue_from = datetime(2026, 1, 2, tzinfo=UTC)
    queue_to = datetime(2026, 1, 3, tzinfo=UTC)

    _validate_resume_queue_available(
        "fiuna_airbyte",
        {
            "min_measured_at": queue_from,
            "max_measured_at": queue_to,
            "null_time_row_count": 0,
            "row_count": 2,
        },
        queue_from,
        queue_to + timedelta(microseconds=1),
        queue_from,
        queue_to + timedelta(days=1),
    )


def test_resume_rejects_source_after_queue_rows_are_cleaned():
    with pytest.raises(RuntimeError, match="no rows.*remain"):
        _validate_resume_queue_available(
            "fiuna_airbyte",
            {
                "min_measured_at": None,
                "max_measured_at": None,
                "null_time_row_count": 0,
                "row_count": 0,
            },
            None,
            None,
            None,
            None,
        )


def test_resume_rejects_requested_scope_outside_retained_queue():
    with pytest.raises(RuntimeError, match="requested measured-time scope"):
        _validate_resume_queue_available(
            "fiuna_airbyte",
            {
                "min_measured_at": datetime(2026, 1, 5, tzinfo=UTC),
                "max_measured_at": datetime(2026, 1, 6, tzinfo=UTC),
                "null_time_row_count": 0,
                "row_count": 2,
            },
            None,
            None,
            datetime(2026, 1, 1, tzinfo=UTC),
            datetime(2026, 1, 2, tzinfo=UTC),
        )


def test_backfill_resume_stops_before_processing_when_queue_is_empty(monkeypatch):
    from pipelines.flows import canonical_measurement_backfill as flow_module

    engine = MagicMock()
    result = SimpleNamespace(run_results_path=None)
    settings = SimpleNamespace(
        DBT_TARGET="prod",
        SLACK_WEBHOOK_URL=None,
        MEASUREMENT_BACKFILL_PROCESS_BATCH_HOURS=24,
    )
    process_run = MagicMock()

    monkeypatch.setattr(flow_module, "get_settings", lambda: settings)
    monkeypatch.setattr(flow_module, "get_engine", lambda _settings: engine)
    monkeypatch.setattr(flow_module, "ensure_ops_audit_tables", lambda _engine: None)
    monkeypatch.setattr(flow_module, "get_flow_context", lambda: {})
    monkeypatch.setattr(flow_module, "_git_sha", lambda: "abc123")
    monkeypatch.setattr(flow_module, "dbt_deps", lambda _settings: result)
    monkeypatch.setattr(flow_module, "_persist_result", lambda *args: {})
    monkeypatch.setattr(flow_module, "raise_if_failed", lambda *args: None)
    monkeypatch.setattr(flow_module, "notify_flow_failure", lambda *args: None)
    monkeypatch.setattr(flow_module, "load_measurement_source_registry", lambda _: {})
    monkeypatch.setattr(
        flow_module,
        "validate_measurement_sources",
        lambda *_args, **_kwargs: ["fiuna_airbyte"],
    )
    monkeypatch.setattr(
        flow_module,
        "get_measurement_process_bounds",
        lambda *_args: {
            "min_measured_at": None,
            "max_measured_at": None,
            "null_time_row_count": 0,
            "row_count": 0,
        },
    )
    monkeypatch.setattr(flow_module, "dbt_run_selector", process_run)

    with pytest.raises(RuntimeError, match="run_ingest=False"):
        _call_flow(
            flow_module.canonical_measurement_backfill,
            run_prep=False,
            run_ingest=False,
            run_tests=False,
        )

    process_run.assert_not_called()
    engine.dispose.assert_called_once_with()


def test_backfill_smoke_tests_receive_effective_queue_bounds(monkeypatch):
    from pipelines.flows import canonical_measurement_backfill as flow_module

    queue_from = datetime(2026, 7, 31, tzinfo=UTC)
    queue_max = datetime(2026, 8, 1, 23, tzinfo=UTC)
    queue_to = queue_max + timedelta(microseconds=1)
    engine = MagicMock()
    result = SimpleNamespace(run_results_path=None)
    settings = SimpleNamespace(
        DBT_TARGET="prod",
        SLACK_WEBHOOK_URL=None,
        MEASUREMENT_BACKFILL_PROCESS_BATCH_HOURS=48,
    )
    test_selector = MagicMock(return_value=result)

    monkeypatch.setattr(flow_module, "get_settings", lambda: settings)
    monkeypatch.setattr(flow_module, "get_engine", lambda _settings: engine)
    monkeypatch.setattr(flow_module, "ensure_ops_audit_tables", lambda _engine: None)
    monkeypatch.setattr(flow_module, "get_flow_context", lambda: {})
    monkeypatch.setattr(flow_module, "_git_sha", lambda: "abc123")
    monkeypatch.setattr(flow_module, "dbt_deps", lambda _settings: result)
    monkeypatch.setattr(flow_module, "_persist_result", lambda *args: {})
    monkeypatch.setattr(flow_module, "raise_if_failed", lambda *args: None)
    monkeypatch.setattr(flow_module, "notify_flow_failure", lambda *args: None)
    monkeypatch.setattr(flow_module, "load_measurement_source_registry", lambda _: {})
    monkeypatch.setattr(
        flow_module,
        "validate_measurement_sources",
        lambda *_args, **_kwargs: ["meteostat_airbyte"],
    )
    monkeypatch.setattr(
        flow_module,
        "get_measurement_process_bounds",
        lambda *_args: {
            "min_measured_at": queue_from,
            "max_measured_at": queue_max,
            "null_time_row_count": 0,
            "row_count": 48,
        },
    )
    monkeypatch.setattr(flow_module, "dbt_run_selector", lambda *args, **kwargs: result)
    monkeypatch.setattr(flow_module, "dbt_test_selector", test_selector)

    _call_flow(
        flow_module.canonical_measurement_backfill,
        data_sources=["meteostat_airbyte"],
        run_prep=False,
        run_ingest=False,
        run_tests=True,
    )

    test_selector.assert_called_once_with(
        settings,
        selector=flow_module.SELECTOR_CANONICAL_BATCH_SMOKE_TESTS,
        vars_payload={
            "measurement_batch_data_source": "meteostat_airbyte",
            "measurement_batch_measured_at_from": queue_from,
            "measurement_batch_measured_at_to": queue_to,
        },
    )
    engine.dispose.assert_called_once_with()
