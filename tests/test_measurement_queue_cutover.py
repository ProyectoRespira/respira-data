from __future__ import annotations

from contextlib import nullcontext
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from pipelines.flows import canonical_measurement_queue_cutover as cutover
from pipelines.tasks.measurement_runtime import QueueProcessWindow


def _call_flow(flow_or_fn, *args, **kwargs):
    return getattr(flow_or_fn, "fn", flow_or_fn)(*args, **kwargs)


def _result(status: str = "success", command: str = "dbt") -> SimpleNamespace:
    return SimpleNamespace(status=status, command=command, run_results_path=None)


def _settings() -> SimpleNamespace:
    return SimpleNamespace(
        DBT_TARGET="prod",
        DBT_PROJECT_DIR="/app/dbt",
        SLACK_WEBHOOK_URL=None,
        MEASUREMENT_QUEUE_CUTOVER_BATCH_HOURS=168,
        MEASUREMENT_QUEUE_CUTOVER_MAX_QUEUE_ROWS=250_000,
        MEASUREMENT_INCREMENTAL_MAX_EXPANDED_ROWS=2_000_000,
        MEASUREMENT_TIMESTAMP_QUEUE_RETENTION_HOURS=168,
    )


def _configure_execute_flow(
    monkeypatch,
    windows: list[QueueProcessWindow],
    null_rows: int = 0,
) -> MagicMock:
    engine = MagicMock()
    monkeypatch.setattr(cutover, "get_settings", _settings)
    monkeypatch.setattr(cutover, "get_engine", lambda _settings: engine)
    monkeypatch.setattr(cutover, "get_flow_context", lambda: {})
    monkeypatch.setattr(cutover, "_git_sha", lambda: "abc123")
    monkeypatch.setattr(
        cutover, "measurement_publish_lock", lambda _engine: nullcontext()
    )
    monkeypatch.setattr(cutover, "validate_legacy_runtime_tables", lambda _engine: None)
    monkeypatch.setattr(cutover, "ensure_ops_audit_tables", lambda _engine: None)
    monkeypatch.setattr(
        cutover, "ensure_measurement_queue_runtime_indexes", lambda _engine: None
    )
    monkeypatch.setattr(cutover, "dbt_deps", lambda _settings: _result())
    monkeypatch.setattr(cutover, "_persist_result", lambda *_args: {})
    monkeypatch.setattr(
        cutover,
        "load_measurement_source_registry",
        lambda _settings: {"meteostat_airbyte": {"variables": {"a": "a"}}},
    )
    monkeypatch.setattr(
        cutover,
        "validate_measurement_sources",
        lambda *_args, **_kwargs: ["meteostat_airbyte"],
    )
    monkeypatch.setattr(
        cutover, "get_measurement_queue_index_status", lambda _engine: {}
    )
    monkeypatch.setattr(
        cutover, "plan_unmarked_queue_windows", lambda *_args, **_kwargs: windows
    )
    monkeypatch.setattr(
        cutover,
        "get_unmarked_null_time_row_count",
        lambda *_args: null_rows,
    )
    monkeypatch.setattr(cutover, "notify_flow_failure", lambda *_args: None)
    return engine


def test_cutover_execute_requires_confirmation():
    with pytest.raises(ValueError, match="confirm=True"):
        _call_flow(cutover.canonical_measurement_queue_cutover, mode="execute")


def test_cutover_plan_is_read_only(monkeypatch):
    engine = MagicMock()
    start = datetime(2026, 1, 1, tzinfo=UTC)
    window = QueueProcessWindow(
        "meteostat_airbyte", start, start + timedelta(hours=1), 10, 80
    )
    dbt_deps = MagicMock()
    ensure_indexes = MagicMock()

    monkeypatch.setattr(cutover, "get_settings", _settings)
    monkeypatch.setattr(cutover, "get_engine", lambda _settings: engine)
    monkeypatch.setattr(cutover, "get_flow_context", lambda: {})
    monkeypatch.setattr(cutover, "_git_sha", lambda: "abc123")
    monkeypatch.setattr(
        cutover, "measurement_publish_lock", lambda _engine: nullcontext()
    )
    monkeypatch.setattr(cutover, "validate_legacy_runtime_tables", lambda _engine: None)
    monkeypatch.setattr(
        cutover,
        "load_measurement_source_registry",
        lambda _settings: {"meteostat_airbyte": {"variables": {"a": "a"}}},
    )
    monkeypatch.setattr(
        cutover,
        "validate_measurement_sources",
        lambda *_args, **_kwargs: ["meteostat_airbyte"],
    )
    monkeypatch.setattr(
        cutover, "get_measurement_queue_index_status", lambda _engine: {}
    )
    monkeypatch.setattr(
        cutover, "plan_unmarked_queue_windows", lambda *_args, **_kwargs: [window]
    )
    monkeypatch.setattr(cutover, "get_unmarked_null_time_row_count", lambda *_args: 0)
    monkeypatch.setattr(cutover, "dbt_deps", dbt_deps)
    monkeypatch.setattr(
        cutover, "ensure_measurement_queue_runtime_indexes", ensure_indexes
    )
    monkeypatch.setattr(cutover, "notify_flow_failure", lambda *_args: None)

    _call_flow(
        cutover.canonical_measurement_queue_cutover,
        mode="plan",
        data_sources=["meteostat_airbyte"],
    )

    dbt_deps.assert_not_called()
    ensure_indexes.assert_not_called()
    engine.dispose.assert_called_once_with()


def test_cutover_execute_refuses_oversized_single_timestamp_without_cleanup(
    monkeypatch,
):
    engine = MagicMock()
    start = datetime(2026, 1, 1, tzinfo=UTC)
    oversized = QueueProcessWindow(
        "meteostat_airbyte",
        start,
        start + timedelta(microseconds=1),
        300_000,
        2_400_000,
        oversized_single_timestamp=True,
    )
    cleanup = MagicMock()
    process = MagicMock()

    monkeypatch.setattr(cutover, "get_settings", _settings)
    monkeypatch.setattr(cutover, "get_engine", lambda _settings: engine)
    monkeypatch.setattr(cutover, "get_flow_context", lambda: {})
    monkeypatch.setattr(cutover, "_git_sha", lambda: "abc123")
    monkeypatch.setattr(
        cutover, "measurement_publish_lock", lambda _engine: nullcontext()
    )
    monkeypatch.setattr(cutover, "validate_legacy_runtime_tables", lambda _engine: None)
    monkeypatch.setattr(cutover, "ensure_ops_audit_tables", lambda _engine: None)
    monkeypatch.setattr(
        cutover, "ensure_measurement_queue_runtime_indexes", lambda _engine: None
    )
    monkeypatch.setattr(cutover, "dbt_deps", lambda _settings: _result())
    monkeypatch.setattr(cutover, "_persist_result", lambda *_args: {})
    monkeypatch.setattr(
        cutover,
        "load_measurement_source_registry",
        lambda _settings: {
            "meteostat_airbyte": {"variables": {str(i): str(i) for i in range(8)}}
        },
    )
    monkeypatch.setattr(
        cutover,
        "validate_measurement_sources",
        lambda *_args, **_kwargs: ["meteostat_airbyte"],
    )
    monkeypatch.setattr(
        cutover, "get_measurement_queue_index_status", lambda _engine: {}
    )
    monkeypatch.setattr(
        cutover, "plan_unmarked_queue_windows", lambda *_args, **_kwargs: [oversized]
    )
    monkeypatch.setattr(cutover, "get_unmarked_null_time_row_count", lambda *_args: 0)
    monkeypatch.setattr(cutover, "dbt_run_selector", process)
    monkeypatch.setattr(cutover, "cleanup_measurement_timestamp_queue", cleanup)
    monkeypatch.setattr(cutover, "notify_flow_failure", lambda *_args: None)

    with pytest.raises(RuntimeError, match="cannot be split"):
        _call_flow(
            cutover.canonical_measurement_queue_cutover,
            mode="execute",
            confirm=True,
            data_sources=["meteostat_airbyte"],
        )

    process.assert_not_called()
    cleanup.assert_not_called()
    engine.dispose.assert_called_once_with()


def test_witness_success_refreshes_state_without_republishing(monkeypatch):
    settings = _settings()
    engine = MagicMock()
    execution: list[str] = []

    monkeypatch.setattr(
        cutover,
        "dbt_test_selector",
        lambda _settings, selector, vars_payload: (
            execution.append(f"test:{selector}") or _result()
        ),
    )
    monkeypatch.setattr(
        cutover,
        "dbt_run_selector",
        lambda _settings, selector, vars_payload: (
            execution.append(f"run:{selector}") or _result()
        ),
    )
    monkeypatch.setattr(cutover, "_persist_result", lambda *_args: {})

    cutover._validate_or_repair_window(
        settings,
        engine,
        {},
        {"measurement_batch_data_source": "meteostat_airbyte"},
        repair_failed_windows=True,
    )

    assert execution == [
        f"test:{cutover.SELECTOR_CANONICAL_CUTOVER_WITNESS_TESTS}",
        f"run:{cutover.SELECTOR_CANONICAL_INCREMENTAL_STATE}",
        f"test:{cutover.SELECTOR_CANONICAL_BATCH_SMOKE_TESTS}",
    ]
    assert f"run:{cutover.SELECTOR_CANONICAL_BATCH_PROCESS}" not in execution


def test_successful_window_is_cleaned_only_after_validation(monkeypatch):
    start = datetime(2026, 1, 1, tzinfo=UTC)
    window = QueueProcessWindow(
        "meteostat_airbyte", start, start + timedelta(hours=1), 10, 10
    )
    engine = _configure_execute_flow(monkeypatch, [window])
    execution: list[str] = []
    monkeypatch.setattr(
        cutover,
        "_validate_or_repair_window",
        lambda *_args, **_kwargs: execution.append("validated"),
    )
    monkeypatch.setattr(
        cutover,
        "cleanup_measurement_timestamp_queue",
        lambda *_args, **_kwargs: execution.append("cleaned") or 10,
    )

    _call_flow(
        cutover.canonical_measurement_queue_cutover,
        mode="execute",
        confirm=True,
        data_sources=["meteostat_airbyte"],
    )

    assert execution == ["validated", "cleaned"]
    engine.dispose.assert_called_once_with()


@pytest.mark.parametrize("failed_gate", ["publish", "state", "smoke"])
def test_failed_window_gate_preserves_queue(monkeypatch, failed_gate):
    start = datetime(2026, 1, 1, tzinfo=UTC)
    window = QueueProcessWindow(
        "meteostat_airbyte", start, start + timedelta(hours=1), 10, 10
    )
    engine = _configure_execute_flow(monkeypatch, [window])
    cleanup = MagicMock()
    monkeypatch.setattr(
        cutover,
        "_validate_or_repair_window",
        MagicMock(side_effect=RuntimeError(f"{failed_gate} failed")),
    )
    monkeypatch.setattr(cutover, "cleanup_measurement_timestamp_queue", cleanup)

    with pytest.raises(RuntimeError, match=f"{failed_gate} failed"):
        _call_flow(
            cutover.canonical_measurement_queue_cutover,
            mode="execute",
            confirm=True,
            data_sources=["meteostat_airbyte"],
        )

    cleanup.assert_not_called()
    engine.dispose.assert_called_once_with()


def test_null_time_rows_use_dedicated_source_scope(monkeypatch):
    engine = _configure_execute_flow(monkeypatch, [], null_rows=20)
    repair = MagicMock()
    cleanup = MagicMock(return_value=20)
    monkeypatch.setattr(cutover, "_repair_and_validate_window", repair)
    monkeypatch.setattr(cutover, "cleanup_measurement_timestamp_queue", cleanup)

    _call_flow(
        cutover.canonical_measurement_queue_cutover,
        mode="execute",
        confirm=True,
        data_sources=["meteostat_airbyte"],
    )

    null_vars = repair.call_args.args[3]
    assert null_vars == {
        "measurement_batch_data_source": "meteostat_airbyte",
        "measurement_batch_unmarked_only": True,
        "measurement_batch_include_null_time_rows": True,
    }
    assert cleanup.call_args.kwargs["include_null_time_rows"] is True
    engine.dispose.assert_called_once_with()


def test_witness_mismatch_repairs_and_retests(monkeypatch):
    settings = _settings()
    engine = MagicMock()
    execution: list[str] = []
    witness_failure = _result("failed", "dbt test witness")
    success = _result()

    def test_selector(_settings, selector, vars_payload):
        execution.append(f"test:{selector}")
        if selector == cutover.SELECTOR_CANONICAL_CUTOVER_WITNESS_TESTS:
            return witness_failure
        return success

    monkeypatch.setattr(cutover, "dbt_test_selector", test_selector)
    monkeypatch.setattr(
        cutover,
        "dbt_run_selector",
        lambda _settings, selector, vars_payload: (
            execution.append(f"run:{selector}") or success
        ),
    )
    monkeypatch.setattr(
        cutover,
        "_persist_result",
        lambda _engine, result, _ctx: (
            {"tests_failed": 1} if result is witness_failure else {}
        ),
    )

    cutover._validate_or_repair_window(
        settings,
        engine,
        {},
        {"measurement_batch_data_source": "meteostat_airbyte"},
        repair_failed_windows=True,
    )

    assert execution == [
        f"test:{cutover.SELECTOR_CANONICAL_CUTOVER_WITNESS_TESTS}",
        f"run:{cutover.SELECTOR_CANONICAL_BATCH_PROCESS}",
        f"test:{cutover.SELECTOR_CANONICAL_BATCH_SMOKE_TESTS}",
    ]


def test_non_test_witness_failure_is_not_repaired(monkeypatch):
    failure = _result("failed", "dbt test witness")
    process = MagicMock()
    monkeypatch.setattr(cutover, "dbt_test_selector", lambda *_args, **_kwargs: failure)
    monkeypatch.setattr(cutover, "dbt_run_selector", process)
    monkeypatch.setattr(cutover, "_persist_result", lambda *_args: {"tests_failed": 0})

    with pytest.raises(RuntimeError, match="witness tests failed"):
        cutover._validate_or_repair_window(
            _settings(), MagicMock(), {}, {}, repair_failed_windows=True
        )

    process.assert_not_called()
