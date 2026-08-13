from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from pipelines.flows import canonical_full_refresh as full_refresh_module
from pipelines.flows import canonical_incremental as incremental_module


def _call_flow(flow_or_fn, *args, **kwargs):
    fn = getattr(flow_or_fn, "fn", flow_or_fn)
    return fn(*args, **kwargs)


def _settings() -> SimpleNamespace:
    return SimpleNamespace(
        DBT_TARGET="prod",
        SLACK_WEBHOOK_URL=None,
        MEASUREMENT_TIMESTAMP_QUEUE_RETENTION_HOURS=168,
    )


def _result(command: str) -> SimpleNamespace:
    return SimpleNamespace(
        status="success",
        command=command,
        run_results_path=None,
    )


def _configure_common(monkeypatch, module):
    engine = MagicMock()
    monkeypatch.setattr(module, "get_settings", lambda: _settings())
    monkeypatch.setattr(module, "get_engine", lambda _settings: engine)
    monkeypatch.setattr(module, "ensure_ops_audit_tables", lambda _engine: None)
    if hasattr(module, "_validate_stream_state_ready"):
        monkeypatch.setattr(
            module, "_validate_stream_state_ready", lambda _engine: None
        )
    monkeypatch.setattr(module, "get_flow_context", lambda: {})
    monkeypatch.setattr(module, "_git_sha", lambda: "abc123")
    monkeypatch.setattr(module, "_summary_from_result", lambda _result: {})
    monkeypatch.setattr(module, "persist_dbt_audit", lambda *args: None)
    monkeypatch.setattr(module, "raise_if_failed", lambda *args: None)
    monkeypatch.setattr(module, "notify_flow_failure", lambda *args: None)
    if hasattr(module, "cleanup_measurement_timestamp_queue"):
        monkeypatch.setattr(
            module, "cleanup_measurement_timestamp_queue", lambda *args, **kwargs: 0
        )
    return engine


def test_canonical_incremental_seeds_and_tests_shared_core_before_models(monkeypatch):
    execution_order: list[str] = []
    engine = _configure_common(monkeypatch, incremental_module)

    monkeypatch.setattr(
        incremental_module,
        "dbt_deps",
        lambda _settings: execution_order.append("deps") or _result("deps"),
    )
    monkeypatch.setattr(
        incremental_module,
        "dbt_seed_selector",
        lambda _settings, selector: (
            execution_order.append(f"seed:{selector}") or _result("seed")
        ),
    )
    monkeypatch.setattr(
        incremental_module,
        "dbt_test_selector",
        lambda _settings, selector: (
            execution_order.append(f"test:{selector}") or _result("test")
        ),
    )
    monkeypatch.setattr(
        incremental_module,
        "dbt_run_selector",
        lambda _settings, selector: (
            execution_order.append(f"run:{selector}") or _result("run")
        ),
    )

    _call_flow(incremental_module.canonical_incremental)

    assert execution_order == [
        "deps",
        "seed:shared_core_seed",
        "test:shared_core_seed_tests",
        "run:canonical_incremental_core",
        "run:canonical_silver",
        "run:canonical_incremental_state",
        "test:canonical_batch_smoke_tests",
    ]
    engine.dispose.assert_called_once_with()


def test_canonical_incremental_does_not_refresh_state_after_silver_failure(
    monkeypatch,
):
    execution_order: list[str] = []
    engine = _configure_common(monkeypatch, incremental_module)
    monkeypatch.setattr(
        incremental_module,
        "dbt_deps",
        lambda _settings: execution_order.append("deps") or _result("deps"),
    )
    monkeypatch.setattr(
        incremental_module,
        "dbt_seed_selector",
        lambda _settings, selector: (
            execution_order.append(f"seed:{selector}") or _result("seed")
        ),
    )
    monkeypatch.setattr(
        incremental_module,
        "dbt_test_selector",
        lambda _settings, selector: (
            execution_order.append(f"test:{selector}") or _result("test")
        ),
    )
    monkeypatch.setattr(
        incremental_module,
        "dbt_run_selector",
        lambda _settings, selector: (
            execution_order.append(f"run:{selector}") or _result("run")
        ),
    )

    def _raise_on_silver(_result, message):
        if message == "canonical silver stage failed":
            raise RuntimeError(message)

    monkeypatch.setattr(incremental_module, "raise_if_failed", _raise_on_silver)

    with pytest.raises(RuntimeError, match="canonical silver stage failed"):
        _call_flow(incremental_module.canonical_incremental)

    assert "run:canonical_incremental_state" not in execution_order
    assert "test:canonical_batch_smoke_tests" not in execution_order
    engine.dispose.assert_called_once_with()


def test_canonical_incremental_cleans_queue_only_after_smoke_success(monkeypatch):
    execution_order: list[str] = []
    _configure_common(monkeypatch, incremental_module)

    monkeypatch.setattr(
        incremental_module,
        "dbt_deps",
        lambda _settings: execution_order.append("deps") or _result("deps"),
    )
    monkeypatch.setattr(
        incremental_module,
        "dbt_seed_selector",
        lambda _settings, selector: _result(f"seed:{selector}"),
    )
    monkeypatch.setattr(
        incremental_module,
        "dbt_run_selector",
        lambda _settings, selector: (
            execution_order.append(f"run:{selector}") or _result("run")
        ),
    )
    monkeypatch.setattr(
        incremental_module,
        "dbt_test_selector",
        lambda _settings, selector: (
            execution_order.append(f"test:{selector}") or _result("test")
        ),
    )
    monkeypatch.setattr(
        incremental_module,
        "cleanup_measurement_timestamp_queue",
        lambda *_args, **_kwargs: execution_order.append("cleanup") or 12,
    )

    _call_flow(incremental_module.canonical_incremental)

    assert execution_order[-4:] == [
        "run:canonical_silver",
        "run:canonical_incremental_state",
        "test:canonical_batch_smoke_tests",
        "cleanup",
    ]


def test_canonical_incremental_preserves_queue_when_smoke_tests_fail(monkeypatch):
    _configure_common(monkeypatch, incremental_module)
    cleanup = MagicMock()

    monkeypatch.setattr(incremental_module, "dbt_deps", lambda _: _result("deps"))
    monkeypatch.setattr(
        incremental_module,
        "dbt_seed_selector",
        lambda _settings, selector: _result("seed"),
    )
    monkeypatch.setattr(
        incremental_module,
        "dbt_run_selector",
        lambda _settings, selector: _result("run"),
    )
    monkeypatch.setattr(
        incremental_module,
        "dbt_test_selector",
        lambda _settings, selector: _result("test"),
    )
    monkeypatch.setattr(
        incremental_module,
        "cleanup_measurement_timestamp_queue",
        cleanup,
    )

    def _raise_on_smoke(_result, message):
        if message == "canonical incremental smoke tests failed":
            raise RuntimeError(message)

    monkeypatch.setattr(incremental_module, "raise_if_failed", _raise_on_smoke)

    with pytest.raises(RuntimeError, match="incremental smoke tests failed"):
        _call_flow(incremental_module.canonical_incremental)

    cleanup.assert_not_called()


def test_canonical_incremental_notifies_when_stream_state_is_not_ready(monkeypatch):
    engine = _configure_common(monkeypatch, incremental_module)
    notify = MagicMock()
    deps = MagicMock()
    monkeypatch.setattr(incremental_module, "notify_flow_failure", notify)
    monkeypatch.setattr(incremental_module, "dbt_deps", deps)
    monkeypatch.setattr(
        incremental_module,
        "_validate_stream_state_ready",
        MagicMock(side_effect=RuntimeError("stream state not ready")),
    )

    with pytest.raises(RuntimeError, match="stream state not ready"):
        _call_flow(incremental_module.canonical_incremental)

    deps.assert_not_called()
    notify.assert_called_once()
    engine.dispose.assert_called_once_with()


def test_stream_state_readiness_rejects_empty_state_over_existing_silver(
    monkeypatch,
):
    inspector = MagicMock()
    inspector.has_table.side_effect = [True, True]
    monkeypatch.setattr(incremental_module, "inspect", lambda _engine: inspector)
    monkeypatch.setattr(
        incremental_module,
        "_relation_has_rows",
        MagicMock(side_effect=[True, False]),
    )

    with pytest.raises(RuntimeError, match="is empty while silver"):
        incremental_module._validate_stream_state_ready(object())


def test_stream_state_readiness_rejects_missing_state_relation(monkeypatch):
    inspector = MagicMock()
    inspector.has_table.return_value = False
    monkeypatch.setattr(incremental_module, "inspect", lambda _engine: inspector)

    with pytest.raises(RuntimeError, match="is missing"):
        incremental_module._validate_stream_state_ready(object())


def test_stream_state_readiness_allows_cold_start_without_silver(monkeypatch):
    inspector = MagicMock()
    inspector.has_table.side_effect = [True, False]
    relation_has_rows = MagicMock()
    monkeypatch.setattr(incremental_module, "inspect", lambda _engine: inspector)
    monkeypatch.setattr(incremental_module, "_relation_has_rows", relation_has_rows)

    incremental_module._validate_stream_state_ready(object())

    relation_has_rows.assert_not_called()


def test_canonical_full_refresh_full_refreshes_shared_seeds_before_models(
    monkeypatch,
):
    execution_order: list[str] = []
    engine = _configure_common(monkeypatch, full_refresh_module)

    monkeypatch.setattr(
        full_refresh_module,
        "dbt_deps",
        lambda _settings: execution_order.append("deps") or _result("deps"),
    )

    def _seed_selector(_settings, selector, full_refresh=False):
        execution_order.append(f"seed:{selector}:full_refresh={full_refresh}")
        return _result("seed")

    monkeypatch.setattr(full_refresh_module, "dbt_seed_selector", _seed_selector)
    monkeypatch.setattr(
        full_refresh_module,
        "dbt_run_selector",
        lambda _settings, selector, full_refresh=False: (
            execution_order.append(f"run:{selector}:full_refresh={full_refresh}")
            or _result("run")
        ),
    )
    monkeypatch.setattr(
        full_refresh_module,
        "dbt_test_selector",
        lambda _settings, selector: (
            execution_order.append(f"test:{selector}") or _result("test")
        ),
    )

    _call_flow(full_refresh_module.canonical_full_refresh)

    assert execution_order == [
        "deps",
        "seed:shared_core_seed:full_refresh=True",
        "test:shared_core_seed_tests",
        "run:canonical_full_refresh:full_refresh=True",
        "test:canonical_full_refresh",
    ]
    engine.dispose.assert_called_once_with()
