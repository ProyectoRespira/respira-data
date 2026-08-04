from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

from pipelines.flows import canonical_full_refresh as full_refresh_module
from pipelines.flows import canonical_incremental as incremental_module


def _call_flow(flow_or_fn, *args, **kwargs):
    fn = getattr(flow_or_fn, "fn", flow_or_fn)
    return fn(*args, **kwargs)


def _settings() -> SimpleNamespace:
    return SimpleNamespace(
        DBT_TARGET="prod",
        SLACK_WEBHOOK_URL=None,
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
    monkeypatch.setattr(module, "get_flow_context", lambda: {})
    monkeypatch.setattr(module, "_git_sha", lambda: "abc123")
    monkeypatch.setattr(module, "_summary_from_result", lambda _result: {})
    monkeypatch.setattr(module, "persist_dbt_audit", lambda *args: None)
    monkeypatch.setattr(module, "raise_if_failed", lambda *args: None)
    monkeypatch.setattr(module, "notify_flow_failure", lambda *args: None)
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
        lambda _settings, selector: execution_order.append(f"seed:{selector}")
        or _result("seed"),
    )
    monkeypatch.setattr(
        incremental_module,
        "dbt_test_selector",
        lambda _settings, selector: execution_order.append(f"test:{selector}")
        or _result("test"),
    )
    monkeypatch.setattr(
        incremental_module,
        "dbt_run_selector",
        lambda _settings, selector: execution_order.append(f"run:{selector}")
        or _result("run"),
    )

    _call_flow(incremental_module.canonical_incremental)

    assert execution_order == [
        "deps",
        "seed:shared_core_seed",
        "test:shared_core_seed_tests",
        "run:canonical_incremental_core",
        "run:canonical_silver",
    ]
    engine.dispose.assert_called_once_with()


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
        lambda _settings, selector, full_refresh=False: execution_order.append(
            f"run:{selector}:full_refresh={full_refresh}"
        )
        or _result("run"),
    )
    monkeypatch.setattr(
        full_refresh_module,
        "dbt_test_selector",
        lambda _settings, selector: execution_order.append(f"test:{selector}")
        or _result("test"),
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
