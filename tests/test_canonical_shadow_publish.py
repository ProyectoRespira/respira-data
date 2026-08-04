from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from pipelines.flows import canonical_shadow_publish as flow_module


def _call(callable_or_prefect_object, *args, **kwargs):
    fn = getattr(callable_or_prefect_object, "fn", callable_or_prefect_object)
    return fn(*args, **kwargs)


def _dt(day: int) -> datetime:
    return datetime(2026, 7, day, tzinfo=UTC)


def test_open_ended_scope_requires_extraction_lower_bound():
    with pytest.raises(ValueError, match="requires extracted_at_from"):
        flow_module._validate_and_build_shadow_vars(
            "fiuna_airbyte", None, None, None, None
        )


def test_open_ended_scope_builds_stream_state_vars():
    mode, vars_payload = flow_module._validate_and_build_shadow_vars(
        "fiuna_airbyte", _dt(1), _dt(2), None, None
    )

    assert mode == "stream_state"
    assert vars_payload == {
        "measurement_batch_data_source": "fiuna_airbyte",
        "measurement_shadow_anchor_mode": "stream_state",
        "measurement_batch_extracted_at_from": _dt(1),
        "measurement_batch_extracted_at_to": _dt(2),
    }


def test_bounded_scope_builds_prior_silver_vars():
    mode, vars_payload = flow_module._validate_and_build_shadow_vars(
        "fiuna_airbyte", None, None, _dt(1), _dt(2)
    )

    assert mode == "prior_silver"
    assert vars_payload == {
        "measurement_batch_data_source": "fiuna_airbyte",
        "measurement_shadow_anchor_mode": "prior_silver",
        "measurement_batch_measured_at_from": _dt(1),
        "measurement_batch_measured_at_to": _dt(2),
    }


@pytest.mark.parametrize(
    "kwargs, message",
    [
        (
            {"measured_at_from": _dt(1)},
            "requires both measured_at_from and measured_at_to",
        ),
        (
            {
                "extracted_at_from": _dt(1),
                "measured_at_from": _dt(1),
                "measured_at_to": _dt(2),
            },
            "does not accept extraction bounds",
        ),
        (
            {"measured_at_from": _dt(2), "measured_at_to": _dt(1)},
            "measured_at_from must be earlier",
        ),
        (
            {"extracted_at_from": _dt(2), "extracted_at_to": _dt(1)},
            "extracted_at_from must be earlier",
        ),
    ],
)
def test_scope_rejects_ambiguous_or_reversed_bounds(kwargs, message):
    with pytest.raises(ValueError, match=message):
        flow_module._validate_and_build_shadow_vars(
            "fiuna_airbyte",
            kwargs.get("extracted_at_from"),
            kwargs.get("extracted_at_to"),
            kwargs.get("measured_at_from"),
            kwargs.get("measured_at_to"),
        )


@patch.object(flow_module, "get_run_logger", return_value=MagicMock())
@patch.object(flow_module, "notify_flow_failure")
@patch.object(flow_module, "_state_relation_is_ready", return_value=True)
@patch.object(flow_module, "_git_sha", return_value="abc123")
@patch.object(flow_module, "_persist_result", return_value={})
@patch.object(flow_module, "raise_if_failed")
@patch.object(flow_module, "dbt_test_selector")
@patch.object(flow_module, "dbt_run_selector")
@patch.object(flow_module, "dbt_deps")
@patch.object(flow_module, "validate_measurement_sources")
@patch.object(flow_module, "load_measurement_source_registry")
@patch.object(flow_module, "ensure_ops_audit_tables")
@patch.object(flow_module, "get_engine")
@patch.object(flow_module, "get_settings")
def test_reset_flow_runs_state_publish_refresh_and_tests_in_order(
    mock_get_settings,
    mock_get_engine,
    _mock_ensure_ops,
    mock_load_registry,
    _mock_validate_sources,
    mock_deps,
    mock_run,
    mock_test,
    _mock_raise,
    _mock_persist,
    _mock_git_sha,
    _mock_state_ready,
    mock_notify,
    _mock_logger,
):
    settings = SimpleNamespace(DBT_TARGET="prod", SLACK_WEBHOOK_URL=None)
    engine = MagicMock()
    events: list[tuple] = []
    mock_get_settings.return_value = settings
    mock_get_engine.return_value = engine
    mock_load_registry.return_value = {"fiuna_airbyte": {}}
    mock_deps.side_effect = (
        lambda *_args, **_kwargs: events.append(("deps",)) or MagicMock()
    )
    mock_run.side_effect = (
        lambda _settings, **kwargs: events.append(("run", kwargs)) or MagicMock()
    )
    mock_test.side_effect = (
        lambda _settings, **kwargs: events.append(("test", kwargs)) or MagicMock()
    )

    _call(
        flow_module.canonical_shadow_publish,
        data_source_name="fiuna_airbyte",
        extracted_at_from=_dt(1),
        extracted_at_to=_dt(2),
        reset_shadow=True,
    )

    assert [event[0] for event in events] == ["deps", "run", "run", "run", "test"]
    assert events[1][1] == {
        "selector": "canonical_shadow_state",
        "full_refresh": True,
    }
    assert events[2][1]["selector"] == "canonical_shadow_publish"
    assert events[2][1]["full_refresh"] is True
    assert events[3][1]["selector"] == "canonical_shadow_state"
    assert events[4][1]["selector"] == "canonical_shadow_tests"
    assert (
        events[2][1]["vars_payload"]["measurement_shadow_anchor_mode"] == "stream_state"
    )
    engine.dispose.assert_called_once_with()
    mock_notify.assert_not_called()
    _mock_state_ready.assert_called_once_with(engine, "ops")


@patch.object(flow_module, "get_run_logger", return_value=MagicMock())
@patch.object(flow_module, "notify_flow_failure")
@patch.object(flow_module, "_state_relation_is_ready", return_value=False)
@patch.object(flow_module, "_git_sha", return_value=None)
@patch.object(flow_module, "_persist_result", return_value={})
@patch.object(flow_module, "raise_if_failed")
@patch.object(flow_module, "dbt_run_selector")
@patch.object(flow_module, "dbt_deps", return_value=MagicMock())
@patch.object(flow_module, "validate_measurement_sources")
@patch.object(flow_module, "load_measurement_source_registry", return_value={})
@patch.object(flow_module, "ensure_ops_audit_tables")
@patch.object(flow_module, "get_engine")
@patch.object(flow_module, "get_settings")
def test_non_reset_flow_requires_existing_shadow_state(
    mock_get_settings,
    mock_get_engine,
    _mock_ensure_ops,
    _mock_registry,
    _mock_validate,
    _mock_deps,
    mock_run,
    _mock_raise,
    _mock_persist,
    _mock_git,
    _mock_state_ready,
    mock_notify,
    _mock_logger,
):
    settings = SimpleNamespace(DBT_TARGET="prod", SLACK_WEBHOOK_URL=None)
    engine = MagicMock()
    mock_get_settings.return_value = settings
    mock_get_engine.return_value = engine

    with pytest.raises(RuntimeError, match="reset_shadow=True"):
        _call(
            flow_module.canonical_shadow_publish,
            data_source_name="fiuna_airbyte",
            extracted_at_from=_dt(1),
        )

    mock_run.assert_not_called()
    mock_notify.assert_called_once()
    engine.dispose.assert_called_once_with()
    _mock_state_ready.assert_called_once_with(engine, "shadow")
