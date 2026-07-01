from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, call, patch

from pipelines.config.projects import ProjectConfig
from pipelines.flows import project_pipeline as pipeline_module


def _call_flow(flow_or_fn, *args, **kwargs):
    fn = getattr(flow_or_fn, "fn", flow_or_fn)
    return fn(*args, **kwargs)


def _make_project() -> ProjectConfig:
    return ProjectConfig(
        project_code="respira_gold",
        dbt_selector="project_respira_gold",
        dbt_tests_selector="project_respira_gold_tests",
        schema_name="respira_gold",
        inference_enabled=True,
        inference_source_table="respira_gold.station_inference_features",
        inference_runs_table="respira_gold.inference_runs",
        inference_results_table="respira_gold.inference_results",
        dbt_seed_selector="project_respira_gold_seed",
        dbt_seed_tests_selector="project_respira_gold_seed_tests",
    )


def _make_settings() -> SimpleNamespace:
    return SimpleNamespace(
        DBT_TARGET="prod",
        SLACK_WEBHOOK_URL="https://example.test/slack",
    )


def _make_result(status: str, command: str) -> SimpleNamespace:
    return SimpleNamespace(
        status=status,
        command=command,
        run_results_path=None,
    )


@patch("pipelines.flows.project_pipeline.notify_flow_failure")
@patch("pipelines.flows.project_pipeline.notify_dbt_tests_failed")
@patch("pipelines.flows.project_pipeline.project_inference")
@patch("pipelines.flows.project_pipeline.persist_dbt_audit")
@patch("pipelines.flows.project_pipeline._summary_from_result")
@patch("pipelines.flows.project_pipeline.dbt_test_selector")
@patch("pipelines.flows.project_pipeline.dbt_run_selector")
@patch("pipelines.flows.project_pipeline.dbt_seed_selector")
@patch("pipelines.flows.project_pipeline.dbt_deps")
@patch("pipelines.flows.project_pipeline.get_project_config")
@patch("pipelines.flows.project_pipeline.get_settings")
@patch("pipelines.flows.project_pipeline.get_engine")
@patch("pipelines.flows.project_pipeline.ensure_ops_audit_tables")
@patch("pipelines.flows.project_pipeline.get_flow_context")
@patch("pipelines.flows.project_pipeline._git_sha")
def test_project_pipeline_runs_seed_and_seed_tests_before_project_run(
    mock_git_sha,
    mock_flow_context,
    mock_ensure_ops,
    mock_get_engine,
    mock_get_settings,
    mock_get_project,
    mock_dbt_deps,
    mock_dbt_seed_selector,
    mock_dbt_run_selector,
    mock_dbt_test_selector,
    mock_summary_from_result,
    mock_persist_dbt_audit,
    mock_project_inference,
    mock_notify_dbt_tests_failed,
    mock_notify_flow_failure,
):
    settings = _make_settings()
    project = _make_project()
    engine = MagicMock()

    mock_git_sha.return_value = "abc123"
    mock_flow_context.return_value = {}
    mock_get_settings.return_value = settings
    mock_get_project.return_value = project
    mock_get_engine.return_value = engine

    deps_result = _make_result("success", "dbt deps")
    seed_result = _make_result("success", "dbt seed")
    seed_tests_result = _make_result(
        "success", "dbt test project_respira_gold_seed_tests"
    )
    project_result = _make_result("success", "dbt run project_respira_gold")
    project_tests_result = _make_result(
        "success", "dbt test project_respira_gold_tests"
    )

    execution_order: list[str] = []

    mock_dbt_deps.side_effect = (
        lambda _settings: execution_order.append("deps") or deps_result
    )
    mock_dbt_seed_selector.side_effect = (
        lambda _settings, selector: execution_order.append(f"seed:{selector}")
        or seed_result
    )
    mock_dbt_run_selector.side_effect = (
        lambda _settings, selector: execution_order.append(f"run:{selector}")
        or project_result
    )

    def _test_selector_side_effect(_settings, selector):
        execution_order.append(f"test:{selector}")
        if selector == project.dbt_seed_tests_selector:
            return seed_tests_result
        return project_tests_result

    mock_dbt_test_selector.side_effect = _test_selector_side_effect
    mock_project_inference.side_effect = lambda **kwargs: execution_order.append(
        "inference"
    )
    mock_summary_from_result.side_effect = [
        {"tests_failed": 0, "tests_passed": 0},
        {"tests_failed": 0, "tests_passed": 0},
        {"tests_failed": 0, "tests_passed": 1},
        {"tests_failed": 0, "tests_passed": 0},
        {"tests_failed": 0, "tests_passed": 2},
    ]

    _call_flow(pipeline_module.project_pipeline, project_code="respira_gold")

    assert execution_order == [
        "deps",
        "seed:project_respira_gold_seed",
        "test:project_respira_gold_seed_tests",
        "run:project_respira_gold",
        "test:project_respira_gold_tests",
        "inference",
    ]
    assert mock_persist_dbt_audit.call_count == 5
    mock_notify_dbt_tests_failed.assert_not_called()
    mock_notify_flow_failure.assert_not_called()
    engine.dispose.assert_called_once_with()


@patch("pipelines.flows.project_pipeline.notify_flow_failure")
@patch("pipelines.flows.project_pipeline.notify_dbt_tests_failed")
@patch("pipelines.flows.project_pipeline.project_inference")
@patch("pipelines.flows.project_pipeline.persist_dbt_audit")
@patch("pipelines.flows.project_pipeline._summary_from_result")
@patch("pipelines.flows.project_pipeline.dbt_test_selector")
@patch("pipelines.flows.project_pipeline.dbt_run_selector")
@patch("pipelines.flows.project_pipeline.dbt_seed_selector")
@patch("pipelines.flows.project_pipeline.dbt_deps")
@patch("pipelines.flows.project_pipeline.get_project_config")
@patch("pipelines.flows.project_pipeline.get_settings")
@patch("pipelines.flows.project_pipeline.get_engine")
@patch("pipelines.flows.project_pipeline.ensure_ops_audit_tables")
@patch("pipelines.flows.project_pipeline.get_flow_context")
@patch("pipelines.flows.project_pipeline._git_sha")
def test_project_pipeline_aborts_when_seed_step_fails(
    mock_git_sha,
    mock_flow_context,
    mock_ensure_ops,
    mock_get_engine,
    mock_get_settings,
    mock_get_project,
    mock_dbt_deps,
    mock_dbt_seed_selector,
    mock_dbt_run_selector,
    mock_dbt_test_selector,
    mock_summary_from_result,
    mock_persist_dbt_audit,
    mock_project_inference,
    mock_notify_dbt_tests_failed,
    mock_notify_flow_failure,
):
    settings = _make_settings()
    project = _make_project()
    engine = MagicMock()

    mock_git_sha.return_value = "abc123"
    mock_flow_context.return_value = {}
    mock_get_settings.return_value = settings
    mock_get_project.return_value = project
    mock_get_engine.return_value = engine
    mock_dbt_deps.return_value = _make_result("success", "dbt deps")
    mock_dbt_seed_selector.return_value = _make_result("failed", "dbt seed")
    mock_summary_from_result.side_effect = [
        {"tests_failed": 0, "tests_passed": 0},
        {"tests_failed": 0, "tests_passed": 0},
    ]

    with patch(
        "pipelines.flows.project_pipeline.raise_if_failed",
        side_effect=pipeline_module.raise_if_failed,
    ) as mock_raise_if_failed:
        try:
            _call_flow(pipeline_module.project_pipeline, project_code="respira_gold")
        except RuntimeError as exc:
            assert "dbt project seed stage failed" in str(exc)
        else:
            raise AssertionError("project_pipeline should have raised on seed failure")

    mock_dbt_test_selector.assert_not_called()
    mock_dbt_run_selector.assert_not_called()
    mock_project_inference.assert_not_called()
    mock_notify_dbt_tests_failed.assert_not_called()
    mock_notify_flow_failure.assert_called_once()
    assert mock_raise_if_failed.call_args_list[:2] == [
        call(mock_dbt_deps.return_value, "dbt deps failed"),
        call(
            mock_dbt_seed_selector.return_value,
            "dbt project seed stage failed for respira_gold",
        ),
    ]
    engine.dispose.assert_called_once_with()


@patch("pipelines.flows.project_pipeline.notify_flow_failure")
@patch("pipelines.flows.project_pipeline.notify_dbt_tests_failed")
@patch("pipelines.flows.project_pipeline.project_inference")
@patch("pipelines.flows.project_pipeline.persist_dbt_audit")
@patch("pipelines.flows.project_pipeline._summary_from_result")
@patch("pipelines.flows.project_pipeline.dbt_test_selector")
@patch("pipelines.flows.project_pipeline.dbt_run_selector")
@patch("pipelines.flows.project_pipeline.dbt_seed_selector")
@patch("pipelines.flows.project_pipeline.dbt_deps")
@patch("pipelines.flows.project_pipeline.get_project_config")
@patch("pipelines.flows.project_pipeline.get_settings")
@patch("pipelines.flows.project_pipeline.get_engine")
@patch("pipelines.flows.project_pipeline.ensure_ops_audit_tables")
@patch("pipelines.flows.project_pipeline.get_flow_context")
@patch("pipelines.flows.project_pipeline._git_sha")
def test_project_pipeline_keeps_project_test_alert_behavior_after_seed_validation(
    mock_git_sha,
    mock_flow_context,
    mock_ensure_ops,
    mock_get_engine,
    mock_get_settings,
    mock_get_project,
    mock_dbt_deps,
    mock_dbt_seed_selector,
    mock_dbt_run_selector,
    mock_dbt_test_selector,
    mock_summary_from_result,
    mock_persist_dbt_audit,
    mock_project_inference,
    mock_notify_dbt_tests_failed,
    mock_notify_flow_failure,
):
    settings = _make_settings()
    project = _make_project()
    engine = MagicMock()

    mock_git_sha.return_value = "abc123"
    mock_flow_context.return_value = {}
    mock_get_settings.return_value = settings
    mock_get_project.return_value = project
    mock_get_engine.return_value = engine
    mock_dbt_deps.return_value = _make_result("success", "dbt deps")
    mock_dbt_seed_selector.return_value = _make_result("success", "dbt seed")
    mock_dbt_run_selector.return_value = _make_result(
        "success", "dbt run project_respira_gold"
    )

    seed_tests_result = _make_result(
        "success", "dbt test project_respira_gold_seed_tests"
    )
    project_tests_result = _make_result("failed", "dbt test project_respira_gold_tests")

    mock_dbt_test_selector.side_effect = [
        seed_tests_result,
        project_tests_result,
    ]
    mock_summary_from_result.side_effect = [
        {"tests_failed": 0, "tests_passed": 0},
        {"tests_failed": 0, "tests_passed": 0},
        {"tests_failed": 0, "tests_passed": 1},
        {"tests_failed": 0, "tests_passed": 0},
        {
            "tests_failed": 1,
            "tests_passed": 3,
            "error_summary": "station status override mismatch",
        },
    ]

    _call_flow(pipeline_module.project_pipeline, project_code="respira_gold")

    mock_notify_dbt_tests_failed.assert_called_once()
    mock_project_inference.assert_called_once_with(
        project_code="respira_gold",
        as_of=None,
        engine=engine,
    )
    mock_notify_flow_failure.assert_not_called()
    engine.dispose.assert_called_once_with()
