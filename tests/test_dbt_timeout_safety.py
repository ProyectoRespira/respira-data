from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

from pipelines.tasks import dbt_tasks


def test_dbt_application_name_is_unique_scoped_and_postgres_safe():
    first = dbt_tasks._dbt_application_name("run", "canonical_silver")
    second = dbt_tasks._dbt_application_name("run", "canonical_silver")

    assert first.startswith("respira_canonical_silver_")
    assert first != second
    assert len(first) <= 63


def test_clear_stale_run_results_removes_previous_artifact(tmp_path):
    artifact = tmp_path / "run_results.json"
    artifact.write_text("{}", encoding="utf-8")

    dbt_tasks._clear_stale_run_results(str(artifact))

    assert not artifact.exists()


def test_cancel_backends_targets_exact_application_name(monkeypatch):
    engine = MagicMock()
    begin_connection = engine.begin.return_value.__enter__.return_value
    pid_result = MagicMock()
    pid_result.scalars.return_value = [123]
    cancel_result = MagicMock()
    cancel_result.scalar_one.return_value = True
    begin_connection.execute.side_effect = [pid_result, cancel_result]
    poll_connection = engine.connect.return_value.__enter__.return_value
    poll_result = MagicMock()
    poll_result.scalars.return_value = []
    poll_connection.execute.return_value = poll_result
    monkeypatch.setattr(dbt_tasks, "create_engine", lambda *_args, **_kwargs: engine)
    settings = SimpleNamespace(database_dsn=lambda: "postgresql://unused")

    cancelled, terminated = dbt_tasks._cancel_tagged_backends(
        settings, "respira_canonical_silver_exact", MagicMock(), grace_s=0.1
    )

    assert (cancelled, terminated) == (1, 0)
    first_params = begin_connection.execute.call_args_list[0].args[1]
    assert first_params == {"application_name": "respira_canonical_silver_exact"}
    engine.dispose.assert_called_once_with()


def test_guarded_timeout_uses_cancellable_subprocess_even_when_prefect_dbt_enabled(
    monkeypatch,
):
    settings = SimpleNamespace(
        DBT_USE_PREFECT_DBT=True,
        DBT_TIMEOUT_CANONICAL_SILVER_S=1_800,
    )
    subprocess_result = object()
    prefect_runner = MagicMock()
    monkeypatch.setattr(dbt_tasks, "_run_with_prefect_dbt_if_available", prefect_runner)
    monkeypatch.setattr(
        dbt_tasks, "_run_subprocess", lambda *_args, **_kwargs: subprocess_result
    )

    result = dbt_tasks._run_dbt(
        settings,
        command="run",
        selector="canonical_silver",
        full_refresh=False,
    )

    assert result is subprocess_result
    prefect_runner.assert_not_called()
