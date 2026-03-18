from __future__ import annotations

from types import SimpleNamespace

from pipelines.tasks.dbt_tasks import _build_dbt_command


def _settings() -> SimpleNamespace:
    return SimpleNamespace(
        DBT_PROJECT_DIR="/app/dbt",
        DBT_PROFILES_DIR="/app/dbt",
        DBT_TARGET="prod",
        DBT_THREADS=1,
    )


def test_build_dbt_command_starts_with_command_token():
    cmd = _build_dbt_command(_settings(), command="deps", selector=None, full_refresh=False)
    assert cmd[:2] == ["dbt", "deps"]
    assert "--project-dir" in cmd
    assert "--profiles-dir" in cmd
    assert "--threads" not in cmd


def test_build_dbt_command_supports_multiword_command():
    cmd = _build_dbt_command(_settings(), command="source freshness", selector=None, full_refresh=False)
    assert cmd[:3] == ["dbt", "source", "freshness"]


def test_build_dbt_command_adds_threads_for_run_like_commands():
    cmd = _build_dbt_command(_settings(), command="run", selector="canonical_core", full_refresh=False)
    assert "--threads" in cmd
