from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace

from pipelines.tasks.dbt_tasks import _build_dbt_command, _timeout_for_command


def _settings() -> SimpleNamespace:
    return SimpleNamespace(
        DBT_PROJECT_DIR="/app/dbt",
        DBT_PROFILES_DIR="/app/dbt",
        DBT_TARGET="prod",
        DBT_THREADS=1,
    )


def test_build_dbt_command_starts_with_command_token():
    cmd = _build_dbt_command(
        _settings(), command="deps", selector=None, full_refresh=False
    )
    assert cmd[:2] == ["dbt", "deps"]
    assert "--project-dir" in cmd
    assert "--profiles-dir" in cmd
    assert "--threads" not in cmd


def test_build_dbt_command_supports_multiword_command():
    cmd = _build_dbt_command(
        _settings(), command="source freshness", selector=None, full_refresh=False
    )
    assert cmd[:3] == ["dbt", "source", "freshness"]


def test_build_dbt_command_adds_threads_for_run_like_commands():
    cmd = _build_dbt_command(
        _settings(), command="run", selector="canonical_core", full_refresh=False
    )
    assert "--threads" in cmd


def test_build_dbt_command_supports_seed_with_selector():
    cmd = _build_dbt_command(
        _settings(),
        command="seed",
        selector="project_respira_gold_seed",
        full_refresh=False,
    )

    assert cmd[:2] == ["dbt", "seed"]
    assert "--threads" in cmd
    assert "--selector" in cmd
    selector_index = cmd.index("--selector")
    assert cmd[selector_index + 1] == "project_respira_gold_seed"


def test_build_dbt_command_adds_vars_payload():
    cmd = _build_dbt_command(
        _settings(),
        command="run",
        selector="canonical_batch_process",
        full_refresh=False,
        vars_payload={
            "measurement_batch_data_source": "fiuna_airbyte",
            "measurement_batch_measured_at_from": datetime(
                2026, 1, 1, 0, 0, tzinfo=UTC
            ),
        },
    )

    assert "--vars" in cmd
    vars_index = cmd.index("--vars")
    assert "fiuna_airbyte" in cmd[vars_index + 1]
    assert "2026-01-01T00:00:00+00:00" in cmd[vars_index + 1]


def test_timeout_for_command_disables_zero_timeout():
    settings = SimpleNamespace(
        DBT_TIMEOUT_TESTS_S=1200,
        DBT_TIMEOUT_CANONICAL_CORE_S=0,
        DBT_TIMEOUT_CANONICAL_BATCH_INGEST_S=3600,
        DBT_TIMEOUT_CANONICAL_SILVER_S=1800,
        DBT_TIMEOUT_PROJECT_S=1200,
    )

    assert (
        _timeout_for_command(settings, command="run", selector="canonical_core") is None
    )


def test_shadow_publish_uses_silver_timeout():
    settings = SimpleNamespace(
        DBT_TIMEOUT_TESTS_S=1200,
        DBT_TIMEOUT_CANONICAL_CORE_S=0,
        DBT_TIMEOUT_CANONICAL_BATCH_INGEST_S=3600,
        DBT_TIMEOUT_CANONICAL_SILVER_S=1800,
        DBT_TIMEOUT_PROJECT_S=1200,
    )

    assert (
        _timeout_for_command(
            settings, command="run", selector="canonical_shadow_publish"
        )
        == 1800
    )


def test_incremental_state_refresh_uses_silver_timeout():
    settings = SimpleNamespace(
        DBT_TIMEOUT_TESTS_S=1200,
        DBT_TIMEOUT_CANONICAL_CORE_S=0,
        DBT_TIMEOUT_CANONICAL_BATCH_INGEST_S=3600,
        DBT_TIMEOUT_CANONICAL_SILVER_S=1800,
        DBT_TIMEOUT_PROJECT_S=1200,
    )

    assert (
        _timeout_for_command(
            settings, command="run", selector="canonical_incremental_state"
        )
        == 1800
    )
