from __future__ import annotations

from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]


def _read(relative_path: str) -> str:
    return (REPO_ROOT / relative_path).read_text(encoding="utf-8").lower()


def test_shadow_values_do_not_depend_on_legacy_value_history():
    sql = _read("dbt/models/canonical/shadow/int_measurements_values_silver_shadow.sql")

    assert "ref('int_measurements_long_shadow')" in sql
    assert "int_measurements_values_silver')" not in sql
    assert "source('shadow_runtime', 'measurement_stream_state')" in sql
    assert "ref('fct_measurements_silver')" in sql
    assert "fact.timestamp < cast(" in sql
    assert "forward_fill_last_valid" in sql
    assert ") < (" in sql


def test_shadow_state_only_advances_from_published_shadow_winners():
    sql = _read("dbt/models/canonical/shadow/measurement_stream_state_shadow.sql")

    assert "ref('fct_measurements_silver_shadow')" in sql
    assert "fact.source_row_id = values.source_row_id" in sql
    assert "coalesce(candidates.last_cursor_id, -1)" in sql
    assert ") > (" in sql
    assert "now() as updated_at" in sql
    assert "source('ops', 'measurement_stream_state')" in sql


def test_production_selectors_do_not_include_shadow_path():
    selectors = _read("dbt/selectors.yml")
    full_refresh = selectors.split("- name: canonical_full_refresh", maxsplit=1)[1]

    assert "models/canonical/shadow" not in full_refresh
    assert "- name: canonical_shadow_publish" in selectors
    assert "- name: canonical_shadow_state" in selectors


def test_shadow_models_are_aliased_to_isolated_contract_names():
    project = _read("dbt/dbt_project.yml")
    fact = _read("dbt/models/canonical/shadow/fct_measurements_silver_shadow.sql")
    state = _read("dbt/models/canonical/shadow/measurement_stream_state_shadow.sql")

    assert "+schema: shadow" not in project
    assert "schema='silver'" in fact
    assert "alias='fct_measurements_silver_shadow'" in fact
    assert "schema='ops'" in state
    assert "alias='measurement_stream_state_shadow'" in state
