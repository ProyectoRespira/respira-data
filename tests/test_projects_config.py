from __future__ import annotations

import pytest

from pipelines.config.projects import get_project_config, list_project_configs


def test_get_project_config_respira_gold():
    project = get_project_config("respira_gold")

    assert project.project_code == "respira_gold"
    assert project.dbt_selector == "project_respira_gold"
    assert project.dbt_tests_selector == "project_respira_gold_tests"
    assert project.schema_name == "respira_gold"


def test_get_project_config_rejects_unknown_project():
    with pytest.raises(ValueError):
        get_project_config("unknown_project")


def test_list_project_configs_returns_registered_projects():
    project_codes = {project.project_code for project in list_project_configs()}
    assert "respira_gold" in project_codes


def test_project_config_accepts_valid_sql_identifiers():
    from pipelines.config.projects import ProjectConfig

    config = ProjectConfig(
        project_code="test_project",
        dbt_selector="project_test",
        dbt_tests_selector="project_test_tests",
        schema_name="test_schema",
        inference_enabled=False,
        inference_source_table="test_schema.source_table",
        inference_runs_table="test_schema.inference_runs",
        inference_results_table="test_schema.inference_results",
    )
    assert config.schema_name == "test_schema"


@pytest.mark.parametrize(
    "field_name,bad_value",
    [
        ("schema_name", "respira_gold; DROP TABLE--"),
        ("inference_source_table", "schema.table; DELETE FROM"),
        ("inference_runs_table", "UPPER_CASE_TABLE"),
        ("inference_results_table", "table with spaces"),
    ],
)
def test_project_config_rejects_unsafe_sql_identifiers(field_name, bad_value):
    from pipelines.config.projects import ProjectConfig

    defaults = {
        "project_code": "test",
        "dbt_selector": "sel",
        "dbt_tests_selector": "sel_tests",
        "schema_name": "safe_schema",
        "inference_enabled": False,
        "inference_source_table": "safe_schema.source",
        "inference_runs_table": "safe_schema.runs",
        "inference_results_table": "safe_schema.results",
    }
    defaults[field_name] = bad_value

    with pytest.raises(ValueError, match="unsafe SQL identifier"):
        ProjectConfig(**defaults)
