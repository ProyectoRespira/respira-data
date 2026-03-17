from __future__ import annotations

import pytest

from config.projects import get_project_config, list_project_configs


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
