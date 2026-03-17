from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ProjectConfig:
    project_code: str
    dbt_selector: str
    dbt_tests_selector: str
    schema_name: str
    inference_enabled: bool
    inference_source_table: str
    inference_runs_table: str
    inference_results_table: str


PROJECTS: dict[str, ProjectConfig] = {
    "respira_gold": ProjectConfig(
        project_code="respira_gold",
        dbt_selector="project_respira_gold",
        dbt_tests_selector="project_respira_gold_tests",
        schema_name="respira_gold",
        inference_enabled=True,
        inference_source_table="respira_gold.station_inference_features",
        inference_runs_table="respira_gold.inference_runs",
        inference_results_table="respira_gold.inference_results",
    )
}


def get_project_config(project_code: str) -> ProjectConfig:
    try:
        return PROJECTS[project_code]
    except KeyError as exc:
        supported = ", ".join(sorted(PROJECTS))
        raise ValueError(f"Unknown project_code '{project_code}'. Supported values: {supported}") from exc


def list_project_configs() -> list[ProjectConfig]:
    return list(PROJECTS.values())
