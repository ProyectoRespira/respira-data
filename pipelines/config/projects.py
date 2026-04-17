from __future__ import annotations

import re
from dataclasses import dataclass

_SAFE_SQL_IDENTIFIER = re.compile(r"^[a-z_][a-z0-9_.]*$")


def is_safe_sql_identifier(value: str) -> bool:
    return bool(value) and isinstance(value, str) and _SAFE_SQL_IDENTIFIER.match(value) is not None


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

    def __post_init__(self) -> None:
        for field_name in (
            "schema_name",
            "inference_source_table",
            "inference_runs_table",
            "inference_results_table",
        ):
            value = getattr(self, field_name)
            if not _SAFE_SQL_IDENTIFIER.match(value):
                raise ValueError(
                    f"ProjectConfig.{field_name} contains unsafe SQL identifier: {value!r}"
                )


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
