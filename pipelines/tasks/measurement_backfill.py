from __future__ import annotations

import csv
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, TypedDict

import yaml  # type: ignore[import-untyped]
from sqlalchemy import text

from pipelines.compat import task

NO_CACHE: Any | None
try:
    from prefect.cache_policies import NO_CACHE as _PREFECT_NO_CACHE
except Exception:  # noqa: BLE001
    NO_CACHE = None
else:
    NO_CACHE = _PREFECT_NO_CACHE

INTERMEDIATE_SCHEMA = "intermediate"
TIMESTAMPS_TABLE = "int_measurement_timestamps_silver"


class MeasurementProcessBounds(TypedDict):
    min_measured_at: datetime | None
    max_measured_at: datetime | None
    null_time_row_count: int


def _read_csv_column(path: Path, column_name: str) -> set[str]:
    with path.open("r", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        return {
            (row.get(column_name) or "").strip()
            for row in reader
            if (row.get(column_name) or "").strip()
        }


def _dbt_root(settings) -> Path:
    return Path(settings.DBT_PROJECT_DIR)


def _load_measurement_source_registry(settings) -> dict[str, Any]:
    project_path = _dbt_root(settings) / "dbt_project.yml"
    data = yaml.safe_load(project_path.read_text(encoding="utf-8")) or {}
    registry = (data.get("vars") or {}).get("measurements_sources") or {}

    if not isinstance(registry, dict) or not registry:
        raise ValueError(
            "vars.measurements_sources is missing or empty in dbt_project.yml"
        )

    return registry


def _validate_measurement_sources(
    settings,
    source_registry: dict[str, Any],
    requested_sources: list[str] | None = None,
) -> list[str]:
    selected_sources = requested_sources or list(source_registry.keys())

    if not selected_sources:
        raise ValueError("At least one measurement source must be selected.")

    seeds_dir = _dbt_root(settings) / "seeds"
    data_sources = _read_csv_column(seeds_dir / "data_sources.csv", "name")
    variables = _read_csv_column(seeds_dir / "variables.csv", "code")
    variable_rules = _read_csv_column(seeds_dir / "variable_rules.csv", "variable_code")

    validated_sources: list[str] = []
    for source_name in selected_sources:
        if source_name not in source_registry:
            raise ValueError(
                f"Unknown measurement source '{source_name}'. "
                f"Expected one of: {', '.join(sorted(source_registry))}"
            )
        if source_name not in data_sources:
            raise ValueError(
                f"Measurement source '{source_name}' is missing from seeds/data_sources.csv."
            )

        cfg = source_registry[source_name] or {}
        variables_map = cfg.get("variables") or {}
        if not variables_map:
            raise ValueError(
                f"Measurement source '{source_name}' has no configured variables in dbt_project.yml."
            )

        missing_variables = sorted(
            variable_code
            for variable_code in variables_map
            if variable_code not in variables
        )
        if missing_variables:
            raise ValueError(
                f"Measurement source '{source_name}' uses variables missing from seeds/variables.csv: "
                f"{', '.join(missing_variables)}"
            )

        missing_rules = sorted(
            variable_code
            for variable_code in variables_map
            if variable_code not in variable_rules
        )
        if missing_rules:
            raise ValueError(
                f"Measurement source '{source_name}' uses variables missing from seeds/variable_rules.csv: "
                f"{', '.join(missing_rules)}"
            )

        validated_sources.append(source_name)

    return validated_sources


def _get_measurement_process_bounds(
    engine, data_source_name: str
) -> MeasurementProcessBounds:
    query = text(
        f"""
        select
            min(measured_at_silver) as min_measured_at,
            max(measured_at_silver) as max_measured_at,
            sum(case when measured_at_silver is null then 1 else 0 end) as null_time_row_count
        from {INTERMEDIATE_SCHEMA}.{TIMESTAMPS_TABLE}
        where data_source_name = :data_source_name
        """
    )

    with engine.begin() as conn:
        row = (
            conn.execute(query, {"data_source_name": data_source_name}).mappings().one()
        )

    return {
        "min_measured_at": row["min_measured_at"],
        "max_measured_at": row["max_measured_at"],
        "null_time_row_count": int(row["null_time_row_count"] or 0),
    }


def _build_measured_at_windows(
    measured_at_from: datetime | None,
    measured_at_to: datetime | None,
    batch_hours: int,
) -> list[dict[str, datetime]]:
    if batch_hours <= 0:
        raise ValueError("process_batch_hours must be greater than zero.")

    if (
        measured_at_from is None
        or measured_at_to is None
        or measured_at_from >= measured_at_to
    ):
        return []

    windows: list[dict[str, datetime]] = []
    current_start = measured_at_from
    step = timedelta(hours=batch_hours)

    while current_start < measured_at_to:
        current_end = min(current_start + step, measured_at_to)
        windows.append(
            {
                "measured_at_from": current_start,
                "measured_at_to": current_end,
            }
        )
        current_start = current_end

    return windows


@task(name="load_measurement_source_registry")
def load_measurement_source_registry(settings) -> dict[str, Any]:
    return _load_measurement_source_registry(settings)


@task(name="validate_measurement_sources")
def validate_measurement_sources(
    settings,
    source_registry: dict[str, Any],
    requested_sources: list[str] | None = None,
) -> list[str]:
    return _validate_measurement_sources(settings, source_registry, requested_sources)


@task(
    name="get_measurement_process_bounds",
    **({"cache_policy": NO_CACHE} if NO_CACHE is not None else {}),
)
def get_measurement_process_bounds(
    engine, data_source_name: str
) -> MeasurementProcessBounds:
    return _get_measurement_process_bounds(engine, data_source_name)


@task(name="build_measured_at_windows")
def build_measured_at_windows(
    measured_at_from: datetime | None,
    measured_at_to: datetime | None,
    batch_hours: int,
) -> list[dict[str, datetime]]:
    return _build_measured_at_windows(measured_at_from, measured_at_to, batch_hours)
