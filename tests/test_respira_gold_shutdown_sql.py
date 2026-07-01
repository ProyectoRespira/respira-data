from __future__ import annotations

from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]


def _read_model(relative_path: str) -> str:
    return (REPO_ROOT / relative_path).read_text(encoding="utf-8")


def test_station_readings_gold_filters_inactive_air_quality_stations():
    sql = _read_model(
        "dbt/models/projects/respira_gold/gold/station_readings_gold.sql"
    )

    assert "ref('int_air_quality_stations')" in sql
    assert "where status = 'active'" in sql
    assert "join active_air_quality_stations active_stations" in sql


def test_region_readings_gold_excludes_stations_that_are_off():
    sql = _read_model(
        "dbt/models/projects/respira_gold/gold/region_readings_gold.sql"
    )

    assert "from {{ ref('stations') }}" in sql
    assert "and is_station_on" in sql
