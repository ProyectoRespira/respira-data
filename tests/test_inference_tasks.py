from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import MagicMock

from pipelines.tasks.inference_tasks import list_candidate_stations


def _make_project():
    from pipelines.config.projects import ProjectConfig

    return ProjectConfig(
        project_code="respira_gold",
        dbt_selector="project_respira_gold",
        dbt_tests_selector="project_respira_gold_tests",
        schema_name="respira_gold",
        inference_enabled=True,
        inference_source_table="respira_gold.station_inference_features",
        inference_runs_table="respira_gold.inference_runs",
        inference_results_table="respira_gold.inference_results",
    )


def test_list_candidate_stations_filters_manual_shutdowns():
    engine = MagicMock()
    connection = engine.connect.return_value.__enter__.return_value
    connection.execute.return_value.all.return_value = [(3,), (7,)]
    as_of = datetime(2025, 1, 2, 12, 0, tzinfo=UTC)

    station_ids = list_candidate_stations(
        engine,
        _make_project(),
        as_of=as_of,
        window_hours=24,
    )

    assert station_ids == [3, 7]

    execute_call = connection.execute.call_args
    query = str(execute_call.args[0])
    params = execute_call.args[1]

    assert "join respira_gold.stations stations" in query
    assert "stations.is_station_on = true" in query
    assert params["as_of"] == as_of
    assert params["window_start"] == datetime(2025, 1, 1, 12, 0, tzinfo=UTC)
