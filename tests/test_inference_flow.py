from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch
from uuid import uuid4

import pytest

from pipelines.flows.project_inference import InferenceRunParams, _process_single_station


def _make_params(**overrides) -> InferenceRunParams:
    defaults = {
        "as_of": datetime(2025, 1, 1, 12, 0, tzinfo=timezone.utc),
        "window_hours": 24,
        "min_points": 18,
        "model_6h_path": "/models/6h.pkl",
        "model_12h_path": "/models/12h.pkl",
        "model_6h_version": "v1",
        "model_12h_version": "v1",
    }
    defaults.update(overrides)
    return InferenceRunParams(**defaults)


def _make_project():
    from pipelines.config.projects import ProjectConfig

    return ProjectConfig(
        project_code="test_project",
        dbt_selector="project_test",
        dbt_tests_selector="project_test_tests",
        schema_name="test_schema",
        inference_enabled=True,
        inference_source_table="test_schema.source",
        inference_runs_table="test_schema.runs",
        inference_results_table="test_schema.results",
    )


def _make_row(date_hour: int) -> dict:
    from inference.feature_adapter import REQUIRED_FEATURE_COLUMNS

    row = {
        "station_id": 1,
        "date_utc": datetime(2025, 1, 1, date_hour, 0, tzinfo=timezone.utc),
    }
    for col in REQUIRED_FEATURE_COLUMNS:
        row[col] = 1.0
    return row


@patch("pipelines.flows.project_inference.get_run_logger", return_value=MagicMock())
@patch("pipelines.flows.project_inference.persist_station_status")
@patch("pipelines.flows.project_inference.persist_inference_result")
@patch("pipelines.flows.project_inference.load_station_window")
def test_process_station_success(mock_load, mock_persist_result, mock_persist_status, _mock_logger):
    rows = [_make_row(h) for h in range(18)]
    mock_load.return_value = rows

    predictor_6h = MagicMock()
    predictor_6h.predict_window.return_value = {"meta": {}, "points": [{"ts": "t", "yhat": 1.0}]}
    predictor_12h = MagicMock()
    predictor_12h.predict_window.return_value = {"meta": {}, "points": [{"ts": "t", "yhat": 2.0}]}

    result = _process_single_station(
        engine=MagicMock(),
        project=_make_project(),
        station_id=1,
        params=_make_params(),
        predictor_6h=predictor_6h,
        predictor_12h=predictor_12h,
        inference_run_id=uuid4(),
    )

    assert result == "success"
    assert mock_persist_result.call_count == 2
    assert mock_persist_status.call_count == 1
    status_call = mock_persist_status.call_args
    assert status_call.kwargs["status"] == "success"


@patch("pipelines.flows.project_inference.get_run_logger", return_value=MagicMock())
@patch("pipelines.flows.project_inference.persist_station_status")
@patch("pipelines.flows.project_inference.persist_inference_result")
@patch("pipelines.flows.project_inference.load_station_window")
def test_process_station_skipped_insufficient_rows(mock_load, mock_persist_result, mock_persist_status, _mock_logger):
    mock_load.return_value = [_make_row(h) for h in range(5)]

    result = _process_single_station(
        engine=MagicMock(),
        project=_make_project(),
        station_id=1,
        params=_make_params(),
        predictor_6h=MagicMock(),
        predictor_12h=MagicMock(),
        inference_run_id=uuid4(),
    )

    assert result == "skipped"
    assert mock_persist_result.call_count == 0
    assert mock_persist_status.call_count == 1
    status_call = mock_persist_status.call_args
    assert status_call.kwargs["status"] == "skipped"


@patch("pipelines.flows.project_inference.get_run_logger", return_value=MagicMock())
@patch("pipelines.flows.project_inference.persist_station_status")
@patch("pipelines.flows.project_inference.persist_inference_result")
@patch("pipelines.flows.project_inference.load_station_window")
def test_process_station_failed_on_exception(mock_load, mock_persist_result, mock_persist_status, _mock_logger):
    mock_load.side_effect = RuntimeError("DB connection lost")

    result = _process_single_station(
        engine=MagicMock(),
        project=_make_project(),
        station_id=1,
        params=_make_params(),
        predictor_6h=MagicMock(),
        predictor_12h=MagicMock(),
        inference_run_id=uuid4(),
    )

    assert result == "failed"
    assert mock_persist_result.call_count == 0
    assert mock_persist_status.call_count == 1
    status_call = mock_persist_status.call_args
    assert status_call.kwargs["status"] == "failed"
    assert "DB connection lost" in status_call.kwargs["reason_detail"]
