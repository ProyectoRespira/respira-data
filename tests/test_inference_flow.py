from __future__ import annotations

from datetime import datetime, timezone
from typing import Any
from unittest.mock import MagicMock, patch
from uuid import uuid4

import pandas as pd

from pipelines.flows.project_inference import (
    InferenceRunParams,
    _aqi_input_points,
    _process_single_station,
    _storage_points_from_prediction,
)


def _make_params(**overrides: Any) -> InferenceRunParams:
    defaults: dict[str, Any] = {
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
    predictor_6h.predict_prepared_window.return_value = {"meta": {}, "points": [{"ts": "t", "yhat": 1.0}]}
    predictor_12h = MagicMock()
    predictor_12h.predict_prepared_window.return_value = {"meta": {}, "points": [{"ts": "t", "yhat": 2.0}]}

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
    assert mock_persist_result.call_count == 1
    assert mock_persist_status.call_count == 1
    predictor_6h.predict_prepared_window.assert_called_once()
    predictor_12h.predict_prepared_window.assert_called_once()
    assert predictor_6h.predict_prepared_window.call_args.kwargs["horizon_hours"] == 6
    assert predictor_12h.predict_prepared_window.call_args.kwargs["horizon_hours"] == 12
    status_call = mock_persist_status.call_args
    assert status_call.kwargs["status"] == "success"
    result_call = mock_persist_result.call_args
    assert result_call.kwargs["forecast_6h"][0]["value"] == 1.0
    assert result_call.kwargs["forecast_12h"][0]["value"] == 2.0
    assert "aqi_input" in result_call.kwargs


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


def test_storage_points_from_prediction_uses_value_and_timestamp():
    prediction = {
        "meta": {},
        "points": [
            {"ts": "2026-03-30T04:00:00Z", "yhat": 50.0},
            {"ts": "2026-03-30T05:00:00Z", "yhat": 49.2},
        ],
    }

    assert _storage_points_from_prediction(prediction) == [
        {"value": 50, "timestamp": "2026-03-30T04:00:00"},
        {"value": 49, "timestamp": "2026-03-30T05:00:00"},
    ]


def test_aqi_input_points_uses_value_and_timestamp():
    frame = pd.DataFrame({
        "date_utc": [
            pd.Timestamp("2026-03-29T03:00:00Z"),
            pd.Timestamp("2026-03-29T04:00:00Z"),
        ],
        "aqi_pm2_5": [37.0, 36.0],
    })

    assert _aqi_input_points(frame) == [
        {"value": 37, "timestamp": "2026-03-29T03:00:00"},
        {"value": 36, "timestamp": "2026-03-29T04:00:00"},
    ]
