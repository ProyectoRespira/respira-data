from __future__ import annotations

import json
import sys
import types

import pandas as pd

from inference.feature_adapter import REQUIRED_COLUMNS, REQUIRED_FEATURE_COLUMNS, rows_to_feature_frame
from inference.predictor import WindowPredictor


class DummyModel:
    def predict(self, features_frame):
        return [12.5, 13.1]


def _sample_rows() -> list[dict]:
    base = {
        "station_id": 1001,
        "date_utc": "2026-01-01T00:00:00Z",
    }
    for col in REQUIRED_FEATURE_COLUMNS:
        base[col] = 1.0

    next_row = dict(base)
    next_row["date_utc"] = "2026-01-01T01:00:00Z"

    return [base, next_row]


def test_predictor_output_is_json_serializable():
    rows = _sample_rows()
    frame = rows_to_feature_frame(rows)

    predictor = WindowPredictor(model=DummyModel(), model_version="test-v1")
    output = predictor.predict_window(frame, horizon_hours=6)

    assert "meta" in output
    assert "points" in output
    assert output["meta"]["model_version"] == "test-v1"
    assert len(output["points"]) >= 1
    assert all("ts" in point and "yhat" in point for point in output["points"])

    json.dumps(output)


def test_rows_to_feature_frame_keeps_required_columns():
    frame = rows_to_feature_frame(_sample_rows())
    assert all(column in frame.columns for column in REQUIRED_COLUMNS)


def test_predictor_supports_darts_models(monkeypatch):
    rows = _sample_rows()
    frame = rows_to_feature_frame(rows)
    captured: dict[str, pd.DataFrame] = {}

    class FakePredictionSeries:
        def __init__(self, series: pd.Series):
            self._series = series

        def pd_series(self):
            return self._series

    class FakeTimeSeries:
        def __init__(self, frame: pd.DataFrame, selected_columns: list[str] | None = None):
            self._frame = frame.reset_index(drop=True)
            self._selected_columns = selected_columns

        @classmethod
        def from_dataframe(cls, frame: pd.DataFrame, time_col: str, value_cols: list[str], freq: str):
            captured["frame"] = frame.copy()
            assert time_col == "date_utc"
            assert freq == "h"
            return cls(frame[["date_utc", *value_cols]].copy())

        def __getitem__(self, key):
            if isinstance(key, list):
                return FakeTimeSeries(self._frame[["date_utc", *key]].copy(), key)
            return FakeTimeSeries(self._frame[["date_utc", key]].copy(), [key])

    fake_darts = types.ModuleType("darts")
    fake_darts.TimeSeries = FakeTimeSeries
    monkeypatch.setitem(sys.modules, "darts", fake_darts)

    class DummyDartsModel:
        def predict(self, horizon: int, series=None, past_covariates=None):
            assert horizon == 6
            assert series is not None
            assert past_covariates is not None

            start = pd.Timestamp("2026-01-01T04:00:00Z")
            index = pd.date_range(start=start, periods=horizon, freq="h", tz="UTC")
            values = pd.Series([50, 51, 52, 53, 54, 55], index=index)
            return FakePredictionSeries(values)

    predictor = WindowPredictor(model=DummyDartsModel(), model_version="darts-v1")
    output = predictor.predict_window(
        frame,
        horizon_hours=6,
        as_of=pd.Timestamp("2026-01-01T03:00:00Z"),
    )

    assert captured["frame"]["date_utc"].max() == pd.Timestamp("2026-01-01T03:00:00Z")
    assert output["meta"]["model_version"] == "darts-v1"
    assert len(output["points"]) == 6
    assert output["points"][0]["yhat"] == 50.0


# --- R7.2: Normalizer-specific tests ---

from inference.predictor import (
    _DartsSeriesNormalizer,
    _DataFrameNormalizer,
    _DictNormalizer,
    _IterableNormalizer,
    _ScalarNormalizer,
)

_SAMPLE_TIMESTAMPS = [pd.Timestamp("2026-01-01T00:00:00Z"), pd.Timestamp("2026-01-01T01:00:00Z")]
_POINT_KEYS = {"ts", "yhat", "yhat_lower", "yhat_upper"}


def test_darts_series_normalizer():
    class FakeSeries:
        def pd_series(self):
            index = pd.date_range("2026-01-01", periods=3, freq="h", tz="UTC")
            return pd.Series([10.0, 20.0, 30.0], index=index)

    n = _DartsSeriesNormalizer()
    fake = FakeSeries()
    assert n.can_handle(fake)
    points = n.normalize(fake, _SAMPLE_TIMESTAMPS, 6)
    assert len(points) == 3
    assert all(set(p.keys()) == _POINT_KEYS for p in points)
    assert points[0]["yhat"] == 10.0


def test_dataframe_normalizer():
    df = pd.DataFrame({
        "ts": ["2026-01-01T00:00:00Z", "2026-01-01T01:00:00Z"],
        "yhat": [1.5, 2.5],
        "yhat_lower": [1.0, 2.0],
        "yhat_upper": [2.0, 3.0],
    })
    n = _DataFrameNormalizer()
    assert n.can_handle(df)
    assert not n.can_handle({"a": 1})
    points = n.normalize(df, _SAMPLE_TIMESTAMPS, 6)
    assert len(points) == 2
    assert points[0]["yhat"] == 1.5


def test_dict_normalizer_single_point():
    raw = {"yhat": 42.0, "yhat_lower": 40.0, "yhat_upper": 44.0}
    n = _DictNormalizer()
    assert n.can_handle(raw)
    points = n.normalize(raw, _SAMPLE_TIMESTAMPS, 6)
    assert len(points) == 1
    assert points[0]["yhat"] == 42.0


def test_dict_normalizer_with_points_list():
    raw = {"points": [{"ts": "2026-01-01T00:00:00Z", "yhat": 5.0}]}
    n = _DictNormalizer()
    points = n.normalize(raw, _SAMPLE_TIMESTAMPS, 6)
    assert len(points) == 1
    assert points[0]["yhat"] == 5.0


def test_iterable_normalizer():
    raw = [1.0, 2.0, 3.0]
    n = _IterableNormalizer()
    assert n.can_handle(raw)
    assert n.can_handle((1,))
    points = n.normalize(raw, _SAMPLE_TIMESTAMPS, 6)
    assert len(points) == 3
    assert points[0]["yhat"] == 1.0


def test_scalar_normalizer():
    n = _ScalarNormalizer()
    assert n.can_handle(42.0)
    assert n.can_handle("not_a_number")
    points = n.normalize(42.0, _SAMPLE_TIMESTAMPS, 6)
    assert len(points) == 1
    assert points[0]["yhat"] == 42.0

    points_bad = n.normalize("not_a_number", _SAMPLE_TIMESTAMPS, 6)
    assert len(points_bad) == 1
    assert points_bad[0]["yhat"] is None
