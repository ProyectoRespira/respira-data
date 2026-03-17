from __future__ import annotations

import json

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
