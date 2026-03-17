from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

import pandas as pd


class WindowPredictor:
    def __init__(self, model: Any, model_version: str = "unknown"):
        if not hasattr(model, "predict"):
            raise TypeError("Model must implement predict")
        self.model = model
        self.model_version = model_version

    def predict_window(self, features_frame: pd.DataFrame, horizon_hours: int) -> dict[str, Any]:
        if features_frame.empty:
            raise ValueError("features_frame is empty")

        raw_predictions = self.model.predict(features_frame)
        generated_at = datetime.now(timezone.utc).isoformat()

        points = _normalize_predictions(raw_predictions, features_frame, horizon_hours)
        return {
            "meta": {
                "model_version": self.model_version,
                "generated_at": generated_at,
                "horizon_hours": horizon_hours,
            },
            "points": points,
        }


def predict_window(model: Any, features_frame: pd.DataFrame, horizon_hours: int, model_version: str = "unknown") -> dict[str, Any]:
    return WindowPredictor(model=model, model_version=model_version).predict_window(
        features_frame=features_frame,
        horizon_hours=horizon_hours,
    )


def _normalize_predictions(raw_predictions: Any, features_frame: pd.DataFrame, horizon_hours: int) -> list[dict[str, Any]]:
    timestamps = list(features_frame["date_utc"])
    if not timestamps:
        timestamps = [datetime.now(timezone.utc)]

    if isinstance(raw_predictions, pd.DataFrame):
        return _from_dataframe(raw_predictions)

    if isinstance(raw_predictions, dict):
        existing_points = raw_predictions.get("points")
        if isinstance(existing_points, list):
            return _normalize_point_dicts(existing_points)
        value = raw_predictions.get("yhat")
        ts = raw_predictions.get("ts") or _fallback_ts(timestamps, horizon_hours)
        return [_point(ts, value, raw_predictions.get("yhat_lower"), raw_predictions.get("yhat_upper"))]

    if isinstance(raw_predictions, (list, tuple)):
        return _from_iterable(raw_predictions, timestamps, horizon_hours)

    try:
        scalar = float(raw_predictions)
        ts = _fallback_ts(timestamps, horizon_hours)
        return [_point(ts, scalar, None, None)]
    except (TypeError, ValueError):
        ts = _fallback_ts(timestamps, horizon_hours)
        return [_point(ts, None, None, None)]


def _from_dataframe(frame: pd.DataFrame) -> list[dict[str, Any]]:
    points: list[dict[str, Any]] = []
    for _, row in frame.iterrows():
        ts = row.get("ts") or row.get("date_utc") or row.get("timestamp")
        points.append(
            _point(
                ts,
                row.get("yhat"),
                row.get("yhat_lower"),
                row.get("yhat_upper"),
            )
        )
    return points


def _from_iterable(predictions: list[Any] | tuple[Any, ...], timestamps: list[Any], horizon_hours: int) -> list[dict[str, Any]]:
    points: list[dict[str, Any]] = []
    start_ts = _fallback_ts(timestamps, 1)

    for idx, item in enumerate(predictions):
        if isinstance(item, dict):
            points.append(
                _point(
                    item.get("ts") or _shift_ts(start_ts, idx),
                    item.get("yhat"),
                    item.get("yhat_lower"),
                    item.get("yhat_upper"),
                )
            )
            continue

        yhat = None
        yhat_lower = None
        yhat_upper = None

        if isinstance(item, (list, tuple)):
            if len(item) > 0:
                yhat = item[0]
            if len(item) > 1:
                yhat_lower = item[1]
            if len(item) > 2:
                yhat_upper = item[2]
        else:
            yhat = item

        ts = _shift_ts(start_ts, idx)
        points.append(_point(ts, yhat, yhat_lower, yhat_upper))

    if not points:
        ts = _fallback_ts(timestamps, horizon_hours)
        points.append(_point(ts, None, None, None))

    return points


def _normalize_point_dicts(points: list[Any]) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    for item in points:
        if isinstance(item, dict):
            normalized.append(_point(item.get("ts"), item.get("yhat"), item.get("yhat_lower"), item.get("yhat_upper")))
    return normalized


def _fallback_ts(timestamps: list[Any], horizon_hours: int) -> str:
    if timestamps:
        ts = timestamps[-1]
        if hasattr(ts, "to_pydatetime"):
            ts = ts.to_pydatetime()
        if isinstance(ts, datetime):
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=timezone.utc)
            return (ts + timedelta(hours=horizon_hours)).isoformat()
    return datetime.now(timezone.utc).isoformat()


def _shift_ts(start_ts: str, hours: int) -> str:
    parsed = pd.to_datetime(start_ts, utc=True)
    shifted = parsed + pd.Timedelta(hours=hours)
    return shifted.isoformat()


def _point(ts: Any, yhat: Any, yhat_lower: Any, yhat_upper: Any) -> dict[str, Any]:
    ts_parsed = pd.to_datetime(ts, utc=True, errors="coerce")
    if pd.isna(ts_parsed):
        ts_value = datetime.now(timezone.utc).isoformat()
    else:
        ts_value = ts_parsed.isoformat()

    def _num(value: Any) -> float | None:
        if value is None:
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    return {
        "ts": ts_value,
        "yhat": _num(yhat),
        "yhat_lower": _num(yhat_lower),
        "yhat_upper": _num(yhat_upper),
    }
