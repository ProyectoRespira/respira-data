from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

import pandas as pd


TARGET_COLUMN = "aqi_pm2_5"
NON_COVARIATE_COLUMNS = {"date_utc", "station_id"}


class WindowPredictor:
    def __init__(self, model: Any, model_version: str = "unknown"):
        if not hasattr(model, "predict"):
            raise TypeError("Model must implement predict")
        self.model = model
        self.model_version = model_version

    def predict_window(
        self,
        features_frame: pd.DataFrame,
        horizon_hours: int,
        as_of: datetime | None = None,
    ) -> dict[str, Any]:
        if features_frame.empty:
            raise ValueError("features_frame is empty")

        prepared_frame = _prepare_prediction_frame(features_frame, as_of=as_of)
        return self.predict_prepared_window(prepared_frame, horizon_hours)

    def predict_prepared_window(
        self,
        prepared_frame: pd.DataFrame,
        horizon_hours: int,
    ) -> dict[str, Any]:
        if prepared_frame.empty:
            raise ValueError("prepared_frame is empty")

        raw_predictions = _predict(self.model, prepared_frame, horizon_hours)
        generated_at = datetime.now(timezone.utc).isoformat()

        points = _normalize_predictions(raw_predictions, prepared_frame, horizon_hours)
        return {
            "meta": {
                "model_version": self.model_version,
                "generated_at": generated_at,
                "horizon_hours": horizon_hours,
            },
            "points": points,
        }


def predict_window(
    model: Any,
    features_frame: pd.DataFrame,
    horizon_hours: int,
    model_version: str = "unknown",
    as_of: datetime | None = None,
) -> dict[str, Any]:
    return WindowPredictor(model=model, model_version=model_version).predict_window(
        features_frame=features_frame,
        horizon_hours=horizon_hours,
        as_of=as_of,
    )


def prepare_prediction_frame(features_frame: pd.DataFrame, as_of: datetime | None = None) -> pd.DataFrame:
    return _prepare_prediction_frame(features_frame, as_of)


def _predict(model: Any, features_frame: pd.DataFrame, horizon_hours: int) -> Any:
    try:
        return _predict_with_darts(model, features_frame, horizon_hours)
    except (ImportError, TypeError, ValueError, AttributeError):
        return model.predict(features_frame)


def _predict_with_darts(model: Any, features_frame: pd.DataFrame, horizon_hours: int) -> Any:
    from darts import TimeSeries  # type: ignore[import-untyped]

    covariate_columns = [column for column in features_frame.columns if column not in NON_COVARIATE_COLUMNS]
    series_columns = [TARGET_COLUMN, *[column for column in covariate_columns if column != TARGET_COLUMN]]
    input_frame = features_frame[["date_utc", *series_columns]].copy()
    input_frame["date_utc"] = pd.to_datetime(input_frame["date_utc"], utc=True, errors="coerce")
    darts_frame = input_frame.copy()
    # Darts expects a tz-naive datetime index; keep values normalized to UTC and
    # strip timezone info only for the conversion boundary.
    darts_frame["date_utc"] = darts_frame["date_utc"].dt.tz_convert(None)

    ts = TimeSeries.from_dataframe(darts_frame, time_col="date_utc", value_cols=series_columns, freq="h")
    target_series = ts[TARGET_COLUMN]

    if covariate_columns:
        return model.predict(horizon_hours, series=target_series, past_covariates=ts[covariate_columns])
    return model.predict(horizon_hours, series=target_series)


def _prepare_prediction_frame(features_frame: pd.DataFrame, as_of: datetime | None) -> pd.DataFrame:
    frame = features_frame.reset_index(drop=True).copy()
    frame["date_utc"] = pd.to_datetime(frame["date_utc"], utc=True, errors="coerce")
    frame = frame.dropna(subset=["date_utc"]).sort_values("date_utc", ascending=True)

    duplicate_mask = frame.duplicated(subset=["date_utc"], keep="last")
    if duplicate_mask.any():
        frame = frame[~duplicate_mask]

    end_ts = pd.to_datetime(as_of, utc=True) if as_of is not None else frame["date_utc"].max()
    if pd.isna(end_ts):
        raise ValueError("Unable to determine prediction end timestamp")

    full_range = pd.date_range(start=frame["date_utc"].min(), end=end_ts, freq="h", tz="UTC")
    frame = frame.set_index("date_utc").reindex(full_range).rename_axis("date_utc").reset_index()

    value_columns = [column for column in frame.columns if column != "date_utc"]
    if value_columns:
        frame[value_columns] = frame[value_columns].ffill().bfill()

    return frame


class _DartsSeriesNormalizer:
    def can_handle(self, raw: Any) -> bool:
        return hasattr(raw, "pd_series")

    def normalize(self, raw: Any, timestamps: list[Any], horizon_hours: int) -> list[dict[str, Any]]:
        pandas_series = raw.pd_series().round(0)
        return [_point(ts, value, None, None) for ts, value in pandas_series.items()]


class _DataFrameNormalizer:
    def can_handle(self, raw: Any) -> bool:
        return isinstance(raw, pd.DataFrame)

    def normalize(self, raw: Any, timestamps: list[Any], horizon_hours: int) -> list[dict[str, Any]]:
        points: list[dict[str, Any]] = []
        for _, row in raw.iterrows():
            ts = row.get("ts") or row.get("date_utc") or row.get("timestamp")
            points.append(_point(ts, row.get("yhat"), row.get("yhat_lower"), row.get("yhat_upper")))
        return points


class _DictNormalizer:
    def can_handle(self, raw: Any) -> bool:
        return isinstance(raw, dict)

    def normalize(self, raw: Any, timestamps: list[Any], horizon_hours: int) -> list[dict[str, Any]]:
        existing_points = raw.get("points")
        if isinstance(existing_points, list):
            return _normalize_point_dicts(existing_points)
        value = raw.get("yhat")
        ts = raw.get("ts") or _fallback_ts(timestamps, horizon_hours)
        return [_point(ts, value, raw.get("yhat_lower"), raw.get("yhat_upper"))]


class _IterableNormalizer:
    def can_handle(self, raw: Any) -> bool:
        return isinstance(raw, (list, tuple))

    def normalize(self, raw: Any, timestamps: list[Any], horizon_hours: int) -> list[dict[str, Any]]:
        points: list[dict[str, Any]] = []
        start_ts = _fallback_ts(timestamps, 1)

        for idx, item in enumerate(raw):
            if isinstance(item, dict):
                points.append(
                    _point(
                        item.get("ts") or _shift_ts(start_ts, idx),
                        item.get("yhat"), item.get("yhat_lower"), item.get("yhat_upper"),
                    )
                )
                continue

            yhat, yhat_lower, yhat_upper = None, None, None
            if isinstance(item, (list, tuple)):
                if len(item) > 0:
                    yhat = item[0]
                if len(item) > 1:
                    yhat_lower = item[1]
                if len(item) > 2:
                    yhat_upper = item[2]
            else:
                yhat = item

            points.append(_point(_shift_ts(start_ts, idx), yhat, yhat_lower, yhat_upper))

        if not points:
            points.append(_point(_fallback_ts(timestamps, horizon_hours), None, None, None))
        return points


class _ScalarNormalizer:
    def can_handle(self, raw: Any) -> bool:
        import numbers

        if isinstance(raw, bool):
            return False
        if isinstance(raw, numbers.Number):
            return True
        try:
            float(raw)
        except (TypeError, ValueError):
            return False
        return True

    def normalize(self, raw: Any, timestamps: list[Any], horizon_hours: int) -> list[dict[str, Any]]:
        ts = _fallback_ts(timestamps, horizon_hours)
        try:
            return [_point(ts, float(raw), None, None)]
        except (TypeError, ValueError):
            return [_point(ts, None, None, None)]


NORMALIZERS: list[Any] = [
    _DartsSeriesNormalizer(),
    _DataFrameNormalizer(),
    _DictNormalizer(),
    _IterableNormalizer(),
    _ScalarNormalizer(),
]


def _normalize_predictions(raw_predictions: Any, features_frame: pd.DataFrame, horizon_hours: int) -> list[dict[str, Any]]:
    timestamps = list(features_frame["date_utc"])
    if not timestamps:
        timestamps = [datetime.now(timezone.utc)]

    for normalizer in NORMALIZERS:
        if normalizer.can_handle(raw_predictions):
            return normalizer.normalize(raw_predictions, timestamps, horizon_hours)

    ts = _fallback_ts(timestamps, horizon_hours)
    return [_point(ts, None, None, None)]


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
