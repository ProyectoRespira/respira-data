from __future__ import annotations

import json
import logging
import math
import os
import sys
import warnings
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from time import perf_counter
from typing import Any
from uuid import uuid4

import pandas as pd

TEST_DIR = Path(__file__).resolve().parent
REPO_ROOT = TEST_DIR.parents[1]
SRC_ROOT = REPO_ROOT / "src"
os.environ.setdefault("MPLCONFIGDIR", str(TEST_DIR / ".mplconfig"))
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))


def _configure_runtime_noise() -> None:
    logging.getLogger("darts.models").setLevel(logging.ERROR)
    warnings.filterwarnings(
        "ignore",
        message="X does not have valid feature names, but LGBMRegressor was fitted with feature names",
        category=UserWarning,
    )
    warnings.filterwarnings(
        "ignore",
        category=SyntaxWarning,
        module=r"statsforecast\.models",
    )


_configure_runtime_noise()

from inference.feature_adapter import (  # noqa: E402
    REQUIRED_FEATURE_COLUMNS,
    rows_to_feature_frame,
)
from inference.model_loader import load_pickle_model  # noqa: E402
from inference.predictor import WindowPredictor  # noqa: E402

CSV_PATH = TEST_DIR / "fake_station_inference_features.csv"
MODELS_DIR = REPO_ROOT / "models"
DEFAULT_WINDOW_HOURS = 24
DEFAULT_MIN_POINTS = 18

STATION_SCENARIOS = {
    1: "24h completas hasta as_of",
    2: "datos viejos; ultima lectura hace un mes",
    3: "faltantes intermedios en la ventana",
    4: "24h completas pero la ultima lectura fue hace 8h",
}


@dataclass
class LoadedPredictors:
    predictor_6h: WindowPredictor
    predictor_12h: WindowPredictor
    model_6h_path: Path
    model_12h_path: Path
    model_6h_version: str
    model_12h_version: str


def resolve_default_model_paths(models_dir: Path = MODELS_DIR) -> tuple[Path, Path]:
    model_6h_candidates = sorted(models_dir.glob("*model-6h.pkl"))
    model_12h_candidates = sorted(models_dir.glob("*model-12h.pkl"))

    if not model_6h_candidates or not model_12h_candidates:
        raise FileNotFoundError(
            f"No se encontraron modelos esperados en {models_dir}. "
            "Se esperaba al menos un archivo *model-6h.pkl y uno *model-12h.pkl."
        )

    return model_6h_candidates[-1], model_12h_candidates[-1]


def load_predictors(
    model_6h_path: Path | None = None,
    model_12h_path: Path | None = None,
) -> LoadedPredictors:
    resolved_model_6h_path, resolved_model_12h_path = (
        (
            Path(model_6h_path),
            Path(model_12h_path),
        )
        if model_6h_path and model_12h_path
        else resolve_default_model_paths()
    )

    model_6h = load_pickle_model(str(resolved_model_6h_path))
    model_12h = load_pickle_model(str(resolved_model_12h_path))

    return LoadedPredictors(
        predictor_6h=WindowPredictor(
            model=model_6h.model, model_version=resolved_model_6h_path.stem
        ),
        predictor_12h=WindowPredictor(
            model=model_12h.model, model_version=resolved_model_12h_path.stem
        ),
        model_6h_path=resolved_model_6h_path,
        model_12h_path=resolved_model_12h_path,
        model_6h_version=resolved_model_6h_path.stem,
        model_12h_version=resolved_model_12h_path.stem,
    )


def load_fake_data(csv_path: Path = CSV_PATH) -> pd.DataFrame:
    if not csv_path.exists():
        raise FileNotFoundError(
            f"No existe {csv_path}. Ejecuta primero generate_fake_csv.py para crear el dataset de prueba."
        )

    frame = pd.read_csv(csv_path)
    frame["date_utc"] = pd.to_datetime(frame["date_utc"], utc=True)
    return frame.sort_values(["station_id", "date_utc"]).reset_index(drop=True)


def default_as_of(frame: pd.DataFrame) -> datetime:
    return frame["date_utc"].max().to_pydatetime()


def run_fake_inference(
    csv_path: Path = CSV_PATH,
    model_6h_path: Path | None = None,
    model_12h_path: Path | None = None,
    as_of: datetime | None = None,
    window_hours: int = DEFAULT_WINDOW_HOURS,
    min_points: int = DEFAULT_MIN_POINTS,
) -> dict[str, Any]:
    started_at = datetime.now(UTC)
    dataset = load_fake_data(csv_path)
    as_of_value = _ensure_utc(as_of or default_as_of(dataset))
    predictors = load_predictors(
        model_6h_path=model_6h_path, model_12h_path=model_12h_path
    )

    station_statuses: list[dict[str, Any]] = []
    inference_results: list[dict[str, Any]] = []
    station_summaries: list[dict[str, Any]] = []
    station_inputs: dict[int, list[dict[str, Any]]] = {}
    run_id: str | None = None
    counters = {
        "stations_total": 0,
        "stations_success": 0,
        "stations_skipped": 0,
        "stations_failed": 0,
    }

    window_start = as_of_value - timedelta(hours=window_hours)
    station_ids = sorted(
        int(station_id) for station_id in dataset["station_id"].unique()
    )
    counters["stations_total"] = len(station_ids)

    for station_id in station_ids:
        station_started = perf_counter()
        station_all_rows = dataset[dataset["station_id"] == station_id].copy()
        station_rows = station_all_rows[
            (station_all_rows["date_utc"] > window_start)
            & (station_all_rows["date_utc"] <= as_of_value)
        ].copy()

        summary = {
            "station_id": station_id,
            "scenario": STATION_SCENARIOS.get(station_id, "sin escenario"),
            "rows_total": int(len(station_all_rows)),
            "rows_in_window": int(len(station_rows)),
            "latest_date_utc": _maybe_iso(station_all_rows["date_utc"].max()),
        }

        try:
            if len(station_rows) < min_points:
                counters["stations_skipped"] += 1
                summary["rows_after_complete_filter"] = 0
                station_statuses.append(
                    _station_status(
                        run_id=run_id,
                        station_id=station_id,
                        status="skipped",
                        reason_code="min_points",
                        reason_detail=f"Rows in window: {len(station_rows)}",
                        duration_s=_elapsed_seconds(station_started),
                    )
                )
                station_summaries.append(summary)
                continue

            complete_rows = _filter_complete_records(station_rows)
            summary["rows_after_complete_filter"] = int(len(complete_rows))
            if len(complete_rows) < min_points:
                counters["stations_skipped"] += 1
                station_statuses.append(
                    _station_status(
                        run_id=run_id,
                        station_id=station_id,
                        status="skipped",
                        reason_code="min_points_or_nulls",
                        reason_detail=f"Rows after complete filter: {len(complete_rows)}",
                        duration_s=_elapsed_seconds(station_started),
                    )
                )
                station_summaries.append(summary)
                continue

            feature_frame = rows_to_feature_frame(complete_rows)
            station_inputs[station_id] = _aqi_input_points(feature_frame)

            prediction_6h = predictors.predictor_6h.predict_window(
                feature_frame, horizon_hours=6, as_of=as_of_value
            )
            prediction_12h = predictors.predictor_12h.predict_window(
                feature_frame, horizon_hours=12, as_of=as_of_value
            )

            if run_id is None:
                run_id = str(uuid4())

            inference_results.append(
                _inference_result(
                    run_id=run_id,
                    station_id=station_id,
                    as_of=as_of_value,
                    horizon_hours=6,
                    model_version=predictors.model_6h_version,
                    predictions_json=prediction_6h,
                )
            )
            inference_results.append(
                _inference_result(
                    run_id=run_id,
                    station_id=station_id,
                    as_of=as_of_value,
                    horizon_hours=12,
                    model_version=predictors.model_12h_version,
                    predictions_json=prediction_12h,
                )
            )

            counters["stations_success"] += 1
            station_statuses.append(
                _station_status(
                    run_id=run_id,
                    station_id=station_id,
                    status="success",
                    reason_code=None,
                    reason_detail=None,
                    duration_s=_elapsed_seconds(station_started),
                )
            )
            station_summaries.append(summary)
        except Exception as exc:  # noqa: BLE001
            counters["stations_failed"] += 1
            station_statuses.append(
                _station_status(
                    run_id=run_id,
                    station_id=station_id,
                    status="failed",
                    reason_code="exception",
                    reason_detail=str(exc),
                    duration_s=_elapsed_seconds(station_started),
                )
            )
            station_summaries.append(summary)

    ended_at = datetime.now(UTC)
    inference_run = None
    if run_id is not None:
        inference_run = {
            "id": run_id,
            "deployment": "streamlit-local-test",
            "flow_run_id": "local-csv-test",
            "as_of": as_of_value.isoformat(),
            "window_hours": window_hours,
            "min_points": min_points,
            "model_6h_version": predictors.model_6h_version,
            "model_12h_version": predictors.model_12h_version,
            "model_6h_path": str(predictors.model_6h_path),
            "model_12h_path": str(predictors.model_12h_path),
            "started_at": started_at.isoformat(),
            "ended_at": ended_at.isoformat(),
            "duration_s": int((ended_at - started_at).total_seconds()),
            "status": "success"
            if counters["stations_success"] > 0 and counters["stations_failed"] == 0
            else "partial",
            "stations_total": counters["stations_total"],
            "stations_success": counters["stations_success"],
            "stations_skipped": counters["stations_skipped"],
            "stations_failed": counters["stations_failed"],
        }

    return {
        "meta": {
            "csv_path": str(csv_path),
            "models_dir": str(MODELS_DIR),
            "as_of": as_of_value.isoformat(),
            "window_start": window_start.isoformat(),
            "window_hours": window_hours,
            "min_points": min_points,
            "model_6h_path": str(predictors.model_6h_path),
            "model_12h_path": str(predictors.model_12h_path),
        },
        "inference_run": inference_run,
        "inference_results": inference_results,
        "station_status": station_statuses,
        "station_summaries": station_summaries,
        "station_inputs": station_inputs,
    }


def dump_results(output_dir: Path, payload: dict[str, Any]) -> dict[str, str]:
    output_dir.mkdir(parents=True, exist_ok=True)

    paths = {
        "full_payload": output_dir / "full_payload.json",
        "inference_run": output_dir / "inference_run.json",
        "inference_results": output_dir / "inference_results.json",
        "station_status": output_dir / "station_status.json",
    }

    paths["full_payload"].write_text(
        json.dumps(payload, indent=2, ensure_ascii=True), encoding="utf-8"
    )
    paths["inference_run"].write_text(
        json.dumps(payload.get("inference_run"), indent=2, ensure_ascii=True),
        encoding="utf-8",
    )
    paths["inference_results"].write_text(
        json.dumps(payload.get("inference_results"), indent=2, ensure_ascii=True),
        encoding="utf-8",
    )
    paths["station_status"].write_text(
        json.dumps(payload.get("station_status"), indent=2, ensure_ascii=True),
        encoding="utf-8",
    )
    return {key: str(value) for key, value in paths.items()}


def _filter_complete_records(frame: pd.DataFrame) -> list[dict[str, Any]]:
    complete_rows: list[dict[str, Any]] = []
    for row in frame.to_dict(orient="records"):
        if any(_is_missing(row.get(column)) for column in REQUIRED_FEATURE_COLUMNS):
            continue
        complete_rows.append(row)
    return complete_rows


def _is_missing(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, float) and math.isnan(value):
        return True
    return False


def _aqi_input_points(feature_frame: pd.DataFrame) -> list[dict[str, Any]]:
    return [
        {
            "ts": pd.to_datetime(row["date_utc"], utc=True).isoformat(),
            "aqi_pm2_5": float(row["aqi_pm2_5"]),
        }
        for _, row in feature_frame[["date_utc", "aqi_pm2_5"]].iterrows()
    ]


def _station_status(
    run_id: str | None,
    station_id: int,
    status: str,
    reason_code: str | None,
    reason_detail: str | None,
    duration_s: int,
) -> dict[str, Any]:
    return {
        "id": str(uuid4()),
        "inference_run_id": run_id,
        "station_id": station_id,
        "scenario": STATION_SCENARIOS.get(station_id),
        "status": status,
        "reason_code": reason_code,
        "reason_detail": reason_detail,
        "duration_s": duration_s,
    }


def _inference_result(
    run_id: str,
    station_id: int,
    as_of: datetime,
    horizon_hours: int,
    model_version: str,
    predictions_json: dict[str, Any],
) -> dict[str, Any]:
    return {
        "id": str(uuid4()),
        "inference_run_id": run_id,
        "station_id": station_id,
        "scenario": STATION_SCENARIOS.get(station_id),
        "as_of": as_of.isoformat(),
        "horizon_hours": horizon_hours,
        "model_version": model_version,
        "predictions_json": predictions_json,
    }


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _elapsed_seconds(started_at: float) -> int:
    return max(0, int(perf_counter() - started_at))


def _maybe_iso(value: Any) -> str | None:
    if value is None or pd.isna(value):
        return None
    return pd.to_datetime(value, utc=True).isoformat()
