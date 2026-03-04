from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path

PREFECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PREFECT_ROOT.parent / "src"
if str(PREFECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PREFECT_ROOT))
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from compat import flow, get_flow_context, get_run_logger
from config.settings import get_settings
from inference.feature_adapter import REQUIRED_FEATURE_COLUMNS, rows_to_feature_frame
from inference.model_loader import load_pickle_model
from inference.predictor import WindowPredictor
from tasks.db import get_engine
from tasks.inference_tasks import (
    create_inference_run,
    filter_complete_rows,
    finalize_inference_run,
    list_candidate_stations,
    load_station_window,
    persist_inference_result,
    persist_station_status,
    validate_min_points,
)


@flow(name="inference_per_station")
def inference_per_station(
    as_of: datetime | None = None,
    window_hours: int | None = None,
    min_points: int | None = None,
    model_6h_path: str | None = None,
    model_12h_path: str | None = None,
    model_6h_version: str | None = None,
    model_12h_version: str | None = None,
) -> None:
    logger = get_run_logger()
    settings = get_settings()

    as_of_value = as_of.astimezone(timezone.utc) if as_of else datetime.now(timezone.utc)
    window_hours_value = int(window_hours or settings.DEFAULT_WINDOW_HOURS)
    min_points_value = int(min_points or settings.INFERENCE_MIN_POINTS)

    model_6h_path_value = model_6h_path or settings.MODEL_6H_PATH
    model_12h_path_value = model_12h_path or settings.MODEL_12H_PATH
    model_6h_version_value = model_6h_version or settings.MODEL_6H_VERSION
    model_12h_version_value = model_12h_version or settings.MODEL_12H_VERSION

    if not model_6h_path_value or not model_12h_path_value:
        raise ValueError("Both model_6h_path and model_12h_path are required")

    model_6h = load_pickle_model(model_6h_path_value)
    model_12h = load_pickle_model(model_12h_path_value)
    predictor_6h = WindowPredictor(model=model_6h.model, model_version=model_6h_version_value)
    predictor_12h = WindowPredictor(model=model_12h.model, model_version=model_12h_version_value)

    engine = get_engine(settings)
    flow_ctx = get_flow_context()

    run_ctx = {
        **flow_ctx,
        "as_of": as_of_value,
        "window_hours": window_hours_value,
        "min_points": min_points_value,
        "model_6h_version": model_6h_version_value,
        "model_12h_version": model_12h_version_value,
        "model_6h_path": model_6h_path_value,
        "model_12h_path": model_12h_path_value,
        "started_at": datetime.now(timezone.utc),
        "status": "success",
    }

    inference_run_id = None
    counters = {
        "stations_total": 0,
        "stations_success": 0,
        "stations_skipped": 0,
        "stations_failed": 0,
    }

    try:
        inference_run_id = create_inference_run(engine, run_ctx)
        stations = list_candidate_stations(engine, as_of_value, window_hours_value)
        counters["stations_total"] = len(stations)
        logger.info("Found %s candidate stations for inference", len(stations))

        for station_id in stations:
            station_started = datetime.now(timezone.utc)
            try:
                rows = load_station_window(engine, station_id, as_of_value, window_hours_value)
                if not validate_min_points(rows, min_points_value):
                    counters["stations_skipped"] += 1
                    persist_station_status(
                        engine,
                        inference_run_id,
                        station_id,
                        status="skipped",
                        reason_code="min_points",
                        reason_detail=f"Rows in window: {len(rows)}",
                        duration_s=int((datetime.now(timezone.utc) - station_started).total_seconds()),
                    )
                    continue

                complete_rows = filter_complete_rows(rows, REQUIRED_FEATURE_COLUMNS)
                if not validate_min_points(complete_rows, min_points_value):
                    counters["stations_skipped"] += 1
                    persist_station_status(
                        engine,
                        inference_run_id,
                        station_id,
                        status="skipped",
                        reason_code="min_points_or_nulls",
                        reason_detail=f"Rows after null filter: {len(complete_rows)}",
                        duration_s=int((datetime.now(timezone.utc) - station_started).total_seconds()),
                    )
                    continue

                feature_frame = rows_to_feature_frame(complete_rows)

                prediction_6h = predictor_6h.predict_window(feature_frame, horizon_hours=6)
                persist_inference_result(
                    engine,
                    inference_run_id,
                    station_id,
                    as_of_value,
                    horizon_hours=6,
                    model_version=model_6h_version_value,
                    predictions_json=prediction_6h,
                )

                prediction_12h = predictor_12h.predict_window(feature_frame, horizon_hours=12)
                persist_inference_result(
                    engine,
                    inference_run_id,
                    station_id,
                    as_of_value,
                    horizon_hours=12,
                    model_version=model_12h_version_value,
                    predictions_json=prediction_12h,
                )

                counters["stations_success"] += 1
                persist_station_status(
                    engine,
                    inference_run_id,
                    station_id,
                    status="success",
                    reason_code=None,
                    reason_detail=None,
                    duration_s=int((datetime.now(timezone.utc) - station_started).total_seconds()),
                )
            except Exception as station_exc:  # noqa: BLE001
                counters["stations_failed"] += 1
                logger.exception("Station inference failed for station_id=%s", station_id)
                persist_station_status(
                    engine,
                    inference_run_id,
                    station_id,
                    status="failed",
                    reason_code="exception",
                    reason_detail=str(station_exc)[:500],
                    duration_s=int((datetime.now(timezone.utc) - station_started).total_seconds()),
                )

        finalize_inference_run(
            engine,
            inference_run_id,
            counters=counters,
            status="success",
            error_summary=None,
        )

    except Exception as exc:  # noqa: BLE001
        logger.exception("Inference flow failed")
        if inference_run_id is not None:
            finalize_inference_run(
                engine,
                inference_run_id,
                counters=counters,
                status="failed",
                error_summary=str(exc)[:1000],
            )
        raise
    finally:
        engine.dispose()


if __name__ == "__main__":
    inference_per_station()
