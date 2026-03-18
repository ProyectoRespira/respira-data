from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from pipelines.compat import flow, get_flow_context, get_run_logger
from pipelines.config.projects import ProjectConfig, get_project_config
from pipelines.config.settings import RuntimeSettings, get_settings
from inference.feature_adapter import REQUIRED_FEATURE_COLUMNS, rows_to_feature_frame
from inference.model_loader import load_pickle_model
from inference.predictor import WindowPredictor
from pipelines.tasks.db import ensure_ops_audit_tables, ensure_project_inference_tables, get_engine
from pipelines.tasks.inference_tasks import (
    create_inference_run,
    filter_complete_rows,
    finalize_inference_run,
    list_candidate_stations,
    load_station_window,
    persist_inference_result,
    persist_station_status,
    validate_min_points,
)


@dataclass(frozen=True)
class InferenceRunParams:
    as_of: datetime
    window_hours: int
    min_points: int
    model_6h_path: str
    model_12h_path: str
    model_6h_version: str
    model_12h_version: str


def _resolve_params(
    settings: RuntimeSettings,
    as_of: datetime | None = None,
    window_hours: int | None = None,
    min_points: int | None = None,
    model_6h_path: str | None = None,
    model_12h_path: str | None = None,
    model_6h_version: str | None = None,
    model_12h_version: str | None = None,
) -> InferenceRunParams:
    resolved_6h = model_6h_path or settings.MODEL_6H_PATH
    resolved_12h = model_12h_path or settings.MODEL_12H_PATH
    if not resolved_6h or not resolved_12h:
        raise ValueError("Both model_6h_path and model_12h_path are required")

    return InferenceRunParams(
        as_of=as_of.astimezone(timezone.utc) if as_of else datetime.now(timezone.utc),
        window_hours=int(window_hours or settings.DEFAULT_WINDOW_HOURS),
        min_points=int(min_points or settings.INFERENCE_MIN_POINTS),
        model_6h_path=resolved_6h,
        model_12h_path=resolved_12h,
        model_6h_version=model_6h_version or settings.MODEL_6H_VERSION,
        model_12h_version=model_12h_version or settings.MODEL_12H_VERSION,
    )


def _load_inference_models(params: InferenceRunParams) -> tuple[WindowPredictor, WindowPredictor]:
    model_6h = load_pickle_model(params.model_6h_path)
    model_12h = load_pickle_model(params.model_12h_path)
    return (
        WindowPredictor(model=model_6h.model, model_version=params.model_6h_version),
        WindowPredictor(model=model_12h.model, model_version=params.model_12h_version),
    )


def _process_single_station(
    engine: Any,
    project: ProjectConfig,
    station_id: int,
    params: InferenceRunParams,
    predictor_6h: WindowPredictor,
    predictor_12h: WindowPredictor,
    inference_run_id: Any,
) -> str:
    logger = get_run_logger()
    station_started = datetime.now(timezone.utc)

    try:
        rows = load_station_window(engine, project, station_id, params.as_of, params.window_hours)
        if not validate_min_points(rows, params.min_points):
            _persist_status(
                engine, project, inference_run_id, station_id, station_started,
                status="skipped", reason_code="min_points",
                reason_detail=f"Rows in window: {len(rows)}",
            )
            return "skipped"

        complete_rows = filter_complete_rows(rows, REQUIRED_FEATURE_COLUMNS)
        if not validate_min_points(complete_rows, params.min_points):
            _persist_status(
                engine, project, inference_run_id, station_id, station_started,
                status="skipped", reason_code="min_points_or_nulls",
                reason_detail=f"Rows after null filter: {len(complete_rows)}",
            )
            return "skipped"

        feature_frame = rows_to_feature_frame(complete_rows)
        prediction_6h = predictor_6h.predict_window(feature_frame, horizon_hours=6, as_of=params.as_of)
        prediction_12h = predictor_12h.predict_window(feature_frame, horizon_hours=12, as_of=params.as_of)

        persist_inference_result(
            engine, project, inference_run_id, station_id, params.as_of,
            horizon_hours=6, model_version=params.model_6h_version,
            predictions_json=prediction_6h,
        )
        persist_inference_result(
            engine, project, inference_run_id, station_id, params.as_of,
            horizon_hours=12, model_version=params.model_12h_version,
            predictions_json=prediction_12h,
        )

        _persist_status(
            engine, project, inference_run_id, station_id, station_started,
            status="success", reason_code=None, reason_detail=None,
        )
        return "success"

    except Exception as exc:  # noqa: BLE001
        logger.exception(
            "Station inference failed for project_code=%s station_id=%s",
            project.project_code, station_id,
        )
        _persist_status(
            engine, project, inference_run_id, station_id, station_started,
            status="failed", reason_code="exception",
            reason_detail=str(exc)[:500],
        )
        return "failed"


def _persist_status(
    engine: Any,
    project: ProjectConfig,
    inference_run_id: Any,
    station_id: int,
    station_started: datetime,
    status: str,
    reason_code: str | None,
    reason_detail: str | None,
) -> None:
    duration_s = int((datetime.now(timezone.utc) - station_started).total_seconds())
    persist_station_status(
        engine, project.project_code, inference_run_id, station_id,
        status=status, reason_code=reason_code,
        reason_detail=reason_detail, duration_s=duration_s,
    )


@flow(name="project_inference")
def project_inference(
    project_code: str,
    as_of: datetime | None = None,
    window_hours: int | None = None,
    min_points: int | None = None,
    model_6h_path: str | None = None,
    model_12h_path: str | None = None,
    model_6h_version: str | None = None,
    model_12h_version: str | None = None,
    engine: Any = None,
) -> None:
    logger = get_run_logger()
    settings = get_settings()
    project = get_project_config(project_code)

    if not project.inference_enabled:
        logger.info("Inference disabled for project_code=%s. Skipping.", project_code)
        return

    params = _resolve_params(
        settings,
        as_of=as_of, window_hours=window_hours, min_points=min_points,
        model_6h_path=model_6h_path, model_12h_path=model_12h_path,
        model_6h_version=model_6h_version, model_12h_version=model_12h_version,
    )
    predictor_6h, predictor_12h = _load_inference_models(params)

    owns_engine = engine is None
    if owns_engine:
        engine = get_engine(settings)
    ensure_ops_audit_tables(engine)
    ensure_project_inference_tables(engine, project)

    flow_ctx = get_flow_context()
    run_ctx = {
        **flow_ctx,
        "project_code": project.project_code,
        "as_of": params.as_of,
        "window_hours": params.window_hours,
        "min_points": params.min_points,
        "model_6h_version": params.model_6h_version,
        "model_12h_version": params.model_12h_version,
        "model_6h_path": params.model_6h_path,
        "model_12h_path": params.model_12h_path,
        "started_at": datetime.now(timezone.utc),
        "status": "running",
    }

    inference_run_id = create_inference_run(engine, project, run_ctx)
    counters = {
        "stations_total": 0,
        "stations_success": 0,
        "stations_skipped": 0,
        "stations_failed": 0,
    }

    try:
        stations = list_candidate_stations(engine, project, params.as_of, params.window_hours)
        counters["stations_total"] = len(stations)
        logger.info(
            "Found %s candidate stations for inference in project_code=%s",
            len(stations), project.project_code,
        )

        for station_id in stations:
            result = _process_single_station(
                engine, project, station_id, params,
                predictor_6h, predictor_12h, inference_run_id,
            )
            counters[f"stations_{result}"] += 1

        finalize_inference_run(
            engine, project, inference_run_id,
            counters=counters, status="success", error_summary=None,
        )

    except Exception as exc:  # noqa: BLE001
        logger.exception("Inference flow failed for project_code=%s", project.project_code)
        finalize_inference_run(
            engine, project, inference_run_id,
            counters=counters, status="failed", error_summary=str(exc)[:1000],
        )
        raise
    finally:
        if owns_engine:
            engine.dispose()


if __name__ == "__main__":
    project_inference(project_code="respira_gold")
