from __future__ import annotations

import math
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy import text

PREFECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PREFECT_ROOT.parent / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from inference.feature_adapter import REQUIRED_FEATURE_COLUMNS


SOURCE_TABLE = '"respira-gold".station_inference_features'
REQUIRED_COLUMNS = ["station_id", "date_utc", *REQUIRED_FEATURE_COLUMNS]
INSERT_INFERENCE_RUN_QUERY = text(
    """
    insert into "respira-gold".inference_runs (
        id,
        flow_run_id,
        deployment,
        as_of,
        window_hours,
        min_points,
        model_6h_version,
        model_12h_version,
        model_6h_path,
        model_12h_path,
        started_at,
        ended_at,
        duration_s,
        status,
        stations_total,
        stations_success,
        stations_skipped,
        stations_failed,
        error_summary,
        created_at
    ) values (
        :id,
        :flow_run_id,
        :deployment,
        :as_of,
        :window_hours,
        :min_points,
        :model_6h_version,
        :model_12h_version,
        :model_6h_path,
        :model_12h_path,
        :started_at,
        null,
        null,
        :status,
        0,
        0,
        0,
        0,
        null,
        now()
    )
    """
)
UPSERT_STATION_STATUS_QUERY = text(
    """
    insert into ops.inference_station_status (
        id,
        inference_run_id,
        station_id,
        status,
        reason_code,
        reason_detail,
        duration_s,
        created_at
    ) values (
        :id,
        :inference_run_id,
        :station_id,
        :status,
        :reason_code,
        :reason_detail,
        :duration_s,
        now()
    )
    on conflict (inference_run_id, station_id)
    do update set
        status = excluded.status,
        reason_code = excluded.reason_code,
        reason_detail = excluded.reason_detail,
        duration_s = excluded.duration_s
    """
)
UPSERT_INFERENCE_RESULT_QUERY = text(
    """
    insert into "respira-gold".inference_results (
        id,
        inference_run_id,
        station_id,
        as_of,
        horizon_hours,
        model_version,
        predictions_json,
        created_at
    ) values (
        :id,
        :inference_run_id,
        :station_id,
        :as_of,
        :horizon_hours,
        :model_version,
        cast(:predictions_json as jsonb),
        now()
    )
    on conflict (inference_run_id, station_id, horizon_hours)
    do update set
        model_version = excluded.model_version,
        predictions_json = excluded.predictions_json
    """
)


def _ensure_utc(as_of: datetime) -> datetime:
    if as_of.tzinfo is None:
        return as_of.replace(tzinfo=timezone.utc)
    return as_of.astimezone(timezone.utc)


def list_candidate_stations(engine, as_of: datetime, window_hours: int) -> list[int]:
    as_of_utc = _ensure_utc(as_of)
    window_start = as_of_utc - timedelta(hours=window_hours)

    query = text(
        f"""
        select distinct station_id
        from {SOURCE_TABLE}
        where date_utc > :window_start
          and date_utc <= :as_of
        order by station_id
        """
    )

    with engine.connect() as conn:
        rows = conn.execute(query, {"window_start": window_start, "as_of": as_of_utc}).all()

    return [int(row[0]) for row in rows]


def load_station_window(engine, station_id: int, as_of: datetime, window_hours: int) -> list[dict[str, Any]]:
    as_of_utc = _ensure_utc(as_of)
    window_start = as_of_utc - timedelta(hours=window_hours)
    selected_columns = ", ".join(REQUIRED_COLUMNS)

    query = text(
        f"""
        select {selected_columns}
        from {SOURCE_TABLE}
        where station_id = :station_id
          and date_utc > :window_start
          and date_utc <= :as_of
        order by date_utc asc
        """
    )

    with engine.connect() as conn:
        rows = conn.execute(
            query,
            {
                "station_id": station_id,
                "window_start": window_start,
                "as_of": as_of_utc,
            },
        ).mappings().all()

    return [dict(row) for row in rows]


def _is_missing(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, float) and math.isnan(value):
        return True
    return False


def filter_complete_rows(rows: list[dict[str, Any]], required_feature_cols: list[str]) -> list[dict[str, Any]]:
    filtered_rows: list[dict[str, Any]] = []
    for row in rows:
        if any(_is_missing(row.get(column)) for column in required_feature_cols):
            continue
        filtered_rows.append(row)
    return filtered_rows


def validate_min_points(rows: list[dict[str, Any]], min_points: int) -> bool:
    return len(rows) >= min_points


def create_inference_run(engine, ctx: dict[str, Any]) -> UUID:
    inference_run_id = uuid4()
    payload = _build_inference_run_payload(inference_run_id, ctx)

    with engine.begin() as conn:
        conn.execute(INSERT_INFERENCE_RUN_QUERY, payload)

    return inference_run_id


def initialize_inference_run(
    engine,
    ctx: dict[str, Any],
    station_statuses: list[dict[str, Any]],
    inference_results: list[dict[str, Any]],
) -> UUID:
    inference_run_id = uuid4()
    run_payload = _build_inference_run_payload(inference_run_id, ctx)

    with engine.begin() as conn:
        conn.execute(INSERT_INFERENCE_RUN_QUERY, run_payload)
        for status_payload in station_statuses:
            conn.execute(UPSERT_STATION_STATUS_QUERY, _build_station_status_payload(inference_run_id, status_payload))
        for result_payload in inference_results:
            conn.execute(UPSERT_INFERENCE_RESULT_QUERY, _build_inference_result_payload(inference_run_id, result_payload))

    return inference_run_id


def _build_inference_run_payload(inference_run_id: UUID, ctx: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": str(inference_run_id),
        "flow_run_id": ctx.get("flow_run_id", "local"),
        "deployment": ctx.get("deployment"),
        "as_of": _ensure_utc(ctx["as_of"]),
        "window_hours": int(ctx["window_hours"]),
        "min_points": int(ctx["min_points"]),
        "model_6h_version": ctx["model_6h_version"],
        "model_12h_version": ctx["model_12h_version"],
        "model_6h_path": ctx.get("model_6h_path"),
        "model_12h_path": ctx.get("model_12h_path"),
        "started_at": _ensure_utc(ctx.get("started_at", datetime.now(timezone.utc))),
        "status": ctx.get("status", "success"),
    }


def persist_station_status(
    engine,
    inference_run_id: UUID,
    station_id: int,
    status: str,
    reason_code: str | None,
    reason_detail: str | None,
    duration_s: int | None,
) -> None:
    with engine.begin() as conn:
        conn.execute(
            UPSERT_STATION_STATUS_QUERY,
            _build_station_status_payload(
                inference_run_id,
                {
                    "station_id": station_id,
                    "status": status,
                    "reason_code": reason_code,
                    "reason_detail": reason_detail,
                    "duration_s": duration_s,
                },
            ),
        )


def persist_inference_result(
    engine,
    inference_run_id: UUID,
    station_id: int,
    as_of: datetime,
    horizon_hours: int,
    model_version: str,
    predictions_json: dict[str, Any],
) -> None:
    with engine.begin() as conn:
        conn.execute(
            UPSERT_INFERENCE_RESULT_QUERY,
            _build_inference_result_payload(
                inference_run_id,
                {
                    "station_id": station_id,
                    "as_of": as_of,
                    "horizon_hours": horizon_hours,
                    "model_version": model_version,
                    "predictions_json": predictions_json,
                },
            ),
        )


def _build_station_status_payload(inference_run_id: UUID, payload: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": str(uuid4()),
        "inference_run_id": str(inference_run_id),
        "station_id": int(payload["station_id"]),
        "status": payload["status"],
        "reason_code": payload.get("reason_code"),
        "reason_detail": payload.get("reason_detail"),
        "duration_s": payload.get("duration_s"),
    }


def _build_inference_result_payload(inference_run_id: UUID, payload: dict[str, Any]) -> dict[str, Any]:
    import json

    return {
        "id": str(uuid4()),
        "inference_run_id": str(inference_run_id),
        "station_id": int(payload["station_id"]),
        "as_of": _ensure_utc(payload["as_of"]),
        "horizon_hours": int(payload["horizon_hours"]),
        "model_version": payload["model_version"],
        "predictions_json": json.dumps(payload["predictions_json"]),
    }


def finalize_inference_run(
    engine,
    inference_run_id: UUID,
    counters: dict[str, int],
    status: str,
    error_summary: str | None,
) -> None:
    ended_at = datetime.now(timezone.utc)

    with engine.begin() as conn:
        started_at = conn.execute(
            text('select started_at from "respira-gold".inference_runs where id = :id'),
            {"id": str(inference_run_id)},
        ).scalar_one()

        duration_s = int((ended_at - _ensure_utc(started_at)).total_seconds())

        conn.execute(
            text(
                """
                update "respira-gold".inference_runs
                set ended_at = :ended_at,
                    duration_s = :duration_s,
                    status = :status,
                    stations_total = :stations_total,
                    stations_success = :stations_success,
                    stations_skipped = :stations_skipped,
                    stations_failed = :stations_failed,
                    error_summary = :error_summary
                where id = :id
                """
            ),
            {
                "id": str(inference_run_id),
                "ended_at": ended_at,
                "duration_s": duration_s,
                "status": status,
                "stations_total": int(counters.get("stations_total", 0)),
                "stations_success": int(counters.get("stations_success", 0)),
                "stations_skipped": int(counters.get("stations_skipped", 0)),
                "stations_failed": int(counters.get("stations_failed", 0)),
                "error_summary": error_summary,
            },
        )
