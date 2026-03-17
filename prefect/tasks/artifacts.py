from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from tasks.dbt_tasks import DbtTaskResult

logger = logging.getLogger(__name__)


def load_run_results(path: str) -> dict[str, Any]:
    run_results_path = Path(path)
    if not run_results_path.exists():
        return {}
    with run_results_path.open("r", encoding="utf-8") as fh:
        return json.load(fh)


def summarize_run_results(run_results: dict[str, Any]) -> dict[str, Any]:
    if not run_results:
        return {
            "models_passed": 0,
            "models_failed": 0,
            "tests_passed": 0,
            "tests_failed": 0,
            "error_summary": None,
        }

    models_passed = 0
    models_failed = 0
    tests_passed = 0
    tests_failed = 0
    errors: list[str] = []

    for result in run_results.get("results", []):
        unique_id = result.get("unique_id", "")
        status = str(result.get("status", "")).lower()
        message = result.get("message") or ""

        is_model = unique_id.startswith("model.")
        is_test = unique_id.startswith("test.")

        if is_model:
            if status in {"success", "pass", "warn", "skipped"}:
                models_passed += 1
            elif status in {"error", "fail", "failed"}:
                models_failed += 1
        elif is_test:
            if status in {"success", "pass", "warn", "skipped"}:
                tests_passed += 1
            elif status in {"error", "fail", "failed"}:
                tests_failed += 1

        if status in {"error", "fail", "failed"}:
            short_msg = message.strip().replace("\n", " ")[:220]
            errors.append(f"{unique_id}: {short_msg}" if short_msg else unique_id)

    error_summary = "; ".join(errors[:5]) if errors else None

    return {
        "models_passed": models_passed,
        "models_failed": models_failed,
        "tests_passed": tests_passed,
        "tests_failed": tests_failed,
        "error_summary": error_summary,
    }


def persist_dbt_audit(engine, dbt_result: DbtTaskResult, summary: dict[str, Any], ctx: dict[str, Any]) -> UUID:
    audit_id = uuid4()
    run_results_json = (
        load_run_results(dbt_result.run_results_path) if dbt_result.run_results_path else None
    )

    query = text(
        """
        insert into ops.dbt_run_audit (
            id,
            flow_run_id,
            deployment,
            target,
            git_sha,
            project_code,
            command,
            selector,
            started_at,
            ended_at,
            duration_s,
            status,
            models_passed,
            models_failed,
            tests_passed,
            tests_failed,
            error_summary,
            run_results_json,
            created_at
        ) values (
            :id,
            :flow_run_id,
            :deployment,
            :target,
            :git_sha,
            :project_code,
            :command,
            :selector,
            :started_at,
            :ended_at,
            :duration_s,
            :status,
            :models_passed,
            :models_failed,
            :tests_passed,
            :tests_failed,
            :error_summary,
            cast(:run_results_json as jsonb),
            :created_at
        )
        """
    )

    created_at = datetime.now(timezone.utc)
    payload = {
        "id": str(audit_id),
        "flow_run_id": ctx.get("flow_run_id", "unknown"),
        "deployment": ctx.get("deployment"),
        "target": ctx.get("target", "prod"),
        "git_sha": ctx.get("git_sha"),
        "project_code": ctx.get("project_code"),
        "command": dbt_result.command,
        "selector": dbt_result.selector,
        "started_at": dbt_result.started_at,
        "ended_at": dbt_result.ended_at,
        "duration_s": dbt_result.duration_s,
        "status": dbt_result.status,
        "models_passed": int(summary.get("models_passed", 0)),
        "models_failed": int(summary.get("models_failed", 0)),
        "tests_passed": int(summary.get("tests_passed", 0)),
        "tests_failed": int(summary.get("tests_failed", 0)),
        "error_summary": summary.get("error_summary"),
        "run_results_json": json.dumps(run_results_json) if run_results_json is not None else None,
        "created_at": created_at,
    }

    try:
        with engine.begin() as conn:
            conn.execute(query, payload)
    except SQLAlchemyError as exc:
        logger.warning("Skipping dbt audit persistence: %s", exc)

    return audit_id
