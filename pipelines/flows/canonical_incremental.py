from __future__ import annotations

import subprocess
from pathlib import Path

from pipelines.compat import flow, get_flow_context, get_run_logger
from pipelines.config.selectors import SELECTOR_CANONICAL_CORE, SELECTOR_CANONICAL_SILVER
from pipelines.config.settings import get_settings
from pipelines.tasks.artifacts import load_run_results, persist_dbt_audit, summarize_run_results
from pipelines.tasks.db import ensure_ops_audit_tables, get_engine
from pipelines.tasks.dbt_tasks import dbt_deps, dbt_run_selector
from pipelines.tasks.gates import raise_if_failed
from pipelines.tasks.notifications import notify_flow_failure

REPO_ROOT = Path(__file__).resolve().parents[2]


def _git_sha() -> str | None:
    try:
        completed = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            cwd=REPO_ROOT,
            capture_output=True,
            text=True,
            check=False,
        )
        if completed.returncode == 0:
            return completed.stdout.strip() or None
    except Exception:  # noqa: BLE001
        return None
    return None


def _summary_from_result(result) -> dict:
    run_results = load_run_results(result.run_results_path) if result.run_results_path else {}
    return summarize_run_results(run_results)


@flow(name="canonical_incremental")
def canonical_incremental() -> None:
    logger = get_run_logger()
    settings = get_settings()
    engine = get_engine(settings)
    ensure_ops_audit_tables(engine)

    ctx = get_flow_context()
    ctx.update(
        {
            "target": settings.DBT_TARGET,
            "git_sha": _git_sha(),
            "project_code": None,
            "slack_webhook_url": settings.SLACK_WEBHOOK_URL,
            "flow_name": "canonical_incremental",
        }
    )

    try:
        deps_result = dbt_deps(settings)
        deps_summary = _summary_from_result(deps_result)
        persist_dbt_audit(engine, deps_result, deps_summary, ctx)
        raise_if_failed(deps_result, "dbt deps failed")

        core_result = dbt_run_selector(settings, selector=SELECTOR_CANONICAL_CORE)
        core_summary = _summary_from_result(core_result)
        persist_dbt_audit(engine, core_result, core_summary, ctx)
        raise_if_failed(core_result, "canonical core stage failed")

        silver_result = dbt_run_selector(settings, selector=SELECTOR_CANONICAL_SILVER)
        silver_summary = _summary_from_result(silver_result)
        persist_dbt_audit(engine, silver_result, silver_summary, ctx)
        raise_if_failed(silver_result, "canonical silver stage failed")

        logger.info("canonical_incremental completed successfully")
    except Exception as exc:  # noqa: BLE001
        notify_flow_failure(ctx, str(exc))
        raise
    finally:
        engine.dispose()


if __name__ == "__main__":
    canonical_incremental()
