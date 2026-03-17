from __future__ import annotations

import subprocess
import sys
from pathlib import Path

PREFECT_ROOT = Path(__file__).resolve().parents[1]
if str(PREFECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PREFECT_ROOT))

from compat import flow, get_flow_context, get_run_logger
from config.selectors import SELECTOR_CORE, SELECTOR_FACTS
from config.settings import get_settings
from tasks.artifacts import load_run_results, persist_dbt_audit, summarize_run_results
from tasks.db import ensure_ops_audit_tables, get_engine
from tasks.dbt_tasks import dbt_deps, dbt_run_selector
from tasks.gates import raise_if_failed
from tasks.notifications import notify_flow_failure


def _git_sha() -> str | None:
    try:
        completed = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            cwd=PREFECT_ROOT.parent,
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


@flow(name="dbt_incremental")
def dbt_incremental() -> None:
    logger = get_run_logger()
    settings = get_settings()
    engine = get_engine(settings)
    ensure_ops_audit_tables(engine)

    ctx = get_flow_context()
    ctx.update(
        {
            "target": settings.DBT_TARGET,
            "git_sha": _git_sha(),
            "slack_webhook_url": settings.SLACK_WEBHOOK_URL,
            "flow_name": "dbt_incremental",
        }
    )

    try:
        deps_result = dbt_deps(settings)
        deps_summary = _summary_from_result(deps_result)
        persist_dbt_audit(engine, deps_result, deps_summary, ctx)
        raise_if_failed(deps_result, "dbt deps failed")

        core_result = dbt_run_selector(settings, selector=SELECTOR_CORE)
        core_summary = _summary_from_result(core_result)
        persist_dbt_audit(engine, core_result, core_summary, ctx)
        raise_if_failed(core_result, "dbt core stage failed")

        facts_result = dbt_run_selector(settings, selector=SELECTOR_FACTS)
        facts_summary = _summary_from_result(facts_result)
        persist_dbt_audit(engine, facts_result, facts_summary, ctx)
        raise_if_failed(facts_result, "dbt facts stage failed")

        logger.info("dbt_incremental completed successfully")
    except Exception as exc:  # noqa: BLE001
        notify_flow_failure(ctx, str(exc))
        raise
    finally:
        engine.dispose()


if __name__ == "__main__":
    dbt_incremental()
