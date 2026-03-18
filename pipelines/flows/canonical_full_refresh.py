from __future__ import annotations

import subprocess
from pathlib import Path

from pipelines.compat import flow, get_flow_context, get_run_logger
from pipelines.config.selectors import SELECTOR_CANONICAL_FULL_REFRESH
from pipelines.config.settings import get_settings
from pipelines.tasks.artifacts import load_run_results, persist_dbt_audit, summarize_run_results
from pipelines.tasks.db import ensure_ops_audit_tables, get_engine
from pipelines.tasks.dbt_tasks import dbt_deps, dbt_run_selector, dbt_test_selector
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


@flow(name="canonical_full_refresh")
def canonical_full_refresh() -> None:
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
            "flow_name": "canonical_full_refresh",
        }
    )

    try:
        deps_result = dbt_deps(settings)
        deps_summary = _summary_from_result(deps_result)
        persist_dbt_audit(engine, deps_result, deps_summary, ctx)
        raise_if_failed(deps_result, "dbt deps failed")

        refresh_result = dbt_run_selector(
            settings,
            selector=SELECTOR_CANONICAL_FULL_REFRESH,
            full_refresh=True,
        )
        refresh_summary = _summary_from_result(refresh_result)
        persist_dbt_audit(engine, refresh_result, refresh_summary, ctx)
        raise_if_failed(refresh_result, "canonical full refresh stage failed")

        test_result = dbt_test_selector(settings, selector=SELECTOR_CANONICAL_FULL_REFRESH)
        test_summary = _summary_from_result(test_result)
        persist_dbt_audit(engine, test_result, test_summary, ctx)
        raise_if_failed(test_result, "canonical full refresh tests failed")

        logger.info("canonical_full_refresh completed")
    except Exception as exc:  # noqa: BLE001
        notify_flow_failure(ctx, str(exc))
        raise
    finally:
        engine.dispose()


if __name__ == "__main__":
    canonical_full_refresh()
