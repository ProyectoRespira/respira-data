from __future__ import annotations

import subprocess
import sys
from pathlib import Path

PREFECT_ROOT = Path(__file__).resolve().parents[1]
if str(PREFECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PREFECT_ROOT))

from compat import flow, get_flow_context, get_run_logger
from config.selectors import SELECTOR_GOLD, SELECTOR_GOLD_TESTS
from config.settings import get_settings
from tasks.artifacts import load_run_results, persist_dbt_audit, summarize_run_results
from tasks.db import get_engine
from tasks.dbt_tasks import dbt_deps, dbt_run_selector, dbt_test_selector
from tasks.gates import format_test_alert, raise_if_failed, should_alert_on_tests
from tasks.notifications import notify_flow_failure, notify_gold_tests_failed


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


@flow(name="dbt_gold")
def dbt_gold() -> None:
    logger = get_run_logger()
    settings = get_settings()
    engine = get_engine(settings)

    ctx = get_flow_context()
    ctx.update(
        {
            "target": settings.DBT_TARGET,
            "git_sha": _git_sha(),
            "slack_webhook_url": settings.SLACK_WEBHOOK_URL,
            "flow_name": "dbt_gold",
        }
    )

    try:
        deps_result = dbt_deps(settings)
        deps_summary = _summary_from_result(deps_result)
        persist_dbt_audit(engine, deps_result, deps_summary, ctx)
        raise_if_failed(deps_result, "dbt deps failed")

        gold_result = dbt_run_selector(settings, selector=SELECTOR_GOLD)
        gold_summary = _summary_from_result(gold_result)
        persist_dbt_audit(engine, gold_result, gold_summary, ctx)
        raise_if_failed(gold_result, "dbt gold stage failed")

        test_result = dbt_test_selector(settings, selector=SELECTOR_GOLD_TESTS)
        test_summary = _summary_from_result(test_result)
        persist_dbt_audit(engine, test_result, test_summary, ctx)

        # dbt test can return non-zero when data tests fail.
        if test_result.status != "success" and int(test_summary.get("tests_failed", 0)) <= 0:
            raise RuntimeError("dbt gold tests command failed unexpectedly")

        if should_alert_on_tests(test_summary):
            ctx["selector"] = SELECTOR_GOLD_TESTS
            notify_gold_tests_failed(ctx, test_summary)
            logger.warning(format_test_alert(test_summary, SELECTOR_GOLD_TESTS))

        logger.info("dbt_gold completed")
    except Exception as exc:  # noqa: BLE001
        notify_flow_failure(ctx, str(exc))
        raise
    finally:
        engine.dispose()


if __name__ == "__main__":
    dbt_gold()
