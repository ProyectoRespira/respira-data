from __future__ import annotations

import subprocess
from datetime import datetime
from pathlib import Path

from pipelines.compat import flow, get_flow_context, get_run_logger
from pipelines.config.projects import get_project_config
from pipelines.config.settings import get_settings
from pipelines.flows.project_inference import project_inference
from pipelines.tasks.artifacts import load_run_results, persist_dbt_audit, summarize_run_results
from pipelines.tasks.db import ensure_ops_audit_tables, get_engine
from pipelines.tasks.dbt_tasks import dbt_deps, dbt_run_selector, dbt_test_selector
from pipelines.tasks.gates import format_test_alert, raise_if_failed, should_alert_on_tests
from pipelines.tasks.notifications import notify_dbt_tests_failed, notify_flow_failure

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


@flow(name="project_pipeline")
def project_pipeline(project_code: str, as_of: datetime | None = None) -> None:
    logger = get_run_logger()
    settings = get_settings()
    project = get_project_config(project_code)
    engine = get_engine(settings)
    ensure_ops_audit_tables(engine)

    ctx = get_flow_context()
    ctx.update(
        {
            "target": settings.DBT_TARGET,
            "git_sha": _git_sha(),
            "project_code": project.project_code,
            "slack_webhook_url": settings.SLACK_WEBHOOK_URL,
            "flow_name": "project_pipeline",
        }
    )

    try:
        deps_result = dbt_deps(settings)
        deps_summary = _summary_from_result(deps_result)
        persist_dbt_audit(engine, deps_result, deps_summary, ctx)
        raise_if_failed(deps_result, "dbt deps failed")

        project_result = dbt_run_selector(settings, selector=project.dbt_selector)
        project_summary = _summary_from_result(project_result)
        persist_dbt_audit(engine, project_result, project_summary, ctx)
        raise_if_failed(project_result, f"dbt project stage failed for {project.project_code}")

        test_result = dbt_test_selector(settings, selector=project.dbt_tests_selector)
        test_summary = _summary_from_result(test_result)
        persist_dbt_audit(engine, test_result, test_summary, ctx)

        if test_result.status != "success" and int(test_summary.get("tests_failed", 0)) <= 0:
            raise RuntimeError(f"dbt project tests command failed unexpectedly for {project.project_code}")

        if should_alert_on_tests(test_summary):
            ctx["selector"] = project.dbt_tests_selector
            notify_dbt_tests_failed(ctx, test_summary)
            logger.warning(format_test_alert(test_summary, project.dbt_tests_selector))

        if project.inference_enabled:
            project_inference(project_code=project.project_code, as_of=as_of, engine=engine)

        logger.info("project_pipeline completed for project_code=%s", project.project_code)
    except Exception as exc:  # noqa: BLE001
        notify_flow_failure(ctx, str(exc))
        raise
    finally:
        engine.dispose()


if __name__ == "__main__":
    project_pipeline(project_code="respira_gold")
