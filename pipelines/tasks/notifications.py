from __future__ import annotations

import json
from typing import Any
from urllib import request

from pipelines.compat import get_run_logger, task


def _send_slack(webhook_url: str | None, message: str) -> None:
    logger = get_run_logger()
    if not webhook_url:
        logger.info("Slack webhook not configured. Skipping alert.")
        return

    payload = json.dumps({"text": message}).encode("utf-8")
    req = request.Request(
        webhook_url,
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )

    try:
        with request.urlopen(req, timeout=10) as response:
            logger.info("Slack notification sent with status code %s", response.status)
    except Exception as exc:  # noqa: BLE001
        logger.warning("Failed to send Slack notification: %s", exc)


@task(name="notify_slack")
def notify_slack(webhook_url: str | None, message: str) -> None:
    _send_slack(webhook_url, message)


@task(name="notify_dbt_tests_failed")
def notify_dbt_tests_failed(ctx: dict[str, Any], summary: dict[str, Any]) -> None:
    selector = ctx.get("selector", "unknown_tests_selector")
    tests_failed = int(summary.get("tests_failed", 0))
    error_summary = summary.get("error_summary") or "No error summary"
    project_code = ctx.get("project_code")
    scope = f" project='{project_code}'" if project_code else ""
    message = (
        f"[Respira] dbt tests failed on selector '{selector}'{scope}. "
        f"tests_failed={tests_failed}. error_summary={error_summary}"
    )
    _send_slack(ctx.get("slack_webhook_url"), message)


@task(name="notify_flow_failure")
def notify_flow_failure(ctx: dict[str, Any], message: str) -> None:
    flow_name = ctx.get("flow_name", "unknown_flow")
    flow_run_id = ctx.get("flow_run_id", "unknown")
    alert_message = f"[Respira] Flow failure in '{flow_name}' ({flow_run_id}): {message}"
    _send_slack(ctx.get("slack_webhook_url"), alert_message)
