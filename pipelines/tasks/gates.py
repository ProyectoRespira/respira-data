from __future__ import annotations


def raise_if_failed(dbt_result, message: str) -> None:
    if dbt_result.status != "success":
        raise RuntimeError(f"{message}: {dbt_result.command}")


def should_alert_on_tests(summary: dict) -> bool:
    return int(summary.get("tests_failed", 0)) > 0


def format_test_alert(summary: dict, selector: str) -> str:
    tests_failed = int(summary.get("tests_failed", 0))
    tests_passed = int(summary.get("tests_passed", 0))
    error_summary = summary.get("error_summary") or "No error summary available"
    return (
        f"dbt tests alert for selector '{selector}': "
        f"tests_failed={tests_failed}, tests_passed={tests_passed}. "
        f"errors={error_summary}"
    )
