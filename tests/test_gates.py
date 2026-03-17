from __future__ import annotations

from types import SimpleNamespace

import pytest

from tasks.gates import format_test_alert, raise_if_failed, should_alert_on_tests


def test_raise_if_failed_raises_for_failed_status():
    failed = SimpleNamespace(status="failed", command="dbt run --selector core")
    with pytest.raises(RuntimeError):
        raise_if_failed(failed, "core failed")


def test_raise_if_failed_noop_for_success():
    ok = SimpleNamespace(status="success", command="dbt run --selector core")
    raise_if_failed(ok, "should not raise")


def test_should_alert_on_tests_only_when_failed():
    assert should_alert_on_tests({"tests_failed": 1}) is True
    assert should_alert_on_tests({"tests_failed": 0}) is False


def test_format_test_alert_contains_selector_and_counts():
    msg = format_test_alert(
        {"tests_failed": 2, "tests_passed": 10, "error_summary": "bad null test"},
        selector="gold_tests",
    )
    assert "gold_tests" in msg
    assert "tests_failed=2" in msg
