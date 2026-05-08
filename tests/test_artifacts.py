from __future__ import annotations

import json

from pipelines.tasks.artifacts import load_run_results, summarize_run_results


def test_load_run_results_and_summarize(tmp_path):
    run_results_path = tmp_path / "run_results.json"
    payload = {
        "results": [
            {"unique_id": "model.respira_data.ok_model", "status": "success"},
            {
                "unique_id": "model.respira_data.bad_model",
                "status": "error",
                "message": "model failed",
            },
            {"unique_id": "test.respira_data.ok_test", "status": "pass"},
            {
                "unique_id": "test.respira_data.bad_test",
                "status": "fail",
                "message": "test failed",
            },
        ]
    }
    run_results_path.write_text(json.dumps(payload), encoding="utf-8")

    loaded = load_run_results(str(run_results_path))
    summary = summarize_run_results(loaded)

    assert summary["models_passed"] == 1
    assert summary["models_failed"] == 1
    assert summary["tests_passed"] == 1
    assert summary["tests_failed"] == 1
    assert "bad_test" in (summary["error_summary"] or "")


def test_summarize_empty_results():
    summary = summarize_run_results({})
    assert summary == {
        "models_passed": 0,
        "models_failed": 0,
        "tests_passed": 0,
        "tests_failed": 0,
        "error_summary": None,
    }
