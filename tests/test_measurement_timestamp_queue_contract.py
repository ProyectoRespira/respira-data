from __future__ import annotations

from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]


def _read(relative_path: str) -> str:
    return (REPO_ROOT / relative_path).read_text(encoding="utf-8")


def test_timestamp_relation_has_incremental_queue_contract():
    sql = _read(
        "dbt/models/canonical/intermediate/int_measurement_timestamps_silver.sql"
    )

    assert "materialized='incremental'" in sql
    assert "unique_key=['data_source_name', 'source_row_id']" in sql
    assert "incremental_strategy='merge'" in sql
    assert "tags=['measurement_processing_queue']" in sql
    assert "['data_source_name', 'extracted_at']" in sql
    assert "['data_source_name', 'measured_at_silver']" in sql


def test_incremental_checkpoint_is_derived_from_retained_queue_rows():
    macros = _read("dbt/macros/measurement_batching.sql")
    predicate = macros.split(
        "{% macro measurement_source_incremental_predicate", maxsplit=1
    )[1]

    assert "max(extracted_at)" in predicate
    assert "from " in predicate
    assert "this" in predicate
    assert "where data_source_name" in predicate


def test_backfill_bounds_are_derived_from_timestamp_queue():
    task_source = _read("pipelines/tasks/measurement_backfill.py")

    assert 'TIMESTAMPS_TABLE = "int_measurement_timestamps_silver"' in task_source
    assert "min(measured_at_silver) as min_measured_at" in task_source
    assert "max(measured_at_silver) as max_measured_at" in task_source
    assert "count(*) as row_count" in task_source
