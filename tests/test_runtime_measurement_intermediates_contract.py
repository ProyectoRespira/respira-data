from __future__ import annotations

from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]


def _read(relative_path: str) -> str:
    return (REPO_ROOT / relative_path).read_text(encoding="utf-8")


def test_runtime_long_and_values_models_are_ephemeral():
    long_sql = _read(
        "dbt/models/canonical/intermediate/int_measurements_long.sql"
    ).lower()
    values_sql = _read(
        "dbt/models/canonical/intermediate/int_measurements_values_silver.sql"
    ).lower()

    assert "materialized='ephemeral'" in long_sql
    assert "materialized='ephemeral'" in values_sql
    assert "incremental_strategy" not in long_sql
    assert "incremental_strategy" not in values_sql
    assert "{{ this }}" not in long_sql
    assert "{{ this }}" not in values_sql


def test_runtime_queue_selection_uses_unmarked_rows_or_explicit_scope():
    long_sql = _read("dbt/models/canonical/intermediate/int_measurements_long.sql")
    batching_sql = _read("dbt/macros/measurement_batching.sql")

    assert "measurement_runtime_queue_row_predicate" in long_sql
    assert "flags.FULL_REFRESH" in batching_sql
    assert "measurement_has_process_batch_scope()" in batching_sql
    assert 'cleanup_eligible_column_name ~ " is null"' in batching_sql
    assert "measurement_process_row_predicate" in batching_sql


def test_silver_consumes_only_inline_values_without_historical_cutoff():
    fact_sql = _read("dbt/models/canonical/silver/fct_measurements_silver.sql")

    assert "ref('int_measurements_values_silver')" in fact_sql
    assert "existing_source_cutoffs" not in fact_sql
    assert "max_ingested_at" not in fact_sql
    assert "unique_key=['stream_id','timestamp']" in fact_sql


def test_debug_models_are_explicit_bounded_tables_in_intermediate_schema():
    project = _read("dbt/dbt_project.yml")
    selectors = _read("dbt/selectors.yml")
    debug_macro = _read("dbt/macros/measurement_debug.sql")
    debug_long = _read("dbt/models/canonical/debug/debug_int_measurements_long.sql")
    debug_values = _read(
        "dbt/models/canonical/debug/debug_int_measurements_values_silver.sql"
    )

    assert "debug:\n        +schema: intermediate" in project
    assert "- name: canonical_debug_intermediate" in selectors
    assert "value: models/canonical/debug" in selectors
    assert "measurement_batch_data_source" in debug_macro
    assert "requires both measured-time bounds" in debug_macro
    assert "materialized='table'" in debug_long
    assert "materialized='table'" in debug_values
    assert "alias='debug_int_measurements_long'" in debug_long
    assert "alias='debug_int_measurements_values_silver'" in debug_values


def test_retirement_operation_is_confirmed_paired_and_reversible():
    macro = _read("dbt/macros/manage_runtime_measurement_intermediates.sql")

    assert "confirm: true" in macro
    assert "config.materialized != 'ephemeral'" in macro
    assert "quarantine" in macro
    assert "restore" in macro
    assert "drop" in macro
    assert "_pre_ephemeral" in macro
    assert "both runtime relations must move together" in macro
    assert "refusing to manage non-table relation" in macro.lower()


def test_scoped_smoke_test_proves_every_queue_row_reaches_long():
    smoke_sql = _read("dbt/tests/batch_smoke/source_rows_preserved_in_long.sql")

    assert "measurement_runtime_queue_row_predicate" in smoke_sql
    assert "expected_row_count" in smoke_sql
    assert "actual_row_count" in smoke_sql
    assert "ref('int_measurements_long')" in smoke_sql
