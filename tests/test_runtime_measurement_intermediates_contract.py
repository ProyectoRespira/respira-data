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


def test_production_selectors_exclude_standalone_runtime_intermediates():
    selectors = _read("dbt/selectors.yml")
    compatibility_view = _read(
        "dbt/models/canonical/intermediate/int_measurements_time_silver.sql"
    )

    for selector_name in (
        "canonical_core",
        "canonical_incremental_core",
        "canonical_full_refresh",
    ):
        selector = selectors.split(f"- name: {selector_name}", maxsplit=1)[1]
        selector = selector.split("- name:", maxsplit=1)[0]
        assert "exclude:" in selector
        assert "int_measurements_long.sql" in selector
        assert "int_measurements_values_silver.sql" in selector
        assert "int_measurement_payloads.sql" in selector
        assert "int_measurements_time_silver.sql" not in selector

    assert "materialized='view'" in compatibility_view
    assert "ref('int_measurements_long')" in compatibility_view


def test_runtime_queue_selection_uses_unmarked_rows_or_explicit_scope():
    long_sql = _read("dbt/models/canonical/intermediate/int_measurements_long.sql")
    batching_sql = _read("dbt/macros/measurement_batching.sql")

    assert "measurement_runtime_queue_row_predicate" in long_sql
    assert "flags.FULL_REFRESH" in batching_sql
    assert "measurement_has_process_batch_scope()" in batching_sql
    assert 'cleanup_eligible_column_name ~ " is null"' in batching_sql
    assert "measurement_process_row_predicate" in batching_sql
    assert "measurement_batch_unmarked_only" in batching_sql


def test_silver_consumes_only_inline_values_without_historical_cutoff():
    fact_sql = _read("dbt/models/canonical/silver/fct_measurements_silver.sql")

    assert "ref('int_measurements_values_silver')" in fact_sql
    assert "existing_source_cutoffs" not in fact_sql
    assert "max_ingested_at" not in fact_sql
    assert "unique_key=['stream_id','timestamp']" in fact_sql
    assert "order by ingested_at desc, source_row_id desc" in fact_sql


def test_debug_models_are_explicit_bounded_tables_in_intermediate_schema():
    project = _read("dbt/dbt_project.yml")
    selectors = _read("dbt/selectors.yml")
    debug_macro = _read("dbt/macros/measurement_debug.sql")
    debug_long = _read("dbt/models/canonical/debug/debug_int_measurements_long.sql")
    debug_values = _read(
        "dbt/models/canonical/debug/debug_int_measurements_values_silver.sql"
    )
    debug_payloads = _read(
        "dbt/models/canonical/debug/debug_int_measurement_payloads.sql"
    )

    assert "debug:\n        +schema: intermediate" in project
    assert "- name: canonical_debug_intermediate" in selectors
    assert "models/canonical/debug/debug_int_measurements_long.sql" in selectors
    assert (
        "models/canonical/debug/debug_int_measurements_values_silver.sql" in selectors
    )
    assert "- name: canonical_debug_payload_audit" in selectors
    assert "models/canonical/debug/debug_int_measurement_payloads.sql" in selectors
    assert "measurement_batch_data_source" in debug_macro
    assert "requires both measured-time bounds" in debug_macro
    assert "requires both extracted-time bounds" in debug_macro
    assert "materialized='table'" in debug_long
    assert "materialized='table'" in debug_values
    assert "materialized='table'" in debug_payloads
    assert "alias='debug_int_measurements_long'" in debug_long
    assert "alias='debug_int_measurements_values_silver'" in debug_values
    assert "alias='debug_int_measurement_payloads'" in debug_payloads
    assert "measurement_payloads_from_source" in debug_payloads


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


def test_payload_audit_is_explicit_and_has_guarded_retirement():
    selectors = _read("dbt/selectors.yml")
    payload_model = _read(
        "dbt/models/canonical/intermediate/int_measurement_payloads.sql"
    )
    payload_guard = _read("dbt/macros/measurement_payload_audit.sql")
    retirement = _read("dbt/macros/manage_measurement_payload_audit.sql")

    for selector_name in (
        "canonical_core",
        "canonical_incremental_core",
        "canonical_full_refresh",
    ):
        selector = selectors.split(f"- name: {selector_name}", maxsplit=1)[1]
        selector = selector.split("- name:", maxsplit=1)[0]
        assert "exclude:" in selector
        assert "int_measurement_payloads.sql" in selector

    assert "- name: canonical_batch_payload_audit" in selectors
    assert "validate_measurement_payload_audit_scope" in payload_model
    assert "requires an explicit measurement_batch_data_source" in payload_guard
    assert "confirm: true" in retirement
    assert "_pre_opt_in" in retirement
    assert "canonical_relation is not none" in retirement
    assert "drop table {{ backup_relation }}" in retirement


def test_scoped_smoke_test_uses_exact_runtime_and_cutover_coverage_gates():
    smoke_sql = _read("dbt/tests/batch_smoke/source_rows_preserved_in_long.sql")
    project = _read("dbt/dbt_project.yml")

    assert "measurement_runtime_queue_row_predicate" in smoke_sql
    assert "expected_row_count" in smoke_sql
    assert "actual_row_count" in smoke_sql
    assert "ref('int_measurements_long')" in smoke_sql
    assert "measurement_batch_unmarked_only()" in smoke_sql
    assert "source('legacy_runtime_intermediate', 'int_measurements_long')" in smoke_sql
    assert "coverage_ratio" in smoke_sql
    assert "where not is_complete" in smoke_sql
    assert "measurement_cutover_min_long_coverage_ratio: 0.90" in project


def test_cutover_witnesses_are_bounded_and_use_physical_legacy_sources():
    selectors = _read("dbt/selectors.yml")
    guard = _read("dbt/macros/measurement_cutover.sql")
    long_witness = _read("dbt/tests/cutover/legacy_long_rows_match_queue.sql")
    values_witness = _read("dbt/tests/cutover/legacy_values_match_published_silver.sql")

    assert "canonical_cutover_witness_tests" in selectors
    assert "require both measured-time bounds" in guard
    assert "legacy_runtime_intermediate" in long_witness
    assert "legacy_runtime_intermediate" in values_witness
    assert "cleanup_eligible_at is null" in long_witness
    assert "measurement_cutover_witness_enabled" in long_witness


def test_batch_smoke_checks_stream_state_coverage():
    state_smoke = _read("dbt/tests/batch_smoke/source_state_covers_published_queue.sql")

    assert "measurement_runtime_queue_row_predicate" in state_smoke
    assert "ref('measurement_stream_state')" in state_smoke
    assert "last_measured_at_silver" in state_smoke


def test_queue_runtime_indexes_have_existing_relation_reconciliation():
    queue_model = _read(
        "dbt/models/canonical/intermediate/int_measurement_timestamps_silver.sql"
    )
    runtime_task = _read("pipelines/tasks/measurement_runtime.py")

    assert "['data_source_name', 'measured_at_silver']" in queue_model
    assert "['cleanup_eligible_at', 'extracted_at']" in queue_model
    assert "['data_source_name', 'extracted_at', 'source_row_id']" in queue_model
    assert "merge_exclude_columns=['cleanup_eligible_at']" in queue_model
    assert "create index concurrently if not exists" in runtime_task
    assert "indisvalid" in runtime_task


def test_dbt_profile_accepts_per_process_application_name():
    profiles = _read("dbt/profiles.yml")

    assert profiles.count("DBT_APPLICATION_NAME") == 2
