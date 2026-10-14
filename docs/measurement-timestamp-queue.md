# Measurement Timestamp Processing Queue

`intermediate.int_measurement_timestamps_silver` is an operational processing
queue. It is not a historical dataset or a downstream analytical contract.
Canonical history remains in `silver.fct_measurements_silver`.

## Contract and grain

The queue has exactly one row per `(data_source_name, source_row_id)`. A dbt
unique test and the incremental merge key enforce that grain. Re-ingesting the
same source row updates the queued canonical-time fields instead of appending a
second copy.

The queue has three runtime responsibilities:

- `extracted_at` provides the per-source incremental ingest checkpoint.
- `measured_at_silver` defines the half-open `[from, to)` windows used by the
  backfill process phase.
- the retained source-row identities join the prepared wide source relation to
  long/value processing, which allows a failed or interrupted backfill to resume
  without repeating ingest.

Rows with `measured_at_silver is null` remain at the same queue grain. The
backfill flow counts them separately and processes them in its dedicated
null-time pass.

## Incremental behavior

Normal incremental ingestion merges source rows into the queue by
`(data_source_name, source_row_id)`. For each source, the greatest retained
`extracted_at` is the checkpoint used to select the next source rows. The
inclusive boundary intentionally re-reads the checkpoint edge; merge semantics
make that retry idempotent.

The queue is only a work-selection and coordination layer. Successful canonical
history and carry-forward state live in `silver.fct_measurements_silver` and
`ops.measurement_stream_state`, respectively.

## Backfill process bounds and resume

After the ingest phase, `canonical_measurement_backfill` reads the source's
minimum and maximum non-null `measured_at_silver` directly from the queue. It
intersects those bounds with optional `process_measured_at_from` and
`process_measured_at_to` arguments, then creates half-open process windows.

`run_ingest=False` resumes against already-landed queue rows. This behavior is
guaranteed only while every source row required by the requested process scope
still remains in the queue. If the source has no retained rows, or a requested
measured-time scope does not overlap its retained rows, the flow fails and asks
the operator to rerun with `run_ingest=True`.

This is intentionally not a permanent replay guarantee. Replaying data outside
queue retention requires re-landing it from raw/prepared staging.

## Cleanup eligibility

Retention age is a cleanup floor, not proof that a row is safe to delete. A
queue row becomes eligible only after all work for its process scope has
completed successfully:

1. the silver publish succeeded;
2. `ops.measurement_stream_state` refreshed from that successful publish; and
3. the source-scoped smoke tests passed.

The successful gate records `cleanup_eligible_at` on every queue row in the
verified scope. Cleanup requires this marker as well as sufficient age, so an
old row from a failed or unfinished run cannot be deleted by a later scheduled
incremental. Incremental merges preserve this orchestration-owned marker for
matched source rows, including the intentionally re-read checkpoint edge; only
new queue rows enter with a null marker and require processing. The scoped
row-expansion smoke test replaces any dependency on persisted long/value
intermediates.

For a backfill, eligibility is evaluated window by window so an unfinished
window remains resumable. Null-time rows are not eligible until their dedicated
pass succeeds. Recent rows stay in the queue even when otherwise eligible, as
defined by the configured retention floor.

Automatic cleanup runs with a default retention floor of `168` hours, configured
through `MEASUREMENT_TIMESTAMP_QUEUE_RETENTION_HOURS`. Age is evaluated from
`extracted_at` using database time. The floor is a minimum retention period:
being old is necessary but is never sufficient by itself.

Cleanup sequencing is enforced by orchestration:

- normal incrementals publish silver, refresh stream state, pass the canonical
  smoke tests, and then prune eligible non-null rows;
- backfills perform publish/state refresh, scoped smoke tests, and cleanup for
  each successful measured-time window before moving to the next window;
- null-time rows use a separate process/test/cleanup pass after all measured-time
  windows succeed, and only that pass may set their eligibility marker;
- disabling backfill smoke tests with `run_tests=False` also disables cleanup.

One deterministic row at the newest `extracted_at` edge for every source is
exempt from deletion even when it is older than the retention floor. This is
the durable incremental checkpoint that prevents an inactive source from
falling back to a full raw history scan. Other rows sharing that extraction
timestamp may be removed after their own process scope succeeds.

If publication, state refresh, smoke tests, or the cleanup transaction fails,
the affected queue rows remain. Completed old backfill windows may already have
been pruned before a later window fails; `run_ingest=False` then resumes from
the rows belonging to the unfinished retained windows.

## Operational guidance

- Use `run_ingest=False` only to continue a previously landed backfill.
- If the resume guard reports missing rows, rerun with `run_ingest=True` and the
  original source/extraction scope.
- Query silver, not this queue, for canonical history.
- Do not manually delete rows from an active or failed process window.
- Set `MEASUREMENT_TIMESTAMP_QUEUE_RETENTION_HOURS` to a positive integer and
  size it for the environment's normal incident-response window.
- Normal incrementals fail before silver when the estimated unmarked expansion
  exceeds `MEASUREMENT_INCREMENTAL_MAX_EXPANDED_ROWS` (default `2000000`). Use
  the guarded queue-cutover deployment to drain that backlog window by window;
  do not bypass the guard by only increasing the dbt timeout.
