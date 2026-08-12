# ops.measurement_stream_state

`ops.measurement_stream_state` is the persisted carry-forward state table for
open-ended canonical measurement incrementals.

## Purpose

- Replace the runtime dependency on historical `intermediate` value history for
  per-stream carry-forward state.
- Store only the latest successfully published state per
  `(data_source_name, station_code, variable_code)`.
- Preserve the deterministic ordering metadata needed to recreate anchor rows
  during later incremental runs.

## Schema

The table DDL lives in `pipelines/sql/02_ops_audit.sql`. Both
`warehouse_bootstrap` and the dedicated `measurement_stream_state_bootstrap`
flow execute that DDL strictly: a DDL error fails the flow. Both deployments
are manual-only, so registering them does not execute their DDL. The stream
state bootstrap flow always creates or verifies the table before populating it.

Columns:

- `data_source_name text not null`
- `station_code text not null`
- `variable_code text not null`
- `last_measured_at_silver timestamptz not null`
- `last_cursor_id bigint null`
- `last_extracted_at timestamptz not null`
- `last_source_row_id text not null`
- `last_value_silver double precision not null`
- `updated_at timestamptz not null default now()`

Primary key:

- `(data_source_name, station_code, variable_code)`

Additional indexes:

- `idx_measurement_stream_state_updated_at`
- `idx_measurement_stream_state_data_source_watermark`

Deterministic watermark:

- `(last_measured_at_silver, coalesce(last_cursor_id, -1), last_extracted_at, last_source_row_id)`

`last_cursor_id` stays nullable for non-cursor sources, but it is still part of
the logical watermark when present.

## Refresh Contract

Refreshes into `ops.measurement_stream_state` must follow these rules:

1. Derive candidate rows only from successfully published canonical
   measurements.
2. Upsert by `(data_source_name, station_code, variable_code)`.
3. Advance a row only when the incoming deterministic watermark is newer than
   the stored watermark.
4. Treat a rerun of the same successful batch as a no-op for the persisted
   state fields.
5. Change `updated_at` only when the stored state changes.

This gives the runtime an idempotent state refresh contract: retries may
re-submit the same published batch without corrupting or regressing stream
continuity.

## Production Incremental Runtime

`canonical_incremental` now uses this table as the only carry-forward anchor
authority. The stored watermark must be strictly earlier than the first target
row before `last_value_silver` can seed forward-fill processing; equal or future
state is not used as an anchor.

After `silver.fct_measurements_silver` publishes successfully, the flow runs the
`canonical_incremental_state` selector. Its candidates must match a published
fact exactly on stream, source row, canonical timestamp, ingestion timestamp,
and value. It keeps only the newest candidate per natural stream and submits
only rows whose full watermark advances the stored state. Consequently, a
retry after a state-stage failure is safe, and rerunning stable inputs leaves
`updated_at` unchanged.

The flow fails before dbt processing when silver already contains history but
the state table is empty. A warehouse with neither silver history nor state
rows is allowed as a cold start.

## Bootstrap Contract

Before the publish-path cutover away from persisted intermediate history, run
the manual Prefect deployment:

- flow: `measurement_stream_state_bootstrap`
- deployment: `measurement-stream-state-bootstrap`
- work pool: `canonical`
- schedule: none

The flow uses the latest published row per stream from
`silver.fct_measurements_silver` as the publication authority, then joins that
small candidate set to `intermediate.int_measurements_values_silver`. The
intermediate row supplies `cursor_id` and the complete deterministic watermark.
The join must agree exactly on source identity, natural stream identity,
canonical time, extraction time, and canonical value.

The bootstrap runs in one repeatable-read transaction and takes a transaction
advisory lock. It fails and rolls back without partial state when:

- no published streams are found
- a published stream has no exact intermediate match
- a candidate has an incomplete required state field
- duplicate natural stream keys are found
- post-write state has missing, extra, or mismatched rows

The statement timeout defaults to `1800` seconds through
`MEASUREMENT_STREAM_STATE_BOOTSTRAP_TIMEOUT_S`. It can be overridden with the
flow parameter `statement_timeout_seconds`.

### Idempotent rerun

The upsert advances a stream only when its incoming deterministic watermark is
newer. Run the deployment twice against the same stable inputs. The second run
must report:

- `affected=0`
- `inserted=0`
- `updated=0`
- `unchanged=published_streams`
- `missing=0`, `extra=0`, and `mismatched=0`

Because the conflict update is skipped, `updated_at` remains unchanged.

### Demo and production cutover

1. Deploy the worker image and confirm these deployments are visible:
   - `warehouse_bootstrap/warehouse-bootstrap`
   - `measurement_stream_state_bootstrap/measurement-stream-state-bootstrap`
2. Run the stream-state bootstrap in demo and inspect its metrics. A read-only
   rehearsal on 2026-07-28 found `169/169` exact stream matches; this is a
   historical baseline, not a hard-coded expected count.
3. Run it again immediately and verify the idempotent rerun contract above.
4. Repeat the same two runs in production while the legacy intermediate tables
   remain available.
5. Immediately before the later publish-path cutover, pause the
   `canonical-incremental` schedule and wait for active runs to finish.
6. Rerun bootstrap twice and enable the new publisher only after full coverage
   and idempotency both pass.
7. Keep legacy intermediate history until shadow equivalence and the rollback
   window are complete.
8. Run `canonical_incremental` with a stable acceptance batch, verify
   carry-forward into silver, then rerun without changing inputs and confirm
   silver plus state values and `updated_at` are unchanged.

Until the dedicated full-refresh and backfill state-refresh work is complete,
rerun `measurement_stream_state_bootstrap` after either workflow and before
resuming `canonical_incremental`. Otherwise the first incremental could anchor
from state that predates the newly published history.

### Rollback

Before enabling the new publish path, copy the bootstrapped state to a dated
`ops.measurement_stream_state_pre_publish_cutover_*` snapshot. If the new path
is rejected:

1. switch execution back to the legacy publisher and resume its schedule
2. leave `ops.measurement_stream_state` in place for diagnosis; the legacy
   anchor path does not consume it
3. retain the snapshot and all legacy intermediate tables
4. before another cutover attempt, restore the snapshot or explicitly clear
   the state table and rerun the bootstrap from retained intermediate history

Clearing or restoring state is a deliberate operator action; the bootstrap
flow never deletes or regresses state automatically.
