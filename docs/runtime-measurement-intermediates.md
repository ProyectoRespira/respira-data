# Runtime Measurement Intermediates

`int_measurements_long` and `int_measurements_values_silver` are ephemeral dbt
models. Production runs inline their SQL into silver publication and do not
create or update historical relations with those names. Published history
continues to live in `silver.fct_measurements_silver`.

## Runtime selection and carry-in state

Normal incrementals process timestamp queue rows whose `cleanup_eligible_at` is
null. Explicit backfills process their source and half-open measured-time scope
regardless of that marker. A successful run marks the selected queue scope only
after silver publish, stream-state refresh, and both row-expansion and
values-to-fact smoke tests pass.

Value forward-fill uses:

- `ops.measurement_stream_state` for normal open-ended processing;
- the latest published silver row before a bounded historical window; and
- no carry-in anchor during a full refresh.

The temporary `int_measurements_time_silver` compatibility view exposes the
same recent or explicitly scoped inline transformation. It is not a historical
contract, and broad ad hoc queries can be expensive.

## Bounded debug materialization

The debug selector writes fixed tables to `intermediate` and replaces them on
each run:

- `debug_int_measurements_long`
- `debug_int_measurements_values_silver`

A source plus a bounded measured-time window is required:

```bash
dbt run --target prod --selector canonical_debug_intermediate --vars '{
  "measurement_batch_data_source": "airelibre_airbyte",
  "measurement_batch_measured_at_from": "2026-01-01T19:00:00Z",
  "measurement_batch_measured_at_to": "2026-01-01T20:00:00Z"
}'
```

For the dedicated null-time pass, provide the source and
`measurement_batch_include_null_time_rows: true` without measured-time bounds.
Unbounded execution is rejected.

Raw payload inspection uses a separate selector and fixed table so it does not
depend on the production payload audit relation:

```bash
dbt run --target prod --selector canonical_debug_payload_audit --vars '{
  "measurement_batch_data_source": "airelibre_airbyte",
  "measurement_batch_extracted_at_from": "2026-01-01T19:00:00Z",
  "measurement_batch_extracted_at_to": "2026-01-01T20:00:00Z"
}'
```

This replaces `intermediate.debug_int_measurement_payloads` on every run.
Both extracted-time bounds and one registered source are required; unbounded
payload debugging is rejected.

## Opt-in production payload audit

Normal incremental, core, and full-refresh selectors do not build
`intermediate.int_measurement_payloads`. A backfill builds it only when
`include_payload_audit=True` is combined with `run_ingest=True`; the audit uses
the same source and optional extracted-time bounds as that ingest scope.

After validating both explicit payload paths, quarantine the old historical
audit table:

```bash
dbt run-operation manage_measurement_payload_audit \
  --target prod \
  --args '{action: quarantine, confirm: true}'
```

Restore it while the canonical name remains free, or drop only its backup after
the observation period:

```bash
dbt run-operation manage_measurement_payload_audit \
  --target prod \
  --args '{action: restore, confirm: true}'

dbt run-operation manage_measurement_payload_audit \
  --target prod \
  --args '{action: drop, confirm: true}'
```

The backup is named `int_measurement_payloads_pre_opt_in`. If an explicit
backfill recreates the canonical audit table during the observation period,
restore refuses the conflicting state while drop remains limited to the old
backup.

## Retiring legacy physical tables

Changing a dbt model to ephemeral does not remove a relation created by an
older deployment. Manage the two legacy tables as a pair.

After deploying and validating a scoped run, quarantine them:

```bash
dbt run-operation manage_runtime_measurement_intermediates \
  --target prod \
  --args '{action: quarantine, confirm: true}'
```

This renames them to:

- `int_measurements_long_pre_ephemeral`
- `int_measurements_values_silver_pre_ephemeral`

If validation fails, restore both:

```bash
dbt run-operation manage_runtime_measurement_intermediates \
  --target prod \
  --args '{action: restore, confirm: true}'
```

After the observation period, reclaim storage:

```bash
dbt run-operation manage_runtime_measurement_intermediates \
  --target prod \
  --args '{action: drop, confirm: true}'
```

The operation verifies that both runtime models are ephemeral, accepts only
tables, refuses partial/ambiguous states, and never drops canonical relation
names directly.

## Recovering a pre-marker queue

Do not run an unbounded `canonical_incremental` when a pre-existing queue has a
large `cleanup_eligible_at is null` baseline. The incremental flow now refuses
an estimated inline expansion above
`MEASUREMENT_INCREMENTAL_MAX_EXPANDED_ROWS` (default `2000000`) and directs the
operator to `canonical_measurement_queue_cutover`.

The cutover deployment is manual and defaults to `mode=plan`. Keep the normal
incremental schedule paused and keep both legacy physical intermediate tables
at their canonical names. First run plan mode for one source and review its
nonempty windows, row estimates, oversized-timestamp warnings, null-time count,
and required-index status. Execute the same source with:

- `mode=execute`
- `confirm=true`
- `repair_failed_windows=true`

If one measured timestamp alone exceeds either the 250000-row queue limit or
the 2000000-row expanded limit, plan mode reports it and execute mode refuses
the source. The scope must be investigated explicitly; cutover never bypasses
the cap merely because a time boundary cannot split it.

Execution reconciles queue indexes concurrently. For each bounded window it
uses the legacy tables as publication witnesses, refreshes stream state, and
checks the current inline transformation. A witness or inline mismatch is
repaired through `canonical_batch_process`; cleanup occurs only after the
post-repair smoke tests pass. Completed windows are marked/cleaned immediately,
so rerunning the deployment resumes from unmarked rows.

The historical cutover long-expansion smoke test evaluates the entire bounded
scope and requires a configurable complete-row coverage ratio, defaulting to
`0.90` through the dbt variable
`measurement_cutover_min_long_coverage_ratio`. This is a deterministic coverage
gate, not random sampling or a statistical confidence interval. Exact long-row
expansion remains required during normal incremental processing, and fact and
stream-state smoke tests remain exact for every value produced by the runtime
transformation.

Use this demo order to reduce risk: `meteostat_airbyte`, `fiuna_airbyte`,
`airelibre_airbyte`, `mades_open_airbyte`, then `respira_airbyte`. Null-time
rows run in a dedicated source-wide pass. More than 250000 null-time rows are
rejected unless `allow_oversized_null_time_scope=true` is explicitly supplied.

After every source finishes, run `canonical_incremental` twice. The first run
must remain below the expansion cap and the second must be idempotent. Only then
use the quarantine operation above. Plan mode never runs dbt or changes queue
eligibility.

All production measurement writers share a PostgreSQL advisory lock. Timed-out
dbt subprocesses receive a unique database application name; orchestration
cancels only sessions with that exact name and terminates a tagged backend if
it survives cancellation. Commands with an enabled timeout always use the
cancellable subprocess runner, even if `DBT_USE_PREFECT_DBT=true`.

## Demo acceptance

1. Record count and checksum evidence for a bounded silver window.
2. Run the scoped backfill with smoke tests enabled.
3. Quarantine the legacy tables.
4. Repeat the scoped backfill and run `canonical_incremental` twice.
5. Confirm silver is unchanged, the second incremental is idempotent, stream
   state does not regress, retention succeeds, and no missing-relation error
   references the quarantined tables.
6. Run the debug selector for the same scope.
7. Drop the quarantined tables only after the observation period.
