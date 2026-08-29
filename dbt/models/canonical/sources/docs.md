{% docs measurement_stream_state_contract %}

### measurement_stream_state contract

`ops.measurement_stream_state` stores the latest successfully published
carry-forward state for each canonical measurement stream.

Rules:
- Grain: one row per `(data_source_name, station_code, variable_code)`.
- Deterministic watermark:
  `(last_measured_at_silver, coalesce(last_cursor_id, -1), last_extracted_at, last_source_row_id)`.
- `last_cursor_id` is nullable for sources that do not expose a stable cursor.
- Rows must be derived only from successfully published canonical measurements.
- Refreshes must upsert by the primary key and only advance state when the
  incoming watermark is newer than the stored watermark.
- Rerunning the same successful batch should be a no-op for the persisted state
  fields; `updated_at` should only change when the stored state changes.
- Bootstrap history comes from `silver.fct_measurements_silver`. The recent
  timestamp queue supplies `last_cursor_id` when that source row is still
  retained; otherwise bootstrap safely stores a null cursor and runtime refresh
  restores cursor-aware state on the next published advance.

{% enddocs %}
