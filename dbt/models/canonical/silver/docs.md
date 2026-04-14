{% docs fct_measurements_silver_contract %}

### fct_measurements_silver contract

This table is the canonical silver fact for time series ingestion.

Rules:
- Grain: one row per (stream_id, timestamp).
- timestamp is the silver measurement time after validation/imputation.
- value_parsed is a validated numeric value; invalid/out-of-range values are excluded.
- source_row_id points to the raw payload stored in int_measurement_payloads for auditability.
- No regular sampling guarantees (gaps and irregular intervals may exist).

{% enddocs %}
