#!/usr/bin/env bash
set -euo pipefail

log() {
  printf '[prefect-worker] %s\n' "$*"
}

wait_for_prefect_api() {
  local max_attempts="${PREFECT_API_WAIT_MAX_ATTEMPTS:-90}"
  local sleep_seconds="${PREFECT_API_WAIT_SLEEP_SECONDS:-2}"
  local attempt=1

  while (( attempt <= max_attempts )); do
    if python3 - <<'PY'
import os
import sys
import urllib.request

api_url = os.environ.get("PREFECT_API_URL", "").rstrip("/")
if not api_url:
    sys.exit(1)

health_url = f"{api_url}/health"
try:
    with urllib.request.urlopen(health_url, timeout=5) as response:
        body = response.read().decode("utf-8").strip().lower()
        if response.status == 200 and body in {"true", '"true"', "1"}:
            sys.exit(0)
except Exception:
    pass

sys.exit(1)
PY
    then
      log "Prefect API ready at ${PREFECT_API_URL}"
      return 0
    fi

    log "Waiting for Prefect API (${attempt}/${max_attempts})..."
    attempt=$((attempt + 1))
    sleep "${sleep_seconds}"
  done

  log "Prefect API did not become ready in time."
  return 1
}

deploy_flow() {
  local entrypoint="$1"
  local deployment_name="$2"
  local schedule="${3:-}"

  local cmd=(
    prefect --no-prompt deploy "${entrypoint}"
    --name "${deployment_name}"
    --pool "${PREFECT_WORK_POOL}"
  )

  if [[ -n "${schedule}" ]]; then
    cmd+=(--cron "${schedule}" --timezone "${PREFECT_SCHEDULE_TIMEZONE}")
  fi

  "${cmd[@]}"
}

main() {
  export PREFECT_API_URL="${PREFECT_API_URL:-http://prefect_server:4200/api}"
  export PREFECT_WORK_POOL="${PREFECT_WORK_POOL:-default}"
  export PREFECT_WORKER_TYPE="${PREFECT_WORKER_TYPE:-process}"
  export PREFECT_SCHEDULE_TIMEZONE="${PREFECT_SCHEDULE_TIMEZONE:-UTC}"
  export PREFECT_DBT_INCREMENTAL_CRON="${PREFECT_DBT_INCREMENTAL_CRON:-5 * * * *}"
  export PREFECT_DBT_GOLD_CRON="${PREFECT_DBT_GOLD_CRON:-15 * * * *}"
  export PREFECT_GOLD_THEN_INFERENCE_CRON="${PREFECT_GOLD_THEN_INFERENCE_CRON:-20 * * * *}"
  export PREFECT_INFERENCE_CRON="${PREFECT_INFERENCE_CRON:-25 * * * *}"

  wait_for_prefect_api

  log "Ensuring work pool '${PREFECT_WORK_POOL}' exists..."
  prefect --no-prompt work-pool create \
    --type "${PREFECT_WORKER_TYPE}" \
    "${PREFECT_WORK_POOL}" \
    --overwrite

  log "Deploying dbt flows..."
  deploy_flow "prefect/flows/dbt_incremental.py:dbt_incremental" "dbt-incremental" "${PREFECT_DBT_INCREMENTAL_CRON}"
  deploy_flow "prefect/flows/dbt_gold.py:dbt_gold" "dbt-gold" "${PREFECT_DBT_GOLD_CRON}"
  deploy_flow "prefect/flows/dbt_full_refresh.py:dbt_full_refresh" "dbt-full-refresh"

  if [[ -n "${MODEL_6H_PATH:-}" && -n "${MODEL_12H_PATH:-}" ]]; then
    log "Model paths found. Deploying inference flows with schedules..."
    deploy_flow "prefect/flows/inference_per_station.py:inference_per_station" "inference-per-station" "${PREFECT_INFERENCE_CRON}"
    deploy_flow "prefect/flows/gold_then_inference.py:gold_then_inference" "gold-then-inference" "${PREFECT_GOLD_THEN_INFERENCE_CRON}"
  else
    log "MODEL_6H_PATH/MODEL_12H_PATH not set. Deploying inference flows without schedules."
    deploy_flow "prefect/flows/inference_per_station.py:inference_per_station" "inference-per-station"
    deploy_flow "prefect/flows/gold_then_inference.py:gold_then_inference" "gold-then-inference"
  fi

  log "Starting worker..."
  exec prefect worker start \
    --pool "${PREFECT_WORK_POOL}" \
    --type "${PREFECT_WORKER_TYPE}"
}

main "$@"
