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
  shift 3 || true

  local cmd=(
    prefect --no-prompt deploy "${entrypoint}"
    --name "${deployment_name}"
    --pool "${PREFECT_WORK_POOL}"
  )

  if [[ -n "${schedule}" ]]; then
    cmd+=(--cron "${schedule}" --timezone "${PREFECT_SCHEDULE_TIMEZONE}")
  fi

  if [[ "$#" -gt 0 ]]; then
    cmd+=("$@")
  fi

  "${cmd[@]}"
}

main() {
  export PREFECT_API_URL="${PREFECT_API_URL:-http://prefect_server:4200/api}"
  export PREFECT_WORK_POOL="${PREFECT_WORK_POOL:-default}"
  export PREFECT_WORKER_TYPE="${PREFECT_WORKER_TYPE:-process}"
  export PREFECT_SCHEDULE_TIMEZONE="${PREFECT_SCHEDULE_TIMEZONE:-UTC}"
  export PREFECT_CANONICAL_INCREMENTAL_CRON="${PREFECT_CANONICAL_INCREMENTAL_CRON:-5 * * * *}"
  export PREFECT_PROJECT_RESPIRA_GOLD_CRON="${PREFECT_PROJECT_RESPIRA_GOLD_CRON:-20 * * * *}"

  wait_for_prefect_api

  log "Ensuring work pool '${PREFECT_WORK_POOL}' exists..."
  prefect --no-prompt work-pool create \
    --type "${PREFECT_WORKER_TYPE}" \
    "${PREFECT_WORK_POOL}" \
    --overwrite

  log "Deploying canonical flows..."
  deploy_flow \
    "prefect/flows/canonical_incremental.py:canonical_incremental" \
    "canonical-incremental" \
    "${PREFECT_CANONICAL_INCREMENTAL_CRON}"
  deploy_flow \
    "prefect/flows/canonical_full_refresh.py:canonical_full_refresh" \
    "canonical-full-refresh"

  if [[ -n "${MODEL_6H_PATH:-}" && -n "${MODEL_12H_PATH:-}" ]]; then
    log "Model paths found. Deploying project pipeline with schedule..."
    deploy_flow \
      "prefect/flows/project_pipeline.py:project_pipeline" \
      "project-pipeline-respira_gold" \
      "${PREFECT_PROJECT_RESPIRA_GOLD_CRON}" \
      --param "project_code=respira_gold"
  else
    log "MODEL_6H_PATH/MODEL_12H_PATH not set. Deploying project pipeline without schedule."
    deploy_flow \
      "prefect/flows/project_pipeline.py:project_pipeline" \
      "project-pipeline-respira_gold" \
      "" \
      --param "project_code=respira_gold"
  fi

  log "Starting worker..."
  exec prefect worker start \
    --pool "${PREFECT_WORK_POOL}" \
    --type "${PREFECT_WORKER_TYPE}"
}

main "$@"
