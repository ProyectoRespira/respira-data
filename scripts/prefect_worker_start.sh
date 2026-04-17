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
  local pool_name="$3"
  shift 3

  local schedule=""
  if [[ "$#" -gt 0 ]]; then
    schedule="$1"
    shift
  fi

  local cmd=(
    prefect --no-prompt deploy "${entrypoint}"
    --name "${deployment_name}"
    --pool "${pool_name}"
  )

  if [[ -n "${schedule}" ]]; then
    cmd+=(--cron "${schedule}" --timezone "${PREFECT_SCHEDULE_TIMEZONE}")
  fi

  if [[ "$#" -gt 0 ]]; then
    cmd+=("$@")
  fi

  "${cmd[@]}"
}

ensure_work_pool() {
  local pool_name="$1"

  log "Ensuring work pool '${pool_name}' exists..."
  prefect --no-prompt work-pool create \
    --type "${PREFECT_WORKER_TYPE}" \
    "${pool_name}" \
    --overwrite
}

start_worker_for_pool() {
  local pool_name="$1"

  log "Starting worker for pool '${pool_name}'..."
  prefect worker start \
    --pool "${pool_name}" \
    --type "${PREFECT_WORKER_TYPE}" &

  WORKER_PIDS+=("$!")
}

cleanup_workers() {
  local exit_code="${1:-0}"

  if [[ "${#WORKER_PIDS[@]}" -gt 0 ]]; then
    kill "${WORKER_PIDS[@]}" 2>/dev/null || true
    wait "${WORKER_PIDS[@]}" 2>/dev/null || true
  fi

  exit "${exit_code}"
}

main() {
  export PREFECT_API_URL="${PREFECT_API_URL:-http://prefect_server:4200/api}"
  export PREFECT_WORKER_TYPE="${PREFECT_WORKER_TYPE:-process}"
  export PREFECT_SCHEDULE_TIMEZONE="${PREFECT_SCHEDULE_TIMEZONE:-UTC}"
  export PREFECT_CANONICAL_INCREMENTAL_CRON="${PREFECT_CANONICAL_INCREMENTAL_CRON:-5 * * * *}"
  export PREFECT_PROJECT_RESPIRA_GOLD_CRON="${PREFECT_PROJECT_RESPIRA_GOLD_CRON:-20 * * * *}"
  export PREFECT_CANONICAL_WORK_POOL="${PREFECT_CANONICAL_WORK_POOL:-${PREFECT_WORK_POOL:-canonical}}"
  export PREFECT_PROJECT_RESPIRA_GOLD_WORK_POOL="${PREFECT_PROJECT_RESPIRA_GOLD_WORK_POOL:-${PREFECT_WORK_POOL:-respira_gold}}"

  declare -a pools=(
    "${PREFECT_CANONICAL_WORK_POOL}"
    "${PREFECT_PROJECT_RESPIRA_GOLD_WORK_POOL}"
  )
  declare -A seen_pools=()
  declare -A started_pools=()
  declare -ag WORKER_PIDS=()

  wait_for_prefect_api

  for pool_name in "${pools[@]}"; do
    if [[ -n "${seen_pools[${pool_name}]:-}" ]]; then
      continue
    fi
    seen_pools["${pool_name}"]=1
    ensure_work_pool "${pool_name}"
  done

  log "Deploying canonical flows..."
  deploy_flow \
    "pipelines/flows/canonical_incremental.py:canonical_incremental" \
    "canonical-incremental" \
    "${PREFECT_CANONICAL_WORK_POOL}" \
    "${PREFECT_CANONICAL_INCREMENTAL_CRON}"
  deploy_flow \
    "pipelines/flows/canonical_full_refresh.py:canonical_full_refresh" \
    "canonical-full-refresh" \
    "${PREFECT_CANONICAL_WORK_POOL}"

  if [[ -n "${MODEL_6H_PATH:-}" && -n "${MODEL_12H_PATH:-}" ]]; then
    log "Model paths found. Deploying project pipeline with schedule..."
    deploy_flow \
      "pipelines/flows/project_pipeline.py:project_pipeline" \
      "project-pipeline-respira_gold" \
      "${PREFECT_PROJECT_RESPIRA_GOLD_WORK_POOL}" \
      "${PREFECT_PROJECT_RESPIRA_GOLD_CRON}" \
      --param "project_code=respira_gold"
  else
    log "MODEL_6H_PATH/MODEL_12H_PATH not set. Deploying project pipeline without schedule."
    deploy_flow \
      "pipelines/flows/project_pipeline.py:project_pipeline" \
      "project-pipeline-respira_gold" \
      "${PREFECT_PROJECT_RESPIRA_GOLD_WORK_POOL}" \
      "" \
      --param "project_code=respira_gold"
  fi

  trap 'cleanup_workers 0' INT TERM

  for pool_name in "${pools[@]}"; do
    if [[ -n "${started_pools[${pool_name}]:-}" ]]; then
      continue
    fi
    started_pools["${pool_name}"]=1
    start_worker_for_pool "${pool_name}"
  done

  wait -n "${WORKER_PIDS[@]}"
  cleanup_workers $?
}

main "$@"
