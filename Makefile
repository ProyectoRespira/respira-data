SHELL := /bin/bash
.DEFAULT_GOAL := help

# Docker Compose wrapper (v2)
DC := docker compose

# Run dbt inside the app container (ephemeral)
DBT := $(DC) run --rm app bash -lc

# Run Prefect flows inside the app container (ephemeral)
PREFECT_RUN := $(DC) run --rm app bash -lc

# Run inference-related flows inside the worker container (ephemeral)
WORKER_RUN := $(DC) run --rm prefect_worker bash -lc

# Optional: run arbitrary shell inside the app container (ephemeral)
APP_SHELL := $(DC) run --rm app bash

.PHONY: help
help:
	@echo "Targets:"
	@echo "  up                Start services (prefect_server, app, prefect_worker)"
	@echo "  up-build          Rebuild images and start all services"
	@echo "  down              Stop services"
	@echo "  ps                Show running containers"
	@echo "  logs              Tail logs (all services)"
	@echo "  logs-worker       Tail logs (prefect_worker)"
	@echo "  shell             Open a shell in the app container"
	@echo ""
	@echo "dbt:"
	@echo "  dbt-debug         dbt debug"
	@echo "  dbt-deps          dbt deps"
	@echo "  seed              dbt seed"
	@echo "  seed-fr           dbt seed --full-refresh"
	@echo "  run               dbt run (all models)"
	@echo "  test              dbt test (all tests)"
	@echo ""
	@echo "Layered runs:"
	@echo "  run-canonical-core    dbt run --selector canonical_core"
	@echo "  run-canonical-silver  dbt run --selector canonical_silver"
	@echo "  run-project-respira_gold dbt run --selector project_respira_gold"
	@echo ""
	@echo "Build flows:"
	@echo "  build             deps + seed + run-all + test"
	@echo "  build-fr          deps + seed(full refresh) + run(full refresh) + test"
	@echo ""
	@echo "Prefect:"
	@echo "  prefect-bootstrap Ensure ops tables and project inference tables"
	@echo "  run-canonical-incremental Run canonical_incremental flow"
	@echo "  run-canonical-full-refresh Run canonical_full_refresh flow (manual)"
	@echo "  run-project-pipeline Run project_pipeline for respira_gold"
	@echo "  run-project-inference Run project_inference for respira_gold"
	@echo "  smoke-test        Run minimal unit tests for orchestration"
	@echo ""
	@echo "Selection helpers:"
	@echo "  ls                dbt ls"
	@echo "  docs              dbt docs generate"
	@echo ""
	@echo "Variables:"
	@echo "  TARGET=prod       (default: prod)"

# Default dbt target (use --target prod for now)
TARGET ?= prod

.PHONY: up
up:
	$(DC) up -d

.PHONY: up-build
up-build:
	$(DC) up -d --build

.PHONY: down
down:
	$(DC) down

.PHONY: ps
ps:
	$(DC) ps

.PHONY: logs
logs:
	$(DC) logs -f --tail=200

.PHONY: logs-worker
logs-worker:
	$(DC) logs -f --tail=200 prefect_worker

.PHONY: shell
shell:
	$(APP_SHELL)

# -----------------------
# dbt basics
# -----------------------
.PHONY: dbt-debug
dbt-debug:
	$(DBT) "cd dbt && dbt debug --target $(TARGET)"

.PHONY: dbt-deps
dbt-deps:
	$(DBT) "cd dbt && dbt deps"

.PHONY: seed
seed:
	$(DBT) "cd dbt && dbt seed --target $(TARGET)"

.PHONY: seed-fr
seed-fr:
	$(DBT) "cd dbt && dbt seed --target $(TARGET) --full-refresh"

.PHONY: run
run:
	$(DBT) "cd dbt && dbt run --target $(TARGET)"

.PHONY: test
test:
	$(DBT) "cd dbt && dbt test --target $(TARGET)"

.PHONY: ls
ls:
	$(DBT) "cd dbt && dbt ls --target $(TARGET)"

.PHONY: docs
docs:
	$(DBT) "cd dbt && dbt docs generate --target $(TARGET)"

# -----------------------
# Layered runs
# -----------------------
.PHONY: run-canonical-core
run-canonical-core:
	$(DBT) "cd dbt && dbt run --target $(TARGET) --selector canonical_core"

.PHONY: run-canonical-silver
run-canonical-silver:
	$(DBT) "cd dbt && dbt run --target $(TARGET) --selector canonical_silver"

.PHONY: run-project-respira_gold
run-project-respira_gold:
	$(DBT) "cd dbt && dbt run --target $(TARGET) --selector project_respira_gold"

# -----------------------
# Build flows
# -----------------------
.PHONY: build
build: dbt-deps seed run test

# Full refresh for when schemas/logic change significantly
.PHONY: build-fr
build-fr: dbt-deps seed-fr
	$(DBT) "cd dbt && dbt run --target $(TARGET) --selector canonical_full_refresh --full-refresh"
	$(DBT) "cd dbt && dbt run --target $(TARGET) --selector project_respira_gold"
	$(DBT) "cd dbt && dbt test --target $(TARGET) --selector project_respira_gold_tests"

# A fast inner-loop option (no deps, no seed) for iteration
.PHONY: quick
quick:
	$(DBT) "cd dbt && dbt run --target $(TARGET) --selector canonical_core"
	$(DBT) "cd dbt && dbt run --target $(TARGET) --selector canonical_silver"

# -----------------------
# Prefect flows
# -----------------------
.PHONY: prefect-bootstrap
prefect-bootstrap:
	$(PREFECT_RUN) "python3 prefect/flows/warehouse_bootstrap.py"

.PHONY: run-canonical-incremental
run-canonical-incremental:
	$(PREFECT_RUN) "python3 prefect/flows/canonical_incremental.py"

.PHONY: run-canonical-full-refresh
run-canonical-full-refresh:
	$(PREFECT_RUN) "python3 prefect/flows/canonical_full_refresh.py"

.PHONY: run-project-inference
run-project-inference:
	$(WORKER_RUN) "python3 prefect/flows/project_inference.py"

.PHONY: run-project-pipeline
run-project-pipeline:
	$(WORKER_RUN) "python3 prefect/flows/project_pipeline.py"

.PHONY: smoke-test
smoke-test:
	poetry run pytest -q tests/test_artifacts.py tests/test_dbt_tasks_command.py tests/test_gates.py tests/test_inference_json.py tests/test_projects_config.py
