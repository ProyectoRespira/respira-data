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
	@echo "  run-staging       dbt run --select staging+"
	@echo "  run-intermediate  dbt run --select intermediate+"
	@echo "  run-core          dbt run --select marts.core+"
	@echo "  run-facts         dbt run --select marts.facts+"
	@echo ""
	@echo "Build flows:"
	@echo "  build             deps + seed + run-all + test"
	@echo "  build-fr          deps + seed(full refresh) + run(full refresh) + test"
	@echo ""
	@echo "Prefect:"
	@echo "  prefect-bootstrap Run warehouse bootstrap SQL (01 optional + 02 ops)"
	@echo "  run-dbt-incremental Run dbt_incremental flow"
	@echo "  run-dbt-gold      Run dbt_gold flow"
	@echo "  run-dbt-full-refresh Run dbt_full_refresh flow (manual)"
	@echo "  run-inference     Run inference_per_station flow"
	@echo "  run-gold-then-inference Run coordinator flow"
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
.PHONY: run-staging
run-staging:
	$(DBT) "cd dbt && dbt run --target $(TARGET) --select staging+"

.PHONY: run-intermediate
run-intermediate:
	$(DBT) "cd dbt && dbt run --target $(TARGET) --select intermediate+"

.PHONY: run-core
run-core:
	$(DBT) "cd dbt && dbt run --target $(TARGET) --select marts.core+"

.PHONY: run-facts
run-facts:
	$(DBT) "cd dbt && dbt run --target $(TARGET) --select marts.facts+"

# -----------------------
# Build flows
# -----------------------
.PHONY: build
build: dbt-deps seed run test

# Full refresh for when schemas/logic change significantly
.PHONY: build-fr
build-fr: dbt-deps seed-fr
	$(DBT) "cd dbt && dbt run --target $(TARGET) --full-refresh"
	$(DBT) "cd dbt && dbt test --target $(TARGET)"

# A fast inner-loop option (no deps, no seed) for iteration
.PHONY: quick
quick:
	$(DBT) "cd dbt && dbt run --target $(TARGET) --select staging+ intermediate+"
	$(DBT) "cd dbt && dbt test --target $(TARGET) --select intermediate+ marts.facts.fct_measurements_silver"

# -----------------------
# Prefect flows
# -----------------------
.PHONY: prefect-bootstrap
prefect-bootstrap:
	$(PREFECT_RUN) "python3 prefect/flows/warehouse_bootstrap.py"

.PHONY: run-dbt-incremental
run-dbt-incremental:
	$(PREFECT_RUN) "python3 prefect/flows/dbt_incremental.py"

.PHONY: run-dbt-gold
run-dbt-gold:
	$(PREFECT_RUN) "python3 prefect/flows/dbt_gold.py"

.PHONY: run-dbt-full-refresh
run-dbt-full-refresh:
	$(PREFECT_RUN) "python3 prefect/flows/dbt_full_refresh.py"

.PHONY: run-inference
run-inference:
	$(WORKER_RUN) "python3 prefect/flows/inference_per_station.py"

.PHONY: run-gold-then-inference
run-gold-then-inference:
	$(WORKER_RUN) "python3 prefect/flows/gold_then_inference.py"

.PHONY: smoke-test
smoke-test:
	poetry run pytest -q tests/test_artifacts.py tests/test_gates.py tests/test_inference_json.py
