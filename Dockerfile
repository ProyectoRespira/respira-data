# Stage 1: Builder
# Resolves the locked runtime dependencies; nothing from this stage except the
# installed packages reaches the final image.
FROM python:3.12-slim AS builder

ENV PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /build

# Poetry lives in its own venv so it is not copied into the runtime image
# together with /usr/local.
RUN python -m venv /opt/poetry \
    && /opt/poetry/bin/pip install "poetry==2.2.1" "poetry-plugin-export==1.9.0"

COPY pyproject.toml poetry.lock /build/

# Only the main group: the app container runs dbt and the canonical/bootstrap
# flows. Inference (darts, lightgbm, xgboost) runs in the worker image and dev
# tools are not needed at runtime. --no-compile skips precompiled .pyc files
# (Python compiles on import), matching the previous Poetry-based image.
RUN /opt/poetry/bin/poetry export --only main --format requirements.txt --output requirements.txt \
    && pip install --no-compile -r requirements.txt


# Stage 2: Runtime
FROM python:3.12-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    postgresql-client \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /usr/local/lib/python3.12/site-packages /usr/local/lib/python3.12/site-packages
COPY --from=builder /usr/local/bin /usr/local/bin

# Replaces the editable root install: makes the `pipelines` and `inference`
# packages importable from the copied source tree.
ENV PYTHONPATH=/app:/app/src

COPY . /app
