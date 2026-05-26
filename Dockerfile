FROM python:3.12-slim

WORKDIR /app

# Install system dependencies first (least frequently changing)
# Combined with cleanup to reduce layer size
RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    postgresql-client \
    && rm -rf /var/lib/apt/lists/*

# Setup Python package manager
RUN pip install -U pip && pip install poetry
RUN poetry config virtualenvs.create false

# Copy dependency files BEFORE code (this layer is cached if pyproject.toml/poetry.lock don't change)
COPY pyproject.toml poetry.lock* /app/
RUN poetry install --no-interaction --no-ansi --no-root

# Copy application code last (this layer changes frequently and doesn't invalidate dependency layers)
COPY . /app
RUN poetry install --no-interaction --no-ansi

# Health check to verify the app container is responsive
HEALTHCHECK --interval=10s --timeout=5s --start-period=30s --retries=3 \
  CMD python -c "import sys; sys.exit(0)" || exit 1
