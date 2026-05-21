FROM python:3.12-slim

WORKDIR /app

# System dependencies required by dbt and general tooling
RUN apt-get update \
    && apt-get install -y --no-install-recommends git postgresql-client \
    && rm -rf /var/lib/apt/lists/*

RUN pip install -U pip && pip install poetry
RUN poetry config virtualenvs.create false

COPY pyproject.toml poetry.lock* /app/
RUN poetry install --no-interaction --no-ansi --no-root

COPY . /app
RUN poetry install --no-interaction --no-ansi
