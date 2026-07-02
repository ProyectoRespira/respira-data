## 📌 Title
**[RES-114/RES-116/RES-110] Docker runtime hardening, env docs/template expansion, and GHCR image publishing workflows**

---

## 🔍 Description
**[Essential]**

- Problem this solves:
  - Docker/runtime reliability gaps (service health dependency and worker image build/runtime separation).
  - Missing centralized env-variable guidance and incomplete `.env.example` coverage.
  - No standardized dev/release GHCR publishing flow with traceable tags and supply-chain metadata.
- New behavior:
  - Prefect server has healthcheck and dependent services use `condition: service_healthy`.
  - App/worker compose env values are parameterized for easier `.env` overrides.
  - `.env.example` is expanded with required/optional grouping and operational comments.
  - New env documentation page with variable catalog and usage context.
  - New GitHub workflows publish app/worker images on `dev` push/manual and on release publish.
  - Docker build context now includes `README.md` to avoid Poetry packaging failures.
- Previous behavior:
  - Prefect dependency readiness was weaker and app values were partially hardcoded.
  - Env onboarding was fragmented and missing several runtime variables.
  - Image publishing strategy was not aligned to separate dev/release lifecycle with stronger metadata.

---

## 🎯 Motivation / Rationale

This PR consolidates deployment and runtime reliability improvements while improving operator/developer onboarding:

- Aligns runtime startup order with health-based orchestration.
- Reduces environment misconfiguration risk with explicit docs and annotated template.
- Enables safer, traceable, and repeatable image publication for dev and release channels.

---

## 🛠️ Changes Made
**[Essential]**

- [x] New feature
- [x] Bug fix
- [x] Refactor
- [x] Documentation
- [ ] Dependency updates

### Details:

- Docker and compose:
  - Updated [Dockerfile](../Dockerfile) and [Dockerfile.worker](../Dockerfile.worker).
  - Updated [docker-compose.yml](../docker-compose.yml) with env-override-friendly values and health-aware dependency behavior.
  - Added [ .dockerignore ](../.dockerignore) (includes `README.md` in build context to support Poetry package metadata handling).
- Environment docs and template:
  - Updated [ .env.example ](../.env.example) with grouped sections, defaults, and setup comments.
  - Added [docs/respira-data-env-vars.md](./respira-data-env-vars.md) as centralized env reference for this repository.
- GHCR publishing workflows:
  - Added [ .github/workflows/dev-image.yml ](../.github/workflows/dev-image.yml) for `dev` push + manual publish.
  - Added [ .github/workflows/release-image.yml ](../.github/workflows/release-image.yml) for release-published builds from `main` constraints.
  - Includes docker metadata tags, GHA layer caching, SBOM/provenance, and image attestations.
- Data model adjustments present in branch diff:
  - Added [dbt/models/canonical/staging/stg_mades_open_stations.sql](../dbt/models/canonical/staging/stg_mades_open_stations.sql).
  - Updated [dbt/seeds/stations_static.csv](../dbt/seeds/stations_static.csv).

---

## 🔗 Related Tickets
**[Good to have]**

Closes:
- RES-114 (compose healthcheck/startup reliability and docker optimizations)

Related to:
- RES-116 (env reference and `.env.example` improvements in respira-data scope)
- RES-110 (GHCR dev/release image publishing strategy for respira-data images)
- RES-109 (CI hardening direction; this PR focuses image workflows rather than full test/dbt validation pipeline)

---

## 🧪 Testing
**[Optional but recommended]**

### Manual

- Test Case A: Render compose config after compose edits.
  - Command: `docker compose config >/tmp/compose.rendered.yml`
  - Result: succeeded.
- Test Case B: Validate workflow YAML/schema in editor diagnostics.
  - Result: no errors in new workflow files after fixes.

### Automated

- [ ] Unit tests added
- [ ] Integration tests updated
- [ ] CI/CD passes without errors

---

## 📸 Evidence (if applicable)
**[Essential for frontend/UI changes]**

N/A (backend/data/platform changes only).

---

## ⚠️ Impact / Risks
**[Optional]**

- [ ] Breaking changes
- [ ] Requires data migration
- [x] Infrastructure or configuration changes

### Impact description:

- New workflow behavior affects image publication paths and tags.
- Compose/runtime behavior now relies on health conditions and env-driven overrides.
- Operators should review release/tag expectations before first release publish.

---

## 📚 Pre-Review Checklist

- [ ] Code formatted (lint/formatter applied)
- [x] Naming conventions followed
- [x] Documentation updated (README, ADRs, relevant comments)
- [x] Manual flow testing completed
- [ ] No debug logs or unnecessary prints remaining
- [ ] No open TODOs without a ticket
- [x] Security reviewed (validations, sanitization, permissions)

---

## 👥 Suggested Reviewers

- @platform
- @data-engineering
