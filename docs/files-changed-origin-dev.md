# Files Changed in This Branch (vs origin/dev)

Comparison method: git diff --name-status origin/dev...HEAD

## Added

| File | Explanation |
|---|---|
| .dockerignore | Added Docker build context exclusions and keep/include rules to improve build performance and reliability. |
| .github/pull_request_template.md | Added a standardized PR template to improve review quality and release traceability. |
| .github/workflows/dev-image.yml | Added dev image publishing workflow (push to dev + manual dispatch) with GHCR push, caching, metadata tags, SBOM/provenance, and attestations. |
| .github/workflows/release-image.yml | Added release image workflow triggered by published releases with semver/main validation and GHCR publication. |
| dbt/models/canonical/staging/stg_mades_open_stations.sql | Added new canonical staging model for MADES open stations ingestion and normalization. |
| docs/respira-data-env-vars.md | Added centralized environment variable reference documentation for respira-data. |

## Modified

| File | Explanation |
|---|---|
| .env.example | Expanded and reorganized env template with required/optional grouping, defaults, and setup guidance. |
| Dockerfile | Updated app image build steps and dependency install flow to align with runtime/build improvements. |
| Dockerfile.worker | Updated worker image to improve dependency/build behavior and runtime setup. |
| dbt/seeds/stations_static.csv | Updated static station seed data to align with current canonical/project data expectations. |
| docker-compose.yml | Updated service configuration with health-aware dependencies and env-override-friendly settings. |

## Removed

| File | Explanation |
|---|---|
| None | No files were removed in this branch compared to origin/dev. |

## Summary

- Added: 6
- Modified: 5
- Removed: 0
- Total changed files: 11
