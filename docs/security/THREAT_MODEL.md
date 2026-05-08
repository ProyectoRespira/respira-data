# Public Threat Model Overview

## Purpose

This document is a public-facing summary of the main security risks considered for the Respira application. It is intentionally general and avoids implementation details that could be directly useful for targeted attacks.

The goal is to explain, at a high level:

- what kinds of threats matter for a self-hosted environmental data platform
- what types of protections are important
- which areas deserve continued review as the project evolves

## System Overview

Respira is a self-hosted data platform for collecting, transforming, and analyzing environmental data (primarily air quality). Each operator maintains and deploys their own instance by building containers from this codebase. In this repository, the main components are data ingestion into Postgres, dbt transformations, Prefect orchestration, and inference workflows.

Key characteristics:
- **Self-hosted:** Each operator runs and maintains their own independent instance
- **Code-only distribution:** No pre-built artifacts; users build containers from source
- **Shared-data capability:** Some deployments may handle data for multiple organizations or projects
- **Data-dependent:** Trustworthiness relies on accurate environmental measurements and analysis

Because instances are operator-maintained, security concerns shift from centralized platform availability toward deployment-time decisions, data handling practices, and upstream code integrity.

## Main Threat Areas

### 1. Data Integrity

Users rely on Respira to display trustworthy environmental information. If data is altered, corrupted, or presented incorrectly, decisions based on that information can be harmed.

**Why it matters:**  
Public health decisions (air quality warnings, operational planning) depend on accurate data. Compromised data undermines the value and trust in the platform.

**High-level mitigations:**

- validate all external data sources before ingestion
- implement consistency checks and data anomaly detection
- restrict who and what can modify stored data
- audit data-handling code when architecture changes
- implement cryptographic verification where feasible (e.g., checksums for sensor feeds)

### 2. Code Integrity & Supply Chain

As a self-hosted platform distributed as source code, users depend on this repository being the authoritative source of the application. Compromised code can affect every instance built from it.

**Why it matters:**  
Unlike SaaS platforms, malicious code in this repository directly impacts all downstream deployments.

**High-level mitigations:**

- protect repository integrity (branch protection, required reviews, strong authentication for maintainers)
- sign releases to allow users to verify source authenticity
- clearly document the expected build process and deployment steps
- monitor for unusual changes to build configuration or dependencies
- maintain a security incident response plan for code-related issues

### 3. Deployment & Configuration Security

Because each user builds and deploys their own instance, they are responsible for secure deployment practices. However, the codebase should provide secure defaults and clear guidance.

**Why it matters:**  
Weak deployment practices (hardcoded secrets, insecure network exposure, unpatched containers) can compromise individual instances.

**High-level mitigations:**

- provide secure defaults in configuration examples (no credentials in code)
- document best practices for containerization and networking
- make it easy to rotate secrets and update dependencies
- clearly mark sensitive configuration fields
- provide automated security checks where practical (dependency scanning, linting)

### 4. Access Control & Authentication

Operational interfaces and sensitive actions should be protected from unauthorized access. This is especially important for GitHub maintainers, deployment operators, database access, and orchestration surfaces such as Prefect.

**Why it matters:**  
Unauthorized access can modify pipeline code, alter warehouse data, expose secrets, or interfere with scheduled operations.

**High-level mitigations:**

- require strong authentication for maintainers and deployment operators
- enforce least-privilege access for database users, CI/CD, and orchestration services
- log and audit sensitive operations
- support multi-factor authentication where practical for maintainer and operator accounts
- review who can access repositories, containers, secrets, and Prefect work pools

### 5. Information Exposure

The platform may unintentionally reveal sensitive operational details through logs, error messages, deployment configuration, orchestration surfaces, or documentation.

**Why it matters:**  
Unnecessary disclosure makes exploitation easier and can expose confidential configuration or internal state.

**High-level mitigations:**

- minimize sensitive data in logs and error messages
- avoid exposing internal system details through Prefect, container, or deployment surfaces
- review what information is accessible without authentication or with default local configuration
- sanitize user input before display (prevent injection attacks)
- periodically audit public-facing endpoints and documentation

### 6. Dependency Vulnerabilities

The application depends on third-party libraries and frameworks. Vulnerabilities in those dependencies can be inherited by Respira.

**Why it matters:**  
Dependencies are supply chain risks that can be exploited through the application if not kept current.

**High-level mitigations:**

- keep dependencies up to date and monitor for disclosed vulnerabilities
- implement automated security scanning of dependency trees
- document a clear process for patching vulnerable dependencies
- use pinned versions to ensure reproducible builds
- audit critical or security-sensitive dependencies

### 7. Auditability & Incident Response

Security work is harder when important actions cannot be traced after the fact. A clear incident response process enables faster detection and recovery.

**Why it matters:**  
Good visibility reduces response time and improves recovery. In self-hosted scenarios, maintainers need a clear process to communicate patches to users.

**High-level mitigations:**

- implement comprehensive security and operational logging
- define a clear vulnerability disclosure and incident response process
- maintain documentation for operators on monitoring and alerting
- plan for coordinated disclosure with affected downstream users
- regularly review and test incident response procedures

## Security Considerations for Self-Hosted Deployments

Because Respira is self-hosted, some traditional security concerns shift:

| Aspect | Centralized SaaS | Self-Hosted Respira |
|--------|------------------|-------------------|
| **Availability** | Platform responsibility | Operator responsibility |
| **Code updates** | Instant platform-wide | User-initiated via source update |
| **Patch timing** | Synchronized across users | Staggered; may be delayed by operators |
| **Secrets management** | Centralized | Distributed; each operator manages their own |
| **Audit logs** | Centralized review | Operator's responsibility to review |
| **Incident scope** | Can affect all users equally | Each instance is independent |

This means:
- **Patching is distributed:** Each operator is responsible for pulling updates. Security advisories should be clear and actionable.
- **Communication is critical:** When a vulnerability is found, the maintainers must publish clear guidance on which versions are affected and what action is needed.
- **Operators are security partners:** Each operator's deployment practices directly affect their own instance. Documentation should emphasize secure defaults.

## Current Security Themes

As a code repository providing a self-hosted application, ongoing security focus should include:

- monitoring and patching upstream dependency vulnerabilities
- protecting repository integrity and maintainer accounts
- providing operators with secure deployment guidance
- maintaining clear communication during security incidents
- supporting operators in detecting and responding to application-level threats
- improving visibility into data handling and storage practices

## Intended Use Of This Document

This public summary supports transparency about security thinking without disclosing implementation details that would make targeted attacks easier. It should be read together with normal operational security practices and revisited when the system or deployment model changes substantially.

For the incident response process and how maintainers handle reported vulnerabilities, see [INCIDENT_RESPONSE.md](INCIDENT_RESPONSE.md).
