# Sprint 5 Observability & CI/CD Reporting

## Goal

Sprint 5 improves Snowflake CI/CD observability so every validation run leaves a
reviewable summary behind for analytics engineers and non-technical
stakeholders.

## What Changed

- `scripts/validate_snowflake_models.py` now writes two runtime reports under
  `artifacts/`:
  - `snowflake_validation_report.json`
  - `snowflake_validation_report.md`
- The validator still prints `[PASS]`, `[FAIL]`, and `[WARN]` logs to stdout,
  but it now also persists a structured run summary for later inspection.
- `.github/workflows/snowflake_deploy.yml` uploads those reports as a GitHub
  Actions artifact after the validation step.
- The Markdown report is also written into the GitHub Actions step summary when
  the workflow runs in GitHub.

## What The Reports Include

Each run summary includes:

- total tests run
- passed count
- failed count
- warning count
- failing test names
- warning test names
- key marts table row counts
- schema object counts
- per-test status, severity, and failing row totals

## JSON Report Purpose

The JSON report is intended for machine-readable downstream use. It can be
consumed by future tooling for:

- Slack alert formatting
- audit logging
- dashboarding
- trend analysis across deployments

## Markdown Report Purpose

The Markdown report is intended for humans reviewing CI/CD outcomes. It gives a
compact view of:

- overall warehouse validation status
- which tests failed
- which tests emitted warnings
- whether core marts tables have expected row counts

This reduces the need to read raw job logs line by line.

## Runtime Output Handling

Reports are written to the runtime-only `artifacts/` directory. That directory
is ignored by Git because it contains generated execution output, not source
code.

## CI/CD Review Flow

The intended review path is:

1. GitHub Actions deploys Snowflake SQL models.
2. The validation runner executes warehouse checks and SQL tests.
3. JSON and Markdown reports are written into `artifacts/`.
4. GitHub Actions uploads the reports as an artifact.
5. Reviewers inspect the step summary or download the artifact when they need
   more detail.

## Why This Matters

Sprint 4 made the pipeline fail fast on bad warehouse data. Sprint 5 makes the
result observable and reviewable.

That is the difference between a pipeline that is technically correct and one
that is operationally useful.
