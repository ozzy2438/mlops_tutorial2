# Sprint 3 CI/CD Snowflake Deployment

## What CI/CD Means Here

CI/CD means using GitHub Actions to run a repeatable deployment process whenever
approved code reaches `main`. For this project, the pipeline deploys Snowflake
SQL files and then runs basic validation checks so the team can catch deployment
or modelling issues early.

## How GitHub Actions Deploys Snowflake SQL

The workflow in `.github/workflows/snowflake_deploy.yml` runs on pushes to
`main` and can also be started manually with `workflow_dispatch`.

The workflow:

1. Checks out the repository.
2. Sets up Python 3.11.
3. Installs `snowflake-connector-python`.
4. Runs `scripts/deploy_snowflake_sql.py`.
5. Runs `scripts/validate_snowflake_models.py`.

## Required GitHub Secrets

The workflow expects these GitHub Actions secrets:

- `SNOWFLAKE_ACCOUNT`
- `SNOWFLAKE_USER`
- `SNOWFLAKE_PASSWORD`
- `SNOWFLAKE_ROLE`
- `SNOWFLAKE_WAREHOUSE`
- `SNOWFLAKE_DATABASE`

These values are read as environment variables. They must not be committed to
GitHub.

## Deployment Order

Snowflake SQL is deployed in this medallion order:

1. `snowflake/setup/`
2. `snowflake/staging/`
3. `snowflake/int_layer/`
4. `snowflake/marts_layer/`

Files are executed alphabetically within each folder, which keeps deployment
deterministic.

## Team Collaboration

- `ozzy2410` creates the feature branch and pull request.
- GitHub Actions runs deployment checks after code reaches `main`, or manually
  when triggered by the team.
- `ozzy2438` reviews and merges the pull request.
- Deployment runs after merge to `main` and prints a validation summary.

## Known Limitations

- Password authentication is used for this learning sprint only.
- Future improvement should use Snowflake key-pair authentication or
  OIDC/workload identity federation.
- Azure SAS tokens should never be committed.
- SQL files containing placeholders such as `<AZURE_SAS_TOKEN_PLACEHOLDER>` are
  skipped by the deployment script and may require manual secret injection or a
  safer future automation path.
