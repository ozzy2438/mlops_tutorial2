# mlops_tutorial2

## Sprint 3 - CI/CD Deployment

Sprint 3 adds GitHub Actions automation for deploying the Snowflake SQL models.

Deployment flow:

```text
GitHub push/merge -> GitHub Actions -> Snowflake SQL deployment -> validation summary
```

The deployment runs SQL in medallion order:

```text
setup -> staging -> int_layer -> marts_layer
```

Snowflake credentials are supplied through GitHub Actions secrets and must not
be committed to the repository.
