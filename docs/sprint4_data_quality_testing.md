# Sprint 4 Data Quality & Automated Warehouse Validation

## Why Data Quality Matters

Analytics engineering is only useful when downstream users can trust the
warehouse. Data quality validation protects that trust by checking keys,
relationships, and business rules before analysts or machine learning workflows
read the marts layer.

## Medallion Validation Strategy

Sprint 4 focuses on warehouse-level controls around the medallion architecture:

- `setup` deploys safe database and schema objects.
- `staging` standardizes raw inputs.
- `int_layer` enriches business logic.
- `marts_layer` publishes analytics-ready facts and dimensions.
- `tests` verifies whether marts outputs still satisfy warehouse quality rules.

The tests are stored in `snowflake/tests/` and are executed after deployment.

## Warehouse Testing Philosophy

The project uses fail-fast testing. If a validation query finds any failing
rows, the deployment is treated as unsuccessful. This is intentional.

The philosophy is:

- validate the warehouse where the data lives,
- test deterministic business rules,
- make failures visible in CI/CD,
- prevent silent trust erosion in analytics tables.

## What Validations Were Added

Sprint 4 adds SQL tests for:

- NOT NULL key checks on customer, order, product, and store identifiers
- uniqueness checks for dimension and fact business keys
- referential integrity checks between facts and dimensions
- business rule checks for negative sales, zero-quantity orders, future order
  dates, and duplicate fact grain rows

Each SQL file returns one aggregated result row containing:

- `test_name`
- `failure_count`
- `failure_message`

## CI/CD Validation Flow

GitHub Actions now runs in this order:

1. Deploy Snowflake SQL models
2. Run post-deploy warehouse object and row-count checks
3. Execute every SQL test in `snowflake/tests/`
4. Emit readable `[PASS]` / `[FAIL]` logs
5. Fail the pipeline immediately when any validation fails

## Example Failures

Examples of failures the pipeline will now catch:

- `[FAIL] DIM_CUSTOMERS unique customer_id check`
- `[FAIL] FCT_ORDERS negative sales amount check`
- `[FAIL] FCT_ORDER_ITEMS FK to DIM_PRODUCTS check`
- `[FAIL] FCT_ORDERS future date check`

The validation runner can also emit `[WARN]` when a documented source-data
exception should stay visible without failing the deployment.

## Why Pipelines Should Fail Fast

Fail-fast pipelines keep bad data from becoming accepted data. If a deploy
creates invalid marts tables, the pipeline must stop so the team investigates
before users query the warehouse.

This is a production-grade analytics engineering control, not optional hygiene.

## Zero-Item Orders Decision

The first Sprint 4 validation run failed on the zero-quantity order check. The
team investigated the warehouse before changing the rule.

Observed counts:

- `STAGING.STG_ORDERS`: `2,088,006`
- distinct `order_id` in `STAGING.STG_ORDER_ITEMS`: `2,073,420`
- `MARTS_LAYER.FCT_ORDERS` rows where `total_items = 0`: `14,586`

Root-cause findings:

- The `14,586` zero-item fact rows exactly match the count of `STG_ORDERS`
  records with no matching `STG_ORDER_ITEMS` rows.
- There is no reverse key mismatch from `STG_ORDER_ITEMS` into `STG_ORDERS`.
- The affected orders have `subtotal_amount = 0`, `tax_amount = 0`, and
  `order_total_amount = 0`.

Decision:

- These rows are treated as valid historical or incomplete source order headers,
  not as a join bug.
- The warehouse keeps them visible in `FCT_ORDERS` because removing them would
  introduce new business logic that is not present in the source system.
- The zero-item test is therefore split into:
  - a hard failure for null or negative item counts, or zero-item orders with
    non-zero financial amounts
  - a warning for zero-item, zero-value order headers

This keeps the pipeline strict on analytical inconsistency while documenting a
real source-data exception.

## Reviewer Notes

Reviewers should verify:

- the SQL tests cover key warehouse trust risks,
- the Python runner fails the process on any failed test,
- logs are readable enough to diagnose failures quickly,
- new tests can be added by dropping another `.sql` file into
  `snowflake/tests/`,
- warehouse quality standards continue to prioritize key integrity, referential
  integrity, and business-rule correctness over permissive deployment.
