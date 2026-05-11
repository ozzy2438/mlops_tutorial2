"""Run lightweight validation checks after Snowflake SQL deployment."""

from __future__ import annotations

import os
import sys

import snowflake.connector


REQUIRED_ENV_VARS = [
    "SNOWFLAKE_ACCOUNT",
    "SNOWFLAKE_USER",
    "SNOWFLAKE_PASSWORD",
    "SNOWFLAKE_ROLE",
    "SNOWFLAKE_WAREHOUSE",
    "SNOWFLAKE_DATABASE",
]

REQUIRED_SCHEMAS = ["STAGING", "INT_LAYER", "MARTS_LAYER"]
KEY_MARTS_TABLES = [
    "DIM_CUSTOMERS",
    "DIM_PRODUCTS",
    "DIM_STORES",
    "FCT_ORDERS",
    "FCT_ORDER_ITEMS",
    "RETAIL_SALES_WIDE",
]


def require_environment() -> dict[str, str]:
    missing = [name for name in REQUIRED_ENV_VARS if not os.getenv(name)]
    if missing:
        raise RuntimeError(
            "Missing required Snowflake environment variables: "
            + ", ".join(missing)
        )
    return {name: os.environ[name] for name in REQUIRED_ENV_VARS}


def connect_to_snowflake(env: dict[str, str]):
    return snowflake.connector.connect(
        account=env["SNOWFLAKE_ACCOUNT"],
        user=env["SNOWFLAKE_USER"],
        password=env["SNOWFLAKE_PASSWORD"],
        role=env["SNOWFLAKE_ROLE"],
        warehouse=env["SNOWFLAKE_WAREHOUSE"],
        database=env["SNOWFLAKE_DATABASE"],
    )


def fetch_one(cursor, query: str, params: tuple | None = None):
    cursor.execute(query, params or ())
    return cursor.fetchone()[0]


def main() -> int:
    env = require_environment()
    database = env["SNOWFLAKE_DATABASE"].upper()
    failures: list[str] = []

    print("Starting Snowflake post-deploy validation.")

    conn = connect_to_snowflake(env)
    try:
        with conn.cursor() as cursor:
            cursor.execute(f"USE ROLE {env['SNOWFLAKE_ROLE']}")
            cursor.execute(f"USE WAREHOUSE {env['SNOWFLAKE_WAREHOUSE']}")
            cursor.execute(f"USE DATABASE {env['SNOWFLAKE_DATABASE']}")

            for schema in REQUIRED_SCHEMAS:
                object_count = fetch_one(
                    cursor,
                    """
                    SELECT COUNT(*)
                    FROM INFORMATION_SCHEMA.TABLES
                    WHERE TABLE_CATALOG = %s
                      AND TABLE_SCHEMA = %s
                    """,
                    (database, schema),
                )
                print(f"{schema}: {object_count} tables/views found")
                if object_count == 0:
                    failures.append(f"{schema} has no deployed objects")

            for table_name in KEY_MARTS_TABLES:
                exists = fetch_one(
                    cursor,
                    """
                    SELECT COUNT(*)
                    FROM INFORMATION_SCHEMA.TABLES
                    WHERE TABLE_CATALOG = %s
                      AND TABLE_SCHEMA = 'MARTS_LAYER'
                      AND TABLE_NAME = %s
                    """,
                    (database, table_name),
                )
                if not exists:
                    print(f"WARNING: MARTS_LAYER.{table_name} does not exist")
                    failures.append(f"MARTS_LAYER.{table_name} is missing")
                    continue

                row_count = fetch_one(
                    cursor,
                    f"SELECT COUNT(*) FROM {database}.MARTS_LAYER.{table_name}",
                )
                print(f"MARTS_LAYER.{table_name}: {row_count} rows")
                if row_count == 0:
                    failures.append(f"MARTS_LAYER.{table_name} has zero rows")

        if failures:
            print("Snowflake validation failed:")
            for failure in failures:
                print(f"- {failure}")
            return 1

        print("Snowflake validation passed.")
        return 0
    finally:
        conn.close()
        print("Snowflake connection closed.")


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as error:
        print(f"Validation failed: {error}", file=sys.stderr)
        raise
