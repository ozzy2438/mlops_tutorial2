"""Run Snowflake warehouse validation and data quality test checks."""

from __future__ import annotations

import os
import sys
from pathlib import Path

import snowflake.connector


ROOT = Path(__file__).resolve().parents[1]
TESTS_DIR = ROOT / "snowflake" / "tests"
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


def split_sql_statements(sql_text: str) -> list[str]:
    """Split simple SQL files on semicolons outside quoted strings."""
    statements: list[str] = []
    current: list[str] = []
    in_single_quote = False
    in_double_quote = False
    in_line_comment = False
    in_block_comment = False
    i = 0

    while i < len(sql_text):
        char = sql_text[i]
        next_char = sql_text[i + 1] if i + 1 < len(sql_text) else ""

        if in_line_comment:
            current.append(char)
            if char == "\n":
                in_line_comment = False
            i += 1
            continue

        if in_block_comment:
            current.append(char)
            if char == "*" and next_char == "/":
                current.append(next_char)
                in_block_comment = False
                i += 2
            else:
                i += 1
            continue

        if not in_single_quote and not in_double_quote:
            if char == "-" and next_char == "-":
                current.append(char)
                current.append(next_char)
                in_line_comment = True
                i += 2
                continue
            if char == "/" and next_char == "*":
                current.append(char)
                current.append(next_char)
                in_block_comment = True
                i += 2
                continue

        if char == "'" and not in_double_quote:
            current.append(char)
            if in_single_quote and next_char == "'":
                current.append(next_char)
                i += 2
                continue
            in_single_quote = not in_single_quote
            i += 1
            continue

        if char == '"' and not in_single_quote:
            in_double_quote = not in_double_quote
            current.append(char)
            i += 1
            continue

        if char == ";" and not in_single_quote and not in_double_quote:
            statement = "".join(current).strip()
            if statement:
                statements.append(statement)
            current = []
            i += 1
            continue

        current.append(char)
        i += 1

    tail = "".join(current).strip()
    if tail:
        statements.append(tail)

    return statements


def log_pass(message: str) -> None:
    print(f"[PASS] {message}")


def log_fail(message: str) -> None:
    print(f"[FAIL] {message}")


def validate_schema_object_counts(cursor, database: str) -> list[str]:
    failures: list[str] = []

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
        if object_count == 0:
            message = f"{schema} object count check failed: no deployed objects found"
            log_fail(message)
            failures.append(message)
        else:
            log_pass(f"{schema} object count check ({object_count} tables/views)")

    return failures


def validate_key_mart_row_counts(cursor, database: str) -> list[str]:
    failures: list[str] = []

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
            message = f"MARTS_LAYER.{table_name} existence check failed: object is missing"
            log_fail(message)
            failures.append(message)
            continue

        row_count = fetch_one(
            cursor,
            f"SELECT COUNT(*) FROM {database}.MARTS_LAYER.{table_name}",
        )
        if row_count == 0:
            message = f"MARTS_LAYER.{table_name} row count check failed: zero rows found"
            log_fail(message)
            failures.append(message)
        else:
            log_pass(f"MARTS_LAYER.{table_name} row count check ({row_count} rows)")

    return failures


def execute_quality_tests(cursor) -> list[str]:
    failures: list[str] = []
    test_files = sorted(TESTS_DIR.glob("*.sql"))

    if not test_files:
        message = f"No SQL tests found under {TESTS_DIR}"
        log_fail(message)
        return [message]

    print(f"Running {len(test_files)} Snowflake data quality SQL tests.")

    for path in test_files:
        statements = split_sql_statements(path.read_text(encoding="utf-8"))
        if not statements:
            message = f"{path.name} contains no executable SQL statements"
            log_fail(message)
            failures.append(message)
            continue

        rows = []
        columns = []
        try:
            for statement in statements:
                cursor.execute(statement)
                if cursor.description:
                    rows = cursor.fetchall()
                    columns = [description[0].upper() for description in cursor.description]
        except Exception as error:
            message = f"{path.name} execution error: {error}"
            log_fail(message)
            failures.append(message)
            continue

        if not rows:
            message = f"{path.name} returned no validation result row"
            log_fail(message)
            failures.append(message)
            continue

        first_row = rows[0]
        result = dict(zip(columns, first_row))
        test_name = result.get("TEST_NAME", path.stem)
        failure_count = int(result.get("FAILURE_COUNT", 0))
        failure_message = result.get(
            "FAILURE_MESSAGE",
            f"{test_name} returned {failure_count} failures",
        )

        if failure_count == 0:
            log_pass(test_name)
        else:
            log_fail(f"{test_name} ({failure_count} failing rows/groups) - {failure_message}")
            failures.append(f"{test_name}: {failure_message} [{failure_count}]")

    return failures


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

            failures.extend(validate_schema_object_counts(cursor, database))
            failures.extend(validate_key_mart_row_counts(cursor, database))
            failures.extend(execute_quality_tests(cursor))

        if failures:
            print("Snowflake validation failed.")
            print(f"Failure count: {len(failures)}")
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
