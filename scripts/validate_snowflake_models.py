"""Run Snowflake warehouse validation checks and publish summary reports."""

from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import snowflake.connector


ROOT = Path(__file__).resolve().parents[1]
TESTS_DIR = ROOT / "snowflake" / "tests"
ARTIFACTS_DIR = ROOT / "artifacts"
JSON_REPORT_PATH = ARTIFACTS_DIR / "snowflake_validation_report.json"
MARKDOWN_REPORT_PATH = ARTIFACTS_DIR / "snowflake_validation_report.md"
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


def log_warn(message: str) -> None:
    print(f"[WARN] {message}")


def initialize_report() -> dict:
    return {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "status": "running",
        "database": None,
        "summary": {
            "total_tests_run": 0,
            "passed_count": 0,
            "failed_count": 0,
            "warning_count": 0,
        },
        "failing_test_names": [],
        "warning_test_names": [],
        "schema_object_counts": [],
        "key_table_row_counts": [],
        "tests": [],
        "failures": [],
        "warnings": [],
        "fatal_error": None,
    }


def validate_schema_object_counts(cursor, database: str) -> tuple[list[str], list[dict]]:
    failures: list[str] = []
    results: list[dict] = []

    for schema in REQUIRED_SCHEMAS:
        object_count = int(
            fetch_one(
                cursor,
                """
                SELECT COUNT(*)
                FROM INFORMATION_SCHEMA.TABLES
                WHERE TABLE_CATALOG = %s
                  AND TABLE_SCHEMA = %s
                """,
                (database, schema),
            )
        )
        if object_count == 0:
            message = f"{schema} object count check failed: no deployed objects found"
            log_fail(message)
            failures.append(message)
            status = "FAIL"
        else:
            message = f"{schema} object count check ({object_count} tables/views)"
            log_pass(message)
            status = "PASS"

        results.append(
            {
                "schema": schema,
                "object_count": object_count,
                "status": status,
                "message": message,
            }
        )

    return failures, results


def validate_key_mart_row_counts(cursor, database: str) -> tuple[list[str], list[dict]]:
    failures: list[str] = []
    results: list[dict] = []

    for table_name in KEY_MARTS_TABLES:
        exists = int(
            fetch_one(
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
        )
        if not exists:
            message = (
                f"MARTS_LAYER.{table_name} existence check failed: object is missing"
            )
            log_fail(message)
            failures.append(message)
            results.append(
                {
                    "table_name": table_name,
                    "exists": False,
                    "row_count": None,
                    "status": "FAIL",
                    "message": message,
                }
            )
            continue

        row_count = int(
            fetch_one(
                cursor,
                f"SELECT COUNT(*) FROM {database}.MARTS_LAYER.{table_name}",
            )
        )
        if row_count == 0:
            message = (
                f"MARTS_LAYER.{table_name} row count check failed: zero rows found"
            )
            log_fail(message)
            failures.append(message)
            status = "FAIL"
        else:
            message = f"MARTS_LAYER.{table_name} row count check ({row_count} rows)"
            log_pass(message)
            status = "PASS"

        results.append(
            {
                "table_name": table_name,
                "exists": True,
                "row_count": row_count,
                "status": status,
                "message": message,
            }
        )

    return failures, results


def execute_quality_tests(cursor) -> tuple[list[str], list[str], list[dict]]:
    failures: list[str] = []
    warnings: list[str] = []
    results: list[dict] = []
    test_files = sorted(TESTS_DIR.glob("*.sql"))

    if not test_files:
        message = f"No SQL tests found under {TESTS_DIR}"
        log_fail(message)
        failures.append(message)
        return failures, warnings, results

    print(f"Running {len(test_files)} Snowflake data quality SQL tests.")

    for path in test_files:
        statements = split_sql_statements(path.read_text(encoding="utf-8"))
        if not statements:
            message = f"{path.name} contains no executable SQL statements"
            log_fail(message)
            failures.append(message)
            results.append(
                {
                    "file_name": path.name,
                    "test_name": path.stem,
                    "severity": "FAIL",
                    "status": "FAIL",
                    "failure_count": 1,
                    "failure_message": message,
                }
            )
            continue

        rows = []
        columns = []
        try:
            for statement in statements:
                cursor.execute(statement)
                if cursor.description:
                    rows = cursor.fetchall()
                    columns = [
                        description[0].upper() for description in cursor.description
                    ]
        except Exception as error:
            message = f"{path.name} execution error: {error}"
            log_fail(message)
            failures.append(message)
            results.append(
                {
                    "file_name": path.name,
                    "test_name": path.stem,
                    "severity": "FAIL",
                    "status": "FAIL",
                    "failure_count": 1,
                    "failure_message": message,
                }
            )
            continue

        if not rows:
            message = f"{path.name} returned no validation result row"
            log_fail(message)
            failures.append(message)
            results.append(
                {
                    "file_name": path.name,
                    "test_name": path.stem,
                    "severity": "FAIL",
                    "status": "FAIL",
                    "failure_count": 1,
                    "failure_message": message,
                }
            )
            continue

        for row in rows:
            result = dict(zip(columns, row))
            test_name = str(result.get("TEST_NAME", path.stem))
            failure_count = int(result.get("FAILURE_COUNT", 0))
            failure_message = str(
                result.get(
                    "FAILURE_MESSAGE",
                    f"{test_name} returned {failure_count} failures",
                )
            )
            severity = str(result.get("SEVERITY", "FAIL")).upper()

            if severity not in {"FAIL", "WARN"}:
                severity = "FAIL"
                failure_count = max(failure_count, 1)
                failure_message = (
                    f"Unsupported severity returned by {path.name} for {test_name}"
                )

            if failure_count == 0:
                status = "PASS"
                log_pass(test_name)
            elif severity == "WARN":
                status = "WARN"
                log_warn(
                    f"{test_name} ({failure_count} observed rows/groups) - "
                    f"{failure_message}"
                )
                warnings.append(f"{test_name}: {failure_message} [{failure_count}]")
            else:
                status = "FAIL"
                log_fail(
                    f"{test_name} ({failure_count} failing rows/groups) - "
                    f"{failure_message}"
                )
                failures.append(f"{test_name}: {failure_message} [{failure_count}]")

            results.append(
                {
                    "file_name": path.name,
                    "test_name": test_name,
                    "severity": severity,
                    "status": status,
                    "failure_count": failure_count,
                    "failure_message": failure_message,
                }
            )

    if warnings:
        print(f"Warning count: {len(warnings)}")
        for warning in warnings:
            print(f"- {warning}")

    return failures, warnings, results


def finalize_report(
    report: dict,
    database: str | None,
    schema_results: list[dict],
    table_results: list[dict],
    test_results: list[dict],
    failures: list[str],
    warnings: list[str],
    fatal_error: str | None,
) -> dict:
    passed_count = sum(1 for result in test_results if result["status"] == "PASS")
    failed_count = sum(1 for result in test_results if result["status"] == "FAIL")
    warning_count = sum(1 for result in test_results if result["status"] == "WARN")

    report["database"] = database
    report["schema_object_counts"] = schema_results
    report["key_table_row_counts"] = table_results
    report["tests"] = test_results
    report["failures"] = failures
    report["warnings"] = warnings
    report["fatal_error"] = fatal_error
    report["summary"] = {
        "total_tests_run": len(test_results),
        "passed_count": passed_count,
        "failed_count": failed_count,
        "warning_count": warning_count,
    }
    report["failing_test_names"] = [
        result["test_name"] for result in test_results if result["status"] == "FAIL"
    ]
    report["warning_test_names"] = [
        result["test_name"] for result in test_results if result["status"] == "WARN"
    ]

    if fatal_error:
        report["status"] = "error"
    elif failures:
        report["status"] = "failed"
    else:
        report["status"] = "passed"

    return report


def render_markdown_report(report: dict) -> str:
    summary = report["summary"]
    failing_names = report["failing_test_names"] or ["None"]
    warning_names = report["warning_test_names"] or ["None"]

    lines = [
        "# Snowflake Validation Report",
        "",
        f"- Generated at (UTC): `{report['generated_at_utc']}`",
        f"- Database: `{report['database'] or 'unknown'}`",
        f"- Overall status: `{report['status'].upper()}`",
        "",
        "## Summary",
        "",
        "| Metric | Value |",
        "| --- | ---: |",
        f"| Total tests run | {summary['total_tests_run']} |",
        f"| Passed count | {summary['passed_count']} |",
        f"| Failed count | {summary['failed_count']} |",
        f"| Warning count | {summary['warning_count']} |",
        "",
        "## Failing Test Names",
        "",
    ]
    lines.extend(f"- {name}" for name in failing_names)
    lines.extend(["", "## Warning Test Names", ""])
    lines.extend(f"- {name}" for name in warning_names)

    lines.extend(
        [
            "",
            "## Key Table Row Counts",
            "",
            "| Table | Status | Row Count | Message |",
            "| --- | --- | ---: | --- |",
        ]
    )
    for result in report["key_table_row_counts"]:
        row_count = result["row_count"] if result["row_count"] is not None else "n/a"
        lines.append(
            f"| `{result['table_name']}` | {result['status']} | {row_count} | "
            f"{result['message']} |"
        )

    lines.extend(
        [
            "",
            "## Schema Object Counts",
            "",
            "| Schema | Status | Object Count | Message |",
            "| --- | --- | ---: | --- |",
        ]
    )
    for result in report["schema_object_counts"]:
        lines.append(
            f"| `{result['schema']}` | {result['status']} | "
            f"{result['object_count']} | {result['message']} |"
        )

    lines.extend(
        [
            "",
            "## SQL Test Results",
            "",
            "| Test Name | Status | Rule Severity | Failing Rows | Source File |",
            "| --- | --- | --- | ---: | --- |",
        ]
    )
    for result in report["tests"]:
        lines.append(
            f"| `{result['test_name']}` | {result['status']} | {result['severity']} | "
            f"{result['failure_count']} | `{result['file_name']}` |"
        )

    if report["failures"]:
        lines.extend(["", "## Failure Details", ""])
        lines.extend(f"- {failure}" for failure in report["failures"])

    if report["warnings"]:
        lines.extend(["", "## Warning Details", ""])
        lines.extend(f"- {warning}" for warning in report["warnings"])

    if report["fatal_error"]:
        lines.extend(["", "## Fatal Error", "", f"- {report['fatal_error']}"])

    lines.append("")
    return "\n".join(lines)


def write_reports(report: dict) -> None:
    ARTIFACTS_DIR.mkdir(parents=True, exist_ok=True)
    JSON_REPORT_PATH.write_text(json.dumps(report, indent=2), encoding="utf-8")
    markdown_report = render_markdown_report(report)
    MARKDOWN_REPORT_PATH.write_text(markdown_report, encoding="utf-8")

    step_summary_path = os.getenv("GITHUB_STEP_SUMMARY")
    if step_summary_path:
        Path(step_summary_path).write_text(markdown_report, encoding="utf-8")

    print(f"Validation JSON report written to {JSON_REPORT_PATH}")
    print(f"Validation Markdown report written to {MARKDOWN_REPORT_PATH}")


def main() -> int:
    report = initialize_report()
    database: str | None = None
    failures: list[str] = []
    warnings: list[str] = []
    schema_results: list[dict] = []
    table_results: list[dict] = []
    test_results: list[dict] = []
    fatal_error: str | None = None
    conn = None

    try:
        env = require_environment()
        database = env["SNOWFLAKE_DATABASE"].upper()

        print("Starting Snowflake post-deploy validation.")

        conn = connect_to_snowflake(env)
        with conn.cursor() as cursor:
            cursor.execute(f"USE ROLE {env['SNOWFLAKE_ROLE']}")
            cursor.execute(f"USE WAREHOUSE {env['SNOWFLAKE_WAREHOUSE']}")
            cursor.execute(f"USE DATABASE {env['SNOWFLAKE_DATABASE']}")

            schema_failures, schema_results = validate_schema_object_counts(
                cursor, database
            )
            failures.extend(schema_failures)

            table_failures, table_results = validate_key_mart_row_counts(
                cursor, database
            )
            failures.extend(table_failures)

            test_failures, warnings, test_results = execute_quality_tests(cursor)
            failures.extend(test_failures)

        report = finalize_report(
            report,
            database,
            schema_results,
            table_results,
            test_results,
            failures,
            warnings,
            fatal_error,
        )

        if failures:
            print("Snowflake validation failed.")
            print(f"Failure count: {len(failures)}")
            for failure in failures:
                print(f"- {failure}")
            return 1

        print("Snowflake validation passed.")
        return 0
    except Exception as error:
        fatal_error = str(error)
        failures.append(f"Runtime error: {fatal_error}")
        report = finalize_report(
            report,
            database,
            schema_results,
            table_results,
            test_results,
            failures,
            warnings,
            fatal_error,
        )
        print(f"Validation failed: {fatal_error}", file=sys.stderr)
        return 1
    finally:
        if conn is not None:
            conn.close()
            print("Snowflake connection closed.")

        write_reports(report)


if __name__ == "__main__":
    raise SystemExit(main())
