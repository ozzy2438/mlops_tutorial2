"""Deploy Snowflake SQL files in medallion-layer order.

The script reads Snowflake connection details from environment variables and
never prints secrets. Files containing credential placeholders are skipped with
a clear warning so unsafe SQL is not deployed accidentally.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

import snowflake.connector


ROOT = Path(__file__).resolve().parents[1]
DEPLOYMENT_DIRS = [
    ROOT / "snowflake" / "setup",
    ROOT / "snowflake" / "staging",
    ROOT / "snowflake" / "int_layer",
    ROOT / "snowflake" / "marts_layer",
    ROOT / "snowflake" / "ml",
]

DIRECTORY_SCHEMAS = {
    "setup": "RAW",
    "staging": "STAGING",
    "int_layer": "INT_LAYER",
    "marts_layer": "MARTS_LAYER",
    "ml": "ML_LAYER",
}

REQUIRED_ENV_VARS = [
    "SNOWFLAKE_ACCOUNT",
    "SNOWFLAKE_USER",
    "SNOWFLAKE_PASSWORD",
    "SNOWFLAKE_ROLE",
    "SNOWFLAKE_WAREHOUSE",
    "SNOWFLAKE_DATABASE",
]

PLACEHOLDER_MARKERS = [
    "<AZURE_SAS_TOKEN_PLACEHOLDER>",
    "<SNOWFLAKE_PASSWORD_PLACEHOLDER>",
    "<PASSWORD_PLACEHOLDER>",
    "<GITHUB_TOKEN_PLACEHOLDER>",
    "<AZURE_CONNECTION_STRING_PLACEHOLDER>",
    "<AZURE_STORAGE_KEY_PLACEHOLDER>",
]

CI_CD_SKIP_FILES = {
    ROOT / "snowflake" / "setup" / "02_create_azure_stage.sql",
    ROOT / "snowflake" / "setup" / "03_copy_into_raw_tables.sql",
}

AZURE_INGESTION_SKIP_MESSAGE = (
    "Skipping Azure external stage / raw ingestion script in CI/CD because "
    "it requires runtime SAS credentials."
)


def require_environment() -> dict[str, str]:
    missing = [name for name in REQUIRED_ENV_VARS if not os.getenv(name)]
    if missing:
        raise RuntimeError(
            "Missing required Snowflake environment variables: "
            + ", ".join(missing)
        )

    return {name: os.environ[name] for name in REQUIRED_ENV_VARS}


def split_sql_statements(sql_text: str) -> list[str]:
    """Split simple Snowflake SQL files on semicolons outside quoted strings."""
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


def file_contains_placeholder(path: Path) -> bool:
    content = path.read_text(encoding="utf-8")
    return any(marker in content for marker in PLACEHOLDER_MARKERS)


def should_skip_in_cicd(path: Path) -> bool:
    return path in CI_CD_SKIP_FILES


def sql_files_in_order() -> list[Path]:
    files: list[Path] = []
    for directory in DEPLOYMENT_DIRS:
        if not directory.exists():
            print(f"WARNING: Deployment directory not found: {directory}")
            continue
        files.extend(sorted(directory.glob("*.sql")))
    return files


def schema_for_file(path: Path) -> str | None:
    parent_name = path.parent.name
    return DIRECTORY_SCHEMAS.get(parent_name)


def connect_to_snowflake(env: dict[str, str]):
    return snowflake.connector.connect(
        account=env["SNOWFLAKE_ACCOUNT"],
        user=env["SNOWFLAKE_USER"],
        password=env["SNOWFLAKE_PASSWORD"],
        role=env["SNOWFLAKE_ROLE"],
        warehouse=env["SNOWFLAKE_WAREHOUSE"],
        database=env["SNOWFLAKE_DATABASE"],
    )


def main() -> int:
    env = require_environment()
    files = sql_files_in_order()

    if not files:
        raise RuntimeError("No Snowflake SQL files found to deploy.")

    print("Starting Snowflake SQL deployment.")
    print("Deployment order: setup -> staging -> int_layer -> marts_layer -> ml")

    conn = connect_to_snowflake(env)
    try:
        with conn.cursor() as cursor:
            cursor.execute(f"USE ROLE {env['SNOWFLAKE_ROLE']}")
            cursor.execute(f"USE WAREHOUSE {env['SNOWFLAKE_WAREHOUSE']}")
            cursor.execute(f"USE DATABASE {env['SNOWFLAKE_DATABASE']}")

            for path in files:
                relative_path = path.relative_to(ROOT)
                if should_skip_in_cicd(path):
                    print(f"{AZURE_INGESTION_SKIP_MESSAGE} File: {relative_path}")
                    continue

                if file_contains_placeholder(path):
                    print(
                        f"WARNING: Skipping {relative_path} because it contains "
                        "a credential placeholder."
                    )
                    continue

                schema = schema_for_file(path)
                if schema:
                    cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
                    cursor.execute(f"USE SCHEMA {schema}")

                statements = split_sql_statements(path.read_text(encoding="utf-8"))
                print(f"Executing {relative_path} ({len(statements)} statements)")

                for index, statement in enumerate(statements, start=1):
                    try:
                        cursor.execute(statement)
                    except Exception as exc:
                        print(
                            f"ERROR: Failed executing {relative_path}, "
                            f"statement {index}."
                        )
                        raise exc

        print("Snowflake SQL deployment completed successfully.")
        return 0
    finally:
        conn.close()
        print("Snowflake connection closed.")


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as error:
        print(f"Deployment failed: {error}", file=sys.stderr)
        raise
