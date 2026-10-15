"""CSV ingestion and raw-table schema management."""

from __future__ import annotations

import csv
import json
import logging
import math
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

logger = logging.getLogger(__name__)

DATASETS = {
    "customers": ("customers.csv", "raw_customers", "customer_id"),
    "quotes": ("quotes.csv", "raw_quotes", "quote_id"),
    "policies": ("policies.csv", "raw_policies", "policy_id"),
    "quote_versions": (
        "quote_versions.csv",
        "raw_quote_versions",
        "quote_version_id",
    ),
}

REQUIRED_COLUMNS = {
    "customers": {"customer_id"},
    "quotes": {"quote_id", "customer_id"},
    "policies": {
        "policy_id",
        "policy_number",
        "policy_incepts_at",
        "policy_expires_at",
        "customer_id",
    },
    "quote_versions": {
        "quote_id",
        "quote_version_id",
        "version_number",
        "quote_version_created_at",
    },
}

NON_NULLABLE_COLUMNS = {
    "customers": {"customer_id"},
    "quotes": {"quote_id", "customer_id"},
    "policies": {
        "policy_id",
        "policy_number",
        "policy_incepts_at",
        "policy_expires_at",
        "customer_id",
    },
    "quote_versions": {
        "quote_id",
        "quote_version_id",
        "version_number",
        "quote_version_created_at",
    },
}

TIMESTAMP_COLUMNS = {
    "quote_bound_at",
    "quote_version_created_at",
    "policy_incepts_at",
    "policy_expires_at",
    "policy_terminated_at",
}


class DataQualityError(ValueError):
    """Raised when an input feed contains ambiguous or invalid business data."""


@dataclass(frozen=True)
class LoadStats:
    dataset: str
    rows_loaded: int
    exact_duplicates_skipped: int
    schema_added: tuple[str, ...]
    schema_removed: tuple[str, ...]


def parse_timestamp(value: str | None, field_name: str) -> datetime | None:
    """Parse a feed timestamp and normalize timezone-aware values to UTC."""
    if value is None or not value.strip():
        return None

    text = value.strip()
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError as exc:
        raise DataQualityError(
            f"Invalid timestamp in {field_name}: {value!r}"
        ) from exc

    if parsed.tzinfo is not None:
        parsed = parsed.astimezone(timezone.utc).replace(tzinfo=None)
    return parsed


def _quote_identifier(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def _read_csv(
    dataset: str, file_path: Path
) -> tuple[list[str], list[dict[str, str | None]], int]:
    if not file_path.is_file():
        raise FileNotFoundError(f"Required input file not found: {file_path}")

    with file_path.open("r", encoding="utf-8-sig", newline="") as source:
        reader = csv.DictReader(source)
        if reader.fieldnames is None:
            raise DataQualityError(f"{file_path.name} has no header row")
        source_columns = list(reader.fieldnames)
        columns = [name.strip() for name in source_columns]
        if any(not name for name in columns):
            raise DataQualityError(f"{file_path.name} contains an empty column name")
        if len(columns) != len(set(columns)):
            raise DataQualityError(f"{file_path.name} contains duplicate column names")

        missing = REQUIRED_COLUMNS[dataset] - set(columns)
        if missing:
            raise DataQualityError(
                f"{file_path.name} is missing required columns: {sorted(missing)}"
            )

        unique_rows: dict[str, dict[str, str | None]] = {}
        exact_duplicates = 0
        for line_number, source_row in enumerate(reader, start=2):
            if None in source_row:
                raise DataQualityError(
                    f"{file_path.name}:{line_number} has more values than columns"
                )
            row = {
                column: (source_row.get(source_column) or "").strip() or None
                for column, source_column in zip(columns, source_columns)
            }
            missing_values = [
                column
                for column in NON_NULLABLE_COLUMNS[dataset]
                if row.get(column) is None
            ]
            if missing_values:
                raise DataQualityError(
                    f"{file_path.name}:{line_number} has empty required values: "
                    f"{sorted(missing_values)}"
                )
            key_column = DATASETS[dataset][2]
            key = row.get(key_column)
            if key is None:
                raise DataQualityError(
                    f"{file_path.name}:{line_number} has an empty {key_column}"
                )

            for column in columns:
                value = row[column]
                if column in TIMESTAMP_COLUMNS and value is not None:
                    parse_timestamp(value, column)
                elif column == "version_number" and value is not None:
                    try:
                        if int(value) < 1:
                            raise ValueError
                    except ValueError as exc:
                        raise DataQualityError(
                            f"{file_path.name}:{line_number} has invalid "
                            f"version_number {value!r}"
                        ) from exc
                elif column == "annual_premium" and value is not None:
                    try:
                        premium = float(value)
                    except ValueError as exc:
                        raise DataQualityError(
                            f"{file_path.name}:{line_number} has invalid "
                            f"annual_premium {value!r}"
                        ) from exc
                    if not math.isfinite(premium):
                        raise DataQualityError(
                            f"{file_path.name}:{line_number} has non-finite "
                            "annual_premium"
                        )

            prior = unique_rows.get(key)
            if prior is not None:
                if prior == row:
                    exact_duplicates += 1
                    continue
                raise DataQualityError(
                    f"{file_path.name} has conflicting rows for {key_column}={key!r}"
                )
            unique_rows[key] = row

    logger.info(
        "Inspected %s: %d unique rows, %d exact duplicate rows skipped",
        file_path.name,
        len(unique_rows),
        exact_duplicates,
    )
    return columns, list(unique_rows.values()), exact_duplicates


def _ensure_raw_table(
    connection: sqlite3.Connection,
    table: str,
    key_column: str,
    columns: list[str],
) -> set[str]:
    table_sql = _quote_identifier(table)
    current = {
        row["name"]
        for row in connection.execute(f"PRAGMA table_info({table_sql})").fetchall()
    }
    if not current:
        definitions = [
            f"{_quote_identifier(column)} TEXT"
            for column in columns
        ]
        definitions.append(f"PRIMARY KEY ({_quote_identifier(key_column)})")
        connection.execute(f"CREATE TABLE {table_sql} ({', '.join(definitions)})")
        return set()

    if key_column not in current:
        raise DataQualityError(
            f"Existing table {table} does not have its expected key {key_column}"
        )
    for column in columns:
        if column not in current:
            connection.execute(
                f"ALTER TABLE {table_sql} ADD COLUMN {_quote_identifier(column)} TEXT"
            )
    return current


def load_csv_data(
    connection: sqlite3.Connection, dataset_dir: str | Path
) -> list[LoadStats]:
    """Load all CSVs as a replaceable snapshot in one SQLite transaction.

    Repeated natural keys are accepted only when their complete rows match.
    Older optional columns are retained as NULL if a feed drops them; newly
    introduced columns are added to the raw table and schema history.
    """
    root = Path(dataset_dir)
    prepared: dict[str, tuple[list[str], list[dict[str, str | None]], int]] = {}
    for dataset, (filename, _, _) in DATASETS.items():
        prepared[dataset] = _read_csv(dataset, root / filename)

    stats: list[LoadStats] = []
    with connection:
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS raw_schema_registry (
                dataset TEXT PRIMARY KEY,
                columns_json TEXT NOT NULL
            )
            """
        )
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS raw_schema_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                dataset TEXT NOT NULL,
                observed_at TEXT NOT NULL,
                columns_json TEXT NOT NULL,
                added_columns_json TEXT NOT NULL,
                removed_columns_json TEXT NOT NULL,
                rows_loaded INTEGER NOT NULL,
                exact_duplicates_skipped INTEGER NOT NULL
            )
            """
        )

        for dataset, (_, table, key_column) in DATASETS.items():
            columns, rows, duplicate_count = prepared[dataset]
            prior_schema_row = connection.execute(
                "SELECT columns_json FROM raw_schema_registry WHERE dataset = ?",
                (dataset,),
            ).fetchone()
            prior_columns = (
                set(json.loads(prior_schema_row["columns_json"]))
                if prior_schema_row is not None
                else set()
            )
            added = sorted(set(columns) - prior_columns)
            removed = sorted(prior_columns - set(columns))
            existing_columns = _ensure_raw_table(
                connection, table, key_column, columns
            )
            retained_removed = sorted(set(removed) & existing_columns)
            if added:
                logger.info("%s schema added columns: %s", dataset, added)
            if removed:
                logger.warning(
                    "%s schema removed columns (retained as NULL in raw table): %s",
                    dataset,
                    removed,
                )

            table_sql = _quote_identifier(table)
            connection.execute(f"DELETE FROM {table_sql}")
            columns_sql = ", ".join(_quote_identifier(column) for column in columns)
            placeholders = ", ".join("?" for _ in columns)
            connection.executemany(
                f"INSERT INTO {table_sql} ({columns_sql}) VALUES ({placeholders})",
                [tuple(row[column] for column in columns) for row in rows],
            )

            serialized_columns = json.dumps(columns, separators=(",", ":"))
            connection.execute(
                """
                INSERT INTO raw_schema_registry(dataset, columns_json)
                VALUES (?, ?)
                ON CONFLICT(dataset) DO UPDATE SET columns_json = excluded.columns_json
                """,
                (dataset, serialized_columns),
            )
            connection.execute(
                """
                INSERT INTO raw_schema_history(
                    dataset, observed_at, columns_json, added_columns_json,
                    removed_columns_json, rows_loaded, exact_duplicates_skipped
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    dataset,
                    datetime.now(timezone.utc).isoformat(timespec="seconds"),
                    serialized_columns,
                    json.dumps(added),
                    json.dumps(retained_removed),
                    len(rows),
                    duplicate_count,
                ),
            )
            stats.append(
                LoadStats(
                    dataset=dataset,
                    rows_loaded=len(rows),
                    exact_duplicates_skipped=duplicate_count,
                    schema_added=tuple(added),
                    schema_removed=tuple(retained_removed),
                )
            )
        _validate_relationships(connection)

    return stats


def _validate_relationships(connection: sqlite3.Connection) -> None:
    """Reject non-empty foreign keys that do not resolve in this snapshot."""
    relationships = (
        ("raw_quotes", "customer_id", "raw_customers", "customer_id"),
        ("raw_policies", "customer_id", "raw_customers", "customer_id"),
        ("raw_quote_versions", "quote_id", "raw_quotes", "quote_id"),
    )
    for child_table, child_column, parent_table, parent_column in relationships:
        orphan = connection.execute(
            f"""
            SELECT child.{_quote_identifier(child_column)}
            FROM {_quote_identifier(child_table)} AS child
            LEFT JOIN {_quote_identifier(parent_table)} AS parent
              ON parent.{_quote_identifier(parent_column)}
                 = child.{_quote_identifier(child_column)}
            WHERE child.{_quote_identifier(child_column)} IS NOT NULL
              AND parent.{_quote_identifier(parent_column)} IS NULL
            LIMIT 1
            """
        ).fetchone()
        if orphan is not None:
            raise DataQualityError(
                f"{child_table}.{child_column} references missing "
                f"{parent_table}.{parent_column}: {orphan[0]!r}"
            )

    optional_relationships = (
        ("raw_quotes", "policy_id", "raw_policies", "policy_id"),
        ("raw_quotes", "renewal_quote_id", "raw_quotes", "quote_id"),
    )
    for child_table, child_column, parent_table, parent_column in optional_relationships:
        if child_column not in table_columns(connection, child_table):
            continue
        orphan = connection.execute(
            f"""
            SELECT child.{_quote_identifier(child_column)}
            FROM {_quote_identifier(child_table)} AS child
            LEFT JOIN {_quote_identifier(parent_table)} AS parent
              ON parent.{_quote_identifier(parent_column)}
                 = child.{_quote_identifier(child_column)}
            WHERE child.{_quote_identifier(child_column)} IS NOT NULL
              AND parent.{_quote_identifier(parent_column)} IS NULL
            LIMIT 1
            """
        ).fetchone()
        if orphan is not None:
            raise DataQualityError(
                f"{child_table}.{child_column} references missing "
                f"{parent_table}.{parent_column}: {orphan[0]!r}"
            )


def table_columns(connection: sqlite3.Connection, table: str) -> set[str]:
    """Return the current SQLite column names for a table."""
    return {
        row["name"]
        for row in connection.execute(
            f"PRAGMA table_info({_quote_identifier(table)})"
        ).fetchall()
    }


def optional_column(
    connection: sqlite3.Connection, table: str, column: str, alias: str
) -> str:
    """Return a qualified column expression, or SQL NULL if it is absent."""
    if column in table_columns(connection, table):
        return f"{_quote_identifier(alias)}.{_quote_identifier(column)}"
    return "NULL"


def database_connection(database_path: str | Path) -> sqlite3.Connection:
    """Open a row-addressable SQLite connection with foreign keys enabled."""
    connection = sqlite3.connect(database_path)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA foreign_keys = ON")
    return connection
