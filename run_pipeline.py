"""Run the Quotes & Policies assessment pipeline end to end."""

from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path

from src.analytics import (
    average_premiums,
    bind_rate_by_day,
    overlapping_policy_customers,
)
from src.load_data import database_connection, load_csv_data
from src.models import build_reporting_tables

PROJECT_ROOT = Path(__file__).resolve().parent


def run_pipeline(
    dataset_dir: str | Path,
    database_path: str | Path,
    as_of: str | None = None,
) -> dict[str, object]:
    """Load feeds, rebuild reporting tables, and calculate requested metrics."""
    database = Path(database_path)
    database.parent.mkdir(parents=True, exist_ok=True)
    connection = database_connection(database)
    try:
        load_stats = load_csv_data(connection, dataset_dir)
        build_reporting_tables(connection)
        table_counts = {
            table: connection.execute(
                f'SELECT COUNT(*) FROM "{table}"'
            ).fetchone()[0]
            for table in (
                "latest_quote_versions",
                "active_policies_by_day",
                "renewal_funnel",
            )
        }
        return {
            "loaded": [
                {
                    "dataset": stat.dataset,
                    "rows_loaded": stat.rows_loaded,
                    "exact_duplicates_skipped": stat.exact_duplicates_skipped,
                    "schema_added": list(stat.schema_added),
                    "schema_removed": list(stat.schema_removed),
                }
                for stat in load_stats
            ],
            "reporting_table_rows": table_counts,
            "average_premiums_last_90_days": average_premiums(
                connection, as_of=as_of, days=90
            ),
            "customers_with_overlapping_policies": overlapping_policy_customers(
                connection
            ),
            "bind_rate_by_day_last_30_days": bind_rate_by_day(
                connection, as_of=as_of, days=30
            ),
        }
    finally:
        connection.close()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--dataset-dir",
        type=Path,
        default=PROJECT_ROOT / "task_datasets",
        help="Directory containing the four input CSV files",
    )
    parser.add_argument(
        "--database",
        type=Path,
        default=PROJECT_ROOT / "insurance.db",
        help="SQLite database path (default: ./insurance.db)",
    )
    parser.add_argument(
        "--as-of",
        help="End date (YYYY-MM-DD) for the 90-day and 30-day analytics windows",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    result = run_pipeline(args.dataset_dir, args.database, args.as_of)
    print(json.dumps(result, indent=2, allow_nan=False))


if __name__ == "__main__":
    main()
