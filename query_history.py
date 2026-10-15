"""Query the record state known to the pipeline at a past ingestion time."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from src.load_data import (
    DATASETS,
    database_connection,
    get_record_as_of,
    get_records_as_of,
)

PROJECT_ROOT = Path(__file__).resolve().parent


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--database",
        type=Path,
        default=PROJECT_ROOT / "insurance.db",
        help="SQLite database containing record_history",
    )
    parser.add_argument(
        "--dataset",
        choices=sorted(DATASETS),
        required=True,
        help="Dataset to query",
    )
    parser.add_argument(
        "--as-of",
        required=True,
        help="ISO date or timestamp, e.g. 2025-06-24T12:30:00Z",
    )
    parser.add_argument(
        "--key",
        help="Natural key for one record; omit to return the full dataset snapshot",
    )
    args = parser.parse_args()

    connection = database_connection(args.database)
    try:
        if args.key is not None:
            result = get_record_as_of(
                connection, args.dataset, args.key, args.as_of
            )
        else:
            result = get_records_as_of(connection, args.dataset, args.as_of)
        print(json.dumps(result, indent=2, allow_nan=False))
    finally:
        connection.close()


if __name__ == "__main__":
    main()
