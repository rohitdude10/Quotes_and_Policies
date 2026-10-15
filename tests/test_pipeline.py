from __future__ import annotations

import csv
import sqlite3
import tempfile
import unittest
from pathlib import Path

from src.analytics import (
    average_premiums,
    bind_rate_by_day,
    overlapping_policy_customers,
)
from src.load_data import (
    DataQualityError,
    database_connection,
    get_record_as_of,
    get_records_as_of,
    load_csv_data,
)
from src.models import build_reporting_tables


class PipelineTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary_directory.name)
        self.dataset_dir = self.root / "task_datasets"
        self.dataset_dir.mkdir()
        self.database_path = self.root / "insurance.db"
        self.connections: list[sqlite3.Connection] = []
        self._write_fixture()

    def tearDown(self) -> None:
        for connection in self.connections:
            connection.close()
        self.temporary_directory.cleanup()

    def _write_csv(
        self, filename: str, columns: list[str], rows: list[dict[str, str]]
    ) -> None:
        with (self.dataset_dir / filename).open(
            "w", encoding="utf-8", newline=""
        ) as output:
            writer = csv.DictWriter(output, fieldnames=columns)
            writer.writeheader()
            writer.writerows(rows)

    def _write_fixture(self) -> None:
        self._write_csv(
            "customers.csv",
            ["customer_id", "name", "email"],
            [{"customer_id": "C1", "name": "Customer 1", "email": "c1@example.com"}],
        )
        self._write_csv(
            "quotes.csv",
            [
                "quote_id",
                "quote_bound_at",
                "renewal_quote_id",
                "policy_id",
                "customer_id",
            ],
            [
                {
                    "quote_id": "Q1",
                    "quote_bound_at": "2022-12-20 10:00:00",
                    "renewal_quote_id": "QR",
                    "policy_id": "P1",
                    "customer_id": "C1",
                },
                {
                    "quote_id": "QR",
                    "quote_bound_at": "2023-12-30 10:00:00",
                    "renewal_quote_id": "",
                    "policy_id": "P1R",
                    "customer_id": "C1",
                },
                {
                    "quote_id": "Q2",
                    "quote_bound_at": "2023-12-22 10:00:00",
                    "renewal_quote_id": "",
                    "policy_id": "P2",
                    "customer_id": "C1",
                },
            ],
        )
        self._write_csv(
            "policies.csv",
            [
                "policy_id",
                "policy_number",
                "policy_incepts_at",
                "policy_expires_at",
                "policy_terminated_at",
                "customer_id",
            ],
            [
                {
                    "policy_id": "P1",
                    "policy_number": "PN1",
                    "policy_incepts_at": "2022-12-21 10:00:00",
                    "policy_expires_at": "2024-01-01 10:00:00",
                    "policy_terminated_at": "2023-12-25 10:00:00",
                    "customer_id": "C1",
                },
                {
                    "policy_id": "P1R",
                    "policy_number": "PN1",
                    "policy_incepts_at": "2024-01-04 10:00:00",
                    "policy_expires_at": "2024-02-04 10:00:00",
                    "policy_terminated_at": "",
                    "customer_id": "C1",
                },
                {
                    "policy_id": "P2",
                    "policy_number": "PN2",
                    "policy_incepts_at": "2023-12-22 10:00:00",
                    "policy_expires_at": "2024-01-05 10:00:00",
                    "policy_terminated_at": "",
                    "customer_id": "C1",
                },
            ],
        )
        self._write_csv(
            "quote_versions.csv",
            [
                "quote_id",
                "version_number",
                "quote_status",
                "quote_version_created_at",
                "annual_premium",
                "quote_version_id",
            ],
            [
                {
                    "quote_id": "Q1",
                    "version_number": "1",
                    "quote_status": "QUOTE",
                    "quote_version_created_at": "2022-12-01 10:00:00",
                    "annual_premium": "100",
                    "quote_version_id": "V1",
                },
                {
                    "quote_id": "Q1",
                    "version_number": "2",
                    "quote_status": "QUOTE",
                    "quote_version_created_at": "2022-12-10 10:00:00",
                    "annual_premium": "120",
                    "quote_version_id": "V2",
                },
                {
                    "quote_id": "QR",
                    "version_number": "1",
                    "quote_status": "QUOTE",
                    "quote_version_created_at": "2023-12-20 10:00:00",
                    "annual_premium": "125",
                    "quote_version_id": "VR1",
                },
                {
                    "quote_id": "QR",
                    "version_number": "2",
                    "quote_status": "QUOTE",
                    "quote_version_created_at": "2023-12-21 10:00:00",
                    "annual_premium": "130",
                    "quote_version_id": "VR2",
                },
                {
                    "quote_id": "Q2",
                    "version_number": "1",
                    "quote_status": "QUOTE",
                    "quote_version_created_at": "2023-12-22 09:00:00",
                    "annual_premium": "200",
                    "quote_version_id": "VQ2",
                },
                {
                    "quote_id": "Q2",
                    "version_number": "1",
                    "quote_status": "QUOTE",
                    "quote_version_created_at": "2023-12-22 09:30:00",
                    "annual_premium": "210",
                    "quote_version_id": "VQ2-LATE",
                },
            ],
        )

    def _loaded_connection(self) -> sqlite3.Connection:
        connection = database_connection(self.database_path)
        self.connections.append(connection)
        load_csv_data(connection, self.dataset_dir)
        return connection

    def test_load_is_idempotent_deduplicates_exact_rows_and_tracks_schema(self) -> None:
        versions_path = self.dataset_dir / "quote_versions.csv"
        with versions_path.open("r", encoding="utf-8", newline="") as source:
            rows = list(csv.DictReader(source))
            columns = list(rows[0])
        self._write_csv("quote_versions.csv", columns, rows + [rows[0]])

        connection = database_connection(self.database_path)
        self.connections.append(connection)
        first = load_csv_data(connection, self.dataset_dir)
        self.assertEqual(first[-1].exact_duplicates_skipped, 1)
        second = load_csv_data(connection, self.dataset_dir)
        self.assertEqual(second[-1].rows_loaded, len(rows))
        self.assertEqual(
            connection.execute(
                "SELECT COUNT(*) FROM raw_quote_versions"
            ).fetchone()[0],
            len(rows),
        )

        self._write_csv(
            "customers.csv",
            ["customer_id", "name", "email", "segment"],
            [
                {
                    "customer_id": "C1",
                    "name": "Customer 1",
                    "email": "c1@example.com",
                    "segment": "standard",
                }
            ],
        )
        changed_schema = load_csv_data(connection, self.dataset_dir)
        self.assertIn("segment", changed_schema[0].schema_added)
        self.assertIn(
            "segment",
            {
                row["name"]
                for row in connection.execute(
                    "PRAGMA table_info(raw_customers)"
                ).fetchall()
            },
        )
        connection.close()

    def test_optional_columns_may_be_absent_from_a_new_feed_schema(self) -> None:
        self._write_csv(
            "quotes.csv",
            ["quote_id", "customer_id"],
            [{"quote_id": "Q1", "customer_id": "C1"}],
        )
        self._write_csv(
            "policies.csv",
            [
                "policy_id",
                "policy_number",
                "policy_incepts_at",
                "policy_expires_at",
                "customer_id",
            ],
            [
                {
                    "policy_id": "P1",
                    "policy_number": "PN1",
                    "policy_incepts_at": "2024-01-01 00:00:00",
                    "policy_expires_at": "2024-02-01 00:00:00",
                    "customer_id": "C1",
                }
            ],
        )
        self._write_csv(
            "quote_versions.csv",
            [
                "quote_id",
                "version_number",
                "quote_status",
                "quote_version_created_at",
                "annual_premium",
                "quote_version_id",
            ],
            [
                {
                    "quote_id": "Q1",
                    "version_number": "1",
                    "quote_status": "QUOTE",
                    "quote_version_created_at": "2023-12-31 10:00:00",
                    "annual_premium": "100",
                    "quote_version_id": "V1",
                }
            ],
        )
        connection = self._loaded_connection()
        build_reporting_tables(connection)
        self.assertEqual(
            connection.execute("SELECT COUNT(*) FROM renewal_funnel").fetchone()[0],
            0,
        )
        self.assertEqual(overlapping_policy_customers(connection), 0)

    def test_record_history_supports_as_of_queries_updates_and_deletions(self) -> None:
        self._write_csv(
            "customers.csv",
            ["customer_id", "name", "email"],
            [
                {
                    "customer_id": "C1",
                    "name": "Customer 1",
                    "email": "c1@example.com",
                },
                {
                    "customer_id": "C2",
                    "name": "Customer 2",
                    "email": "c2@example.com",
                },
            ],
        )
        connection = database_connection(self.database_path)
        self.connections.append(connection)
        load_csv_data(connection, self.dataset_dir)
        first_version = connection.execute(
            """
            SELECT valid_from FROM record_history
            WHERE dataset = 'customers' AND record_key = 'C1' AND is_current = 1
            """
        ).fetchone()["valid_from"]
        first_snapshot = get_records_as_of(connection, "customers", first_version)
        self.assertEqual(len(first_snapshot), 2)
        self.assertEqual(
            get_record_as_of(connection, "customers", "C1", first_version)["name"],
            "Customer 1",
        )

        self._write_csv(
            "customers.csv",
            ["customer_id", "name", "email"],
            [
                {
                    "customer_id": "C1",
                    "name": "Updated Customer",
                    "email": "c1@example.com",
                }
            ],
        )
        load_csv_data(connection, self.dataset_dir)
        self.assertEqual(
            get_record_as_of(connection, "customers", "C1", first_version)["name"],
            "Customer 1",
        )
        latest = connection.execute(
            """
            SELECT valid_from FROM record_history
            WHERE dataset = 'customers' AND record_key = 'C1' AND is_current = 1
            """
        ).fetchone()["valid_from"]
        self.assertEqual(
            get_record_as_of(connection, "customers", "C1", latest)["name"],
            "Updated Customer",
        )
        self.assertIsNone(
            get_record_as_of(connection, "customers", "C2", latest)
        )
        self.assertEqual(len(get_records_as_of(connection, "customers", latest)), 1)
        self.assertEqual(
            connection.execute(
                """
                SELECT COUNT(*) FROM record_history
                WHERE dataset = 'customers' AND record_key = 'C1'
                """
            ).fetchone()[0],
            2,
        )

        load_csv_data(connection, self.dataset_dir)
        self.assertEqual(
            connection.execute(
                """
                SELECT COUNT(*) FROM record_history
                WHERE dataset = 'customers' AND record_key = 'C1'
                """
            ).fetchone()[0],
            2,
        )

    def test_conflicting_duplicate_natural_keys_fail_loudly(self) -> None:
        versions_path = self.dataset_dir / "quote_versions.csv"
        with versions_path.open("r", encoding="utf-8", newline="") as source:
            rows = list(csv.DictReader(source))
            columns = list(rows[0])
        conflict = dict(rows[0], annual_premium="999")
        self._write_csv("quote_versions.csv", columns, rows + [conflict])

        connection = database_connection(self.database_path)
        self.connections.append(connection)
        with self.assertRaises(DataQualityError):
            load_csv_data(connection, self.dataset_dir)
        self.assertEqual(
            connection.execute(
                "SELECT COUNT(*) FROM sqlite_master WHERE name = 'raw_quotes'"
            ).fetchone()[0],
            0,
        )
        connection.close()

    def test_reporting_models_use_versions_dates_and_renewal_links(self) -> None:
        connection = self._loaded_connection()
        build_reporting_tables(connection)

        latest = connection.execute(
            "SELECT version_number, annual_premium FROM latest_quote_versions "
            "WHERE quote_id = 'Q1'"
        ).fetchone()
        self.assertEqual((latest["version_number"], latest["annual_premium"]), (2, 120))
        tied_latest = connection.execute(
            "SELECT annual_premium FROM latest_quote_versions WHERE quote_id = 'Q2'"
        ).fetchone()
        self.assertEqual(tied_latest["annual_premium"], 210)

        daily = dict(
            connection.execute(
                "SELECT date_day, number_of_active_policies "
                "FROM active_policies_by_day"
            ).fetchall()
        )
        self.assertEqual(daily["2023-12-24"], 2)
        self.assertEqual(daily["2023-12-25"], 1)
        self.assertEqual(daily["2024-01-04"], 2)

        renewal = connection.execute(
            "SELECT original_policy_cancelled_after_renewal, "
            "renewal_quote_edited, renewal_bound, renewal_incepted "
            "FROM renewal_funnel WHERE original_policy_id = 'P1'"
        ).fetchone()
        self.assertEqual(tuple(renewal), (1, 1, 1, 1))
        self.assertEqual(overlapping_policy_customers(connection), 1)
        connection.close()

    def test_analytics_use_event_dates_and_return_complete_bind_calendar(self) -> None:
        connection = self._loaded_connection()
        build_reporting_tables(connection)
        averages = average_premiums(connection, as_of="2024-01-03", days=90)
        self.assertEqual(averages["average_quoted_premium"], 162.5)
        self.assertEqual(averages["quoted_quote_count"], 2)
        self.assertEqual(averages["average_bound_premium"], 170)
        self.assertEqual(averages["bound_quote_count"], 2)
        self.assertEqual(averages["average_incepted_premium"], 210)
        self.assertEqual(averages["incepted_policy_count"], 1)

        rates = bind_rate_by_day(connection, as_of="2024-01-03", days=30)
        self.assertEqual(len(rates), 30)
        self.assertEqual(rates[-1]["date_day"], "2024-01-03")
        self.assertEqual(rates[-1]["quotes_created"], 0)
        self.assertIsNone(rates[-1]["bind_rate_pct"])
        self.assertEqual(
            sum(row["quotes_created"] for row in rates),
            2,
        )
        connection.close()


if __name__ == "__main__":
    unittest.main()
