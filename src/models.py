"""Build reporting models from the raw SQLite snapshot."""

from __future__ import annotations

import sqlite3
from collections import defaultdict
from datetime import date, timedelta

from src.load_data import (
    DataQualityError,
    optional_column,
    parse_timestamp,
)


def _create_latest_quote_versions(connection: sqlite3.Connection) -> None:
    version_table = "raw_quote_versions"
    quote_table = "raw_quotes"
    version_status = optional_column(
        connection, version_table, "quote_status", "v"
    )
    version_premium = optional_column(
        connection, version_table, "annual_premium", "v"
    )
    quote_customer = optional_column(connection, quote_table, "customer_id", "q")
    quote_bound = optional_column(connection, quote_table, "quote_bound_at", "q")
    quote_renewal = optional_column(
        connection, quote_table, "renewal_quote_id", "q"
    )

    connection.execute(
        """
        CREATE TABLE latest_quote_versions (
            quote_id TEXT PRIMARY KEY,
            quote_version_id TEXT NOT NULL,
            version_number INTEGER NOT NULL,
            customer_id TEXT,
            annual_premium REAL,
            quote_status TEXT,
            quote_version_created_at TEXT NOT NULL,
            quote_bound_at TEXT,
            renewal_quote_id TEXT
        )
        """
    )
    connection.execute(
        f"""
        INSERT INTO latest_quote_versions (
            quote_id, quote_version_id, version_number, customer_id,
            annual_premium, quote_status, quote_version_created_at,
            quote_bound_at, renewal_quote_id
        )
        SELECT
            ranked.quote_id,
            ranked.quote_version_id,
            CAST(ranked.version_number AS INTEGER),
            {quote_customer},
            CAST(ranked.annual_premium AS REAL),
            ranked.quote_status,
            ranked.quote_version_created_at,
            {quote_bound},
            {quote_renewal}
        FROM (
            SELECT
                v.quote_id,
                v.quote_version_id,
                v.version_number,
                {version_premium} AS annual_premium,
                {version_status} AS quote_status,
                v.quote_version_created_at,
                ROW_NUMBER() OVER (
                    PARTITION BY v.quote_id
                    ORDER BY
                        CAST(v.version_number AS INTEGER) DESC,
                        datetime(v.quote_version_created_at) DESC,
                        v.quote_version_id DESC
                ) AS version_rank
            FROM raw_quote_versions AS v
        ) AS ranked
        LEFT JOIN raw_quotes AS q ON q.quote_id = ranked.quote_id
        WHERE ranked.version_rank = 1
        """
    )


def _create_active_policies_by_day(connection: sqlite3.Connection) -> None:
    termination = optional_column(
        connection, "raw_policies", "policy_terminated_at", "raw_policies"
    )
    policies = connection.execute(
        f"""
        SELECT policy_id, policy_incepts_at, policy_expires_at,
               {termination} AS policy_terminated_at
        FROM raw_policies
        """
    ).fetchall()
    if not policies:
        connection.execute(
            """
            CREATE TABLE active_policies_by_day (
                date_day TEXT PRIMARY KEY,
                number_of_active_policies INTEGER NOT NULL
            )
            """
        )
        return

    parsed: list[tuple[date, date, str]] = []
    for policy in policies:
        inception = parse_timestamp(
            policy["policy_incepts_at"], "policy_incepts_at"
        )
        expiry = parse_timestamp(policy["policy_expires_at"], "policy_expires_at")
        termination = parse_timestamp(
            policy["policy_terminated_at"], "policy_terminated_at"
        )
        if inception is None or expiry is None:
            raise DataQualityError(
                f"Policy {policy['policy_id']!r} is missing an inception or expiry"
            )
        effective_end = min(expiry, termination) if termination else expiry
        if effective_end < inception:
            raise DataQualityError(
                f"Policy {policy['policy_id']!r} ends before it starts"
            )
        parsed.append((inception.date(), effective_end.date(), policy["policy_id"]))

    first_day = min(start for start, _, _ in parsed)
    final_day = max(
        parse_timestamp(row["policy_expires_at"], "policy_expires_at").date()
        for row in policies
    )
    changes: dict[date, int] = defaultdict(int)
    for start, end, _ in parsed:
        changes[start] += 1
        changes[end] -= 1

    connection.execute(
        """
        CREATE TABLE active_policies_by_day (
            date_day TEXT PRIMARY KEY,
            number_of_active_policies INTEGER NOT NULL
        )
        """
    )
    daily_rows: list[tuple[str, int]] = []
    count = 0
    current_day = first_day
    while current_day <= final_day:
        count += changes[current_day]
        daily_rows.append((current_day.isoformat(), count))
        current_day += timedelta(days=1)
    connection.executemany(
        """
        INSERT INTO active_policies_by_day(date_day, number_of_active_policies)
        VALUES (?, ?)
        """,
        daily_rows,
    )


def _create_renewal_funnel(connection: sqlite3.Connection) -> None:
    renewal_link = optional_column(
        connection, "raw_quotes", "renewal_quote_id", "original_quote"
    )
    original_policy_id = optional_column(
        connection, "raw_quotes", "policy_id", "original_quote"
    )
    renewal_policy_id = optional_column(
        connection, "raw_quotes", "policy_id", "renewal_quote"
    )
    renewal_bound_at = optional_column(
        connection, "raw_quotes", "quote_bound_at", "renewal_quote"
    )
    original_termination = optional_column(
        connection, "raw_policies", "policy_terminated_at", "original_policy"
    )

    connection.execute(
        """
        CREATE TABLE renewal_funnel (
            original_policy_id TEXT NOT NULL,
            policy_number TEXT NOT NULL,
            customer_id TEXT NOT NULL,
            original_quote_id TEXT NOT NULL,
            renewal_quote_id TEXT NOT NULL,
            renewal_quote_created_at TEXT,
            original_policy_terminated_at TEXT,
            original_policy_cancelled_after_renewal INTEGER NOT NULL,
            renewal_quote_edited INTEGER NOT NULL,
            renewal_bound INTEGER NOT NULL,
            renewal_bound_at TEXT,
            renewal_incepted INTEGER NOT NULL,
            renewal_policy_id TEXT,
            renewal_policy_incepts_at TEXT,
            PRIMARY KEY (original_policy_id, renewal_quote_id)
        )
        """
    )
    connection.execute(
        f"""
        INSERT INTO renewal_funnel (
            original_policy_id, policy_number, customer_id, original_quote_id,
            renewal_quote_id, renewal_quote_created_at,
            original_policy_terminated_at,
            original_policy_cancelled_after_renewal, renewal_quote_edited,
            renewal_bound, renewal_bound_at, renewal_incepted,
            renewal_policy_id, renewal_policy_incepts_at
        )
        SELECT DISTINCT
            original_policy.policy_id,
            original_policy.policy_number,
            original_policy.customer_id,
            original_quote.quote_id,
            renewal_quote.quote_id,
            (
                SELECT MIN(datetime(v.quote_version_created_at))
                FROM raw_quote_versions AS v
                WHERE v.quote_id = renewal_quote.quote_id
            ),
            {original_termination},
            CASE
                WHEN {original_termination} IS NOT NULL
                 AND (
                    SELECT MIN(datetime(v.quote_version_created_at))
                    FROM raw_quote_versions AS v
                    WHERE v.quote_id = renewal_quote.quote_id
                 ) IS NOT NULL
                 AND datetime({original_termination}) > (
                    SELECT MIN(datetime(v.quote_version_created_at))
                    FROM raw_quote_versions AS v
                    WHERE v.quote_id = renewal_quote.quote_id
                 )
                THEN 1 ELSE 0
            END,
            CASE WHEN (
                SELECT COUNT(DISTINCT v.version_number)
                FROM raw_quote_versions AS v
                WHERE v.quote_id = renewal_quote.quote_id
            ) > 1 THEN 1 ELSE 0 END,
            CASE WHEN {renewal_bound_at} IS NOT NULL THEN 1 ELSE 0 END,
            {renewal_bound_at},
            CASE WHEN renewal_policy.policy_incepts_at IS NOT NULL
                 THEN 1 ELSE 0 END,
            {renewal_policy_id},
            renewal_policy.policy_incepts_at
        FROM raw_quotes AS original_quote
        JOIN raw_policies AS original_policy
          ON original_policy.policy_id = {original_policy_id}
        JOIN raw_quotes AS renewal_quote
          ON renewal_quote.quote_id = {renewal_link}
        LEFT JOIN raw_policies AS renewal_policy
          ON renewal_policy.policy_id = {renewal_policy_id}
        WHERE {renewal_link} IS NOT NULL
        """
    )


def build_reporting_tables(connection: sqlite3.Connection) -> None:
    """Atomically rebuild the three assessment reporting tables."""
    with connection:
        connection.execute("DROP TABLE IF EXISTS latest_quote_versions")
        connection.execute("DROP TABLE IF EXISTS active_policies_by_day")
        connection.execute("DROP TABLE IF EXISTS renewal_funnel")
        _create_latest_quote_versions(connection)
        _create_active_policies_by_day(connection)
        _create_renewal_funnel(connection)
