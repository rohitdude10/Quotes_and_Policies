"""Business analytics over the raw and modeled SQLite tables."""

from __future__ import annotations

import sqlite3
from datetime import date, datetime, timedelta, timezone
from typing import Any

from src.load_data import optional_column


def _as_date(value: date | str | None) -> date:
    if value is None:
        return datetime.now(timezone.utc).date()
    if isinstance(value, date):
        return value
    return date.fromisoformat(value)


def _window_start(as_of: date, days: int) -> date:
    if days < 1:
        raise ValueError("days must be at least 1")
    return as_of - timedelta(days=days - 1)


def _average(
    connection: sqlite3.Connection,
    query: str,
    start_day: date,
    end_day: date,
) -> tuple[float | None, int]:
    row = connection.execute(query, (start_day.isoformat(), end_day.isoformat())).fetchone()
    return (row["average_premium"], row["event_count"])


def average_premiums(
    connection: sqlite3.Connection,
    as_of: date | str | None = None,
    days: int = 90,
) -> dict[str, Any]:
    """Average offered, bound, and incepted premium over calendar-day windows.

    Each metric uses the latest non-null premium version available at its
    corresponding event time. The quoted metric uses the first offered
    (QUOTE-status) version as the quote's creation-time premium.
    """
    end_day = _as_date(as_of)
    start_day = _window_start(end_day, days)
    status = optional_column(
        connection, "raw_quote_versions", "quote_status", "v"
    )
    premium = optional_column(
        connection, "raw_quote_versions", "annual_premium", "v"
    )
    status_filter = f"{status} = 'QUOTE'" if status != "NULL" else "1 = 1"

    quoted_query = f"""
        WITH ranked AS (
            SELECT
                v.quote_id,
                date(datetime(v.quote_version_created_at)) AS event_day,
                CAST({premium} AS REAL) AS premium,
                ROW_NUMBER() OVER (
                    PARTITION BY v.quote_id
                    ORDER BY datetime(v.quote_version_created_at),
                             CAST(v.version_number AS INTEGER),
                             v.quote_version_id
                ) AS event_rank
            FROM raw_quote_versions AS v
            WHERE {premium} IS NOT NULL AND {status_filter}
        )
        SELECT AVG(premium) AS average_premium, COUNT(*) AS event_count
        FROM ranked
        WHERE event_rank = 1 AND event_day BETWEEN ? AND ?
    """
    quoted_average, quoted_count = _average(
        connection, quoted_query, start_day, end_day
    )

    bound_at = optional_column(connection, "raw_quotes", "quote_bound_at", "q")
    bound_query = f"""
        WITH ranked AS (
            SELECT
                q.quote_id,
                date(datetime({bound_at})) AS event_day,
                CAST({premium} AS REAL) AS premium,
                ROW_NUMBER() OVER (
                    PARTITION BY q.quote_id
                    ORDER BY datetime(v.quote_version_created_at) DESC,
                             CAST(v.version_number AS INTEGER) DESC,
                             v.quote_version_id DESC
                ) AS event_rank
            FROM raw_quotes AS q
            JOIN raw_quote_versions AS v ON v.quote_id = q.quote_id
            WHERE {bound_at} IS NOT NULL
              AND datetime(v.quote_version_created_at) <= datetime({bound_at})
              AND {premium} IS NOT NULL
        )
        SELECT AVG(premium) AS average_premium, COUNT(*) AS event_count
        FROM ranked
        WHERE event_rank = 1 AND event_day BETWEEN ? AND ?
    """
    bound_average, bound_count = _average(
        connection, bound_query, start_day, end_day
    )

    policy_link = optional_column(connection, "raw_quotes", "policy_id", "q")
    incepted_query = f"""
        WITH ranked AS (
            SELECT
                p.policy_id,
                date(datetime(p.policy_incepts_at)) AS event_day,
                CAST({premium} AS REAL) AS premium,
                ROW_NUMBER() OVER (
                    PARTITION BY p.policy_id
                    ORDER BY datetime(v.quote_version_created_at) DESC,
                             CAST(v.version_number AS INTEGER) DESC,
                             v.quote_version_id DESC
                ) AS event_rank
            FROM raw_policies AS p
            JOIN raw_quotes AS q ON {policy_link} = p.policy_id
            JOIN raw_quote_versions AS v ON v.quote_id = q.quote_id
            WHERE datetime(v.quote_version_created_at)
                    <= datetime(p.policy_incepts_at)
              AND {premium} IS NOT NULL
        )
        SELECT AVG(premium) AS average_premium, COUNT(*) AS event_count
        FROM ranked
        WHERE event_rank = 1 AND event_day BETWEEN ? AND ?
    """
    incepted_average, incepted_count = _average(
        connection, incepted_query, start_day, end_day
    )

    return {
        "window_start": start_day.isoformat(),
        "window_end": end_day.isoformat(),
        "average_quoted_premium": quoted_average,
        "quoted_quote_count": quoted_count,
        "average_bound_premium": bound_average,
        "bound_quote_count": bound_count,
        "average_incepted_premium": incepted_average,
        "incepted_policy_count": incepted_count,
    }


def overlapping_policy_customers(connection: sqlite3.Connection) -> int:
    """Count customers with overlapping coverage from different policy numbers."""
    termination = optional_column(
        connection, "raw_policies", "policy_terminated_at", "raw_policies"
    )
    row = connection.execute(
        f"""
        WITH effective_periods AS (
            SELECT
                policy_id,
                policy_number,
                customer_id,
                datetime(policy_incepts_at) AS start_at,
                CASE
                    WHEN {termination} IS NOT NULL
                     AND datetime({termination}) < datetime(policy_expires_at)
                    THEN datetime({termination})
                    ELSE datetime(policy_expires_at)
                END AS end_at
            FROM raw_policies
        )
        SELECT COUNT(DISTINCT first.customer_id) AS customer_count
        FROM effective_periods AS first
        JOIN effective_periods AS second
          ON second.customer_id = first.customer_id
         AND second.policy_id > first.policy_id
         AND second.policy_number <> first.policy_number
         AND first.start_at < second.end_at
         AND second.start_at < first.end_at
        """
    ).fetchone()
    return row["customer_count"]


def bind_rate_by_day(
    connection: sqlite3.Connection,
    as_of: date | str | None = None,
    days: int = 30,
) -> list[dict[str, Any]]:
    """Return quote-created cohorts and their bound share for each calendar day."""
    end_day = _as_date(as_of)
    start_day = _window_start(end_day, days)
    bound_at = optional_column(connection, "raw_quotes", "quote_bound_at", "q")
    created_quotes = connection.execute(
        f"""
        WITH quote_creation AS (
            SELECT
                quote_id,
                MIN(datetime(quote_version_created_at)) AS created_at
            FROM raw_quote_versions
            GROUP BY quote_id
        )
        SELECT
            date(quote_creation.created_at) AS created_day,
            date(datetime({bound_at})) AS bound_day
        FROM quote_creation
        LEFT JOIN raw_quotes AS q ON q.quote_id = quote_creation.quote_id
        WHERE date(quote_creation.created_at) BETWEEN ? AND ?
        """,
        (start_day.isoformat(), end_day.isoformat()),
    ).fetchall()

    daily: dict[date, dict[str, int]] = {}
    current_day = start_day
    while current_day <= end_day:
        daily[current_day] = {"quotes_created": 0, "quotes_bound": 0}
        current_day += timedelta(days=1)

    for quote in created_quotes:
        created_day = date.fromisoformat(quote["created_day"])
        daily[created_day]["quotes_created"] += 1
        bound_day_value = quote["bound_day"]
        if bound_day_value and bound_day_value <= end_day.isoformat():
            daily[created_day]["quotes_bound"] += 1

    return [
        {
            "date_day": day.isoformat(),
            "quotes_created": counts["quotes_created"],
            "quotes_bound": counts["quotes_bound"],
            "bind_rate_pct": (
                round(counts["quotes_bound"] * 100.0 / counts["quotes_created"], 2)
                if counts["quotes_created"]
                else None
            ),
        }
        for day, counts in daily.items()
    ]
