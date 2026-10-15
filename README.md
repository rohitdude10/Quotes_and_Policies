# Quotes & Policies Data Engineering Assessment

A small, rerunnable Python and SQLite pipeline that ingests the four supplied
CSV feeds, builds reporting tables, and answers the three requested business
questions. It uses only the Python standard library.

## Run it

From this directory, use Python 3.10 or newer:

```powershell
python run_pipeline.py
python -m unittest discover -s tests -v
```

By default, the runner reads `task_datasets\` and writes `insurance.db` in this
directory. The database is generated output and is excluded from version
control. Override the input/database location or the analytics end date with:

```powershell
python run_pipeline.py --dataset-dir "path\to\csvs" --database "path\to\db.sqlite" --as-of 2025-06-24
```

The CSV snapshots currently end in June 2025. The default analytics date is
today, so the literal last-90-day and last-30-day windows will be empty until
new data arrives. Use `--as-of 2025-06-24` to inspect the supplied dataset's
latest complete period.

## Pipeline stages

1. **Load raw snapshots** — `src/load_data.py` validates required fields,
   timestamps, version numbers, and premiums; removes only exact duplicate
   rows; rejects conflicting repeated natural keys; and loads all four raw
   tables in one transaction.
2. **Track schema changes** — newly added CSV columns are added to raw SQLite
   tables. Removed columns are retained as nullable columns and noted in
   `raw_schema_history`. Core identifier and event columns remain required;
   quote lifecycle fields that can legitimately be absent are treated as
   optional.
3. **Build reporting models** — `src/models.py` replaces all three reporting
   tables atomically while preserving historical versions in the raw tables.
4. **Calculate analytics** — `src/analytics.py` runs the average premium,
   overlapping-customer, and daily bind-rate queries.

The pipeline can be safely run again: raw tables are replaced with the
validated current file snapshots, and reporting models are rebuilt from those
tables. Schema history intentionally records one observation per run.

## Reporting models and assumptions

### `latest_quote_versions`

One row per quote, ranked by numeric `version_number` descending, then
`quote_version_created_at` descending and `quote_version_id` descending for
deterministic tie-breaking. Customer and lifecycle fields are joined from
`quotes.csv`; premium, status, and version timestamp come from
`quote_versions.csv`.

### `active_policies_by_day`

The date range runs from the earliest policy inception date through the latest
policy expiry date represented in the feed. A policy counts as active at the
start of a calendar day when its inception date is on or before that date and
its effective end date is after that date. Expiry and termination are
end-exclusive; the earlier timestamp determines the effective end. Daily
counts are therefore a midnight snapshot, and a day with no active policies
is still included.

### `renewal_funnel`

An eligible renewal is a quote with a `renewal_quote_id` linked to a renewal
quote and associated to an original policy through `quotes.policy_id`. The
renewal creation timestamp is its earliest version timestamp. Cancellation is
true only when the original policy termination timestamp is later than that
creation timestamp. Editing means more than one distinct version number;
binding uses the renewal quote's non-null `quote_bound_at`; inception uses a
linked renewed policy's non-null `policy_incepts_at`. Missing events remain
false flags and should not be interpreted as proof of customer intent.

## Analytics definitions

- **Average quoted premium:** first version with `quote_status = 'QUOTE'` and
  a non-null premium, grouped by the date it was created.
- **Average bound premium:** last non-null premium version available at or
  before the quote's binding timestamp.
- **Average incepted premium:** last non-null premium version available at or
  before the linked policy's inception timestamp.
- All premium averages use their respective event dates within an inclusive
  N-calendar-day window ending on `--as-of`. Event versions created after
  binding/inception are never used for those metrics.
- **Overlapping policy customers:** customers with at least one pair of
  overlapping effective policy periods for different `policy_number` values.
  Policy years sharing a policy number are treated as the same contract chain
  and do not by themselves qualify.
- **Daily bind rate:** each quote is assigned to the date of its earliest
  quote-version timestamp, since the source has no separate quote-created
  timestamp. The numerator counts those cohort quotes with a binding timestamp
  on or before `--as-of`, including later binds. This is a quote-creation
  cohort rate (not a same-day bind rate); recent cohorts may not have had as
  much time to convert. All dates in the 30-day window are returned.
  `bind_rate_pct` is `NULL` when no quotes were created that day.

## Inspecting the database

The raw tables are `raw_customers`, `raw_quotes`, `raw_policies`, and
`raw_quote_versions`. The three reporting tables are
`latest_quote_versions`, `active_policies_by_day`, and `renewal_funnel`.
`raw_schema_registry` holds the currently observed schemas, while
`raw_schema_history` records each load and schema change.

For production, the same batch stages could be scheduled with EventBridge and
run on AWS Glue, ECS, or AWS Batch. S3 is suitable for immutable CSV landing
and Athena for querying the transformed datasets; use Redshift if the workload
needs a dedicated shared warehouse. SQLite is only the local assessment
database, not a distributed production store.
