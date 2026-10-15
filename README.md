# Quotes & Policies Data Engineering Assessment

A small, rerunnable Python and SQLite pipeline that ingests the four supplied
CSV feeds, builds reporting tables, and answers the three requested business
questions. It uses only the Python standard library.

## Contents

- [Local setup and execution](#local-setup-and-execution)
  - [Prerequisites](#prerequisites)
  - [Run the pipeline](#run-the-pipeline)
  - [Run tests](#run-tests)
  - [Query historical state](#query-historical-state)
- [Technical approach](#technical-approach)
- [Pipeline stages](#pipeline-stages)
- [Point-in-time record history](#point-in-time-record-history)
- [Reporting models](#reporting-models)
  - [`latest_quote_versions`](#latest_quote_versions)
  - [`active_policies_by_day`](#active_policies_by_day)
  - [`renewal_funnel`](#renewal_funnel)
- [Analytics](#analytics)
- [Assumptions, trade-offs, and decisions](#assumptions-trade-offs-and-decisions)
- [Inspecting the database](#inspecting-the-database)
- [Proposed AWS deployment architecture](#proposed-aws-deployment-architecture)
  - [Services and responsibilities](#services-and-responsibilities)
  - [Batch flow](#batch-flow)
  - [Storage and history choices](#storage-and-history-choices)
- [Terraform infrastructure starter](#terraform-infrastructure-starter)
- [Production considerations](#production-considerations)

## Local setup and execution

### Prerequisites

- Python 3.10 or newer.
- The four input files in `task_datasets\`: `customers.csv`, `quotes.csv`,
  `policies.csv`, and `quote_versions.csv`.
- No third-party Python packages are required. SQLite is included with Python.

### Run the pipeline

Open PowerShell in this project directory (`Quotes_Policies`) and run:

```powershell
python --version
python run_pipeline.py
```

The runner creates/updates `insurance.db` in the project directory, loads the
input snapshots, rebuilds reporting tables, and prints the requested metrics as
JSON. For the bundled snapshot, which ends in June 2025, use its final data date
to get meaningful 90-day and 30-day analytics windows:

```powershell
python run_pipeline.py --as-of 2025-06-24
```

To use a different CSV directory or database location:

```powershell
python run_pipeline.py --dataset-dir "C:\data\quotes-policies" --database "C:\data\insurance.db" --as-of 2025-06-24
```

The pipeline is rerunnable: it loads the current CSVs as a snapshot, updates
record-level history, then rebuilds the reporting models. Keep using the same
database file if you want to retain history across later snapshots.

### Run tests

From the project directory:

```powershell
python -m unittest discover -s tests -v
```

### Query historical state

After at least one successful pipeline run, query the dataset snapshot or one
record known at an earlier ingestion time:

```powershell
python query_history.py --dataset policies --as-of 2025-01-15T12:00:00Z
python query_history.py --dataset policies --as-of 2025-01-15 --key 8916eb43-aae8-43de-b9fa-239bdbc62968
```

The first command returns the entire policy snapshot and the second returns
one policy or `null` if it was not present at that time. The date-only value
means midnight UTC at the start of that date. Timestamps use ISO 8601; a
timestamp without a timezone is treated as UTC. The default analytics date is
today, so the recent windows are empty until newer input data arrives.

The database is generated output and is excluded from version control.

## Technical approach

The implementation uses Python's standard library for CSV parsing, validation,
logging, and orchestration, with SQLite for local raw storage and SQL reporting
models. It separates ingestion, transformations, analytics, and command-line
execution into `src/load_data.py`, `src/models.py`, `src/analytics.py`, and
`run_pipeline.py`. Tests use temporary fixture datasets and in-memory-on-disk
SQLite databases, so they do not alter the supplied CSVs.

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

## Point-in-time record history

Every ingestion compares each natural key and complete row with its current
version in `record_history`. New and changed rows start a version; the prior
version is closed by setting `valid_to` and clearing `is_current`. Records
missing from a later complete-file snapshot are also closed, so an as-of query
shows them before removal and omits them afterward. Unchanged reruns do not
create extra versions. Current raw tables, schema history, and record history
are updated in the same SQLite transaction.

This is **system/knowledge-time history**: `valid_from` means when the
pipeline observed a particular row, not when the change became effective in
the insurance business. The CSV feeds contain no source-effective timestamp
or CDC event time, so business-effective (valid-time) history cannot be
inferred from them. A future feed with such a timestamp can be used to add a
separate valid-time axis for full bitemporal queries.

History starts when this version of the pipeline first loads a snapshot.
Earlier states cannot be reconstructed from an existing database whose raw
tables were previously overwritten. Keep the same database and run the
pipeline for each incoming complete snapshot to retain subsequent changes.

The Python query helpers are `get_record_as_of(connection, dataset, key,
as_of)` and `get_records_as_of(connection, dataset, as_of)` in
`src/load_data.py`.

## Reporting models

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

## Analytics

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

## Assumptions, trade-offs, and decisions

- **Snapshot ingestion:** every set of four CSV files is treated as one
  complete snapshot, not a partial CDC feed. Natural keys identify records.
  Only exact duplicate rows are skipped; conflicting rows sharing a key fail
  the load rather than being silently discarded.
- **Schema evolution:** new columns are added to raw tables and removed
  columns are retained as nullable fields. Required identity and event
  columns are still checked; optional quote lifecycle fields may be absent.
- **Transactions and reruns:** load and history updates are transactional.
  Reporting models are rebuilt from current raw snapshots. This is simple and
  deterministic for this assessment's data volume, but may be expensive for
  much larger feeds.
- **Policy activity:** `policy_incepts_at` starts coverage; expiry and
  termination are end-exclusive, and the earlier timestamp ends coverage.
  Daily activity is a midnight snapshot.
- **Renewal outcomes:** absent binding, termination, or inception events are
  represented as false flags, not proof that a customer declined to renew.
  Quote links and linked policy records are used instead of inferring renewals
  from matching customer IDs alone.
- **Premium windows:** the event's own date (quote creation, bind, or
  inception) determines inclusion in an inclusive 90-calendar-day window.
  For binding and inception, use the latest non-null premium version that
  existed by that event timestamp.
- **Bind rate:** quotes are grouped by their earliest version timestamp
  because the source has no separate quote-created timestamp. A quote counts
  as bound if its binding timestamp is on or before the window end. Recent
  cohorts may have less conversion time; this is not a same-day conversion
  metric.
- **History semantics:** `record_history` captures when the pipeline observed
  row values (system/knowledge time). The source files contain no
  business-effective timestamps, so the pipeline cannot infer when an update
  took effect in the insurance domain. History begins on the first run using
  this implementation; overwritten earlier states cannot be recovered.
- **Local versus production storage:** SQLite is dependency-free and suitable
  for this single-user assessment workflow. It is not used as a shared
  distributed production database; the AWS proposal stores analytical
  datasets in S3 and queries them with Athena.

## Inspecting the database

The raw tables are `raw_customers`, `raw_quotes`, `raw_policies`, and
`raw_quote_versions`. The three reporting tables are
`latest_quote_versions`, `active_policies_by_day`, and `renewal_funnel`.
`raw_schema_registry` holds the currently observed schemas, while
`raw_schema_history` records each load and schema change. `record_history`
holds each row version and its system-time validity interval.

## Proposed AWS deployment architecture

For this batch workload, use a serverless S3 data lake with AWS Glue for
cataloging and transformations, Athena for ad hoc SQL, and EventBridge for
scheduling. This keeps the pipeline close to the low-volume CSV workflow while
avoiding a long-running server or a shared SQLite file.

### Services and responsibilities

| Service | Responsibility |
| --- | --- |
| **Amazon S3** | Store incoming CSVs in a versioned landing bucket/prefix, partitioned by source and ingestion date. Keep the original files immutable for replay and audit; use separate `raw/`, `curated/`, and `artifacts/` prefixes. |
| **AWS Transfer Family or source-specific delivery** | Optional secure file transfer when the provider cannot upload directly to S3. For application event feeds, use an API or streaming ingestion service instead of pretending the files are continuously updated. |
| **AWS Glue Data Catalog + Crawlers** | Register datasets and partitions so Athena and Glue jobs can discover them. Prefer explicit schemas and reviewed schema-change handling for production rather than relying solely on crawler inference. |
| **AWS Glue ETL (Python/Spark)** | Validate required fields, deduplicate, check relationships, build latest-quote and renewal models, and calculate policy daily counts. Write partitioned Parquet to `curated/`. Persist record versions with `valid_from`, `valid_to`, and `is_current` based on ingestion time; preserve the source files for reprocessing. |
| **AWS Step Functions** | Orchestrate the batch: discover a new manifest/object, run validation and transformation, publish outputs only after successful checks, and route failures for retry or investigation. For a simple scheduled single job, Glue Workflows or a direct EventBridge-to-Glue trigger is sufficient. |
| **Amazon EventBridge** | Start a scheduled batch (for example, daily) or react to an S3 object-created event. Use an S3 event notification through EventBridge for event-driven arrival; use a schedule when feeds arrive on a predictable cadence. |
| **Amazon Athena** | Query curated Parquet, calculate the requested averages and bind rates, and perform record-history as-of queries. Store Athena query results in a separate S3 results prefix. |
| **Amazon QuickSight (optional)** | Build dashboards from Athena datasets if business users need self-service reporting. |
| **Amazon CloudWatch + SNS** | Centralize Glue/Step Functions logs and metrics, alarm on failed runs, data-quality failures, and stale inputs, and notify the on-call/data team. Include run IDs and source object keys in structured logs. |
| **AWS IAM, KMS, Secrets Manager, Lake Formation** | Apply least-privilege roles to each job, encrypt S3 objects and Athena results with KMS, keep external credentials in Secrets Manager, and govern table/column access with Lake Formation where required. |

### Batch flow

1. The provider uploads a dated CSV batch to the S3 `landing/` prefix. Enable
   bucket versioning, encryption, and lifecycle policies; record an ingestion
   manifest with object keys, checksums, and source timestamps.
2. EventBridge starts a Step Functions workflow (or scheduled Glue workflow).
   The workflow validates that all four expected files are present, then runs
   a Glue job with the manifest and run ID.
3. The Glue job validates schema, identifiers, timestamps, duplicate keys,
   and cross-file references. It writes a quality report and fails the run
   explicitly on invalid/conflicting records rather than publishing partial
   reporting tables.
4. On success, write the immutable raw snapshot and transformed Parquet to
   distinct S3 prefixes, partitioned by ingestion date. Maintain versioned
   records for each natural key and close the previous system-time version
   when a changed or removed record is observed. Publish a run manifest only
   after all outputs are complete; consumers should read only published runs.
5. Update the Glue Catalog, then query with Athena. Athena can select the
   version whose interval contains the requested as-of timestamp:

   ```sql
   SELECT *
   FROM record_history
   WHERE dataset = 'policies'
     AND valid_from <= TIMESTAMP '2025-01-15 12:00:00'
     AND (valid_to IS NULL OR TIMESTAMP '2025-01-15 12:00:00' < valid_to);
   ```

6. CloudWatch captures job and workflow logs/metrics. Alarm on workflow
   failure, missing expected feed, schema drift requiring review, and unusual
   row-count changes. Step Functions can retry transient service failures;
   malformed input should be quarantined to a `rejected/` prefix and reported,
   not silently retried indefinitely.

### Storage and history choices

- Keep original CSV objects immutable in S3 for audit and deterministic
  reprocessing. Use Glue bookmarks only when incremental file discovery is
  desirable; an explicit manifest/run ID is usually clearer for four related
  files that must be processed as one consistent snapshot.
- Prefer Parquet for curated analytical data to reduce Athena scan cost. Store
  ingestion/run metadata alongside rows so a complete snapshot can be
  reconstructed.
- The current local history captures **system/knowledge time** (when a batch
  was observed). The feeds do not contain business-effective timestamps, so
  AWS cannot infer valid-time history either; add a separate effective-time
  field when the source can provide it.
- Do not place a shared SQLite database on S3 or EFS for concurrent analytics.
  Athena over S3 is a good fit for this assessment's batch reporting. Choose
  **Amazon Redshift** (or Redshift Serverless) if workloads require frequent
  low-latency joins, high concurrency, governed dimensional models, or
  predictable warehouse performance. Use **DynamoDB** only if a separate
  application needs low-latency point lookups by record key; it is not a
  replacement for the analytical lake.

## Production considerations

Keep Glue workers and Athena queries scoped to partitions and columns, compact
small output files, and configure S3 lifecycle/retention policies. Set
timeouts, bounded retries, and concurrency limits in orchestration. Make
transforms idempotent by run ID and natural key; publish outputs atomically
(or through a table format such as Apache Iceberg when transactional updates
and snapshot evolution are needed). Add data-quality thresholds and a
reconciliation step comparing input, accepted, rejected, and output counts.

This is a proposed architecture, not deployed infrastructure. SQLite remains
appropriate for the local assessment and tests; S3 plus Glue and Athena is the
recommended initial AWS path for the batch scale described here.

## Terraform infrastructure starter

`infrastructure/terraform/` contains a deployable starter configuration for
the storage, Glue, scheduler, and Athena components described above. It
creates:

- A versioned, public-access-blocked, SSE-S3 encrypted S3 data lake and a
  separate Athena results bucket.
- A least-privilege Glue job role, Glue Data Catalog database, Glue ETL job,
  and a crawler that can register the curated Parquet outputs.
- An EventBridge Scheduler schedule that starts the Glue job daily at 05:00
  UTC by default.
- An Athena workgroup that enforces the configured query-results location.

The included `glue_job.py` is an **infrastructure smoke-test starter**: it
checks for the required CSVs and columns, then stages each feed to Parquet
under an ingestion-date partition. The complete local Python/SQLite models,
history behavior, and business analytics have not yet been ported to Glue
Spark; productionize those transforms and their data-quality checks before
using this job as the assessment's full AWS pipeline.

Prerequisites: Terraform 1.5+, AWS credentials configured for the target
account, and permission to create the listed resources and IAM roles. From the
project root:

```powershell
Set-Location infrastructure/terraform
terraform init
terraform fmt -check
terraform validate
terraform plan
terraform apply
```

After approval, Terraform uploads `glue_job.py` and creates the job resources.
Upload a complete batch using the expected names:

```powershell
$lakeBucket = terraform output -raw lake_bucket_name
aws s3 cp ..\..\task_datasets\customers.csv "s3://$lakeBucket/landing/customers.csv"
aws s3 cp ..\..\task_datasets\quotes.csv "s3://$lakeBucket/landing/quotes.csv"
aws s3 cp ..\..\task_datasets\policies.csv "s3://$lakeBucket/landing/policies.csv"
aws s3 cp ..\..\task_datasets\quote_versions.csv "s3://$lakeBucket/landing/quote_versions.csv"
```

Start an initial run and refresh the Glue Catalog crawler after the job
succeeds:

```powershell
$jobName = terraform output -raw glue_job_name
$crawlerName = terraform output -raw glue_crawler_name
aws glue start-job-run --job-name $jobName
aws glue start-crawler --name $crawlerName
```

The daily schedule invokes the ETL job but intentionally does not automatically
start the crawler; trigger the crawler after successful completion or add
success-dependent orchestration (for example, Step Functions) for production.
Run `terraform destroy` when finished to remove the resources. S3 buckets are
configured with `force_destroy = false`, so empty both buckets first; this
protects retained input and query-result data from accidental deletion.

Terraform state is local by default in this starter. For a team or production
deployment, configure a remote encrypted S3 backend with locking and restricted
access before applying; do not commit state files or `terraform.tfvars`.
