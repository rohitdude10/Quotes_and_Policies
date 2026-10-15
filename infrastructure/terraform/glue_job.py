"""Starter Glue job: validate the four input CSVs and stage Parquet datasets."""

from __future__ import annotations

import sys
from datetime import datetime, timezone

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql.functions import lit

args = getResolvedOptions(
    sys.argv,
    ["JOB_NAME", "LANDING_PATH", "CURATED_PATH", "DATABASE_NAME"],
)

required_columns = {
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

sc = SparkContext.getOrCreate()
glue_context = GlueContext(sc)
spark = glue_context.spark_session
job = Job(glue_context)
job.init(args["JOB_NAME"], args)

landing_path = args["LANDING_PATH"].rstrip("/")
curated_path = args["CURATED_PATH"].rstrip("/")
ingestion_date = datetime.now(timezone.utc).date().isoformat()

for dataset, required in required_columns.items():
    source_path = f"{landing_path}/{dataset}.csv"
    frame = (
        spark.read.option("header", "true")
        .option("mode", "FAILFAST")
        .csv(source_path)
    )
    actual_columns = set(frame.columns)
    missing = sorted(required - actual_columns)
    if missing:
        raise ValueError(f"{dataset}.csv is missing required columns: {missing}")

    key_column = {
        "customers": "customer_id",
        "quotes": "quote_id",
        "policies": "policy_id",
        "quote_versions": "quote_version_id",
    }[dataset]
    if frame.filter(frame[key_column].isNull()).limit(1).count():
        raise ValueError(f"{dataset}.csv contains an empty {key_column}")

    output_path = (
        f"{curated_path}/{dataset}/ingestion_date={ingestion_date}"
    )
    (
        frame.dropDuplicates()
        .withColumn("ingestion_date", lit(ingestion_date))
        .write.mode("overwrite")
        .parquet(output_path)
    )
    print(
        f"Staged dataset={dataset} input={source_path} output={output_path}"
    )

job.commit()
