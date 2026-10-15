output "landing_s3_uri" {
  description = "Upload the four CSVs to this prefix as one complete batch."
  value       = "s3://${aws_s3_bucket.lake.bucket}/landing/"
}

output "lake_bucket_name" {
  description = "S3 bucket holding landing, scripts, temporary, and curated data."
  value       = aws_s3_bucket.lake.bucket
}

output "athena_results_bucket_name" {
  description = "S3 bucket holding Athena query results."
  value       = aws_s3_bucket.athena_results.bucket
}

output "curated_s3_uri" {
  description = "Glue output prefix for partitioned Parquet datasets."
  value       = "s3://${aws_s3_bucket.lake.bucket}/curated/"
}

output "glue_job_name" {
  description = "Glue ETL job name."
  value       = aws_glue_job.quotes_policies.name
}

output "glue_database_name" {
  description = "Glue Data Catalog database name."
  value       = aws_glue_catalog_database.quotes_policies.name
}

output "glue_crawler_name" {
  description = "Run this crawler after a successful Glue job to refresh tables."
  value       = aws_glue_crawler.curated.name
}

output "athena_workgroup" {
  description = "Athena workgroup with encrypted S3 query results."
  value       = aws_athena_workgroup.quotes_policies.name
}

output "athena_results_s3_uri" {
  description = "Athena query-results prefix."
  value       = "s3://${aws_s3_bucket.athena_results.bucket}/query-results/"
}
