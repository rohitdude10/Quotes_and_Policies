data "aws_caller_identity" "current" {}
data "aws_partition" "current" {}
data "aws_region" "current" {}

resource "random_id" "bucket_suffix" {
  byte_length = 4
}

locals {
  name            = var.project_name
  lake_bucket     = "${var.project_name}-${data.aws_caller_identity.current.account_id}-${random_id.bucket_suffix.hex}-lake"
  results_bucket  = "${var.project_name}-${data.aws_caller_identity.current.account_id}-${random_id.bucket_suffix.hex}-athena"
  glue_job_name   = "${var.project_name}-batch"
  glue_database   = replace(var.project_name, "-", "_")
  glue_role_name  = "${var.project_name}-glue-role"
  schedule_role   = "${var.project_name}-scheduler-role"
  crawler_name    = "${var.project_name}-curated-crawler"
}

resource "aws_s3_bucket" "lake" {
  bucket        = local.lake_bucket
  force_destroy = false
}

resource "aws_s3_bucket_public_access_block" "lake" {
  bucket                  = aws_s3_bucket.lake.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_versioning" "lake" {
  bucket = aws_s3_bucket.lake.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "lake" {
  bucket = aws_s3_bucket.lake.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "lake" {
  bucket = aws_s3_bucket.lake.id

  rule {
    id     = "abort-incomplete-multipart-uploads"
    status = "Enabled"

    filter {}

    abort_incomplete_multipart_upload {
      days_after_initiation = 7
    }
  }
}

resource "aws_s3_bucket" "athena_results" {
  bucket        = local.results_bucket
  force_destroy = false
}

resource "aws_s3_bucket_public_access_block" "athena_results" {
  bucket                  = aws_s3_bucket.athena_results.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "athena_results" {
  bucket = aws_s3_bucket.athena_results.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_glue_catalog_database" "quotes_policies" {
  name         = local.glue_database
  description  = "Catalog for Quotes & Policies curated batch data"
  location_uri = "s3://${aws_s3_bucket.lake.bucket}/curated/"
}

data "aws_iam_policy_document" "glue_assume_role" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["glue.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "glue" {
  name               = local.glue_role_name
  assume_role_policy = data.aws_iam_policy_document.glue_assume_role.json
}

data "aws_iam_policy_document" "glue_job" {
  statement {
    sid       = "ListPipelinePrefixes"
    effect    = "Allow"
    actions   = ["s3:ListBucket"]
    resources = [aws_s3_bucket.lake.arn]

    condition {
      test     = "StringLike"
      variable = "s3:prefix"
      values   = ["landing", "landing/*", "curated", "curated/", "curated/*", "tmp", "tmp/*", "scripts", "scripts/*"]
    }
  }

  statement {
    sid       = "ReadInputsAndScript"
    effect    = "Allow"
    actions   = ["s3:GetObject"]
    resources = [
      "${aws_s3_bucket.lake.arn}/landing/*",
      "${aws_s3_bucket.lake.arn}/curated/*",
      "${aws_s3_bucket.lake.arn}/scripts/*",
    ]
  }

  statement {
    sid       = "WriteCuratedAndTemporaryData"
    effect    = "Allow"
    actions   = ["s3:PutObject", "s3:DeleteObject"]
    resources = [
      "${aws_s3_bucket.lake.arn}/curated/*",
      "${aws_s3_bucket.lake.arn}/tmp/*",
    ]
  }

  statement {
    sid    = "GlueCatalogAccess"
    effect = "Allow"
    actions = [
      "glue:GetDatabase",
      "glue:GetDatabases",
      "glue:GetTable",
      "glue:GetTables",
      "glue:CreateTable",
      "glue:UpdateTable",
      "glue:GetPartition",
      "glue:GetPartitions",
      "glue:CreatePartition",
      "glue:BatchCreatePartition",
      "glue:UpdatePartition",
      "glue:BatchUpdatePartition",
    ]
    resources = [
      "arn:${data.aws_partition.current.partition}:glue:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:catalog",
      "arn:${data.aws_partition.current.partition}:glue:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:database/${aws_glue_catalog_database.quotes_policies.name}",
      "arn:${data.aws_partition.current.partition}:glue:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:table/${aws_glue_catalog_database.quotes_policies.name}/*",
    ]
  }

  statement {
    sid    = "WriteGlueJobLogs"
    effect = "Allow"
    actions = [
      "logs:CreateLogGroup",
      "logs:CreateLogStream",
      "logs:PutLogEvents",
    ]
    resources = [
      "arn:${data.aws_partition.current.partition}:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:log-group:/aws-glue/*",
      "arn:${data.aws_partition.current.partition}:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:log-group:/aws-glue/*:log-stream:*",
    ]
  }
}

resource "aws_iam_role_policy" "glue_job" {
  name   = "${local.glue_role_name}-permissions"
  role   = aws_iam_role.glue.id
  policy = data.aws_iam_policy_document.glue_job.json
}

resource "aws_glue_job" "quotes_policies" {
  name         = local.glue_job_name
  role_arn     = aws_iam_role.glue.arn
  glue_version = "4.0"
  worker_type  = var.glue_worker_type
  number_of_workers = var.glue_number_of_workers
  timeout      = 30
  max_retries  = 1

  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.lake.bucket}/${aws_s3_object.glue_script.key}"
    python_version  = "3"
  }

  default_arguments = {
    "--job-language"                     = "python"
    "--enable-metrics"                   = "true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-glue-datacatalog"          = "true"
    "--TempDir"                          = "s3://${aws_s3_bucket.lake.bucket}/tmp/"
    "--LANDING_PATH"                     = "s3://${aws_s3_bucket.lake.bucket}/landing/"
    "--CURATED_PATH"                     = "s3://${aws_s3_bucket.lake.bucket}/curated/"
    "--DATABASE_NAME"                    = aws_glue_catalog_database.quotes_policies.name
  }

  execution_property {
    max_concurrent_runs = 1
  }

  depends_on = [
    aws_iam_role_policy.glue_job,
    aws_s3_object.glue_script,
  ]
}

resource "aws_s3_object" "glue_script" {
  bucket       = aws_s3_bucket.lake.id
  key          = "scripts/quotes_policies_job.py"
  source       = "${path.module}/glue_job.py"
  etag         = filemd5("${path.module}/glue_job.py")
  content_type = "text/x-python"
}

resource "aws_glue_crawler" "curated" {
  name          = local.crawler_name
  database_name = aws_glue_catalog_database.quotes_policies.name
  role          = aws_iam_role.glue.arn

  s3_target {
    path = "s3://${aws_s3_bucket.lake.bucket}/curated/"
  }

  schema_change_policy {
    delete_behavior = "LOG"
    update_behavior = "UPDATE_IN_DATABASE"
  }

  depends_on = [aws_iam_role_policy.glue_job]
}

data "aws_iam_policy_document" "scheduler_assume_role" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["scheduler.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "scheduler" {
  name               = local.schedule_role
  assume_role_policy = data.aws_iam_policy_document.scheduler_assume_role.json
}

data "aws_iam_policy_document" "scheduler_start_glue" {
  statement {
    effect    = "Allow"
    actions   = ["glue:StartJobRun"]
    resources = [aws_glue_job.quotes_policies.arn]
  }
}

resource "aws_iam_role_policy" "scheduler_start_glue" {
  name   = "${local.schedule_role}-start-job"
  role   = aws_iam_role.scheduler.id
  policy = data.aws_iam_policy_document.scheduler_start_glue.json
}

resource "aws_scheduler_schedule" "daily" {
  name                         = "${local.name}-daily"
  description                  = "Start the Quotes & Policies Glue batch"
  schedule_expression          = var.schedule_expression
  schedule_expression_timezone = "UTC"
  state                        = "ENABLED"

  flexible_time_window {
    mode = "OFF"
  }

  target {
    arn      = "arn:${data.aws_partition.current.partition}:scheduler:::aws-sdk:glue:startJobRun"
    role_arn = aws_iam_role.scheduler.arn
    input    = jsonencode({ JobName = aws_glue_job.quotes_policies.name })
  }

  depends_on = [aws_iam_role_policy.scheduler_start_glue]
}

resource "aws_athena_workgroup" "quotes_policies" {
  name = "${local.name}-analytics"

  configuration {
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = true

    result_configuration {
      output_location = "s3://${aws_s3_bucket.athena_results.bucket}/query-results/"
    }
  }
}
