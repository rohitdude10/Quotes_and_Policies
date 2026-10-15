variable "aws_region" {
  description = "AWS region for the pipeline resources."
  type        = string
  default     = "us-east-1"
}

variable "project_name" {
  description = "Short lowercase project identifier used in AWS resource names."
  type        = string
  default     = "quotes-policies"

  validation {
    condition     = can(regex("^[a-z0-9-]{3,30}$", var.project_name))
    error_message = "project_name must contain only lowercase letters, digits, and hyphens."
  }
}

variable "schedule_expression" {
  description = "EventBridge Scheduler expression. Schedules use UTC."
  type        = string
  default     = "cron(0 5 * * ? *)"
}

variable "glue_worker_type" {
  description = "Glue worker class; G.1X is sufficient for a small assessment feed."
  type        = string
  default     = "G.1X"
}

variable "glue_number_of_workers" {
  description = "Number of Glue workers."
  type        = number
  default     = 2

  validation {
    condition     = var.glue_number_of_workers >= 2
    error_message = "Glue requires at least two workers for this job configuration."
  }
}
