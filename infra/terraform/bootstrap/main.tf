# Cria o bucket do state remoto. Roda uma vez, com state local:
#   terraform -chdir=infra/terraform/bootstrap init && terraform -chdir=infra/terraform/bootstrap apply
# O lock usa o arquivo .tflock no próprio S3 (use_lockfile, TF >= 1.10), sem DynamoDB.
terraform {
  required_version = ">= 1.10"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0"
    }
  }
}

variable "region" {
  type    = string
  default = "us-east-1"
}

variable "bucket_name" {
  description = "Nome globalmente único do bucket de state"
  type        = string
}

provider "aws" {
  region = var.region
  default_tags {
    tags = {
      project    = "desafio-itau-leilao"
      env        = "shared"
      owner      = "mblopes"
      managed-by = "terraform"
    }
  }
}

resource "aws_s3_bucket" "state" {
  bucket = var.bucket_name
}

resource "aws_s3_bucket_versioning" "state" {
  bucket = aws_s3_bucket.state.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "state" {
  bucket = aws_s3_bucket.state.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "state" {
  bucket                  = aws_s3_bucket.state.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# FinOps: versões antigas do state não ficam acumulando para sempre.
resource "aws_s3_bucket_lifecycle_configuration" "state" {
  bucket = aws_s3_bucket.state.id
  rule {
    id     = "expire-old-versions"
    status = "Enabled"
    filter {}
    noncurrent_version_expiration {
      noncurrent_days = 30
    }
  }
}

output "bucket" {
  value = aws_s3_bucket.state.bucket
}
