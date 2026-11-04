terraform {
  required_version = ">= 1.10"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.7"
    }
  }

  # Configuração parcial: bucket/region vêm de backend.hcl (ver backend.hcl.example).
  #   terraform init -backend-config=backend.hcl
  backend "s3" {
    key          = "desafio-itau/dev/terraform.tfstate"
    encrypt      = true
    use_lockfile = true
  }
}

provider "aws" {
  region = var.region

  default_tags {
    tags = {
      project     = var.project
      env         = var.env
      owner       = var.owner
      cost-center = var.cost_center
      managed-by  = "terraform"
    }
  }
}
