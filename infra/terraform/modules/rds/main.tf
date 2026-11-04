# RDS PostgreSQL Single-AZ: o Aurora tem piso de custo bem maior e, no MVP, o RDS basta.
# A senha vai para o SSM Parameter Store (SecureString, gratuito no tier standard) em vez do
# Secrets Manager (US$ 0,40/segredo/mês).
resource "random_password" "master" {
  length  = 32
  special = false
}

resource "aws_ssm_parameter" "db_password" {
  name  = "/${var.name}/db/password"
  type  = "SecureString"
  value = random_password.master.result
}

resource "aws_db_subnet_group" "this" {
  name       = var.name
  subnet_ids = var.subnet_ids
}

resource "aws_db_parameter_group" "this" {
  name   = var.name
  family = "postgres17"

  parameter {
    name  = "log_min_duration_statement"
    value = "500"
  }
}

resource "aws_db_instance" "this" {
  identifier                   = var.name
  engine                       = "postgres"
  engine_version               = "17"
  instance_class               = var.instance_class
  allocated_storage            = 20
  storage_type                 = "gp3"
  storage_encrypted            = true
  db_name                      = "auction"
  username                     = "auction"
  password                     = random_password.master.result
  db_subnet_group_name         = aws_db_subnet_group.this.name
  parameter_group_name         = aws_db_parameter_group.this.name
  vpc_security_group_ids       = [var.security_group_id]
  multi_az                     = var.multi_az
  publicly_accessible          = false
  backup_retention_period      = var.backup_retention_days
  performance_insights_enabled = false
  auto_minor_version_upgrade   = true
  apply_immediately            = true
  # Ambiente de demo: destroy sem snapshot final, para não deixar custo parado.
  skip_final_snapshot = true
  deletion_protection = false
}

variable "name" {
  type = string
}

variable "subnet_ids" {
  type = list(string)
}

variable "security_group_id" {
  type = string
}

variable "instance_class" {
  type    = string
  default = "db.t4g.micro"
}

variable "multi_az" {
  type    = bool
  default = false
}

variable "backup_retention_days" {
  type    = number
  default = 1
}

output "endpoint" {
  value = aws_db_instance.this.address
}

output "identifier" {
  value = aws_db_instance.this.identifier
}

output "password_parameter_arn" {
  value = aws_ssm_parameter.db_password.arn
}
