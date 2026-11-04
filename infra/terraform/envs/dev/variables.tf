variable "region" {
  type    = string
  default = "us-east-1"
}

variable "project" {
  type    = string
  default = "leilao"
}

variable "env" {
  type    = string
  default = "dev"
}

variable "owner" {
  type = string
}

variable "cost_center" {
  type    = string
  default = "desafio-tecnico"
}

variable "alert_email" {
  description = "Recebe alarmes (SNS) e alertas de custo (Budgets)"
  type        = string
}

variable "image_tag" {
  description = "Tag da imagem no ECR (scripts/build-push.sh imprime a tag gerada)"
  type        = string
}

variable "certificate_arn" {
  description = "Certificado ACM para HTTPS; null serve só HTTP (demo)"
  type        = string
  default     = null
}

variable "redis_node_type" {
  type    = string
  default = "cache.t4g.micro"
}

variable "redis_node_count" {
  description = "1 = MVP; 2 = réplica com failover automático Multi-AZ"
  type        = number
  default     = 1
}

variable "db_instance_class" {
  type    = string
  default = "db.t4g.micro"
}

variable "db_multi_az" {
  type    = bool
  default = false
}

variable "min_tasks" {
  type    = number
  default = 2
}

variable "max_tasks" {
  type    = number
  default = 10
}

variable "on_demand_base" {
  type    = number
  default = 2
}

variable "spot_enabled" {
  type    = bool
  default = true
}

variable "monthly_budget_usd" {
  type    = number
  default = 150
}
