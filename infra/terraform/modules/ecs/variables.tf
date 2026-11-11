variable "name" {
  type = string
}

variable "image" {
  description = "URI completa da imagem (ECR) com tag"
  type        = string
}

variable "adot_image" {
  type    = string
  default = "public.ecr.aws/aws-observability/aws-otel-collector:v0.43.3"
}

variable "app_port" {
  type    = number
  default = 8080
}

# 1 vCPU / 2 GB: um processo Python (asyncio) usa até um núcleo. Medido em teste de carga:
# ~2.200 lances/s por task de 1 vCPU contra ~1.000/s com 0,5 vCPU, que era throttled pela cota do cgroup.
# Custo por lance igual ou menor e metade das tasks. 2 GB é o mínimo do Fargate para 1 vCPU (usa ~80 MB).
variable "cpu" {
  type    = number
  default = 1024
}

variable "memory" {
  type    = number
  default = 2048
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
  description = "Tasks sempre on-demand; o excedente vai para Spot"
  type        = number
  default     = 2
}

variable "spot_enabled" {
  type    = bool
  default = true
}

variable "requests_per_target" {
  description = "Requests por minuto por task antes de escalar: ~70% dos ~2.200/s medidos por task"
  type        = number
  default     = 90000
}

variable "subnet_ids" {
  type = list(string)
}

variable "security_group_id" {
  type = string
}

variable "target_group_arn" {
  type = string
}

variable "alb_arn_suffix" {
  type = string
}

variable "target_group_arn_suffix" {
  type = string
}

variable "db_endpoint" {
  type = string
}

variable "db_password_parameter_arn" {
  type = string
}

variable "redis_endpoint" {
  type = string
}

variable "cognito_user_pool_id" {
  type = string
}

variable "cognito_user_pool_arn" {
  type = string
}

variable "cognito_client_id" {
  type = string
}

variable "log_retention_days" {
  type    = number
  default = 7
}

variable "trace_sampling_ratio" {
  type    = number
  default = 0.05
}
