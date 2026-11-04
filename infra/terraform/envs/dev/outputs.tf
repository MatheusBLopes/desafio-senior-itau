output "alb_url" {
  value = "http://${module.alb.dns_name}"
}

output "ecr_repository_url" {
  value = module.ecr.repository_url
}

output "dashboard_url" {
  value = module.observability.dashboard_url
}

output "log_group" {
  value = module.ecs.log_group_name
}

# Lidos por scripts/aws-pause.sh e scripts/aws-resume.sh.
output "ecs_cluster_name" {
  value = module.ecs.cluster_name
}

output "ecs_service_name" {
  value = module.ecs.service_name
}

output "db_identifier" {
  value = module.rds.identifier
}

output "cognito_user_pool_id" {
  value = module.cognito.user_pool_id
}

output "cognito_client_id" {
  value = module.cognito.client_id
}
