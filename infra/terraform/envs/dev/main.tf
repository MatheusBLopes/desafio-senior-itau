# Arquitetura básica (MVP). Redundância e DR estão documentados no README, com o caminho para
# ativar: ElastiCache node_count = 2, RDS multi_az = true, min_tasks em 2+ AZs (já é o padrão).
locals {
  name = "${var.project}-${var.env}"
}

module "network" {
  source = "../../modules/network"
  name   = local.name
}

module "ecr" {
  source = "../../modules/ecr"
  name   = "${local.name}/bid-service"
}

module "alb" {
  source            = "../../modules/alb"
  name              = local.name
  vpc_id            = module.network.vpc_id
  subnet_ids        = module.network.public_subnet_ids
  security_group_id = module.network.alb_sg_id
  certificate_arn   = var.certificate_arn
}

module "elasticache" {
  source            = "../../modules/elasticache"
  name              = local.name
  subnet_ids        = module.network.private_subnet_ids
  security_group_id = module.network.redis_sg_id
  node_type         = var.redis_node_type
  node_count        = var.redis_node_count
}

module "rds" {
  source            = "../../modules/rds"
  name              = local.name
  subnet_ids        = module.network.private_subnet_ids
  security_group_id = module.network.db_sg_id
  instance_class    = var.db_instance_class
  multi_az          = var.db_multi_az
}

module "ecs" {
  source                    = "../../modules/ecs"
  name                      = local.name
  image                     = "${module.ecr.repository_url}:${var.image_tag}"
  subnet_ids                = module.network.public_subnet_ids
  security_group_id         = module.network.app_sg_id
  target_group_arn          = module.alb.target_group_arn
  alb_arn_suffix            = module.alb.arn_suffix
  target_group_arn_suffix   = module.alb.target_group_arn_suffix
  db_endpoint               = module.rds.endpoint
  db_password_parameter_arn = module.rds.password_parameter_arn
  redis_endpoint            = module.elasticache.primary_endpoint
  min_tasks                 = var.min_tasks
  max_tasks                 = var.max_tasks
  on_demand_base            = var.on_demand_base
  spot_enabled              = var.spot_enabled
}

module "observability" {
  source                     = "../../modules/observability"
  name                       = local.name
  alert_email                = var.alert_email
  alb_arn_suffix             = module.alb.arn_suffix
  cluster_name               = module.ecs.cluster_name
  service_name               = module.ecs.service_name
  redis_replication_group_id = module.elasticache.replication_group_id
  db_identifier              = module.rds.identifier
}

module "budget" {
  source            = "../../modules/budget"
  name              = local.name
  project_tag       = var.project
  monthly_limit_usd = var.monthly_budget_usd
  alert_email       = var.alert_email
}
