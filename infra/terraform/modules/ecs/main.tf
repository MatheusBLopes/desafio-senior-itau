# ECS Fargate ARM64 (Graviton, ~20% mais barato que x86). Capacity provider misto: a base é
# on-demand (uma interrupção Spot nos segundos finais derrubaria WebSockets no pico) e o excedente
# do auto scaling vai para Fargate Spot (até ~70% mais barato).
data "aws_region" "current" {}

resource "aws_ecs_cluster" "this" {
  name = var.name

  # Container Insights cobra por métrica; as métricas nativas do ECS bastam para o MVP.
  setting {
    name  = "containerInsights"
    value = "disabled"
  }
}

resource "aws_ecs_cluster_capacity_providers" "this" {
  cluster_name       = aws_ecs_cluster.this.name
  capacity_providers = ["FARGATE", "FARGATE_SPOT"]
}

resource "aws_cloudwatch_log_group" "app" {
  name              = "/ecs/${var.name}"
  retention_in_days = var.log_retention_days
}

# --- IAM ---------------------------------------------------------------------------------------

data "aws_iam_policy_document" "assume_ecs_tasks" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["ecs-tasks.amazonaws.com"]
    }
  }
}

# Execution role: puxar imagem, escrever logs e ler o segredo do banco.
resource "aws_iam_role" "execution" {
  name               = "${var.name}-execution"
  assume_role_policy = data.aws_iam_policy_document.assume_ecs_tasks.json
}

resource "aws_iam_role_policy_attachment" "execution" {
  role       = aws_iam_role.execution.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy"
}

resource "aws_iam_role_policy" "execution_ssm" {
  name = "read-db-password"
  role = aws_iam_role.execution.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["ssm:GetParameters"]
      Resource = [var.db_password_parameter_arn]
    }]
  })
}

# Task role: o que a aplicação e o sidecar ADOT fazem em runtime.
resource "aws_iam_role" "task" {
  name               = "${var.name}-task"
  assume_role_policy = data.aws_iam_policy_document.assume_ecs_tasks.json
}

resource "aws_iam_role_policy" "task" {
  name = "metrics-and-traces"
  role = aws_iam_role.task.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect    = "Allow"
        Action    = ["cloudwatch:PutMetricData"]
        Resource  = "*"
        Condition = { StringEquals = { "cloudwatch:namespace" = "Auction" } }
      },
      {
        Effect   = "Allow"
        Action   = ["xray:PutTraceSegments", "xray:PutTelemetryRecords", "xray:GetSamplingRules", "xray:GetSamplingTargets"]
        Resource = "*"
      },
      {
        # Confirmar o cadastro na hora, sem e-mail. Só essa ação administrativa, só neste user pool.
        Effect   = "Allow"
        Action   = ["cognito-idp:AdminConfirmSignUp"]
        Resource = var.cognito_user_pool_arn
      }
    ]
  })
}

# --- Task definition ---------------------------------------------------------------------------

locals {
  app_environment = {
    DB_HOST    = var.db_endpoint
    DB_USER    = "auction"
    REDIS_HOST = var.redis_endpoint
    REDIS_TLS  = "true"
    AWS_REGION = data.aws_region.current.region
    JSON_LOGS  = "true"
    # Autenticação: access token (JWT) do Cognito em Authorization: Bearer.
    AUTH_MODE            = "cognito"
    COGNITO_USER_POOL_ID = var.cognito_user_pool_id
    COGNITO_CLIENT_ID    = var.cognito_client_id
    # Métricas de negócio (namespace Auction) via Embedded Metric Format: linhas JSON no stdout que o
    # CloudWatch Logs transforma em métricas, sem chamadas PutMetricData.
    CLOUDWATCH_EMF = "true"
    # Traces: auto-instrumentação OpenTelemetry → sidecar ADOT → X-Ray, amostrando 5% (FinOps).
    OTEL_ENABLED                = "true"
    OTEL_SERVICE_NAME           = "bid-service"
    OTEL_EXPORTER_OTLP_ENDPOINT = "http://localhost:4317"
    OTEL_EXPORTER_OTLP_PROTOCOL = "grpc"
    OTEL_TRACES_EXPORTER        = "otlp"
    OTEL_TRACES_SAMPLER         = "parentbased_traceidratio"
    OTEL_TRACES_SAMPLER_ARG     = tostring(var.trace_sampling_ratio)
    OTEL_PROPAGATORS            = "tracecontext,baggage,xray"
    OTEL_PYTHON_ID_GENERATOR    = "xray"
    OTEL_METRICS_EXPORTER       = "none"
    OTEL_LOGS_EXPORTER          = "none"
    # trace_id e span_id em cada linha de log (correlação log ↔ trace no X-Ray).
    OTEL_PYTHON_LOG_CORRELATION = "true"
  }
}

resource "aws_ecs_task_definition" "app" {
  family                   = var.name
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = var.cpu
  memory                   = var.memory
  execution_role_arn       = aws_iam_role.execution.arn
  task_role_arn            = aws_iam_role.task.arn

  runtime_platform {
    operating_system_family = "LINUX"
    cpu_architecture        = "ARM64"
  }

  container_definitions = jsonencode([
    {
      name         = "app"
      image        = var.image
      essential    = true
      portMappings = [{ containerPort = var.app_port, protocol = "tcp" }]
      environment  = [for k, v in local.app_environment : { name = k, value = v }]
      secrets      = [{ name = "DB_PASSWORD", valueFrom = var.db_password_parameter_arn }]
      stopTimeout  = 30
      logConfiguration = {
        logDriver = "awslogs"
        options = {
          awslogs-group         = aws_cloudwatch_log_group.app.name
          awslogs-region        = data.aws_region.current.region
          awslogs-stream-prefix = "app"
        }
      }
      dependsOn = [{ containerName = "adot", condition = "START" }]
    },
    {
      # Sidecar consome parte do CPU/memória da task: entra no dimensionamento e no custo.
      name      = "adot"
      image     = var.adot_image
      essential = false
      command   = ["--config=/etc/ecs/ecs-default-config.yaml"]
      logConfiguration = {
        logDriver = "awslogs"
        options = {
          awslogs-group         = aws_cloudwatch_log_group.app.name
          awslogs-region        = data.aws_region.current.region
          awslogs-stream-prefix = "adot"
        }
      }
    }
  ])
}

# --- Service + auto scaling --------------------------------------------------------------------

resource "aws_ecs_service" "app" {
  name                               = var.name
  cluster                            = aws_ecs_cluster.this.id
  task_definition                    = aws_ecs_task_definition.app.arn
  desired_count                      = var.min_tasks
  health_check_grace_period_seconds  = 60
  deployment_minimum_healthy_percent = 100
  deployment_maximum_percent         = 200

  capacity_provider_strategy {
    capacity_provider = "FARGATE"
    base              = var.on_demand_base
    weight            = var.spot_enabled ? 1 : 100
  }

  dynamic "capacity_provider_strategy" {
    for_each = var.spot_enabled ? [1] : []
    content {
      capacity_provider = "FARGATE_SPOT"
      weight            = 1
    }
  }

  network_configuration {
    subnets          = var.subnet_ids
    security_groups  = [var.security_group_id]
    assign_public_ip = true # sem NAT: IP público para alcançar ECR/CloudWatch/X-Ray; o SG só aceita o ALB
  }

  load_balancer {
    target_group_arn = var.target_group_arn
    container_name   = "app"
    container_port   = var.app_port
  }

  deployment_circuit_breaker {
    enable   = true
    rollback = true
  }

  lifecycle {
    ignore_changes = [desired_count] # controlado pelo auto scaling
  }

  depends_on = [aws_ecs_cluster_capacity_providers.this]
}

resource "aws_appautoscaling_target" "app" {
  service_namespace  = "ecs"
  resource_id        = "service/${aws_ecs_cluster.this.name}/${aws_ecs_service.app.name}"
  scalable_dimension = "ecs:service:DesiredCount"
  min_capacity       = var.min_tasks
  max_capacity       = var.max_tasks
}

resource "aws_appautoscaling_policy" "cpu" {
  name               = "${var.name}-cpu"
  policy_type        = "TargetTrackingScaling"
  service_namespace  = aws_appautoscaling_target.app.service_namespace
  resource_id        = aws_appautoscaling_target.app.resource_id
  scalable_dimension = aws_appautoscaling_target.app.scalable_dimension

  target_tracking_scaling_policy_configuration {
    target_value       = 60
    scale_in_cooldown  = 120
    scale_out_cooldown = 30
    predefined_metric_specification {
      predefined_metric_type = "ECSServiceAverageCPUUtilization"
    }
  }
}

resource "aws_appautoscaling_policy" "requests" {
  name               = "${var.name}-requests"
  policy_type        = "TargetTrackingScaling"
  service_namespace  = aws_appautoscaling_target.app.service_namespace
  resource_id        = aws_appautoscaling_target.app.resource_id
  scalable_dimension = aws_appautoscaling_target.app.scalable_dimension

  target_tracking_scaling_policy_configuration {
    target_value       = var.requests_per_target
    scale_in_cooldown  = 120
    scale_out_cooldown = 30
    predefined_metric_specification {
      predefined_metric_type = "ALBRequestCountPerTarget"
      resource_label         = "${var.alb_arn_suffix}/${var.target_group_arn_suffix}"
    }
  }
}
