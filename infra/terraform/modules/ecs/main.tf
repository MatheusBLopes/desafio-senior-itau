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

# Logs do sidecar ADOT (startup e telemetria do próprio coletor) num grupo separado e de retenção curta:
# o grupo da aplicação fica só com os logs do bid-service.
resource "aws_cloudwatch_log_group" "adot" {
  name              = "/ecs/${var.name}/adot"
  retention_in_days = 1
}

# Métricas de negócio (EMF): a aplicação grava aqui por PutLogEvents, fora do grupo dos logs. O CloudWatch
# extrai as métricas na ingestão; as linhas em si não servem depois, então a retenção é mínima.
resource "aws_cloudwatch_log_group" "metrics" {
  name              = "/ecs/${var.name}/metrics"
  retention_in_days = 1
}

# Consultas prontas no CloudWatch Logs Insights (grátis; pagas só quando executadas, por GB lido).
# No console: CloudWatch → Logs Insights → Consultas salvas → pasta com o nome do ambiente.
locals {
  log_queries = {
    "1 - Logs da aplicação"                               = <<-EOT
      fields @timestamp, level, message, requestId, userId, auctionId
      | filter ispresent(level)
      | sort @timestamp desc
      | limit 200
    EOT
    "2 - Requisições"                                     = <<-EOT
      fields @timestamp, status, method, route, durationMs, userId, auctionId, amount, outcome, reason, errorCode, requestId
      | filter logger = "api.access"
      | sort @timestamp desc
      | limit 200
    EOT
    "3 - Erros e avisos"                                  = <<-EOT
      fields @timestamp, level, message, errorCode, error, requestId, auctionId, logger
      | filter level in ["WARNING", "ERROR"]
      | sort @timestamp desc
      | limit 200
    EOT
    "4 - Rastrear uma requisição (troque o X-Request-Id)" = <<-EOT
      fields @timestamp, level, message, logger, @message
      | filter requestId = "COLE-AQUI-O-X-REQUEST-ID"
      | sort @timestamp asc
    EOT
    "5 - Linha do tempo de um leilão (troque o id)"       = <<-EOT
      fields @timestamp, level, message, userId, amount, outcome, reason, seq, winnerId, finalPrice
      | filter auctionId = "COLE-AQUI-O-ID-DO-LEILAO"
      | sort @timestamp asc
    EOT
    "6 - Lances por resultado"                            = <<-EOT
      filter logger = "api.access" and route = "/auctions/{auction_id}/bids"
      | stats count(*) as lances by status, outcome, reason, errorCode
      | sort lances desc
    EOT
    "7 - Latência por rota"                               = <<-EOT
      filter logger = "api.access"
      | stats count(*) as requisicoes, avg(durationMs) as media_ms, pct(durationMs, 99) as p99_ms by method, route
      | sort requisicoes desc
    EOT
  }
}

resource "aws_cloudwatch_query_definition" "app" {
  for_each        = local.log_queries
  name            = "${var.name}/${each.key}"
  log_group_names = [aws_cloudwatch_log_group.app.name]
  query_string    = each.value
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
  name = "traces-metrics-and-signup"
  role = aws_iam_role.task.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["xray:PutTraceSegments", "xray:PutTelemetryRecords", "xray:GetSamplingRules", "xray:GetSamplingTargets"]
        Resource = "*"
      },
      {
        # Métricas EMF: só criar o stream da task e gravar, só no grupo de métricas.
        Effect   = "Allow"
        Action   = ["logs:CreateLogStream", "logs:PutLogEvents"]
        Resource = "${aws_cloudwatch_log_group.metrics.arn}:*"
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
    # Métricas de negócio (namespace Auction) via Embedded Metric Format: linhas JSON que o CloudWatch Logs
    # transforma em métricas, sem PutMetricData. Vão para o grupo /metrics, não para o dos logs.
    CLOUDWATCH_EMF           = "true"
    CLOUDWATCH_EMF_LOG_GROUP = aws_cloudwatch_log_group.metrics.name
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
          awslogs-group         = aws_cloudwatch_log_group.adot.name
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
