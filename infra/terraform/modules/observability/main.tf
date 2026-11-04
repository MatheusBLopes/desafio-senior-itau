# Alarmes + dashboard. Notificação por e-mail via SNS (a assinatura precisa ser confirmada).
data "aws_region" "current" {}

resource "aws_sns_topic" "alerts" {
  name = "${var.name}-alerts"
}

resource "aws_sns_topic_subscription" "email" {
  topic_arn = aws_sns_topic.alerts.arn
  protocol  = "email"
  endpoint  = var.alert_email
}

locals {
  alarms = {
    alb-5xx = {
      namespace   = "AWS/ApplicationELB", metric = "HTTPCode_Target_5XX_Count", stat = "Sum", threshold = 10
      dimensions  = { LoadBalancer = var.alb_arn_suffix }
      description = "Respostas 5xx das tasks"
    }
    alb-p95-latency = {
      namespace   = "AWS/ApplicationELB", metric = "TargetResponseTime", stat = "p95", threshold = var.p95_latency_seconds
      dimensions  = { LoadBalancer = var.alb_arn_suffix }
      description = "Latencia p95 no ALB"
    }
    ecs-cpu = {
      namespace   = "AWS/ECS", metric = "CPUUtilization", stat = "Average", threshold = 85
      dimensions  = { ClusterName = var.cluster_name, ServiceName = var.service_name }
      description = "CPU media das tasks"
    }
    ecs-memory = {
      namespace   = "AWS/ECS", metric = "MemoryUtilization", stat = "Average", threshold = 85
      dimensions  = { ClusterName = var.cluster_name, ServiceName = var.service_name }
      description = "Memoria media das tasks"
    }
    valkey-memory = {
      namespace   = "AWS/ElastiCache", metric = "DatabaseMemoryUsagePercentage", stat = "Maximum", threshold = 75
      dimensions  = { CacheClusterId = "${var.redis_replication_group_id}-001" }
      description = "Memoria do Valkey (noeviction: cheio = escrita recusada)"
    }
    valkey-cpu = {
      namespace   = "AWS/ElastiCache", metric = "EngineCPUUtilization", stat = "Maximum", threshold = 70
      dimensions  = { CacheClusterId = "${var.redis_replication_group_id}-001" }
      description = "Thread principal do Valkey (gargalo de leilao quente)"
    }
    rds-cpu = {
      namespace   = "AWS/RDS", metric = "CPUUtilization", stat = "Average", threshold = 80
      dimensions  = { DBInstanceIdentifier = var.db_identifier }
      description = "CPU do Postgres"
    }
    rds-connections = {
      namespace   = "AWS/RDS", metric = "DatabaseConnections", stat = "Maximum", threshold = 70
      dimensions  = { DBInstanceIdentifier = var.db_identifier }
      description = "Conexoes no Postgres (t4g.micro aceita ~80)"
    }
    stream-backlog = {
      namespace   = "Auction", metric = "auction.stream.backlog", stat = "Maximum", threshold = 10000
      dimensions  = { application = "bid-service" }
      description = "Lances aceitos ainda nao persistidos (lag do write-behind)"
    }
    # Um unico lance aceito que colidiu no Postgres ja exige reconciliacao: alarma no primeiro minuto.
    persist-conflicts = {
      namespace   = "Auction", metric = "auction.persist.conflicts.count", stat = "Sum", threshold = 0
      dimensions  = { application = "bid-service" }
      description = "Lances aceitos que colidiram no Postgres (ver tabela bid_conflicts)"
      evaluation  = 1
    }
  }
}

resource "aws_cloudwatch_metric_alarm" "this" {
  for_each = local.alarms

  alarm_name          = "${var.name}-${each.key}"
  alarm_description   = each.value.description
  namespace           = each.value.namespace
  metric_name         = each.value.metric
  dimensions          = each.value.dimensions
  statistic           = contains(["Sum", "Average", "Maximum", "Minimum", "SampleCount"], each.value.stat) ? each.value.stat : null
  extended_statistic  = contains(["Sum", "Average", "Maximum", "Minimum", "SampleCount"], each.value.stat) ? null : each.value.stat
  period              = 60
  evaluation_periods  = lookup(each.value, "evaluation", 3)
  datapoints_to_alarm = lookup(each.value, "evaluation", 2)
  threshold           = each.value.threshold
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "notBreaching"
  alarm_actions       = [aws_sns_topic.alerts.arn]
  ok_actions          = [aws_sns_topic.alerts.arn]
}

resource "aws_cloudwatch_dashboard" "this" {
  dashboard_name = var.name
  dashboard_body = jsonencode({
    widgets = [
      for i, w in [
        { title = "Lances por resultado", metrics = [
          [{ expression = "SEARCH('{Auction,application,outcome,reason} MetricName=\"auction.bids.count\"', 'Sum', 60)", id = "bids" }]
        ] },
        { title = "Latencia do Lua e da API (p95, ms)", metrics = [
          ["Auction", "auction.lua.latency.percentile", "application", "bid-service", "phi", "0.95", { stat = "Maximum" }],
          ["Auction", "auction.bid.latency.percentile", "application", "bid-service", "phi", "0.95", { stat = "Maximum" }]
        ] },
        { title = "Backlog do stream (write-behind)", metrics = [
          ["Auction", "auction.stream.backlog", "application", "bid-service", { stat = "Maximum" }]
        ] },
        { title = "WebSockets ativos", metrics = [
          ["Auction", "auction.websocket.sessions", "application", "bid-service", { stat = "Sum" }]
        ] },
        { title = "ALB: requests, 5xx e p95", metrics = [
          ["AWS/ApplicationELB", "RequestCount", "LoadBalancer", var.alb_arn_suffix, { stat = "Sum" }],
          ["AWS/ApplicationELB", "HTTPCode_Target_5XX_Count", "LoadBalancer", var.alb_arn_suffix, { stat = "Sum" }],
          ["AWS/ApplicationELB", "TargetResponseTime", "LoadBalancer", var.alb_arn_suffix, { stat = "p95", yAxis = "right" }]
        ] },
        { title = "ECS: CPU e memoria", metrics = [
          ["AWS/ECS", "CPUUtilization", "ClusterName", var.cluster_name, "ServiceName", var.service_name],
          ["AWS/ECS", "MemoryUtilization", "ClusterName", var.cluster_name, "ServiceName", var.service_name]
        ] },
        { title = "Valkey: CPU do engine e memoria", metrics = [
          ["AWS/ElastiCache", "EngineCPUUtilization", "CacheClusterId", "${var.redis_replication_group_id}-001"],
          ["AWS/ElastiCache", "DatabaseMemoryUsagePercentage", "CacheClusterId", "${var.redis_replication_group_id}-001"]
        ] },
        { title = "RDS: CPU e conexoes", metrics = [
          ["AWS/RDS", "CPUUtilization", "DBInstanceIdentifier", var.db_identifier],
          ["AWS/RDS", "DatabaseConnections", "DBInstanceIdentifier", var.db_identifier, { yAxis = "right" }]
        ] }
        ] : {
        type   = "metric"
        x      = (i % 2) * 12
        y      = floor(i / 2) * 6
        width  = 12
        height = 6
        properties = {
          title   = w.title
          region  = data.aws_region.current.region
          view    = "timeSeries"
          period  = 60
          metrics = w.metrics
        }
      }
    ]
  })
}

variable "name" {
  type = string
}

variable "alert_email" {
  type = string
}

variable "alb_arn_suffix" {
  type = string
}

variable "cluster_name" {
  type = string
}

variable "service_name" {
  type = string
}

variable "redis_replication_group_id" {
  type = string
}

variable "db_identifier" {
  type = string
}

variable "p95_latency_seconds" {
  type    = number
  default = 0.25
}

output "dashboard_url" {
  value = "https://${data.aws_region.current.region}.console.aws.amazon.com/cloudwatch/home#dashboards/dashboard/${aws_cloudwatch_dashboard.this.dashboard_name}"
}
