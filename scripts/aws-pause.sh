#!/usr/bin/env bash
# Pausa o ambiente da AWS sem destruí-lo: tasks em 0 e RDS parado.
#   ./scripts/aws-pause.sh          (retomar: ./scripts/aws-resume.sh)
#
# O que continua cobrando (não tem pausa na AWS): ALB, os IPv4 dele, o nó do ElastiCache e o disco do RDS,
# ~US$ 1/dia. Custo zero só com terraform destroy. O RDS parado volta sozinho depois de 7 dias (regra da AWS).
set -euo pipefail

TF="$(dirname "$0")/../infra/terraform/envs/dev"
out() { terraform -chdir="$TF" output -raw "$1"; }
CLUSTER="$(out ecs_cluster_name)"
SERVICE="$(out ecs_service_name)"
DB="$(out db_identifier)"
RESOURCE="service/$CLUSTER/$SERVICE"

echo "== auto scaling travado em 0 (senão ele religaria as tasks)"
aws application-autoscaling register-scalable-target --service-namespace ecs \
  --scalable-dimension ecs:service:DesiredCount --resource-id "$RESOURCE" --min-capacity 0 --max-capacity 0

echo "== tasks em 0"
aws ecs update-service --cluster "$CLUSTER" --service "$SERVICE" --desired-count 0 >/dev/null
aws ecs wait services-stable --cluster "$CLUSTER" --services "$SERVICE"

echo "== parando o RDS"
STATUS="$(aws rds describe-db-instances --db-instance-identifier "$DB" --query 'DBInstances[0].DBInstanceStatus' --output text)"
if [[ "$STATUS" == "available" ]]; then
  aws rds stop-db-instance --db-instance-identifier "$DB" >/dev/null
fi
for _ in $(seq 1 60); do
  STATUS="$(aws rds describe-db-instances --db-instance-identifier "$DB" --query 'DBInstances[0].DBInstanceStatus' --output text)"
  [[ "$STATUS" == "stopped" ]] && break
  sleep 10
done

RUNNING="$(aws ecs describe-services --cluster "$CLUSTER" --services "$SERVICE" --query 'services[0].runningCount' --output text)"
echo "pausado: tasks rodando=$RUNNING, RDS=$STATUS"
