#!/usr/bin/env bash
# Retoma o ambiente pausado por aws-pause.sh: RDS de volta, 2 tasks, auto scaling de 2 a 10, smoke test.
#   ./scripts/aws-resume.sh
set -euo pipefail

TF="$(dirname "$0")/../infra/terraform/envs/dev"
out() { terraform -chdir="$TF" output -raw "$1"; }
CLUSTER="$(out ecs_cluster_name)"
SERVICE="$(out ecs_service_name)"
DB="$(out db_identifier)"
ALB="$(out alb_url)"
RESOURCE="service/$CLUSTER/$SERVICE"

echo "== iniciando o RDS (alguns minutos)"
STATUS="$(aws rds describe-db-instances --db-instance-identifier "$DB" --query 'DBInstances[0].DBInstanceStatus' --output text)"
if [[ "$STATUS" == "stopped" ]]; then
  aws rds start-db-instance --db-instance-identifier "$DB" >/dev/null
fi
aws rds wait db-instance-available --db-instance-identifier "$DB"

echo "== auto scaling de volta para 2..10 e 2 tasks"
aws application-autoscaling register-scalable-target --service-namespace ecs \
  --scalable-dimension ecs:service:DesiredCount --resource-id "$RESOURCE" --min-capacity 2 --max-capacity 10
aws ecs update-service --cluster "$CLUSTER" --service "$SERVICE" --desired-count 2 >/dev/null
aws ecs wait services-stable --cluster "$CLUSTER" --services "$SERVICE"

echo "== aguardando o ALB enxergar as tasks"
for _ in $(seq 1 30); do
  curl -sf "$ALB/readyz" >/dev/null && break
  sleep 5
done

"$(dirname "$0")/smoke-test.sh" "$ALB"
echo "no ar: $ALB"
