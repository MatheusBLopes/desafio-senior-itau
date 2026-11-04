#!/usr/bin/env bash
# Build ARM64 (Graviton) da imagem e push para o ECR criado pelo Terraform.
#   ./scripts/build-push.sh <ecr-repository-url> [região]
# Imprime a tag gerada, que vai em image_tag no terraform.tfvars.
#
# Build cruzado sem buildx nem QEMU: o uv instala os wheels aarch64 num estágio x86 e o estágio final
# (base arm64, referenciada pelo digest da arquitetura) só copia arquivos. Ver bid-service/Dockerfile.
set -euo pipefail

REPO="${1:?uso: $0 <ecr-repository-url> [região]}"
REGION="${2:-us-east-1}"
REGISTRY="${REPO%%/*}"
TAG="$(git rev-parse --short HEAD 2>/dev/null || echo local)-$(date +%Y%m%d%H%M%S)"

ARM_BASE="python:3.13-slim@$(docker manifest inspect python:3.13-slim | python3 -c '
import json, sys
m = json.load(sys.stdin)["manifests"]
print(next(x["digest"] for x in m if x["platform"].get("os") == "linux" and x["platform"].get("architecture") == "arm64"))')"

docker build \
  --build-arg RUNTIME_IMAGE="$ARM_BASE" \
  --build-arg PY_PLATFORM=aarch64-manylinux_2_28 \
  -t "$REPO:$TAG" "$(dirname "$0")/../bid-service"

ARCH="$(docker image inspect "$REPO:$TAG" --format '{{.Architecture}}')"
[[ "$ARCH" == "arm64" ]] || { echo "imagem saiu $ARCH, esperado arm64" >&2; exit 1; }

aws ecr get-login-password --region "$REGION" | docker login --username AWS --password-stdin "$REGISTRY"
docker push "$REPO:$TAG"

echo
echo "image_tag = \"$TAG\""
