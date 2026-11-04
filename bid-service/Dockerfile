# Build cruzado, sem emulação: a imagem final roda em Fargate ARM64 (Graviton) e pode ser montada numa
# máquina x86 sem QEMU nem buildx.
#   local (x86):  docker build -t bid-service .
#   AWS (ARM64):  ../scripts/build-push.sh (passa RUNTIME_IMAGE = digest arm64 da base e PY_PLATFORM aarch64)
# A base de destino vai por digest da arquitetura: o builder clássico do Docker (sem buildx) ignoraria
# --platform e reusaria a imagem amd64 em cache.
#
# Como: o estágio de build roda na arquitetura da máquina e o uv instala os wheels da arquitetura de
# DESTINO (--python-platform). O estágio final só copia arquivos (nenhum RUN), então não precisa executar
# nada da outra arquitetura. Bytecode .pyc é portável entre arquiteturas.
ARG BUILD_PLATFORM=linux/amd64
ARG RUNTIME_IMAGE=python:3.13-slim

FROM --platform=${BUILD_PLATFORM} python:3.13-slim AS build
COPY --from=ghcr.io/astral-sh/uv:0.8.22 /uv /bin/uv
ARG PY_PLATFORM=x86_64-manylinux_2_28
WORKDIR /app
ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy UV_PYTHON_DOWNLOADS=never
COPY pyproject.toml uv.lock ./
# Dependências antes do código: a camada só é refeita quando o uv.lock muda.
RUN uv export --frozen --no-dev --extra otel --no-hashes --no-emit-project -o /tmp/requirements.txt \
 && uv venv /app/.venv --python /usr/local/bin/python3.13 \
 && uv pip install --python /app/.venv/bin/python --python-platform "$PY_PLATFORM" --only-binary :all: \
      -r /tmp/requirements.txt
COPY src src
RUN uv pip install --python /app/.venv/bin/python --python-platform "$PY_PLATFORM" --no-deps .

FROM ${RUNTIME_IMAGE}
WORKDIR /app
# O venv aponta para /usr/local/bin/python3.13, que existe no mesmo lugar nas duas arquiteturas.
COPY --from=build /app/.venv /app/.venv
COPY entrypoint.sh ./
ENV PATH="/app/.venv/bin:$PATH" PYTHONUNBUFFERED=1 HOME=/tmp
# Usuário sem privilégios por UID numérico: criar um com useradd exigiria um RUN na arquitetura de destino.
USER 10001:10001
# 8080: API, WebSocket e /readyz (ALB). 8081: métricas e health detalhado, só na rede interna.
EXPOSE 8080 8081
ENTRYPOINT ["/app/entrypoint.sh"]
