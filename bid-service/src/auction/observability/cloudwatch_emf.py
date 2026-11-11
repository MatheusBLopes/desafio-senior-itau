"""Envio das linhas EMF a um log group só de métricas, fora do grupo dos logs da aplicação.

O driver awslogs do ECS manda todo o stdout para o mesmo log group, sem separar por conteúdo: se as
métricas saíssem no stdout, ficariam misturadas com os logs. Aqui elas vão por PutLogEvents (uma chamada
por intervalo por task) para /ecs/<ambiente>/metrics. Com o header x-amzn-logs-format: json/emf, o
CloudWatch extrai as métricas na ingestão, igual faria com o stdout; continua sem PutMetricData.
"""

import asyncio
import logging
import os
import uuid
from typing import Any

import httpx
import orjson

from .logs import fields

log = logging.getLogger(__name__)


def _task_id() -> str:
    """Id da task do ECS (do endpoint de metadata), para nomear o log stream; fora do ECS, um aleatório."""
    metadata_uri = os.environ.get("ECS_CONTAINER_METADATA_URI_V4")
    if metadata_uri:
        try:
            task_arn = httpx.get(f"{metadata_uri}/task", timeout=2).json()["TaskARN"]
            return task_arn.rsplit("/", 1)[-1]
        except Exception as e:
            log.warning("metadata do ECS indisponível; log stream de métricas com id aleatório",
                        extra=fields(error=e))
    return uuid.uuid4().hex


class CloudWatchEmfSink:
    """Escreve as linhas EMF de cada intervalo no log group de métricas, num stream por task."""

    def __init__(self, log_group: str, region: str, client: Any = None, stream: str | None = None):
        """Cria o cliente boto3 de logs (ou usa o dado, nos testes) com o header que ativa o EMF."""
        if client is None:
            import boto3  # só na AWS

            client = boto3.client("logs", region_name=region)
            client.meta.events.register("before-sign.logs.PutLogEvents", _add_emf_header)
        self._client = client
        self._log_group = log_group
        self._stream = stream
        self._stream_ready = False

    async def send(self, lines: list[bytes]) -> None:
        """Envia as linhas do intervalo numa chamada; falha vira aviso e as linhas do intervalo se perdem."""
        if not lines:
            return
        try:
            await asyncio.to_thread(self._put, lines)
        except Exception as e:
            log.warning("falha ao enviar métricas ao CloudWatch",
                        extra=fields(logGroup=self._log_group, lines=len(lines), error=e))

    def _put(self, lines: list[bytes]) -> None:
        """Cria o stream na primeira vez e grava os eventos (síncrono: roda numa thread)."""
        if not self._stream_ready:
            self._stream = self._stream or f"app/{_task_id()}"
            try:
                self._client.create_log_stream(logGroupName=self._log_group, logStreamName=self._stream)
            except self._client.exceptions.ResourceAlreadyExistsException:
                pass
            self._stream_ready = True
        events = [{"timestamp": orjson.loads(line)["_aws"]["Timestamp"], "message": line.decode()}
                  for line in lines]
        self._client.put_log_events(logGroupName=self._log_group, logStreamName=self._stream, logEvents=events)


def _add_emf_header(request: Any, **_: Any) -> None:
    """Sem este header, o CloudWatch guarda as linhas como log comum e não extrai as métricas."""
    request.headers["x-amzn-logs-format"] = "json/emf"
