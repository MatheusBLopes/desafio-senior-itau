"""Logs estruturados: uma linha JSON por evento, com o contexto da requisição (requestId, userId, auctionId, seq).

Só a aplicação escreve em INFO. Bibliotecas de terceiros (uvicorn, httpx, botocore, OpenTelemetry) ficam em
WARNING: o log mostra o que aconteceu com os leilões e as requisições, não o funcionamento interno delas.
"""

import contextvars
import logging
import sys
import time
import uuid
from typing import Any

import orjson

APP_LOGGER = "auction"

# --- Contexto de log (equivalente ao MDC do Java) ---------------------------------------------------

log_context: contextvars.ContextVar[dict[str, Any]] = contextvars.ContextVar("log_context", default={})


def _json_value(value: object) -> object:
    """Números e booleanos ficam como estão (para filtrar com > e < no CloudWatch); o resto vira texto."""
    if isinstance(value, (bool, int, float)) or value is None:
        return value
    return str(value)


def bind(**fields: object) -> None:
    """Acrescenta campos (auctionId, userId, seq...) a todos os logs da requisição atual."""
    log_context.set({**log_context.get(), **{name: _json_value(value) for name, value in fields.items()}})


def new_request_id() -> str:
    """Identificador curto e único de uma requisição, devolvido ao cliente no header X-Request-Id."""
    return uuid.uuid4().hex[:16]


def fields(**values: object) -> dict[str, dict[str, object]]:
    """Campos extras de uma linha de log: `log.info("...", extra=fields(auctionId=x))`."""
    return {"fields": {name: _json_value(value) for name, value in values.items()}}


def _event_fields(record: logging.LogRecord) -> dict[str, object]:
    """Contexto da requisição mais os campos extras da própria linha."""
    return {**log_context.get(), **getattr(record, "fields", {})}


def _short_logger(name: str) -> str:
    """auction.bidding.service → bidding.service: o prefixo é igual em todas as linhas."""
    return name.removeprefix(APP_LOGGER + ".")


class JsonFormatter(logging.Formatter):
    """Uma linha JSON por evento, com o contexto da requisição e o trace_id do OpenTelemetry, se houver."""

    def format(self, record: logging.LogRecord) -> str:
        """Monta o JSON do evento: horário, nível, mensagem, contexto, campos extras, trace e stack trace."""
        entry: dict[str, object] = {
            "@timestamp": time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(record.created))
            + f".{int(record.msecs):03d}Z",
            "level": record.levelname,
            "message": record.getMessage(),
            **_event_fields(record),
            "logger": _short_logger(record.name),
        }
        trace_id = getattr(record, "otelTraceID", None)
        if trace_id and trace_id != "0":
            entry["trace_id"] = trace_id
            entry["span_id"] = getattr(record, "otelSpanID", None)
        if record.exc_info:
            entry["error"] = f"{record.exc_info[0].__name__}: {record.exc_info[1]}"
            entry["stack_trace"] = self.formatException(record.exc_info)
        return orjson.dumps(entry, default=str).decode()


class TextFormatter(logging.Formatter):
    """Uma linha legível por evento, para o terminal no desenvolvimento: os campos vêm no fim como chave=valor."""

    def format(self, record: logging.LogRecord) -> str:
        """Horário, nível, origem, mensagem e campos."""
        moment = time.strftime("%H:%M:%S", time.localtime(record.created)) + f".{int(record.msecs):03d}"
        extra = " ".join(f"{name}={value}" for name, value in _event_fields(record).items())
        line = f"{moment} {record.levelname:<7} {_short_logger(record.name)} - {record.getMessage()}"
        if extra:
            line += f"  [{extra}]"
        if record.exc_info:
            line += "\n" + self.formatException(record.exc_info)
        return line


def configure_logging(json_logs: bool, level: str = "INFO") -> None:
    """Liga o log no stdout: JSON na AWS (o CloudWatch indexa os campos), texto legível no desenvolvimento.

    A aplicação usa o nível pedido (LOG_LEVEL); todo o resto fica em WARNING.
    """
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter() if json_logs else TextFormatter())
    root = logging.getLogger()
    root.handlers[:] = [handler]
    root.setLevel(logging.WARNING)
    logging.getLogger(APP_LOGGER).setLevel(level.upper())
    # O uvicorn configura os próprios loggers; sem isto, as mensagens de startup dele voltariam em INFO.
    for name in ("uvicorn", "uvicorn.error", "uvicorn.access"):
        uvicorn_logger = logging.getLogger(name)
        uvicorn_logger.handlers.clear()
        uvicorn_logger.propagate = True
        uvicorn_logger.setLevel(logging.WARNING)
