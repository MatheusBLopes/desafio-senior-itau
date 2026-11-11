"""Logs estruturados: uma linha JSON por evento, com o contexto da requisição (auctionId, userId, seq)."""

import contextvars
import logging
import sys
import time

import orjson

# --- Contexto de log (equivalente ao MDC do Java) ---------------------------------------------------

log_context: contextvars.ContextVar[dict[str, str]] = contextvars.ContextVar("log_context", default={})


def bind(**fields: object) -> None:
    """Acrescenta campos (auctionId, userId, seq) aos logs da requisição atual."""
    log_context.set({**log_context.get(), **{name: str(value) for name, value in fields.items()}})


class JsonFormatter(logging.Formatter):
    """Uma linha JSON por evento, com o contexto da requisição e o trace_id do OpenTelemetry, se houver."""

    def format(self, record: logging.LogRecord) -> str:
        """Monta o JSON do evento: horário, nível, mensagem, contexto, trace e stack trace."""
        entry = {
            "@timestamp": time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(record.created))
            + f".{int(record.msecs):03d}Z",
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            **log_context.get(),
        }
        trace_id = getattr(record, "otelTraceID", None)
        if trace_id and trace_id != "0":
            entry["trace_id"] = trace_id
            entry["span_id"] = getattr(record, "otelSpanID", None)
        if record.exc_info:
            entry["stack_trace"] = self.formatException(record.exc_info)
        return orjson.dumps(entry).decode()


def configure_logging(json_logs: bool) -> None:
    """Liga o log no stdout: JSON na AWS (o CloudWatch indexa os campos), texto simples no desenvolvimento."""
    handler = logging.StreamHandler(sys.stdout)
    if json_logs:
        handler.setFormatter(JsonFormatter())
    else:
        handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)-5s %(name)s - %(message)s"))
    root = logging.getLogger()
    root.handlers[:] = [handler]
    root.setLevel(logging.INFO)
