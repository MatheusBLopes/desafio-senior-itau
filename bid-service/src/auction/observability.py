"""Logs estruturados, métricas Prometheus e exportação para o CloudWatch via Embedded Metric Format.

Nenhuma métrica usa auctionId ou userId como label/dimensão: no CloudWatch cada combinação de
dimensões é uma série cobrada. Esses identificadores ficam nos logs (contextvars) e nos traces.
"""

import asyncio
import contextvars
import logging
import random
import sys
import time
from collections import Counter as Tally

import orjson
from prometheus_client import Counter, Gauge, Histogram

# --- Contexto de log (equivalente ao MDC do Java) ---------------------------------------------------

log_context: contextvars.ContextVar[dict[str, str]] = contextvars.ContextVar("log_context", default={})


def bind(**fields: object) -> None:
    """Acrescenta campos (auctionId, userId, seq) aos logs da requisição atual."""
    log_context.set({**log_context.get(), **{k: str(v) for k, v in fields.items()}})


class JsonFormatter(logging.Formatter):
    """Uma linha JSON por evento, com o contexto da requisição e o trace_id do OpenTelemetry, se houver."""

    def format(self, record: logging.LogRecord) -> str:
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
    handler = logging.StreamHandler(sys.stdout)
    if json_logs:
        handler.setFormatter(JsonFormatter())
    else:
        handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)-5s %(name)s - %(message)s"))
    root = logging.getLogger()
    root.handlers[:] = [handler]
    root.setLevel(logging.INFO)


# --- Métricas -----------------------------------------------------------------------------------------

LATENCY_BUCKETS = (.0005, .001, .002, .005, .01, .025, .05, .1, .25, .5, 1, 2.5)

BIDS = Counter("auction_bids", "Lances por resultado", ["outcome", "reason"])
BID_LATENCY = Histogram("auction_bid_latency_seconds", "Tempo total de um lance na task", buckets=LATENCY_BUCKETS)
LUA_LATENCY = Histogram("auction_lua_latency_seconds", "Tempo do EVALSHA do place_bid.lua", buckets=LATENCY_BUCKETS)
STREAM_BACKLOG = Gauge("auction_stream_backlog", "Eventos ainda não persistidos no Postgres")
PERSIST_CONFLICTS = Counter("auction_persist_conflicts", "Lances aceitos no Valkey que colidiram no Postgres")
REQUESTS_SHED = Counter("auction_requests_shed", "Requisições recusadas com 503 por excesso de carga na task")
WEBSOCKET_SESSIONS = Gauge("auction_websocket_sessions", "Conexões WebSocket ativas nesta task")


class _Reservoir:
    """Amostra uniforme de tamanho fixo (reservoir sampling): percentis por intervalo sem guardar tudo."""

    def __init__(self, size: int = 5000):
        self.size = size
        self.values: list[float] = []
        self.seen = 0

    def add(self, v: float) -> None:
        self.seen += 1
        if len(self.values) < self.size:
            self.values.append(v)
        else:
            i = random.randrange(self.seen)
            if i < self.size:
                self.values[i] = v

    def drain(self) -> list[float]:
        values, self.values, self.seen = self.values, [], 0
        return sorted(values)


class Metrics:
    """Fachada das métricas: alimenta o Prometheus (porta interna) e o acumulador do CloudWatch."""

    def __init__(self) -> None:
        self._outcomes: Tally[tuple[str, str]] = Tally()
        self._conflicts = 0
        self._shed_reported = 0.0
        self._bid = _Reservoir()
        self._lua = _Reservoir()

    def outcome(self, outcome: str, reason: str | None) -> None:
        reason = reason or "none"
        BIDS.labels(outcome, reason).inc()
        self._outcomes[(outcome, reason)] += 1

    def bid_latency(self, seconds: float) -> None:
        BID_LATENCY.observe(seconds)
        self._bid.add(seconds)

    def lua_latency(self, seconds: float) -> None:
        LUA_LATENCY.observe(seconds)
        self._lua.add(seconds)

    def conflicts(self, n: int) -> None:
        PERSIST_CONFLICTS.inc(n)
        self._conflicts += n

    def emf_lines(self, namespace: str) -> list[bytes]:
        """Drena o intervalo em linhas EMF: o CloudWatch Logs extrai as métricas, sem PutMetricData.

        Os nomes e dimensões são os que os alarmes e o dashboard do Terraform consultam.
        """
        now = int(time.time() * 1000)
        app = {"application": "bid-service"}

        def emf(name: str, value: float, unit: str, **dims: str) -> bytes:
            dimensions = {**app, **dims}
            return orjson.dumps({
                "_aws": {"Timestamp": now, "CloudWatchMetrics": [{
                    "Namespace": namespace, "Dimensions": [list(dimensions)],
                    "Metrics": [{"Name": name, "Unit": unit}]}]},
                **dimensions, name: value,
            })

        lines = [emf("auction.bids.count", n, "Count", outcome=o, reason=r) for (o, r), n in self._outcomes.items()]
        self._outcomes.clear()
        lines.append(emf("auction.persist.conflicts.count", self._conflicts, "Count"))
        self._conflicts = 0
        lines.append(emf("auction.requests.shed.count", REQUESTS_SHED._value.get() - self._shed_reported, "Count"))
        self._shed_reported = REQUESTS_SHED._value.get()
        lines.append(emf("auction.stream.backlog", STREAM_BACKLOG._value.get(), "Count"))
        lines.append(emf("auction.websocket.sessions", WEBSOCKET_SESSIONS._value.get(), "Count"))
        for name, reservoir in (("auction.bid.latency.percentile", self._bid),
                                ("auction.lua.latency.percentile", self._lua)):
            values = reservoir.drain()
            for phi in ("0.5", "0.95", "0.99"):
                if values:
                    v = values[min(len(values) - 1, int(float(phi) * len(values)))]
                    lines.append(emf(name, v, "Seconds", phi=phi))
        return lines


async def export_emf(metrics: Metrics, namespace: str, step_s: float) -> None:
    """A cada intervalo, escreve as métricas no stdout; o driver awslogs do ECS leva ao CloudWatch."""
    while True:
        await asyncio.sleep(step_s)
        for line in metrics.emf_lines(namespace):
            sys.stdout.buffer.write(line + b"\n")
        sys.stdout.flush()
