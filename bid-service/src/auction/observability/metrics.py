"""Métricas Prometheus e exportação para o CloudWatch via Embedded Metric Format.

Nenhuma métrica usa auctionId ou userId como label/dimensão: no CloudWatch cada combinação de
dimensões é uma série cobrada. Esses identificadores ficam nos logs (contextvars) e nos traces.
"""

import asyncio
import random
import sys
import time
from collections import Counter as Tally

import orjson
from prometheus_client import Counter, Gauge, Histogram

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
        """Amostra vazia com capacidade para `size` valores."""
        self.size = size
        self.values: list[float] = []
        self.seen = 0

    def add(self, value: float) -> None:
        """Considera um valor: entra na amostra com probabilidade size/seen, mantendo-a uniforme."""
        self.seen += 1
        if len(self.values) < self.size:
            self.values.append(value)
        else:
            slot = random.randrange(self.seen)
            if slot < self.size:
                self.values[slot] = value

    def drain(self) -> list[float]:
        """Devolve a amostra do intervalo, ordenada, e começa um intervalo novo."""
        values, self.values, self.seen = self.values, [], 0
        return sorted(values)


class Metrics:
    """Fachada das métricas: alimenta o Prometheus (porta interna) e o acumulador do CloudWatch."""

    def __init__(self) -> None:
        """Acumuladores do intervalo atual do CloudWatch (os contadores do Prometheus são globais)."""
        self._outcomes: Tally[tuple[str, str]] = Tally()
        self._conflicts = 0
        self._shed_reported = 0.0
        self._bid = _Reservoir()
        self._lua = _Reservoir()

    def outcome(self, outcome: str, reason: str | None) -> None:
        """Conta um lance pelo resultado (ACCEPTED/REJECTED) e motivo."""
        reason = reason or "none"
        BIDS.labels(outcome, reason).inc()
        self._outcomes[(outcome, reason)] += 1

    def bid_latency(self, seconds: float) -> None:
        """Registra o tempo total de um lance dentro da task."""
        BID_LATENCY.observe(seconds)
        self._bid.add(seconds)

    def lua_latency(self, seconds: float) -> None:
        """Registra o tempo de uma chamada ao place_bid.lua."""
        LUA_LATENCY.observe(seconds)
        self._lua.add(seconds)

    def conflicts(self, count: int) -> None:
        """Conta lances aceitos que colidiram no Postgres e foram para bid_conflicts."""
        PERSIST_CONFLICTS.inc(count)
        self._conflicts += count

    def emf_lines(self, namespace: str) -> list[bytes]:
        """Drena o intervalo em linhas EMF: o CloudWatch Logs extrai as métricas, sem PutMetricData.

        Os nomes e dimensões são os que os alarmes e o dashboard do Terraform consultam.
        """
        now = int(time.time() * 1000)
        app = {"application": "bid-service"}

        def emf(name: str, value: float, unit: str, **dims: str) -> bytes:
            """Uma linha EMF com uma métrica e suas dimensões."""
            dimensions = {**app, **dims}
            return orjson.dumps({
                "_aws": {"Timestamp": now, "CloudWatchMetrics": [{
                    "Namespace": namespace, "Dimensions": [list(dimensions)],
                    "Metrics": [{"Name": name, "Unit": unit}]}]},
                **dimensions, name: value,
            })

        lines = [emf("auction.bids.count", count, "Count", outcome=outcome, reason=reason)
                 for (outcome, reason), count in self._outcomes.items()]
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
                    value = values[min(len(values) - 1, int(float(phi) * len(values)))]
                    lines.append(emf(name, value, "Seconds", phi=phi))
        return lines


async def export_emf(metrics: Metrics, namespace: str, step_s: float) -> None:
    """A cada intervalo, escreve as métricas no stdout; o driver awslogs do ECS leva ao CloudWatch."""
    while True:
        await asyncio.sleep(step_s)
        for line in metrics.emf_lines(namespace):
            sys.stdout.buffer.write(line + b"\n")
        sys.stdout.flush()
