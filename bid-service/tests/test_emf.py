"""As métricas exportadas ao CloudWatch têm os nomes e dimensões que os alarmes do Terraform consultam."""

import re
from pathlib import Path

import orjson

from auction.observability.cloudwatch_emf import CloudWatchEmfSink
from auction.observability.metrics import Metrics

TERRAFORM = Path(__file__).resolve().parents[2] / "infra/terraform/modules/observability/main.tf"


def emitted_by_name(lines: list[bytes]) -> dict[str, list[dict]]:
    """Agrupa as linhas EMF pelo nome de cada métrica que elas carregam (uma linha pode ter várias)."""
    emitted: dict[str, list[dict]] = {}
    for raw in lines:
        line = orjson.loads(raw)
        spec = line["_aws"]["CloudWatchMetrics"][0]
        assert spec["Namespace"] == "Auction"
        assert line["application"] == "bid-service"
        assert all(d in line for d in spec["Dimensions"][0])  # cada dimensão tem valor na linha
        for metric in spec["Metrics"]:
            assert metric["Name"] in line  # cada métrica tem valor na linha
            emitted.setdefault(metric["Name"], []).append(line)
    return emitted


def test_emf_lines_match_the_metrics_used_by_alarms_and_dashboard():
    m = Metrics()
    m.outcome("ACCEPTED", None)
    m.outcome("REJECTED", "BID_TOO_LOW")
    m.conflicts(1)
    m.bid_latency(0.002)
    m.lua_latency(0.001)

    emitted = emitted_by_name(m.emf_lines("Auction"))

    # Todo nome do namespace Auction citado no Terraform é emitido.
    referenced = set(re.findall(r'"(auction\.[a-z.]+)"', TERRAFORM.read_text()))
    assert referenced, "nenhuma métrica Auction encontrada no Terraform"
    assert referenced <= set(emitted), f"faltam: {referenced - set(emitted)}"

    assert {(l["outcome"], l["reason"]) for l in emitted["auction.bids.count"]} == {
        ("ACCEPTED", "none"), ("REJECTED", "BID_TOO_LOW")}
    assert emitted["auction.persist.conflicts.count"][0]["auction.persist.conflicts.count"] == 1
    assert {l["phi"] for l in emitted["auction.bid.latency.percentile"]} == {"0.5", "0.95", "0.99"}

    # Intervalo sem atividade: nenhuma linha (os alarmes tratam a ausência como "sem problema").
    assert m.emf_lines("Auction") == []

    # O intervalo seguinte começa do zero.
    m.outcome("ACCEPTED", None)
    again = emitted_by_name(m.emf_lines("Auction"))
    assert again["auction.persist.conflicts.count"][0]["auction.persist.conflicts.count"] == 0
    assert again["auction.bids.count"][0]["auction.bids.count"] == 1


class FakeLogsClient:
    """Cliente de logs do boto3 de mentira: guarda as chamadas e pode falhar no put."""

    class exceptions:
        class ResourceAlreadyExistsException(Exception):
            pass

    def __init__(self, fail: bool = False):
        self.streams: list[str] = []
        self.puts: list[dict] = []
        self.fail = fail

    def create_log_stream(self, logGroupName, logStreamName):
        if logStreamName in self.streams:
            raise self.exceptions.ResourceAlreadyExistsException()
        self.streams.append(logStreamName)

    def put_log_events(self, **call):
        if self.fail:
            raise RuntimeError("throttled")
        self.puts.append(call)


async def test_metrics_go_to_their_own_log_group_not_to_stdout(caplog, capsys):
    m = Metrics()
    m.outcome("ACCEPTED", None)
    lines = m.emf_lines("Auction")
    client = FakeLogsClient()
    sink = CloudWatchEmfSink("/ecs/leilao-dev/metrics", "us-east-1", client=client, stream="app/t1")

    await sink.send(lines)
    await sink.send([])  # intervalo ocioso: nenhuma chamada
    await sink.send(lines)

    assert client.streams == ["app/t1"]  # o stream é criado uma vez só
    assert len(client.puts) == 2
    put = client.puts[0]
    assert put["logGroupName"] == "/ecs/leilao-dev/metrics" and put["logStreamName"] == "app/t1"
    assert [e["message"].encode() for e in put["logEvents"]] == lines
    assert put["logEvents"][0]["timestamp"] == orjson.loads(lines[0])["_aws"]["Timestamp"]
    assert capsys.readouterr().out == ""  # nada de métrica no stdout (o log da aplicação)

    # Falha no CloudWatch vira aviso, sem derrubar a exportação.
    failing = CloudWatchEmfSink("/g", "us-east-1", client=FakeLogsClient(fail=True), stream="s")
    await failing.send(lines)
    assert any("falha ao enviar métricas" in r.getMessage() for r in caplog.records)
