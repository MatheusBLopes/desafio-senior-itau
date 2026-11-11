"""As métricas exportadas ao CloudWatch têm os nomes e dimensões que os alarmes do Terraform consultam."""

import re
from pathlib import Path

import orjson

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
