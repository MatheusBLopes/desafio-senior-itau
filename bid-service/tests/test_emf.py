"""As métricas exportadas ao CloudWatch têm os nomes e dimensões que os alarmes do Terraform consultam."""

import re
from pathlib import Path

import orjson

from auction.observability import Metrics

TERRAFORM = Path(__file__).resolve().parents[2] / "infra/terraform/modules/observability/main.tf"


def test_emf_lines_match_the_metrics_used_by_alarms_and_dashboard():
    m = Metrics()
    m.outcome("ACCEPTED", None)
    m.outcome("REJECTED", "BID_TOO_LOW")
    m.conflicts(1)
    m.bid_latency(0.002)
    m.lua_latency(0.001)

    lines = [orjson.loads(line) for line in m.emf_lines("Auction")]
    emitted = {}
    for line in lines:
        spec = line["_aws"]["CloudWatchMetrics"][0]
        assert spec["Namespace"] == "Auction"
        name = spec["Metrics"][0]["Name"]
        assert line["application"] == "bid-service"
        assert all(d in line for d in spec["Dimensions"][0])  # cada dimensão tem valor na linha
        emitted.setdefault(name, []).append(line)

    # Todo nome do namespace Auction citado no Terraform é emitido.
    referenced = set(re.findall(r'"(auction\.[a-z.]+)"', TERRAFORM.read_text()))
    assert referenced, "nenhuma métrica Auction encontrada no Terraform"
    assert referenced <= set(emitted), f"faltam: {referenced - set(emitted)}"

    assert {(l["outcome"], l["reason"]) for l in emitted["auction.bids.count"]} == {
        ("ACCEPTED", "none"), ("REJECTED", "BID_TOO_LOW")}
    assert emitted["auction.persist.conflicts.count"][0]["auction.persist.conflicts.count"] == 1
    assert {l["phi"] for l in emitted["auction.bid.latency.percentile"]} == {"0.5", "0.95", "0.99"}

    # O intervalo seguinte começa do zero.
    again = {orjson.loads(x)["_aws"]["CloudWatchMetrics"][0]["Metrics"][0]["Name"]: orjson.loads(x)
             for x in m.emf_lines("Auction")}
    assert again["auction.persist.conflicts.count"]["auction.persist.conflicts.count"] == 0
    assert "auction.bids.count" not in again
