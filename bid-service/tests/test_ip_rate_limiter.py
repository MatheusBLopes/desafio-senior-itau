"""Unitário, com relógio controlado: 3 requisições de burst e reposição de 1 por segundo por IP."""

import pytest

from auction.bids import ClientIpRateLimiter, client_ip
from auction.common import ApiError
from auction.config import RateLimitConfig


class FakeClock:
    def __init__(self) -> None:
        self.now = 0.0

    def __call__(self) -> float:
        return self.now


@pytest.fixture
def clock() -> FakeClock:
    return FakeClock()


@pytest.fixture
def limiter(clock: FakeClock) -> ClientIpRateLimiter:
    return ClientIpRateLimiter(RateLimitConfig(per_ip_capacity=3, per_ip_refill_per_second=1), clock)


def assert_rate_limited(limiter: ClientIpRateLimiter, ip: str) -> None:
    with pytest.raises(ApiError) as e:
        limiter.check(ip)
    assert e.value.status == 429


def test_limits_each_ip_regardless_of_the_user_header(limiter, clock):
    for _ in range(3):
        limiter.check("203.0.113.7")
    assert_rate_limited(limiter, "203.0.113.7")

    limiter.check("198.51.100.1")

    clock.now += 1
    limiter.check("203.0.113.7")
    assert_rate_limited(limiter, "203.0.113.7")


def test_idle_buckets_are_evicted_once_full(limiter, clock):
    limiter.check("203.0.113.7")
    limiter.evict_idle()
    assert len(limiter) == 1

    clock.now += 3
    limiter.evict_idle()
    assert len(limiter) == 0


def test_client_ip_trusts_forwarded_for_only_from_private_proxies():
    # Atrás do ALB: o último endereço público do X-Forwarded-For é o cliente; os da esquerda podem ser forjados.
    assert client_ip("10.0.1.5", "1.2.3.4, 203.0.113.7") == "203.0.113.7"
    assert client_ip("10.0.1.5", "203.0.113.7, 10.0.2.9") == "203.0.113.7"
    # Conexão direta de um IP público: o cabeçalho é ignorado.
    assert client_ip("198.51.100.1", "203.0.113.7") == "198.51.100.1"
    assert client_ip("10.0.1.5", None) == "10.0.1.5"
