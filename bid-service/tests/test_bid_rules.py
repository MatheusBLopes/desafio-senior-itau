"""Regras do place_bid.lua, exercitadas contra um Valkey real."""

from datetime import datetime, timedelta
from decimal import Decimal

from auction.core.time import to_datetime
from auction.storage import keys
from auction.lifecycle.models import CreateAuctionRequest
from auction.bidding.result import ACCEPTED, AUCTION_CLOSED, BID_TOO_LOW, IDEMPOTENCY_KEY_REUSED, REJECTED

from .conftest import eventually, key, new_auction


async def test_first_bid_must_cover_starting_price_without_increment(server):
    c = server.c
    aid = await new_auction(c, 60)

    assert (await c.bids.place(aid, "ana", key(), 9_999)).reason == BID_TOO_LOW

    first = await c.bids.place(aid, "ana", key(), 10_000)
    assert first.outcome == ACCEPTED
    assert first.seq == 1
    assert first.minimum_next == 10_100


async def test_next_bids_need_current_price_plus_increment_and_tie_goes_to_first(server):
    c = server.c
    aid = await new_auction(c, 60)
    await c.bids.place(aid, "ana", key(), 10_000)

    tie = await c.bids.place(aid, "bia", key(), 10_000)
    assert tie.outcome == REJECTED
    assert tie.reason == BID_TOO_LOW

    assert (await c.bids.place(aid, "bia", key(), 10_099)).reason == BID_TOO_LOW

    ok = await c.bids.place(aid, "bia", key(), 10_100)
    assert ok.outcome == ACCEPTED
    assert ok.seq == 2
    assert (await c.auctions.get(aid))["leaderId"] == "bia"


async def test_retry_returns_original_result_instead_of_duplicating(server):
    c = server.c
    aid = await new_auction(c, 60)
    k = key()

    original = await c.bids.place(aid, "ana", k, 10_000)
    retry = await c.bids.place(aid, "ana", k, 10_000)

    assert retry.replayed
    assert retry.seq == original.seq
    assert (await c.auctions.get(aid))["seq"] == 1


async def test_retry_of_a_rejection_returns_the_same_rejection(server):
    c = server.c
    aid = await new_auction(c, 60)
    await c.bids.place(aid, "ana", key(), 20_000)
    k = key()

    rejected = await c.bids.place(aid, "bia", k, 15_000)
    retry = await c.bids.place(aid, "bia", k, 15_000)

    assert rejected.reason == BID_TOO_LOW
    assert retry.reason == BID_TOO_LOW


async def test_same_key_with_different_amount_is_refused(server):
    c = server.c
    aid = await new_auction(c, 60)
    k = key()
    await c.bids.place(aid, "ana", k, 10_000)

    reused = await c.bids.place(aid, "ana", k, 50_000)

    assert reused.reason == IDEMPOTENCY_KEY_REUSED
    assert (await c.auctions.get(aid))["currentPrice"] == "100.00"


async def test_idempotency_key_is_scoped_per_user(server):
    c = server.c
    aid = await new_auction(c, 60)
    shared = key()

    assert (await c.bids.place(aid, "ana", shared, 10_000)).accepted
    other = await c.bids.place(aid, "bia", shared, 10_100)

    assert not other.replayed
    assert other.seq == 2


async def test_bid_after_deadline_is_rejected_even_before_the_closer_runs(server):
    c = server.c
    aid = await new_auction(c, 5)

    async def closed() -> bool:
        return not (await c.auctions.get(aid))["acceptingBids"]

    await eventually(closed, 10)

    late = await c.bids.place(aid, "ana", key(), 10_000)

    assert late.reason == AUCTION_CLOSED


async def test_bid_inside_anti_sniping_window_extends_deadline(server):
    # Janela de 10 s num leilão de 5 s: qualquer lance cai na janela e empurra o fim para agora + 30 s.
    c = server.c
    aid = await new_auction(c, 5, 10, 30)
    before = datetime.fromisoformat((await c.auctions.get(aid))["endsAt"])

    bid = await c.bids.place(aid, "ana", key(), 10_000)

    assert bid.extended
    assert to_datetime(bid.ends_at) > before + timedelta(seconds=20)


async def test_pre_filter_rejects_locally_but_still_honours_retries(server):
    c = server.c
    aid = await new_auction(c, 60)
    k = key()
    await c.bids.place(aid, "ana", k, 10_000)
    await c.bids.place(aid, "bia", key(), 50_000)

    # O cache da task é alimentado pelo Pub/Sub; espera o lance de 500,00 chegar.
    async def filtered() -> bool:
        return (await c.bids.place(aid, "caio", key(), 20_000)).filtered

    await eventually(filtered, 5)

    retry = await c.bids.place(aid, "ana", k, 10_000)
    assert not retry.filtered
    assert retry.replayed
    assert retry.outcome == ACCEPTED
    assert retry.amount == 10_000


async def test_pre_filter_rejection_does_not_lock_the_key_but_never_duplicates(server):
    # Contrato: só um lance avaliado pelo Lua trava a key. A rejeição local por lance
    # superado não é gravada, para não custar uma key no Valkey por lance no pico.
    c = server.c
    aid = await new_auction(c, 60)
    await c.bids.place(aid, "ana", key(), 50_000)
    await eventually(lambda: (s := c.cache.get(aid)) is not None and s.seq == 1, 5)
    k = key()

    rejected = await c.bids.place(aid, "bia", k, 20_000)
    assert rejected.filtered
    assert not await c.redis.exists(keys.idempotency_key(aid, "bia", k))
    assert (await c.bids.place(aid, "bia", k, 20_000)).reason == BID_TOO_LOW

    new_bid = await c.bids.place(aid, "bia", k, 60_000)
    assert new_bid.accepted
    retry = await c.bids.place(aid, "bia", k, 60_000)
    assert retry.replayed
    assert retry.seq == new_bid.seq
    assert (await c.auctions.get(aid))["seq"] == 2


async def test_large_amounts_keep_cent_precision(server):
    # Perto do teto (12 dígitos inteiros): o próximo mínimo tem 15 dígitos em centavos, além do %.14g do cjson.
    c = server.c
    top = Decimal("999999999999.99")
    view = await c.auctions.create(CreateAuctionRequest(
        title="Item caro", startingPrice=top, minIncrement=top, durationSeconds=60,
        antiSnipingWindowSeconds=0, antiSnipingExtendSeconds=0))
    aid = keys.auction_id_or_none(view["id"])

    first = await c.bids.place(aid, "ana", key(), 99_999_999_999_999)

    assert first.to_json()["minimumNextBid"] == "1999999999999.98"
    await eventually(lambda: (s := c.cache.get(aid)) is not None and s.seq == 1, 5)
    assert c.cache.get(aid).minimum_next == 199_999_999_999_998
    assert (await c.bids.place(aid, "bia", key(), 199_999_999_999_997)).reason == BID_TOO_LOW
    assert (await c.bids.place(aid, "bia", key(), 199_999_999_999_998)).accepted


async def test_accepted_key_outlives_the_auction_while_rejection_keeps_the_ttl(server):
    # Leilão de 3 dias com TTL de 24 h: a key do aceite não pode expirar com o leilão aberto.
    c = server.c
    aid = await new_auction(c, 3 * 24 * 3600)
    accepted, rejected = key(), key()
    await c.bids.place(aid, "ana", accepted, 10_000)
    # Sem o cache da task, a rejeição vem do Lua (que a memoriza) e não do pré-filtro.
    await eventually(lambda: (s := c.cache.get(aid)) is not None and s.seq == 1, 5)
    c.cache.evict(aid)
    assert not (await c.bids.place(aid, "bia", rejected, 10_000)).filtered

    hour = 3600
    assert await c.redis.ttl(keys.idempotency_key(aid, "ana", accepted)) >= (3 * 24 + 23) * hour
    assert await c.redis.ttl(keys.idempotency_key(aid, "bia", rejected)) <= 24 * hour
