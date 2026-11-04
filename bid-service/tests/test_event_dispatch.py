"""Os eventos do Pub/Sub são aplicados na ordem em que foram publicados."""

import orjson

from .conftest import eventually, key, new_auction


async def test_pub_sub_events_are_applied_in_publish_order(server):
    c = server.c
    aid = await new_auction(c, 60)
    for i in range(50):
        await c.bids.place(aid, f"user{i}", key(), 10_000 + i * 100)
    await eventually(lambda: (s := c.cache.get(aid)) is not None and s.seq == 50, 5)

    # Uma reidratação (substitui o estado) seguida de um lance publicado depois dela. Se o lance fosse
    # processado antes do AUCTION_LOADED, o cache terminaria no estado reidratado (seq 60).
    channel = c.settings.events_channel
    for round_ in range(20):
        base = 60 + round_ * 2
        await c.redis.publish(channel, orjson.dumps({
            "type": "AUCTION_LOADED", "auctionId": str(aid), "seq": str(base), "userId": "x", "amount": "90000",
            "minimumNext": "90100", "endsAt": "9999999999999", "status": "OPEN"}))
        await c.redis.publish(channel, orjson.dumps({
            "type": "BID_ACCEPTED", "auctionId": str(aid), "seq": str(base + 1), "userId": "y", "amount": "90100",
            "minimumNext": "90200", "endsAt": "9999999999999", "extended": False, "serverTime": "0"}))

    await eventually(lambda: c.cache.get(aid).seq == 99, 5)
    assert c.cache.get(aid).leader_id == "y"
