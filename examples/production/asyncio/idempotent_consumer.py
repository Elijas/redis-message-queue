"""Make an async consumer idempotent so at-least-once redelivery is safe.

At-least-once delivery (the default with visibility-timeout reclaim) can hand
the same payload to a consumer more than once: a crash after the side effect but
before ack, a reclaim of a slow-but-healthy handler, or a Redis clock jump all
produce duplicate deliveries. The fix is to make the side effect idempotent —
running it twice must be indistinguishable from running it once.

This example carries a stable business id in each payload and guards the side
effect with an atomic Redis ``SET <key> NX EX <ttl>``. The first delivery of a
given id wins the key and performs the side effect; a duplicate delivery finds
the key already set and skips the side effect while still acking the message.

Set REDIS_URL to override the default local Redis URL.
Set REDIS_MAX_CONNECTIONS to size the finite Redis connection pool.
"""

import asyncio
import json
import logging
import os

from redis.asyncio import Redis

from redis_message_queue.asyncio import RedisMessageQueue

REDIS_CONNECTION_STRING = os.getenv("REDIS_URL") or "redis://localhost:6379/0"
REDIS_MAX_CONNECTIONS = int(os.getenv("REDIS_MAX_CONNECTIONS", "32"))

# How long the idempotency marker survives. It must outlive the widest window
# in which a duplicate can still arrive: visibility-timeout reclaim plus
# max_delivery_count retries, plus any clock-jump slack. Too short and a late
# duplicate re-runs the side effect; too long and the markers cost more memory.
IDEMPOTENCY_TTL_SECONDS = 24 * 60 * 60

log = logging.getLogger(__name__)


async def do_side_effect(business_id: str, payload: dict) -> None:
    """Stand-in for the real, non-idempotent side effect (charge a card, send an
    email, insert a row). Runs at most once per business_id within the TTL."""
    print(f"side effect for id={business_id}: {payload['body']}")


async def handle(client: Redis, message: str) -> None:
    # process_message() yields the raw payload (str/bytes), not a message-id
    # object, so the idempotency key must come from the payload itself.
    payload = json.loads(message)
    business_id = payload["id"]

    key = f"idempotency:my_message_queue:{business_id}"
    # SET NX EX is atomic: exactly one delivery gets the key. `won` is truthy
    # only for that delivery; every duplicate sees the existing key and skips.
    won = await client.set(key, "1", nx=True, ex=IDEMPOTENCY_TTL_SECONDS)
    if not won:
        log.info("duplicate id=%s already processed; skipping side effect", business_id)
        return

    try:
        await do_side_effect(business_id, payload)
    except Exception:
        # The side effect failed, so this delivery did not "happen". Release the
        # marker (DEL) so a redelivery is allowed to retry from scratch, then let
        # the exception propagate to the failed/DLQ path. Keep the marker instead
        # (delete nothing) only if you would rather suppress all retries.
        await client.delete(key)
        raise


async def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    client = Redis.from_url(
        REDIS_CONNECTION_STRING,
        decode_responses=True,
        max_connections=REDIS_MAX_CONNECTIONS,
    )
    queue = RedisMessageQueue(
        name="my_message_queue",
        client=client,
        visibility_timeout_seconds=300,
    )

    try:
        # Publish two distinct ids, then re-publish one of them to simulate a
        # duplicate delivery. The guard runs the side effect twice total, not
        # three times.
        await queue.publish({"id": "order-1", "body": "charge order 1"})
        await queue.publish({"id": "order-2", "body": "charge order 2"})
        await queue.publish({"id": "order-1", "body": "charge order 1 (duplicate)"})

        for _ in range(3):
            async with queue.process_message() as message:
                if message is None:
                    break
                await handle(client, message)
    finally:
        await client.aclose()


if __name__ == "__main__":
    asyncio.run(main())
