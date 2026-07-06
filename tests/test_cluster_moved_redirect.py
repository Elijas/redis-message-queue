"""Simulated Redis Cluster MOVED/ASK redirect coverage for the lease path.

Why this file exists
--------------------
``tests/test_integration_cluster.py`` covers hash-tagged happy paths and the
cross-slot ``pcall`` regressions, but nothing exercises what happens when a
slot migration (MOVED/ASK redirect) lands *while a lease is outstanding*.
``docs/production-readiness.md`` lists this as a known gap: a real 3-node
resharding fixture is needed to close it, and none is available in CI.

What is simulated vs. what a real cluster would do
--------------------------------------------------
These tests do NOT stand up a multi-node cluster. They run against an ordinary
single-node Redis and inject a ``redis.exceptions.MovedError`` / ``AskError``
on the FIRST invocation of a lease-path script eval, then let the operation
succeed on retry. That mimics a slot migrating mid-lease: the first attempt is
rejected with a redirect, a later attempt (after the migration settles) works.

The important caveat, stated honestly: with a real ``redis.RedisCluster``
client, redis-py intercepts MOVED/ASK *below* ``client.eval`` — it refreshes
its slot map and re-issues the command against the new node transparently, so
the gateway never observes a bare ``MovedError`` during an ordinary single-slot
migration. A bare redirect only reaches application code once redis-py has
exhausted its own redirect-following (a genuinely degraded/unstable cluster).
By injecting at the gateway's ``_eval`` boundary we reproduce exactly that
"redirect escaped redis-py" case and pin how this library behaves when it does.

Behavior pinned here (current code, verified against a live single-node Redis)
------------------------------------------------------------------------------
* ``MovedError`` and ``AskError`` are ``redis.exceptions.ResponseError``
  subclasses. ``is_redis_retryable_exception`` classifies them as NOT
  retryable, so the gateway's tenacity retry makes a single attempt and
  re-raises the original redirect error — a documented, catchable exception.
* ``_eval`` does not misclassify a redirect as a Lua-script error:
  ``wrap_lua_response_error`` only rewraps ``WRONGTYPE``/``OOM`` messages, so a
  MOVED/ASK ``ResponseError`` propagates unchanged rather than as
  ``LuaScriptError``.
* Because the redirect replaces the eval (the Lua body never runs), lease
  bookkeeping is left intact and consistent: the claim path leaves the message
  in ``pending``; the ack/renew paths leave the message in ``processing`` with
  its lease token. A subsequent call (migration settled) then succeeds. The
  redirect does NOT poison the lease — nothing is stranded.
"""

import pytest
from redis.exceptions import AskError, MovedError, ResponseError

from redis_message_queue._config import is_redis_retryable_exception
from redis_message_queue.redis_message_queue import RedisMessageQueue

pytestmark = pytest.mark.integration

# Both redirect flavours share one code path in this library; parametrizing over
# them documents that ASK is handled identically to MOVED today.
REDIRECT_ERRORS = [
    pytest.param(lambda: MovedError("1234 127.0.0.1:6380"), id="MOVED"),
    pytest.param(lambda: AskError("1234 127.0.0.1:6380"), id="ASK"),
]


def _make_lease_queue(client, queue_name):
    return RedisMessageQueue(
        queue_name,
        client=client,
        deduplication=False,
        visibility_timeout_seconds=30,
        max_delivery_count=None,
    )


def _inject_redirect_on_first_eval(gateway, redirect_error: ResponseError) -> None:
    """Make the gateway's next script eval raise ``redirect_error`` once.

    Patches ``RedisGateway._eval`` (the single choke point every lease-path Lua
    call goes through) so the FIRST call raises the injected redirect and every
    later call delegates to the real implementation — simulating a slot that has
    migrated for one attempt and then settled.
    """
    original_eval = gateway._eval
    state = {"calls": 0}

    def eval_with_redirect(*args, **kwargs):
        state["calls"] += 1
        if state["calls"] == 1:
            raise redirect_error
        return original_eval(*args, **kwargs)

    gateway._eval = eval_with_redirect


def _claim_one(queue):
    claimed = queue._redis.wait_for_message_and_move(queue.key.pending, queue.key.processing)
    assert claimed is not None
    return claimed


@pytest.mark.parametrize("make_redirect", REDIRECT_ERRORS)
def test_redirect_is_catchable_non_retryable_response_error(make_redirect):
    """A MOVED/ASK redirect is a catchable ``ResponseError`` the retry layer treats as non-retryable.

    Pins the classification the lease path depends on: redirects are NOT in the
    retryable set, so the gateway surfaces them instead of silently looping.
    """
    redirect = make_redirect()
    assert isinstance(redirect, ResponseError)
    assert is_redis_retryable_exception(redirect) is False


@pytest.mark.parametrize("make_redirect", REDIRECT_ERRORS)
def test_renew_lease_surfaces_redirect_then_recovers(make_redirect, real_redis_client, queue_name):
    """renew_message_lease surfaces a mid-lease redirect, leaving the lease reclaimable.

    Simulates a slot migration during a heartbeat renewal. The gateway makes a
    single eval attempt, re-raises the raw redirect (non-retryable), and the
    lease token / processing entry are left intact so a settled-migration retry
    renews normally.
    """
    redirect = make_redirect()
    queue = _make_lease_queue(real_redis_client, queue_name)
    queue.publish("renew-me")
    claimed = _claim_one(queue)

    lease_tokens_key = queue._redis._lease_tokens_key(queue.key.processing)
    _inject_redirect_on_first_eval(queue._redis, redirect)

    with pytest.raises(type(redirect)) as excinfo:
        queue._redis.renew_message_lease(queue.key.processing, claimed.stored_message, claimed.lease_token)
    assert excinfo.value is redirect

    # Lease bookkeeping untouched: the Lua body never ran under the redirect.
    assert real_redis_client.llen(queue.key.processing) == 1
    assert real_redis_client.hlen(lease_tokens_key) == 1

    # Migration settled: the same renewal now succeeds transparently.
    assert queue._redis.renew_message_lease(queue.key.processing, claimed.stored_message, claimed.lease_token) is True


@pytest.mark.parametrize("make_redirect", REDIRECT_ERRORS)
def test_ack_remove_surfaces_redirect_then_recovers(make_redirect, real_redis_client, queue_name):
    """remove_message (ack) surfaces a mid-lease redirect without losing the message.

    The redirect replaces the ack's Lua eval, so the message stays in
    ``processing`` under its lease. A retry after the simulated migration
    removes it exactly once.
    """
    redirect = make_redirect()
    queue = _make_lease_queue(real_redis_client, queue_name)
    queue.publish("ack-me")
    claimed = _claim_one(queue)

    _inject_redirect_on_first_eval(queue._redis, redirect)

    with pytest.raises(type(redirect)):
        queue._redis.remove_message(queue.key.processing, claimed.stored_message, lease_token=claimed.lease_token)

    # Not acked, not stranded: still claimable/reclaimable.
    assert real_redis_client.llen(queue.key.processing) == 1

    assert (
        queue._redis.remove_message(queue.key.processing, claimed.stored_message, lease_token=claimed.lease_token)
        is True
    )
    assert real_redis_client.llen(queue.key.processing) == 0


@pytest.mark.parametrize("make_redirect", REDIRECT_ERRORS)
def test_ack_move_surfaces_redirect_then_recovers(make_redirect, real_redis_client, queue_name):
    """move_message (ack-to-completed) surfaces a mid-lease redirect without losing the message."""
    redirect = make_redirect()
    queue = _make_lease_queue(real_redis_client, queue_name)
    queue.publish("move-me")
    claimed = _claim_one(queue)

    _inject_redirect_on_first_eval(queue._redis, redirect)

    with pytest.raises(type(redirect)):
        queue._redis.move_message(
            queue.key.processing,
            queue.key.completed,
            claimed.stored_message,
            lease_token=claimed.lease_token,
        )

    assert real_redis_client.llen(queue.key.processing) == 1
    assert real_redis_client.llen(queue.key.completed) == 0

    assert (
        queue._redis.move_message(
            queue.key.processing,
            queue.key.completed,
            claimed.stored_message,
            lease_token=claimed.lease_token,
        )
        is True
    )
    assert real_redis_client.llen(queue.key.processing) == 0
    assert real_redis_client.llen(queue.key.completed) == 1


@pytest.mark.parametrize("make_redirect", REDIRECT_ERRORS)
def test_claim_surfaces_redirect_leaving_message_in_pending(make_redirect, real_redis_client, queue_name):
    """wait_for_message_and_move surfaces a redirect and leaves the message safely in pending.

    The claim's Lua eval is what atomically moves pending -> processing. A
    redirect there means nothing moved: the message is still in ``pending`` and
    a retry claims it normally.
    """
    redirect = make_redirect()
    queue = _make_lease_queue(real_redis_client, queue_name)
    queue.publish("claim-me")

    _inject_redirect_on_first_eval(queue._redis, redirect)

    with pytest.raises(type(redirect)):
        queue._redis.wait_for_message_and_move(queue.key.pending, queue.key.processing)

    assert real_redis_client.llen(queue.key.pending) == 1
    assert real_redis_client.llen(queue.key.processing) == 0

    claimed = queue._redis.wait_for_message_and_move(queue.key.pending, queue.key.processing)
    assert claimed is not None
    assert real_redis_client.llen(queue.key.pending) == 0
    assert real_redis_client.llen(queue.key.processing) == 1


def test_redirect_not_rewrapped_as_lua_script_error(real_redis_client, queue_name):
    """A redirect ``ResponseError`` propagates unchanged, not as ``LuaScriptError``.

    ``_eval`` catches ``ResponseError`` to rewrap only WRONGTYPE/OOM Lua errors;
    this guards that a MOVED/ASK redirect is never misclassified into the
    library's own ``LuaScriptError`` type (which would change its retry and
    catch semantics).
    """
    from redis_message_queue._exceptions import LuaScriptError

    redirect = MovedError("1234 127.0.0.1:6380")
    queue = _make_lease_queue(real_redis_client, queue_name)
    queue.publish("wrap-check")
    claimed = _claim_one(queue)

    _inject_redirect_on_first_eval(queue._redis, redirect)
    with pytest.raises(ResponseError) as excinfo:
        queue._redis.renew_message_lease(queue.key.processing, claimed.stored_message, claimed.lease_token)

    assert type(excinfo.value) is MovedError
    assert not isinstance(excinfo.value, LuaScriptError)


# KNOWN UNKNOWN: this simulates redirects escaping to the gateway by patching
# _eval; with a real redis.RedisCluster client, redirect-following happens below
# client.eval, so a bare MovedError only surfaces once redis-py exhausts its own
# retries. A true 3-node resharding fixture would confirm the below-the-gateway
# transparent-follow path that this test cannot reach.
