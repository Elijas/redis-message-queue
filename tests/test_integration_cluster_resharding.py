"""Live slot-migration (resharding) coverage against a real multi-node cluster.

``test_cluster_moved_redirect.py`` pins MOVED/ASK handling by *simulating* a
redirect at the gateway's script-eval boundary. This file closes the remaining
gap: it performs a real ``CLUSTER SETSLOT`` + ``MIGRATE`` handshake on a live
multi-node cluster while a lease is outstanding, then drives lease renewal,
ack, and a fresh claim through the gateway. redis-py's ``RedisCluster`` is
expected to observe the ``MOVED`` reply below ``client.eval``, refresh its
slot map, and re-route transparently — none of it should surface to the
gateway.

Requires ``REDIS_CLUSTER_URL`` (the dedicated ``real-redis-cluster`` CI job);
skips locally without a cluster.
"""

import os
import time

import pytest
import redis
from redis.cluster import key_slot

from redis_message_queue import ClaimedMessage
from redis_message_queue.redis_message_queue import RedisMessageQueue

pytestmark = pytest.mark.integration

# Dedicated hash tag so a failed migration can never poison sibling tests'
# slots ({cluster-test}, {cluster-test-b1}); the fixture still migrates the
# slot back to its original owner afterwards to leave topology untouched.
RESHARDING_TAG = "{resharding-test}"

# Migration steps are individually fast (single container, loopback); this
# bounds the polling loops that wait for cluster-wide topology agreement.
_SETTLE_TIMEOUT_SECONDS = 10.0
_SETTLE_POLL_INTERVAL_SECONDS = 0.05


@pytest.fixture(scope="session")
def real_redis_cluster_url():
    url = os.environ.get("REDIS_CLUSTER_URL")
    if not url:
        pytest.skip("Redis Cluster not available")
    return url


@pytest.fixture()
def real_redis_cluster_client(real_redis_cluster_url):
    client = redis.RedisCluster.from_url(real_redis_cluster_url, socket_connect_timeout=2)
    try:
        client.ping()
    except redis.RedisError as exc:
        client.close()
        pytest.skip(f"Redis Cluster not available: {exc}")
    _delete_resharding_test_keys(client)
    yield client
    _delete_resharding_test_keys(client)
    client.close()


def _delete_resharding_test_keys(client) -> None:
    for key in list(client.scan_iter(match=f"{RESHARDING_TAG}*")):
        client.delete(key)


class _MasterNode:
    """One cluster master reached over a direct (non-cluster) connection.

    The migration handshake must address individual nodes; the routing
    ``RedisCluster`` client would re-route or fan out the ``CLUSTER SETSLOT``
    admin commands, so each master gets a plain ``redis.Redis`` connection.
    """

    def __init__(self, host: str, port: int) -> None:
        self.host = host
        self.port = port
        self.conn = redis.Redis(host=host, port=port, socket_connect_timeout=2)
        self.node_id: str = self.conn.execute_command("CLUSTER MYID").decode()

    @property
    def name(self) -> str:
        return f"{self.host}:{self.port}"

    def close(self) -> None:
        self.conn.close()


@pytest.fixture()
def cluster_masters(real_redis_cluster_client):
    masters = [_MasterNode(node.host, node.port) for node in real_redis_cluster_client.get_primaries()]
    if len(masters) < 2:
        pytest.skip("slot migration requires at least two masters")
    yield masters
    for master in masters:
        master.close()


def _master_by_name(masters: list[_MasterNode], name: str) -> _MasterNode:
    for master in masters:
        if master.name == name:
            return master
    raise AssertionError(f"no direct connection for cluster node {name!r}; masters: {[m.name for m in masters]}")


def _slot_owner_name(master: _MasterNode, probe_key: str) -> str:
    """Return ``host:port`` of the node this master believes owns the probe key's slot.

    Sends a read for the key directly to the master: the owner answers, any
    other node replies ``MOVED <slot> <host:port>`` (raised as a typed
    ``MovedError`` even on a plain client — the parser maps the prefix). This
    avoids parsing ``CLUSTER SLOTS``/``CLUSTER SHARDS`` topology payloads.
    """
    try:
        master.conn.exists(probe_key)
    except redis.exceptions.MovedError as exc:
        return f"{exc.host}:{exc.port}"
    return master.name


def _wait_for_slot_owner(masters: list[_MasterNode], probe_key: str, expected_owner: _MasterNode) -> None:
    """Poll until every master agrees the probe key's slot is owned by ``expected_owner``."""
    deadline = time.monotonic() + _SETTLE_TIMEOUT_SECONDS
    while True:
        owners = {master.name: _slot_owner_name(master, probe_key) for master in masters}
        if all(owner == expected_owner.name for owner in owners.values()):
            return
        if time.monotonic() >= deadline:
            raise AssertionError(f"slot ownership did not settle on {expected_owner.name}; current view: {owners}")
        time.sleep(_SETTLE_POLL_INTERVAL_SECONDS)


def _migrate_slot(
    masters: list[_MasterNode],
    slot: int,
    source: _MasterNode,
    target: _MasterNode,
    probe_key: str,
) -> None:
    """Live-migrate ``slot`` from ``source`` to ``target`` (real resharding handshake)."""
    target.conn.execute_command("CLUSTER SETSLOT", slot, "IMPORTING", source.node_id)
    source.conn.execute_command("CLUSTER SETSLOT", slot, "MIGRATING", target.node_id)

    while True:
        keys = source.conn.execute_command("CLUSTER GETKEYSINSLOT", slot, 100)
        if not keys:
            break
        source.conn.execute_command("MIGRATE", target.host, target.port, "", 0, 5000, "KEYS", *keys)

    # Finalize on the target first (per the Redis resharding protocol), then
    # the source, then broadcast so every master converges without waiting for
    # gossip.
    target.conn.execute_command("CLUSTER SETSLOT", slot, "NODE", target.node_id)
    source.conn.execute_command("CLUSTER SETSLOT", slot, "NODE", target.node_id)
    for master in masters:
        if master.name not in (source.name, target.name):
            master.conn.execute_command("CLUSTER SETSLOT", slot, "NODE", target.node_id)

    _wait_for_slot_owner(masters, probe_key, target)


def test_live_slot_migration_is_transparent_to_gateway_and_queue(
    real_redis_cluster_client,
    cluster_masters,
):
    """Reshard the queue's slot mid-lease; renew, ack, and a fresh claim must all succeed.

    Pins redis-py's transparent redirect-following below ``client.eval``: after
    the migration the routing client's slot map is stale, so the first script
    eval hits the old owner, gets a real ``MOVED``, and redis-py must refresh
    and re-route without the gateway ever seeing an error.
    """
    queue = RedisMessageQueue(
        RESHARDING_TAG,
        client=real_redis_cluster_client,
        deduplication=False,
        visibility_timeout_seconds=60,
        max_delivery_count=None,
    )

    # Publish + claim so a lease is outstanding across the migration.
    queue.publish("survive-resharding")
    claimed = queue._redis.wait_for_message_and_move(queue.key.pending, queue.key.processing)
    assert isinstance(claimed, ClaimedMessage)

    slot = key_slot(RESHARDING_TAG.encode("utf-8"))
    source = _master_by_name(cluster_masters, real_redis_cluster_client.get_node_from_key(queue.key.pending).name)
    target = next(master for master in cluster_masters if master.name != source.name)

    _migrate_slot(cluster_masters, slot, source, target, probe_key=queue.key.pending)
    try:
        # The data physically moved: the queue state now lives on the target.
        assert source.conn.execute_command("CLUSTER COUNTKEYSINSLOT", slot) == 0
        assert target.conn.execute_command("CLUSTER COUNTKEYSINSLOT", slot) > 0

        # Lease renewal through the gateway: the routing client's slot map is
        # stale, so this eval takes the real MOVED → refresh → re-route path.
        assert (
            queue._redis.renew_message_lease(queue.key.processing, claimed.stored_message, claimed.lease_token) is True
        )

        # Ack through the gateway completes the in-flight message.
        assert (
            queue._redis.remove_message(queue.key.processing, claimed.stored_message, lease_token=claimed.lease_token)
            is True
        )
        assert real_redis_cluster_client.llen(queue.key.pending) == 0
        assert real_redis_cluster_client.llen(queue.key.processing) == 0

        # Queue-level end-to-end after the migration: a fresh publish + claim
        # cycle completes normally against the new slot owner.
        queue.publish("post-migration")
        with queue.process_message() as message:
            assert message == b"post-migration"
        assert real_redis_cluster_client.llen(queue.key.pending) == 0
        assert real_redis_cluster_client.llen(queue.key.processing) == 0
    finally:
        # Migrate the slot back so the cluster topology is exactly as found
        # and sibling tests (and re-runs) see a clean state.
        _migrate_slot(cluster_masters, slot, target, source, probe_key=queue.key.pending)
