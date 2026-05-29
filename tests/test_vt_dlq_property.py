"""Property/fuzz the visibility-timeout ``delivery_count`` -> dead-letter machine.

This complements ``tests/_model_based.py`` rather than overlapping it. That model
constructs its gateway with **no** ``max_delivery_count`` and **no**
``dead_letter_queue`` (see ``_run_model_test``), so delivery-count accounting and
DLQ routing are entirely unmodeled there. This module fills that gap: it drives a
gateway with the DLQ machine **active** and walks randomized interleavings of the
redeliver -> max_delivery -> dead-letter arc.

Why visibility-timeout expiry is the redelivery mechanism: the only redelivery path
in this library is lease expiry + reclaim. ``process_message`` treats handler
exceptions as TERMINAL (move-to-failed / remove), not requeue, so "nack -> redeliver"
== "lease expiry -> reclaim". We simulate expiry deterministically by forcing a lease
deadline into the past (``zadd`` score=0) -- the same technique the in-tree DLQ tests
use (``tests/test_dead_letter.py``). No real sleeps; fully deterministic per seed.

Drift-resistant by construction:
  * Every publish uses a globally-unique payload (``deduplication=False``), so each
    queue location holds a payload at most once and conservation is a clean set
    relation.
  * All structural / conservation invariants are checked against ACTUAL Redis state
    read every step -- the Python model never predicts which message the Lua claims,
    so it cannot drift into false positives from list-ordering assumptions.
  * The single quantity the model "tracks" is ``delivered[payload]`` = number of times
    a claim RETURNED that payload (an observed output), plus tokens superseded across a
    claim (computed by diffing the ``lease_tokens`` hash before/after). Both are derived
    from observed state, not predicted.

Invariants asserted after every step (map to the OH-B3 audit lane's 5 invariants):
  INV-1  Conservation / no-loss: the multiset of payloads across
         {pending, processing, completed, failed, dead_letter, removed} equals exactly
         the set of published payloads -- each in exactly one location, none lost,
         invented, or duplicated.
  INV-2a delivery_count bounded: every ``delivery_counts`` value is in
         ``[1, max_delivery_count]``, and every key is an envelope currently in pending
         OR processing (terminal ops -- ack/fail/remove/DLQ -- all HDEL the count).
  INV-2b delivery_count exact vs. observed: for every message that still has a count,
         the server-side count == ``delivered[payload]``. Catches spurious in/decrements.
  INV-2c DLQ threshold: a message is dead-lettered only when its count exceeds
         ``max_delivery_count``. The DLQ event reports ``delivery_count == max+1`` and the
         message was returned to a consumer exactly ``max_delivery_count`` times first.
  INV-3  Single active claim: ``lease_tokens`` keys == processing members, all token
         values distinct, and processing has no duplicate envelopes.
  INV-4  Stale-token safety: ack/remove/renew with a superseded token returns False and
         mutates nothing.
  INV-5  No resurrection: a dead-lettered payload never reappears in pending/processing,
         and a stale renew never restores a deadline.

A drain epilogue then forces every live message terminal and asserts the queue is fully
empty with no orphaned lease/deadline/delivery metadata.

On failure the run raises ``AssertionError`` with the full op trace + the seed, so the
failing sequence reproduces exactly (deterministic RNG).

CI cost: ``SEEDS`` x ``len(PARAM_GRID)`` x ``STEPS`` operations (see constants below).
The portable audit harness this is derived from used 40 seeds x 8 combos x 200 steps
(~64k ops, ~50s). This banked form trims the seed count for a CI-reasonable budget while
preserving the param grid and per-step invariant depth; tune ``SEEDS``/``STEPS`` to drive
the machine harder locally.
"""

import random
import time
from collections import Counter
from dataclasses import dataclass, field

import fakeredis
import pytest

from redis_message_queue import EventOperation
from redis_message_queue._redis_gateway import RedisGateway
from redis_message_queue._stored_message import ClaimedMessage, extract_stored_message_id
from redis_message_queue.redis_message_queue import RedisMessageQueue

VT_SECONDS = 30  # nominal lease; never waited out -- expiry is simulated via zadd score=0


def _decode(raw):
    return raw.decode("utf-8") if isinstance(raw, (bytes, bytearray)) else raw


@dataclass
class Model:
    """Observed-state shadow. Identity unit is the unique payload string."""

    published: set = field(default_factory=set)
    msgid_to_payload: dict = field(default_factory=dict)
    env_to_payload: dict = field(default_factory=dict)
    delivered: Counter = field(default_factory=Counter)  # payload -> times a claim returned it
    dlqd: set = field(default_factory=set)  # payloads observed dead-lettered (terminal)
    removed: set = field(default_factory=set)  # payloads acked via remove_message (terminal, no queue)
    stale_tokens: list = field(default_factory=list)  # lease tokens known to be superseded
    payload_counter: int = 0


class Harness:
    def __init__(self, seed, max_delivery_count, enable_completed, enable_failed, queue_name="ohb3"):
        self.rng = random.Random(seed)
        self.seed = seed
        self.max_dc = max_delivery_count
        self.enable_completed = enable_completed
        self.enable_failed = enable_failed
        self.client = fakeredis.FakeRedis()
        self.dlq_key = f"{queue_name}::dead_letter"
        self.gateway = RedisGateway(
            redis_client=self.client,
            retry_budget_seconds=0,
            message_wait_interval_seconds=0,
            message_visibility_timeout_seconds=VT_SECONDS,
            max_delivery_count=max_delivery_count,
            dead_letter_queue=self.dlq_key,
        )
        self.events = []
        self.queue = RedisMessageQueue(
            queue_name,
            gateway=self.gateway,
            deduplication=False,
            enable_completed_queue=enable_completed,
            enable_failed_queue=enable_failed,
            on_event=self.events.append,
        )
        self.model = Model()
        self.history = []

        self.k_pending = self.queue.key.pending
        self.k_processing = self.queue.key.processing
        self.k_completed = self.queue.key.completed
        self.k_failed = self.queue.key.failed
        self.k_tokens = self.gateway._lease_tokens_key(self.k_processing)
        self.k_deadlines = self.gateway._lease_deadlines_key(self.k_processing)
        self.k_counts = self.gateway._delivery_counts_key(self.k_processing)
        # The gateway LPUSHes dead-letters to exactly the string we configured.
        assert self.gateway._optional_dead_letter_key(self.k_processing) == self.dlq_key

    # -- redis readers -------------------------------------------------

    def _processing_tokens(self):
        return {k: _decode(v) for k, v in self.client.hgetall(self.k_tokens).items()}

    def _pending_envs(self):
        return self.client.lrange(self.k_pending, 0, -1)

    def _processing_envs(self):
        return self.client.lrange(self.k_processing, 0, -1)

    # -- commands ------------------------------------------------------

    def cmd_publish(self):
        m = self.model
        m.payload_counter += 1
        payload = f"m{m.payload_counter}"
        accepted = self.queue.publish(payload)
        assert accepted is True, f"publish({payload!r}) rejected"
        env = self.client.lindex(self.k_pending, 0)  # LPUSH -> newest at head
        mid = extract_stored_message_id(env)
        m.published.add(payload)
        m.env_to_payload[env] = payload
        m.msgid_to_payload[mid] = payload
        return f"publish({payload})"

    def _consume_events_since(self, start):
        """Process DLQ/reclaim events emitted since index `start`, asserting INV-2c."""
        for ev in self.events[start:]:
            if ev.operation is EventOperation.DLQ:
                payload = self.model.msgid_to_payload[ev.message_id]
                assert ev.delivery_count == self.max_dc + 1, (
                    f"DLQ event delivery_count={ev.delivery_count}, expected max+1={self.max_dc + 1} for {payload!r}"
                )
                assert ev.max_delivery_count == self.max_dc, (
                    f"DLQ event max_delivery_count={ev.max_delivery_count}, expected {self.max_dc}"
                )
                assert self.model.delivered[payload] == self.max_dc, (
                    f"DLQ'd {payload!r} was delivered {self.model.delivered[payload]} times, "
                    f"expected exactly max_delivery_count={self.max_dc} before dead-lettering"
                )
                self.model.dlqd.add(payload)

    def cmd_claim(self):
        tokens_before = self._processing_tokens()
        ev_start = len(self.events)
        result = self.gateway.wait_for_message_and_move(self.k_pending, self.k_processing)
        self._consume_events_since(ev_start)

        if isinstance(result, ClaimedMessage):
            env = result.stored_message
            payload = self.model.env_to_payload[env]
            self.model.delivered[payload] += 1
            # the returned token must match what's now in the hash for this env
            assert _decode(self.client.hget(self.k_tokens, env)) == result.lease_token, (
                f"returned lease_token {result.lease_token} != stored token for {payload!r}"
            )

        # Any env whose token changed or vanished across this claim => old token now stale.
        tokens_after = self._processing_tokens()
        for env, tok in tokens_before.items():
            if tokens_after.get(env) != tok:
                self.model.stale_tokens.append(tok)

        if result is None:
            return "claim->None"
        return f"claim->{self.model.env_to_payload[result.stored_message]}"

    def cmd_expire(self):
        envs = list(self._processing_tokens().keys())
        env = self.rng.choice(envs)
        self.client.zadd(self.k_deadlines, {env: 0})  # score 0 << now => expired
        return f"expire({self.model.env_to_payload[env]})"

    def _terminal_ack(self, to_completed):
        tokens = self._processing_tokens()
        env = self.rng.choice(list(tokens.keys()))
        token = tokens[env]
        payload = self.model.env_to_payload[env]
        dest_enabled = self.enable_completed if to_completed else self.enable_failed
        dest_key = self.k_completed if to_completed else self.k_failed
        if dest_enabled:
            applied = self.gateway.move_message(self.k_processing, dest_key, env, lease_token=token)
            assert applied is True, f"ack move_message returned {applied!r}, expected True"
        else:
            applied = self.gateway.remove_message(self.k_processing, env, lease_token=token)
            assert applied is True, f"ack remove_message returned {applied!r}, expected True"
            self.model.removed.add(payload)
        self.model.stale_tokens.append(token)
        label = "complete" if to_completed else "fail"
        return f"{label}({payload})"

    def cmd_ack_complete(self):
        return self._terminal_ack(to_completed=True)

    def cmd_ack_fail(self):
        return self._terminal_ack(to_completed=False)

    def cmd_renew(self):
        tokens = self._processing_tokens()
        env = self.rng.choice(list(tokens.keys()))
        token = tokens[env]
        result = self.gateway.renew_message_lease(self.k_processing, env, token)
        assert result is True, f"valid renew returned {result!r}, expected True"
        score = self.client.zscore(self.k_deadlines, env)
        now_ms = time.time() * 1000
        assert score is not None and score > now_ms, (
            f"renew did not push deadline into the future: score={score} now_ms~={now_ms:.0f}"
        )
        return f"renew({self.model.env_to_payload[env]})"

    def cmd_stale_renew(self):
        tokens = self._processing_tokens()
        env = self.rng.choice(list(tokens.keys()))
        stale = self.rng.choice(self.model.stale_tokens)
        assert stale != tokens[env], "internal: stale token coincides with active token"
        score_before = self.client.zscore(self.k_deadlines, env)
        proc_before = self.client.lrange(self.k_processing, 0, -1)
        result = self.gateway.renew_message_lease(self.k_processing, env, stale)
        assert result is False, f"stale renew returned {result!r}, expected False"
        assert self.client.zscore(self.k_deadlines, env) == score_before, "stale renew mutated a deadline"
        assert self.client.lrange(self.k_processing, 0, -1) == proc_before, "stale renew mutated processing"
        return f"stale_renew(stale={stale})"

    def cmd_stale_ack(self):
        tokens = self._processing_tokens()
        env = self.rng.choice(list(tokens.keys()))
        stale = self.rng.choice(self.model.stale_tokens)
        assert stale != tokens[env], "internal: stale token coincides with active token"
        to_completed = self.rng.random() < 0.5
        dest_enabled = self.enable_completed if to_completed else self.enable_failed
        dest_key = self.k_completed if to_completed else self.k_failed
        proc_before = self.client.llen(self.k_processing)
        dest_before = self.client.llen(dest_key) if dest_enabled else None
        dlq_before = self.client.llen(self.dlq_key)
        if dest_enabled:
            result = self.gateway.move_message(self.k_processing, dest_key, env, lease_token=stale)
        else:
            result = self.gateway.remove_message(self.k_processing, env, lease_token=stale)
        assert result is False, f"stale ack returned {result!r}, expected False"
        assert self.client.llen(self.k_processing) == proc_before, "stale ack mutated processing"
        if dest_enabled:
            assert self.client.llen(dest_key) == dest_before, "stale ack mutated destination queue"
        assert self.client.llen(self.dlq_key) == dlq_before, "stale ack mutated DLQ"
        return f"stale_ack(stale={stale})"

    # -- command picker ------------------------------------------------

    def pick(self):
        m = self.model
        has_processing = bool(self._processing_tokens())
        has_stale = bool(m.stale_tokens)
        choices = [("publish", 25), ("claim", 30)]
        if has_processing:
            choices += [("expire", 22), ("ack_complete", 8), ("ack_fail", 6), ("renew", 6)]
            if has_stale:
                choices += [("stale_renew", 4), ("stale_ack", 4)]
        names = [c[0] for c in choices]
        weights = [c[1] for c in choices]
        return self.rng.choices(names, weights=weights, k=1)[0]

    def step(self):
        name = self.pick()
        desc = getattr(self, f"cmd_{name}")()
        self.history.append(desc)
        self.check_invariants(desc)

    # -- invariant checker --------------------------------------------

    def check_invariants(self, step_desc):
        try:
            self._check()
        except AssertionError as exc:
            lines = [
                f"INVARIANT VIOLATION (seed={self.seed}, max_dc={self.max_dc}, "
                f"completed={self.enable_completed}, failed={self.enable_failed})",
                f"At step: {step_desc}",
                f"Error: {exc}",
                "",
                f"Full history ({len(self.history)} ops):",
            ]
            for i, h in enumerate(self.history):
                marker = ">>>" if i == len(self.history) - 1 else "   "
                lines.append(f" {marker} {i:4d}: {h}")
            raise AssertionError("\n".join(lines)) from None

    def _check(self):
        m = self.model
        pending_envs = self._pending_envs()
        processing_envs = self._processing_envs()
        tokens = self._processing_tokens()  # env -> token
        completed_raw = self.client.lrange(self.k_completed, 0, -1) if self.enable_completed else []
        failed_raw = self.client.lrange(self.k_failed, 0, -1) if self.enable_failed else []
        dlq_raw = self.client.lrange(self.dlq_key, 0, -1)
        counts = {k: int(v) for k, v in self.client.hgetall(self.k_counts).items()}
        deadline_envs = set(self.client.zrange(self.k_deadlines, 0, -1))

        # INV-3: single active claim / lease metadata consistency.
        assert set(tokens.keys()) == set(processing_envs), (
            f"lease_tokens keys {len(tokens)} != processing members {len(set(processing_envs))}"
        )
        assert deadline_envs == set(processing_envs), (
            f"lease_deadlines members != processing members "
            f"(|deadlines|={len(deadline_envs)}, |processing|={len(set(processing_envs))})"
        )
        token_values = list(tokens.values())
        assert len(token_values) == len(set(token_values)), "duplicate lease token across active claims"
        assert len(processing_envs) == len(set(processing_envs)), "duplicate envelope in processing"
        assert len(pending_envs) == len(set(pending_envs)), "duplicate envelope in pending"
        if not processing_envs:
            assert not tokens, "processing empty but lease_tokens non-empty"
            assert not deadline_envs, "processing empty but lease_deadlines non-empty"

        # INV-1: conservation / no-loss. Each payload in exactly one location.
        locations = []
        locations += [m.env_to_payload[e] for e in pending_envs]
        locations += [m.env_to_payload[e] for e in processing_envs]
        locations += [_decode(r) for r in completed_raw]
        locations += [_decode(r) for r in failed_raw]
        locations += [_decode(r) for r in dlq_raw]
        locations += list(m.removed)
        dupes = {p: c for p, c in Counter(locations).items() if c > 1}
        assert not dupes, f"payload(s) in >1 location (no-dup/no-resurrection): {dupes}"
        loc_set = set(locations)
        assert loc_set == m.published, (
            f"conservation breach: missing={sorted(m.published - loc_set)[:10]} "
            f"unexpected={sorted(loc_set - m.published)[:10]}"
        )

        # INV-2a: delivery_count bounded + scoped to pending|processing envelopes.
        active_envs = set(pending_envs) | set(processing_envs)
        for env, c in counts.items():
            assert env in active_envs, (
                f"delivery_counts entry for {m.env_to_payload.get(env, env)!r} not in pending|processing "
                f"(orphaned count after a terminal op)"
            )
            assert 1 <= c <= self.max_dc, (
                f"delivery_count {c} out of bounds [1,{self.max_dc}] for {m.env_to_payload[env]!r}"
            )

        # INV-2b: server-side count == observed deliveries for every live message.
        for env, c in counts.items():
            payload = m.env_to_payload[env]
            assert c == m.delivered[payload], (
                f"delivery_count drift for {payload!r}: redis={c} vs observed deliveries={m.delivered[payload]}"
            )

        # INV-2c / INV-5: dead-lettered payloads are terminal in the DLQ and never resurface.
        dlq_payloads = {_decode(r) for r in dlq_raw}
        for payload in m.dlqd:
            assert payload in dlq_payloads, f"dead-lettered {payload!r} missing from DLQ list"
            assert m.delivered[payload] == self.max_dc, (
                f"dead-lettered {payload!r} delivered {m.delivered[payload]}x, expected {self.max_dc}"
            )

    # -- drain epilogue ------------------------------------------------

    def drain(self):
        """Force every live message to a terminal state; assert fully-empty + no orphans."""
        safety = len(self.model.published) + 50
        for _ in range(safety):
            # Ack everything currently in processing (no redelivery).
            while True:
                tokens = self._processing_tokens()
                if not tokens:
                    break
                self.step_drain_ack(tokens)
            if self.client.llen(self.k_pending) == 0:
                break
            ev_start = len(self.events)
            self.gateway.wait_for_message_and_move(self.k_pending, self.k_processing)
            self._consume_events_since(ev_start)
        # Final sweep of any straggler the last claim left in processing.
        while True:
            tokens = self._processing_tokens()
            if not tokens:
                break
            self.step_drain_ack(tokens)

        assert self.client.llen(self.k_pending) == 0, "drain: pending not empty"
        assert self.client.llen(self.k_processing) == 0, "drain: processing not empty"
        assert self.client.hlen(self.k_tokens) == 0, "drain: lease_tokens not empty"
        assert self.client.zcard(self.k_deadlines) == 0, "drain: lease_deadlines not empty"
        assert self.client.hlen(self.k_counts) == 0, "drain: delivery_counts not empty"

        completed = self.client.llen(self.k_completed) if self.enable_completed else 0
        failed = self.client.llen(self.k_failed) if self.enable_failed else 0
        dlq = self.client.llen(self.dlq_key)
        removed = len(self.model.removed)
        terminal = completed + failed + dlq + removed
        assert terminal == len(self.model.published), (
            f"drain conservation: completed({completed})+failed({failed})+dlq({dlq})+removed({removed})"
            f"={terminal} != published({len(self.model.published)})"
        )
        self.check_invariants("drain-final")

    def step_drain_ack(self, tokens):
        env = next(iter(tokens))
        token = tokens[env]
        payload = self.model.env_to_payload[env]
        if self.enable_completed:
            applied = self.gateway.move_message(self.k_processing, self.k_completed, env, lease_token=token)
        elif self.enable_failed:
            applied = self.gateway.move_message(self.k_processing, self.k_failed, env, lease_token=token)
        else:
            applied = self.gateway.remove_message(self.k_processing, env, lease_token=token)
            if applied is True:
                self.model.removed.add(payload)
        assert applied is True, f"drain ack returned {applied!r}"
        self.model.stale_tokens.append(token)


PARAM_GRID = [
    # (max_delivery_count, enable_completed, enable_failed)
    (1, True, True),
    (2, True, True),
    (3, True, True),
    (5, True, True),
    (3, False, False),  # remove_message terminal path
    (2, True, False),
    (1, False, True),
    (10, True, True),
]


def run_one(seed, steps, max_dc, enable_completed, enable_failed):
    h = Harness(seed, max_dc, enable_completed, enable_failed)
    for _ in range(steps):
        h.step()
    h.drain()
    return {
        "published": len(h.model.published),
        "dlqd": len(h.model.dlqd),
        "ops": len(h.history),
        "max_delivered": max(h.model.delivered.values(), default=0),
    }


# Bounded, deterministic CI budget: SEEDS x len(PARAM_GRID) x STEPS operations
# = 16 x 8 x 200 = 25,600 operations, ~19s locally (CPython 3.14, fakeredis).
# Each (seed, param-combo) is a fully independent fakeredis run with per-step invariant
# checks plus a drain epilogue. The portable audit harness used 40 seeds (~64k ops, ~50s);
# this trims the seed count to a CI-reasonable budget while keeping the full 8-combo param
# grid and per-step depth. Every seed in range(SEEDS) deterministically reaches the DLQ
# (verified), so the per-seed saw_dlq assertion below is a real coverage guard, not a flake.
SEEDS = 16
STEPS = 200


@pytest.mark.parametrize("seed", range(SEEDS))
def test_vt_dlq_invariants_hold(seed):
    """Walk randomized claim/expire/ack interleavings with the DLQ machine active.

    For each seed, runs every (max_delivery_count, completed, failed) combo in
    PARAM_GRID. ``run_one`` checks INV-1..INV-5 after every step and after a full drain;
    a violation raises with the seed + complete op trace for exact replay. Also asserts
    the DLQ path was actually exercised for this seed (the machine under test was driven).
    """
    saw_dlq = False
    for max_dc, enable_completed, enable_failed in PARAM_GRID:
        stats = run_one(
            seed,
            steps=STEPS,
            max_dc=max_dc,
            enable_completed=enable_completed,
            enable_failed=enable_failed,
        )
        saw_dlq = saw_dlq or stats["dlqd"] > 0
    assert saw_dlq, (
        f"seed={seed}: no message reached the DLQ across the param grid; "
        "the delivery_count->dead-letter machine was not exercised"
    )
