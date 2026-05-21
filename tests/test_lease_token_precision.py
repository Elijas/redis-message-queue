import shutil
import socket
import subprocess
import tempfile
import time
from collections.abc import AsyncIterator, Iterator
from uuid import uuid4

import pytest
import pytest_asyncio
import redis
import redis.asyncio

from redis_message_queue._redis_gateway import RedisGateway
from redis_message_queue._stored_message import encode_stored_message
from redis_message_queue.asyncio._redis_gateway import RedisGateway as AsyncRedisGateway

HIGH_COUNTER_SEED = 9_007_199_254_740_990
TWO_TO_53_MINUS_ONE = 2**53 - 1


@pytest.fixture(scope="module")
def redis_server_url() -> Iterator[str]:
    if shutil.which("redis-server") is None:
        pytest.skip("redis-server is required for lease token precision tests")

    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()

    tmpdir = tempfile.mkdtemp(prefix="rmq-lease-token-precision-")
    proc = subprocess.Popen(
        [
            "redis-server",
            "--port",
            str(port),
            "--bind",
            "127.0.0.1",
            "--save",
            "",
            "--appendonly",
            "no",
            "--dir",
            tmpdir,
            "--loglevel",
            "warning",
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    client = redis.Redis(host="127.0.0.1", port=port)
    try:
        for _ in range(200):
            try:
                client.ping()
                break
            except redis.ConnectionError:
                time.sleep(0.025)
        else:
            output = proc.stdout.read() if proc.stdout is not None else ""
            pytest.fail(f"redis-server did not start: {output}")

        yield f"redis://127.0.0.1:{port}/0"
    finally:
        try:
            client.shutdown(nosave=True)
        except redis.RedisError:
            pass
        finally:
            client.close()
        if proc.poll() is None:
            proc.terminate()
        try:
            proc.wait(timeout=5)
        finally:
            shutil.rmtree(tmpdir, ignore_errors=True)


@pytest.fixture()
def redis_client(redis_server_url: str) -> Iterator[redis.Redis]:
    client = redis.Redis.from_url(redis_server_url)
    client.flushdb()
    try:
        yield client
    finally:
        client.flushdb()
        client.close()


@pytest_asyncio.fixture()
async def async_redis_client(redis_server_url: str) -> AsyncIterator[redis.asyncio.Redis]:
    client = redis.asyncio.Redis.from_url(redis_server_url)
    await client.flushdb()
    try:
        yield client
    finally:
        await client.flushdb()
        await client.aclose()


def test_sync_default_counter_claims_have_distinct_lease_tokens(redis_client: redis.Redis) -> None:
    tokens = _claim_tokens_sync(redis_client)

    assert len(tokens) == len(set(tokens))
    _assert_plain_positive_integer_tokens(tokens)


@pytest.mark.asyncio
async def test_async_default_counter_claims_have_distinct_lease_tokens(
    async_redis_client: redis.asyncio.Redis,
) -> None:
    tokens = await _claim_tokens_async(async_redis_client)

    assert len(tokens) == len(set(tokens))
    _assert_plain_positive_integer_tokens(tokens)


@pytest.mark.parametrize(
    ("seed", "expected_tokens"),
    [
        (
            HIGH_COUNTER_SEED,
            ["9007199254740991", "9007199254740992", "9007199254740993"],
        ),
        (
            TWO_TO_53_MINUS_ONE,
            ["9007199254740992", "9007199254740993", "9007199254740994"],
        ),
    ],
)
def test_sync_preseeded_counter_preserves_exact_integer_tokens(
    redis_client: redis.Redis,
    seed: int,
    expected_tokens: list[str],
) -> None:
    tokens = _claim_tokens_sync(redis_client, seed=seed)

    assert tokens == expected_tokens
    assert len(tokens) == len(set(tokens))
    _assert_plain_positive_integer_tokens(tokens)


@pytest.mark.parametrize(
    ("seed", "expected_tokens"),
    [
        (
            HIGH_COUNTER_SEED,
            ["9007199254740991", "9007199254740992", "9007199254740993"],
        ),
        (
            TWO_TO_53_MINUS_ONE,
            ["9007199254740992", "9007199254740993", "9007199254740994"],
        ),
    ],
)
@pytest.mark.asyncio
async def test_async_preseeded_counter_preserves_exact_integer_tokens(
    async_redis_client: redis.asyncio.Redis,
    seed: int,
    expected_tokens: list[str],
) -> None:
    tokens = await _claim_tokens_async(async_redis_client, seed=seed)

    assert tokens == expected_tokens
    assert len(tokens) == len(set(tokens))
    _assert_plain_positive_integer_tokens(tokens)


def test_sync_high_counter_token_format_is_plain_positive_integer(redis_client: redis.Redis) -> None:
    tokens = _claim_tokens_sync(redis_client, seed=TWO_TO_53_MINUS_ONE)

    _assert_plain_positive_integer_tokens(tokens)


@pytest.mark.asyncio
async def test_async_high_counter_token_format_is_plain_positive_integer(
    async_redis_client: redis.asyncio.Redis,
) -> None:
    tokens = await _claim_tokens_async(async_redis_client, seed=TWO_TO_53_MINUS_ONE)

    _assert_plain_positive_integer_tokens(tokens)


def _gateway(client: redis.Redis) -> RedisGateway:
    return RedisGateway(
        redis_client=client,
        retry_budget_seconds=0,
        message_wait_interval_seconds=0,
        message_visibility_timeout_seconds=30,
    )


def _async_gateway(client: redis.asyncio.Redis) -> AsyncRedisGateway:
    return AsyncRedisGateway(
        redis_client=client,
        retry_budget_seconds=0,
        message_wait_interval_seconds=0,
        message_visibility_timeout_seconds=30,
    )


def _claim_tokens_sync(redis_client: redis.Redis, *, seed: int | None = None) -> list[str]:
    gateway = _gateway(redis_client)
    pending, processing = _queue_keys()
    for index in range(3):
        redis_client.rpush(pending, encode_stored_message(f"message-{index}"))

    if seed is not None:
        redis_client.set(gateway._lease_token_counter_key(processing), seed)

    tokens = []
    for _ in range(3):
        claimed = gateway.wait_for_message_and_move(pending, processing)
        assert claimed is not None
        tokens.append(claimed.lease_token)

    _assert_token_indexes_have_no_collisions(redis_client, gateway, processing, tokens)
    return tokens


async def _claim_tokens_async(redis_client: redis.asyncio.Redis, *, seed: int | None = None) -> list[str]:
    gateway = _async_gateway(redis_client)
    pending, processing = _queue_keys()
    for index in range(3):
        await redis_client.rpush(pending, encode_stored_message(f"message-{index}"))

    if seed is not None:
        await redis_client.set(gateway._lease_token_counter_key(processing), seed)

    tokens = []
    for _ in range(3):
        claimed = await gateway.wait_for_message_and_move(pending, processing)
        assert claimed is not None
        tokens.append(claimed.lease_token)

    await _assert_async_token_indexes_have_no_collisions(redis_client, gateway, processing, tokens)
    return tokens


def _queue_keys() -> tuple[str, str]:
    prefix = f"lease-token-precision:{uuid4().hex}"
    return f"{prefix}:pending", f"{prefix}:processing"


def _assert_plain_positive_integer_tokens(tokens: list[str]) -> None:
    for token in tokens:
        assert isinstance(token, str)
        assert token.isdecimal()
        assert int(token) > 0
        assert "." not in token
        assert "e+" not in token.lower()


def _assert_token_indexes_have_no_collisions(
    redis_client: redis.Redis,
    gateway: RedisGateway,
    processing: str,
    tokens: list[str],
) -> None:
    expected_keys = {token.encode("utf-8") for token in tokens}
    refs = redis_client.hgetall(gateway._claim_result_refs_key(processing))
    backrefs = redis_client.hgetall(gateway._claim_result_backrefs_key(processing))

    assert set(refs) == expected_keys
    assert set(backrefs) == expected_keys


async def _assert_async_token_indexes_have_no_collisions(
    redis_client: redis.asyncio.Redis,
    gateway: AsyncRedisGateway,
    processing: str,
    tokens: list[str],
) -> None:
    expected_keys = {token.encode("utf-8") for token in tokens}
    refs = await redis_client.hgetall(gateway._claim_result_refs_key(processing))
    backrefs = await redis_client.hgetall(gateway._claim_result_backrefs_key(processing))

    assert set(refs) == expected_keys
    assert set(backrefs) == expected_keys
