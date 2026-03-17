import os
from uuid import uuid4

import pytest
import pytest_asyncio
import redis
import redis.asyncio


@pytest.fixture(scope="session")
def real_redis_url():
    return os.environ.get("REDIS_URL", "redis://localhost:6379/15")


@pytest.fixture()
def real_redis_client(real_redis_url):
    client = redis.Redis.from_url(real_redis_url)
    try:
        client.ping()
    except redis.ConnectionError:
        pytest.skip("Redis not available")
    yield client
    client.close()


@pytest_asyncio.fixture()
async def real_async_redis_client(real_redis_url):
    client = redis.asyncio.Redis.from_url(real_redis_url)
    try:
        await client.ping()
    except redis.ConnectionError:
        pytest.skip("Redis not available")
    yield client
    await client.aclose()


@pytest.fixture()
def queue_name():
    return f"rmq-integ-{uuid4().hex[:12]}"


@pytest.fixture(autouse=True)
def _cleanup_integration_keys(request, real_redis_url, queue_name):
    """After each @pytest.mark.integration test, delete all keys matching the queue_name prefix."""
    if "integration" not in [m.name for m in request.node.iter_markers()]:
        yield
        return

    yield

    # Use a separate sync client for cleanup regardless of test type
    client = redis.Redis.from_url(real_redis_url)
    try:
        cursor = 0
        while True:
            cursor, keys = client.scan(cursor=cursor, match=f"{queue_name}*", count=100)
            if keys:
                client.delete(*keys)
            if cursor == 0:
                break
    except redis.ConnectionError:
        pass
    finally:
        client.close()
