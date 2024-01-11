import pytest


@pytest.fixture
def redis_client(request):
    import fakeredis

    redis_client = fakeredis.FakeRedis()
    return redis_client
