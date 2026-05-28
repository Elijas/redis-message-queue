import random
import runpy
import time

import fakeredis
import pytest
import redis


class StopExample(Exception):
    pass


def test_from_readme_publisher_prints_publish_feedback(monkeypatch, capsys) -> None:
    fake_client = fakeredis.FakeRedis(decode_responses=True)
    monkeypatch.setattr(redis.Redis, "from_url", lambda *args, **kwargs: fake_client)
    monkeypatch.setattr(random, "randint", lambda start, stop: 42)

    sleep_calls = 0
    original_sleep = time.sleep

    def stop_after_second_publish(seconds: float) -> None:
        nonlocal sleep_calls
        sleep_calls += 1
        if sleep_calls == 2:
            raise StopExample
        original_sleep(0)

    monkeypatch.setattr(time, "sleep", stop_after_second_publish)

    with pytest.raises(StopExample):
        runpy.run_module("examples.from_readme.send_messages", run_name="__main__")

    stdout = capsys.readouterr().out
    assert "Success: Sent message 'Hello (id=42)'." in stdout
    assert "Duplicate: Message 'Hello (id=42)' was already sent previously." in stdout
    assert fake_client.llen("my_message_queue::pending") == 1
