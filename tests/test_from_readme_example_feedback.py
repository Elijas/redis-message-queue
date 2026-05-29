import random
import re
import runpy
import time
import tomllib
from pathlib import Path

import fakeredis
import pytest
import redis
import redis.asyncio

README_PATH = Path(__file__).resolve().parents[1] / "README.md"


class StopExample(Exception):
    pass


def _markdown_section(markdown: str, start_heading: str, end_heading: str) -> str:
    assert start_heading in markdown
    section_start = markdown.index(start_heading) + len(start_heading)
    section_end = markdown.index(end_heading, section_start)
    return markdown[section_start:section_end]


def _single_python_block(section: str) -> str:
    blocks = re.findall(r"```python\n(.*?)\n```", section, flags=re.DOTALL)
    assert len(blocks) == 1
    return blocks[0]


def _run_readme_python_block(block: str) -> None:
    exec(compile(block, str(README_PATH), "exec"), {"__name__": "__main__"})


def test_readme_sync_quickstart_is_rerunnable(monkeypatch, capsys) -> None:
    server = fakeredis.FakeServer()

    def from_url(*args, **kwargs):
        return fakeredis.FakeRedis(
            server=server,
            decode_responses=kwargs.get("decode_responses", False),
        )

    monkeypatch.setattr(redis.Redis, "from_url", from_url)
    readme = README_PATH.read_text(encoding="utf-8")
    section = _markdown_section(readme, "## Quickstart", "### Async quickstart")
    block = _single_python_block(section)

    _run_readme_python_block(block)
    _run_readme_python_block(block)

    assert capsys.readouterr().out.splitlines() == ["got hello", "got hello"]


def test_readme_async_quickstart_is_rerunnable(monkeypatch, capsys) -> None:
    server = fakeredis.FakeServer()

    def from_url(*args, **kwargs):
        return fakeredis.FakeAsyncRedis(
            server=server,
            decode_responses=kwargs.get("decode_responses", False),
        )

    monkeypatch.setattr(redis.asyncio.Redis, "from_url", from_url)
    readme = README_PATH.read_text(encoding="utf-8")
    section = _markdown_section(readme, "### Async quickstart", "## Why redis-message-queue")
    block = _single_python_block(section)

    _run_readme_python_block(block)
    _run_readme_python_block(block)

    assert capsys.readouterr().out.splitlines() == ["got hello", "got hello"]


def test_readme_redis_py_floor_matches_project_dependency() -> None:
    pyproject = tomllib.loads((README_PATH.parent / "pyproject.toml").read_text(encoding="utf-8"))
    redis_dependencies = [
        dependency for dependency in pyproject["project"]["dependencies"] if dependency.startswith("redis")
    ]
    assert redis_dependencies == ["redis>=5.0.1,<8.0.0"]

    readme = README_PATH.read_text(encoding="utf-8")
    upgrading_section = _markdown_section(readme, "## Upgrading", "### Configuration changes on live queues")
    assert f"`{redis_dependencies[0]}`" in upgrading_section
    assert "`redis>=5.0.0,<8.0.0`" not in upgrading_section


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
