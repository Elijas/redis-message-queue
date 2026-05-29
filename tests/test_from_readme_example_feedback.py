import importlib.abc
import random
import re
import runpy
import sys
import time
import tomllib
from pathlib import Path

import fakeredis
import pytest
import redis
import redis.asyncio
from packaging.requirements import Requirement
from packaging.utils import canonicalize_name
from packaging.version import Version

from redis_message_queue import RedisMessageQueue

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


class OptionalTelemetryBlocker(importlib.abc.MetaPathFinder):
    blocked_roots = ("opentelemetry", "prometheus_client")

    def find_spec(self, fullname: str, path: object | None, target: object | None = None):
        root = fullname.split(".", 1)[0]
        if root in self.blocked_roots:
            raise ModuleNotFoundError(f"No module named {root!r}")
        return None


def _run_readme_python_block(block: str, namespace: dict[str, object] | None = None) -> dict[str, object]:
    globals_dict = {"__name__": "__main__"}
    if namespace is not None:
        globals_dict.update(namespace)
    exec(compile(block, str(README_PATH), "exec"), globals_dict)
    return globals_dict


def _project_redis_dependency() -> tuple[str, Requirement]:
    pyproject = tomllib.loads((README_PATH.parent / "pyproject.toml").read_text(encoding="utf-8"))
    redis_dependencies = [
        dependency
        for dependency in pyproject["project"]["dependencies"]
        if canonicalize_name(Requirement(dependency).name) == "redis"
    ]
    assert len(redis_dependencies) == 1
    redis_dependency = redis_dependencies[0]
    return redis_dependency, Requirement(redis_dependency)


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


def test_readme_observability_telemetry_block_runs_without_optional_exporters() -> None:
    readme = README_PATH.read_text(encoding="utf-8")
    section = _markdown_section(readme, "## Observability", "#### \u26a0 Secrets in `event.error`")
    blocks = re.findall(r"```python\n(.*?)\n```", section, flags=re.DOTALL)
    block = next(block for block in blocks if "rmq_events_total" in block)
    blocker = OptionalTelemetryBlocker()
    removed_modules = {
        module_name: sys.modules.pop(module_name)
        for module_name in list(sys.modules)
        if module_name.split(".", 1)[0] in blocker.blocked_roots
    }

    sys.meta_path.insert(0, blocker)
    try:
        namespace = _run_readme_python_block(
            block,
            {"client": fakeredis.FakeRedis(decode_responses=True)},
        )
        queue = namespace["queue"]
        assert isinstance(queue, RedisMessageQueue)
        assert queue.publish("hello") is True
    finally:
        sys.meta_path.remove(blocker)
        sys.modules.update(removed_modules)


def test_readme_redis_py_requirement_matches_project_dependency() -> None:
    redis_dependency, _ = _project_redis_dependency()

    readme = README_PATH.read_text(encoding="utf-8")
    upgrading_section = _markdown_section(readme, "## Upgrading", "### Configuration changes on live queues")
    assert f"`{redis_dependency}`" in upgrading_section
    assert "`redis>=5.0.0,<8.0.0`" not in upgrading_section


def test_redis_py_dependency_remains_below_8_until_redis_8_is_verified() -> None:
    redis_dependency, redis_requirement = _project_redis_dependency()

    assert Version("8.0.0") not in redis_requirement.specifier, (
        "redis-py 8 support is not verified; keep [project].dependencies capped below "
        f"8.0.0 or update this policy guard with verification evidence. Current dependency: {redis_dependency}"
    )


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
