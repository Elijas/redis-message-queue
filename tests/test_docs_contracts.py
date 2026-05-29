import inspect
import re
import tomllib
from pathlib import Path

import redis_message_queue as rmq
import redis_message_queue.asyncio as async_rmq
from redis_message_queue import ClaimStoreFailedError, RedisMessageQueueError
from redis_message_queue._event import EventOperation, EventOutcome
from redis_message_queue.asyncio.redis_message_queue import RedisMessageQueue as AsyncRedisMessageQueue
from redis_message_queue.redis_message_queue import RedisMessageQueue

ROOT = Path(__file__).resolve().parents[1]
README_PATH = ROOT / "README.md"
PRODUCTION_READINESS_PATH = ROOT / "docs" / "production-readiness.md"


def _markdown_section(markdown: str, start_heading: str, end_heading: str) -> str:
    assert start_heading in markdown
    section_start = markdown.index(start_heading) + len(start_heading)
    section_end = markdown.index(end_heading, section_start)
    return markdown[section_start:section_end]


def _public_exception_names(module) -> list[str]:
    names = []
    for name in module.__all__:
        obj = getattr(module, name)
        if inspect.isclass(obj) and issubclass(obj, RedisMessageQueueError):
            names.append(name)
    return names


def _documented_exception_names(markdown_section: str) -> list[str]:
    return re.findall(r"^\s+- `([A-Za-z_][A-Za-z0-9_]*)`", markdown_section, flags=re.MULTILINE)


def test_readme_documents_complete_at_most_once_configuration() -> None:
    readme = README_PATH.read_text(encoding="utf-8")
    normalized_readme = " ".join(readme.split())
    delivery_semantics = _markdown_section(readme, "### Delivery semantics", "## Configuration")

    assert "visibility_timeout_seconds=None, max_delivery_count=None" in delivery_semantics
    assert "| With `visibility_timeout_seconds=None` |" not in delivery_semantics
    assert (
        "With `visibility_timeout_seconds=None, max_delivery_count=None`, there is no automatic reclaim path"
        in normalized_readme
    )


def test_constructor_docstrings_document_complete_at_most_once_configuration() -> None:
    for queue_cls in (RedisMessageQueue, AsyncRedisMessageQueue):
        docstring = inspect.getdoc(queue_cls.__init__)
        assert docstring is not None
        normalized_docstring = " ".join(docstring.split())
        assert "``visibility_timeout_seconds=None`` and ``max_delivery_count=None``" in normalized_docstring


def test_production_readiness_metadata_avoids_stale_exact_suite_counts() -> None:
    doc = PRODUCTION_READINESS_PATH.read_text(encoding="utf-8")
    pyproject = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    version_line_match = re.search(r"^Applicable version: (.+)$", doc, flags=re.MULTILINE)

    assert version_line_match is not None
    version_line = version_line_match.group(1)
    assert "pyproject.toml" in version_line
    assert pyproject["project"]["version"] not in version_line
    assert not re.search(r"The test suite includes [\d,]+ tests across [\d,]+ files", doc)
    assert "run `uv run pytest --collect-only -q`" in doc
    assert "cancellation during dedup-key computation" not in doc


def test_production_readiness_documents_builtin_default_dlq_key() -> None:
    doc = PRODUCTION_READINESS_PATH.read_text(encoding="utf-8")
    r9_line = next(line for line in doc.splitlines() if line.startswith("| R9 |"))

    assert "`LLEN {name}::dlq`" in r9_line
    assert "`LLEN {name}::dead_letter`" not in r9_line
    assert "built-in `client=` path" in r9_line
    assert "Custom gateways can choose a different `dead_letter_queue`" in r9_line


def test_docs_describe_vt_claim_store_failure_observability() -> None:
    readme = README_PATH.read_text(encoding="utf-8")
    prod = PRODUCTION_READINESS_PATH.read_text(encoding="utf-8")
    event_timing = _markdown_section(readme, "#### Event timing vs. Redis commit", "#### Drain events")
    silent_paths = _markdown_section(readme, "#### Intentionally silent paths", "The public exception hierarchy")
    r21_line = next(line for line in prod.splitlines() if line.startswith("| R21 |"))
    claim_failure_event = f"`{EventOperation.CLAIM.value}/{EventOutcome.FAILURE.value}`"
    exception_name = ClaimStoreFailedError.__name__

    assert "`claim_empty/skipped`" not in readme
    assert "`claim_empty/skipped`" not in prod
    assert "VT claim-store OOM compensation" not in silent_paths

    for docs_text in (event_timing, r21_line):
        assert exception_name in docs_text
        assert claim_failure_event in docs_text
        assert "return-to-pending" in docs_text
        assert "pending" in docs_text
        assert "processing" in docs_text


def test_public_exception_hierarchy_docs_match_exports() -> None:
    readme = README_PATH.read_text(encoding="utf-8")
    prod = PRODUCTION_READINESS_PATH.read_text(encoding="utf-8")
    expected_names = _public_exception_names(rmq)

    assert expected_names == _public_exception_names(async_rmq)

    readme_exception_section = _markdown_section(
        readme,
        "The public exception hierarchy is rooted",
        "## Known limitations",
    )
    production_exception_section = _markdown_section(prod, "### Exception handling design", "## Test Coverage Summary")

    assert _documented_exception_names(readme_exception_section) == expected_names
    assert _documented_exception_names(production_exception_section) == expected_names
    assert "as of v7.0.0" not in production_exception_section
