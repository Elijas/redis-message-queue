import ast
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


def _residual_risk_rows(markdown: str) -> dict[str, list[str]]:
    rows: dict[str, list[str]] = {}
    for line in markdown.splitlines():
        if not line.startswith("| R"):
            continue
        cells = [cell.strip() for cell in line.strip().strip("|").split("|")]
        if len(cells) == 4 and cells[0].startswith("R"):
            rows[cells[0]] = cells
    return rows


def _requires_python_lower_bound(requires_python: str) -> str:
    matches = re.findall(r"(>=|>)\s*([0-9]+(?:\.[0-9]+)*)", requires_python)
    assert matches, f"requires-python has no explicit lower bound: {requires_python}"

    def version_key(match: tuple[str, str]) -> tuple[int, ...]:
        return tuple(int(part) for part in match[1].split("."))

    operator, version = max(matches, key=version_key)
    return f"{operator} {version}"


def _call_name(func: ast.AST) -> str:
    if isinstance(func, ast.Attribute):
        return f"{_call_name(func.value)}.{func.attr}"
    if isinstance(func, ast.Name):
        return func.id
    return ""


def test_readme_onboarding_documents_python_runtime_floor() -> None:
    readme = README_PATH.read_text(encoding="utf-8")
    onboarding = readme[: readme.index("## Quickstart")]
    pyproject = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    python_floor = _requires_python_lower_bound(pyproject["project"]["requires-python"])

    expected_requirement = f"Requires Python {python_floor} and Redis server >= 6.2."

    assert expected_requirement in onboarding


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


def test_on_event_reentrancy_warning_present_in_readme_and_docstrings() -> None:
    readme = README_PATH.read_text(encoding="utf-8")
    dispatch_context = _markdown_section(readme, "#### Event dispatch context", "#### Event timing vs. Redis commit")
    dequoted = "\n".join(line.lstrip("> ") for line in dispatch_context.splitlines())
    readme_text = " ".join(dequoted.replace("`", "").split())

    assert "must not call back into the same queue instance" in readme_text
    assert "deadlock" in readme_text.lower()
    for method in ("publish()", "drain()", "close()", "aclose()"):
        assert method in readme_text

    for queue_cls, teardown_method in (
        (RedisMessageQueue, "close()"),
        (AsyncRedisMessageQueue, "aclose()"),
    ):
        docstring = inspect.getdoc(queue_cls.__init__)
        assert docstring is not None
        docstring_text = " ".join(docstring.replace("`", "").split())
        assert "must not call back into the same queue instance" in docstring_text
        assert "deadlock" in docstring_text.lower()
        assert "publish()" in docstring_text
        assert "drain()" in docstring_text
        assert teardown_method in docstring_text


def test_constructor_docstrings_warn_on_heartbeat_failure_must_not_block_event_loop() -> None:
    for queue_cls in (RedisMessageQueue, AsyncRedisMessageQueue):
        docstring = inspect.getdoc(queue_cls.__init__)
        assert docstring is not None
        normalized_docstring = " ".join(docstring.split())
        assert "on_heartbeat_failure" in normalized_docstring
        # Where the callback runs on each surface (parity divergence documented symmetrically).
        assert "background thread (sync queue)" in normalized_docstring
        assert "event loop (async queue)" in normalized_docstring
        # The async blocking hazard plus its cross-message and shutdown consequences.
        assert "MUST NOT block" in normalized_docstring
        assert "freezes the event loop" in normalized_docstring
        assert "expire and be reclaimed" in normalized_docstring


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


def test_production_readiness_does_not_overstate_residual_risk_evidence() -> None:
    doc = PRODUCTION_READINESS_PATH.read_text(encoding="utf-8")
    rows = _residual_risk_rows(doc)
    unevidenced_rows = {risk_id: cells[3] for risk_id, cells in rows.items() if cells[3] in {"", "-", "\u2014"}}

    assert "Each item is independently tested" not in doc
    assert "Where Tested / Documented" in doc
    assert unevidenced_rows == {"R13": "\u2014"}
    assert "ANSI escape sequences or newline characters" in rows["R13"][2]
    assert "does not sanitize queue names beyond checking for the key separator" in rows["R13"][2]


def test_production_readiness_documents_builtin_default_dlq_key() -> None:
    doc = PRODUCTION_READINESS_PATH.read_text(encoding="utf-8")
    r9_line = next(line for line in doc.splitlines() if line.startswith("| R9 |"))

    assert "`LLEN {name}::dlq`" in r9_line
    assert "`LLEN {name}::dead_letter`" not in r9_line
    assert "built-in `client=` path" in r9_line
    assert "Custom gateways can choose a different `dead_letter_queue`" in r9_line


def test_production_readiness_documents_explicit_none_for_legacy_unbounded_defaults() -> None:
    doc = PRODUCTION_READINESS_PATH.read_text(encoding="utf-8")
    rows = _residual_risk_rows(doc)

    assert "omitting `max_delivery_count` uses the capped default of `10`" in rows["R2"][2]
    assert "`max_delivery_count=None` explicitly" in rows["R2"][2]
    assert "Without `max_delivery_count`" not in rows["R2"][2]
    assert "omitting these parameters uses the capped default of `1000`" in rows["R3"][2]
    assert "`max_completed_length=None` / `max_failed_length=None` explicitly" in rows["R3"][2]
    assert "Without these parameters" not in rows["R3"][2]


def test_production_examples_use_finite_redis_connection_pools() -> None:
    missing: list[str] = []
    none_values: list[str] = []
    production_root = ROOT / "examples" / "production"

    for path in sorted(production_root.rglob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            if not _call_name(node.func).endswith(".from_url"):
                continue
            max_connections = [keyword for keyword in node.keywords if keyword.arg == "max_connections"]
            location = f"{path.relative_to(ROOT)}:{node.lineno}"
            if not max_connections:
                missing.append(location)
            elif isinstance(max_connections[0].value, ast.Constant) and max_connections[0].value.value is None:
                none_values.append(location)

    assert missing == []
    assert none_values == []


def test_production_receive_examples_describe_handler_failures_as_failed_queue() -> None:
    for relative in (
        "examples/production/receive_messages.py",
        "examples/production/asyncio/receive_messages.py",
    ):
        text = (ROOT / relative).read_text(encoding="utf-8")
        assert "handler failed; message moved to failed queue" in text
        assert "handler failed; message routed to failed queue or dead-letter queue" not in text


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


def test_docs_document_claim_cache_replay_silent_event_loss() -> None:
    readme = README_PATH.read_text(encoding="utf-8")
    silent_paths = _markdown_section(readme, "#### Intentionally silent paths", "The public exception hierarchy")
    normalized = " ".join(silent_paths.split())
    reclaim_event = f"`{EventOperation.CLAIM_RECLAIM.value}`"
    dlq_event = f"`{EventOperation.DLQ.value}`"

    assert "cache-replay" in normalized
    assert "lost-reply" in normalized
    assert reclaim_event in silent_paths
    assert dlq_event in silent_paths
    assert "not re-emitted" in normalized
    assert "state stays correct" in normalized


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
