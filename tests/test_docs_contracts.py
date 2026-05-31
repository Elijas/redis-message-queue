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
PRODUCTION_EXAMPLES_ROOT = ROOT / "examples" / "production"
SYNC_RECEIVE_EXAMPLE_PATH = ROOT / "examples" / "receive_messages.py"

TERMINAL_RETENTION_EXAMPLE_MARKERS = (
    "raw payload bytes",
    "inspect first",
    "manual replay/repair/trim/archive is application-owned",
)


def _markdown_section(markdown: str, start_heading: str, end_heading: str) -> str:
    assert start_heading in markdown
    section_start = markdown.index(start_heading) + len(start_heading)
    section_end = markdown.index(end_heading, section_start)
    return markdown[section_start:section_end]


def _markdown_section_to_end(markdown: str, start_heading: str) -> str:
    assert start_heading in markdown
    section_start = markdown.index(start_heading) + len(start_heading)
    return markdown[section_start:]


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


def _production_examples_matching(setting_pattern: str) -> dict[str, str]:
    matches = {}
    for path in sorted(PRODUCTION_EXAMPLES_ROOT.rglob("*.py")):
        text = path.read_text(encoding="utf-8")
        if re.search(setting_pattern, text):
            matches[path.relative_to(ROOT).as_posix()] = text
    return matches


def _markdown_fenced_examples(markdown: str) -> list[tuple[int, int, str]]:
    examples = []
    for match in re.finditer(r"```[^\n]*\n(?P<body>.*?)```", markdown, flags=re.DOTALL):
        start_line = markdown[: match.start()].count("\n") + 1
        end_line = markdown[: match.end()].count("\n") + 1
        examples.append((start_line, end_line, match.group("body")))
    return examples


def _normalized_contract_text(text: str) -> str:
    return " ".join(text.replace("#", "").split()).lower()


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


def test_readme_quickstarts_warn_about_local_redis_data_plane() -> None:
    readme = README_PATH.read_text(encoding="utf-8")
    quickstart = _markdown_section(readme, "## Quickstart", "## Why redis-message-queue")
    warning = quickstart[: quickstart.index("```python")]
    normalized = " ".join(line.lstrip("> ").strip() for line in warning.splitlines())

    assert "sync and async quickstarts" in normalized
    assert "`redis://localhost:6379/0`" in warning
    assert "fixed queue namespace `quickstart`" in normalized
    assert "publishes a message" in normalized
    assert "claims and removes one message" in normalized
    assert "local DB 0" in normalized
    assert "`quickstart` data" in normalized
    assert "disposable Redis instance" in normalized
    assert "separate DB/port" in normalized
    assert "change the URL/queue name" in normalized


def test_readme_running_locally_documents_example_output_and_uv_sigint_caveat() -> None:
    readme = README_PATH.read_text(encoding="utf-8")
    section = _markdown_section_to_end(readme, "## Running locally")
    normalized = " ".join(section.split())

    assert "uv run python -m examples.send_messages" in section
    assert "uv run python -m examples.receive_messages" in section
    assert "long-running" in normalized

    for expected_output in (
        "`Success: Sent message ...`",
        "`Duplicate: Message ...`",
        "`Received Message: ...`",
        "`Finished processing message ...`",
        "`Exiting...`",
        "`Received signal: ...`",
    ):
        assert expected_output in section
    assert "stderr" in normalized

    assert "`uv run`" in section
    assert "process group" in normalized
    assert "`time.sleep(...)`" in section
    assert "`KeyboardInterrupt` traceback" in normalized


def test_readme_running_locally_warns_about_local_redis_data_plane() -> None:
    readme = README_PATH.read_text(encoding="utf-8")
    section = _markdown_section_to_end(readme, "## Running locally")
    normalized = " ".join(section.split())

    assert "`REDIS_URL`" in section
    assert "`redis://localhost:6379/0`" in section
    assert "`my_message_queue`" in section
    assert "database 0" in normalized
    assert "existing local Redis data" in normalized
    assert "disposable Redis instance" in normalized
    assert "separate Redis database" in normalized


def test_sync_receive_example_comment_documents_single_signal_and_uv_process_group_caveat() -> None:
    source = SYNC_RECEIVE_EXAMPLE_PATH.read_text(encoding="utf-8")
    comment = source[
        source.index("    # A single handled shutdown signal") : source.index(
            "    handler = GracefulInterruptHandler()"
        )
    ]
    normalized = " ".join(line.removeprefix("    # ").strip() for line in comment.splitlines())

    assert "single handled shutdown signal" in normalized
    assert "loop stop between messages or after the current simulated work finishes" in normalized
    assert "`uv run`" in normalized
    assert "process group" in normalized
    assert "time.sleep(...)" in normalized
    assert "KeyboardInterrupt traceback" in normalized
    assert "clean Exiting..." in normalized


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


def test_readme_scopes_handler_failure_cleanup_to_ordinary_exceptions() -> None:
    readme = README_PATH.read_text(encoding="utf-8")
    delivery_semantics = _markdown_section(readme, "### Delivery semantics", "## Configuration")
    ordering_semantics = _markdown_section(
        readme,
        "### Ordering and multi-consumer fairness",
        "### If you need stronger ordering or fairness guarantees",
    )
    migration_semantics = _markdown_section(
        readme,
        "## Migrating from RQ / Celery / Dramatiq / taskiq",
        "## Production notes",
    )
    delivery_text = " ".join(line.lstrip("> ").strip() for line in delivery_semantics.splitlines())
    ordering_text = " ".join(ordering_semantics.split())
    migration_text = " ".join(migration_semantics.split())

    for section in (delivery_semantics, ordering_semantics, migration_semantics):
        assert "Ordinary `Exception` subclasses raised by handler code" in section
        assert "`BaseException`" in section
        assert "[Abandoned in-flight messages](#abandoned-in-flight-messages)" in section

    assert "Fatal `BaseException` paths" in delivery_text
    assert "fatal `BaseException` shutdown/cancellation paths" in ordering_text
    assert "Fatal `BaseException` shutdown or cancellation paths" in migration_text
    assert "raising an ordinary `Exception` inside `process_message()`" in delivery_text
    assert "not failed handler work" in delivery_text
    assert "message in `processing` for visibility-timeout reclaim" in delivery_text
    assert "`visibility_timeout_seconds=None, max_delivery_count=None`" in delivery_semantics
    assert "[Graceful shutdown](#graceful-shutdown)" in delivery_semantics

    for fatal_path in (
        "`KeyboardInterrupt`",
        "`SystemExit`",
        "externally cancelled async tasks",
        "`asyncio.CancelledError`",
    ):
        assert fatal_path in delivery_semantics


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


def test_production_readiness_dlq_trim_guidance_has_local_safeguards() -> None:
    doc = PRODUCTION_READINESS_PATH.read_text(encoding="utf-8")
    r9_line = next(line for line in doc.splitlines() if line.startswith("| R9 |"))
    normalized = " ".join(r9_line.split())

    assert "trim" in normalized
    assert "inspect the configured DLQ" in normalized
    assert "export/archive retained raw-payload records" in normalized
    assert "application-owned retention policy" in normalized


def test_production_readiness_processing_cleanup_guidance_has_local_safeguards() -> None:
    doc = PRODUCTION_READINESS_PATH.read_text(encoding="utf-8")
    r8_line = next(line for line in doc.splitlines() if line.startswith("| R8 |"))
    normalized = " ".join(r8_line.split())

    assert "manual cleanup of the processing queue" in normalized
    assert "`claim_result_ids`" in normalized
    assert "`claim_result_backrefs`" in normalized
    assert "draining or otherwise quiescing consumers" in normalized
    assert "confirming the claims are abandoned" in normalized
    assert "inspecting/exporting/archiving payloads as needed" in normalized
    assert "application-owned recovery/retention policy" in normalized


def test_production_readiness_terminal_rows_link_manual_handling_contracts() -> None:
    doc = PRODUCTION_READINESS_PATH.read_text(encoding="utf-8")
    rows = _residual_risk_rows(doc)

    assert "[README success/failure tracking](../README.md#success-and-failure-tracking)" in rows["R3"][3]
    assert "[README dead-letter queue](../README.md#dead-letter-queue)" in rows["R9"][3]
    assert "[README success/failure tracking](../README.md#success-and-failure-tracking)" in rows["R14"][3]


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


def test_production_examples_with_completed_retention_point_to_contract() -> None:
    examples = _production_examples_matching(r"\benable_completed_queue\s*=\s*True\b|\bmax_completed_length\s*=")
    expected_current = {
        "examples/production/asyncio/receive_messages.py",
        "examples/production/asyncio/send_messages.py",
        "examples/production/receive_messages.py",
        "examples/production/send_messages.py",
    }

    assert expected_current <= set(examples)
    for relative, text in examples.items():
        normalized = _normalized_contract_text(text)
        assert "#success-and-failure-tracking" in text, relative
        for marker in TERMINAL_RETENTION_EXAMPLE_MARKERS:
            assert marker in normalized, relative


def test_production_examples_with_failed_retention_point_to_contract() -> None:
    examples = _production_examples_matching(r"\benable_failed_queue\s*=\s*True\b|\bmax_failed_length\s*=")
    expected_current = {
        "examples/production/asyncio/receive_messages.py",
        "examples/production/receive_messages.py",
    }

    assert expected_current <= set(examples)
    for relative, text in examples.items():
        normalized = _normalized_contract_text(text)
        assert "#success-and-failure-tracking" in text, relative
        for marker in TERMINAL_RETENTION_EXAMPLE_MARKERS:
            assert marker in normalized, relative


def test_production_examples_with_dlq_routing_point_to_contract() -> None:
    examples = _production_examples_matching(r"\bmax_delivery_count\s*=\s*(?!None\b)")
    expected_current = {
        "examples/production/asyncio/receive_messages.py",
        "examples/production/receive_messages.py",
    }

    assert expected_current <= set(examples)
    for relative, text in examples.items():
        normalized = _normalized_contract_text(text)
        assert "#dead-letter-queue" in text, relative
        for marker in TERMINAL_RETENTION_EXAMPLE_MARKERS:
            assert marker in normalized, relative


def test_readme_documents_completed_queue_inspection_contract() -> None:
    readme = README_PATH.read_text(encoding="utf-8")
    section = _markdown_section(readme, "### Success and failure tracking", "### Publish backpressure")
    normalized = " ".join(section.split())

    assert "terminal audit/inspection records" in normalized
    assert "not a result backend" in normalized
    assert "`queue.key.completed`" in section
    assert "`{name}::completed`" in section
    assert '`key_separator="::"`' in section
    assert "raw payload bytes only" in normalized
    assert "without result values, timestamps, delivery counts, exception metadata" in normalized
    assert "deduplication keys" in normalized
    assert "internal delivery envelope" in normalized
    assert "`LRANGE queue.key.completed 0 -1`" in section
    assert "Archive, export, or trim completed records" in normalized
    assert "application's retention policy" in normalized
    assert "automatic replay or retry semantics" in normalized


def test_readme_documents_failed_queue_manual_reprocessing_contract() -> None:
    readme = README_PATH.read_text(encoding="utf-8")
    section = _markdown_section(readme, "### Success and failure tracking", "### Publish backpressure")
    normalized = " ".join(section.split())

    assert "retain failed messages for inspection/manual repair" in section
    assert "retained for inspection and application-owned manual reprocessing" in normalized
    assert "not automatically retried" in normalized
    assert "`queue.key.failed`" in section
    assert "`{name}::failed`" in section
    assert '`key_separator="::"`' in section
    assert "raw payload bytes only" in normalized
    assert "not exception metadata, timestamps, delivery counts" in normalized
    assert "internal delivery envelope" in normalized
    assert "inspect first" in normalized
    assert "`LRANGE queue.key.failed 0 -1`" in section
    assert "republish or move records to a separate repair queue" in normalized
    assert "idempotency and deduplication policy" in normalized
    assert "suppressed by publish-side deduplication" in normalized
    assert "original dedup key is live" in normalized
    assert "changes the replay key" in normalized
    assert "waits for TTL expiry" in normalized
    assert "disables deduplication for that path" in normalized
    assert "Do not treat blind `LPUSH` or `RPUSH`" in section
    assert "universal safe replay workflow" in normalized


def test_readme_documents_dlq_manual_handling_contract() -> None:
    readme = README_PATH.read_text(encoding="utf-8")
    section = _markdown_section(readme, "### Dead-letter queue", "### Graceful shutdown")
    normalized = " ".join(section.split())

    assert "Manual DLQ handling" in section
    assert "DLQ entries are terminal retained records" in normalized
    assert "are not automatically retried or moved back to pending by the library" in normalized
    assert "For built-in `client=` queues, inspect `{name}::dlq`" in normalized
    assert "for custom gateway queues, inspect the configured `dead_letter_queue`" in normalized
    assert "`queue.key.dead_letter`" in section
    assert "`{name}::dead_letter`" in section
    assert "not the built-in default DLQ list" in normalized
    assert "raw payload bytes only" in normalized
    assert "without exception metadata, final delivery-count metadata, timestamps, deduplication keys" in normalized
    assert "internal delivery envelope" in normalized
    assert "Two identical payloads dead-lettered separately are indistinguishable" in normalized
    assert "inspect first" in normalized
    assert "`LLEN` / `LRANGE`" in section
    assert "configured DLQ key or application-owned tooling" in normalized
    assert "intentionally republish" in normalized
    assert "move records to a separate repair queue" in normalized
    assert "trim/archive, or discard" in normalized
    assert "idempotency and deduplication policy" in normalized
    assert "suppressed by publish-side deduplication" in normalized
    assert "original dedup key is live" in normalized
    assert "changes the replay key" in normalized
    assert "waits for TTL expiry" in normalized
    assert "disables deduplication for the repair path" in normalized
    assert "Do not treat blind `LPUSH` or `RPUSH` of DLQ records back to pending" in normalized
    assert "universal safe replay workflow" in normalized


def test_readme_custom_gateway_dlq_examples_point_to_manual_handling_contract() -> None:
    readme = README_PATH.read_text(encoding="utf-8")
    lines = readme.splitlines()
    failures: list[str] = []

    for start_line, end_line, body in _markdown_fenced_examples(readme):
        if "dead_letter_queue" not in body or "max_delivery_count" not in body:
            continue

        window = "\n".join(lines[max(0, start_line - 9) : min(len(lines), end_line + 8)])
        normalized = " ".join(window.split()).lower()
        required_terms = (
            "#dead-letter-queue",
            "manual inspection",
            "repair",
            "archive",
            "trim",
            "replay",
            "terminal retained raw payloads",
            "not automatically retried",
        )
        missing_terms = [term for term in required_terms if term not in normalized]
        if missing_terms:
            failures.append(f"README.md:{start_line}-{end_line} missing {missing_terms}")

    assert failures == []


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
