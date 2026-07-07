# redis-message-queue

## Releasing a New Version

Releases are fully automated via GitHub Actions. Do NOT bump versions or publish locally.

### Trigger a release

```bash
gh workflow run release.yml --ref main -f bump=patch
gh workflow run release.yml --ref main -f bump=minor
gh workflow run release.yml --ref main -f bump=major
```

Confirm `.github/workflows/release.yml` exists on `main` before triggering.

### What the workflow does

1. Runs full CI test suite (`.github/workflows/ci.yml`)
2. `bump-my-version bump <patch|minor|major>` updates `pyproject.toml` and creates the bump commit
3. `devtools/bump_readme_version.py` updates README install guidance
4. Runs lock/build/ruff verification
5. Creates an annotated `vX.Y.Z` tag and atomically pushes `main` plus the tag
6. Re-runs CI on the immutable tag
7. Builds from the tag and publishes to PyPI via OIDC trusted publishing

### Version is tracked in package metadata

- `pyproject.toml` (`version` field + `[tool.bumpversion] current_version`)

Both entries are updated automatically by `bump-my-version`. Do not edit these manually.

## Development

```bash
uv sync
uv run pytest
uv run ruff check .
uv run ruff format --check .
```

Integration tests require a local Redis; they use `redis://localhost:6379/15` by
default (override with `REDIS_URL`) and clean up their own keys. Without a
reachable Redis, integration tests are skipped, not failed — a green run
without Redis is not a full run. A `Taskfile.yml` mirrors these commands
(`task test`, `task lint-check`, `task lint-types` for the non-blocking mypy
check).

## Documentation conventions

Public docs — `README.md`, `CHANGELOG.md`, `UPGRADING.md`, `docs/*.md` — must read in
**user-facing register**: describe behavior and changes in terms a user can act on, never
internal development codenames (audit-round IDs like `R7`, lane IDs like `L02`, finding IDs
like `AD-13` / `AC-03` / `AB-05`, `OH-*` / `PF*` / `FIX-*` tracker IDs, single-letter labels
like `B2`/`M5`/`H5`, or capsule date-codenames). A user cannot look those up. This is
enforced by `tests/test_public_docs_no_internal_codenames.py`, which fails CI on any leak.
`CLAUDE.md` and contributor docs are out of scope and may reference internal IDs.

The `README.md` is a **bounded front door** (pitch, quickstarts, mental model, pointers);
reference and operational depth lives in tiered `docs/*.md` reached by one-line pointers,
not inlined into the README.

## Repo map and change checklist

- Sync queue: `redis_message_queue/redis_message_queue.py`; async mirror (kept in lockstep by hand, same method/parameter names): `redis_message_queue/asyncio/redis_message_queue.py`. Gateways: `_redis_gateway.py` / `_abstract_redis_gateway.py` (plus `asyncio/` siblings).
- Adding/changing a constructor option touches BOTH queue classes, the parameter table in `docs/api-reference.md`, the matching `docs/configuration.md` section, and `tests/test_gateway_constructor.py`.
- Docs are executable contracts: `tests/test_docs_contracts.py` pins doc phrasing/links and `tests/test_from_readme_example_feedback.py` extracts and runs the README quickstart blocks — run both after any README/docs edit.
