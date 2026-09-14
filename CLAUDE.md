# redis-message-queue

## Releasing a New Version

Releases are fully automated via GitHub Actions. Do NOT bump versions or publish locally.

Releasing is **two dispatches** with a reviewed PR between them. `main` is
protected by strict required CI with no bypass, so the version bump cannot be
pushed straight to `main` — it lands through a normal PR like any other change.

Confirm `.github/workflows/release.yml` exists on `main` before triggering.

### Step 1 — prepare the release PR

```bash
gh workflow run release.yml --ref main -f bump=patch
gh workflow run release.yml --ref main -f bump=minor
gh workflow run release.yml --ref main -f bump=major
```

This runs CI, bumps the version, refreshes the lockfile and formatting, and
opens a `release/vX.Y.Z` PR. Nothing is tagged or published. Review and merge
that PR once required CI is green.

### Step 2 — finalize after the PR merges

```bash
gh workflow run release.yml --ref main -f finalize=v1.2.3
```

This verifies `main` really carries that version, tags `main`'s tip, and
publishes to PyPI. Running it before the PR merges fails with a clear error
rather than releasing anything.

### What the workflow does

**Prepare (`-f bump=...`)**

1. Verifies the dispatch runs from `refs/heads/main` and that exactly one mode input is set, then runs full CI (`.github/workflows/ci.yml`)
2. Fails fast if the run is pinned to a commit that is no longer `origin/main`'s tip
3. `bump-my-version bump <patch|minor|major>` updates `pyproject.toml`
4. `devtools/bump_readme_version.py` updates README install guidance
5. Runs lock/build/ruff verification and a porcelain check
6. Refuses to proceed if tag `vX.Y.Z` or branch `release/vX.Y.Z` already exists
7. Commits to `release/vX.Y.Z`, pushes **that branch only**, and opens a PR — `main` is never written
8. Explicitly dispatches `ci.yml` and `codeql.yml` onto the release branch (a branch pushed by `GITHUB_TOKEN` fires no `push`/`pull_request` events, so the PR would otherwise have no checks to satisfy)

**Finalize (`-f finalize=vX.Y.Z`)**

9. Verifies the run is on `main`'s live tip, that `pyproject.toml` is at that exact version, and that the tag does not already exist
10. Creates an annotated `vX.Y.Z` tag and pushes **the tag only** — the tag namespace is outside branch protection, so this needs no bypass
11. Internally re-dispatches `release.yml` on the immutable tag (a tag pushed by `GITHUB_TOKEN` does not fire `push: tags:`)
12. Re-runs CI on the tag, then publishes to PyPI via OIDC trusted publishing — checking out the tested SHA (not the mutable tag name), verifying the tag matches the built version, and refusing any commit not reachable from `origin/main`

### Repository setting this depends on

Settings → Actions → General → Workflow permissions →
**"Allow GitHub Actions to create and approve pull requests"** must be enabled,
or step 1 cannot open its PR. Default token permissions stay read-only; this
toggle is separate from them. Auto-merge needs no PR approvals.

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

Public docs — `README.md`, `CHANGELOG.md`, `docs/*.md` — must read in
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
