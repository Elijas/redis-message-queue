# redis-message-queue

## Releasing a New Version

Releases are fully automated via GitHub Actions. Do NOT bump versions or publish locally.

### Trigger a release

```bash
gh workflow run release.yml --ref main -f bump=patch
gh workflow run release.yml --ref main -f bump=minor
gh workflow run release.yml --ref main -f bump=major
```

Run the command after the release workflow migration is merged.

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
