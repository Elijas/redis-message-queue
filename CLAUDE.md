# redis-message-queue

## Releasing a New Version

Releases are triggered manually via GitHub Actions. Do NOT publish locally.

### Steps

1. Bump version in `pyproject.toml` (`version` field)
2. Update the PyPI version badge in `README.md`
3. Commit and push to `main`
4. Trigger the release workflow:

```bash
gh workflow run release.yml
```

### What the workflow does

1. Runs full CI test suite (`.github/workflows/ci.yml`)
2. Builds with `poetry build`
3. Publishes to PyPI via OIDC trusted publishing

## Development

```bash
poetry install --with test
poetry run pytest
uvx ruff check .
uvx ruff format --check .
```
