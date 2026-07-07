"""Pin the PyPI long_description link rewrite (see ``hatch_build.py``).

The on-disk README keeps repo-relative links (GitHub-correct, and enforced by
``test_docs_contracts.py``). ``hatch_build.py``'s metadata hook rewrites them to
absolute GitHub URLs in the *published PyPI long_description only*. These tests
pin that rewrite so a regression can't silently ship a PyPI page full of 404
links, and assert the on-disk README is never absolutized.
"""

import tomllib
from pathlib import Path

from hatch_build import rewrite_readme_links

ROOT = Path(__file__).resolve().parents[1]
README = ROOT / "README.md"
REPO_URL = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))["project"]["urls"]["Repository"]
BLOB_BASE = f"{REPO_URL}/blob/main/"


def test_relative_repo_links_become_absolute() -> None:
    md = "See [cfg](docs/configuration.md#dedup), [ch](CHANGELOG.md), and [lic](LICENSE)."
    out = rewrite_readme_links(md, REPO_URL)
    assert f"({BLOB_BASE}docs/configuration.md#dedup)" in out
    assert f"({BLOB_BASE}CHANGELOG.md)" in out
    assert f"({BLOB_BASE}LICENSE)" in out


def test_external_anchor_and_mailto_targets_untouched() -> None:
    md = (
        "[ex](https://github.com/x/y/tree/main/examples) "
        "[a](#in-page) "
        "![badge](https://img.shields.io/x) "
        "[m](mailto:x@example.com)"
    )
    assert rewrite_readme_links(md, REPO_URL) == md


def test_real_readme_has_no_relative_repo_links_after_rewrite() -> None:
    rewritten = rewrite_readme_links(README.read_text(encoding="utf-8"), REPO_URL)
    # No repo-relative file link survives — these are exactly the ones that 404 on PyPI.
    for dead in ("](docs/", "](CHANGELOG.md", "](LICENSE)"):
        assert dead not in rewritten, dead
    # In-page anchors are preserved (they resolve on both GitHub and PyPI).
    assert "](#" in rewritten
    # Already-absolute links are preserved verbatim.
    assert f"]({REPO_URL}/tree/main/examples)" in rewritten


def test_source_readme_stays_relative() -> None:
    # The on-disk README must NOT be absolutized: relative links are GitHub-correct
    # and test_docs_contracts.py asserts the docs/ prefix on README cross-file links.
    source = README.read_text(encoding="utf-8")
    assert "](docs/configuration.md#deduplication)" in source
    assert BLOB_BASE not in source
