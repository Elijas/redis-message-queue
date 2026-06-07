"""Hatch metadata hook: publish the README to PyPI with repo-relative links absolutized.

The on-disk ``README.md`` deliberately keeps repo-relative links (e.g.
``docs/configuration.md#deduplication``) so they resolve on GitHub's repo
renderer and satisfy the doc-contract tests (``tests/test_docs_contracts.py``
asserts the ``docs/`` prefix on README cross-file links). Those same relative
links 404 on the PyPI project page, which resolves them against ``pypi.org``
instead of the repository. This hook rewrites repo-relative Markdown link
targets to absolute ``<repo>/blob/main/...`` URLs *only in the long_description
embedded in the built artifact* — the source file is never modified.

Scope is intentionally narrow: only the PyPI long_description differs from the
source README, so a defect here can at worst mis-render the PyPI page; it cannot
change the installed package's runtime behavior.
"""

import re
import tomllib
from pathlib import Path

from hatchling.metadata.plugin.interface import MetadataHookInterface

# Markdown link/image targets we must NOT rewrite: external URLs, in-page
# anchors (``#section``), and ``mailto:`` links already resolve everywhere.
_EXTERNAL_TARGET = re.compile(r"^(?:https?://|#|mailto:)")
# A Markdown link/image target: the ``](target)`` tail. ``[^)\s]+`` stops at the
# first space, so a titled link (``](url "title")``) simply isn't matched and is
# left untouched rather than mangled.
_MARKDOWN_TARGET = re.compile(r"\]\(([^)\s]+)\)")


def rewrite_readme_links(markdown: str, repo_url: str) -> str:
    """Rewrite repo-relative Markdown link targets to absolute GitHub blob URLs.

    External (``http(s)://``), in-page (``#anchor``), and ``mailto:`` targets are
    returned unchanged. This makes README links resolve on the PyPI project page,
    which — unlike GitHub — does not resolve relative repository paths.
    """
    base = f"{repo_url.rstrip('/')}/blob/main/"

    def _absolutize(match: re.Match) -> str:
        target = match.group(1)
        if _EXTERNAL_TARGET.match(target):
            return match.group(0)
        return f"]({base}{target})"

    return _MARKDOWN_TARGET.sub(_absolutize, markdown)


class CustomMetadataHook(MetadataHookInterface):
    """Replace the file-based README with an inline, link-absolutized long_description."""

    def update(self, metadata: dict) -> None:
        root = Path(self.root)
        pyproject = tomllib.loads((root / "pyproject.toml").read_text(encoding="utf-8"))
        repo_url = pyproject["project"]["urls"]["Repository"]
        readme_text = (root / "README.md").read_text(encoding="utf-8")
        metadata["readme"] = {
            "content-type": "text/markdown",
            "text": rewrite_readme_links(readme_text, repo_url),
        }
