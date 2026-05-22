from __future__ import annotations

import re
import subprocess
import sys
from pathlib import Path

BASELINE_PATH = Path(__file__).with_name("mypy_baseline.txt")
MYPY_ERROR_RE = re.compile(r"^Found (\d+) errors? in ", re.MULTILINE)


def _read_baseline() -> int:
    for line in BASELINE_PATH.read_text(encoding="utf-8").splitlines():
        value = line.strip()
        if value and not value.startswith("#"):
            return int(value)
    raise ValueError(f"{BASELINE_PATH} does not contain a baseline count")


def _count_mypy_errors(output: str) -> int:
    if "Success: no issues found" in output:
        return 0
    match = MYPY_ERROR_RE.search(output)
    if match is None:
        raise ValueError("could not find mypy error summary in output")
    return int(match.group(1))


def main() -> int:
    baseline = _read_baseline()
    result = subprocess.run(
        [sys.executable, "-m", "mypy", "redis_message_queue/"],
        check=False,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    output = result.stdout
    print(output, end="")

    try:
        current = _count_mypy_errors(output)
    except ValueError as exc:
        print(f"mypy baseline check failed: {exc}", file=sys.stderr)
        return result.returncode or 1

    print(f"mypy errors: current={current}, baseline={baseline}")
    if current > baseline:
        print(
            "mypy error count exceeded the committed baseline; fix the new errors or ratchet the baseline down only.",
            file=sys.stderr,
        )
        return 1
    if result.returncode not in (0, 1):
        return result.returncode
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
