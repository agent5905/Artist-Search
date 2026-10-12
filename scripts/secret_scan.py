"""Secret and image gate (hard rules 1 and 8). Stdlib only.

    python scripts/secret_scan.py          # staged files (the pre-commit hook)
    python scripts/secret_scan.py --all    # every tracked file (the per-round fixed check)

Fails if a file contains a real value from .env, matches a known key format, or is an image.
"""

import pathlib
import re
import subprocess
import sys

ROOT = pathlib.Path(__file__).resolve().parent.parent

# ponytail: a handful of key formats plus exact .env values; swap in gitleaks in CI if this repo grows more key types.
PATTERNS = [
    re.compile(p)
    for p in (
        r"gh[pousr]_[A-Za-z0-9]{36,}",
        r"github_pat_[A-Za-z0-9_]{40,}",
        r"sb_secret_[A-Za-z0-9_-]{20,}",
        r"sbp_[a-f0-9]{40}",
        r"cf[au]t_[A-Za-z0-9_-]{30,}",
        r"eyJ[A-Za-z0-9_-]{20,}\.eyJ[A-Za-z0-9_-]{20,}\.",
        r"AKIA[0-9A-Z]{16}",
        r"-----BEGIN [A-Z ]*PRIVATE KEY-----",
    )
]
# Strict reading of "no image files are tracked in git": icons go inline in markup.
IMAGE_EXTS = {".png", ".jpg", ".jpeg", ".gif", ".webp", ".avif", ".bmp", ".tif", ".tiff", ".ico", ".svg"}
MIN_VALUE_LEN = 16  # shorter .env values ("main", the repo name) are not secrets and appear everywhere


def env_values(path: pathlib.Path) -> list[str]:
    if not path.exists():
        return []
    values = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            value = line.split("=", 1)[1].strip().strip("\"'")
            if len(value) >= MIN_VALUE_LEN:
                values.append(value)
    return values


def problems(name: str, text: str, secrets: list[str]) -> list[str]:
    found = []
    if pathlib.PurePosixPath(name).suffix.lower() in IMAGE_EXTS:
        found.append("image file (hard rule 1: never store artwork in the repo)")
    base = pathlib.PurePosixPath(name).name
    if base.startswith(".env") and base != ".env.example":
        found.append("env files must never be committed")
    if any(s in text for s in secrets):
        found.append("contains a value from .env")
    found += [f"matches key pattern {p.pattern[:24]}" for p in PATTERNS if p.search(text)]
    return found


def git(*args: str) -> bytes:
    return subprocess.run(["git", *args], cwd=ROOT, capture_output=True, check=True).stdout


def main() -> int:
    scan_all = "--all" in sys.argv
    listing = git("ls-files", "-z") if scan_all else git("diff", "--cached", "--name-only", "--diff-filter=ACMR", "-z")
    secrets = env_values(ROOT / ".env")
    bad = 0
    for name in filter(None, listing.decode().split("\0")):
        blob = (ROOT / name).read_bytes() if scan_all else git("show", f":{name}")
        for problem in problems(name, blob.decode("utf-8", errors="ignore"), secrets):
            print(f"BLOCKED {name}: {problem}")  # never print the matching text
            bad += 1
    print("secret scan:", "FAILED" if bad else "clean")
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main())
