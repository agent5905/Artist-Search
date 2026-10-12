#!/bin/sh
# End of a round: run the fixed checks, then push main. Usage: sh scripts/ship.sh
set -e
cd "$(dirname "$0")/.."

uv run ruff check .
uv run ruff format --check .
uv run mypy
uv run pytest -q
python scripts/secret_scan.py --all

# The database and Supabase checks gate the push. The Cloudflare token check does not, until the owner replaces it.
python scripts/doctor.py | tee doctor.out || true
if grep -q -E "^FAIL  (supabase|pgvector|database)" doctor.out; then rm -f doctor.out; exit 1; fi
rm -f doctor.out

# The token comes from .env and is handed to git through a credential helper, so it is never printed or stored.
GH_TOKEN="$(grep -E '^GH_TOKEN=' .env | head -1 | cut -d= -f2- | tr -d '\r' | sed -e "s/^[\"']//" -e "s/[\"']$//")"
export GH_TOKEN
GIT_TERMINAL_PROMPT=0 GCM_INTERACTIVE=never git -c credential.helper= \
  -c credential.helper='!f() { test "$1" = get && echo username=x-access-token && echo "password=$GH_TOKEN"; }; f' \
  push origin main
