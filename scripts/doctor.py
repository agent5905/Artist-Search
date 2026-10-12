"""Stack check: Supabase reachable, pgvector present, database under budget, Cloudflare token valid.

    python scripts/doctor.py

Stdlib only. Reads .env and prints no value from it. Exit code 1 if anything fails,
so it doubles as the "database size is under the 400 MB budget" fixed check.
"""

import json
import pathlib
import sys
import urllib.error
import urllib.request

ROOT = pathlib.Path(__file__).resolve().parent.parent
DB_BUDGET_MB = 400  # spec hard rule 10: Supabase free tier is 500 MB, budget 400


def load_env() -> dict[str, str]:
    env = {}
    for line in (ROOT / ".env").read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            key, value = line.split("=", 1)
            env[key.strip()] = value.strip().strip("\"'")
    return env


def call(url: str, token: str, body: dict | None = None):
    request = urllib.request.Request(
        url,
        data=json.dumps(body).encode() if body is not None else None,
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "User-Agent": "artist-search-doctor",
        },
    )
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            return json.load(response)
    except urllib.error.HTTPError as error:
        # The URL holds the project ref / account id, so report the status only.
        raise RuntimeError(f"HTTP {error.code}") from None


def sql(env: dict[str, str], query: str) -> list[dict]:
    url = f"https://api.supabase.com/v1/projects/{env['SUPABASE_PROJECT_REF']}/database/query"
    return call(url, env["SUPABASE_ACCESS_TOKEN"], {"query": query})


def check_supabase(env: dict[str, str]) -> str:
    url = f"https://api.supabase.com/v1/projects/{env['SUPABASE_PROJECT_REF']}"
    project = call(url, env["SUPABASE_ACCESS_TOKEN"])
    if project.get("status") != "ACTIVE_HEALTHY":
        raise RuntimeError(f"project status {project.get('status')}")
    return f"{project['status']}, region {project.get('region')}, Postgres {project.get('database', {}).get('version')}"


def check_pgvector(env: dict[str, str]) -> str:
    rows = sql(env, "select default_version, installed_version from pg_available_extensions where name = 'vector'")
    if not rows:
        raise RuntimeError("pgvector is not available on this project")
    installed = rows[0]["installed_version"]
    return f"installed {installed}" if installed else f"available {rows[0]['default_version']}, NOT installed"


def check_db_size(env: dict[str, str]) -> str:
    size_mb = int(sql(env, "select pg_database_size(current_database()) as bytes")[0]["bytes"]) / 1024 / 1024
    if size_mb > DB_BUDGET_MB:
        raise RuntimeError(f"{size_mb:.1f} MB is over the {DB_BUDGET_MB} MB budget")
    return f"{size_mb:.1f} MB of {DB_BUDGET_MB} MB budget"


def check_cloudflare(env: dict[str, str]) -> str:
    token = env["CLOUDFLARE_API_TOKEN"]
    for url in (
        f"https://api.cloudflare.com/client/v4/accounts/{env['CLOUDFLARE_ACCOUNT_ID']}/tokens/verify",
        "https://api.cloudflare.com/client/v4/user/tokens/verify",
    ):
        try:
            return f"token {call(url, token)['result']['status']}"
        except RuntimeError:
            continue
    raise RuntimeError("token did not verify")


def main() -> int:
    env = load_env()
    failed = 0
    for name, check in (
        ("supabase project", check_supabase),
        ("pgvector", check_pgvector),
        ("database size", check_db_size),
        ("cloudflare", check_cloudflare),
    ):
        try:
            print(f"ok    {name}: {check(env)}")
        except Exception as error:  # report every check, then fail
            print(f"FAIL  {name}: {error}")
            failed += 1
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
