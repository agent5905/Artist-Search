"""Supabase access: migrations through the Management API, rows and functions through the Data API.

    uv run python -m pipeline.db migrate

Nothing here prints a value from .env. Errors carry the HTTP status and the server's message,
never the request URL (it holds the project ref).
"""

import sys
import time
from typing import Any, Literal

import httpx

from pipeline.config import ROOT, load_env

Key = Literal["publishable", "secret"]
_KEY_ENV = {"publishable": "VITE_SUPABASE_PUBLISHABLE_KEY", "secret": "SUPABASE_SECRET_KEY"}
_http = httpx.Client(timeout=180)  # a full-scale load or rewrite is one statement of a minute or more


def _send(method: str, url: str, headers: dict[str, str], **kwargs: Any) -> httpx.Response:
    try:
        return _http.request(method, url, headers=headers, **kwargs)
    except (httpx.HTTPError, httpx.InvalidURL) as error:
        # httpx errors can name the URL, so only the kind of error is passed on.
        raise RuntimeError(f"request failed: {type(error).__name__}") from None


def _checked(response: httpx.Response) -> Any:
    if response.is_error:
        message = response.text[:500]
        for value in load_env().values():
            if len(value) >= 8:
                message = message.replace(value, "***")
        raise RuntimeError(f"HTTP {response.status_code}: {message}")
    return response.json() if response.content else None


def sql(query: str) -> list[dict[str, Any]]:
    """Run SQL as the database owner. All statements of one call commit together or not at all."""
    env = load_env()
    url = f"https://api.supabase.com/v1/projects/{env['SUPABASE_PROJECT_REF']}/database/query"
    return _checked(
        _send("POST", url, {"Authorization": f"Bearer {env['SUPABASE_ACCESS_TOKEN']}"}, json={"query": query})
    )


def data_api(method: str, path: str, key: Key, **kwargs: Any) -> httpx.Response:
    """One Data API call, status unchecked.

    The key goes in the `apikey` header and nothing else is needed: the publishable key
    runs as the `anon` role, the secret key as `service_role`.
    """
    env = load_env()
    headers = {"apikey": env[_KEY_ENV[key]], **kwargs.pop("headers", {})}
    return _send(method, f"{env['VITE_SUPABASE_URL']}/rest/v1/{path}", headers, **kwargs)


def upsert(table: str, rows: list[dict[str, Any]], on_conflict: str, returning: str = "*") -> list[dict[str, Any]]:
    """Insert rows with the service key, updating the ones that already exist on the on_conflict columns.

    Every row needs the same keys. Returns the rows written, limited to the `returning` columns
    (pass "id,slug" to avoid downloading embeddings). The database leaves two kinds of row out,
    and so they are missing from the result: an artist who was removed (or who links to a removed
    site or profile), and a work that already holds exactly these values, which is not rewritten.
    After loading works, call refresh_style_centroids() and then vacuum_analyze().
    """
    if not rows:
        return []
    # ponytail: one request per call. Ceiling: a few thousand rows with embeddings; batch the calls past that.
    response = data_api(
        "POST",
        table,
        "secret",
        params={"on_conflict": on_conflict, "select": returning},
        headers={"Prefer": "resolution=merge-duplicates,return=representation"},
        json=rows,
    )
    return _checked(response)


def rpc(name: str, args: dict[str, Any], key: Key = "publishable") -> Any:
    """Call a database function as the public role (publishable) or the service role (secret)."""
    return _checked(data_api("POST", f"rpc/{name}", key, json=args))


def migrate() -> None:
    """Apply sql/*.sql in name order. Every statement is idempotent, so rerunning changes nothing."""
    for path in sorted((ROOT / "sql").glob("*.sql")):
        sql(path.read_text(encoding="utf-8"))
    # The Data API reads the schema asynchronously. Wait until it lists every function it can call.
    sql("notify pgrst, 'reload schema'")
    callable_functions = sql("""
        select '/rpc/' || proname as path from pg_proc
        where pronamespace = 'public'::regnamespace and has_function_privilege('service_role', oid, 'execute')
          and prorettype <> 'trigger'::regtype and (pronargs = 0 or proargnames is not null)
    """)
    wanted = {row["path"] for row in callable_functions}
    for _ in range(30):
        listing = data_api("GET", "", "secret")
        if listing.is_success and wanted <= set(listing.json()["paths"]):
            return
        time.sleep(1)
    raise RuntimeError("the Data API did not pick up the schema within 30 seconds")


def refresh_style_centroids() -> int:
    """Rebuild what similar_artists reads (artist_styles) from the works. Returns how many artists changed.

    similar_artists cannot see an artist until this has run. It goes through the Management API,
    which has no 8 s limit: a first fill for 3,000 artists writes about 46 MB and takes 8 to 13 s.
    """
    return sql("select public.refresh_style_centroids() as changed")[0]["changed"]


def vacuum_analyze() -> None:
    """Clear dead row versions and refresh the planner's statistics. Run it after a load."""
    for table in ("artists", "works", "artist_styles"):
        sql(f"vacuum (analyze) public.{table}")  # one call each: vacuum cannot share a transaction


if __name__ == "__main__":
    if sys.argv[1:] != ["migrate"]:
        sys.exit("usage: python -m pipeline.db migrate")
    migrate()
    size = sql("select pg_database_size(current_database()) as bytes")[0]["bytes"]
    print(f"migrated. Database size: {int(size) / 1024 / 1024:.1f} MB")
