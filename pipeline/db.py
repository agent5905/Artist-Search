"""Supabase access: migrations through the Management API, rows and functions through the Data API.

    uv run python -m pipeline.db migrate

Nothing here prints a value from .env. Errors carry the HTTP status and the server's message,
never the request URL (it holds the project ref).
"""

import itertools
import sys
import time
from collections.abc import Callable, Iterable
from typing import Any, Literal

import httpx

from pipeline.config import ROOT, load_env

Key = Literal["publishable", "secret"]
_KEY_ENV = {"publishable": "VITE_SUPABASE_PUBLISHABLE_KEY", "secret": "SUPABASE_SECRET_KEY"}
_http = httpx.Client(timeout=180)  # a vacuum or a rebuild at full scale is one statement of a minute or more
# Artists written per batch by load(). With ten works each that is a statement of about 100 rows, which
# stays far inside the Data API's 8 s even when the free tier is throttled (500 rows did not).
LOAD_BATCH = 10
VACUUM_EVERY = 50  # batches between vacuums during a load, so old row versions never pile up
LOAD_REST = 5.0  # the longest load() rests after a batch, in seconds


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

    Every row needs the same keys. Artists conflict on "slug", works on "artist_id,key" (the key is
    the deviation page for a DeviantArt work and the image for a work on the artist's own site).
    Returns the rows written, limited to the `returning` columns (pass "slug" to avoid
    downloading embeddings). The database leaves two kinds of row out, and so they are missing from
    the result: an artist who was removed (or who links to a removed site or profile), and a row
    that already holds exactly these values, which is not rewritten. So read the ids to attach works
    with from rpc("artist_ids", {}, "secret"), not from here. To load artists with their works, use load().
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
    """Apply sql/*.sql in name order, all or nothing. Every statement is idempotent, so rerunning changes nothing."""
    # One call, so one transaction: no visitor meets tables of one version under functions of another.
    sql("\n".join(path.read_text(encoding="utf-8") for path in sorted((ROOT / "sql").glob("*.sql"))))
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


def load(artists: list[dict[str, Any]], works: list[dict[str, Any]], neighbors: list[dict[str, Any]]) -> int:
    """Write artists, their works and their style neighbours a few at a time, so the public never
    meets a database in mid-rewrite. Returns how many neighbour rows were skipped.

    An artist is a row of the artists table: "slug", "name" and "da_username" are required;
    "site_url" with "site_domain" (both or neither), "tagline", "top_tags", "search_text", "profile",
    "specialties", "mediums", "open_for_work", "profile_embedding" and "hidden" are optional.
    A work names its artist by "artist_slug" and needs "source" ('deviantart' or 'site'), "page_url"
    and "image_url"; "title", "description", "tags", "position" and the image's size in pixels,
    "width" with "height" (both or neither), are optional.
    A neighbour is {"artist_slug", "neighbor_slug", "rank", "score"}, rank 1 the nearest; "picks" is optional:
    the "position" of each of the neighbour's works to show beside the artist, first to last, four at most.
    Within one call every artist needs the same keys, and so does every work.

    First the artists with their works, LOAD_BATCH artists at a time. Then the neighbours, by which
    time every artist they can name is in the database: each artist named as an "artist_slug" has all
    their previous neighbours replaced, and an artist named in no row keeps theirs. A neighbour row
    naming an artist who is not in the database (removed, or never loaded) is skipped and counted.
    To leave an artist with no neighbours, send one row for them with None as "neighbor_slug",
    "rank" and "score": it too is counted as skipped, so take the number of those off the answer.
    A work is written only with its artist: one whose artist is not in `artists`, or whose artist the
    database skipped (a removed artist), is left out.

    After each batch it rests for as long as the batch took (LOAD_REST at most), so the load uses
    half of what the database can do and visitors get the rest; a slow database is given more room.
    Dead row versions are vacuumed every VACUUM_EVERY batches, without the statistics pass: that one
    samples whole tables, which is the heaviest reading a load would do. Safe to repeat: rows that
    have not changed are not rewritten.
    """
    by_artist: dict[str, list[dict[str, Any]]] = {}
    for work in works:
        row = {name: value for name, value in work.items() if name != "artist_slug"}
        by_artist.setdefault(work["artist_slug"], []).append(row)
    lists: dict[str, list[dict[str, Any]]] = {}
    for row in neighbors:
        lists.setdefault(row["artist_slug"], []).append(row)
    _paced(itertools.batched(artists, LOAD_BATCH), lambda batch: _load_batch(list(batch), by_artist))
    skipped = _paced(
        itertools.batched(lists.values(), LOAD_BATCH),  # an artist's list is never split: it is replaced whole
        lambda batch: rpc("replace_neighbors", {"p_rows": [row for rows in batch for row in rows]}, "secret"),
    )
    after_load()
    return skipped


def _paced(batches: Iterable[Any], write: Callable[[Any], int]) -> int:
    """Write each batch, rest after it, vacuum every VACUUM_EVERY batches. Returns the sum of what write returns."""
    total = 0
    for number, batch in enumerate(batches, start=1):
        began = time.perf_counter()
        for attempt in (1, 2):
            try:
                total += write(batch)
                break
            except RuntimeError as error:
                # One more try for a batch the database was too busy to finish in time (it is safe to repeat).
                if attempt == 2 or "57014" not in str(error):
                    raise
                time.sleep(LOAD_REST)
        time.sleep(min(time.perf_counter() - began, LOAD_REST))
        if number % VACUUM_EVERY == 0:
            vacuum_analyze(analyze=False)
    return total


def _load_batch(batch: list[dict[str, Any]], by_artist: dict[str, list[dict[str, Any]]]) -> int:
    slugs = [row["slug"] for row in batch]
    upsert("artists", batch, "slug", "slug")
    ids = rpc("artist_ids", {"p_slugs": slugs}, "secret")
    rows = [{**work, "artist_id": ids[slug]} for slug in slugs if slug in ids for work in by_artist.get(slug, [])]
    upsert("works", rows, "artist_id,key", "id")
    return 0


def after_load() -> None:
    """Finish a load: sweep, rebuild what the public functions read, vacuum. load() calls it.

    Call it yourself after writing rows with upsert(). The sweep deletes any artist on the removal
    list that a removal had to leave because a load held the row. Both go through the Management
    API, which has no 8 s limit.
    """
    sql("select public.delete_removed()")
    sql("select public.refresh_search_words()")
    vacuum_analyze()


def vacuum_analyze(analyze: bool = True) -> None:
    """Clear dead row versions and, unless told not to, refresh the planner's statistics."""
    command = "vacuum (analyze)" if analyze else "vacuum"
    for table in ("artists", "works", "artist_neighbors", "search_words"):
        sql(f"{command} public.{table}")  # one call each: vacuum cannot share a transaction


if __name__ == "__main__":
    if sys.argv[1:] != ["migrate"]:
        sys.exit("usage: python -m pipeline.db migrate")
    migrate()
    size = sql("select pg_database_size(current_database()) as bytes")[0]["bytes"]
    print(f"migrated. Database size: {int(size) / 1024 / 1024:.1f} MB")
