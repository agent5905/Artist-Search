"""Removals: look for each pending request's proof code where only the artist can put it, and remove whoever posted it.

    uv run python -m pipeline.removals --dry-run     what a run would remove, and nothing changed anywhere
    uv run python -m pipeline.removals

Hard rule 7 and owner decision 6. The site gives an artist a code for their slug (start_removal_proof) and logs
their request (request_removal). This stage reads every pending request with the service key, newest first, and
looks for that slug's code in two places, in this order, and nowhere else:
  1. The DeviantArt profile of the handle the artist is listed under, through the official API: its bio (the
     About text the site points to) and its tagline. Only the account holder writes those two fields: a comment,
     a watcher's note or anybody's deviation is not in them. The answer counts only when it is the account the
     local library keeps for that handle, renamed or not, or, for a handle it keeps none for, that handle's own.
  2. When the code is not there and the artist has a site on record: that site's front page, through the polite
     fetcher (pipeline/crawl.py), which reads robots.txt first and opens a site only when its front page links
     back to that profile. And only while the profile, as just read, still names the site: a site the artist has
     left, or one their profile never named, is not theirs to prove with.
The code is the slug's own, so one pasted onto another artist's page proves nothing for them, and no page of
another artist is read for this one. Codes do not expire. One made before the owner replaced the secret is not
the slug's code any more, and proves nothing.

A code found where it must be goes to confirm_removal, which checks it again and removes the artist at once and
for good. The two key hashes that removal put on the database's list then go on the local store's list
(store.import_removed), so no stage here collects the artist again. The next push or collect purges what is kept
about them, under every handle of their account, as it does for every removal on the database's list.

Anything less leaves the request alone for the owner (reject_removal, purge_removed): a slug that is no artist's,
a page that could not be read, a code that is not there. A request stays pending until it is answered, so a code
posted after one check is found by the next.

One request at a time. Every request to DeviantArt waits its turn in discover.Pace, under its lock: a run that
finds another stage talking to the API stops before it asks anything. Every request to a site goes through the
fetcher. A run with nothing pending asks neither.

`--dry-run` makes the same reads and requests, and removes nothing: no call to confirm_removal, nothing written to
the local store. (The API's pace file is kept as for any request to it: discover.Pace.) Stdout is one JSON object
of counts; nothing names an artist. The service key is read by pipeline/db.py and never printed.
"""

import argparse
import html
import json
import pathlib
import re
import sys
import traceback
from typing import Any

import httpx

from pipeline import config, crawl, db, discover, library, store

CODE = re.compile(r"artist-search-removal-[0-9a-f]{32}")  # what removal_code() makes (sql/002_functions.sql)
PROFILE_FIELDS = ("bio", "tagline")  # the profile's own text: see the module's docstring
_TAG = re.compile(r"<[^>]*>")
COUNTS = (
    "requests_pending",
    "requests_for_no_artist",  # removed since, or never listed: nobody to look for
    "proven_on_deviantart",
    "proven_on_own_site",
    "artists_removed",  # with --dry-run: would be
    "proofs_refused_by_database",  # the code changed between the read and the removal
    "left_for_owner_no_code_found",
    "left_for_owner_not_checked",  # a page could not be read, or was not the artist's: looked at again next run
)
ARTIST_COLUMNS = "slug,da_username,site_url,site_domain,da_key,site_key"


def _rows(table: str, **params: str) -> list[dict[str, Any]]:
    """Rows of a table of the public database, read through the Data API with the service key."""
    return db._checked(db.data_api("GET", table, "secret", params=params))


def _holds(text: object, code: str) -> bool:
    """True when the text holds the code: as written, or with its markup taken out (a page may split a long word
    with <wbr>, or spell a hyphen as an entity). As written also finds it in an attribute, a <meta> tag's say."""
    return isinstance(text, str) and any(code in version for version in (text, html.unescape(_TAG.sub("", text))))


def _look(
    api: discover._Api, fetcher: crawl.PoliteFetcher, artist: dict[str, Any], code: str, local: store.Snapshot
) -> str:
    """Where the artist's code is: "deviantart" or "own_site". Or why it was not found: "no_code_found" when every
    page that could hold it was read, "not_checked" when one could not be, or was somebody else's."""
    handle = artist["da_username"]
    if not isinstance(handle, str) or not store.HANDLE.fullmatch(handle):
        return "not_checked"
    response = api.get(f"/user/profile/{handle}")
    try:
        profile = response.json() if response.status_code == 200 else None
    except ValueError:
        profile = None
    if not isinstance(profile, dict):
        return "not_checked"
    user = profile.get("user")
    user_id, name = (user.get("userid"), user.get("username")) if isinstance(user, dict) else (None, None)
    # The artist's own profile: the account the library keeps for the handle, under whatever handle it answers
    # now; for a handle the library keeps no account for, that handle's profile. Not a profile the handle has
    # passed to another account, and not one answer given for every lookup, which the API did once.
    kept = library.account_hash(store.artist_key(handle), local)
    if kept:
        theirs = kept in (library.account(user_id), library.account(user_id, "da-user"))
    else:
        theirs = isinstance(name, str) and name.lower() == handle.lower()
    if not theirs:
        return "not_checked"
    if any(_holds(profile.get(field), code) for field in PROFILE_FIELDS):
        return "deviantart"
    named = {store.registrable_domain(host) for host in store.hosts_in(profile.get("website"))}
    if not artist["site_url"] or artist["site_domain"] not in named:
        return "no_code_found"
    opened = fetcher.open_site(artist["site_url"], discover.PROFILE_URL.format(handle))
    if isinstance(opened, crawl.FetchResult):  # robots.txt, an AI opt-out, no link back, no answer
        return "not_checked"
    page = opened.front_page  # the page that proved the site theirs. None only for a site opened twice in a run
    if page is None or not page.ok:
        return "not_checked"
    return "own_site" if _holds(page.content.decode("utf-8", "replace"), code) else "no_code_found"


def run(
    counts: dict[str, int],
    dry_run: bool,
    client_id: str,
    client_secret: str,
    *,
    local: pathlib.Path | str | None = None,
    transport: httpx.BaseTransport | None = None,
    fetcher: crawl.PoliteFetcher | None = None,
) -> None:
    """One pass over the pending requests, counted into `counts` (COUNTS) as it goes, so that a run that stops
    still says what it did. discover.Stop when another run holds the API, or the API refuses for too long;
    RuntimeError when the database fails. Either ends the run there, and what was removed stays removed."""
    # ponytail: every pending request is looked at on every run, one API request each and up to three of their
    # site's. Ceiling: anyone can file a request for every slug, and a run then takes 2 s an artist. Cap the run
    # and remember when each was last looked at, if that happens.
    requests = _rows("removal_requests", select="artist_slug", order="last_at.desc")
    counts["requests_pending"] = len(requests)
    pending = []
    for request in requests:
        found = _rows("artists", select=ARTIST_COLUMNS, slug=f"eq.{request['artist_slug']}")
        if found:
            pending.append(found[0])
        else:
            counts["requests_for_no_artist"] += 1
    if not pending:
        return
    fetcher = fetcher or crawl.PoliteFetcher()
    # The API client the collect stage uses: the project's user agent, no redirect followed, no URL logged.
    with (
        discover._http_logs_off(),
        httpx.Client(
            headers={"User-Agent": config.USER_AGENT}, transport=transport, timeout=30, follow_redirects=False
        ) as http,
    ):
        api = discover._Api(http, client_id, client_secret)
        with api.pace, store.snapshot(local) as now:  # the lock first: a run that cannot have the API reads nothing
            for artist in pending:
                code = db.rpc("start_removal_proof", {"p_slug": artist["slug"]}, "secret")
                where = _look(api, fetcher, artist, code, now) if CODE.fullmatch(str(code)) else "not_checked"
                if where not in ("deviantart", "own_site"):
                    counts[f"left_for_owner_{where}"] += 1
                    continue
                counts[f"proven_on_{where}"] += 1
                if dry_run:
                    counts["artists_removed"] += 1
                # The database first: it is the judge of the code. Here only once it has removed them, and only
                # what it put on its own list, which every later push takes as the database's own removal.
                elif db.rpc("confirm_removal", {"p_slug": artist["slug"], "p_code": code}, "secret") is True:
                    store.import_removed([key for key in (artist["da_key"], artist["site_key"]) if key], local)
                    counts["artists_removed"] += 1
                else:
                    counts["proofs_refused_by_database"] += 1


def _where(exc: Exception) -> str:
    """An exception as its type and the line of this file it came through. Never its message: a database error
    can quote the row it refused."""
    lines = [frame.lineno for frame in traceback.extract_tb(exc.__traceback__) if frame.filename == __file__]
    return f"{type(exc).__name__} at removals.py line {lines[-1] if lines else '?'}"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    parser.add_argument("--dry-run", action="store_true", help="make the same checks, and remove nobody")
    args = parser.parse_args(argv)
    try:
        env = config.load_env()
    except FileNotFoundError:
        env = {}
    missing = [name for name in discover.ENV_NAMES if not env.get(name)]
    if missing:
        print(f"Missing in .env: {', '.join(missing)} (see .env.example)", file=sys.stderr)
        return 1
    counts = dict.fromkeys(COUNTS, 0)
    status = 0
    try:
        run(counts, args.dry_run, *(env[name] for name in discover.ENV_NAMES))
    except discover.Stop as stop:
        print(f"stopped: {stop}", file=sys.stderr)
        status = 1
    except Exception as exc:  # deliberately broad: nothing gets out of here as a traceback
        print(f"stopped: {_where(exc)}", file=sys.stderr)
        status = 1
    print(json.dumps(counts))  # counts only, nothing about any artist
    return status


if __name__ == "__main__":
    sys.exit(main())
