"""Removals: look for each due request's proof code where only the artist can put it, and remove whoever posted it.

    uv run python -m pipeline.removals --dry-run     what a run would remove, and nothing changed anywhere
    uv run python -m pipeline.removals

Hard rule 7 and owner decision 6. On the site an artist asks for their code (start_removal_proof, through the
site's server function), which files their request as one a code was given out for. A request without a code
(request_removal) is the owner's alone: this stage never reads it.

The queue (sql/003_limits.sql: removal_requests.code_at and due_at). A run reads, with the service key, at most
MAX_REQUESTS of the requests that are due, the one due longest first, and moves each one it looks at on: an hour
while its code is under a day old, a day while under a week, a week after that (_next_look). So a code is looked
for soon after it is given out and at least daily in its first week, a stale request costs one look a week, and
asking again moves nothing (the database keeps code_at and due_at as they are).

It looks for that slug's code in two places, and nowhere else:
  1. The DeviantArt profile of the handle the artist is listed under, through the official API: its bio (the
     About text the site points to) and its tagline, which only the account holder writes. Only when the answer
     is the account the local library keeps for the artist (under either spelling of its hash), renamed or not.
     For an artist the library keeps no account for, this place is not read: whoever holds the handle now could
     be somebody else.
  2. Their own site on record, while the profile as just read (the artist's account, or for an artist with no
     account kept, the handle's own) still names it: a <meta name="artist-search-removal"> in the front page's
     head, or the file /.well-known/artist-search-removal.txt holding the code and nothing else, read through
     the polite fetcher (crawl.PoliteFetcher.read_proof). A comment, a guestbook entry or anything else a
     visitor can write is in neither. A site their profile no longer names, such as a domain they let lapse and
     moved on from, or the site of a deleted account, is not read.
Case, the space around the code and HTML entities do not matter, nor do characters that show as nothing. The code
is the slug's own, so one pasted onto another artist's page proves nothing for them. Codes do not expire. One
made before the owner replaced the secret is not the slug's code any more, and proves nothing.

A code found where it must be goes to confirm_removal, which checks it again and removes the artist at once and
for good. The two key hashes that removal put on the database's list then go on the local store's list
(store.import_removed), so no stage here collects the artist again. The next push or collect purges what is kept
about them, under every handle of their account, as it does for every removal on the database's list.

Anything less leaves the request for the owner (reject_removal, purge_removed), and for its next look.

Every request to DeviantArt waits its turn in discover.Pace, and holds the API's lock for that request alone: a
collect run can start between two of them, and this run then stops at its next one. Every request to a site goes
through the fetcher. A run with nothing due asks neither.

`--dry-run` makes the same reads and requests, and changes nothing: no call to confirm_removal, nothing written to
the local store, no request moved on. (The API's pace file is kept as for any request to it: discover.Pace.) Stdout
is one JSON object of counts; nothing names an artist. The service key is read by pipeline/db.py and never printed.
"""

import argparse
import html
import json
import pathlib
import re
import sys
import traceback
from datetime import UTC, datetime, timedelta
from typing import Any

import httpx

from pipeline import config, crawl, db, discover, library, store

CODE = re.compile(r"artist-search-removal-[0-9a-f]{32}")  # what removal_code() makes (sql/002_functions.sql)
PROOF_NAME = "artist-search-removal"  # the <meta> name, and the file under /.well-known/ (crawl.read_proof)
PROFILE_FIELDS = ("bio", "tagline")  # the profile's own text: see the module's docstring
MAX_REQUESTS = 30  # looked at in one run: about a minute of the API, and the sites of those that have one
HOUR, DAY, WEEK = timedelta(hours=1), timedelta(days=1), timedelta(weeks=1)
_TAG = re.compile(r"<[^>]*>")
_UNSEEN = re.compile("[­​-‍⁠﻿]")  # a soft hyphen, zero-width spaces and joiners, a BOM
COUNTS = (
    "requests_due",  # read this run: MAX_REQUESTS at most
    "requests_for_no_artist",  # removed since, or never listed: nobody to look for
    "proven_on_deviantart",
    "proven_on_own_site",
    "artists_removed",  # with --dry-run: would be
    "proofs_refused_by_database",  # the code changed between the read and the removal
    "left_for_owner_no_code_found",
    "left_for_owner_not_checked",  # a place could not be read, or there is none we may read: looked at again later
)
ARTIST_COLUMNS = "slug,da_username,site_url,site_domain,da_key,site_key"


def _rows(table: str, **params: str) -> list[dict[str, Any]]:
    """Rows of a table of the public database, read through the Data API with the service key."""
    return db._checked(db.data_api("GET", table, "secret", params=params))


def _plain(text: str) -> str:
    """Text as a code is looked for in it: entities undone, what shows as nothing taken out, in lower case."""
    return _UNSEEN.sub("", html.unescape(text)).lower()


def _holds(text: object, code: str) -> bool:
    """True when the profile's text holds the code: as written, or with its markup taken out (a page may split a
    long word with <wbr>). As written also finds it in an attribute, a link's say."""
    return isinstance(text, str) and any(code in _plain(version) for version in (text, _TAG.sub("", text)))


def _next_look(code_at: str, now: datetime) -> datetime:
    """When a request is looked at again: an hour on while its code is under a day old, a day on while under a week,
    then a week on. So a code put up in its first week is found within a day, and a stale request costs little."""
    age = now - datetime.fromisoformat(code_at)
    return now + (HOUR if age < DAY else DAY if age < WEEK else WEEK)


def _move_on(request: dict[str, Any], now: datetime, dry_run: bool) -> None:
    """Set when the request is looked at next. Not in a dry run, which changes nothing."""
    if not dry_run:
        due = _next_look(request["code_at"], now).isoformat()
        where = {"artist_slug": f"eq.{request['artist_slug']}"}
        db._checked(db.data_api("PATCH", "removal_requests", "secret", params=where, json={"due_at": due}))


def _profile(api: discover._Api, handle: str) -> dict[str, Any] | None:
    """The handle's profile as the API answers it, or None. The API's lock is held for this request alone."""
    with api.pace:
        response = api.get(f"/user/profile/{handle}")
    try:
        profile = response.json() if response.status_code == 200 else None
    except ValueError:
        return None
    return profile if isinstance(profile, dict) else None


def _look(
    api: discover._Api, fetcher: crawl.PoliteFetcher, artist: dict[str, Any], code: str, local: store.Snapshot
) -> str:
    """Where the artist's code is: "deviantart" or "own_site". Or why it was not found: "no_code_found" when every
    place it may be in was read, "not_checked" when one could not be, or there is none we may read."""
    handle = artist["da_username"]
    if not isinstance(handle, str) or not store.HANDLE.fullmatch(handle):
        return "not_checked"
    profile = _profile(api, handle)
    if profile is None:
        return "not_checked"
    user = profile.get("user")
    user_id, name = (user.get("userid"), user.get("username")) if isinstance(user, dict) else (None, None)
    # The artist's own profile: the account the library keeps for the handle, under whatever handle it answers now.
    # Not a profile the handle has passed to another account, and not one answer given for every lookup, which the
    # API did once. Where the library keeps no account, the handle's own profile may only vouch for the site.
    kept = library.account_hash(store.artist_key(handle), local)
    if kept:
        if kept not in (library.account(user_id), library.account(user_id, "da-user")):
            return "not_checked"
        if any(_holds(profile.get(field), code) for field in PROFILE_FIELDS):
            return "deviantart"
    elif not (isinstance(name, str) and name.lower() == handle.lower()):
        return "not_checked"
    named = {store.registrable_domain(host) for host in store.hosts_in(profile.get("website"))}
    if not artist["site_url"] or artist["site_domain"] not in named:
        return "no_code_found" if kept else "not_checked"
    found = fetcher.read_proof(artist["site_url"], PROOF_NAME, lambda said: _plain(said).strip() == code)
    if isinstance(found, str):  # robots.txt shuts us out, no answer, a redirect off the site
        return "not_checked"
    return "own_site" if found else "no_code_found"


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
    """One pass over the requests that are due, counted into `counts` (COUNTS) as it goes, so that a run that stops
    still says what it did. discover.Stop when another run holds the API, or the API refuses for too long;
    RuntimeError when the database fails. Either ends the run there: what was removed stays removed, and the
    request being looked at stays due."""
    now = datetime.fromtimestamp(discover.now(), UTC)
    due = _rows(
        "removal_requests",
        select="artist_slug,code_at",
        code_at="not.is.null",  # a request no code was given out for is the owner's alone
        due_at=f"lte.{now.isoformat()}",
        order="due_at.asc,code_at.asc",  # the one due longest first; filing again moves nothing
        limit=str(MAX_REQUESTS),
    )
    counts["requests_due"] = len(due)
    pending = []
    for request in due:
        found = _rows("artists", select=ARTIST_COLUMNS, slug=f"eq.{request['artist_slug']}")
        if found:
            pending.append((request, found[0]))
        else:
            counts["requests_for_no_artist"] += 1
            _move_on(request, now, dry_run)
    if not pending:
        return
    fetcher = fetcher or crawl.PoliteFetcher()
    # The API client the collect stage uses: the project's user agent, no redirect followed, no URL logged.
    with (
        discover._http_logs_off(),
        httpx.Client(
            headers={"User-Agent": config.USER_AGENT}, transport=transport, timeout=30, follow_redirects=False
        ) as http,
        store.snapshot(local) as kept,
    ):
        api = discover._Api(http, client_id, client_secret)
        for request, artist in pending:
            code = db.rpc("start_removal_proof", {"p_slug": artist["slug"]}, "secret")
            where = _look(api, fetcher, artist, code, kept) if CODE.fullmatch(str(code)) else "not_checked"
            if where not in ("deviantart", "own_site"):
                counts[f"left_for_owner_{where}"] += 1
            else:
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
            _move_on(request, now, dry_run)


def _where(exc: Exception) -> str:
    """An exception as its type and the line of this file it came through. Never its message: a database error
    can quote the row it refused."""
    lines = [frame.lineno for frame in traceback.extract_tb(exc.__traceback__) if frame.filename == __file__]
    return f"{type(exc).__name__} at removals.py line {lines[-1] if lines else '?'}"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    parser.add_argument("--dry-run", action="store_true", help="make the same checks, and change nothing")
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
