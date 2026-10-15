"""Removals: look for each due request's proof code where only the artist can put it, and remove whoever posted it.

    uv run python -m pipeline.removals --dry-run     what a run would remove, and nothing changed anywhere
    uv run python -m pipeline.removals
    uv run python -m pipeline.removals --pending     the open requests, for the owner to answer
    uv run python -m pipeline.removals --grant SLUG [SLUG ...]     the owner's yes: removed for good
    uv run python -m pipeline.removals --reject SLUG [SLUG ...]    the owner's no: closed, nothing else changes

Hard rule 7 and owner decision 6. On the site an artist asks for their code (start_removal_proof, through the
site's server function), which files their request as one a code was given out for. A request without a code
(request_removal) is the owner's alone: a run only counts it. A request is a lasting record (sql/001_schema.sql:
removal_requests): it keeps the handle and site the artist was listed with when they asked, and no push, hold-back,
rename or deletion of their public row touches it. Only a proven removal or the owner's answer closes it.

The queue (removal_requests.code_at and due_at). A run reads, with the service key, at most MAX_REQUESTS of the
open requests with a code that are due, the one due longest first, and moves each one it looks at on: an hour
while its code was asked for under a day ago, a day while under two weeks ago, a week after that (_next_look).
Asking for the code again starts that over, and makes the request due at once unless it already was; it never
puts a request ahead of one due before it. The page says so, and tells an artist who puts the code up later than
two weeks to ask for it again.

It looks for the request's code in two places, and nowhere else:
  1. The DeviantArt profile of the handle the request was made for, through the official API: its bio (the About
     text the site points to) and its tagline, which only the account holder writes. Only when the answer is the
     account the local library keeps for the artist (under either spelling of its hash), renamed or not: when the
     handle answers for somebody else, or not at all, the account's other handles kept here are asked, so a
     rename leaves no request behind. For an artist the library keeps no account for, this place is not read:
     whoever holds the handle now could be somebody else.
  2. Their own site on the request, while the profile as just read (the artist's account, or for an artist with no
     account kept, the handle's own) still names it: a <meta name="artist-search-removal"> in the front page's
     head, or the file /.well-known/artist-search-removal.txt holding the code and nothing else, read through
     the polite fetcher (crawl.PoliteFetcher.read_proof). A comment, a guestbook entry or anything else a
     visitor can write is in neither. A site their profile no longer names, such as a domain they let lapse and
     moved on from, or the site of a deleted account, is not read.
Case, the space around the code and HTML entities do not matter, nor do characters that show as nothing. The code
is the slug's own, so one pasted onto another artist's page proves nothing for them. Codes do not expire. One
made before the owner replaced the secret is not the slug's code any more, and proves nothing.

A code found where it must be goes to confirm_removal, which checks it again and removes the artist at once and
for good, listed at that moment or not, and closes the request. The two key hashes the request was made under then
go on the local store's list (store.import_removed), so no stage here collects the artist again. The next push or
collect purges what is kept about them, under every handle of their account, as it does for every removal on the
database's list. The owner's --grant does the same for an open request without proof. A request whose artist is
removed another way (under another handle of their account, say) is closed by the database as that happens.

Anything less leaves the request open, for the owner and for its next look.

Every request to DeviantArt waits its turn in discover.Pace, and holds the API's lock for that request alone: a
collect run can start between two of them, and this run then stops at its next one. Every request to a site goes
through the fetcher. A run with nothing due asks neither.

`--dry-run` makes the same reads and requests, and changes nothing: no call to confirm_removal, nothing written to
the local store, no request moved on. (The API's pace file is kept as for any request to it: discover.Pace.) Stdout
is one JSON object of counts, with how many requests are open with a code and without one, proven, and closed
otherwise; nothing names an artist. Only --pending prints a handle. The service key is read by pipeline/db.py and
never printed.
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
TALLY = (  # every request in the database, by state, as the run began (removal_tally in sql/002_functions.sql)
    "requests_open_with_code",
    "requests_open_without_code",  # the owner's to answer: --pending lists them
    "requests_proven",
    "requests_closed",  # granted or rejected by the owner, or their artist removed another way
)
COUNTS = (
    *TALLY,
    "requests_due",  # read this run: MAX_REQUESTS at most
    "proven_on_deviantart",
    "proven_on_own_site",
    "artists_removed",  # with --dry-run: would be
    "proofs_refused_by_database",  # the code changed between the read and the removal
    "left_for_owner_no_code_found",
    "left_for_owner_not_checked",  # a place could not be read, or there is none we may read: looked at again later
)
REQUEST_COLUMNS = "artist_slug,code_at,da_username,site_url,site_domain,da_key,site_key"


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
    """When a request is looked at again: an hour on while its code was asked for under a day ago, a day on while
    under two weeks ago, then a week on. So a code put up in its first two weeks is found within a day, and a stale
    request costs little. The page says this, and that asking for the code again starts it over."""
    age = now - datetime.fromisoformat(code_at)
    return now + (HOUR if age < DAY else DAY if age < 2 * WEEK else WEEK)


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
    # The artist's own profile: the account the library keeps for the handle, under whatever handle it answers now.
    # Not a profile the handle has passed to another account, and not one answer given for every lookup, which the
    # API did once. Where the library keeps no account, the handle's own profile may only vouch for the site.
    kept = library.account_hash(store.artist_key(handle), local)
    others = library.same_account(store.artist_key(handle), local) if kept else []
    for asked in [handle, *(key.removeprefix("da:") for key in others)]:  # a renamed account may answer only anew
        profile = _profile(api, asked)
        user = profile.get("user") if profile else None
        user_id, name = (user.get("userid"), user.get("username")) if isinstance(user, dict) else (None, None)
        if not kept or kept in (library.account(user_id), library.account(user_id, "da-user")):
            break
    if profile is None:
        return "not_checked"
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
    tally = db.rpc("removal_tally", {}, "secret")
    counts.update({name: int(tally[name]) for name in TALLY})
    due = _rows(
        "removal_requests",
        select=REQUEST_COLUMNS,
        outcome="is.null",
        code_at="not.is.null",  # a request no code was given out for is the owner's alone
        due_at=f"lte.{now.isoformat()}",
        order="due_at.asc,code_at.asc",  # the one due longest first
        limit=str(MAX_REQUESTS),
    )
    counts["requests_due"] = len(due)
    if not due:
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
        for request in due:
            slug = request["artist_slug"]
            code = db.rpc("removal_code", {"p_slug": slug}, "secret")
            where = _look(api, fetcher, request, code, kept) if CODE.fullmatch(str(code)) else "not_checked"
            if where not in ("deviantart", "own_site"):
                counts[f"left_for_owner_{where}"] += 1
            else:
                counts[f"proven_on_{where}"] += 1
                if dry_run:
                    counts["artists_removed"] += 1
                # The database first: it is the judge of the code. Here only once it has removed them, and only
                # what it put on its own list, which every later push takes as the database's own removal.
                elif db.rpc("confirm_removal", {"p_slug": slug, "p_code": code}, "secret") is True:
                    store.import_removed([key for key in (request["da_key"], request["site_key"]) if key], local)
                    counts["artists_removed"] += 1
                else:
                    counts["proofs_refused_by_database"] += 1
            _move_on(request, now, dry_run)


def pending() -> list[str]:
    """One line for each open request, the oldest first: the slug and the public handle it was made for, when it was
    first and last asked for and how often, and whether a code was given out and a note left. For the owner, who
    reads a note in the database's removal_requests table and answers with --grant or --reject."""
    rows = _rows(
        "removal_requests",
        select="artist_slug,da_username,first_at,last_at,requests,code_at,note",
        outcome="is.null",
        order="first_at.asc",
    )
    return [
        f"{row['artist_slug']}  handle {row['da_username'] or '?'}  first asked {row['first_at'][:10]}, "
        f"last {row['last_at'][:10]}, {row['requests']} times  code given out: {'yes' if row['code_at'] else 'no'}  "
        f"note: {'yes' if row['note'] else 'no'}"
        for row in rows
    ]


def answer(grant: list[str], reject: list[str], local: pathlib.Path | str | None = None) -> dict[str, int]:
    """The owner's answer to open requests, by slug. A granted one is removed for good as a proven one is, here at once
    too; a rejected one is closed and nothing else changes. A slug with no open request is left alone."""
    counts = {"granted": 0, "rejected": 0}
    for slug in grant:
        keys = _rows("removal_requests", select="da_key,site_key", artist_slug=f"eq.{slug}", outcome="is.null")
        if keys and db.rpc("purge_removed", {"p_slugs": [slug]}, "secret") == 1:
            store.import_removed([key for key in keys[0].values() if key], local)
            counts["granted"] += 1
    for slug in reject:
        counts["rejected"] += db.rpc("reject_removal", {"p_slug": slug}, "secret") is True
    return counts


def _where(exc: Exception) -> str:
    """An exception as its type and the line of this file it came through. Never its message: a database error
    can quote the row it refused."""
    lines = [frame.lineno for frame in traceback.extract_tb(exc.__traceback__) if frame.filename == __file__]
    return f"{type(exc).__name__} at removals.py line {lines[-1] if lines else '?'}"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    parser.add_argument("--dry-run", action="store_true", help="make the same checks, and change nothing")
    parser.add_argument("--pending", action="store_true", help="list the open requests, and change nothing")
    parser.add_argument("--grant", nargs="+", default=[], metavar="SLUG", help="remove them for good")
    parser.add_argument("--reject", nargs="+", default=[], metavar="SLUG", help="close their requests, and no more")
    args = parser.parse_args(argv)
    try:
        if args.pending:
            print("\n".join(pending()) or "no open requests")
            return 0
        if args.grant or args.reject:
            print(json.dumps(answer(args.grant, args.reject)))
            return 0
    except Exception as exc:  # deliberately broad: nothing gets out of here as a traceback
        print(f"stopped: {_where(exc)}", file=sys.stderr)
        return 1
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
