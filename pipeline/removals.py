"""Removals: look for each due request's proof code where only the artist can put it, and remove whoever posted it.

    uv run python -m pipeline.removals --dry-run     what a run would remove, and nothing changed anywhere
    uv run python -m pipeline.removals
    uv run python -m pipeline.removals --pending     the open requests, for the owner to answer
    uv run python -m pipeline.removals --grant ID [ID ...]     the owner's yes: removed for good
    uv run python -m pipeline.removals --reject ID [ID ...]    the owner's no: closed, nothing else changes

Hard rule 7 and owner decision 6. On the site an artist asks for their code (start_removal_proof, through the
site's server function), which files their request as one a code was given out for. A request without a code
(request_removal) is the owner's alone: a run only counts it. A request is a lasting record (sql/001_schema.sql:
removal_requests), bound when it is filed to the artist the claim-or-remove page showed, listed then or not: their
handle, their site and the account the library keeps for them (artists.da_account, which the push stage sends).
Nothing afterwards changes that, and no push, hold-back, rename or deletion of their public row touches it: a slug
that passes to another handle, or a site to another artist, takes nobody else into the request. Only a proven
removal or the owner's answer closes it, and either removes and bars what it was bound to and nothing else.

The queue (removal_requests.code_at, due_at and networks). A run reads, with the service key, at most MAX_REQUESTS
of the open requests with a code that are due, in turns between the networks the codes were asked from, the one
due longest first in each turn (due_removals), and moves each one it looks at on: an hour while its code was asked
for under a day ago, six hours while under two weeks ago, a week after that (_next_look). Asking for the code again
starts that over, and makes the request due at once unless it already was, even while a run is looking at it
(_move_on); it never puts a request ahead of one due before it in its network's turn. So one network that asks for
hundreds of codes holds back only its own. A
request is in the turns of every network that asked for its code (the first MAX_REQUESTS of them) and is taken in
the first it comes to, so a stranger who asks for an artist's code, before or after the artist, takes nothing from
them: asking only ever brings a request forward.

What the page promises: a code put up in the first two weeks is found usually within a few hours, and at most
within a day; one put up later, after the code is asked for again. That holds while a run is made every EVERY:
the owner's schedule, or a collect or discovery run's own (discover.look_between). A code is then looked for
within six hours and EVERY of going up, and a run takes the due request of every network in its turn, MAX_REQUESTS
networks a run. So it holds under any number of codes asked for from fewer than MAX_REQUESTS networks at once. From
more, each run takes the requests due longest of MAX_REQUESTS of them, and a look slips by the backlog: about an
hour for every 120 codes due before it, so a day for 3,000 (150 networks' 20 each: sql/003_limits.sql). That is
the accepted limit. A flood kept up from that many networks asks for more looks than the runs make (a code is
looked at 24 times on its first day, and the runs make 2,880 looks a day), so the slip grows while it lasts. An
artist whose own network is one of those flooding (an office behind one address) waits in its turn, and so does
one whose site or DeviantArt does not answer, and one whose code MAX_REQUESTS of the flooding networks asked for
before they did.

It looks for the request's code in two places, and nowhere else:
  1. The DeviantArt profile of the handle the request was made for, through the official API: its bio (the About
     text the site points to) and its tagline, which only the account holder writes. Only when the answer is the
     account the request is bound to (under either spelling of its hash), renamed or not: when the handle answers
     for somebody else, or not at all, the account's other handles kept here are asked, so a rename leaves no
     request behind. For a request bound to no account (the library kept none for the artist), this place is not
     read: whoever holds the handle now could be somebody else. The site's page says so to such an artist.
  2. Their own site on the request, while the profile as just read (the account's, or for a request bound to no
     account, the handle's own) still names it: a <meta name="artist-search-removal"> in the front page's
     head, or the file /.well-known/artist-search-removal.txt holding the code and nothing else, read through
     the polite fetcher (crawl.PoliteFetcher.read_proof). A comment, a guestbook entry or anything else a
     visitor can write is in neither. A site their profile no longer names, such as a domain they let lapse and
     moved on from, or the site of a deleted account, is not read.
Case, the space around the code and HTML entities do not matter, nor do characters that show as nothing. The code
is the slug's own, so one pasted onto another artist's page proves nothing for them; and it is read only in the
places of the artist the request is bound to, so a slug that has passed to another handle proves nothing for that
handle either. Codes do not expire. One made before the owner replaced the secret, or before codes were made
under words of their own (removal_code: none was given out), is not the slug's code any more, and proves nothing.

A code found where it must be goes to confirm_removal, which checks it again and removes the artist at once and
for good, listed at that moment or not, and closes the request. What it bars is what the proof covered: the handle
and the account the request is bound to, and its site only while the profile just read still names it (and no
other handle is listed with it: grant_request). Those key hashes, as the database answers them, go on the local
store's list (store.import_removed), so no stage here collects the artist again. The next push or collect purges
what is kept about them, under every handle of their account, as it does for every removal on the database's
list. The owner's --grant does the same for an open request without proof (its site included, on the same
condition). A request whose artist is removed another way (under another handle of their account, say) is closed
by the database as that happens.

Anything less leaves the request open, for the owner and for its next look.

Every request to DeviantArt waits its turn in discover.Pace, and holds the API's lock for that request alone: a
collect run can start between two of them, and this run then stops at its next one. A collect run holds the API
for hours and a discovery run for an hour or more, so each makes these runs itself, every EVERY between two of its
own requests, through its own client, at its pace and under its lock (discover.look_between): the look the page
promises goes on while they run. Every request to a site goes through the fetcher, one a run, and is over within
crawl.PROOF_SECONDS_PER_RESPONSE whatever the site does: one that runs out of time closes the site for the run,
and its request is moved on to its next look like any other. So a run takes at most about MAX_REQUESTS such
deadlines, and a slow site holds no run, lock or queue. A run with nothing due asks neither.

`--dry-run` makes the same reads and requests, and changes nothing: no call to confirm_removal, nothing written to
the local store, no request moved on. (The API's pace file is kept as for any request to it: discover.Pace.) Stdout
is one JSON object of counts, with how many requests are open with a code and without one, proven, and closed
otherwise; nothing names an artist. Only --pending prints a handle, and what was written in the notes. The service
key is read by pipeline/db.py and never printed.
"""

import argparse
import contextlib
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
EVERY = timedelta(minutes=15)  # how often a run is due: the owner's schedule (README), and a long run's own
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
    # The code changed between the read and the removal, or the request was closed meanwhile: its account was
    # barred earlier in the run, by the proof of a request for another of its handles.
    "proofs_refused_by_database",
    "left_for_owner_no_code_found",
    "left_for_owner_not_checked",  # a place could not be read, or there is none we may read: looked at again later
)


def _rows(table: str, **params: str) -> list[dict[str, Any]]:
    """Rows of a table of the public database, read through the Data API with the service key: every row, a page at
    a time, since the Data API answers at most its max-rows (1,000 by default) and says nothing of the rest.
    `order` must make the order total, or a page can repeat a row and miss another."""
    rows: list[dict[str, Any]] = []
    while True:
        page = db._checked(db.data_api("GET", table, "secret", params=params | {"offset": str(len(rows))}))
        rows += page
        if not page:
            return rows


def _plain(text: str) -> str:
    """Text as a code is looked for in it: entities undone, what shows as nothing taken out, in lower case."""
    return _UNSEEN.sub("", html.unescape(text)).lower()


def _holds(text: object, code: str) -> bool:
    """True when the profile's text holds the code: as written, or with its markup taken out (a page may split a
    long word with <wbr>). As written also finds it in an attribute, a link's say."""
    return isinstance(text, str) and any(code in _plain(version) for version in (text, _TAG.sub("", text)))


def _next_look(code_at: str, now: datetime) -> datetime:
    """When a request is looked at again: an hour on while its code was asked for under a day ago, six hours on
    while under two weeks ago, then a week on. So a code put up in its first two weeks is found within a few hours,
    well inside the day the page promises (see the module's docstring), and a stale request costs little. The page
    says that, and that asking for the code again starts it over."""
    age = now - datetime.fromisoformat(code_at)
    return now + (HOUR if age < DAY else 6 * HOUR if age < 2 * WEEK else WEEK)


def _move_on(request: dict[str, Any], now: datetime, dry_run: bool) -> None:
    """Set when the request is looked at next. Not in a dry run, which changes nothing. Only while its code_at is
    still the one read: the code asked for again during the look (the page tells an artist to, after two weeks)
    made it due at once, and that stands, where "a week on" from the old code_at would have buried it."""
    if not dry_run:
        due = _next_look(request["code_at"], now).isoformat()
        # Not one a proof just closed, nor one asked for again since it was read.
        where = {"id": f"eq.{request['id']}", "outcome": "is.null", "code_at": f"eq.{request['code_at']}"}
        db._checked(db.data_api("PATCH", "removal_requests", "secret", params=where, json={"due_at": due}))


def _profile(api: discover._Api, handle: str) -> dict[str, Any] | None:
    """The handle's profile as the API answers it, or None. The API's lock is held for this request alone, unless
    this run is a collect or discovery run's, which holds it already (discover.Pace takes it again for nothing)."""
    with api.pace:
        response = api.get(f"/user/profile/{handle}")
    try:
        profile = response.json() if response.status_code == 200 else None
    except ValueError:
        return None
    return profile if isinstance(profile, dict) else None


def _look(
    api: discover._Api, fetcher: crawl.PoliteFetcher, request: dict[str, Any], code: str, local: store.Snapshot
) -> tuple[str, bool]:
    """Where the request's code is: "deviantart" or "own_site", and whether the profile just read still names the
    request's site, so that the proof covers it. Or why it was not found: "no_code_found" when every place it may be
    in was read, "not_checked" when one could not be, or there is none we may read."""
    handle, kept = request["da_username"], request["da_account"]
    if not isinstance(handle, str) or not store.HANDLE.fullmatch(handle):
        return "not_checked", False
    # The profile of the account the request is bound to, under whatever handle it answers now. Not a profile the
    # handle has passed to another account, and not one answer given for every lookup, which the API did once. A
    # request bound to no account has only the handle's own profile, which may only vouch for the site.
    others = library.same_account(store.artist_key(handle), local) if kept else []
    for asked in [handle, *(key.removeprefix("da:") for key in others)]:  # a renamed account may answer only anew
        profile = _profile(api, asked)
        user = profile.get("user") if profile else None
        user_id, name = (user.get("userid"), user.get("username")) if isinstance(user, dict) else (None, None)
        if not kept or kept in (library.account(user_id), library.account(user_id, "da-user")):
            break
    if profile is None:
        return "not_checked", False
    named = {store.registrable_domain(host) for host in store.hosts_in(profile.get("website"))}
    covered = bool(request["site_url"]) and request["site_domain"] in named
    if kept:
        if kept not in (library.account(user_id), library.account(user_id, "da-user")):
            return "not_checked", False
        if any(_holds(profile.get(field), code) for field in PROFILE_FIELDS):
            return "deviantart", covered
    elif not (isinstance(name, str) and name.lower() == handle.lower()):
        return "not_checked", False
    if not covered:
        return "no_code_found" if kept else "not_checked", False
    found = fetcher.read_proof(request["site_url"], PROOF_NAME, lambda said: _plain(said).strip() == code)
    if isinstance(found, str):  # robots.txt shuts us out, no answer in time, a redirect off the site
        return "not_checked", False
    return "own_site" if found else "no_code_found", True


def run(
    counts: dict[str, int],
    dry_run: bool,
    client_id: str,
    client_secret: str,
    *,
    local: pathlib.Path | str | None = None,
    transport: httpx.BaseTransport | None = None,
    fetcher: crawl.PoliteFetcher | None = None,
    api: discover._Api | None = None,
) -> None:
    """One pass over the requests that are due, counted into `counts` (COUNTS) as it goes, so that a run that stops
    still says what it did. discover.Stop when another run holds the API, or the API refuses for too long;
    RuntimeError when the database fails. Either ends the run there: what was removed stays removed, and the
    request being looked at stays due. Given `api`, a collect or discovery run's, it asks through that (its pace,
    which that run holds) and not a client of its own."""
    now = datetime.fromtimestamp(discover.now(), UTC)
    tally = db.rpc("removal_tally", {}, "secret")
    counts.update({name: int(tally[name]) for name in TALLY})
    # Only requests a code was given out for: one without is the owner's alone.
    due = db.rpc("due_removals", {"p_now": now.isoformat(), "p_limit": MAX_REQUESTS}, "secret") or []
    counts["requests_due"] = len(due)
    if not due:
        return
    fetcher = fetcher or crawl.PoliteFetcher()
    with contextlib.ExitStack() as held:
        if api is None:  # the API client the collect stage uses: the user agent, no redirect followed, no URL logged
            held.enter_context(discover._http_logs_off())
            http = held.enter_context(
                httpx.Client(
                    headers={"User-Agent": config.USER_AGENT}, transport=transport, timeout=30, follow_redirects=False
                )
            )
            api = discover._Api(http, client_id, client_secret)
        kept = held.enter_context(store.snapshot(local))
        for request in due:
            code = db.rpc("removal_code", {"p_slug": request["artist_slug"]}, "secret")
            found = _look(api, fetcher, request, code, kept) if CODE.fullmatch(str(code)) else ("not_checked", False)
            where, site = found
            if where not in ("deviantart", "own_site"):
                counts[f"left_for_owner_{where}"] += 1
            else:
                counts[f"proven_on_{where}"] += 1
                args = {"p_id": request["id"], "p_code": code, "p_site": site}
                if dry_run:
                    counts["artists_removed"] += 1
                # The database first: it is the judge of the code. Here only once it has removed them, and only
                # what it put on its own list, which every later push takes as the database's own removal.
                elif barred := db.rpc("confirm_removal", args, "secret"):
                    store.import_removed(barred, local)
                    counts["artists_removed"] += 1
                else:
                    counts["proofs_refused_by_database"] += 1
            _move_on(request, now, dry_run)


def pending() -> list[str]:
    """One entry for each open request, the oldest first, every one of them: a line with the slug, the request's
    id, the public handle it was made for, when it was first and last asked for and how often, and whether a code
    was given out; then a line for each note kept with it, with when it came (the latest from each network, of the
    ten heard from last: request_removal). For the owner, who reads them and answers by id with --grant or --reject.
    A note is a visitor's words, printed as a quoted string with anything that does not print escaped, so it cannot
    act on the terminal. A slug can have more than one request: the artist listed under it changed, or their site
    did."""
    rows = _rows(
        "removal_requests",
        select="id,artist_slug,da_username,first_at,last_at,requests,code_at,notes",
        outcome="is.null",
        order="first_at.asc,id.asc",
    )
    return [
        f"{row['artist_slug']}  id {row['id']}  handle {row['da_username'] or '?'}  "
        f"first asked {row['first_at'][:10]}, "
        f"last {row['last_at'][:10]}, {row['requests']} times  code given out: {'yes' if row['code_at'] else 'no'}  "
        f"notes: {len(row['notes'])}" + "".join(f"\n    {note['at'][:16]}  {note['note']!r}" for note in row["notes"])
        for row in rows
    ]


def answer(grant: list[int], reject: list[int], local: pathlib.Path | str | None = None) -> dict[str, int]:
    """The owner's answer to open requests, by id. A granted one is removed for good as a proven one is, its site
    with it (grant_request), here at once too; a rejected one is closed and nothing else changes. An id with no
    open request is left alone."""
    counts = {"granted": 0, "rejected": 0}
    for request in grant:
        if barred := db.rpc("grant_request", {"p_id": request, "p_outcome": "granted", "p_site": True}, "secret"):
            store.import_removed(barred, local)
            counts["granted"] += 1
    for request in reject:
        counts["rejected"] += db.rpc("reject_removal", {"p_id": request}, "secret") is True
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
    parser.add_argument("--grant", nargs="+", type=int, default=[], metavar="ID", help="remove them for good")
    parser.add_argument("--reject", nargs="+", type=int, default=[], metavar="ID", help="close them, and no more")
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
