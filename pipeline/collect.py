"""Collect each discovered artist's public profile and about ten recent works through DeviantArt's official API.

    uv run python -m pipeline.collect --max-artists 200

For every artist the discovery stage stored, and nobody on the removal list: four public professional fields of
their profile, and up to MAX_WORKS of their most recent works that are images, explicitly not mature and not
tagged as AI-made. Each work is kept with its own tags and one preview-size image in the local cache, for the
analysis stage (owner decision 7 in PROGRESS.md). pipeline/library.py does the keeping. Output is counts only.

A run starts and ends by deleting what must not be there: everything kept about an artist who has been removed
since, and every cached file no row accounts for. Running it again over every artist also brings what an
earlier version stored up to the rules of this one: each artist's profile and works are replaced whole.

No single artist ends a run. Whatever goes wrong with one is counted, the artist is noted as passed over so that
the next run takes them last, and the run goes on. It stops only for the rate limit, for our own token, and
when MAX_FAILURES artists in a row have failed, which means the API is down.

Nothing is purged because the API failed. What is kept under a handle goes only when the artist is removed, when
their works are mostly AI-made, or when two runs that were otherwise healthy have shown the account gone (the API
said so in one of its documented codes) or shown none of the works kept for it. One work goes when it has left the
gallery or no longer qualifies while others of the artist's still do.

Endpoints (deviantart.readme.io, read 2026-10-12). Each is documented under the security scheme
"oauth2AuthorizationCodeOrClientCredentials" with scope "browse", so a client_credentials token is accepted:
    GET /api/v1/oauth2/user/profile/{username}   no parameters: "expand" would add age, sex and location
    GET /api/v1/oauth2/gallery/all               username, limit (1 to 24)
    GET /api/v1/oauth2/deviation/metadata        deviationids
An image is fetched from the preview address its gallery entry carries, without the token: it is a public file.
"""

import argparse
import pathlib
import re
import sqlite3
import sys
import time
import traceback
import unicodedata
from typing import Any

import httpx

from pipeline import config, discover, library, store

MAX_WORKS = 10  # works kept per artist
# ponytail: one gallery page, the documented maximum of 24. An artist with fewer than MAX_WORKS qualifying works
# among their 24 newest gets fewer. Follow next_offset if too many artists come up short.
GALLERY_LIMIT = 24
IMAGE_INTERVAL = 0.5  # seconds waited before every image download: two a second at most
SERVER_RETRIES = 3  # times a 5XX answer, or an image host's 429, is asked for again. The API docs suggest three.
_WAITS = (*(2.0**attempt for attempt in range(1, SERVER_RETRIES + 1)), None)  # 2, 4 and 8 seconds, then no more
MAX_FAILURES = 5  # artists failing one after another before the run gives up: by then it is the API, not them
MAX_REFUSALS = 3  # works in a row the image host refuses for the rate limit, retries and all, before the run stops
DEAD_AFTER = 2  # runs that must show an account, or all its works, gone before what is kept under its handle is purged
NOT_IMAGE_KEYS = frozenset(("videos", "flash", "text_content", "excerpt"))  # what a film or a text carries

# The tags that say a work is AI-made and can be read no other way. A tag counts when it is one of these, whole,
# once it is in lower case with everything but letters and digits taken out: "AI_Generated" is "aigenerated".
# ponytail: exact matches and a short list, on purpose (the lead's decision, round 4). Four rounds of prefixes,
# suffixes and "contains" erred both ways at once, and the worst way was taking a human artist's protest
# ("no_to_ai", "ai_free_art") for a claim and purging them. The analysis stage reads every work's tags and title
# with a language model and makes the real decision: a tag that is not on this list is left for it.
AI_TAGS = frozenset(
    "ai aiart aiartwork aigenerated aigeneratedart aiimage aiadopt aiadoptable aiadoptables generativeai "
    "midjourney stablediffusion dalle dalle2 dalle3 dreamup novelai nijijourney synthography synthographyart".split()
)

PROCESSED = "artists processed"
PROFILES = "profiles stored"
WORKS = "works stored"
DOWNLOADED = "images downloaded"
CACHED = "images already cached"
PURGED = "purged artists (removed)"
GONE = "purged artists (account gone, or all their works, on two runs)"
ORPHANS = "deleted cached files (no row for them)"
UNDELETED = "deletions that failed"
BAD_KEY = "rows with an unreadable artist key"
NOT_STORED = "skipped artists (the database refused the profile)"
ROW_REFUSED = "skipped works (the database refused the row)"
FAILED = "skipped artists (unexpected error)"
API_FAILED = "skipped artists (the API failed for them)"
REMOVED = "skipped artists (removed)"
RENAMED = "skipped artists (a removed account under a new handle)"
UNAVAILABLE = "skipped artists (account suspended, inactive or gone)"
EMPTIED = "skipped artists (none of the works kept for them came back)"
MOSTLY_AI = "skipped artists (most recent works tagged AI-made)"
MALFORMED = "skipped works (malformed entry)"
NOT_OWN = "skipped works (another author's)"
NOT_IMAGE = "skipped works (not a public image)"
MATURE = "skipped works (mature or unrated)"
NO_METADATA = "skipped works (no tag data)"
AI_TAGGED = "skipped works (tagged AI-made)"
TITLE = "skipped works (contact details in the title)"
IMAGE_FAILED = "skipped works (image download failed)"
IMAGE_LIMITED = "skipped works (image host rate limit)"
IMAGE_WRONG_TYPE = "skipped works (image not an accepted type)"
IMAGE_TOO_LARGE = "skipped works (image over the size cap)"
_COUNTS = (
    *(PROCESSED, PROFILES, WORKS, DOWNLOADED, CACHED, PURGED, GONE, ORPHANS, UNDELETED, BAD_KEY),
    *(NOT_STORED, ROW_REFUSED, FAILED, API_FAILED, REMOVED, RENAMED, UNAVAILABLE, EMPTIED, MOSTLY_AI),
    *(MALFORMED, NOT_OWN, NOT_IMAGE, MATURE, NO_METADATA, AI_TAGGED, TITLE),
    *(IMAGE_FAILED, IMAGE_LIMITED, IMAGE_WRONG_TYPE, IMAGE_TOO_LARGE),
)


class _ApiFailed(Exception):
    """The API would not answer for this artist. The message is safe to print: a name of ours and a status."""


def _is_ai(tags: list[str]) -> bool:
    """True when one of an artist's own tags is, whole, one of AI_TAGS."""
    squashed = ("".join(re.findall(r"[a-z0-9]+", unicodedata.normalize("NFKC", tag).lower())) for tag in tags)
    return not AI_TAGS.isdisjoint(squashed)


def _src(part: object) -> object:
    return part.get("src") if isinstance(part, dict) else None


def _why_not(entry: dict[str, Any], username: str) -> str | None:
    """Why a gallery entry is not kept. None for the artist's own public image that is explicitly not mature."""
    author = entry.get("author")
    name = author.get("username") if isinstance(author, dict) else None
    if not isinstance(name, str) or name.lower() != username.lower():
        return NOT_OWN
    if (
        entry.get("is_deleted") is not False
        or entry.get("tier_access") in ("locked", "locked-subscribed")
        or not NOT_IMAGE_KEYS.isdisjoint(entry)
        or not isinstance(_src(entry.get("content")), str)
        or not isinstance(_src(entry.get("preview")), str)
    ):
        return NOT_IMAGE
    if entry.get("is_mature") is not False:  # only an explicit "not mature" counts
        return MATURE
    return None


def _get(api: discover._Api, path: str, **params: str | int) -> httpx.Response:
    """api.get(), with a 5XX answer asked for again up to SERVER_RETRIES times, each after twice as long a wait.

    The docs say a 500 is worth retrying up to three times. What comes back after that is the caller's to judge.
    """
    for wait in _WAITS:
        response = api.get(path, **params)
        if response.status_code < 500 or wait is None:
            break
        time.sleep(wait)  # on top of the second discover._Api leaves before every request
    return response


def _body(response: httpx.Response, what: str) -> dict[str, Any]:
    """The JSON object of a 200.

    discover.Stop for a 401, which is our token refused even after it was renewed: nobody's turn can succeed.
    _ApiFailed for any other answer: a 5XX that outlasted its retries, a redirect, a 4XX. It says nothing about
    the artist, so it ends their turn and nothing more; several in a row end the run (see _run).
    """
    status = response.status_code
    if status == 401:
        raise discover.Stop(f"{what} failed with HTTP 401")
    if status != 200:  # 429 never gets here: discover._Api waits it out, or stops the run
        raise _ApiFailed(f"{what} answered HTTP {status}")
    body = response.json()
    return body if isinstance(body, dict) else {}


def _gone(response: httpx.Response) -> bool:
    """True when a 4XX carries one of the profile endpoint's documented codes: 0 suspended, 1 inactive, 2 not found.

    That is the API saying the account is no longer public. An outage, a block or a refusal says no such thing,
    which is why no other answer ever counts towards purging anybody.
    """
    try:
        body = response.json() if 400 <= response.status_code < 500 else None
    except ValueError:
        return False
    return isinstance(body, dict) and type(body.get("error_code")) is int and body["error_code"] in (0, 1, 2)


def _fetch_once(http: httpx.Client, url: str) -> tuple[str, bytes] | str:
    """One request for an image: (content type, bytes), or the count it falls under when there is none to keep.

    Sent without the API token. The type is checked before the body is read, and the body is read only as far
    as the size cap. A redirect is not followed, so it counts as a failure.
    """
    try:
        with http.stream("GET", url) as response:
            if response.status_code == 429:
                return IMAGE_LIMITED
            if response.status_code != 200:
                return IMAGE_FAILED
            content_type = response.headers.get("content-type", "").partition(";")[0].strip().lower()
            if content_type not in library.IMAGE_TYPES:
                return IMAGE_WRONG_TYPE
            data = bytearray()
            for chunk in response.iter_bytes():
                data += chunk
                if len(data) > library.MAX_IMAGE_BYTES:
                    return IMAGE_TOO_LARGE
    except httpx.HTTPError:
        return IMAGE_FAILED
    return content_type, bytes(data)


def _fetch_image(http: httpx.Client, url: str) -> tuple[str, bytes] | str:
    """_fetch_once() after the wait that is due, with a 429 asked for again like a 5XX from the API: up to
    SERVER_RETRIES times, each after twice as long a wait. IMAGE_LIMITED when the host still refuses."""
    # An image served from the API's own host is one more request to that host, so it waits the API's interval.
    on_api_host = httpx.URL(url).host == httpx.URL(discover.API_URL).host
    time.sleep(discover.MIN_INTERVAL if on_api_host else IMAGE_INTERVAL)  # the tests replace time.sleep
    for wait in _WAITS:
        fetched = _fetch_once(http, url)
        if fetched != IMAGE_LIMITED or wait is None:
            break
        time.sleep(wait)
    return fetched


def _purge(key: str, counts: dict[str, int], db: pathlib.Path | str | None, unlist: bool = False) -> bool:
    """library.purge() that does not end the run: a file that cannot be deleted is counted, and the run goes on to
    the next artist, whose data must not stay on disk because of it. False when something could not be deleted.
    With `unlist` the handle leaves the queue as well (library.unlist)."""
    try:
        (library.unlist if unlist else library.purge)(key, db)
    except OSError:
        counts[UNDELETED] += 1
        return False
    return True


def _remove(key: str, counts: dict[str, int], db: pathlib.Path | str | None, user_id: object = None) -> bool:
    """Purge an artist who is on the removal list, and first put their account on that list as well.

    The account is the hash of the API's user id, which does not change when a handle does. Without it the
    purge would delete the one thing that says who they were, and the same account under a new handle would be
    collected again (hard rule 7: not re-added later). It is taken from the profile that was stored, and from
    `user_id` when the removal lands in the artist's own turn, which may be their first.
    """
    accounts = {account for account in (library.account_hash(key, db), library.account(user_id)) if account}
    if accounts:
        store.import_removed(accounts, db)
    return _purge(key, counts, db)


def _sweep(counts: dict[str, int], db: pathlib.Path | str | None) -> None:
    """Hard rule 7, at both ends of every run: everything kept about an artist who is now on the removal list
    goes, and so do every cached file no row accounts for and every note about a handle nobody asks about."""
    unreadable = 0
    for key in sorted({*library.artist_keys(db), *library.last_attempts(db)}):
        try:
            if store.is_removed(key, db) and _remove(key, counts, db):
                counts[PURGED] += 1
        except ValueError:  # a key nobody can read is counted, and the artists after it are purged all the same
            unreadable += 1
    counts[BAD_KEY] = max(counts[BAD_KEY], unreadable)  # the same rows at both ends of a run, counted once
    library.drop_stale_skips(db)
    deleted, failed = library.drop_orphans(db)
    counts[ORPHANS] += deleted
    counts[UNDELETED] += failed


def _collect_artist(
    api: discover._Api, http: httpx.Client, username: str, counts: dict[str, int], db: pathlib.Path | str | None
) -> bool:
    """One artist's turn. False when the API says their account is gone, or shows none of the works kept for
    them: that is for _run to settle, once it knows what else the API said in this run. True otherwise."""
    key = store.artist_key(username)
    response = _get(api, f"/user/profile/{username}")
    if _gone(response):
        library.note_skip(key, db=db)
        counts[UNAVAILABLE] += 1
        return False
    profile = _body(response, "profile lookup")
    user = profile.get("user")
    user_id = user.get("userid") if isinstance(user, dict) else None
    # Hard rule 7. An account removed under one handle is the same account under the next: the new handle is
    # barred through the store, and nothing is collected. So is an artist whose website is a removed site.
    renamed = library.account_removed(user_id, db)
    if renamed:
        store.mark_removed(username, db)
    if renamed or store.record_listing(username, store.hosts_in(profile.get("website")), db):
        _remove(key, counts, db, user_id)
        counts[RENAMED if renamed else REMOVED] += 1
        return True
    page = _body(_get(api, "/gallery/all", username=username, limit=GALLERY_LIMIT), "gallery lookup")

    candidates: dict[str, dict[str, Any]] = {}  # deviation id -> gallery entry
    for entry in page.get("results") or []:
        try:
            work_id = library.deviation_id(entry.get("deviationid") if isinstance(entry, dict) else None)
        except ValueError:
            counts[MALFORMED] += 1
            continue
        reason = _why_not(entry, username)
        if reason:
            counts[reason] += 1
        else:
            candidates[work_id] = entry

    metadata: dict[str, dict[str, Any]] = {}  # deviation id -> its metadata, which is where the tags are
    if candidates:
        asked = {f"deviationids[{n}]": id_ for n, id_ in enumerate(candidates)}
        body = _body(_get(api, "/deviation/metadata", **asked), "metadata lookup")
        for item in body.get("metadata") or []:
            if isinstance(item, dict) and isinstance(item.get("deviationid"), str):
                metadata[item["deviationid"].upper()] = item

    clean: list[tuple[str, dict[str, Any], list[str]]] = []  # works that are fit to keep
    ai_made = 0
    for work_id, entry in candidates.items():
        item = metadata.get(work_id, {})
        listed = item.get("tags")
        names = (
            [tag.get("tag_name") if isinstance(tag, dict) else None for tag in listed]
            if isinstance(listed, list)
            else []
        )
        tags = [name for name in names if isinstance(name, str)]
        # No tag list, or an item in it that is not a named tag: what the work is cannot be told, so it is not kept.
        if not isinstance(listed, list) or len(tags) != len(names):
            counts[NO_METADATA] += 1
        elif item.get("is_mature") is not False:
            counts[MATURE] += 1
        elif _is_ai(tags):
            counts[AI_TAGGED] += 1
            ai_made += 1
        else:
            clean.append((work_id, entry, tags))
    if ai_made > len(clean):  # most of the works that could be judged: this is not an artist to list
        _purge(key, counts, db)
        library.note_skip(key, db=db)
        counts[MOSTLY_AI] += 1
        return True

    clean.sort(key=lambda work: library.published_at(work[1].get("published_time")) or "", reverse=True)
    kept: list[dict[str, Any]] = []
    refused = 0  # works in a row whose image the host would not serve for the rate limit
    for work_id, entry, tags in clean:  # newest first
        if len(kept) == MAX_WORKS:
            break
        thumbs = entry.get("thumbs")
        work = {
            "work_id": work_id,
            "page_url": entry.get("url"),
            "title": entry.get("title"),
            "tags": tags,
            "thumb_url": _src(thumbs[-1] if isinstance(thumbs, list) and thumbs else entry["preview"]),
            "published": entry.get("published_time"),
        }
        try:
            preview = library.image_address(_src(entry["preview"]))  # never the full original
            library.checked_work(**work)
        except library.ContactInTitle:  # hard rule 6: the title is in the page address too, so the work is not kept
            counts[TITLE] += 1
            continue
        except ValueError:
            counts[MALFORMED] += 1
            continue
        if library.cached_image(work_id, db):
            counts[CACHED] += 1
        else:
            if store.is_removed(key, db):  # hard rule 7: asked again before every download
                break
            fetched = _fetch_image(http, preview)
            if isinstance(fetched, str):  # this work is not kept, and the next one is tried
                counts[fetched] += 1
                refused = refused + 1 if fetched == IMAGE_LIMITED else 0
                if refused == MAX_REFUSALS:
                    # By now it is us the host is refusing, not these images. The artist is noted as passed
                    # over first, so the next run does not begin with them again and end where this one did.
                    library.note_skip(key, db=db)
                    raise discover.Stop("rate limited by the image host")
                continue
            refused = 0
            try:
                library.save_image(work_id, *fetched)
            except ValueError:  # the bytes do not begin the way the declared type does
                counts[IMAGE_WRONG_TYPE] += 1
                continue
            counts[DOWNLOADED] += 1
        kept.append(work)
    if store.is_removed(key, db):  # and once more before anything is written. What was downloaded is swept up.
        _remove(key, counts, db, user_id)
        counts[REMOVED] += 1
        return True
    if not kept and library.has_works(key, db):
        # This turn would leave the artist with none of the works kept for them. That is what an artist taking
        # everything down looks like, and it is also what an API that answers with empty lists looks like.
        # So it is not acted on at once. Like an account called gone, it is noted and left for _run to settle.
        library.note_skip(key, db=db)
        counts[EMPTIED] += 1
        return False
    try:
        # One transaction for the whole artist: the profile, these works, and nothing else of theirs left behind.
        not_stored, stuck = library.store_artist(
            key,
            user_id=user_id,
            tagline=profile.get("tagline"),
            specialty=profile.get("artist_specialty"),
            level=profile.get("artist_level"),
            website=profile.get("website"),
            works=kept,
            db=db,
        )
    except sqlite3.IntegrityError:
        # A constraint refused the profile row. Nothing of this artist was written and what was downloaded is
        # swept up at the end. One artist the database will not take must not end the run for the rest.
        library.note_skip(key, db=db)
        counts[NOT_STORED] += 1
        return True
    counts[ROW_REFUSED] += not_stored
    counts[UNDELETED] += stuck
    counts[PROFILES] += 1
    counts[WORKS] += len(kept) - not_stored
    return True


def _where(exc: Exception) -> str:
    """An exception as its type and the line of this file it came through. Never its message and never a
    traceback: either can quote response data or a request URL."""
    lines = [frame.lineno for frame in traceback.extract_tb(exc.__traceback__) if frame.filename == __file__]
    return f"{type(exc).__name__} at collect.py line {lines[-1] if lines else '?'}"


def _run(
    api: discover._Api, http: httpx.Client, counts: dict[str, int], max_artists: int, db: pathlib.Path | str | None
) -> None:
    tried = library.last_attempts(db)
    # Whoever has never had a turn comes first, then whoever had theirs longest ago. A run that was cut short
    # picks up where it stopped, and an artist the last run had to pass over waits at the back of this one
    # instead of standing at the front of every run and in the way of everybody behind them.
    artists = sorted(store.all_artists(db), key=lambda artist: tried.get(artist[0], ""))
    failed_in_a_row = answered = 0
    gone: list[str] = []  # the artists whose account, or whose every work, the API showed gone in this run
    for _, username, _ in artists[: max(max_artists, 0)]:
        key = None
        try:
            key = store.artist_key(username)
            if store.is_removed(key, db):  # asked again right before each artist is fetched
                counts[REMOVED] += 1
                continue
            counts[PROCESSED] += 1
            if _collect_artist(api, http, username, counts, db):
                answered += 1
            else:
                gone.append(key)
            failed_in_a_row = 0
        except discover.Stop:
            raise  # the rate limit, or our own token: the next artist would fare no better
        except Exception as exc:
            # No single artist ends the run for the rest. This one is counted and noted as passed over, and what
            # they had downloaded is swept up at the end.
            api_failed = isinstance(exc, _ApiFailed)
            counts[API_FAILED if api_failed else FAILED] += 1
            print(f"skipped an artist: {exc if api_failed else _where(exc)}", file=sys.stderr)
            if key:
                library.note_skip(key, db=db)
            failed_in_a_row += 1
            if failed_in_a_row == MAX_FAILURES:
                raise discover.Stop(f"{MAX_FAILURES} artists in a row failed: the API seems to be down") from None
    # Only a run that was not stopped gets here. What is kept under a handle is purged, and the handle leaves the
    # queue, when DEAD_AFTER runs have shown the account gone (in one of the API's documented codes) or shown
    # none of the works kept for it; and a run counts only if the API answered for more artists than it showed
    # gone. No other answer counts. An outage that answered every lookup alike, twice, once emptied the library.
    if answered > len(gone):
        for key in gone:
            if library.note_skip(key, gone=True, db=db) >= DEAD_AFTER and _purge(key, counts, db, unlist=True):
                counts[GONE] += 1


def collect(
    client_id: str,
    client_secret: str,
    *,
    max_artists: int,
    db: pathlib.Path | str | None = None,
    transport: httpx.BaseTransport | None = None,
) -> int:
    """Run one collection pass and print its counts. Returns the exit code."""
    counts = dict.fromkeys(_COUNTS, 0)
    status = 0
    try:
        # The same client settings as discovery: the project user agent on every request, images included, and no
        # redirect ever followed. discover._Api adds the token, the 1 second between API requests, the backoff
        # on 429 and the one token refresh on 401.
        with (
            discover._http_logs_off(),
            httpx.Client(
                headers={"User-Agent": config.USER_AGENT}, transport=transport, timeout=30, follow_redirects=False
            ) as http,
        ):
            _sweep(counts, db)
            try:
                _run(discover._Api(http, client_id, client_secret), http, counts, max_artists, db)
            finally:
                _sweep(counts, db)  # however the run ended: a removal that landed during it is honoured before exit
    except discover.Stop as stop:
        print(f"stopped: {stop}", file=sys.stderr)
        status = 1
    except Exception as exc:  # deliberately broad: nothing gets out of here as a traceback
        print(f"stopped: {_where(exc)}", file=sys.stderr)
        status = 1
    if counts[UNDELETED]:
        print("incomplete: something that had to be deleted could not be. Run again.", file=sys.stderr)
        status = 1
    if counts[NOT_STORED] or counts[ROW_REFUSED]:
        print("incomplete: the database refused rows it was given. See the counts.", file=sys.stderr)
        status = 1
    if not status and counts[API_FAILED] == counts[PROCESSED] > 0:  # too few of them to stop the run, and all failed
        print("incomplete: the API failed for every artist that was tried.", file=sys.stderr)
        status = 1
    if counts[FAILED]:
        status = 1
    for name, count in counts.items():
        print(f"{name}: {count}")
    return status


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Collect artists' public profiles and recent works from DeviantArt.")
    parser.add_argument("--max-artists", type=int, default=200, help="stop after this many artists")
    args = parser.parse_args(argv)
    try:
        env = config.load_env()
    except FileNotFoundError:
        env = {}
    missing = [name for name in discover.ENV_NAMES if not env.get(name)]
    if missing:
        print(f"Missing in .env: {', '.join(missing)} (see .env.example)", file=sys.stderr)
        return 1
    client_id, client_secret = (env[name] for name in discover.ENV_NAMES)
    return collect(client_id, client_secret, max_artists=args.max_artists)


if __name__ == "__main__":
    sys.exit(main())
