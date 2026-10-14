"""Collect each discovered artist's public profile and about ten recent works through DeviantArt's official API.

    uv run python -m pipeline.collect --max-artists 200

For every artist the discovery stage stored, and nobody on the removal list: four public professional fields of
their profile, and up to MAX_WORKS of their most recent works that are images and explicitly not mature, whatever
their tags say: whether a work is AI-made is for the analysis stage. Each work is kept with its own tags and one
preview-size image in the local cache, for the analysis stage (owner decision 7 in PROGRESS.md). pipeline/library.py
does the keeping. Output is counts only.

This stage adds and brings up to date. It never deletes a stored artist or work because of anything the API or
the image host answered (the lead's decision, round 5, after two rounds of guards on when deleting was allowed
both failed). Two things delete, and nothing else: the removal list (hard rule 7), at both ends of every run and
again before each artist and each download; and the sweep of cached files that no row refers to.
  - A stored work that is missing from a later answer stays, with the time it was last seen.
  - A stored work, or an artist, that the API shows as no longer qualifying (mature, not a public image,
    account gone, a profile that names a removed site) is marked with the reason and the time. Later stages
    read the marks (library.work_marks, library.artist_marks) and decide what is published.
  - A removal reaches the artist it names, under every handle of their account, and nobody else. An own site
    that is kept for an artist is written into the store's row too, which is where the removal of a site looks.
    What a profile answers about a site puts one kind of artist on the removal list and no other: one of whom
    nothing is kept, whose profile names a removed site that no other profile has been seen to list (_bar).
    Nothing kept is deleted for it, since nothing is kept, and they are not collected later whatever their
    profile says then. Anybody else whose profile names a removed site is marked for as long as it does.
  - Nothing that is kept is replaced by nothing. An answer that is empty, or that the library's rules refuse,
    leaves what is kept, and for a refused one the artist or the work is marked.
  - A kept work whose thumbnail has come to show another file (library.own_file: not another size or
    signature of the same one) has its image fetched again. The image that is kept goes only when the new one
    is saved in its place; when the new one cannot be had, the work stays as it was kept, image and all. What
    the analysis stages read from the old image is stale from then on, and they read the new one.
  - An image that could not be had counts, whatever the host answered: MAX_REFUSALS works in a row stop the
    run, and an artist none of whose images was served is passed over, not stored as one with no works.
  - A 200 is believed only in its documented shape. Anything else is the API failing for that artist: nothing
    stored changes, it is counted, and MAX_FAILURES of them in a row stop the run.
  - No verdict is reached on information that is missing: a work is marked only for what its entry says.

No single artist ends a run, and none can stand in the way of the next one: whoever a run fails for or stops at
is noted as passed over first, and the next run takes them last. A run in which the API or the image host was
failing exits non-zero.

This is the one stage that reads a profile: discovery stores an artist from the tally of the browse pages and
reads none. So what a profile's website field says is judged here, on the artist's turn, by the rules discovery
used to enforce: every host the field names is asked of the removal list and recorded for the shared-domain
rule (store.record_listing), a link is written into the store's row only when the field is, as written, the
front page of a domain of their own (_external_links), and the summary says which of five kinds each stored
profile's field was.

Requests to the API wait their turn in discover.Pace, and downloads from the image host in a Pace of their own:
either host asking us to slow down slows every later request to it, not only the one that is asked again, and
either host refusing through an hour of the slowest pace stops the run. A run holds both hosts while it lasts:
one that finds another run talking to either stops before it touches anything, the closing sweep included.

Endpoints (deviantart.readme.io, read 2026-10-12). Each is documented under the security scheme
"oauth2AuthorizationCodeOrClientCredentials" with scope "browse", so a client_credentials token is accepted:
    GET /api/v1/oauth2/user/profile/{username}   no parameters: "expand" would add age, sex and location
    GET /api/v1/oauth2/gallery/all               username, limit (1 to 24)
    GET /api/v1/oauth2/deviation/metadata        deviationids
An image is fetched from the preview address its gallery entry carries, without the token: it is a public file.
"""

import argparse
import pathlib
import sqlite3
import sys
import traceback
from typing import Any

import httpx

from pipeline import config, crawl, discover, library, store

MAX_WORKS = 10  # works kept per artist, each run
# ponytail: one gallery page, the documented maximum of 24. An artist with fewer than MAX_WORKS qualifying works
# among their 24 newest gets fewer. Follow next_offset if too many artists come up short.
GALLERY_LIMIT = 24
IMAGE_INTERVAL = 0.5  # seconds before an image download while the image host has not asked for more (discover.Pace)
SERVER_RETRIES = discover.SERVER_RETRIES  # times an image the host refuses or fails to serve is asked for again
MAX_FAILURES = 5  # artists failing one after another before the run gives up: by then it is the API, not them
MAX_REFUSALS = 3  # works in a row, whoever's, whose image could not be had, whatever was answered, before the run stops
NOT_IMAGE_KEYS = frozenset(("videos", "flash", "text_content", "excerpt"))  # what a film or a text carries

PROCESSED = "artists processed"
PROFILES = "profiles stored"
# Every profile stored falls under exactly one of these five, by what its website field says. Only the count is
# kept. An own site is optional (owner decision 7): an artist is stored either way.
OWN_SITE = "of those, with an own-site link"
NO_WEBSITE = "of those, no website given"
ON_PLATFORM = "of those, website on a listed platform"  # DeviantArt itself is one
OTHER_SHAPE = "of those, website on another domain, not a bare front page"  # a path, subdomain, query or fragment
NO_HOST = "of those, website names no public host"  # free text, a local name, no registrable domain
WORKS = "works stored"
DOWNLOADED = "images downloaded"
CACHED = "images already cached"
PURGED = "purged artists (removed)"
ORPHANS = "deleted cached files (no row for them)"
UNDELETED = "deletions that failed"
BAD_KEY = "rows with an unreadable artist key"
NOT_STORED = "skipped artists (the database refused the profile)"
ROW_REFUSED = "skipped works (the database refused the row, or another artist has it)"
FAILED = "skipped artists (unexpected error)"
API_FAILED = "skipped artists (the API failed for them)"
BAD_NAME = "skipped artists (unreadable handle)"
REMOVED = "skipped artists (removed)"
RENAMED = "skipped artists (a removed account under a new handle)"
UNAVAILABLE = "marked artists (account suspended, inactive or gone)"
LISTS_REMOVED = "marked artists (their profile names a removed site)"
ANSWER_REFUSED = "marked artists (an answer not kept, and what was kept stays)"
NO_IMAGE = "skipped artists (none of their images was served)"
MARKED = "marked works (stored, and no longer qualifying)"
MALFORMED = "skipped works (malformed entry)"
NOT_OWN = "skipped works (another author's)"
NOT_IMAGE = "skipped works (not a public image)"
MATURE = "skipped works (mature or unrated)"
NO_METADATA = "skipped works (no tag data)"
TITLE = "skipped works (contact details in the title or an address)"
IMAGE_FAILED = "skipped works (image not served)"
IMAGE_LIMITED = "skipped works (image host failing or rate limiting)"
IMAGE_WRONG_TYPE = "skipped works (image not an accepted type)"
IMAGE_TOO_LARGE = "skipped works (image over the size cap)"
IMAGE_HOST = "the image host"  # the name its Pace goes by in the summary
_COUNTS = (
    *(PROCESSED, PROFILES, OWN_SITE, NO_WEBSITE, ON_PLATFORM, OTHER_SHAPE, NO_HOST),
    *(WORKS, DOWNLOADED, CACHED, PURGED, ORPHANS, UNDELETED, BAD_KEY),
    *(NOT_STORED, ROW_REFUSED, FAILED, API_FAILED, BAD_NAME, REMOVED, RENAMED, NO_IMAGE),
    *(UNAVAILABLE, LISTS_REMOVED, ANSWER_REFUSED, MARKED),
    *(MALFORMED, NOT_OWN, NOT_IMAGE, MATURE, NO_METADATA, TITLE),
    *(IMAGE_FAILED, IMAGE_LIMITED, IMAGE_WRONG_TYPE, IMAGE_TOO_LARGE),
    *(*discover._PACING, *discover.Pace(IMAGE_INTERVAL, IMAGE_HOST).counts()),  # what each host's pace did
)
# The counts that say the API or the image host was failing. Any of them makes the run exit non-zero.
_FAILING = (FAILED, API_FAILED, IMAGE_LIMITED)
# What is written beside a stored work that no longer qualifies, and beside an artist. Nothing here acts on a mark.
_MARKS = {
    NOT_OWN: "another author's",
    NOT_IMAGE: "not a public image",
    MATURE: "mature",
    TITLE: "contact details",
}
GONE = "account gone"
LISTED = "lists a removed site"


class _ApiFailed(Exception):
    """The API would not answer for this artist. The message is safe to print: a name of ours and a status."""


def _src(part: object) -> object:
    return part.get("src") if isinstance(part, dict) else None


def _why_not(entry: dict[str, Any], username: str) -> tuple[str, bool] | None:
    """Why a gallery entry is not kept: the count it falls under, and whether the entry says so itself (True) or
    only fails to say what is needed (False). None for the artist's own public image that is explicitly not mature.

    The difference is what a stored work is marked on. An entry that says "mature" is a reason. An entry with no
    word on it is a reason not to take a new work, and no reason to think anything of one that is kept.
    """
    author = entry.get("author")
    name = author.get("username") if isinstance(author, dict) else None
    if not isinstance(name, str):
        return NOT_OWN, False
    if name.lower() != username.lower():
        return NOT_OWN, True
    locked = entry.get("tier_access") in ("locked", "locked-subscribed")
    if entry.get("is_deleted") is True or locked or not NOT_IMAGE_KEYS.isdisjoint(entry):
        return NOT_IMAGE, True
    sources = (_src(entry.get("content")), _src(entry.get("preview")))
    if entry.get("is_deleted") is not False or not all(isinstance(source, str) for source in sources):
        return NOT_IMAGE, False
    if entry.get("is_mature") is not False:  # only an explicit "not mature" counts
        return MATURE, entry.get("is_mature") is True
    return None


def _website(website: object) -> httpx.URL | None:
    """The profile's website field as a URL whose host is in its one spelling. None when it names no public host."""
    if not isinstance(website, str) or len(website) > 300:
        return None
    text = website.strip()
    try:
        url = httpx.URL(text if "://" in text else f"https://{text}")  # the parser is_own_site itself uses
        host = store.canonical_host(url.raw_host.decode("ascii"))
        return url.copy_with(host=host) if host else None
    except (httpx.InvalidURL, UnicodeError):  # UnicodeError: a lone surrogate, which JSON can carry
        return None


def _external_links(website: object) -> list[str]:
    """The profile's website field as zero or one link: the front page of the artist's own domain.

    crawl.is_own_site is the rule, and it sees the whole URL: only http(s)://janedoe.art/ or its www. form passes.
    A link with a path, a query, a fragment, a user subdomain, a port or credentials is dropped whole, never
    trimmed to the site root, because it cannot be told from a profile on a platform. What is kept is rebuilt
    as scheme://host/, so nothing else in the field can reach the store.
    """
    url = _website(website)
    if url is None or not crawl.is_own_site(str(url)):
        return []
    return [f"{url.scheme}://{url.raw_host.decode('ascii')}/"]


def _why_no_link(website: object, hosts: set[str]) -> str:
    """Which count a stored profile with no own-site link falls under, given the hosts its website field names.

    For the printed summary only. Nothing about the profile or the link is kept: the answer is one of four labels.
    """
    if not isinstance(website, str) or not website.strip():
        return NO_WEBSITE
    if any(store.registrable_domain(host) for host in hosts):  # a domain that could be their own, in another shape
        return OTHER_SHAPE
    for labels in (host.split(".") for host in hosts):  # the two lists the crawl stage refuses: names, then domains
        on_a_listed_domain = any(".".join(labels[start:]) in crawl.PLATFORM_DOMAINS for start in range(len(labels)))
        if on_a_listed_domain or not crawl.PLATFORM_NAMES.isdisjoint(labels):
            return ON_PLATFORM
    return NO_HOST


def _body(response: httpx.Response, what: str, *lists: str) -> dict[str, Any]:
    """The JSON object of a 200 that has the documented shape: an object, with a list under each of `lists`.

    _ApiFailed for anything else: a 5XX that outlasted its retries, a redirect, a 4XX, a 401 that outlasted a new
    token, a body that is not JSON or not shaped as documented. None of that says anything about the artist, so
    it ends their turn with nothing stored changed, and several in a row end the run (see _run).
    """
    if response.status_code != 200:  # 429 never gets here: discover._Api waits it out, or stops the run
        raise _ApiFailed(f"{what} answered HTTP {response.status_code}")
    try:
        body = response.json()
    except ValueError:
        body = None
    if not isinstance(body, dict) or not all(isinstance(body.get(name), list) for name in lists):
        raise _ApiFailed(f"{what} answered with a body of the wrong shape")
    return body


def _gone(response: httpx.Response) -> bool:
    """True when a 4XX carries one of the profile endpoint's documented codes: 0 suspended, 1 inactive, 2 not found.

    That is the API saying the account is no longer public. An outage, a block or a refusal says no such thing.
    """
    try:
        body = response.json() if 400 <= response.status_code < 500 else None
    except ValueError:
        return False
    return isinstance(body, dict) and type(body.get("error_code")) is int and body["error_code"] in (0, 1, 2)


def _fetch_once(http: httpx.Client, url: str, pace: discover.Pace) -> tuple[str, bytes] | str:
    """One request for an image: (content type, bytes), or the count it falls under when there is none to keep.

    Sent without the API token. The type is checked before the body is read, and the body is read only as far
    as the size cap. A redirect is not followed, so the image is not served. A 429, a 5XX or no answer at all
    is the host refusing or failing: IMAGE_LIMITED, and the host's pace is told, so that every later download
    waits longer, and so that a host which has refused through its hour stops the run (discover.Stop, from
    discover.Pace). Any other answer lets the pace ease.
    """
    try:
        with http.stream("GET", url) as response:
            if response.status_code == 429 or response.status_code >= 500:
                pace.slow(response)
                return IMAGE_LIMITED
            pace.ease()
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
        pace.slow()
        return IMAGE_LIMITED
    return content_type, bytes(data)


def _fetch_image(http: httpx.Client, url: str, api: discover.Pace, images: discover.Pace) -> tuple[str, bytes] | str:
    """_fetch_once() at the pace of the host the image is on, asked for again up to SERVER_RETRIES times while the
    host refuses or fails. IMAGE_LIMITED when it still does: that work is left out, and the pace stays slowed."""
    # An image served from the API's own host is one more request to that host, so it waits the API's interval.
    pace = api if httpx.URL(url).host == httpx.URL(discover.API_URL).host else images
    for left in range(SERVER_RETRIES, -1, -1):
        pace.wait()
        fetched = _fetch_once(http, url, pace)
        if fetched != IMAGE_LIMITED or not left:
            break
    return fetched


def _purge(key: str, counts: dict[str, int], db: pathlib.Path | str | None) -> bool:
    """library.purge() that does not end the run: a file that cannot be deleted is counted, and the run goes on to
    the next artist, whose data must not stay on disk because of it. False when something could not be deleted."""
    try:
        library.purge(key, db)
    except OSError:
        counts[UNDELETED] += 1
        return False
    return True


def _remove(key: str, counts: dict[str, int], db: pathlib.Path | str | None, user_id: object = None) -> bool:
    """Purge an artist who is on the removal list, under every handle their account is known by here, and first
    put the account on that list as well. This is the one way an artist's rows are ever deleted.

    The account is the hash of the API's user id, which does not change when a handle does. Without it the
    purge would delete the one thing that says who they were, and the same account under a new handle would be
    collected again (hard rule 7: not re-added later). It is taken from the profiles that were stored, and from
    `user_id` when the removal lands in the artist's own turn, which may be their first.
    """
    others = library.same_account(key, db)
    accounts = {account for account in (library.account_hash(key, db), library.account(user_id)) if account}
    if accounts:
        store.import_removed(accounts, db)
    done = True
    for other in others:  # the handle the account had before, or took after: barred as well, and purged
        store.mark_removed(other.removeprefix("da:"), db)
        done = _purge(other, counts, db) and done
    return _purge(key, counts, db) and done


def _sweep(counts: dict[str, int], db: pathlib.Path | str | None) -> None:
    """Hard rule 7, at both ends of every run: everything kept about an artist who is now on the removal list
    goes, and so do every cached file no row accounts for and every note about a handle nobody asks about."""
    unreadable = 0
    for key in sorted({*library.artist_keys(db), *library.last_attempts(db), *library.artist_marks(db)}):
        try:
            if store.is_removed(key, db) and _remove(key, counts, db):
                counts[PURGED] += 1
        except ValueError:  # a key nobody can read is counted, and the artists after it are purged all the same
            unreadable += 1
    counts[BAD_KEY] = max(counts[BAD_KEY], unreadable)  # the same rows at both ends of a run, counted once
    library.drop_orphan_rows(db)
    deleted, failed = library.drop_orphans(db)
    counts[ORPHANS] += deleted
    counts[UNDELETED] += failed


def _bar(named: list[tuple[str, object, set[str]]], counts: dict[str, int], db: pathlib.Path | str | None) -> None:
    """Hard rule 7, for the artists of whom nothing is kept and whose profile named a removed site in this run:
    (handle, API user id, the hosts the profile named). Whoever names such a site alone (store.lists_alone) goes
    on the removal list, handle and account, and their row and the notes about them go: what the removal would
    have done had it come after they were collected. The others stay as their turn left them, marked.

    At the end of the run and not on the artist's turn, because only then has every profile of the run been
    read: one website answered for every profile bars nobody. However the run ended.
    """
    for username, user_id, hosts in named:
        if store.lists_alone(username, hosts, db):
            store.mark_removed(username, db)
            counts[PURGED] += _remove(store.artist_key(username), counts, db, user_id)


def _collect_artist(
    api: discover._Api,
    http: httpx.Client,
    images: discover.Pace,
    username: str,
    counts: dict[str, int],
    db: pathlib.Path | str | None,
    refused: list[int],
    named: list[tuple[str, object, set[str]]],
) -> None:
    """One artist's turn. `images` is the image host's pace. `refused` is the run's one count, kept in a list so
    that it outlives the turn: the works in a row, this artist's and the ones before, whose image the host would
    not serve, retries and all. `named` is the run's list for _bar()."""
    key = store.artist_key(username)
    response = api.get(f"/user/profile/{username}")
    if _gone(response):
        # The API's own word that the account is suspended, inactive or missing. It is written down beside
        # whatever is kept under the handle, and nothing is deleted for it.
        library.mark_artist(key, GONE, db)
        library.note_skip(key, db)
        counts[UNAVAILABLE] += 1
        return
    profile = _body(response, "profile lookup")
    user = profile.get("user")
    user_id, name = (user.get("userid"), user.get("username")) if isinstance(user, dict) else (None, None)
    if not (isinstance(user_id, str) and user_id and isinstance(name, str) and name.lower() == username.lower()):
        # Not a profile, or somebody else's: one answer given to every lookup once passed for every artist.
        raise _ApiFailed("profile lookup answered with a profile that is not theirs")
    # Whose account a handle is does not change. A user id that is not the one kept for this handle, or that is
    # kept for another handle the API has not called gone, is the API failing: one wrong id once tied two
    # artists together, and the removal of one then purged and barred the other.
    kept_hash = library.account_hash(key, db)
    if kept_hash and kept_hash not in (library.account(user_id), library.account(user_id, "da-user")):
        raise _ApiFailed("profile lookup answered with a user id that is not theirs")
    marked = library.artist_marks(db)
    old_keys = [other for other in library.holders(user_id, db) if other != key]
    for other in old_keys:  # the same account under another handle: believed only of a handle that is gone
        if marked.get(other, ("",))[0] not in (GONE, library.RENAMED):
            raise _ApiFailed("profile lookup answered with a user id that another artist has")
    # Hard rule 7. An account removed under one handle is the same account under the next: the new handle is
    # barred through the store, and nothing is collected.
    if library.account_removed(user_id, db):
        store.mark_removed(username, db)
        _remove(key, counts, db, user_id)
        counts[RENAMED] += 1
        return
    # The removal list is asked about every host the website field names, however it is written, and the same
    # hosts are recorded as hashes for the shared-domain rule. A profile that names a removed site is one
    # answer's word, and one such answer given for every profile once purged and barred every artist. So
    # nothing is deleted and nobody is barred for it here: the artist is marked, which holds them back from
    # being published, and the mark goes when they are next stored. An artist of whom nothing is kept, under
    # this handle or an older one, is judged again when the run ends (_bar).
    # ponytail: the website field is the only link the profile response documents. Links typed into the bio
    # are not read, because that means parsing DeviantArt text. Revisit if too few artists have a website.
    website = profile.get("website")
    hosts = store.hosts_in(website)
    if store.record_listing(username, hosts, db):
        library.mark_artist(key, LISTED, db)
        library.note_skip(key, db)
        counts[LISTS_REMOVED] += 1
        if not old_keys and key not in library.artist_keys(db):
            named.append((username, user_id, hosts))
        return
    page = _body(api.get("/gallery/all", username=username, limit=GALLERY_LIMIT), "gallery lookup", "results")
    if not isinstance(page.get("has_more"), bool):
        raise _ApiFailed("gallery lookup answered with a body of the wrong shape")

    # What this turn learns about the artist's works that are already kept: a reason for one that no longer
    # qualifies, None for one that still does. library.store_artist() applies it to the works that are stored.
    marks: dict[str, str | None] = {}
    candidates: dict[str, dict[str, Any]] = {}  # deviation id -> gallery entry
    turned_away = set()  # an id listed twice qualifies only if every entry for it does
    for entry in page["results"]:
        try:
            work_id = library.deviation_id(entry.get("deviationid") if isinstance(entry, dict) else None)
        except ValueError:
            counts[MALFORMED] += 1
            continue
        reason = _why_not(entry, username)
        if reason:
            counts[reason[0]] += 1
            turned_away.add(work_id)
            if reason[1]:
                marks[work_id] = _MARKS[reason[0]]
        else:
            candidates[work_id] = entry
    for work_id in turned_away:
        candidates.pop(work_id, None)

    metadata: dict[str, dict[str, Any]] = {}  # deviation id -> its metadata, which is where the tags are
    if candidates:
        asked = {f"deviationids[{n}]": id_ for n, id_ in enumerate(candidates)}
        body = _body(api.get("/deviation/metadata", **asked), "metadata lookup", "metadata")
        for item in body["metadata"]:
            if isinstance(item, dict) and isinstance(item.get("deviationid"), str):
                # Of two items for one id, the one that is read is the one that does not say "not mature".
                work_id = item["deviationid"].upper()
                if metadata.setdefault(work_id, item).get("is_mature") is False or item.get("is_mature") is True:
                    metadata[work_id] = item

    # Whether a work is AI-made is not asked here. Every first-pass rule on tags that this stage had erred in both
    # directions; the analysis stage reads each work's tags and title, and the push stage holds back what it flags.
    clean: list[tuple[str, dict[str, Any], list[str]]] = []  # works that are fit to keep
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
            if item.get("is_mature") is True:
                marks[work_id] = _MARKS[MATURE]
        else:
            clean.append((work_id, entry, tags))

    clean.sort(key=lambda work: library.published_at(work[1].get("published_time")) or "", reverse=True)
    kept_before = library.work_ids(key, db)
    kept: list[dict[str, Any]] = []
    unserved = 0  # works of this turn whose image could not be had
    for work_id, entry, tags in clean:  # newest first
        if len(kept) == MAX_WORKS:
            marks.setdefault(work_id, None)  # seen and still good, though not among the newest this time
            continue
        thumbs = entry.get("thumbs")
        # A thumbnail is one only in the shape DeviantArt makes them. An entry with anything else has none.
        thumb = thumbs[-1] if isinstance(thumbs, list) and thumbs and library.is_thumbnail(_src(thumbs[-1])) else None
        # A new work with no thumbnail is shown by its preview. A work that is kept keeps the one it has.
        shown = _src(thumb or (None if work_id in kept_before else entry["preview"]))
        work = {
            "work_id": work_id,
            "page_url": entry.get("url"),
            "title": entry.get("title"),
            "tags": tags,
            "thumb_url": shown if library.is_thumbnail(shown) else None,
            "thumb_size": (thumb.get("width"), thumb.get("height")) if thumb else None,
            "published": entry.get("published_time"),
        }
        try:
            if work["thumb_url"] is None and work_id not in kept_before:
                raise ValueError("a new work whose thumbnail is no address")
            preview = library.image_address(_src(entry["preview"]))  # never the full original
            library.checked_work(**work)
        except library.ContactInTitle:  # hard rule 6: the title is in the page address too, so the work is not kept
            counts[TITLE] += 1
            marks[work_id] = _MARKS[TITLE]
            continue
        except ValueError:
            counts[MALFORMED] += 1
            continue
        # Cached, and of the file this entry's thumbnail shows. An artist who has put another file under the work
        # has changed what there is to read: the image is fetched again, in the place of the one that is kept.
        if library.cached_image(work_id, db, work["thumb_url"]):
            counts[CACHED] += 1
        else:
            if store.is_removed(key, db):  # hard rule 7: asked again before every download
                break
            fetched = _fetch_image(http, preview, api.pace, images)
            if not isinstance(fetched, str):
                try:
                    library.save_image(work_id, *fetched)
                except ValueError:  # the bytes do not begin the way the declared type does
                    fetched = IMAGE_WRONG_TYPE
            if isinstance(fetched, str):  # no image to keep, whatever was answered: the next work is tried
                counts[fetched] += 1
                unserved += 1
                refused[0] += 1
                if refused[0] == MAX_REFUSALS:  # by now it is the host, or us it is refusing, and not these images
                    raise discover.Stop("the image host is failing or rate limiting")
                continue
            refused[0] = 0
            counts[DOWNLOADED] += 1
            work["fetched"] = True  # the library's word that the image in the cache is this answer's
        kept.append(work)
    if store.is_removed(key, db):  # and once more before anything is written. What was downloaded is swept up.
        _remove(key, counts, db, user_id)
        counts[REMOVED] += 1
        return
    if unserved and not kept:
        # Not one image of theirs was served. That is the image host's word and says nothing about them, so
        # they are not stored as an artist with no works: they are passed over, and nothing kept changes.
        library.note_skip(key, db)
        counts[NO_IMAGE] += 1
        return
    try:
        # One transaction for the whole artist: the profile, these works, and the marks on the others.
        not_stored, marked_works, refused_fields = library.store_artist(
            key,
            user_id=user_id,
            tagline=profile.get("tagline"),
            specialty=profile.get("artist_specialty"),
            level=profile.get("artist_level"),
            website=profile.get("website"),
            works=kept,
            marks=marks,
            old_keys=old_keys,
            db=db,
        )
    except sqlite3.IntegrityError:
        # A constraint refused the profile row. Nothing of this artist was written and what was downloaded is
        # swept up at the end. One artist the database will not take must not end the run for the rest.
        library.note_skip(key, db)
        counts[NOT_STORED] += 1
        return
    counts[ROW_REFUSED] += not_stored
    counts[MARKED] += marked_works
    counts[ANSWER_REFUSED] += bool(refused_fields)
    counts[PROFILES] += 1
    counts[WORKS] += len(kept) - not_stored
    # The website that is kept now, whichever answer gave it. On a removed site, the mark the store just took
    # off goes back on. A site of their own is written into the store's row, where a removal of it looks, and
    # only on an answer whose field is the bare front page as written: the library keeps a website with its
    # query, fragment or port cut off, and a link of that shape is dropped whole, never trimmed to the root.
    links = _external_links(website)
    site = library.website(key, db)
    if store.record_listing(username, store.hosts_in(site), db):
        library.mark_artist(key, LISTED, db)
        counts[LISTS_REMOVED] += 1
    elif links:
        store.keep_own_site(username, site, db)
    counts[OWN_SITE if links else _why_no_link(website, hosts)] += 1


def _where(exc: Exception) -> str:
    """An exception as its type and the line of this file it came through. Never its message and never a
    traceback: either can quote response data or a request URL."""
    lines = [frame.lineno for frame in traceback.extract_tb(exc.__traceback__) if frame.filename == __file__]
    return f"{type(exc).__name__} at collect.py line {lines[-1] if lines else '?'}"


def _run(
    api: discover._Api,
    http: httpx.Client,
    images: discover.Pace,
    counts: dict[str, int],
    max_artists: int,
    db: pathlib.Path | str | None,
    named: list[tuple[str, object, set[str]]],
) -> None:
    tried = library.last_attempts(db)
    # Whoever has never had a turn comes first, then whoever had theirs longest ago. A run that was cut short
    # picks up where it stopped, and an artist the last run had to pass over waits at the back of this one
    # instead of standing at the front of every run and in the way of everybody behind them.
    artists = sorted(store.all_artists(db), key=lambda artist: tried.get(artist[0], ""))
    failed_in_a_row = 0
    refused = [0]  # see _collect_artist: one count for the whole run, or a failing host is never stopped at
    for _, username, _ in artists[: max(max_artists, 0)]:
        try:
            key = store.artist_key(username)
        except ValueError:  # a handle nobody can ask about. It is the store's row that is wrong, not the API.
            counts[BAD_NAME] += 1
            continue
        try:
            if store.is_removed(key, db):  # asked again right before each artist is fetched
                counts[REMOVED] += 1
                continue
            counts[PROCESSED] += 1
            _collect_artist(api, http, images, username, counts, db, refused, named)
            failed_in_a_row = 0
        except discover.Stop:
            # A rate limit, or no token to be had: the next artist would fare no better. The one it stopped at
            # is noted as passed over first, or every later run would begin with them and end here too.
            library.note_skip(key, db)
            raise
        except Exception as exc:
            # No single artist ends the run for the rest. This one is counted and noted as passed over, nothing
            # that is kept about them changes, and what they had downloaded is swept up at the end.
            api_failed = isinstance(exc, _ApiFailed)
            counts[API_FAILED if api_failed else FAILED] += 1
            print(f"skipped an artist: {exc if api_failed else _where(exc)}", file=sys.stderr)
            library.note_skip(key, db)
            failed_in_a_row += 1
            if failed_in_a_row == MAX_FAILURES:
                raise discover.Stop(f"{MAX_FAILURES} artists in a row failed: the API seems to be down") from None


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
        # The same client settings as discovery: the project user agent and Accept-Encoding on every request,
        # images included, and no redirect ever followed. discover._Api adds the token, its one refresh on 401,
        # and the pace of the API; the image host has a pace of its own.
        with (
            discover._http_logs_off(),
            httpx.Client(
                headers={"User-Agent": config.USER_AGENT}, transport=transport, timeout=30, follow_redirects=False
            ) as http,
        ):
            api, images = discover._Api(http, client_id, client_secret), discover.Pace(IMAGE_INTERVAL, IMAGE_HOST)
            # Before every request to either host, the run says where it is (see discover.tell).
            api.pace.tell = images.tell = lambda: discover.tell("collect", counts | api.pace.counts() | images.counts())
            named: list[tuple[str, object, set[str]]] = []  # see _bar
            # Both hosts are this run's from here on. Another run that has either is found out before anything
            # of its is touched: a sweep would take the images it has downloaded and not yet stored for orphans.
            with api.pace, images:
                _sweep(counts, db)
                try:
                    _run(api, http, images, counts, max_artists, db, named)
                finally:
                    try:
                        discover.tell("collect")  # first: whatever follows, no file is left that says a run is on
                        _bar(named, counts, db)
                        counts.update(api.pace.counts() | images.counts())
                    finally:
                        # However the run ended, and whatever went wrong just above: a removal that landed
                        # during the run is honoured before exit.
                        _sweep(counts, db)
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
    if counts[IMAGE_LIMITED]:
        print("incomplete: the image host was failing. See the counts.", file=sys.stderr)
    if any(counts[name] for name in _FAILING):  # the API's failures are on stderr already, one line an artist
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
