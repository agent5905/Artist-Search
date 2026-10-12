"""Find niche artists through DeviantArt's official API: every author with enough work under the niche tags.

    uv run python -m pipeline.discover --max-artists 200

An author with MIN_HITS or more non-mature works under the niche tags is stored, whether or not their profile
names a site of their own (owner decision 7 in PROGRESS.md: DeviantArt is the primary source, an own site is
optional). Later stages collect and analyze the works. This stage has no use for any of that content, so
nothing from a response is printed, logged or stored except the handle, the profile URL built from it, the
front page of the artist's own domain when the profile names one, and a hit count. Output is counts only.
One more thing is kept, as hashes that name nobody: which registrable domain each profile's website field
mentions, so that a domain several profiles list is never taken for one artist's own site, and so that a
removal reaches every account that lists the removed site.

Endpoints (deviantart.readme.io, read 2026-10-11). Each is documented as accepting a client_credentials token:
    POST /oauth2/token                          grant_type=client_credentials
    GET  /api/v1/oauth2/browse/tags             scope "browse"
    GET  /api/v1/oauth2/user/profile/{username} scope "browse"
"""

import argparse
import contextlib
import logging
import pathlib
import sys
import time
import traceback
from collections import Counter
from collections.abc import Iterator
from typing import Any

import httpx

from pipeline import config, crawl, store

ENV_NAMES = ("DEVIANT_ART_ID", "DEVIANT_ART_SECRET")  # the client id and secret, as .env.example names them
TOKEN_URL = "https://www.deviantart.com/oauth2/token"
API_URL = "https://www.deviantart.com/api/v1/oauth2"
PROFILE_URL = "https://www.deviantart.com/{}"
NICHE_TAGS = ("conceptart", "characterdesign", "characterconcept", "creaturedesign", "gameart", "environmentconcept")
MIN_HITS = 3  # deviations under the niche tags before an author counts as a niche artist
PAGE_SIZE = 50  # the documented maximum for browse/tags
MAX_OFFSET = 50000  # the documented maximum for browse/tags
# ponytail: fixed browse depth, 6 tags x 40 pages x 50 = 12,000 deviations a run (4 minutes).
# Raise it, or make it a flag, if a live run finds too few artists.
PAGES_PER_TAG = 40
MIN_INTERVAL = 1.0  # seconds between any two requests
MAX_429 = 6  # HTTP 429s in a row on one request before the run stops. The wait doubles each time: 1 to 32 seconds.
# The summary adds up: candidates = skipped (removed) + profiles unavailable + not reached + artists stored.
NOT_REACHED = "not reached (--max-artists)"
# Every stored artist falls under exactly one of these five: they have an own-site link, or this is why not.
# Only the count is kept. An own site is optional (owner decision 7): an artist is stored either way.
OWN_SITE = "of those, with an own-site link"
NO_WEBSITE = "of those, no website given"
ON_PLATFORM = "of those, website on a listed platform or DeviantArt"
OTHER_SHAPE = "of those, website on another domain, not a bare front page"  # a path, subdomain, query or fragment
NO_HOST = "of those, website names no public host"  # free text, a local name, no registrable domain
_COUNTS = (
    "pages fetched",
    "deviations counted",
    "mature skipped",
    "authors seen",
    f"candidates (>= {MIN_HITS} hits)",
    "skipped (removed)",
    "profiles unavailable",
    NOT_REACHED,
    "profiles fetched",
    "artists stored",
    OWN_SITE,
    NO_WEBSITE,
    ON_PLATFORM,
    OTHER_SHAPE,
    NO_HOST,
)


class Stop(Exception):
    """The run cannot go on. The message is safe to print: it never carries response data or credentials."""


class _Api:
    """Sequential access to the API. Every request, the token request included, goes through _send."""

    def __init__(self, http: httpx.Client, client_id: str, client_secret: str) -> None:
        self._http = http
        self._credentials = {"grant_type": "client_credentials", "client_id": client_id, "client_secret": client_secret}
        self._token = ""
        self._started = False

    def _send(self, method: str, url: str, **kwargs: Any) -> httpx.Response:
        wait = MIN_INTERVAL
        for _ in range(MAX_429):
            if self._started:
                # Not injectable, on purpose: the tests replace time.sleep, so this is the line they prove.
                time.sleep(wait)
            self._started = True
            response = self._http.request(method, url, **kwargs)
            if response.status_code != 429:
                return response
            wait *= 2  # the docs ask for exponential backoff on 429
        raise Stop(f"rate limited: {MAX_429} HTTP 429 responses in a row")

    def _new_token(self) -> None:
        response = self._send("POST", TOKEN_URL, data=self._credentials)  # in the body, never in the URL
        token = response.json().get("access_token") if response.status_code == 200 else None
        if not isinstance(token, str) or not token:
            raise Stop(f"token request failed with HTTP {response.status_code}")
        self._token = token

    def _authorized_get(self, path: str, params: dict[str, str | int]) -> httpx.Response:
        return self._send("GET", API_URL + path, params=params, headers={"Authorization": f"Bearer {self._token}"})

    def get(self, path: str, **params: str | int) -> httpx.Response:
        if not self._token:
            self._new_token()
        response = self._authorized_get(path, params)
        if response.status_code == 401:  # tokens last an hour: get a new one, once
            self._new_token()
            response = self._authorized_get(path, params)
        return response


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
    """Which count a stored artist with no own-site link falls under, given the hosts their website field names.

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


@contextlib.contextmanager
def _http_logs_off() -> Iterator[None]:
    """httpx logs every request URL at INFO, and a profile lookup's URL holds a handle. Not during a run."""
    loggers = [logging.getLogger(name) for name in ("httpx", "httpcore")]
    levels = [logger.level for logger in loggers]
    for logger in loggers:
        logger.setLevel(logging.CRITICAL + 1)
    try:
        yield
    finally:
        for logger, level in zip(loggers, levels, strict=True):
            logger.setLevel(level)


def _run(api: _Api, counts: dict[str, int], max_artists: int, db: pathlib.Path | str | None) -> None:
    hits: Counter[str] = Counter()  # lowercased handle -> distinct non-mature deviations under the niche tags
    names: dict[str, str] = {}  # lowercased handle -> the handle as DeviantArt spells it
    seen: set[str] = set()  # deviation ids, held in memory only, so a work under two tags counts once

    for tag in NICHE_TAGS:
        offset = 0
        for _ in range(PAGES_PER_TAG):
            response = api.get("/browse/tags", tag=tag, offset=offset, limit=PAGE_SIZE)
            if response.status_code != 200:
                raise Stop(f"browse failed with HTTP {response.status_code}")
            page = response.json()
            counts["pages fetched"] += 1
            for deviation in page.get("results") or []:
                if not isinstance(deviation, dict):  # one malformed entry is skipped; it does not end the run
                    continue
                deviation_id, author = deviation.get("deviationid"), deviation.get("author")
                username = author.get("username") if isinstance(author, dict) else None
                if not isinstance(deviation_id, str) or not isinstance(username, str):
                    continue
                if not store.HANDLE.fullmatch(username) or deviation_id in seen:
                    continue
                seen.add(deviation_id)
                if deviation.get("is_mature") is not False:  # only an explicit "not mature" counts
                    counts["mature skipped"] += 1
                    continue
                counts["deviations counted"] += 1
                hits[username.lower()] += 1
                names[username.lower()] = username
                counts["authors seen"] = len(hits)
            # The next page is where the response says it is, never our own arithmetic, and only forwards
            # inside the documented range.
            next_offset = page.get("next_offset")
            if page.get("has_more") is not True or type(next_offset) is not int:
                break
            if not offset < next_offset <= MAX_OFFSET:
                break
            offset = next_offset

    candidates = sorted((key for key, n in hits.items() if n >= MIN_HITS), key=lambda key: (-hits[key], key))
    counts[f"candidates (>= {MIN_HITS} hits)"] = len(candidates)
    for position, key in enumerate(candidates):
        if counts["artists stored"] >= max_artists:
            counts[NOT_REACHED] = len(candidates) - position
            break
        username = names[key]
        # Hard rule 7. A removed artist is skipped without a lookup, with one exception: while their site is not
        # known here, their profile is read once, solely so the site it names goes on the removal list as a hash
        # and a second account cannot bring them back. Nothing else from that profile is used or kept.
        if store.is_removed(store.artist_key(username), db) and not store.site_unresolved(username, db):
            counts["skipped (removed)"] += 1
            continue
        response = api.get(f"/user/profile/{username}")
        status = response.status_code
        # The docs give 4XX for a profile that cannot be shown (suspended, inactive, gone). One such account
        # must not end the run. 401 means our token, and 429 never gets here.
        unavailable = 400 <= status < 500 and status not in (401, 429)
        if status != 200 and not unavailable:
            raise Stop(f"profile lookup failed with HTTP {status}")
        if status == 200:
            counts["profiles fetched"] += 1
        # ponytail: the website field is the only link the profile response documents. Links typed into the bio
        # are not read, because that means parsing DeviantArt text. Revisit if too few artists have a website.
        body = response.json() if status == 200 else None
        website = body.get("website") if isinstance(body, dict) else None
        # The removal list is asked about every host the field names, however it is written, before the own-site
        # rule gets to drop the link. The same hosts are recorded as hashes for the shared-domain rule.
        hosts = store.hosts_in(website)
        if store.record_listing(username, hosts, db):
            counts["skipped (removed)"] += 1
            continue
        if unavailable:
            counts["profiles unavailable"] += 1
            continue
        # Every candidate whose profile can be read is stored. A link is kept only if it is their own site's
        # front page; without one the artist is stored all the same, with no link.
        links = _external_links(website)
        if not store.upsert_artist(username, PROFILE_URL.format(username), links, hits[key], db):
            counts["skipped (removed)"] += 1
            continue
        counts["artists stored"] += 1
        counts[OWN_SITE if links else _why_no_link(website, hosts)] += 1


def discover(
    client_id: str,
    client_secret: str,
    *,
    max_artists: int,
    db: pathlib.Path | str | None = None,
    transport: httpx.BaseTransport | None = None,
) -> int:
    """Run one discovery pass and print its counts. Returns the exit code."""
    counts = dict.fromkeys(_COUNTS, 0)
    status = 0
    try:
        # A redirect is never followed: it would carry the credentials or the token to wherever it points.
        with (
            _http_logs_off(),
            httpx.Client(
                headers={"User-Agent": config.USER_AGENT}, transport=transport, timeout=30, follow_redirects=False
            ) as http,
        ):
            _run(_Api(http, client_id, client_secret), counts, max_artists, db)
    except Stop as stop:
        print(f"stopped: {stop}", file=sys.stderr)
        status = 1
    except Exception as exc:
        # Deliberately broad, and no traceback: an exception message can quote response data or a request URL.
        lines = [frame.lineno for frame in traceback.extract_tb(exc.__traceback__) if frame.filename == __file__]
        print(f"stopped: {type(exc).__name__} at discover.py line {lines[-1] if lines else '?'}", file=sys.stderr)
        status = 1
    for name, count in counts.items():
        print(f"{name}: {count}")
    return status


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Find niche artists through the DeviantArt API (handles and links).")
    parser.add_argument("--max-artists", type=int, default=200, help="stop after storing this many artists")
    args = parser.parse_args(argv)
    try:
        env = config.load_env()
    except FileNotFoundError:
        env = {}
    missing = [name for name in ENV_NAMES if not env.get(name)]
    if missing:
        print(f"Missing in .env: {', '.join(missing)} (see .env.example)", file=sys.stderr)
        return 1
    client_id, client_secret = (env[name] for name in ENV_NAMES)
    return discover(client_id, client_secret, max_artists=args.max_artists)


if __name__ == "__main__":
    sys.exit(main())
