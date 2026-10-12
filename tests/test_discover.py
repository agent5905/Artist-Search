"""Tests for pipeline.discover.

Every username, URL, title and text below is invented for these tests. Nothing came from DeviantArt and
nothing here calls it: all traffic goes to an httpx.MockTransport. Nothing really sleeps either: the tests
replace time.sleep, which is the one thing the code waits with, and the fake clock moves only when it is called.
"""

import hashlib
import inspect
import itertools
import json
import logging
import random
import re
import sqlite3
import threading
import time
from contextlib import closing
from unittest import mock
from urllib.parse import parse_qs

import httpx
import pytest

from pipeline import config, crawl, discover, store

CLIENT_ID = "FAKE-CLIENT-ID-0001"
CLIENT_SECRET = "FAKE-CLIENT-SECRET-0001"
TOKEN_PATH = "/oauth2/token"
BROWSE_PATH = "/api/v1/oauth2/browse/tags"
PROFILE_PATH = "/api/v1/oauth2/user/profile/"

ALPHA, BRAVO, CHARLIE, DELTA = "zz-fake-alpha", "ZZ-Fake-Bravo", "zz-fake-charlie", "zz-fake-delta"
ECHO, FOXTROT, GOLF, HOTEL = "zz-fake-echo", "zz-fake-foxtrot", "zz-fake-golf", "zz-fake-hotel"
INDIA, JULIET, KILO = "zz-fake-india", "zz-fake-juliet", "zz-fake-kilo"

# The website each base-world artist gives on their profile. HOTEL has no profile: the lookup answers 400.
# The made-up domains sit under real suffixes (.art, .com, .co.uk) because the own-site rule reads the Public
# Suffix List and turns .example and .test away. Nothing ever fetches them.
WEBSITES = {
    ALPHA: "https://zz-fake-alpha.art/",
    BRAVO: "http://www.zz-fake-bravo.com",
    CHARLIE: "https://zz-fake-charlie.art",  # 2 hits: must never be looked up
    DELTA: "ZZ-Fake-Delta.co.uk.",  # no scheme, capitals, a trailing dot: still the front page of their domain
    ECHO: "https://zz-fake-echo.art",  # 2 clean hits: must never be looked up
    FOXTROT: "",
    GOLF: f"https://www.deviantart.com/{GOLF}/gallery",  # DeviantArt itself
    INDIA: f"https://www.deviantart.com./{INDIA}/art/FAKE-TITLE-99",  # the same, behind a trailing dot
    JULIET: f"https://www.artstation.com/{JULIET}",  # a hosted gallery, not a site of their own
    # A domain of their own, but with a path, a query and a fragment: dropped whole, never trimmed to the root.
    KILO: "https://zz-fake-kilo.art/folio?mail=fake.kilo@mail.invalid#FAKE-FRAGMENT",
}


def expected_row(name, links, hits):
    return (f"da:{name.lower()}", name, f"https://www.deviantart.com/{name}", links, hits)


# What the base world must leave in the store: key, handle, profile URL, links, hits. Nothing else.
# Every author with three clean works and a readable profile is stored (owner decision 7). A link is kept only
# when the website field is the front page of a domain of their own.
EXPECTED = [
    expected_row(ALPHA, ["https://zz-fake-alpha.art/"], 4),
    expected_row(BRAVO, ["http://www.zz-fake-bravo.com/"], 3),
    expected_row(DELTA, ["https://zz-fake-delta.co.uk/"], 3),
    expected_row(FOXTROT, [], 3),  # no website
    expected_row(GOLF, [], 3),  # DeviantArt itself
    expected_row(INDIA, [], 3),
    expected_row(JULIET, [], 3),  # a hosted gallery
    expected_row(KILO, [], 3),  # a path on their own domain: dropped, not trimmed
]
OWN_SITES = [(key, links[0]) for key, _, _, links, _ in EXPECTED if links]  # what the crawl stage is offered
EXPECTED_STDOUT = """\
pages fetched: 7
deviations counted: 94
mature skipped: 7
authors seen: 71
candidates (>= 3 hits): 9
skipped (removed): 0
profiles unavailable: 1
not reached (--max-artists): 0
profiles fetched: 8
artists stored: 8
of those, with an own-site link: 3
of those, no website given: 1
of those, website on a listed platform or DeviantArt: 3
of those, website on another domain, not a bare front page: 1
of those, website names no public host: 0
"""
BASE_REQUESTS = 17  # 1 token + 7 browse pages + 9 profile lookups
# The base world's requests in order: 0 token, 1-7 browse, then lookups 8 HOTEL (400), 9 ALPHA, 10 BRAVO, 11 DELTA...


def without(*keys):
    return [row for row in EXPECTED if row[0] not in keys]


def deviation(n, username, mature=False):
    """A made-up deviation carrying every kind of content the real API returns, plus tags and a description."""
    return {
        "deviationid": f"00000000-0000-4000-8000-{n:012d}",
        "printid": None,
        "url": f"https://www.deviantart.com/{username}/art/FAKE-TITLE-{n}",
        "title": f"FAKE-TITLE-{n}",
        "is_deleted": False,
        "is_mature": mature,
        "author": {
            "userid": f"FAKE-USERID-{username}",
            "username": username,
            "usericon": f"https://a.fake-cdn.invalid/avatars/FAKE-ICON-{username}.gif",
            "type": "FAKE-USERTYPE",
        },
        "stats": {"comments": 1, "favourites": 2},
        "published_time": "1700000000",
        "excerpt": f"FAKE-EXCERPT-{n}",
        "description": f"FAKE-DESCRIPTION-{n}",
        "tags": [{"tag_name": f"FAKE-TAG-{n}"}],
        "preview": {"src": f"https://images.fake-cdn.invalid/FAKE-PREVIEW-{n}.jpg", "height": 600, "width": 800},
        "content": {"src": f"https://images.fake-cdn.invalid/FAKE-CONTENT-{n}.png", "height": 1200, "width": 1600},
        "thumbs": [{"src": f"https://images.fake-cdn.invalid/FAKE-THUMB-{n}.jpg", "height": 150, "width": 200}],
    }


def profile(username, website):
    """A made-up profile response, private fields included (the real API can return all of these)."""
    return {
        "user": {
            "userid": f"FAKE-USERID-{username}",
            "username": username,
            "usericon": f"https://a.fake-cdn.invalid/avatars/FAKE-ICON-{username}.gif",
            "type": "FAKE-USERTYPE",
            "email": f"FAKE-EMAIL-{username}@mail.invalid",
            "details": {"sex": "FAKE-SEX", "age": 987654321, "joindate": "FAKE-JOINDATE"},
            "geo": {"country": "FAKE-GEO-COUNTRY", "countryid": 555000111, "timezone": "FAKE-TIMEZONE"},
            "profile": {"real_name": f"FAKE-NESTED-REALNAME-{username}", "tagline": "FAKE-NESTED-TAGLINE"},
        },
        "is_watching": False,
        "profile_url": f"https://www.deviantart.com/{username}",
        "user_is_artist": True,
        "artist_level": "FAKE-LEVEL",
        "artist_specialty": "FAKE-SPECIALTY",
        "real_name": f"FAKE-REALNAME-{username}",
        "tagline": f"FAKE-TAGLINE-{username}",
        "countryid": 777000333,
        "country": "FAKE-COUNTRY",
        "website": website,
        "bio": f"FAKE-BIO-{username} with a link to a bare domain, https://zz-fake-bio-link.art which is never read",
        "cover_photo": f"https://images.fake-cdn.invalid/FAKE-COVER-{username}.jpg",
        "last_status": {"body": f"FAKE-STATUS-{username}"},
        "stats": {"user_deviations": 444000111, "profile_pageviews": 444000222},
    }


PRIVATE_MARKERS = ("FAKE-EMAIL", "REALNAME", "FAKE-COUNTRY", "FAKE-GEO", "FAKE-SEX", "FAKE-TIMEZONE", "FAKE-JOINDATE")
PRIVATE_NUMBERS = ("987654321", "555000111", "777000333")  # age and the two country ids


class FakeDA:
    """A made-up DeviantArt behind httpx.MockTransport, with a clock that only its sleep() moves."""

    def __init__(self, by_tag, profiles):
        self.by_tag = by_tag
        self.profiles = {name.lower(): body for name, body in profiles.items()}
        # Like the real API, the fake decides how much a page holds: fewer than the client asks for, so a client
        # that works out the next offset by itself, instead of reading next_offset, skips works.
        self.page_size = 40
        self.page_patch = {}  # (tag, offset) -> fields to overwrite in that page, to serve odd paging
        self.clock = 0.0
        self.log = []  # one (clock, request, status) per request received
        self.served = []  # the text of every response sent
        self.threads = set()
        self.tokens = []
        self.valid = set()
        # Hook, called before each request with its index: return a status or a whole response to force it.
        self.before = lambda index, request: None
        self.transport = httpx.MockTransport(self._handle)

    def sleep(self, seconds):
        self.clock += seconds

    def _handle(self, request):
        if len(self.log) >= 500:  # far more than any test needs: fail a client that loops instead of hanging
            raise RuntimeError("runaway client")
        self.threads.add(threading.get_ident())
        forced = self.before(len(self.log), request)
        if isinstance(forced, httpx.Response):
            response = forced
        elif forced:
            response = httpx.Response(forced, json={"error": "FAKE-ERROR", "error_description": "FAKE-ERROR-TEXT"})
        else:
            response = self._respond(request)
        self.log.append((self.clock, request, response.status_code))
        self.served.append(response.text)
        return response

    def _respond(self, request):
        path = request.url.path
        if path == TOKEN_PATH:
            form = parse_qs(request.content.decode())
            wanted = {"grant_type": ["client_credentials"], "client_id": [CLIENT_ID], "client_secret": [CLIENT_SECRET]}
            if request.method != "POST" or request.url.query or form != wanted:
                return httpx.Response(401, json={"error": "invalid_client", "error_description": "FAKE-ERROR-TEXT"})
            self.tokens.append(f"FAKE-TOKEN-{len(self.tokens) + 1}")
            self.valid = {self.tokens[-1]}
            body = {"access_token": self.tokens[-1], "token_type": "Bearer", "expires_in": 3600, "status": "success"}
            return httpx.Response(200, json=body)
        if request.method != "GET" or request.headers.get("authorization", "")[len("Bearer ") :] not in self.valid:
            return httpx.Response(401, json={"error": "invalid_token", "error_description": "FAKE-ERROR-TEXT"})
        # The fake is stricter than the real API on purpose: a parameter the client was never meant to send
        # (mature_content, expand, with_session) is answered 400, so a test notices it.
        asked = sorted(request.url.params.keys())
        if asked != (["limit", "offset", "tag"] if path == BROWSE_PATH else []):
            return httpx.Response(400, json={"error": "invalid_request", "error_description": "FAKE-ERROR-TEXT"})
        if path == BROWSE_PATH:
            tag = request.url.params["tag"]
            offset, limit = int(request.url.params["offset"]), int(request.url.params["limit"])
            if not (1 <= limit <= 50 and 0 <= offset <= 50000):  # the documented ranges
                return httpx.Response(400, json={"error": "invalid_request", "error_description": "FAKE-ERROR-TEXT"})
            works = self.by_tag.get(tag, [])
            end = offset + min(limit, self.page_size)
            page = {"has_more": end < len(works), "next_offset": end if end < len(works) else None}
            page |= {"results": works[offset:end]} | self.page_patch.get((tag, offset), {})
            return httpx.Response(200, json=page)
        if path.startswith(PROFILE_PATH):
            body = self.profiles.get(path.removeprefix(PROFILE_PATH).lower())
            if body is None:  # what the docs give for a suspended, inactive or missing account
                error = {"error": "invalid_request", "error_description": "FAKE-ERROR-TEXT", "error_code": 2}
                return httpx.Response(400, json=error)
            return httpx.Response(200, json=body)
        return httpx.Response(404, json={})

    def paths(self, prefix):
        return [request.url.path for _, request, _ in self.log if request.url.path.startswith(prefix)]

    def looked_up(self):
        return [path.removeprefix(PROFILE_PATH) for path in self.paths(PROFILE_PATH)]

    def offsets(self, tag):
        """The offsets the client asked for under one tag, in order."""
        browse = [request.url.params for _, request, _ in self.log if request.url.path == BROWSE_PATH]
        return [int(params["offset"]) for params in browse if params["tag"] == tag]

    def gaps(self):
        times = [clock for clock, _, _ in self.log]
        return [later - earlier for earlier, later in itertools.pairwise(times)]


def world(websites=None):
    """The base world. See EXPECTED and EXPECTED_STDOUT for what a correct run makes of it."""
    numbers = itertools.count(1)

    def works(username, n, mature=False):
        return [deviation(next(numbers), username, mature) for _ in range(n)]

    twice = works(ALPHA, 1)  # one work filed under two tags counts once
    fillers = [work for i in range(60) for work in works(f"zz-fake-filler-{i:02d}", 1)]  # forces a second page
    by_tag = {
        "conceptart": works(ALPHA, 2) + twice + works(BRAVO, 3) + works(CHARLIE, 2) + fillers,
        "characterdesign": works(ALPHA, 1)
        + twice
        + works(DELTA, 3)
        + works(DELTA, 2, mature=True)  # 3 clean + 2 mature: 3 hits
        + works(ECHO, 2)
        + works(ECHO, 5, mature=True),  # 2 clean + 5 mature: under the threshold
        "gameart": [work for name in (FOXTROT, GOLF, INDIA, JULIET, KILO) for work in works(name, 3)] + works(HOTEL, 5),
    }
    return FakeDA(by_tag, {name: profile(name, site) for name, site in (WEBSITES | (websites or {})).items()})


def small_world(websites):
    """One artist per website, each with three clean works. Returns the fake and the handles."""
    names = [f"zz-fake-n{i:02d}" for i in range(len(websites))]
    works = [deviation(3 * i + j, name) for i, name in enumerate(names) for j in (1, 2, 3)]
    profiles = {name: profile(name, site) for name, site in zip(names, websites, strict=True)}
    return FakeDA({"gameart": works}, profiles), names


def run(fake, db, max_artists=200):
    with mock.patch.object(time, "sleep", fake.sleep):
        return discover.discover(CLIENT_ID, CLIENT_SECRET, max_artists=max_artists, db=db, transport=fake.transport)


def summary(out):
    """The printed counts as a dict. It also checks that every line is a label and a number, and nothing more."""
    assert all(re.fullmatch(r"[A-Za-z ,()>=0-9-]+: \d+", line) for line in out.splitlines()), out
    return {label: int(count) for label, count in (line.rsplit(": ", 1) for line in out.splitlines())}


def all_rows(db):
    if not db.exists():
        return []
    with closing(sqlite3.connect(db)) as conn:
        return conn.execute("SELECT * FROM artists ORDER BY key").fetchall()


def stored(db):
    return [(key, name, url, json.loads(links), hits) for key, name, url, links, hits, _ in all_rows(db)]


def everything_on_disk(db):
    """The store as SQL text, plus the raw bytes of every file a run could have left behind.

    That is everything under the test's own directory, which `isolated` makes the working directory and the
    cache directory too: a run that wrote a response to some file of its own would be caught here.
    """
    with closing(sqlite3.connect(db)) as conn:
        sql = "\n".join(conn.iterdump())
    raw = b"".join(path.read_bytes() for path in db.parent.parent.rglob("*") if path.is_file())
    return sql + raw.decode("utf-8", errors="ignore")


def strings(value):
    """Every string, and every number of a million or more, inside a JSON-like value."""
    if isinstance(value, dict):
        value = list(value.values())
    if isinstance(value, list):
        for item in value:
            yield from strings(item)
    elif isinstance(value, str) and value:
        yield value
    elif isinstance(value, int) and value >= 10**6:
        yield str(value)


@pytest.fixture(autouse=True)
def isolated(tmp_path, monkeypatch):
    """No test may read the real .env, write the real store, or really sleep."""

    def no_env():
        raise AssertionError("a test tried to read .env")

    monkeypatch.setattr(config, "load_env", no_env)
    monkeypatch.setattr(config, "CACHE_DIR", tmp_path / "default")
    monkeypatch.setattr(config, "LOCAL_DB", tmp_path / "default" / "pipeline.sqlite")
    monkeypatch.chdir(tmp_path)  # a stray relative write lands where everything_on_disk() looks
    monkeypatch.setattr(time, "sleep", lambda seconds: None)


@pytest.fixture(autouse=True)
def nothing_logged(caplog, recwarn):
    """In every test: no log record and no warning may carry made-up DeviantArt data, a handle or a credential.

    Every made-up string in this file has "fake" in it, so that word, in any case, in any record of any logger at
    any level, fails the test. So does one of the private numbers.
    """
    caplog.set_level(logging.DEBUG)  # on the root logger, which every logger that propagates reaches
    yield
    said = [logging.Formatter().format(record) + str(record.args) for record in caplog.get_records("call")]
    said += [str(warning.message) for warning in recwarn.list]
    assert [text for text in said if any(mark in text.lower() for mark in ("fake", *PRIVATE_NUMBERS))] == []


@pytest.fixture
def db(tmp_path):
    return tmp_path / "store" / "pipeline.sqlite"


# --- 1 and 2: no DeviantArt content and no private data, in the store or in the output ---


def test_base_run_stores_exactly_the_expected_rows(db):
    fake = world()
    assert run(fake, db) == 0
    assert stored(db) == EXPECTED
    assert len(fake.log) == BASE_REQUESTS


def test_every_candidate_whose_profile_can_be_read_is_stored_with_or_without_a_site(db):
    """Owner decision 7: an own site is optional. Three clean works under the niche tags are what it takes."""
    assert run(world(), db) == 0
    assert [name for _, name, _, links, _ in stored(db) if links] == [ALPHA, BRAVO, DELTA]
    assert [name for _, name, _, links, _ in stored(db) if not links] == [FOXTROT, GOLF, INDIA, JULIET, KILO]
    assert store.all_artists(db) == [(key, name, url) for key, name, url, _, _ in EXPECTED]  # for the collect stage
    assert store.own_site_candidates(db) == OWN_SITES  # for the crawl stage: only those with a site of their own
    for never in (HOTEL, CHARLIE, ECHO):  # no profile to read; two works; two clean works
        assert f"da:{never}" not in [key for key, *_ in stored(db)]


def test_a_removed_artist_is_in_neither_list_handed_to_the_later_stages(db):
    assert run(world(), db) == 0
    store.mark_removed(f"da:{FOXTROT}", db)  # stored with no link
    store.mark_removed("site:zz-fake-alpha.art", db)  # stored with a link
    left = [key for key, *_ in without("da:zz-fake-foxtrot", "da:zz-fake-alpha")]
    assert [key for key, _, _ in store.all_artists(db)] == left
    assert store.own_site_candidates(db) == OWN_SITES[1:]
    assert run(world(), db) == 0  # and a rerun brings neither back
    assert [key for key, _, _ in store.all_artists(db)] == left


def test_store_holds_no_deviantart_content(db):
    fake = world()
    assert run(fake, db) == 0
    allowed = {text for row in EXPECTED for text in strings(list(row))}
    forbidden = {text for text in strings([fake.by_tag, fake.profiles]) if not any(text in ok for ok in allowed)}
    # The check is not hollow: titles, image URLs, bios and the handles of artists we did not keep are all in it.
    assert len(forbidden) > 900
    assert {"FAKE-TITLE-1", "FAKE-TAG-1", "FAKE-EXCERPT-1", "FAKE-DESCRIPTION-1", HOTEL, CHARLIE} <= forbidden
    assert "https://images.fake-cdn.invalid/FAKE-THUMB-1.jpg" in forbidden
    assert f"FAKE-TAGLINE-{ALPHA}" in forbidden and profile(ALPHA, "")["bio"] in forbidden
    assert {WEBSITES[GOLF], WEBSITES[INDIA], WEBSITES[JULIET], WEBSITES[KILO]} <= forbidden
    disk = everything_on_disk(db)
    assert [text for text in forbidden if text in disk] == []
    for text in ("FAKE", "fake-cdn", "fake-bio-link", "/art/", "artstation", "mail.invalid", "@", "?", "#", "folio"):
        assert text not in "\n".join(str(row) for row in all_rows(db))
    assert "FAKE" not in disk and "fake-cdn" not in disk and "mail.invalid" not in disk and "artstation" not in disk
    assert "zz-fake-kilo.art" not in disk and "folio" not in disk  # a link with a path is dropped whole
    for text in allowed:  # and the scan can see what is stored
        assert text in disk


def test_store_holds_no_private_data_even_when_the_api_returns_it(db):
    fake = world()
    assert run(fake, db) == 0
    sent = "".join(fake.served)
    disk = everything_on_disk(db)
    for marker in (*PRIVATE_MARKERS, *PRIVATE_NUMBERS, "fake.kilo@mail.invalid"):
        assert marker in sent  # the API did return it
        assert marker not in disk
    with closing(sqlite3.connect(db)) as conn:
        tables = {name for (name,) in conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'")}
        columns = [row[1] for row in conn.execute("PRAGMA table_info(artists)")]
    assert tables == {"artists", "removed", "listed"}
    with closing(sqlite3.connect(db)) as conn:  # the two other tables hold sha256 digests and nothing else
        hashes = [
            value
            for row in conn.execute("SELECT key_hash, '' FROM removed UNION SELECT * FROM listed")
            for value in row
        ]
    assert len(hashes) == 8 and all(re.fullmatch(r"[0-9a-f]{64}", value) for value in hashes)  # 4 artists, 4 domains
    assert columns == ["key", "da_username", "da_profile_url", "links", "niche_hits", "discovered_at"]


def test_output_is_counts_only(db, capfd):
    fake = world()
    assert run(fake, db) == 0
    out, err = capfd.readouterr()  # the file descriptors themselves, so a write that bypasses sys.stdout shows too
    assert out.replace("\r\n", "\n") == EXPECTED_STDOUT
    assert err == ""
    printed = (out + err).lower()
    secrets = [CLIENT_ID, CLIENT_SECRET, *fake.tokens]
    for text in [*strings([fake.by_tag, fake.profiles]), *secrets]:  # handles and links included
        assert text.lower() not in printed


@pytest.mark.parametrize(("failure", "error"), [("not json", "JSONDecodeError"), ("network error", "ConnectError")])
def test_a_broken_response_stops_the_run_without_echoing_it(db, capsys, failure, error):
    def handler(request):
        if request.url.path == TOKEN_PATH:
            return httpx.Response(200, json={"access_token": "FAKE-TOKEN-1"})
        if failure == "network error":
            raise httpx.ConnectError(f"FAKE-NETWORK-TEXT {ALPHA}", request=request)
        return httpx.Response(200, text=f"<html>FAKE-HTML-TEXT {ALPHA}</html>")

    status = discover.discover(CLIENT_ID, CLIENT_SECRET, max_artists=5, db=db, transport=httpx.MockTransport(handler))
    out, err = capsys.readouterr()
    assert status == 1
    assert err.startswith(f"stopped: {error} at discover.py line ") and err.count("\n") == 1  # one line, no traceback
    for text in ("FAKE", ALPHA, "Traceback", "deviantart.com", CLIENT_SECRET):
        assert text not in out + err
    assert all_rows(db) == []


def test_unsafe_handles_are_ignored(db):
    unsafe = ["../../oauth2/placebo", "FAKE TITLE WITH SPACES", "zz-fake/x", "zz-fake?x=1", "", "\u00e9" * 5, "z" * 41]
    works = [deviation(n, name) for n, name in enumerate(unsafe * 3, 1)]  # 3 works each: enough to be a candidate
    works += [deviation(n, "zz-fake-orphan") | {"author": None} for n in (101, 102, 103)]  # no author at all
    fake = FakeDA({"conceptart": works}, {name: profile(name, "https://zz-fake-unsafe.art") for name in unsafe})
    assert run(fake, db) == 0
    assert len(fake.log) == 1 + len(discover.NICHE_TAGS)  # the token and one page per tag: no profile lookup
    assert all_rows(db) == []


# Every way of writing a host that is DeviantArt's, or an image host, a social network or a hosted platform.
NOT_THEIR_OWN_SITE = [
    "https://www.deviantart.com/zz-fake/gallery",
    "https://DeviantArt.com/zz-fake",
    "zz-fake.deviantart.com",
    "https://www.deviantart.com./zz-fake/art/FAKE-TITLE-99",  # a trailing dot
    "https://www.deviantart.com../zz-fake/art/FAKE-TITLE-99",
    "https://www.deviantart\u3002com/zz-fake/art/FAKE-TITLE-98",  # an ideographic full stop
    "https://www.deviantart.com\uff0e/zz-fake/art/FAKE-TITLE-98",  # a fullwidth one, trailing
    "https://www.deviantart%2Ecom/zz-fake/art/FAKE-TITLE-97",  # a percent-encoded dot
    "https://www.deviantart.com\\.zz-fake-site.art/",  # a backslash, which browsers read as a slash
    "https://www.\uff24eviantart.com/zz-fake",  # a fullwidth letter
    "https://www.deviantart.com:443/zz-fake",
    "https://images-wixmp-fake.wixmp.com/f/FAKE-IMAGE.jpg",
    "https://images-wixmp-fake.wixmp.com./f/FAKE-IMAGE-1.jpg",
    "https://a.deviantart.net/avatars/FAKE.gif",
    "fav.me/dfake",
    "fav.me./dfake",
    "sta.sh/0fake",
    "https://www.artstation.com/zz-fake",
    "https://twitter.com/zz-fake",
    "https://x.com/zz-fake",
    "https://www.instagram.com./zz-fake",
    "https://zz-fake.tumblr.com",
    "https://zz-fake.carrd.co",
    "https://linktr.ee/zz-fake",
    "https://zz-fake.wixsite.com/folio",
    "https://www.fiverr.com/zz-fake",
]


@pytest.mark.parametrize("website", NOT_THEIR_OWN_SITE)
def test_no_spelling_of_a_platform_host_becomes_a_link(website):
    assert discover._external_links(website) == []


def test_no_spelling_of_a_platform_host_reaches_the_store(db, capsys):
    fake, names = small_world(NOT_THEIR_OWN_SITE)
    assert run(fake, db) == 0
    assert fake.looked_up() == names  # every profile was read
    assert [links for *_, links, _ in stored(db)] == [[]] * len(names)  # each is stored, and none with a link
    counts = summary(capsys.readouterr().out)
    # All of them count as a platform but one: the backslash spelling also names a domain that is not one.
    assert (counts[discover.ON_PLATFORM], counts[discover.OTHER_SHAPE]) == (len(names) - 1, 1)
    assert counts[discover.NO_WEBSITE] == counts[discover.NO_HOST] == 0
    assert "FAKE" not in everything_on_disk(db)


# The lead's rule: only the front page of a bare custom domain is an artist's own site. Each of these may well be
# on a domain of their own, but it has the shape of a profile on a platform, so it is dropped whole. Trimming it
# to the site root would turn "someone's page on a platform nobody has listed" into "the platform is their site".
PROFILE_SHAPED = [
    "https://zz-fake-site.art/work",  # a path
    "https://zz-fake-site.art/zz-fake-user",
    "https://zz-fake-site.art/a/b/",
    "https://zz-fake-site.art//",
    "https://zz-fake-site.art/?user=zz-fake-user",  # a query
    "https://zz-fake-site.art?mail=fake.person@mail.invalid",
    "https://zz-fake-user.zz-fake-site.art",  # a user subdomain
    "https://shop.zz-fake-site.art/",
    "https://www2.zz-fake-site.art",
    "https://www.www.zz-fake-site.art",
    "https://zz-fake-user.zz-fake-site.co.uk",
    "https://zz-fake-site.art/#/zz-fake-user",  # a fragment used as a path
    "https://zz-fake-site.art/#fake.person@mail.invalid",
    "https://zz-fake-site.art/fake.person@mail.invalid",  # an email address, in any position or spelling
    "https://zz-fake-site.art/contact/fake.person%40mail.invalid",
    "https://zz-fake-site.art/fake.person%2540mail.invalid",
    "https://zz-fake-site.art/a;mail=fake.person@mail.invalid",
    "https://fake.person@zz-fake-site.art",
    "https://fake-user:fake-pass@zz-fake-site.art",
    "https://zz-fake-site.art:8443",  # a port of their own
    "https://zz-fake-site.art:8443/",
    "http://zz-fake-site.art:443",  # the other scheme's port
    "https://zz-fake-site.art\\zz-fake-user",  # a backslash, which browsers read as the start of a path
    "https://zz-fake-site.art/%2f",
    "https://zz-fake-site.art/;zz-fake-user",
]


@pytest.mark.parametrize("website", PROFILE_SHAPED)
def test_a_link_with_a_path_a_subdomain_or_a_query_is_dropped(website):
    assert discover._external_links(website) == []
    assert discover._external_links("https://zz-fake-site.art") == ["https://zz-fake-site.art/"]  # the shape decides


def test_profile_shaped_links_are_dropped_whole_and_never_trimmed_to_the_root(db, capsys):
    fake, names = small_world(PROFILE_SHAPED)
    assert run(fake, db) == 0
    assert fake.looked_up() == names  # every profile was read
    assert [links for *_, links, _ in stored(db)] == [[]] * len(names)  # stored with no link: not the bare domain
    assert summary(capsys.readouterr().out)[discover.OTHER_SHAPE] == len(names)
    disk = everything_on_disk(db)
    assert "zz-fake-site" not in disk and "mail.invalid" not in disk


@pytest.mark.parametrize(
    ("website", "links"),
    [
        # the front page of a bare custom domain: kept, rebuilt as scheme://host/ in one spelling
        ("https://zz-fake-site.art", ["https://zz-fake-site.art/"]),
        ("https://zz-fake-site.art/", ["https://zz-fake-site.art/"]),
        ("http://zz-fake-site.art", ["http://zz-fake-site.art/"]),
        ("https://www.zz-fake-site.art", ["https://www.zz-fake-site.art/"]),
        ("  zz-fake-site.art  ", ["https://zz-fake-site.art/"]),
        ("zz-fake-site.art/", ["https://zz-fake-site.art/"]),
        ("HTTP://WWW.ZZ-Fake-Site.Art./", ["http://www.zz-fake-site.art/"]),
        ("zz-fake-site.art.", ["https://zz-fake-site.art/"]),  # a trailing dot
        ("https://www.zz-fake-site.art\u3002/", ["https://www.zz-fake-site.art/"]),  # an ideographic one
        ("https://zz-fake-site\u3002art", ["https://zz-fake-site.art/"]),
        ("https://zz-fake-site.art\uff0e", ["https://zz-fake-site.art/"]),  # a fullwidth one
        ("https://zz-f\u00e4ke-site.art", ["https://xn--zz-fke-site-o8a.art/"]),
        ("https://zz-fake-stra\u00dfe.de", ["https://xn--zz-fake-strae-mdb.de/"]),  # spelled as browsers spell it
        ("https://zz-fake-site.co.uk", ["https://zz-fake-site.co.uk/"]),  # a two-part public suffix
        ("https://www.zz-fake-site.co.uk/", ["https://www.zz-fake-site.co.uk/"]),
        ("https://fakedeviantart.com", ["https://fakedeviantart.com/"]),  # a name that only contains a platform's
        # the same front page written with parts that change nothing: they are not stored
        ("https://zz-fake-site.art:443", ["https://zz-fake-site.art/"]),
        ("http://zz-fake-site.art:80/", ["http://zz-fake-site.art/"]),
        ("https://zz-fake-site.art?", ["https://zz-fake-site.art/"]),
        ("https://zz-fake-site.art/#", ["https://zz-fake-site.art/"]),
        # not a public site at all
        ("", []),
        (" ", []),
        (".", []),
        (None, []),
        (12345, []),
        (["https://zz-fake-site.art"], []),
        (b"https://zz-fake-site.art", []),
        ("FAKE-TAGLINE free text, not a link", []),
        ("see zz-fake-site.art", []),
        ("zz-fake-site.art\n/FAKE-TITLE-1", []),
        ("fake-person@mail.invalid", []),
        ("mailto:fake-person@mail.invalid", []),
        ("ftp://zz-fake-site.art", []),
        ("javascript://zz-fake-site.art/%0aalert(1)", []),
        ("//zz-fake-site.art", []),
        ("https:///zz-fake-site.art", []),
        ("https://", []),
        ("https://localhost", []),
        ("https://127.0.0.1", []),
        ("https://[::1]", []),
        ("https://[not-a-host", []),
        ("https://zz-fake-site.art:99999", []),
        # no public suffix, a suffix on its own, or a subdomain some host hands out
        ("https://zz-fake-site.example", []),
        ("https://zz-fake-site.test", []),
        ("https://zz-fake-site.invalid", []),
        ("https://zz-fake-site.local", []),
        ("https://co.uk", []),
        ("https://art", []),
        ("https://zz-fake.github.io", []),
        ("https://zz-fake.blogspot.com", []),
        # a host that is not a plain name
        ("https://zz-fake_site.art", []),
        ("https://zz-fake-site..art", []),
        ("https://-zz-fake-site.art", []),
        ("https://zz-fake-site-.art", []),
        ("https://zz-fake-site%2Eart", []),
        ("https://zz-fake\u200d-site.art", []),
        ("https://zz-fake-site.art/" + "x" * 300, []),
        ("zz-fake-site.art" + " " * 300, []),
    ],
)
def test_only_the_front_page_of_their_own_domain_is_kept(website, links):
    assert discover._external_links(website) == links


def test_a_lone_surrogate_in_the_website_field_is_dropped_without_raising():
    """JSON can carry half a surrogate pair, and URL parsers choke on it. One odd profile must not end a run."""
    half = chr(0xD800)
    for website in (half + "zz-fake-site.art", "https://zz-fake-site.art/" + half, f"https://zz-fake{half}-site.art"):
        assert discover._external_links(website) == []


def test_whatever_the_website_field_holds_a_kept_link_is_a_canonical_front_page():
    """A seeded fuzz. No input may raise (that would end a run), and only scheme://host/ of an own site comes out."""
    odd = [chr(code) for code in (0x00, 0x09, 0x0A, 0xDF, 0xE4, 0x130, 0x200D, 0x3002, 0xFF0E, 0xFF24, 0xFF61, 0xD800)]
    plain = "https:// http:// HTTPS:// ftp:// // : / ? # @ . .. www. zz-fake-site art com co.uk github.io deviantart"
    plain += " example - _ %2E %40 \\ [ ] ::1 127.0.0.1 :443 :8443 a=b xn-- localhost ; zz-fake-site.art"
    pieces = [*plain.split(), " ", "www.zz-fake-site.art", "https://zz-fake-site.co.uk", *odd]
    rng = random.Random(20261012)
    kept = 0
    for _ in range(20_000):
        website = "".join(rng.choice(pieces) for _ in range(rng.randint(1, 7)))
        for link in discover._external_links(website):
            kept += 1
            host = link.removeprefix("https://").removeprefix("http://").removesuffix("/")
            assert re.fullmatch(r"https?://[a-z0-9.-]+/", link), ascii(website)
            assert store.canonical_host(host) == host, ascii(website)
            assert crawl.is_own_site(link), ascii(website)
            assert store.site_key(link) in (f"site:{host}", f"site:{host.removeprefix('www.')}"), ascii(website)
    assert kept > 100  # the fuzz does reach the kept branch


@pytest.mark.parametrize(
    ("website", "count"),
    [
        ("", discover.NO_WEBSITE),
        ("   ", discover.NO_WEBSITE),
        (None, discover.NO_WEBSITE),
        (12345, discover.NO_WEBSITE),
        ("https://www.deviantart.com/zz-fake/gallery", discover.ON_PLATFORM),
        ("https://www.deviantart.com./zz-fake/art/FAKE-TITLE-99", discover.ON_PLATFORM),
        ("https://www.artstation.com/zz-fake", discover.ON_PLATFORM),  # a listed name, in any label
        ("zz-fake.tumblr.com", discover.ON_PLATFORM),
        ("https://www.pinterest.de/zz-fake", discover.ON_PLATFORM),
        ("https://zz-fake.github.io", discover.ON_PLATFORM),
        ("https://x.com/zz-fake", discover.ON_PLATFORM),  # a listed domain
        ("fav.me/dfake", discover.ON_PLATFORM),
        ("https://zz-fake.cargo.site", discover.ON_PLATFORM),
        ("https://zz-fake-site.art/folio", discover.OTHER_SHAPE),  # a path
        ("https://shop.zz-fake-site.art", discover.OTHER_SHAPE),  # a subdomain
        ("https://zz-fake-site.art/?u=zz-fake", discover.OTHER_SHAPE),  # a query
        ("https://zz-fake-site.art/#/zz-fake", discover.OTHER_SHAPE),  # a fragment
        ("https://zz-fake-site.co.uk:8443", discover.OTHER_SHAPE),
        ("<https://zz-fake-site.art>", discover.OTHER_SHAPE),
        ("zz-fake-site.art (commissions open)", discover.OTHER_SHAPE),
        ("FAKE-TAGLINE free text", discover.NO_HOST),
        ("https://localhost/x", discover.NO_HOST),
        ("https://127.0.0.1", discover.NO_HOST),
        ("https://zz-fake-site.example", discover.NO_HOST),  # no registrable domain, and on no list
        ("https://zz-fake-site.test/folio", discover.NO_HOST),
    ],
)
def test_a_profile_with_no_link_to_keep_is_counted_under_exactly_one_reason(website, count):
    assert discover._external_links(website) == []
    assert discover._why_no_link(website, store.hosts_in(website)) == count


def test_the_summary_splits_the_profiles_with_no_link_and_prints_counts_only(db, capfd):
    websites = [
        "",
        "https://www.artstation.com/zz-fake-one",
        "https://www.deviantart.com/zz-fake-two/gallery",
        "https://zz-fake-three.art/folio",
        "https://zz-fake-user.zz-fake-four.art",
        "https://zz-fake-five.art/?page=2",
        "FAKE-TAGLINE free text",
        "https://zz-fake-kept.art",
    ]
    fake, names = small_world(websites)
    assert run(fake, db) == 0
    out, err = capfd.readouterr()
    counts = summary(out.replace("\r\n", "\n"))
    assert err == ""
    assert counts[discover.NO_WEBSITE] == 1
    assert counts[discover.ON_PLATFORM] == 2
    assert counts[discover.OTHER_SHAPE] == 3
    assert counts[discover.NO_HOST] == 1
    assert counts[discover.OWN_SITE] == 1
    assert counts["artists stored"] == counts["profiles fetched"] == counts["candidates (>= 3 hits)"] == len(names)
    reasons = (discover.OWN_SITE, discover.NO_WEBSITE, discover.ON_PLATFORM, discover.OTHER_SHAPE, discover.NO_HOST)
    assert sum(counts[label] for label in reasons) == counts["artists stored"]  # each stored artist under one
    printed = out.lower()
    for told in ("zz-fake", "artstation", "folio", "free text", ".art", "http", "tagline"):
        assert told not in printed  # a count says how many, never who or where
    assert [bool(links) for *_, links, _ in stored(db)] == [False] * 7 + [True]  # all stored, one with a link
    assert "fake-three" not in everything_on_disk(db) and "fake-four" not in everything_on_disk(db)


# --- 3: rerunning creates no duplicates ---


def test_rerun_creates_no_duplicates(db):
    assert run(world(), db) == 0
    with closing(sqlite3.connect(db)) as conn, conn:
        conn.execute("UPDATE artists SET discovered_at = '2020-01-01T00:00:00+00:00'")  # so a rewrite would show
    first = all_rows(db)
    assert len(first) == len(EXPECTED)
    assert run(world(), db) == 0
    assert run(world(), db) == 0
    assert all_rows(db) == first
    with closing(sqlite3.connect(db)) as conn:
        assert conn.execute("SELECT count(*), count(DISTINCT lower(da_username)) FROM artists").fetchone() == (8, 8)


def test_max_artists_caps_what_one_run_stores(db, capsys):
    fake = world()
    assert run(fake, db, max_artists=1) == 0
    assert stored(db) == EXPECTED[:1]
    assert fake.looked_up() == [HOTEL, ALPHA]  # and the lookups stop there
    counts = summary(capsys.readouterr().out)
    assert counts[discover.NOT_REACHED] == 7  # 9 candidates: one unavailable, one stored, seven not reached
    capped = world()
    assert run(capped, db, max_artists=4) == 0  # a cap on what is stored, with or without a site
    assert stored(db) == EXPECTED[:4]
    assert summary(capsys.readouterr().out)[discover.NOT_REACHED] == 4


# --- 4: removed artists are skipped and never re-added (hard rule 7) ---


def test_a_removed_artist_whose_site_is_known_is_skipped_without_a_lookup(db, capsys):
    assert run(world(), db) == 0  # ALPHA is stored, so their site is known
    capsys.readouterr()
    assert store.mark_removed("da:ZZ-FAKE-ALPHA", db) == 1  # another case than the API uses
    for _ in range(2):
        fake = world()
        assert run(fake, db) == 0
        assert ALPHA not in fake.looked_up()
        assert stored(db) == without("da:zz-fake-alpha")
        assert "skipped (removed): 1\n" in capsys.readouterr().out


@pytest.mark.parametrize("how", ["mark_removed", "import_removed"])
def test_a_removed_handle_never_seen_here_is_looked_up_once_solely_to_bar_its_site(db, capsys, how):
    """Without that one lookup, a second account that lists the removed artist's site is stored on the next run."""
    if how == "mark_removed":
        assert store.mark_removed("da:ZZ-FAKE-ALPHA", db) == 0  # nothing is known about them here
    else:  # the removal arrives from the list of record, into a fresh cache
        assert store.import_removed([hashlib.sha256(b"da:zz-fake-alpha").hexdigest()], db) == 0
    first = world()
    assert run(first, db) == 0
    assert first.looked_up().count(ALPHA) == 1  # read once
    assert stored(db) == without("da:zz-fake-alpha")  # and nothing of it kept
    assert "skipped (removed): 1\n" in capsys.readouterr().out
    assert store.is_removed("site:zz-fake-alpha.art", db)  # their site is on the list now
    assert "alpha" not in everything_on_disk(db).lower()  # as a hash: no handle, no site, nothing else
    # The next run: a second account lists that site, and the removed artist shows a new one.
    second = world({ALPHA: "https://zz-fake-alpha-new.art", BRAVO: "https://www.zz-fake-alpha.art/"})
    assert run(second, db) == 0
    assert ALPHA not in second.looked_up()  # never read again
    assert stored(db) == without("da:zz-fake-alpha", "da:zz-fake-bravo")  # the second account is not stored
    assert store.is_removed(f"da:{BRAVO}", db)
    assert "alpha" not in everything_on_disk(db).lower() and "bravo" not in everything_on_disk(db).lower()


def test_a_handle_removed_after_being_seen_with_no_site_is_still_looked_up_once(db):
    """FOXTROT's profile was read before the removal and named no site, so nothing of theirs could be barred."""
    assert run(world(), db) == 0
    assert store.mark_removed(f"da:{FOXTROT}", db) == 1  # the row they had, which named no site
    moved = {FOXTROT: "https://zz-fake-foxtrot.art/folio"}  # by now they have a site
    after = world(moved)
    assert run(after, db) == 0
    assert after.looked_up().count(FOXTROT) == 1
    assert store.is_removed("site:zz-fake-foxtrot.art", db)
    again = world(moved | {BRAVO: "https://zz-fake-foxtrot.art"})  # and a second account turns up on it
    assert run(again, db) == 0
    assert FOXTROT not in again.looked_up()
    assert stored(db) == without("da:zz-fake-foxtrot", "da:zz-fake-bravo")
    assert "foxtrot" not in everything_on_disk(db).lower()


@pytest.mark.parametrize("handle", [HOTEL, FOXTROT, JULIET, KILO])  # no profile, no website, a platform, a path
def test_the_one_lookup_of_a_removed_handle_is_never_repeated_whatever_it_finds(db, handle):
    store.mark_removed(f"da:{handle}", db)
    first, second = world(), world()
    assert run(first, db) == 0 and run(second, db) == 0
    assert first.looked_up().count(handle) == 1
    assert handle not in second.looked_up()
    assert stored(db) == without(f"da:{handle}")  # everybody else is stored, as ever
    assert not store.is_removed("site:www.artstation.com", db)  # a platform never goes on the list
    assert store.is_removed("site:zz-fake-kilo.art", db) is (handle == KILO)  # a site of their own does, in any shape


def test_artist_removed_after_discovery_is_gone_and_never_readded(db):
    assert run(world(), db) == 0
    assert stored(db) == EXPECTED
    store.mark_removed(f"https://www.deviantart.com/{BRAVO}", db)  # the way an artist would send it
    assert stored(db) == without("da:zz-fake-bravo")
    for _ in range(2):
        fake = world()
        assert run(fake, db) == 0
        assert BRAVO not in fake.looked_up()
        assert stored(db) == without("da:zz-fake-bravo")
    assert "bravo" not in everything_on_disk(db).lower()


def test_artist_whose_site_was_removed_is_not_stored(db, capsys):
    store.mark_removed("site:www.zz-fake-delta.co.uk", db)
    assert run(world(), db) == 0
    assert stored(db) == without("da:zz-fake-delta")
    assert "skipped (removed): 1\n" in capsys.readouterr().out
    assert "delta" not in everything_on_disk(db).lower()


def test_a_removed_site_blocks_every_spelling_of_its_host(db, capsys):
    store.mark_removed("site:zz-fake-d.art", db)
    spellings = [  # each is the front page of the removed site, so each gets as far as the removal check
        "https://zz-fake-d.art",
        "https://zz-fake-d.art./",  # a trailing dot
        "https://www.zz-fake-d.art.",
        "zz-fake-d.art.",
        "HTTPS://WWW.ZZ-Fake-D.Art/",
        "http://zz-fake-d.art:80",
        "https://zz-fake-d\u3002art/",
        "https://www.zz-fake-d.art\uff0e",
    ]
    fake, names = small_world(spellings)
    assert run(fake, db) == 0
    assert fake.looked_up() == names
    assert all_rows(db) == []
    assert f"skipped (removed): {len(names)}\n" in capsys.readouterr().out
    assert all(store.is_removed(f"da:{name}", db) for name in names)  # and each of them is barred from now on
    assert "zz-fake-d" not in everything_on_disk(db).lower()


def test_site_removal_bars_the_artist_whatever_site_they_list_next(db):
    """A site removal is the artist asking to be left out. Moving to a new address must not bring them back."""
    assert run(world(), db) == 0
    store.mark_removed("site:zz-fake-delta.co.uk", db)  # removes DELTA, who was already stored
    store.mark_removed("site:zz-fake-bravo.com", db)  # BRAVO is stored under www.zz-fake-bravo.com: the same site
    assert stored(db) == without("da:zz-fake-delta", "da:zz-fake-bravo")
    moved = {DELTA: "https://zz-fake-delta-new.art", BRAVO: "https://zz-fake-bravo-new.art"}
    for _ in range(2):
        fake = world(moved)
        assert run(fake, db) == 0
        assert DELTA not in fake.looked_up() and BRAVO not in fake.looked_up()
        assert stored(db) == without("da:zz-fake-delta", "da:zz-fake-bravo")
    assert "delta" not in everything_on_disk(db).lower() and "bravo" not in everything_on_disk(db).lower()


def test_site_removed_before_discovery_bars_the_artist_too(db):
    store.mark_removed("site:zz-fake-delta.co.uk", db)
    assert run(world(), db) == 0  # the profile is read once, the removed site is seen, the handle is barred
    assert store.is_removed(f"da:{DELTA}", db)
    fake = world({DELTA: "https://zz-fake-delta-new.art"})
    assert run(fake, db) == 0
    assert DELTA not in fake.looked_up()
    assert stored(db) == without("da:zz-fake-delta")


def test_the_store_has_the_last_word_and_an_artist_it_refuses_is_not_counted_as_stored(db, capsys, monkeypatch):
    """Two checks stand between a removed site and the store. This is the second one, with the first one blind."""
    store.mark_removed("site:zz-fake-delta.co.uk", db)
    monkeypatch.setattr(store, "record_listing", lambda username, hosts, db=None: False)
    assert run(world(), db) == 0
    counts = summary(capsys.readouterr().out)
    assert stored(db) == without("da:zz-fake-delta")
    assert (counts["skipped (removed)"], counts["artists stored"]) == (1, len(EXPECTED) - 1)
    assert store.is_removed(f"da:{DELTA}", db)


@pytest.mark.parametrize(
    "website",
    [
        "https://www.zz-fake-delta.co.uk/folio?page=2",  # the own-site rule drops each of these shapes,
        "https://shop.zz-fake-delta.co.uk",  # so the removal has to be checked before it
        "https://zz-fake-delta.co.uk/#/zz-fake-delta",
        "http://zz-fake-delta.co.uk:8080/",
    ],
)
def test_a_removed_site_bars_whoever_lists_it_in_a_link_of_any_shape(db, capsys, website):
    store.mark_removed("site:zz-fake-delta.co.uk", db)
    fake = world({DELTA: website})
    assert run(fake, db) == 0
    assert DELTA in fake.looked_up()
    assert "skipped (removed): 1\n" in capsys.readouterr().out
    assert store.is_removed(f"da:{DELTA}", db)  # the handle is barred, though the link was never going to be kept
    later = world({DELTA: "https://zz-fake-delta-new.art"})
    assert run(later, db) == 0
    assert DELTA not in later.looked_up()
    assert stored(db) == without("da:zz-fake-delta")
    assert "delta" not in everything_on_disk(db).lower()


def test_removing_one_handle_removes_every_account_that_lists_the_same_site(db):
    """A renamed or second account: two handles, one site. Removing one by handle removes both, and the site."""
    studio = "https://zz-fake-studio.art/"
    assert run(world({ALPHA: studio, BRAVO: "http://www.zz-fake-studio.art"}), db) == 0
    assert [key for key, *_ in stored(db)] == [key for key, *_ in EXPECTED]
    assert store.mark_removed(f"da:{ALPHA}", db) == 2
    assert stored(db) == without("da:zz-fake-alpha", "da:zz-fake-bravo")
    assert store.is_removed(f"da:{BRAVO}", db) and store.is_removed("site:zz-fake-studio.art", db)
    # A later run: both come back under new sites, and a third handle now lists the removed site.
    later = world({ALPHA: "https://zz-fake-alpha-new.art", BRAVO: "https://zz-fake-bravo-new.art", DELTA: studio})
    assert run(later, db) == 0
    assert later.looked_up() == [HOTEL, DELTA, FOXTROT, GOLF, INDIA, JULIET, KILO]  # the two were not even looked up
    gone = ("da:zz-fake-alpha", "da:zz-fake-bravo", "da:zz-fake-delta")
    assert stored(db) == without(*gone)  # and the third handle went the same way, with the row it had
    assert store.is_removed(f"da:{DELTA}", db)
    last = world({DELTA: "https://zz-fake-delta-new.art"})
    assert run(last, db) == 0
    assert stored(db) == without(*gone)
    assert "studio" not in everything_on_disk(db).lower() and "delta" not in everything_on_disk(db).lower()


def test_the_removal_list_of_record_keeps_a_removed_artist_out_of_a_fresh_cache(db):
    assert run(world(), db) == 0
    store.mark_removed(f"da:{ALPHA}", db)
    with closing(sqlite3.connect(db)) as conn:
        of_record = [key_hash for (key_hash,) in conn.execute("SELECT key_hash FROM removed")]
    db.unlink()  # the local store is a cache: gone
    assert store.import_removed(of_record, db) == 0
    for expected_lookups in (1, 0):  # a fresh cache does not know their site was barred: one lookup, then none
        fake = world()
        assert run(fake, db) == 0
        assert fake.looked_up().count(ALPHA) == expected_lookups
        assert stored(db) == without("da:zz-fake-alpha")
    second_account = world({BRAVO: "https://zz-fake-alpha.art"})
    assert run(second_account, db) == 0
    assert stored(db) == without("da:zz-fake-alpha", "da:zz-fake-bravo")


def odd_spellings(host):
    """Ways a profile's website field can name a host that no two URL parsers read alike."""
    joiner, soft_hyphen, fullwidth_zz = chr(0x200D), chr(0xAD), chr(0xFF5A) * 2
    return [
        f"https://{host}\\x",  # a backslash
        f"<https://{host}>",
        f"({host})",
        f'"https://{host}"',
        f"[url=https://{host}]my site[/url]",
        f"{host} (commissions open)",
        f"my site: {host}!",
        f"my site...{host}",  # an ellipsis run into the name
        f"*.{host}",
        f"https://xn--zz-fake-{chr(0xE9)}.art and {host}",  # beside a name that is not valid: still found
        f"portfolio at {host}, prints at zz-fake-prints.art",
        f"https://{host}\n(second line)",
        f"{host};{host}",
        f"https://{host}/" + "x" * 400,  # over the length the own-site rule reads
        "x" * 400 + f" https://{host}",
        f"  {host.upper()}  ",
        f"https://{host}./",
        f"https://www.{host}",
        f"https://shop.{host}/folio",
        f"http://{host}:8080/x?y=1#z",
        f"https://{host}/#/zz-fake-user",
        f"https://fake-user:fake-pass@{host}/",
        f"fake.person@{host}",
        f"//{host}",
        f"https:///{host}",
        f"https://{host.replace('.', '%2E')}/",  # a percent-encoded dot
        f"https://{host.replace('.', chr(0x3002))}",  # an ideographic full stop
        f"https://{host}{chr(0xFF0E)}",  # a fullwidth one, trailing
        f"https://{host.replace('.', chr(0x3002))}/folio",  # the same three inside the name of a link with a path,
        f"https://{host.replace('.', chr(0xFF0E))}/folio",  # which only the scan of the raw field can recognise
        host.replace(".", chr(0xFF61)) + " (commissions open)",
        host.replace("zz", fullwidth_zz),  # fullwidth letters
        host[:3] + joiner + host[3:],  # an invisible character inside the name
        host[:3] + soft_hyphen + host[3:],
        f"https://zz-fake-redirect.art/?to=https://{host}/",
        f"https://zz-fake-redirect.art/out?url=https%3A%2F%2F{host}%2F",
        f"https://{host.replace('.', '%252E')}/",  # encoded twice
        host.replace(".", "&#46;"),  # an HTML entity for the dot
        host.replace(".", "[.]"),  # the dot spelled out, the way people hide an address from robots
        "hxxps://" + host.replace(".", "(.)") + "/folio",
        host.replace(".", " [dot] "),
        host.replace(".", " DOT "),
    ]


def test_a_removed_site_is_recognised_however_the_website_field_spells_it(db, capsys):
    store.mark_removed("site:zz-fake-d.art", db)
    spellings = odd_spellings("zz-fake-d.art")
    fake, names = small_world(spellings)
    assert run(fake, db) == 0
    assert fake.looked_up() == names
    not_barred = [spelling for name, spelling in zip(names, spellings, strict=True) if not store.is_removed(name, db)]
    assert not_barred == []
    assert all_rows(db) == []
    assert f"skipped (removed): {len(names)}\n" in capsys.readouterr().out
    # The next run: each of them shows a clean site of their own. None is read again, none is stored.
    later, _ = small_world([f"https://zz-fake-new-{n:02d}.art" for n in range(len(names))])
    assert run(later, db) == 0
    assert later.looked_up() == [] and all_rows(db) == []
    assert "zz-fake-d" not in everything_on_disk(db).lower()


def test_a_removed_site_with_a_name_outside_ascii_is_recognised_too(db):
    name = "zz-f" + chr(0xE4) + "ke-d.art"  # a-umlaut: xn--zz-fke-d-... once encoded
    assert store.mark_removed(f"site:{name}", db) == 0
    spellings = [f"<https://{name}/folio>", f"{name.upper()} (commissions open)", f"https://www.{name}/?page=2"]
    fake, names = small_world(spellings)
    assert run(fake, db) == 0
    assert [name for name in names if not store.is_removed(name, db)] == []
    assert all_rows(db) == []


def test_only_the_website_field_is_read_for_hosts_never_the_bio_or_the_rest_of_the_profile(db, capsys):
    """Every made-up bio names zz-fake-bio-link.art and every profile carries an email at mail.invalid. Reading
    hosts out of DeviantArt text is not allowed, so a removal of that site must touch nobody."""
    store.mark_removed("site:zz-fake-bio-link.art", db)
    fake = world()
    assert run(fake, db) == 0
    assert "zz-fake-bio-link.art" in "".join(fake.served)  # the API did send it, in the bio
    assert stored(db) == EXPECTED
    assert capsys.readouterr().out == EXPECTED_STDOUT  # nobody skipped as removed
    assert store.own_site_candidates(db) == OWN_SITES


def test_hostnames_are_found_in_running_text_and_nothing_else_is():
    assert store.hosts_in("see WWW.ZZ-Fake-Site.Art/folio or shop.zz-fake-site.co.uk!") == {
        "www.zz-fake-site.art",
        "shop.zz-fake-site.co.uk",
    }
    for nothing in (
        "",
        "FAKE-TAGLINE free text",
        "zz-fake-site",
        "1.2.3.4",
        "-x.art",
        "x.-art",
        None,
        12,
        ["zz-fake.art"],
    ):
        assert store.hosts_in(nothing) == set()
    assert store.hosts_in("x" * 20_000 + " zz-fake-site.art") == set()  # only the first 10,000 characters are read
    assert store.hosts_in(chr(0xD800) + " zz-fake-site.art " + chr(0xDFFF)) == {"zz-fake-site.art"}  # no error


# --- 9: the seeding list never offers the crawl stage a domain that several profiles list ---

FRONT, PATH, SUBDOMAIN = "https://zz-fake-plat.art/", "https://zz-fake-plat.art/u2", "https://u3.zz-fake-plat.art"
ALPHA_SEED = ("da:zz-fake-alpha", FRONT)


def test_the_seeding_list_is_the_stored_artists_with_a_domain_nobody_else_lists(db):
    assert run(world(), db) == 0
    assert store.own_site_candidates(db) == OWN_SITES


def test_a_domain_listed_by_three_profiles_in_one_run_is_not_a_candidate(db):
    """The front page is a bare domain and is stored. The other two listings have a profile's shape and are
    dropped, but they are counted: three profiles naming one domain make it a platform, whatever any list says."""
    assert run(world({ALPHA: FRONT, BRAVO: PATH, DELTA: SUBDOMAIN}), db) == 0
    links = {name: links for _, name, _, links, _ in stored(db)}
    assert (links[ALPHA], links[BRAVO], links[DELTA]) == ([FRONT], [], [])  # all three stored, one link kept
    assert store.own_site_candidates(db) == []
    kept = "\n".join(map(str, all_rows(db)))
    assert "u2" not in kept and "u3" not in kept and kept.count("zz-fake-plat") == 1  # the other two are hashes


def test_the_five_profile_case_leaves_only_the_artist_with_a_domain_to_themselves(db):
    websites = [
        "https://zz-fake-plat.art/",
        "https://zz-fake-plat.art/zz-fake-second",
        "https://zz-fake-third.zz-fake-plat.art",
        "https://www.zz-fake-plat.art/?u=zz-fake-fourth",
        "https://zz-fake-solo.art",
    ]
    fake, names = small_world(websites)
    assert run(fake, db) == 0
    assert store.own_site_candidates(db) == [(f"da:{names[4]}", "https://zz-fake-solo.art/")]


@pytest.mark.parametrize("second_listing", [PATH, SUBDOMAIN, "https://www.zz-fake-plat.art/?u=u4", f"<{FRONT}>"])
def test_a_domain_stops_being_a_candidate_when_a_later_run_finds_a_second_listing(db, second_listing):
    assert run(world({ALPHA: FRONT, BRAVO: "", DELTA: ""}), db) == 0
    assert store.own_site_candidates(db) == [ALPHA_SEED]  # one artist, one domain: a candidate
    assert run(world({ALPHA: FRONT, BRAVO: second_listing, DELTA: ""}), db) == 0
    assert store.own_site_candidates(db) == []


def test_a_domain_listed_in_an_earlier_run_is_not_a_candidate_when_its_front_page_turns_up(db):
    assert run(world({ALPHA: "", BRAVO: PATH, DELTA: SUBDOMAIN}), db) == 0
    assert not any(links for *_, links, _ in stored(db))  # nobody has a link,
    assert "plat" not in everything_on_disk(db).lower()  # and of the two listings nothing is kept but hashes
    assert run(world({ALPHA: FRONT, BRAVO: "", DELTA: ""}), db) == 0
    assert stored(db)[0][3] == [FRONT]
    assert store.own_site_candidates(db) == []


def test_the_same_artist_listing_their_domain_twice_is_still_one_listing(db):
    for website in (FRONT, "https://www.zz-fake-plat.art"):  # two runs, two spellings of one front page
        assert run(world({ALPHA: website, BRAVO: "", DELTA: ""}), db) == 0
    assert store.own_site_candidates(db) == [("da:zz-fake-alpha", "https://www.zz-fake-plat.art/")]
    assert run(world({ALPHA: f"{FRONT}folio", BRAVO: "", DELTA: ""}), db) == 0  # now a path: no link is kept
    assert stored(db)[0][3] == [] and store.own_site_candidates(db) == []


# --- 5: rate limits, backoff, token refresh, redirects, user agent ---


def test_requests_are_sequential_and_at_least_a_second_apart(db):
    fake = world()
    assert run(fake, db) == 0
    assert discover.MIN_INTERVAL == 1.0
    assert len(fake.gaps()) == BASE_REQUESTS - 1  # token, browse and profile requests alike
    assert min(fake.gaps()) >= discover.MIN_INTERVAL
    assert len(fake.threads) == 1  # one thread, one request at a time


def test_the_wait_is_time_sleep_itself_and_no_caller_can_switch_it_off(db, monkeypatch):
    """The production path, with nothing injected: only time.sleep is replaced, so nothing really waits."""
    waits = []
    monkeypatch.setattr(time, "sleep", waits.append)
    fake = world()
    assert discover.discover(CLIENT_ID, CLIENT_SECRET, max_artists=200, db=db, transport=fake.transport) == 0
    assert len(fake.log) == BASE_REQUESTS
    assert len(waits) == BASE_REQUESTS - 1  # one wait before every request but the first
    assert min(waits) >= 1.0
    assert "sleep" not in inspect.signature(discover.discover).parameters


def test_the_http_library_logs_nothing_during_a_run_and_is_left_as_it_was(db, caplog):
    """httpx logs every request URL at INFO, and a profile lookup's URL holds a handle."""
    logging.getLogger("httpx").setLevel(logging.INFO)  # as an application that wants request logs would set it
    try:
        fake, during = world(), []
        loud = [logging.getLogger(name) for name in ("httpx", "httpcore", "httpcore.connection", "httpcore.http11")]
        fake.before = lambda index, request: during.append(any(log.isEnabledFor(logging.CRITICAL) for log in loud))
        assert run(fake, db) == 0
        assert during == [False] * BASE_REQUESTS
        assert [record.name for record in caplog.records if record.name.startswith("http")] == []
        assert logging.getLogger("httpx").level == logging.INFO  # and afterwards it is the application's again
        assert logging.getLogger("httpcore").level == logging.NOTSET
    finally:
        logging.getLogger("httpx").setLevel(logging.NOTSET)


def test_pages_stop_at_the_depth_limit(db, monkeypatch):
    fake = world()
    assert run(fake, db) == 0
    browse = [dict(request.url.params) for _, request, _ in fake.log if request.url.path == BROWSE_PATH]
    assert (
        [(q["tag"], q["offset"], q["limit"]) for q in browse]
        == [
            ("conceptart", "0", "50"),
            ("conceptart", "40", "50"),  # where the response said the next page is, not 0 + 50
            *[(tag, "0", "50") for tag in discover.NICHE_TAGS[1:]],
        ]
    )
    monkeypatch.setattr(discover, "PAGES_PER_TAG", 1)
    shallow = world()
    assert run(shallow, db) == 0
    assert len(shallow.paths(BROWSE_PATH)) == len(discover.NICHE_TAGS)


def test_paging_follows_next_offset_from_the_response(db):
    fake = world()
    fake.page_size = 7  # a client stepping by its own limit of 50 would miss most of the works
    assert run(fake, db) == 0
    assert fake.offsets("conceptart") == list(range(0, 68, 7))
    assert fake.offsets("gameart") == [0, 7, 14]
    assert stored(db) == EXPECTED


@pytest.mark.parametrize(
    "paging",
    [
        {"has_more": False, "next_offset": 40},  # has_more decides, even with a next_offset on offer
        {"has_more": None, "next_offset": 40},
        {"has_more": "true", "next_offset": 40},
        {"has_more": 1, "next_offset": 40},
        {"has_more": True, "next_offset": None},
        {"has_more": True, "next_offset": "40"},
        {"has_more": True, "next_offset": 40.0},
        {"has_more": True, "next_offset": True},
        {"has_more": True, "next_offset": 0},  # not forwards
        {"has_more": True, "next_offset": -40},
        {"has_more": True, "next_offset": 50001},  # past the documented maximum
    ],
)
def test_paging_stops_unless_the_response_gives_a_usable_next_page(db, paging):
    fake = world()
    fake.page_patch[("conceptart", 0)] = paging
    assert run(fake, db) == 0
    assert fake.offsets("conceptart") == [0]
    assert len(fake.paths(BROWSE_PATH)) == len(discover.NICHE_TAGS)  # and it went on to the other tags


@pytest.mark.parametrize("stuck", [40, 0])  # the same page again, or backwards
def test_paging_only_moves_forwards(db, stuck):
    fake = world()
    fake.page_patch[("conceptart", 40)] = {"has_more": True, "next_offset": stuck}
    assert run(fake, db) == 0
    assert fake.offsets("conceptart") == [0, 40]


def test_paging_never_goes_past_offset_50000(db):
    fake = world()
    fake.page_patch[("conceptart", 0)] = {"has_more": True, "next_offset": 50000}  # the last offset allowed
    fake.page_patch[("conceptart", 50000)] = {"has_more": True, "next_offset": 50040}
    assert run(fake, db) == 0  # the fake answers 400 past 50000, which would have stopped the run
    assert fake.offsets("conceptart") == [0, 50000]
    assert discover.MAX_OFFSET == 50000


@pytest.mark.parametrize(
    ("index", "status", "target", "reason"),
    [
        (0, 307, "https://fake-elsewhere.example/oauth2/token", "token request"),  # would re-post the credentials
        (3, 302, "https://fake-elsewhere.example/browse", "browse"),  # would carry the token off site
        (3, 302, "/api/v1/oauth2/placebo", "browse"),
        (9, 301, f"/api/v1/oauth2/user/profile/{BRAVO}", "profile lookup"),
    ],
)
def test_redirects_are_not_followed(db, capsys, index, status, target, reason):
    fake = world()
    fake.before = lambda i, request: httpx.Response(status, headers={"location": target}) if i == index else None
    assert run(fake, db) == 1
    assert len(fake.log) == index + 1  # nothing was sent after the redirect: not to its target, not anywhere
    assert {request.url.host for _, request, _ in fake.log} == {"www.deviantart.com"}
    assert capsys.readouterr().err == f"stopped: {reason} failed with HTTP {status}\n"


def test_429_backs_off_exponentially_then_carries_on(db, capsys):
    fake = world()
    fake.before = lambda index, request: 429 if index in (3, 4, 5) else None
    assert run(fake, db) == 0
    assert [status for _, _, status in fake.log[3:7]] == [429, 429, 429, 200]
    assert len({str(request.url) for _, request, _ in fake.log[3:7]}) == 1  # the same request, retried
    assert fake.gaps()[2:7] == [1.0, 2.0, 4.0, 8.0, 1.0]  # the gaps before requests 3 to 7
    assert len(fake.log) == BASE_REQUESTS + 3
    assert stored(db) == EXPECTED
    assert capsys.readouterr().out == EXPECTED_STDOUT  # nothing counted twice


@pytest.mark.parametrize("first_429", [0, 4, 10])  # the token request, a browse page, the third profile lookup
def test_repeated_429_stops_the_run_cleanly(db, capsys, first_429):
    fake = world()
    fake.before = lambda index, request: 429 if index >= first_429 else None
    assert run(fake, db) == 1
    out, err = capsys.readouterr()
    assert err == f"stopped: rate limited: {discover.MAX_429} HTTP 429 responses in a row\n"
    assert len(fake.log) == first_429 + discover.MAX_429  # it gave up, and sent nothing afterwards
    refused = fake.log[first_429:]
    assert {status for _, _, status in refused} == {429}
    assert len({str(request.url) for _, request, _ in refused}) == 1
    assert fake.gaps()[first_429:] == [2.0, 4.0, 8.0, 16.0, 32.0]
    assert min(fake.gaps(), default=1.0) >= discover.MIN_INTERVAL
    assert stored(db) == (EXPECTED[:1] if first_429 == 10 else [])  # what was stored before the stop is kept
    assert out.count("\n") == len(EXPECTED_STDOUT.splitlines()) and "FAKE" not in out + err


def test_401_refreshes_the_token_once_and_carries_on(db):
    fake = world()

    def expire(index, request):
        if index == 5:
            fake.valid.clear()

    fake.before = expire
    assert run(fake, db) == 0
    assert fake.tokens == ["FAKE-TOKEN-1", "FAKE-TOKEN-2"]
    (_, refused, s5), (_, refresh, s6), (_, retried, s7) = fake.log[5:8]
    assert (s5, s6, s7) == (401, 200, 200)
    assert refresh.url.path == TOKEN_PATH
    assert retried.url == refused.url
    assert refused.headers["authorization"] == "Bearer FAKE-TOKEN-1"
    assert retried.headers["authorization"] == "Bearer FAKE-TOKEN-2"
    assert len(fake.log) == BASE_REQUESTS + 2
    assert min(fake.gaps()) >= discover.MIN_INTERVAL
    assert stored(db) == EXPECTED


def test_a_second_401_stops_the_run_instead_of_looping(db, capsys):
    fake = world()

    def always_expired(index, request):
        if request.url.path != TOKEN_PATH:
            fake.valid.clear()

    fake.before = always_expired
    assert run(fake, db) == 1
    assert [(request.url.path, status) for _, request, status in fake.log] == [
        (TOKEN_PATH, 200),
        (BROWSE_PATH, 401),
        (TOKEN_PATH, 200),
        (BROWSE_PATH, 401),
    ]
    assert capsys.readouterr().err == "stopped: browse failed with HTTP 401\n"
    assert all_rows(db) == []


def test_profile_lookups_refused_with_401_stop_the_run_at_the_first_one(db, capsys):
    """If a client_credentials token turns out not to open profiles, the run stops there. No workaround."""
    fake = world()

    def refuse_profiles(index, request):
        return 401 if request.url.path.startswith(PROFILE_PATH) else None

    fake.before = refuse_profiles
    assert run(fake, db) == 1
    assert [(request.url.path, status) for _, request, status in fake.log[8:]] == [
        (PROFILE_PATH + HOTEL, 401),
        (TOKEN_PATH, 200),
        (PROFILE_PATH + HOTEL, 401),
    ]
    assert capsys.readouterr().err == "stopped: profile lookup failed with HTTP 401\n"
    assert all_rows(db) == []


@pytest.mark.parametrize("status", [400, 403, 404, 410, 418, 451])
def test_a_profile_answered_with_any_4xx_is_skipped_and_the_run_goes_on(db, capsys, status):
    """The docs declare a profile's errors as 4XX. One suspended account must not stop every run at the same place."""
    fake = world()
    fake.before = lambda index, request: status if request.url.path == PROFILE_PATH + ALPHA else None
    assert run(fake, db) == 0
    out, err = capsys.readouterr()
    assert err == ""
    assert "profiles unavailable: 2\n" in out  # HOTEL, which has no profile, and ALPHA
    assert stored(db) == without("da:zz-fake-alpha")
    assert len(fake.log) == BASE_REQUESTS  # every other candidate was still looked up


@pytest.mark.parametrize("status", [204, 302, 500, 503])
def test_any_other_answer_to_a_profile_lookup_stops_the_run(db, capsys, status):
    fake = world()
    fake.before = lambda index, request: status if request.url.path == PROFILE_PATH + ALPHA else None
    assert run(fake, db) == 1
    assert capsys.readouterr().err == f"stopped: profile lookup failed with HTTP {status}\n"
    assert fake.looked_up() == [HOTEL, ALPHA]
    assert all_rows(db) == []


def test_wrong_credentials_stop_the_run_without_printing_them(db, capsys):
    fake = world()
    status = discover.discover("FAKE-WRONG-ID", "FAKE-WRONG-SECRET", max_artists=5, db=db, transport=fake.transport)
    out, err = capsys.readouterr()
    assert status == 1
    assert err == "stopped: token request failed with HTTP 401\n"
    assert len(fake.log) == 1
    assert "FAKE" not in out + err


def test_every_request_sends_the_user_agent_and_keeps_secrets_out_of_urls(db):
    plain, refreshed, limited = world(), world(), world()
    refreshed.before = lambda index, request: refreshed.valid.clear() if index == 5 else None
    limited.before = lambda index, request: 429 if index in (0, 2, 9) else None
    for fake in (plain, refreshed, limited):
        assert run(fake, db) == 0
        assert len(fake.log) >= BASE_REQUESTS
        for _, request, _ in fake.log:
            assert request.headers["user-agent"] == config.USER_AGENT
            assert "gzip" in request.headers["accept-encoding"]  # the docs reject clients without compression
            assert request.url.host == "www.deviantart.com" and request.url.scheme == "https"
            for secret in (CLIENT_ID, CLIENT_SECRET, *fake.tokens):
                assert secret not in str(request.url)
            if request.url.path != TOKEN_PATH:
                assert CLIENT_SECRET.encode() not in request.content
                assert CLIENT_SECRET not in str(request.headers)
    assert config.CONTACT_URL in config.USER_AGENT


# --- 6: mature deviations do not count ---


def test_mature_deviations_do_not_count_toward_niche_hits(db):
    fake = world()
    assert run(fake, db) == 0
    hits = {name: n for _, name, _, _, n in stored(db)}
    assert hits[DELTA] == 3  # 3 clean + 2 mature
    assert ECHO not in fake.looked_up()  # 2 clean + 5 mature never reaches the threshold
    assert CHARLIE not in fake.looked_up()  # 2 clean


def test_malformed_entries_are_skipped_and_the_run_goes_on(db, capsys):
    junk = [
        None,
        "FAKE-STRING",
        17,
        ["FAKE-LIST"],
        {},
        {"deviationid": 5, "is_mature": False, "author": {"username": "zz-fake-junk"}},
        {"deviationid": "FAKE-ID-1", "is_mature": False, "author": "zz-fake-junk"},
        {"deviationid": "FAKE-ID-2", "is_mature": False, "author": ["zz-fake-junk"]},
        {"deviationid": "FAKE-ID-3", "is_mature": False, "author": {"username": 99}},
        {"deviationid": "FAKE-ID-4", "is_mature": False, "author": {"username": ["zz-fake-junk"]}},
    ]
    good = [deviation(n, "zz-fake-good") for n in (1, 2, 3)]
    listy = [deviation(n, "zz-fake-listy") for n in (4, 5, 6)]
    profiles = {"zz-fake-good": profile("zz-fake-good", "https://zz-fake-good.art"), "zz-fake-listy": ["FAKE-LIST"]}
    fake = FakeDA({"gameart": junk * 3 + good + listy}, profiles)
    assert run(fake, db) == 0
    assert fake.looked_up() == ["zz-fake-good", "zz-fake-listy"]  # zz-fake-junk never became a candidate
    # The profile that came back as a list names no website, so that artist is stored with no link.
    assert [(name, links, hits) for _, name, _, links, hits in stored(db)] == [
        ("zz-fake-good", ["https://zz-fake-good.art/"], 3),
        ("zz-fake-listy", [], 3),
    ]
    assert capsys.readouterr().err == ""


def test_three_works_only_count_when_none_is_mature(db):
    clean = [deviation(n, "zz-fake-clean") for n in (1, 2, 3)]
    mixed = [deviation(n, "zz-fake-mixed", mature=n == 6) for n in (4, 5, 6)]
    unlabeled = [deviation(n, "zz-fake-unlabeled") for n in (7, 8, 9)]
    del unlabeled[0]["is_mature"]  # no label is not "not mature"
    names = ("zz-fake-clean", "zz-fake-mixed", "zz-fake-unlabeled")
    fake = FakeDA(
        {"gameart": clean + mixed + unlabeled}, {name: profile(name, f"https://{name}.art") for name in names}
    )
    assert run(fake, db) == 0
    assert fake.looked_up() == ["zz-fake-clean"]
    assert [(name, hits) for _, name, _, _, hits in stored(db)] == [("zz-fake-clean", 3)]


# --- the command line ---


@pytest.mark.parametrize(
    ("env", "named", "not_named"),
    [
        ({"DEVIANT_ART_ID": "FAKE-ID-VALUE-12345"}, ["DEVIANT_ART_SECRET"], ["DEVIANT_ART_ID"]),
        ({"DEVIANT_ART_SECRET": "FAKE-SECRET-VALUE-12345"}, ["DEVIANT_ART_ID"], ["DEVIANT_ART_SECRET"]),
        ({"DEVIANT_ART_ID": "", "DEVIANT_ART_SECRET": ""}, ["DEVIANT_ART_ID", "DEVIANT_ART_SECRET"], []),
        (None, ["DEVIANT_ART_ID", "DEVIANT_ART_SECRET"], []),  # no .env file at all
        (  # the names this project used before the owner chose theirs are not read
            {"DEVIANTART_CLIENT_ID": "FAKE-ID-VALUE-12345", "DEVIANTART_CLIENT_SECRET": "FAKE-SECRET-VALUE-12345"},
            ["DEVIANT_ART_ID", "DEVIANT_ART_SECRET"],
            ["DEVIANTART_CLIENT"],
        ),
    ],
)
def test_missing_credentials_exit_nonzero_naming_the_variable(monkeypatch, capsys, env, named, not_named):
    def load_env():
        if env is None:
            raise FileNotFoundError(".env")
        return env

    def must_not_run(*args, **kwargs):
        raise AssertionError("discovery started without credentials")

    monkeypatch.setattr(config, "load_env", load_env)
    monkeypatch.setattr(discover, "discover", must_not_run)
    assert discover.main([]) != 0
    out, err = capsys.readouterr()
    for name in named:
        assert name in err
    for name in not_named:
        assert name not in err
    assert "FAKE" not in out + err  # never the value of the variable that is there


def test_main_passes_credentials_and_the_limit_to_discover(monkeypatch, capsys):
    calls = []

    def fake_discover(client_id, client_secret, *, max_artists):
        calls.append((client_id, client_secret, max_artists))
        return 7

    env = {"DEVIANT_ART_ID": CLIENT_ID, "DEVIANT_ART_SECRET": CLIENT_SECRET, "OTHER": "FAKE-OTHER"}
    monkeypatch.setattr(config, "load_env", lambda: env)
    monkeypatch.setattr(discover, "discover", fake_discover)
    assert discover.main(["--max-artists", "200"]) == 7
    assert calls == [(CLIENT_ID, CLIENT_SECRET, 200)]
    out, err = capsys.readouterr()
    assert out + err == ""
