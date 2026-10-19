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
import pathlib
import re
import sqlite3
import subprocess
import sys
import threading
import time
from contextlib import closing, suppress
from datetime import UTC, datetime, timedelta
from unittest import mock
from urllib.parse import parse_qs

import httpx
import pytest

from pipeline import config, discover, removals, store

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent / "scripts"))
import gpu  # noqa: E402

CLIENT_ID = "FAKE-CLIENT-ID-0001"
CLIENT_SECRET = "FAKE-CLIENT-SECRET-0001"
TOKEN_PATH = "/oauth2/token"
BROWSE_PATH = "/api/v1/oauth2/browse/tags"

ALPHA, BRAVO, CHARLIE, DELTA = "zz-fake-alpha", "ZZ-Fake-Bravo", "zz-fake-charlie", "zz-fake-delta"
ECHO, FOXTROT, GOLF, HOTEL = "zz-fake-echo", "zz-fake-foxtrot", "zz-fake-golf", "zz-fake-hotel"
INDIA, JULIET, KILO = "zz-fake-india", "zz-fake-juliet", "zz-fake-kilo"


def expected_row(name, hits, links=()):
    return (f"da:{name.lower()}", name, f"https://www.deviantart.com/{name}", list(links), hits)


# What the base world must leave in the store: key, handle, profile URL, links, hits. Nothing else.
# Every author with three clean works under the niche tags is stored (owner decision 7), with no link: discovery
# reads no profile, so it knows no site. CHARLIE and ECHO have two clean works each, and are not.
EXPECTED = [
    expected_row(ALPHA, 4),
    expected_row(BRAVO, 3),
    expected_row(DELTA, 3),
    expected_row(FOXTROT, 3),
    expected_row(GOLF, 3),
    expected_row(HOTEL, 5),
    expected_row(INDIA, 3),
    expected_row(JULIET, 3),
    expected_row(KILO, 3),
]
# The browsing pass as the summary prints it, and after it what the pace did in a run that met no trouble.
EXPECTED_TALLY = """\
pages fetched: 19
deviations counted: 94
mature skipped: 7
authors seen: 71
candidates (>= 3 hits): 9
skipped (removed): 0
not reached (--max-artists): 0
artists stored: 9
"""
EXPECTED_STDOUT = (
    EXPECTED_TALLY
    + """\
artists removed by the removal checks made during the run: 0
removal checks made during the run that failed: 0
requests to the API: 20
rate limit answers from the API: 0
server errors or no answer from the API: 0
longest interval between requests to the API, in seconds: 2
interval between requests to the API now, in seconds: 2
seconds spent waiting between requests to the API: 38
"""
)
# The base world's requests in order: 0 the token, 1 and 2 two pages of the first tag, 3 the second tag, and one
# page of each of the other sixteen. The fifth tag, request 6, is the one with most of the artists under it.
BASE_REQUESTS = 20
GAMEART = 6
LONG_AGO = "2020-01-01T00:00:00+00:00"
KEPT = "Everything stored is kept: run again later"
WAITS = "60 minutes of waits, each 5 minutes or as long as it asked"  # an hour of them stops a run: see Pace
LIMITED = f"stopped: rate limited: the API kept refusing through {WAITS}. {KEPT}\n"


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


class FakeDA:
    """A made-up DeviantArt behind httpx.MockTransport, with a clock that only its sleep() moves."""

    def __init__(self, by_tag):
        self.by_tag = by_tag
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
        # (mature_content, expand, with_session) is answered 400, so a test notices it. Browsing is all there is:
        # a request for anything else, a profile say, is answered 404, and every test would see it in the log.
        if path != BROWSE_PATH:
            return httpx.Response(404, json={})
        if sorted(request.url.params.keys()) != ["limit", "offset", "tag"]:
            return httpx.Response(400, json={"error": "invalid_request", "error_description": "FAKE-ERROR-TEXT"})
        tag = request.url.params["tag"]
        offset, limit = int(request.url.params["offset"]), int(request.url.params["limit"])
        if not (1 <= limit <= 50 and 0 <= offset <= 50000):  # the documented ranges
            return httpx.Response(400, json={"error": "invalid_request", "error_description": "FAKE-ERROR-TEXT"})
        works = self.by_tag.get(tag, [])
        end = offset + min(limit, self.page_size)
        page = {"has_more": end < len(works), "next_offset": end if end < len(works) else None}
        page |= {"results": works[offset:end]} | self.page_patch.get((tag, offset), {})
        return httpx.Response(200, json=page)

    def paths(self):
        return [request.url.path for _, request, _ in self.log]

    def browsed(self):
        """The pages the client asked for, in order, as (tag, offset)."""
        browse = [request.url.params for _, request, _ in self.log if request.url.path == BROWSE_PATH]
        return [(params["tag"], int(params["offset"])) for params in browse]

    def offsets(self, tag):
        """The offsets the client asked for under one tag, in order."""
        return [offset for asked, offset in self.browsed() if asked == tag]

    def gaps(self):
        times = [clock for clock, _, _ in self.log]
        return [later - earlier for earlier, later in itertools.pairwise(times)]


def world(more=None):
    """The base world. See EXPECTED and EXPECTED_STDOUT for what a correct run makes of it. `more` is works to
    add under a tag of its own, the last one browsed."""
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
        discover.NICHE_TAGS[-1]: [deviation(1000 + n, name) for n, name in enumerate(more or [])],
    }
    return FakeDA(by_tag)


def age_the_pass(db, age=None):
    """Make the saved browsing pass as old as this, or far older than discovery carries one on from."""
    began = LONG_AGO if age is None else (datetime.now(UTC) - age).isoformat(timespec="seconds")
    with closing(sqlite3.connect(db)) as conn, conn:
        if conn.execute("SELECT 1 FROM sqlite_master WHERE name = 'browsed'").fetchone():  # no run, no table
            conn.execute("UPDATE browsed SET began_at = ?", (began,))


def run(fake, db, max_artists=200, afresh=True):
    """One run. Unless it is to carry on the pass that is saved, that pass is made too old to carry on first, so
    that the run browses afresh: most tests here are about what a whole pass does."""
    if afresh and db.exists():
        age_the_pass(db)
    with mock.patch.object(time, "sleep", fake.sleep):
        return discover.discover(CLIENT_ID, CLIENT_SECRET, max_artists=max_artists, db=db, transport=fake.transport)


def summary(out):
    """The printed counts as a dict. It also checks that every line is a label and a number, and nothing more."""
    assert all(re.fullmatch(r"[A-Za-z ,()>=0-9-]+: \d+", line) for line in out.splitlines()), out
    counts = {label: int(count) for label, count in (line.rsplit(": ", 1) for line in out.splitlines())}
    assert list(counts) == list(discover._COUNTS)
    parts = ("skipped (removed)", discover.NOT_REACHED, "artists stored")
    assert sum(counts[part] for part in parts) == counts["candidates (>= 3 hits)"]  # the summary adds up
    return counts


def all_rows(db, table="artists"):
    if not db.exists():
        return []
    with closing(sqlite3.connect(db)) as conn:
        return conn.execute(f"SELECT * FROM {table} ORDER BY 1").fetchall()


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


def sha(text):
    return hashlib.sha256(text.encode()).hexdigest()


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
    # The removal checks a pass makes between its pages (look_between) find nothing due here: they are tested apart.
    monkeypatch.setattr(removals, "run", lambda counts, *args, **kwargs: None)


@pytest.fixture(autouse=True)
def nothing_logged(caplog, recwarn):
    """In every test: no log record and no warning may carry made-up DeviantArt data, a handle or a credential.

    Every made-up string in this file has "fake" in it, so that word, in any case, in any record of any logger at
    any level, fails the test.
    """
    caplog.set_level(logging.DEBUG)  # on the root logger, which every logger that propagates reaches
    yield
    said = [logging.Formatter().format(record) + str(record.args) for record in caplog.get_records("call")]
    said += [str(warning.message) for warning in recwarn.list]
    assert [text for text in said if "fake" in text.lower()] == []


@pytest.fixture
def db(tmp_path):
    return tmp_path / "store" / "pipeline.sqlite"


# --- 1: what a run stores: handles and counts, no DeviantArt content, and no profile is read ---


def test_base_run_stores_exactly_the_expected_rows(db):
    fake = world()
    assert run(fake, db) == 0
    assert stored(db) == EXPECTED
    assert len(fake.log) == BASE_REQUESTS


def test_every_candidate_is_stored_straight_from_the_tally_and_no_profile_is_ever_asked_for(db):
    """Discovery asked for some 3,500 profiles a run, only to read the website field, which the collect stage reads
    again on the artist's turn. Three clean works under the niche tags are what it takes (owner decision 7)."""
    fake = world()
    assert run(fake, db) == 0
    assert set(fake.paths()) == {TOKEN_PATH, BROWSE_PATH} and fake.paths().count(TOKEN_PATH) == 1
    assert [links for *_, links, _ in stored(db)] == [[]] * 9  # no profile read, so no site known
    assert store.all_artists(db) == [(key, name, url) for key, name, url, _, _ in EXPECTED]  # for the collect stage
    assert store.own_site_candidates(db) == []  # for the crawl stage: nobody, until collection has read a profile
    for never in (CHARLIE, ECHO):  # two works; two clean works
        assert f"da:{never}" not in [key for key, *_ in stored(db)]


def test_a_later_pass_leaves_the_link_the_collect_stage_wrote_into_a_row(db):
    """Discovery used to write each row's link from the profile it read. It reads none now, and must not blank
    the own site that collection keeps in the row, which is where the crawl stage looks."""
    assert run(world(), db) == 0
    assert store.keep_own_site(ALPHA, "https://zz-fake-alpha.art/", db) is True
    assert run(world(more=[ALPHA]), db) == 0  # a later pass, in which ALPHA has one work more
    assert stored(db)[0] == expected_row(ALPHA, 5, ["https://zz-fake-alpha.art/"])
    assert stored(db)[1:] == EXPECTED[1:]
    assert store.mark_removed("site:zz-fake-alpha.art", db) == 0  # it bars nobody, and holds nobody back
    assert f"da:{ALPHA}" in [key for key, _, _ in store.all_artists(db)] and store.own_site_candidates(db) == []


def test_a_removed_artist_is_in_neither_list_handed_to_the_later_stages(db):
    assert run(world(), db) == 0
    assert store.keep_own_site(ALPHA, "https://zz-fake-alpha.art/", db)  # as the collect stage does
    assert store.keep_own_site(BRAVO, "http://www.zz-fake-bravo.com/", db)
    store.mark_removed(f"da:{FOXTROT}", db)  # stored with no link
    store.mark_removed("site:zz-fake-alpha.art", db)  # stored with a link: the site goes, and they stay
    left = [key for key, *_ in without("da:zz-fake-foxtrot")]
    assert [key for key, _, _ in store.all_artists(db)] == left
    assert store.own_site_candidates(db) == [("da:zz-fake-bravo", "http://www.zz-fake-bravo.com/")]
    assert run(world(), db) == 0  # and a later pass brings neither back
    assert [key for key, _, _ in store.all_artists(db)] == left


def test_store_holds_no_deviantart_content(db):
    fake = world()
    assert run(fake, db) == 0
    allowed = {text for row in EXPECTED for text in strings(list(row))}
    forbidden = {text for text in strings(fake.by_tag) if not any(text in ok for ok in allowed)}
    # The check is not hollow: ids, titles, image URLs and the handles of authors we did not keep are all in it.
    assert len(forbidden) > 900
    assert {"FAKE-TITLE-1", "FAKE-TAG-1", "FAKE-EXCERPT-1", "FAKE-DESCRIPTION-1", ECHO, CHARLIE} <= forbidden
    assert {"00000000-0000-4000-8000-000000000001", "zz-fake-filler-00", f"FAKE-USERID-{ALPHA}"} <= forbidden
    assert "https://images.fake-cdn.invalid/FAKE-THUMB-1.jpg" in forbidden
    disk = everything_on_disk(db)
    assert [text for text in forbidden if text in disk] == []
    for text in ("FAKE", "fake-cdn", "/art/", "@", "?", "#"):
        assert text not in "\n".join(str(row) for row in all_rows(db))
    for text in ("FAKE", "fake-cdn", "/art/", "0000-4000", "filler", "charlie", "echo"):
        assert text not in disk
    for text in allowed:  # and the scan can see what is stored
        assert text in disk


def test_the_saved_pass_is_tags_offsets_and_hashes_that_name_nobody(db):
    """What may be saved of a browsing pass, which earlier critics pinned: no title, description or text, and no
    handle of anybody who posted once or twice under a tag. A hash is enough to count them."""
    assert run(world(), db) == 0
    with closing(sqlite3.connect(db)) as conn:
        tables = {name for (name,) in conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'")}
        columns = [row[1] for row in conn.execute("PRAGMA table_info(artists)")]
    assert tables == {"artists", "removed", "listed", "browsed", "counted"}
    assert columns == ["key", "da_username", "da_profile_url", "links", "niche_hits", "discovered_at"]
    counted = all_rows(db, "counted")
    assert len(counted) == 101 and len([author for _, author in counted if author is None]) == 7  # the mature ones
    assert all(re.fullmatch(r"[0-9a-f]{64}", value) for row in counted for value in row if value is not None)
    assert (sha("work:00000000-0000-4000-8000-000000000001"), sha(f"da:{ALPHA}")) in counted
    assert len([author for _, author in counted if author == sha(f"da:{CHARLIE}")]) == 2  # counted, and not named
    browsed = {tag: (next_offset, pages) for tag, next_offset, pages, _ in all_rows(db, "browsed")}
    assert browsed == {tag: (None, 2 if tag == "conceptart" else 1) for tag in discover.NICHE_TAGS}
    assert all(datetime.fromisoformat(began).utcoffset() == timedelta(0) for *_, began in all_rows(db, "browsed"))
    assert all_rows(db, "removed") == all_rows(db, "listed") == []


def test_output_is_counts_only(db, capfd):
    fake = world()
    assert run(fake, db) == 0
    out, err = capfd.readouterr()  # the file descriptors themselves, so a write that bypasses sys.stdout shows too
    assert out.replace("\r\n", "\n") == EXPECTED_STDOUT
    assert err == ""
    printed = (out + err).lower()
    secrets = [CLIENT_ID, CLIENT_SECRET, *fake.tokens]
    for text in [*strings(fake.by_tag), *secrets]:  # handles included
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
    unsafe = ["../../oauth2/placebo", "FAKE TITLE WITH SPACES", "zz-fake/x", "zz-fake?x=1", "", "é" * 5, "z" * 41]
    works = [deviation(n, name) for n, name in enumerate(unsafe * 3, 1)]  # 3 works each: enough to be a candidate
    works += [deviation(n, "zz-fake-orphan") | {"author": None} for n in (101, 102, 103)]  # no author at all
    fake = FakeDA({"conceptart": works})
    assert run(fake, db) == 0
    assert len(fake.log) == 1 + len(discover.NICHE_TAGS)  # the token and one page per tag
    assert all_rows(db) == [] and all_rows(db, "counted") == []  # not stored, and not counted either


# --- 2: a second run makes no duplicates, and browsing survives a stop ---


def test_rerun_creates_no_duplicates(db):
    assert run(world(), db) == 0
    with closing(sqlite3.connect(db)) as conn, conn:
        conn.execute("UPDATE artists SET discovered_at = '2020-01-01T00:00:00+00:00'")  # so a rewrite would show
    first = all_rows(db)
    assert len(first) == len(EXPECTED)
    assert run(world(), db) == 0  # a later pass
    assert run(world(), db, afresh=False) == 0  # and a run on the pass that is saved
    assert all_rows(db) == first and len(all_rows(db, "counted")) == 101
    with closing(sqlite3.connect(db)) as conn:
        assert conn.execute("SELECT count(*), count(DISTINCT lower(da_username)) FROM artists").fetchone() == (9, 9)


def test_a_finished_pass_is_carried_on_from_for_a_week_and_then_a_run_browses_afresh(db, capsys):
    """How long saved progress stays valid: PASS_VALID from the pass's first page. Until then a run on a finished
    pass has nothing to browse and makes no request. After it, the pass is forgotten, tally and all."""
    assert discover.PASS_VALID == timedelta(days=7)
    assert run(world(), db) == 0
    capsys.readouterr()
    for age in (timedelta(0), timedelta(days=6, hours=23)):
        age_the_pass(db, age)
        again = world()
        assert run(again, db, afresh=False) == 0 and again.log == []  # not even a token
        out = capsys.readouterr().out
        assert out.startswith(EXPECTED_TALLY) and summary(out)["requests to the API"] == 0  # the pass, as saved
    age_the_pass(db, timedelta(days=7, hours=1))
    later = world()
    del later.by_tag["conceptart"]  # by now BRAVO has taken their works down, and ALPHA all but one
    assert run(later, db, afresh=False) == 0 and len(later.log) == BASE_REQUESTS - 1
    counts = summary(capsys.readouterr().out)
    # The old tally is gone, not added to: ALPHA would have five works otherwise, and still be a candidate.
    assert (counts["pages fetched"], counts["deviations counted"], counts["candidates (>= 3 hits)"]) == (18, 27, 7)
    assert len(all_rows(db, "counted")) == 34
    assert stored(db) == EXPECTED  # and discovery never takes anybody out of the store


def test_max_artists_caps_the_artists_a_run_adds_and_the_next_run_stores_the_rest(db, capsys):
    first = world()
    assert run(first, db, max_artists=1) == 0
    assert stored(db) == [expected_row(ALPHA, 3)]  # the first to have works enough, on the first page
    assert first.browsed() == [("conceptart", 0)]  # and browsing stops there
    counts = summary(capsys.readouterr().out)
    assert (counts["candidates (>= 3 hits)"], counts["artists stored"], counts[discover.NOT_REACHED]) == (2, 1, 1)
    second = world()
    assert run(second, db, max_artists=4, afresh=False) == 0  # four more: a cap on what is added, not on the store
    assert [name for _, name, *_ in stored(db)] == [ALPHA, BRAVO, DELTA, FOXTROT, GOLF]
    # The page the cap was reached on is read again, so that nobody on it is lost; no other page is.
    assert second.browsed()[:3] == [("conceptart", 0), ("conceptart", 40), ("characterdesign", 0)]
    assert second.browsed()[-1] == ("gameart", 0)
    counts = summary(capsys.readouterr().out)
    assert (counts["candidates (>= 3 hits)"], counts["artists stored"], counts[discover.NOT_REACHED]) == (9, 5, 4)
    third = world()
    assert run(third, db, afresh=False) == 0
    assert third.browsed()[0] == ("gameart", 0) and len(third.browsed()) == 14
    assert stored(db) == EXPECTED and capsys.readouterr().out.startswith(EXPECTED_TALLY)
    none = world()
    assert run(none, db, max_artists=0) == 0 and stored(db) == EXPECTED  # nobody new, so nothing to cap


@pytest.mark.parametrize("stopped_by", ["the API", "a person"])
def test_a_run_that_is_stopped_while_browsing_keeps_what_it_stored_and_the_next_run_carries_on(db, capsys, stopped_by):
    """A run of 2,160 browse pages was stopped by a 504 after 202 of them and had stored nothing: nothing was
    stored until browsing ended, and the rerun began again from nothing. Progress is saved page by page now, and
    an artist reaches the store on the page that gives them works enough."""
    stopped = world()
    if stopped_by == "the API":
        stopped.before = lambda index, request: 504 if request.url.params.get("tag") == "gameart" else None
        assert run(stopped, db) == 1
        assert capsys.readouterr().err == "stopped: browse failed with HTTP 504\n"
        assert stopped.browsed()[-4:] == [("gameart", 0)] * 4  # asked for again three times, then the run stops
    else:

        def interrupt(index, request):
            if request.url.params.get("tag") == "gameart":
                raise KeyboardInterrupt

        stopped.before = interrupt
        with pytest.raises(KeyboardInterrupt):
            run(stopped, db)
    assert stored(db) == EXPECTED[:3]  # ALPHA, BRAVO and DELTA: stored already, not all at the end
    assert not (config.CACHE_DIR / "discover.status").exists()
    again = world()
    assert run(again, db, afresh=False) == 0
    assert again.browsed() == [(tag, 0) for tag in discover.NICHE_TAGS[4:]]  # from the page it stopped at, and on
    assert len(again.log) == 1 + 14
    assert stored(db) == EXPECTED  # ALPHA with four works: the one filed under two tags was not counted again
    assert capsys.readouterr().out.startswith(EXPECTED_TALLY)  # the whole pass, whichever runs made it


def test_a_work_counted_before_a_stop_is_not_counted_again_after_it(db):
    """ALPHA's fourth work is filed under the first tag and the second. The run stops between the two."""
    stopped = world()
    stopped.before = lambda index, request: 500 if request.url.params.get("tag") == "characterdesign" else None
    assert run(stopped, db) == 1
    assert stored(db) == [expected_row(ALPHA, 3), expected_row(BRAVO, 3)]
    assert run(world(), db, afresh=False) == 0
    assert stored(db) == EXPECTED


def test_the_niche_is_eighteen_tags_each_browsed_a_hundred_and_twenty_pages_deep(db, monkeypatch):
    """Round 8: six tags at 40 pages found about 400 artists, and the owner needs some 3,500 found. The tags are
    specific to concept and character art for games: a broad one would let in anybody."""
    first_six = ("conceptart", "characterdesign", "characterconcept", "creaturedesign", "gameart", "environmentconcept")
    assert discover.NICHE_TAGS[:6] == first_six
    assert len(discover.NICHE_TAGS) == len(set(discover.NICHE_TAGS)) == 18 and discover.PAGES_PER_TAG == 120
    assert all(re.fullmatch(r"[a-z]{7,20}", tag) for tag in discover.NICHE_TAGS)
    assert not {"digitalart", "fantasy", "art", "drawing", "illustration", "fanart"} & set(discover.NICHE_TAGS)
    assert discover.PAGES_PER_TAG * discover.PAGE_SIZE <= discover.MAX_OFFSET
    last = discover.NICHE_TAGS[-1]
    fake = FakeDA({last: [deviation(n, ALPHA) for n in range(1, 201)]})
    fake.page_size = 1  # 200 pages of one work each under the last tag: 120 of them are read, and no more
    assert run(fake, db) == 0
    assert fake.offsets(last) == list(range(120))
    assert stored(db) == [expected_row(ALPHA, 120)]
    monkeypatch.setattr(discover, "PAGES_PER_TAG", 125)  # browsed deeper, the saved pass is carried on from there
    deeper = FakeDA(fake.by_tag)
    deeper.page_size = 1
    assert run(deeper, db, afresh=False) == 0
    assert deeper.browsed() == [(last, offset) for offset in range(120, 125)]
    assert stored(db) == [expected_row(ALPHA, 125)]


# --- 3: removed artists are never stored again (hard rule 7) ---


def test_a_removed_artist_is_never_stored_again_however_often_they_are_browsed(db, capsys):
    assert run(world(), db) == 0
    capsys.readouterr()
    assert store.mark_removed("da:ZZ-FAKE-ALPHA", db) == 1  # another case than the API uses
    for _ in range(2):
        assert run(world(), db) == 0
        assert stored(db) == without("da:zz-fake-alpha")
        counts = summary(capsys.readouterr().out)
        assert (counts["skipped (removed)"], counts["artists stored"]) == (1, 8)


@pytest.mark.parametrize("handle", [ALPHA, HOTEL, FOXTROT, JULIET, KILO])
@pytest.mark.parametrize("how", ["mark_removed", "import_removed"])
def test_a_removed_handle_never_seen_here_is_never_stored_and_bars_only_itself(db, how, handle):
    """Round 7. A removal of a handle bars the handle, and nobody else."""
    key_hash = sha(f"da:{handle.lower()}")
    if how == "mark_removed":
        assert store.mark_removed(f"da:{handle.upper()}", db) == 0  # nothing is known about them here
    else:  # the removal arrives from the list of record, into a fresh cache
        assert store.import_removed([key_hash], db) == 0
    for _ in range(2):
        assert run(world(), db) == 0
        assert stored(db) == without(f"da:{handle}")
    assert all_rows(db, "removed") == [(key_hash, mock.ANY)]  # the handle, and nothing else
    assert handle.lower() not in everything_on_disk(db).lower()


def test_artist_removed_after_discovery_is_gone_and_never_readded(db):
    assert run(world(), db) == 0
    assert stored(db) == EXPECTED
    store.mark_removed(f"https://www.deviantart.com/{BRAVO}", db)  # the way an artist would send it
    assert stored(db) == without("da:zz-fake-bravo")
    for _ in range(2):
        assert run(world(), db) == 0
        assert stored(db) == without("da:zz-fake-bravo")
    assert "bravo" not in everything_on_disk(db).lower()  # in the store, or in what is saved of the pass


def test_a_removal_in_the_middle_of_a_pass_leaves_no_row_that_names_the_artist_and_they_do_not_come_back(db, capsys):
    """What is saved of a pass must outlive a removal without naming anybody: an artist who is removed between a
    stop and the run that carries on is in no table by name, and more works of theirs do not bring them back."""
    stopped = world()
    stopped.before = lambda index, request: 500 if request.url.params.get("tag") == "gameart" else None
    assert run(stopped, db) == 1
    assert stored(db) == EXPECTED[:3]
    assert store.mark_removed(ALPHA, db) == 1
    assert "alpha" not in everything_on_disk(db).lower()
    capsys.readouterr()
    assert run(world(more=[ALPHA] * 3), db, afresh=False) == 0  # three more works of theirs, further on in the pass
    assert stored(db) == without("da:zz-fake-alpha") and "alpha" not in everything_on_disk(db).lower()
    counts = summary(capsys.readouterr().out)
    assert (counts["candidates (>= 3 hits)"], counts["skipped (removed)"], counts["artists stored"]) == (9, 1, 8)


def test_a_removed_site_holds_back_none_of_the_artists_whose_stored_link_it_is_and_bars_nobody(db):
    """The final critic's: a site is anybody's to name, so its removal removes nobody. The fifth critic's: nor does
    it hold anybody back, or an artist who changes their website stays out for good. Whoever links to it is handed
    on like anybody; their handles are not barred, and the site is nobody's to crawl."""
    studio = "https://zz-fake-studio.art/"
    assert run(world(), db) == 0
    for name, site in ((ALPHA, studio), (BRAVO, "http://www.zz-fake-studio.art/")):
        assert store.keep_own_site(name, site, db)  # as the collect stage does, once it has read their profile
    assert store.mark_removed("site:zz-fake-studio.art", db) == 0
    left = [key for key, *_ in EXPECTED]
    assert [key for key, _, _ in store.all_artists(db)] == left and store.own_site_candidates(db) == []
    assert not store.is_removed(f"da:{ALPHA}", db) and not store.is_removed(f"da:{BRAVO}", db)
    assert run(world(), db) == 0  # a later pass leaves the link, and it holds them back no more than before
    assert [key for key, _, _ in store.all_artists(db)] == left


def test_an_artist_whose_site_was_removed_before_they_were_ever_stored_is_known_here_by_handle_alone(db):
    """Discovery cannot know whose site a removed one is: it reads no profile. The artist is stored as a handle
    with no link. The removed site is written into nobody's row, and the collect stage, which reads the profile,
    collects them like anybody and keeps it out of their row (see its tests)."""
    store.mark_removed("site:www.zz-fake-delta.co.uk", db)
    assert run(world(), db) == 0
    assert stored(db) == EXPECTED and not store.is_removed(f"da:{DELTA}", db)
    assert store.keep_own_site(DELTA, "https://zz-fake-delta.co.uk/", db) is False
    assert stored(db) == EXPECTED and "co.uk" not in everything_on_disk(db)


def test_the_removal_list_of_record_keeps_a_removed_artist_out_of_a_fresh_cache(db):
    assert run(world(), db) == 0
    assert store.keep_own_site(DELTA, "https://zz-fake-delta.co.uk/", db)
    store.mark_removed(f"da:{ALPHA}", db)
    store.mark_removed("site:zz-fake-delta.co.uk", db)
    of_record = [key_hash for key_hash, _ in all_rows(db, "removed")]
    db.unlink()  # the local store is a cache: gone
    assert store.import_removed(of_record, db) == 0
    for _ in range(2):  # DELTA's site bars nobody: they are a bare handle again, whom the collect stage holds
        assert run(world(), db) == 0
        assert stored(db) == without("da:zz-fake-alpha")


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


# --- 4: the pace: one interval for the whole run, which slows down when the API asks ---


def test_requests_are_sequential_and_at_least_the_base_interval_apart(db):
    fake = world()
    assert run(fake, db) == 0
    assert discover.BASE_INTERVAL == 2.0  # half the rate that got us limited after most of a day
    assert fake.gaps() == [2.0] * (BASE_REQUESTS - 1)  # the token request and the browse pages alike
    assert len(fake.threads) == 1  # one thread, one request at a time


def test_the_wait_is_time_sleep_itself_and_no_caller_can_switch_it_off(db, monkeypatch):
    """The production path, with nothing injected: only time.sleep is replaced, so nothing really waits."""
    waits = []
    monkeypatch.setattr(time, "sleep", waits.append)
    fake = world()
    assert discover.discover(CLIENT_ID, CLIENT_SECRET, max_artists=200, db=db, transport=fake.transport) == 0
    assert len(fake.log) == BASE_REQUESTS
    assert waits == [2.0] * (BASE_REQUESTS - 1)  # one wait before every request but the first
    assert "sleep" not in inspect.signature(discover.discover).parameters


def limited(fake, indexes, headers=None):
    """Make the fake answer 429 to the requests with these indexes, counted over everything it is sent."""

    def before(index, request):
        if index in indexes:
            return httpx.Response(429, json={"error": "FAKE-RATE-LIMIT"}, headers=headers or {})

    fake.before = before


def test_after_a_429_the_next_different_request_waits_longer_than_the_base_too(db, capsys):
    """The flaw this replaces: a 429 was backed off from for that one request, 1, 2, 4 seconds and so on, and
    after a success the client went straight back to its fixed rate. The API docs call a 429 "a request to slow
    down" and say that a client which keeps to its rate stays limited for longer. Four hours of 429s followed."""
    fake = world()
    limited(fake, {3})
    assert run(fake, db) == 0
    assert [status for _, _, status in fake.log[3:5]] == [429, 200]
    assert fake.log[3][1].url == fake.log[4][1].url != fake.log[5][1].url  # asked again, and then the next page
    assert fake.gaps()[2:6] == pytest.approx([2.0, 4.0, 3.6, 3.24])  # the gaps before requests 3 to 6
    assert fake.gaps()[4] > discover.BASE_INTERVAL  # the one before the next different request
    assert stored(db) == EXPECTED and capsys.readouterr().out.startswith(EXPECTED_TALLY)  # nothing counted twice


def test_a_burst_of_429s_doubles_the_interval_each_time_and_it_comes_down_a_tenth_at_a_time(db, capsys):
    fake = world()
    limited(fake, {3, 4, 5})
    assert run(fake, db) == 0
    assert [status for _, _, status in fake.log[3:7]] == [429, 429, 429, 200]
    assert len({str(request.url) for _, request, _ in fake.log[3:7]}) == 1  # the same request, asked again
    eased = [16.0 * 0.9**n for n in range(1, 17)]  # 14.4, 12.96, 11.66 and so on: sixteen more requests
    assert fake.gaps() == pytest.approx([2.0, 2.0, 2.0, 4.0, 8.0, 16.0, *eased])
    assert len(fake.log) == BASE_REQUESTS + 3 and stored(db) == EXPECTED
    counts = summary(capsys.readouterr().out)  # and the summary counts what happened
    assert counts["requests to the API"] == 23 and counts["rate limit answers from the API"] == 3
    assert counts["longest interval between requests to the API, in seconds"] == 16
    assert counts["interval between requests to the API now, in seconds"] == 3  # 16 x 0.9^17 is 2.67
    assert counts["seconds spent waiting between requests to the API"] == round(sum(fake.gaps())) == 151


def test_429s_that_stop_after_a_while_let_the_interval_back_down_to_the_base_and_never_below_it(db):
    fake = world()
    fake.page_size = 2  # 66 pages, so that the run is long enough to see the interval come all the way down
    limited(fake, {1, 2})
    assert run(fake, db) == 0
    gaps = fake.gaps()
    assert gaps[:3] == [2.0, 4.0, 8.0] and len(gaps) == 66 + 2
    for earlier, later in itertools.pairwise(gaps[2:]):
        assert later == pytest.approx(max(discover.BASE_INTERVAL, earlier * discover.EASE))  # a tenth off, a success
    assert gaps[2:16] == sorted(gaps[2:16], reverse=True) and gaps[15] > 2.0  # fourteen requests to come down
    assert set(gaps[16:]) == {2.0} and min(gaps) == 2.0
    assert stored(db) == EXPECTED


@pytest.mark.parametrize("first_429", [0, 4])  # the token request, and the fourth browse page
def test_a_long_stretch_of_429s_raises_the_interval_to_the_ceiling_and_an_hour_there_stops_the_run(
    db, capsys, first_429
):
    fake = world()
    fake.before = lambda index, request: 429 if index >= first_429 else None
    assert run(fake, db) == 1
    out, err = capsys.readouterr()
    assert err == LIMITED and discover.MAX_INTERVAL == 300.0 and discover.MAX_AT_CEILING == 12
    refused = fake.log[first_429:]
    assert len(refused) == 20 and {status for _, _, status in refused} == {429}  # and nothing was sent afterwards
    assert len({str(request.url) for _, request, _ in refused}) == 1
    climb = [4.0, 8.0, 16.0, 32.0, 64.0, 128.0, 256.0]
    assert fake.gaps()[first_429:] == climb + [300.0] * 12  # an hour at the ceiling, after eight minutes of climbing
    counts = summary(out)
    assert counts["rate limit answers from the API"] == 20
    assert counts["longest interval between requests to the API, in seconds"] == 300
    assert counts["seconds spent waiting between requests to the API"] == 2 * first_429 + 508 + 3600
    assert "FAKE" not in out + err
    # Everything stored before the stop is kept, and so is the place: the next run carries on from the same page.
    assert stored(db) == ([expected_row(ALPHA, 4), expected_row(BRAVO, 3), expected_row(DELTA, 3)] if first_429 else [])
    again = world()
    assert run(again, db, afresh=False) == 0 and stored(db) == EXPECTED
    assert again.browsed()[0] == (discover.NICHE_TAGS[2 if first_429 else 0], 0)


def test_429s_at_the_ceiling_that_stop_short_of_an_hour_do_not_stop_the_run(db, capsys):
    fake = world()
    limited(fake, set(range(2, 21)))  # nineteen in a row: eleven of them at the ceiling
    assert run(fake, db) == 0 and capsys.readouterr().err == ""
    assert fake.gaps()[9:22] == [300.0] * 12 + [270.0]  # and from the first success the interval comes down
    assert stored(db) == EXPECTED


@pytest.mark.parametrize(
    ("asked", "waited"),
    [
        ("100", 100.0),  # longer than the doubled interval: honoured
        ("3600", 3600.0),  # an hour is the most that is waited for
        ("3", 4.0),  # shorter: the interval is what is waited
        ("0", 4.0),
        ("-5", 4.0),  # not a number of seconds
        ("1e3", 4.0),
        ("soon", 4.0),
        ("٣٠٠", 4.0),  # digits, and not ASCII ones
        ("Wed, 21 Oct 2026 07:28:00 GMT", 4.0),  # a date is not read: see the ponytail note in Pace.slow
    ],
)
def test_a_retry_after_is_honoured_when_it_asks_for_longer_and_only_once(db, asked, waited):
    fake = world()
    limited(fake, {3}, {"Retry-After": asked.encode()})
    assert run(fake, db) == 0
    assert fake.gaps()[2:6] == pytest.approx([2.0, waited, 3.6, 3.24])  # the interval itself goes on as it would
    assert stored(db) == EXPECTED


def test_a_retry_after_of_more_than_an_hour_stops_the_run_and_what_is_stored_is_kept(db, capsys):
    fake = world()
    limited(fake, {4}, {"Retry-After": "3601"})
    assert run(fake, db) == 1
    said = "the API asks for a wait of 3601 seconds, more than a run waits"  # and it says how long that was
    assert capsys.readouterr().err == f"stopped: rate limited: {said}. {KEPT}\n"
    assert len(fake.log) == 5 and len(stored(db)) == 3


def test_a_5xx_is_asked_for_again_three_times_and_slows_the_pace_as_a_429_does(db, capsys):
    """A 503 is "try again later", and the docs allow a 500 three retries. One rule for every 5XX: it is asked
    for again SERVER_RETRIES times, and each one doubles the interval for everything that follows. It never
    counts towards the hour that stops a run: what is left after the retries is the caller's to judge."""
    assert discover.SERVER_RETRIES == 3
    fake, failed = world(), []

    def twice(index, request):  # the first two requests for the second tag get a 503; the third is answered
        if request.url.params.get("tag") == "characterdesign" and len(failed) < 2:
            failed.append(index)
            return 503

    fake.before = twice
    assert run(fake, db) == 0
    out, err = capsys.readouterr()
    assert err == "" and stored(db) == EXPECTED and len(fake.log) == BASE_REQUESTS + 2
    assert fake.gaps()[2:7] == pytest.approx([2.0, 4.0, 8.0, 7.2, 6.48])  # before requests 3 to 7
    counts = summary(out)
    assert counts["rate limit answers from the API"] == 0
    assert counts["longest interval between requests to the API, in seconds"] == 8


@pytest.mark.parametrize("status", [500, 502, 503, 504])
def test_a_5xx_that_outlasts_its_retries_stops_the_run_and_the_next_run_carries_on(db, capsys, status):
    fake = world()
    fake.before = lambda index, request: status if index >= 4 else None
    assert run(fake, db) == 1
    assert capsys.readouterr().err == f"stopped: browse failed with HTTP {status}\n"
    assert len(fake.log) == 4 + 1 + discover.SERVER_RETRIES and fake.gaps()[3:] == [2.0, 4.0, 8.0, 16.0]
    assert len(stored(db)) == 3
    assert run(world(), db, afresh=False) == 0 and stored(db) == EXPECTED


def test_a_server_error_on_the_token_request_is_asked_for_again_too(db, capsys):
    fake = world()
    fake.before = lambda index, request: 503 if index < 2 else None
    assert run(fake, db) == 0 and stored(db) == EXPECTED
    down = world()
    down.before = lambda index, request: 503
    assert run(down, db) == 1 and len(down.log) == 4
    assert capsys.readouterr().err == "stopped: token request failed with HTTP 503\n"


def test_the_http_library_logs_nothing_during_a_run_and_is_left_as_it_was(db, caplog):
    """httpx logs every request URL at INFO, and a request's URL can hold a handle."""
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


def test_pages_stop_at_the_depth_limit(db, tmp_path, monkeypatch):
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
    assert run(shallow, tmp_path / "another" / "pipeline.sqlite") == 0
    assert shallow.browsed() == [(tag, 0) for tag in discover.NICHE_TAGS]


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
    assert len(fake.browsed()) == len(discover.NICHE_TAGS)  # and it went on to the other tags
    again = world()  # and the tag is noted as having no more: the saved pass is not carried on into it
    assert run(again, db, afresh=False) == 0 and again.log == []
    assert ("conceptart", None, 1) in [row[:3] for row in all_rows(db, "browsed")]


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
    ],
)
def test_redirects_are_not_followed(db, capsys, index, status, target, reason):
    fake = world()
    fake.before = lambda i, request: httpx.Response(status, headers={"location": target}) if i == index else None
    assert run(fake, db) == 1
    assert len(fake.log) == index + 1  # nothing was sent after the redirect: not to its target, not anywhere
    assert {request.url.host for _, request, _ in fake.log} == {"www.deviantart.com"}
    assert capsys.readouterr().err == f"stopped: {reason} failed with HTTP {status}\n"


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
    assert set(fake.gaps()) == {discover.BASE_INTERVAL}  # a 401 is an answer: it does not slow the pace
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


def test_wrong_credentials_stop_the_run_without_printing_them(db, capsys):
    fake = world()
    status = discover.discover("FAKE-WRONG-ID", "FAKE-WRONG-SECRET", max_artists=5, db=db, transport=fake.transport)
    out, err = capsys.readouterr()
    assert status == 1
    assert err == "stopped: token request failed with HTTP 401\n"
    assert len(fake.log) == 1
    assert "FAKE" not in out + err


def test_every_request_sends_the_user_agent_accepts_compression_and_keeps_secrets_out_of_urls(db):
    plain, refreshed, slowed = world(), world(), world()
    refreshed.before = lambda index, request: refreshed.valid.clear() if index == 5 else None
    slowed.before = lambda index, request: {0: 429, 2: 429, 9: 503}.get(index)
    for fake in (plain, refreshed, slowed):
        assert run(fake, db) == 0
        assert len(fake.log) >= BASE_REQUESTS
        for _, request, _ in fake.log:  # the first asking and every asking again
            assert request.headers["user-agent"] == config.USER_AGENT
            assert "gzip" in request.headers["accept-encoding"]  # the docs reject clients without compression
            assert request.url.host == "www.deviantart.com" and request.url.scheme == "https"
            for secret in (CLIENT_ID, CLIENT_SECRET, *fake.tokens):
                assert secret not in str(request.url)
            if request.url.path != TOKEN_PATH:
                assert CLIENT_SECRET.encode() not in request.content
                assert CLIENT_SECRET not in str(request.headers)
    assert config.CONTACT_URL in config.USER_AGENT


# --- 4b: the hour that stops a run is time spent refused, and only the pace back at its base starts it again ---


def test_a_401_for_a_token_that_expired_while_the_api_was_refusing_does_not_start_the_hour_again(db, capsys):
    """The critic's counterexample. A token lasts 60 minutes and the stop takes 68.5, so the token always expires
    first. An API that answers the expired token with a 401 while it goes on refusing set the count back to
    nothing every time: discovery ran for 2.2 hours and ended on "browse failed with HTTP 401"."""
    fake, issued = world(), []

    def refusing(index, request):
        if request.url.path == TOKEN_PATH:
            issued.append(fake.clock)  # the token endpoint is not limited: a new token is there for the asking
        elif fake.clock - issued[-1] >= 3600:
            fake.valid.clear()  # expired, and the API looks at the token before it looks at the rate: a 401
        else:
            return 429

    fake.before = refusing
    assert run(fake, db) == 1
    assert capsys.readouterr().err == LIMITED
    assert [status for _, _, status in fake.log] == [200] + [429] * 18 + [401, 200] + [429] * 2
    climb = [2.0, 4.0, 8.0, 16.0, 32.0, 64.0, 128.0, 256.0]
    # Eleven waits at the ceiling, the 401, the new token, and the twelfth: the 401 took a tenth off the
    # interval, as any answer does, and nothing off the hour.
    assert fake.gaps() == pytest.approx(climb + [300.0] * 11 + [270.0, 243.0, 300.0])
    assert fake.log[-1][0] == pytest.approx(4623.0)  # 77 minutes in, and not 2.2 hours


def test_one_answer_among_the_refusals_does_not_start_the_hour_again(db, capsys):
    """The critic's counterexample: twelve 429s and one page served, over and over. It never stopped, at three
    requests in ten minutes: twelve in a row was the rule, and one answer set the count back to nothing."""
    fake = world()
    fake.page_size = 1  # pages enough for the run not to end by itself
    fake.before = lambda index, request: 429 if index >= 2 and (index - 2) % 13 != 12 else None
    assert run(fake, db) == 1
    assert capsys.readouterr().err == LIMITED
    climb = [4.0, 8.0, 16.0, 32.0, 64.0, 128.0, 256.0]
    # Five waits at the ceiling, the one page served, and seven more: twelve of them, an hour.
    assert fake.gaps() == [2.0, 2.0] + climb + [300.0] * 5 + [270.0] + [300.0] * 7
    assert [status for _, _, status in fake.log].count(200) == 3 and sum(fake.gaps()) == 4382.0  # 73 minutes


@pytest.mark.parametrize(("second_stretch", "stopped"), [(66, False), (65, True)])
def test_only_the_interval_back_at_its_base_starts_the_hour_again(db, capsys, second_stretch, stopped):
    """What counts as progress. Sixteen 429s, nine of them waited for at the ceiling: 45 minutes. Then answers.
    The forty-eighth brings the interval back to its base, and the sixteen 429s after it have an hour of their
    own. One answer fewer, and the two stretches are one: three more waits at the ceiling, and the run stops."""
    fake = world()
    fake.page_size = 1
    limited(fake, set(range(2, 18)) | set(range(second_stretch, second_stretch + 16)))
    assert run(fake, db) == int(stopped)
    assert capsys.readouterr().err == (LIMITED if stopped else "")
    assert fake.gaps()[17:65] == pytest.approx([300.0 * 0.9**answered for answered in range(48)])  # down to 2.12
    assert fake.gaps()[65] == pytest.approx(2 * 300.0 * 0.9**47 if stopped else 2.0)  # doubled again, or at the base
    assert len(fake.log) == (76 if stopped else 117 + 1 + 32)  # the eleventh 429 of the second stretch, or the end


def test_a_5xx_three_times_for_every_page_stops_the_run_after_an_hour_of_it_and_the_summary_counts_them(db, capsys):
    """The critic's counterexample: 503 three times and then the page, for every page, 75% refused. It never
    stopped: 1,500 requests in 122 hours. A 5XX counted for nothing towards the hour, and the summary's only
    count of refusals was of 429s. A 5XX at the ceiling counts as a 429 does now, and has a count of its own."""
    fake = world()
    fake.before = lambda index, request: None if index % 4 == 3 else 503
    assert run(fake, db) == 1
    out, err = capsys.readouterr()
    assert err == LIMITED
    climb = [4.0, 8.0, 16.0, 14.4, 28.8, 57.6, 115.2, 103.68, 207.36]  # the token, at its fourth asking; a page
    # Three refusals five minutes apart and the page, answered: four times. Then the twelfth at the ceiling, and
    # the refusal after it ends the run.
    assert fake.gaps() == pytest.approx(climb + [300.0, 300.0, 270.0] + [300.0, 300.0, 300.0, 270.0] * 3 + [300.0])
    counts = summary(out)
    assert counts["server errors or no answer from the API"] == 20 and counts["rate limit answers from the API"] == 0
    assert counts["requests to the API"] == 26 and sum(fake.gaps()) < 2 * 3600


TOO_LONG = f"stopped: rate limited: the API asks for a wait of 299 seconds, more than a run waits. {KEPT}\n"


HOUR_OF_299 = [299.0] * 7 + [300.0] * 5  # what is asked for while the interval is shorter, and then the interval


@pytest.mark.parametrize(("asked", "waits", "said"), [("3600", [3600.0], LIMITED), ("299", HOUR_OF_299, TOO_LONG)])
def test_a_retry_after_on_every_429_stops_the_run_within_the_hour_it_would_have_had_anyway(
    db, capsys, asked, waits, said
):
    """The critic's counterexample: `Retry-After: 3600` on every 429 took 19.0 hours to stop. Only a wait at the
    ceiling counted, and a wait the API had asked for by name did not. Whatever a Retry-After asks for counts
    now: an hour asked for at once, or just under five minutes at a time."""
    fake = world()
    fake.before = lambda index, request: (
        httpx.Response(429, json={"error": "FAKE-RATE-LIMIT"}, headers={"Retry-After": asked}) if index >= 4 else None
    )
    assert run(fake, db) == 1
    assert capsys.readouterr().err == said
    assert fake.gaps()[3:] == [2.0, *waits] and sum(waits) <= discover.PATIENCE  # and nothing is sent afterwards
    assert len(stored(db)) == 3  # and what was stored is kept


@pytest.mark.parametrize(("asked", "waited"), [("2100", 2100.0), ("2101", None), ("999999999", None)])
def test_a_retry_after_that_asks_for_more_than_is_left_of_the_hour_stops_the_run_and_says_how_long(
    db, capsys, asked, waited
):
    """After five waits at the ceiling, 25 minutes, 35 are left. A Retry-After of that is waited. A longer one
    is not: the run ends there, and says what the API asked for."""
    fake = world()
    limited(fake, set(range(4, 16)))  # seven that double the interval, and five at the ceiling
    plain = fake.before
    fake.before = lambda index, request: (
        httpx.Response(429, json={"error": "FAKE-RATE-LIMIT"}, headers={"Retry-After": asked})
        if index == 16
        else plain(index, request)
    )
    assert run(fake, db) == (0 if waited else 1)
    said = f"the API asks for a wait of {asked} seconds, more than a run waits"
    assert capsys.readouterr().err == ("" if waited else f"stopped: rate limited: {said}. {KEPT}\n")
    assert fake.gaps()[11:16] == [300.0] * 5 and len(fake.log) == (BASE_REQUESTS + 13 if waited else 17)
    assert waited is None or fake.gaps()[16:18] == pytest.approx([waited, 270.0])


NOT_SECONDS = ["9" * 400, "9" * 5000, "9999999999", "nan", "inf", "-1", "+120", "1_20", "120.0", "0x78", "120, 240", ""]


@pytest.mark.parametrize("asked", NOT_SECONDS, ids=[str(n) for n in range(len(NOT_SECONDS))])
def test_a_retry_after_that_is_not_a_plain_number_of_seconds_is_not_read(asked, monkeypatch):
    """Nine ASCII digits are the most that is read as a number of seconds. Anything else says nothing: the
    doubled interval is what is waited, and the summary's numbers can still be worked out."""
    waits = []
    monkeypatch.setattr(time, "sleep", waits.append)
    with discover.Pace(discover.BASE_INTERVAL, "the API") as pace:  # which makes the folder its file is written in
        pace.wait()
        pace.slow(httpx.Response(429, headers={"Retry-After": asked.encode()}))
        pace.wait()
    assert waits == [4.0] and list(pace.counts().values()) == [2, 1, 0, 4, 4, 4]


def test_a_retry_after_of_400_digits_ends_no_run_and_leaves_no_file_that_says_one_is_on(db, capsys):
    """The critic's counterexample: a value of 400 digits became infinity, and rounding infinity up for the
    summary raised inside the `finally`: "stopped: OverflowError at discover.py line 130", and the file that
    says a run is on was left behind."""
    fake = world()
    limited(fake, {4}, {"Retry-After": b"9" * 400})
    assert run(fake, db) == 0 and capsys.readouterr().err == ""
    assert fake.gaps()[3:7] == pytest.approx([2.0, 4.0, 3.6, 3.24])  # the doubled interval is what is waited
    assert stored(db) == EXPECTED and not (config.CACHE_DIR / "discover.status").exists()


def test_a_retry_after_on_a_5xx_is_waited_as_on_a_429(db):
    """The critic's thirty-fifth break, which no test caught: Retry-After read on a 429 and not on a 5XX."""
    fake = world()
    fake.before = lambda index, request: httpx.Response(503, headers={"Retry-After": "100"}) if index == 3 else None
    assert run(fake, db) == 0 and stored(db) == EXPECTED
    assert fake.gaps()[2:6] == pytest.approx([2.0, 100.0, 3.6, 3.24])


def test_an_error_where_the_run_reads_its_pace_leaves_no_file_that_says_a_run_is_on(db, capsys, monkeypatch):
    counts, calls = discover.Pace.counts, []

    def breaks(pace):
        calls.append(pace)
        if len(calls) > 3:  # three requests in: the file has been written
            raise OverflowError("FAKE-OVERFLOW")
        return counts(pace)

    monkeypatch.setattr(discover.Pace, "counts", breaks)
    assert run(world(), db) == 1
    assert capsys.readouterr().err.startswith("stopped: OverflowError at discover.py line ")
    assert len(calls) == 5 and not (config.CACHE_DIR / "discover.status").exists()


@pytest.mark.parametrize("status", [400, 403, 404])
def test_a_tag_the_api_refuses_is_noted_and_passed_over_and_the_run_goes_on_to_the_next(db, capsys, status):
    """From the critic's notes: a tag answering 4XX stopped every run there, and the fifteen tags after it were
    never reached. It is asked for once, noted, and left for the next run to ask for again."""
    fake = world()
    fake.before = lambda index, request: status if request.url.params.get("tag") == "characterconcept" else None
    assert run(fake, db) == 1
    out, err = capsys.readouterr()
    assert err == f"skipped the tag characterconcept: browse answered HTTP {status}\n"
    assert fake.browsed() == [("conceptart", 0), ("conceptart", 40), *((tag, 0) for tag in discover.NICHE_TAGS[1:])]
    assert stored(db) == EXPECTED and summary(out)["pages fetched"] == 18  # every other tag, as ever
    again = world()
    assert run(again, db, afresh=False) == 0 and again.browsed() == [("characterconcept", 0)]


# --- 4c: the pace outlives the run and the process, and one run talks to the API at a time ---


def on_one_clock(fake):
    """The pace reads the fake's clock as well as sleeping on it, so that the time between two runs is on it too."""
    return mock.patch.object(discover, "now", lambda: fake.clock)


def stopped_at_the_ceiling(fake, db, capsys):
    """A run that is refused from its sixth request on and stops, an hour later, at an interval of 300 seconds.
    Returns how many requests that was. The fake answers again afterwards, from wherever the next run begins."""
    fake.before = lambda index, request: 429 if 5 <= index < 25 else None
    assert run(fake, db) == 1 and capsys.readouterr().err == LIMITED
    assert len(fake.log) == 25 and fake.gaps()[-12:] == [300.0] * 12
    return len(fake.log)


def test_a_run_after_a_stop_begins_at_the_pace_the_stopped_one_ended_with(db, capsys):
    """The critic's counterexample: 429 from request 5 on, a stop at an interval of 300 s, and discovery run
    again in the same process: its first request went 0 s after the last 429, and its interval was back at
    2 s. The pace was one process's memory. It is a file now, and the next run begins where this one ended."""
    fake = world()
    with on_one_clock(fake):
        last = stopped_at_the_ceiling(fake, db, capsys)
        fake.before = lambda index, request: 429 if index < last + 3 else None  # still refusing when it comes back
        assert run(fake, db, afresh=False) == 0
    # Five minutes after the last 429 and not at once; five minutes after each of three more; then down a tenth.
    assert fake.gaps()[last - 1 : last + 5] == pytest.approx([300.0, 300.0, 300.0, 300.0, 270.0, 243.0])
    assert stored(db) == EXPECTED


def first_waits(monkeypatch, at):
    """The waits before the first two requests of a run that begins at this time, by the pace's own clock."""
    waits, pace = [], discover.Pace(discover.BASE_INTERVAL, "the API")
    monkeypatch.setattr(time, "sleep", waits.append)
    monkeypatch.setattr(discover, "now", lambda: at)
    with pace:
        pace.wait()
        pace.wait()
    return waits


@pytest.mark.parametrize(
    ("later", "waits"),
    [
        (0.0, [300.0, 300.0]),  # at once: the whole of the wait the last run left, and then its interval
        (100.0, [300.0, 300.0]),  # in the middle of that wait: a whole interval from now, never the rest of one
        (300.0, [300.0]),  # as that wait ends: nothing to wait for, and nothing off the interval yet
        (3900.0, [150.0]),  # an hour after it ended: the interval is half what it was
        (7500.0, [75.0]),  # two hours: a quarter
        (86700.0, [2.0]),  # a day: back at the base
    ],
)
def test_the_pace_a_run_begins_with_is_the_last_ones_halved_for_every_hour_nobody_asked(later, waits, monkeypatch):
    """The rule for the time between two runs, a minute or a day, in the same process or another. The file says
    when the next request was due. A run that begins sooner waits; one that begins later has the interval
    halved for every hour past that time. Either way its first request goes no sooner after the last request
    anybody made than the interval it begins with."""
    config.CACHE_DIR.mkdir()
    # As a run leaves it whose last request went at 0, at the ceiling: the interval, and when the next is due.
    (config.CACHE_DIR / "the-API.pace").write_text("300.0 300.0\n", encoding="ascii")
    assert first_waits(monkeypatch, later) == waits
    assert later + sum(waits[:-1]) >= waits[-1]  # when the first request goes, and the interval the run begins with


BROKEN = [b"", b"slow", b"300", b"300.0\n", b"2.0 soon", b"2 0 0", b"nan 0", b"2.0 nan", b"inf 0", b"2.0 inf"]
BROKEN += [b"2.0 -inf", b"-1 0", b"1e999 0", b"301 0", b"2.0 99999999999", b"\xff\xfe2\x00 \x000\x00", b"2.0 \x00"]


@pytest.mark.parametrize("left", BROKEN, ids=map(str, range(len(BROKEN))))
def test_a_pace_file_that_is_not_two_numbers_a_run_could_have_left_means_start_slow(left, monkeypatch):
    """Half written, garbage, or not to be read at all: never a crash and never a fast start. Five minutes
    before the first request, and five minutes between requests from there."""
    config.CACHE_DIR.mkdir()
    (config.CACHE_DIR / "the-API.pace").write_bytes(left)
    assert first_waits(monkeypatch, 0.0) == [300.0, 300.0]


def test_every_prefix_of_a_pace_file_a_stopped_run_left_means_start_slow(db, capsys, monkeypatch):
    """The critic's counterexample: a file cut inside its second number, "300.0 18", read as a pace due in 1970,
    and so as a start at the base with no wait, for 9 of the 21 prefixes of the file a stopped run left. Only
    the whole file is believed now, and it is written whole: into a file of its own, which then replaces it."""
    fake = world()
    fake.clock = 1_800_000_000.0  # a wall-clock time like a real one, so that the file's second number is long
    with on_one_clock(fake):
        stopped_at_the_ceiling(fake, db, capsys)
    whole = (config.CACHE_DIR / "the-API.pace").read_bytes()
    assert len(whole) > 15 and sorted(path.name for path in config.CACHE_DIR.iterdir()) == [
        "the-API.lock",
        "the-API.pace",
    ]
    for cut in range(len(whole)):
        (config.CACHE_DIR / "the-API.pace").write_bytes(whole[:cut])
        assert first_waits(monkeypatch, fake.clock) == [300.0, 300.0], whole[:cut]


@pytest.mark.parametrize("when", ["from the start", "after a 429"])
def test_a_pace_file_that_cannot_be_written_stops_the_run_before_its_next_request(db, capsys, monkeypatch, when):
    """The critic's counterexample: a pace file that could not be written failed in silence, and the next run
    began at whatever an older run had left there. Now the run stops, and says so, before another request."""
    config.CACHE_DIR.mkdir()
    if when == "from the start":
        (config.CACHE_DIR / "the-API.next.pace").mkdir()  # nothing can be written where the file is written first
    fake = world()
    fake.before = lambda index, request: (
        (config.CACHE_DIR / "the-API.next.pace").mkdir() or 429 if when != "from the start" and index == 3 else None
    )
    assert run(fake, db) == 1
    assert capsys.readouterr().err == f"stopped: the pace for the API cannot be written in the cache folder. {KEPT}\n"
    assert len(fake.log) == (0 if when == "from the start" else 4)  # nothing sent after it


@pytest.mark.parametrize(("asked", "left"), [("1800", 1200.0), ("7200", 3000.0)])
def test_a_wait_the_api_asked_for_is_left_for_the_next_run_and_an_hour_of_it_at_most(asked, left, monkeypatch):
    """A run that is killed while it waits out a Retry-After, or that stops because the wait is longer than a run
    waits, leaves the wait in the file, and the next run does not ask sooner. An hour is the most a file says."""
    monkeypatch.setattr(discover, "now", lambda: 0.0)
    with discover.Pace(discover.BASE_INTERVAL, "the API") as pace, suppress(discover.Stop):
        pace.wait()
        pace.slow(httpx.Response(429, headers={"Retry-After": asked}))
    assert first_waits(monkeypatch, 600.0) == [left, 4.0]  # ten minutes later: the rest of it, then the interval


@pytest.mark.parametrize(("asked", "requests", "then"), [("3600", 2, 7.2), ("400", 10, 270.0)])
def test_the_refusal_that_stops_a_run_is_written_down_retry_after_and_all(db, capsys, asked, requests, then):
    """The critic's counterexample: every 429 carries `Retry-After: 3600`. The run sent two requests and stopped,
    and the file said what it had said before the second one: 4 s, due 4 s after it. A run begun 5 s later sent
    at once. The refusal that stops a run is written down now, Retry-After and all, before the run stops."""
    fake = world()
    headers = {"Retry-After": asked}
    fake.before = lambda index, request: httpx.Response(429, json={"error": "FAKE-RATE-LIMIT"}, headers=headers)
    with on_one_clock(fake):
        assert run(fake, db) == 1 and capsys.readouterr().err == LIMITED and len(fake.log) == requests
        fake.clock += 5.0  # run again five seconds after the stop, and the API answers
        fake.before = lambda index, request: None
        assert run(fake, db, afresh=False) == 0
    # The last refusal asked for this long, and the next run waits it; then the interval it was left, a tenth off.
    assert fake.gaps()[requests - 1 : requests + 1] == pytest.approx([float(asked), then])


def test_a_word_in_the_pace_file_is_how_to_make_a_run_start_slow(db, capsys):
    """For the first run after a rate limit that no run of this code saw: five minutes before the first request,
    and then down a tenth with every answer, as from any ceiling."""
    config.CACHE_DIR.mkdir()
    (config.CACHE_DIR / "the-API.pace").write_text("slow")
    fake = world()
    with on_one_clock(fake):
        assert run(fake, db) == 0
    assert capsys.readouterr().err == "" and stored(db) == EXPECTED
    assert fake.log[0][0] == 300.0 and fake.gaps()[:3] == pytest.approx([270.0, 243.0, 218.7])


def test_no_pace_file_means_no_run_before_this_one_and_a_run_leaves_two_files_that_hold_numbers_only(db):
    fake = world()
    with on_one_clock(fake):
        assert run(fake, db) == 0
    assert fake.log[0][0] == 0.0 and set(fake.gaps()) == {2.0}  # the base, and no wait before the first request
    left = {path.name: path.read_text(encoding="ascii") for path in config.CACHE_DIR.iterdir()}
    assert left == {"the-API.lock": "", "the-API.pace": f"2.0 {fake.clock + 2.0!r}\n"}  # the interval, and when next


def test_a_second_run_started_while_one_is_on_is_refused_and_touches_nothing(db, capsys):
    """The critic's counterexample: two discovery runs at once on one store both ran, each at the full pace, and
    the pages of the first tag were counted twice. The second one stops now, before it asks for anything."""
    fake, other, seen = world(), world(), []
    told = config.CACHE_DIR / "discover.status"

    def meanwhile(index, request):
        if index == 3:  # the first run is between two requests
            seen.append((run(other, db, afresh=False), told.exists()))

    fake.before = meanwhile
    assert run(fake, db) == 0
    assert seen == [(1, True)] and other.log == []  # refused, and the first run's file is still there
    out, err = capsys.readouterr()
    assert err == "stopped: another run is talking to the API, and one at a time is the rule\n"
    assert out.endswith(EXPECTED_STDOUT) and stored(db) == EXPECTED and fake.gaps() == [2.0] * (BASE_REQUESTS - 1)


HOLDER = """
import pathlib, sys
from pipeline import config, discover
config.CACHE_DIR = pathlib.Path(sys.argv[1])
with discover.Pace(discover.BASE_INTERVAL, "the API"):
    print("held", flush=True)
    sys.stdin.read()
"""


def test_another_process_that_has_the_api_keeps_a_run_out_and_one_that_died_does_not(db, capsys):
    """Two processes, which is what two stages started from two terminals are. The lock is the operating
    system's, so a process that is killed lets go of it by dying: the file it leaves behind is no lock."""
    command = [sys.executable, "-c", HOLDER, str(config.CACHE_DIR)]
    with subprocess.Popen(command, stdin=subprocess.PIPE, stdout=subprocess.PIPE, cwd=config.ROOT) as holder:
        assert holder.stdout.readline().strip() == b"held"
        fake = world()
        assert run(fake, db) == 1 and fake.log == []
        assert capsys.readouterr().err == "stopped: another run is talking to the API, and one at a time is the rule\n"
        holder.kill()  # as a crash or the task manager would: it cleans nothing up
    assert (config.CACHE_DIR / "the-API.lock").exists()
    assert run(world(), db) == 0 and stored(db) == EXPECTED


# --- 5: a long run is not a black box ---


def test_a_run_says_where_it_is_in_a_file_while_it_runs_and_the_file_goes_when_it_ends(db, capfd):
    """Four hours of a run that was answered nothing but 429 could not be told from one that was browsing, or
    stuck. Before every request, the first asking and every asking again, the file says where the run is."""
    fake, said = world(), []
    told = config.CACHE_DIR / "discover.status"

    def before(index, request):
        said.append(told.read_text(encoding="utf-8"))
        return 429 if index in (5, 6) else None

    fake.before = before
    assert run(fake, db) == 0
    assert len(said) == BASE_REQUESTS + 2 and not told.exists()  # one that is there is a run in progress
    assert capfd.readouterr().err == ""  # and nothing of it is on standard error
    for text in said:
        when, *lines = text.splitlines()
        assert datetime.now(UTC) - datetime.fromisoformat(when) < timedelta(minutes=1)
        assert [line.rsplit(": ", 1)[0] for line in lines] == list(discover._COUNTS)  # the summary's own lines
        assert all(re.fullmatch(r"[A-Za-z ,()>=0-9-]+: \d+", line) for line in lines)  # counts only
        assert "fake" not in text.lower() and "http" not in text.lower()
    assert "pages fetched: 0\n" in said[0] and "requests to the API: 0\n" in said[0]
    assert (
        "pages fetched: 4\n" in said[5] and "artists stored: 3\n" in said[5] and "requests to the API: 5\n" in said[5]
    )
    now = "interval between requests to the API now, in seconds"  # browsing, or waiting: this is how to tell
    assert [int(re.search(f"{now}: (\\d+)", text)[1]) for text in said[5:9]] == [2, 4, 8, 8]
    assert "rate limit answers from the API: 2\n" in said[7] and "pages fetched: 4\n" in said[7]


def test_a_status_file_that_cannot_be_written_or_deleted_does_not_end_a_run(db, monkeypatch):
    """It only informs. The pace file is another matter: see the test of one that cannot be written."""

    def locked(real):
        def call(path, *args, **kwargs):
            if path.suffix == ".status":
                raise PermissionError("FAKE-LOCKED")
            return real(path, *args, **kwargs)

        return call

    monkeypatch.setattr(pathlib.Path, "write_text", locked(pathlib.Path.write_text))
    monkeypatch.setattr(pathlib.Path, "unlink", locked(pathlib.Path.unlink))
    assert run(world(), db) == 0 and stored(db) == EXPECTED


def test_the_status_command_shows_where_a_run_says_it_is(tmp_path):
    assert not any("is running" in line for line in gpu.status())
    discover.tell("discover", {"pages fetched": 412, "artists stored": 1200})
    lines = gpu.status()
    at = next(index for index, line in enumerate(lines) if line.startswith("discover is running, or was killed. As of"))
    assert lines[at + 1 : at + 3] == ["  pages fetched: 412", "  artists stored: 1200"]
    discover.tell("discover")
    assert not any("is running" in line for line in gpu.status())


# --- 6: mature deviations do not count ---


def test_mature_deviations_do_not_count_toward_niche_hits(db):
    fake = world()
    assert run(fake, db) == 0
    hits = {name: n for _, name, _, _, n in stored(db)}
    assert hits[DELTA] == 3  # 3 clean + 2 mature
    assert ECHO not in hits  # 2 clean + 5 mature never reaches the threshold
    assert CHARLIE not in hits  # 2 clean


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
    fake = FakeDA({"gameart": junk * 3 + good})
    assert run(fake, db) == 0
    assert [(name, links, hits) for _, name, _, links, hits in stored(db)] == [("zz-fake-good", [], 3)]
    assert len(all_rows(db, "counted")) == 3  # zz-fake-junk never became a candidate, or a row
    assert capsys.readouterr().err == ""


def test_three_works_only_count_when_none_is_mature(db):
    clean = [deviation(n, "zz-fake-clean") for n in (1, 2, 3)]
    mixed = [deviation(n, "zz-fake-mixed", mature=n == 6) for n in (4, 5, 6)]
    unlabeled = [deviation(n, "zz-fake-unlabeled") for n in (7, 8, 9)]
    del unlabeled[0]["is_mature"]  # no label is not "not mature"
    fake = FakeDA({"gameart": clean + mixed + unlabeled})
    assert run(fake, db) == 0
    assert [(name, hits) for _, name, _, _, hits in stored(db)] == [("zz-fake-clean", 3)]
    assert "mixed" not in everything_on_disk(db) and "unlabeled" not in everything_on_disk(db)


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


def test_a_pass_makes_the_removal_checks_between_its_pages_and_a_refusal_in_them_stops_it(db, capsys, monkeypatch):
    """The fifth critic's: a pass holds the API for 72 minutes and made no removal check, and the scheduled ones
    stopped at their first lookup, so an artist's code waited past the look the page promised. The pass makes them
    itself, every removals.EVERY between two pages, through its own client and under its lock. The database failing
    one is counted and the pass goes on; the API refusing one stops the pass, and is never swallowed."""
    fake = world()
    monkeypatch.setattr(removals, "EVERY", timedelta(seconds=10))  # five requests at the base pace
    looks = []

    def checks(counts, dry_run, client_id, client_secret, *, local=None, api=None):
        assert (dry_run, local) == (False, db) and api.pace._lock is not None  # this pass's client, its lock held
        looks.append(fake.clock)
        counts["artists_removed"] += 1
        if len(looks) == 2:
            raise RuntimeError("FAKE-DATABASE-ERROR")

    monkeypatch.setattr(removals, "run", checks)
    with on_one_clock(fake):
        assert run(fake, db) == 0
    out, err = capsys.readouterr()
    counts = summary(out)
    assert len(looks) >= 3 and all(later - earlier >= 10 for earlier, later in itertools.pairwise(looks))
    assert counts[discover.PROVEN] == len(looks) and counts[discover.UNCHECKED] == 1
    assert err.startswith("a removal check failed: RuntimeError") and "FAKE" not in err
    assert stored(db) == EXPECTED

    def refused(counts, *args, **kwargs):
        raise discover.Stop("rate limited: the API kept refusing")

    monkeypatch.setattr(removals, "run", refused)
    asked = len(fake.log)
    with on_one_clock(fake):
        assert run(fake, db) == 1
    out, err = capsys.readouterr()
    assert err == "stopped: rate limited: the API kept refusing\n" and summary(out)[discover.UNCHECKED] == 0
    assert 0 < len(fake.log) - asked < BASE_REQUESTS and stored(db) == EXPECTED  # stopped there, nothing lost
