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
import sys
import threading
import time
from contextlib import closing
from datetime import UTC, datetime, timedelta
from unittest import mock
from urllib.parse import parse_qs

import httpx
import pytest

from pipeline import config, discover, store

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
requests to the API: 20
rate limit answers from the API: 0
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
LIMITED = f"stopped: rate limited: the API answered HTTP 429 12 times in a row, 5 minutes apart. {KEPT}\n"


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
    the own site that collection keeps in the row, which is where the removal of a site looks."""
    assert run(world(), db) == 0
    assert store.keep_own_site(ALPHA, "https://zz-fake-alpha.art/", db) is True
    assert run(world(more=[ALPHA]), db) == 0  # a later pass, in which ALPHA has one work more
    assert stored(db)[0] == expected_row(ALPHA, 5, ["https://zz-fake-alpha.art/"])
    assert stored(db)[1:] == EXPECTED[1:]
    assert store.mark_removed("site:zz-fake-alpha.art", db) == 1  # and so the removal still finds them


def test_a_removed_artist_is_in_neither_list_handed_to_the_later_stages(db):
    assert run(world(), db) == 0
    assert store.keep_own_site(ALPHA, "https://zz-fake-alpha.art/", db)  # as the collect stage does
    assert store.keep_own_site(BRAVO, "http://www.zz-fake-bravo.com/", db)
    store.mark_removed(f"da:{FOXTROT}", db)  # stored with no link
    store.mark_removed("site:zz-fake-alpha.art", db)  # stored with a link
    left = [key for key, *_ in without("da:zz-fake-foxtrot", "da:zz-fake-alpha")]
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


def test_a_removed_site_removes_the_artists_whose_stored_link_it_is_and_they_stay_out(db):
    studio = "https://zz-fake-studio.art/"
    assert run(world(), db) == 0
    for name, site in ((ALPHA, studio), (BRAVO, "http://www.zz-fake-studio.art/")):
        assert store.keep_own_site(name, site, db)  # as the collect stage does, once it has read their profile
    assert store.mark_removed("site:zz-fake-studio.art", db) == 2
    gone = ("da:zz-fake-alpha", "da:zz-fake-bravo")
    assert stored(db) == without(*gone)
    assert store.is_removed(f"da:{ALPHA}", db) and store.is_removed(f"da:{BRAVO}", db)
    assert run(world(), db) == 0  # whatever site they list next: discovery knows them by their handle
    assert stored(db) == without(*gone)
    assert "studio" not in everything_on_disk(db).lower()


def test_an_artist_whose_site_was_removed_before_they_were_ever_stored_is_known_here_by_handle_alone(db):
    """Discovery cannot know whose site a removed one is: it reads no profile. The artist is stored as a handle
    with no link. The removed site is written into nobody's row, and the collect stage, which reads the profile,
    marks them and collects nothing (see its tests)."""
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
    for _ in range(2):
        assert run(world(), db) == 0
        assert stored(db) == without("da:zz-fake-alpha", "da:zz-fake-delta")


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
    limited(again := world(), set(range(2, 14)) | set(range(15, 25)))  # ten more after one success: not in a row
    assert run(again, db) == 0 and stored(db) == EXPECTED


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
    assert capsys.readouterr().err == (f"stopped: rate limited: the API asks for a wait of more than an hour. {KEPT}\n")
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
    def locked(*args, **kwargs):
        raise PermissionError("FAKE-LOCKED")

    monkeypatch.setattr(pathlib.Path, "write_text", locked)
    monkeypatch.setattr(pathlib.Path, "unlink", locked)
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
