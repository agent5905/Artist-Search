"""Tests for pipeline.removals.

The public database is the push tests' FakeDb, with what this stage reads and calls besides: the removal requests
with their queue (code_at, due_at), removal_tally, removal_code, confirm_removal, grant_request and reject_removal,
as sql/002_functions.sql has them. (tests/test_db.py runs the real ones against the owner's database.) It is put
in the place of pipeline.db for both stages, so a run of this one and a push that follows it meet the same
database. DeviantArt's API and the artists' sites are httpx.MockTransports: no network, no real sleeping. The time
is the world's clock, which only a test moves. The local stores are real ones in pytest's temporary directory,
filled by the push tests' own helper. Every handle, site, code and text below is invented.
"""

import hashlib
import hmac
import itertools
import json
import logging
import re
import socket
import sqlite3
import time
from collections import Counter
from datetime import UTC, datetime, timedelta
from urllib.parse import urlsplit

import httpx
import pytest
from test_push import (
    ALPHA,
    BRAVO,
    CHARLIE,
    DELTA,
    FakeDb,
    artist,
    collected_again,
    digest,
    local_state,
    of_artist,
    sent_artists,
    unsynced,
)

from pipeline import collect, config, crawl, db, discover, library, push, removals, store

SECRET = "FAKE-REMOVAL-SECRET"
START = datetime(2026, 10, 14, 12, tzinfo=UTC).timestamp()
FILE = f".well-known/{removals.PROOF_NAME}.txt"


def code(slug, secret=SECRET):
    """removal_code() in sql/002_functions.sql: the slug's code, made with the database's secret."""
    signed = hmac.new(secret.encode(), f"removal code {slug}".encode(), hashlib.sha256)
    return "artist-search-removal-" + signed.hexdigest()[:32]


def expected(**counts):
    """A run's counts (World.check), the tally of every request apart."""
    return {name: 0 for name in removals.COUNTS if name not in removals.TALLY} | counts


def when(text):
    return datetime.fromisoformat(text)


def function_body(name):
    """A function as sql/ makes it, on one line and without comments: for the tests that read the statements
    RemovalsDb follows, so that a break in them fails here too, without the owner's database."""
    sql = "\n".join(
        (config.ROOT / "sql" / f).read_text(encoding="utf-8") for f in ("002_functions.sql", "003_limits.sql")
    )
    sql = re.sub(r"--[^\n]*", "", sql)
    start = sql.index(f"create or replace function public.{name}(")
    return " ".join(sql[start : sql.index("$$;", start)].split())


class RemovalsDb(FakeDb):
    """FakeDb, with the removal requests and the functions this stage calls, as sql/002_functions.sql has them.

    An artist's row carries da_account, the library's hash of their account, as the push stage sends it (a row sent
    without one gets it here, from the library, which is where the push takes it). A request is filed by
    request_removal (no code) or start_removal_proof (a code given out: its checks start over, due at once unless it
    already was, in the turns of the network that asked as well as of the first 30 that did), bound to whom the
    claim page showed (its ticket), or without one to the artist listed under the slug then: their handle, site and
    account, and never one already barred. The same handle and site asked about again is the same request, which
    only counts (and takes an account it lacked, and keeps a note that says something in place of its network's
    earlier one: the latest from each of the ten networks heard from last); anything else is a request of its own.
    due_removals takes the due ones in turns between networks, each in the first turn it has in any of its networks.
    Nothing a push or a load does touches one. A run moves one on only while its code_at is the one the run read.
    confirm_removal and grant_request close an open one, and put on the list the account it is bound to (or, for one
    bound to none, its own handle, when p_handle says the profile answers under it now), and its site when the caller
    says the proof covered it and nobody else is listed with it now (by account, or without one by handle): never
    whoever the slug lists now.
    That deletes whoever they bar, and an open request whose handle or account they bar is closed as "removed". A
    row with a removed handle, site or account is not loaded again (artists_before_write). A closed request keeps
    nothing of the artist. `queue` names an open request by its handle (by its handle and id when the
    handle has two)."""

    def __init__(self):
        super().__init__()
        self.queue = {}  # the request's handle -> its row; a closed one is under "closed:<n>"
        self.now = lambda: datetime.fromtimestamp(START, UTC).isoformat()  # the database's clock: World sets it
        self.secret = SECRET
        self.proofs = {}  # slug -> what removal_code answers instead of the slug's code
        self.confirmed = []  # (slug, code) of every call to confirm_removal
        self.moved = []  # (slug, due_at) of every request moved on

    _checked = staticmethod(lambda response: response.json() if response.content else None)

    @property
    def requests(self):
        return [name for name, row in self.queue.items() if row["outcome"] is None]  # open, in the order filed

    def made_for(self, row):
        """The removal list's two keys for the handle and site a request was made for, as its generated columns."""
        return dict(zip(("da_key", "site_key"), self.keys(row) if row["da_username"] else (None, None), strict=True))

    def _open(self, request_id):
        return next((name for name in self.requests if self.queue[name]["id"] == request_id), None)

    def file(self, slug, with_code, note=None, network=None, shown=None):
        """request_removal (without a code) or start_removal_proof (with one), as the database does it. `shown` is the
        artist row the claim page showed (its ticket: request_binding), `network` the one the code is asked from."""
        listed = next((row for row in self.artists.values() if row["slug"] == slug), None)
        shown = shown if shown and shown["slug"] == slug else listed
        if shown is None:
            return
        now = self.now()
        note = (note or "").strip()[:300] or None  # left(nullif(btrim(p_note), ''), 300)
        bound = {name: shown.get(name) for name in ("da_username", "site_url", "site_domain", "da_account")}
        if {self.made_for(bound)["da_key"], bound["da_account"]} & set(self.removed):
            return  # gone already: a request would only keep their handle
        same = [name for name in self.requests if self.queue[name]["artist_slug"] == slug]
        same = [name for name in same if self.made_for(self.queue[name]) == self.made_for(bound)]
        said = [{"at": now, "note": note, "network": network}] if note and not with_code else []
        if same:
            row = self.queue[same[0]]
            row["da_account"] = row["da_account"] or bound["da_account"]
            if not with_code:
                notes = [kept for kept in row["notes"] if not said or kept["network"] != network] + said
                row |= {"last_at": now, "requests": row["requests"] + 1, "notes": notes[-10:]}
        else:
            row = {"id": next(self._ids), "artist_slug": slug, "notes": said, "first_at": now, "last_at": now}
            row |= bound | {"requests": 1, "code_at": None, "due_at": None, "outcome": None, "networks": []}
            handle = bound["da_username"]
            self.queue[handle if handle not in self.queue else f"{handle}#{row['id']}"] = row
        if with_code:
            row["code_at"], row["due_at"] = now, min(row["due_at"] or now, now, key=when)
            if network not in row["networks"] and len(row["networks"]) < 30:
                row["networks"] = [*row["networks"], network]

    def close(self, name, outcome):
        row = self.queue.pop(name)
        gone = dict.fromkeys(("da_username", "site_url", "site_domain", "da_account", "due_at"))
        closed = f"closed:{next(self._ids)}"
        self.queue[closed] = row | gone | {"artist_slug": closed, "outcome": outcome, "notes": [], "networks": []}

    def grant(self, request_id, outcome, site, handle=None):
        name = self._open(request_id)
        if name is None:
            return None
        row = self.queue[name]
        da_key, site_key = self.made_for(row).values()
        # The account; without one, the request's own handle, and only when the caller has just seen it answer.
        who = row["da_account"] or (da_key if handle and digest(f"da:{handle.lower()}") == da_key else None)
        if who is None:
            return None

        def whose(other):  # an artist listed now, as the request knows its artist: by account, or without, by handle
            return other.get("da_account") if row["da_account"] else self.keys(other)[0]

        taken = any(self.keys(other)[1] == site_key and whose(other) != who for other in self.artists.values())
        keys = [key for key in (who, site_key if site and not taken else None) if key]
        self.close(name, outcome)  # first, so the removal does not close it as "removed"
        self.sql(push._removed_sql(set(keys)))
        return keys

    def data_api(self, method, path, key, params, json=None):
        assert key == "secret" and path == "removal_requests"  # the service key, and nothing else
        if method == "PATCH":
            assert set(params) == {"id", "outcome", "code_at"} and params["outcome"] == "is.null"
            assert set(json) == {"due_at"}
            name = self._open(int(params["id"].removeprefix("eq.")))
            if name and when(self.queue[name]["code_at"]) != when(params["code_at"].removeprefix("eq.")):
                name = None  # its code was asked for again since it was read
            self.moved.append((self.queue[name]["artist_slug"] if name else None, json["due_at"]))
            if name:
                self.queue[name]["due_at"] = json["due_at"]
            return httpx.Response(204)
        assert method == "GET" and params["outcome"] == "is.null"  # only open requests are ever read
        rows = [self.queue[name] | self.made_for(self.queue[name]) for name in self.requests]
        if "id" in params:  # the owner's grant reads the requests it names: in.(<id>,<id>)
            asked = {int(n) for n in params["id"].removeprefix("in.(").removesuffix(")").split(",")}
            rows = [row for row in rows if row["id"] in asked]
        assert params["order"] == "first_at.asc,id.asc"  # the owner's list, a page at a time
        rows = sorted(rows, key=lambda row: (when(row["first_at"]), row["id"]))[int(params["offset"]) :]
        columns = params["select"].split(",")
        return httpx.Response(200, json=[{name: row.get(name) for name in columns} for row in rows])

    def rpc(self, name, args, key):
        assert key == "secret"
        if name == "removal_tally":
            rows = list(self.queue.values())
            return {
                "requests_open_with_code": sum(row["outcome"] is None and row["code_at"] is not None for row in rows),
                "requests_open_without_code": sum(row["outcome"] is None and row["code_at"] is None for row in rows),
                "requests_proven": sum(row["outcome"] == "proven" for row in rows),
                "requests_closed": sum(row["outcome"] not in (None, "proven") for row in rows),
            }
        if name == "removal_code":
            return self.proofs.get(args["p_slug"], code(args["p_slug"], self.secret))
        if name == "due_removals":  # in turns between networks, the one due longest first within a turn
            assert args["p_limit"] == removals.MAX_REQUESTS
            due = [self.queue[name] for name in self.requests if self.queue[name]["code_at"]]
            due = [row for row in due if when(row["due_at"]) <= when(args["p_now"])]
            due.sort(key=lambda row: (when(row["due_at"]), when(row["code_at"]), row["id"]))
            turns = Counter()  # each request in the first turn it has in any network that asked for it
            turn = [min(turns.update([net]) or turns[net] for net in row["networks"] or [None]) for row in due]
            due = [row for _, row in sorted(zip(turn, due, strict=True), key=lambda pair: pair[0])]  # stable
            columns = ("id", "artist_slug", "code_at", "da_username", "site_url", "site_domain", "da_account")
            return [{column: row[column] for column in columns} for row in due[: args["p_limit"]]]
        if name == "grant_request":
            return self.grant(args["p_id"], args["p_outcome"], args["p_site"], args.get("p_handle"))
        if name == "reject_removal":
            request = self._open(args["p_id"])
            return request is not None and not self.close(request, "rejected")
        assert name == "confirm_removal"
        request = self._open(args["p_id"])
        slug = self.queue[request]["artist_slug"] if request else None
        self.confirmed.append((slug, args["p_code"]))
        if slug is None or args["p_code"] != code(slug, self.secret):
            return None
        assert store.HANDLE.fullmatch(args["p_handle"])  # the verifier always says where it found the proof
        return self.grant(args["p_id"], "proven", args["p_site"], args["p_handle"])

    def sql(self, query):
        answer = super().sql(query)
        for artist_id in [key for key, row in self.artists.items() if row.get("da_account") in self.removed]:
            self._delete_artist(artist_id)  # delete_removed, by account
        for name in self.requests:  # removed_after_insert: by the handle or the account, never the site alone
            if {self.made_for(self.queue[name])["da_key"], self.queue[name]["da_account"]} & set(self.removed):
                self.close(name, "removed")
        return answer

    def load(self, artists, works, neighbors):
        def account(row):
            return (
                row["da_account"] if "da_account" in row else library.account_hash(store.artist_key(row["da_username"]))
            )

        artists = [row | {"da_account": account(row)} for row in artists]
        refused = {row["slug"] for row in artists if {*self.keys(row), row["da_account"]} & set(self.removed)}
        refused -= self.skips
        self.skips |= refused  # for this load: a slug is refused for the row it holds, not for good
        try:
            return super().load(artists, works, neighbors)
        finally:
            self.skips -= refused


def profile(handle, *, user=None, name=None, **fields):
    """A profile as the API answers it: the account's id and its handle now, and the fields given."""
    return {"user": {"userid": f"FAKE-USER-{user or handle}", "username": name or handle}, "website": None} | fields


def meta(value):
    """The tag the site asks an artist to put in their front page's head."""
    return f'<meta name="{removals.PROOF_NAME}" content="{value}">'


def page(head="", body=""):
    return f"<!doctype html><html><head><title>Portfolio</title>{head}</head><body><p>My work.</p>{body}</body></html>"


class DeviantArt:
    """The API: a token for anybody, and each handle's profile as `profiles` has it (or an httpx.Response to
    give instead). A handle it lacks is 404. It notes how httpx's logger was set at each request."""

    def __init__(self):
        self.profiles = {}
        self.requests = []
        self.log_levels = set()
        self.transport = httpx.MockTransport(self._serve)

    def _serve(self, request):
        self.requests.append(str(request.url))
        self.log_levels.add(logging.getLogger("httpx").getEffectiveLevel())
        assert request.url.host == "www.deviantart.com"  # nothing else goes through the API's client
        if request.url.path == "/oauth2/token":
            return httpx.Response(200, json={"access_token": "FAKE-TOKEN"})
        assert request.headers["authorization"] == "Bearer FAKE-TOKEN"
        answer = self.profiles.get(request.url.path.removeprefix("/api/v1/oauth2/user/profile/"))
        if isinstance(answer, httpx.Response):
            return answer
        return httpx.Response(404, json={"error_code": 2}) if answer is None else httpx.Response(200, json=answer)


class Clock:
    """The crawler's clock: sleeping moves the hands, and nothing else does."""

    def __init__(self):
        self.now = 1000.0

    def __call__(self):
        return self.now

    def sleep(self, seconds):
        self.now += seconds


class Sites:
    """Artists' own sites: `pages` maps a URL to its body, `raw` to (status, headers, body) for anything else
    (a redirect, a header). Anything in neither is 404, so no robots.txt and no proof file unless one is given."""

    def __init__(self):
        self.pages = {}
        self.raw = {}
        self.slow = set()  # URLs whose answer takes an hour to come
        self.urls = []

    def fetcher(self):
        self.clock = Clock()
        transport = httpx.MockTransport(self._serve)
        return crawl.PoliteFetcher(
            transport=transport, clock=self.clock, sleep=self.clock.sleep, resolver=lambda _: ["93.184.216.34"]
        )

    def _serve(self, request):
        url = str(request.url.copy_with(host=request.headers["host"]))
        self.urls.append(url)
        if url in self.slow:
            self.clock.now += 3600
        if url in self.raw:
            status, headers, *body = self.raw[url]
            return httpx.Response(status, headers=headers, text=body[0] if body else "")
        body = self.pages.get(url)
        return (
            httpx.Response(404)
            if body is None
            else httpx.Response(200, text=body, headers={"content-type": "text/html"})
        )


class World:
    def __init__(self, database):
        self.db, self.da, self.sites = database, DeviantArt(), Sites()
        self.clock = START  # seconds: the verifier's discover.now(), and the database's now()
        database.now = lambda: datetime.fromtimestamp(self.clock, UTC).isoformat()

    def list(self, handle, site=None, *, account=True, code=True):
        """An artist the database lists, as a push leaves them, and their request, filed now with a code given out
        (`code`) or without. `account`: the library keeps their DeviantArt account (they are collected here, unless
        they already were)."""
        if account and library.account_hash(store.artist_key(handle)) is None:
            artist(handle)
        domain = urlsplit(site).hostname.removeprefix("www.") if site else None
        self.db.load(
            [{"slug": handle, "name": handle, "da_username": handle, "site_url": site, "site_domain": domain}], [], []
        )
        self.db.file(handle, with_code=code)

    def later(self, **delta):
        self.clock += timedelta(**delta).total_seconds()

    def check(self, dry_run=False):
        """One run of the stage, in a process of its own: the crawler's state of the run starts afresh. Its counts,
        without the tally of every request in the database, which goes to self.tally."""
        for state in (crawl._LAST, crawl._DELAY, crawl._SPENT, crawl._OWNERS):
            state.clear()
        counts = dict.fromkeys(removals.COUNTS, 0)
        fetcher = self.sites.fetcher()
        try:
            removals.run(
                counts, dry_run, "FAKE-CLIENT-ID", "FAKE-CLIENT-SECRET", transport=self.da.transport, fetcher=fetcher
            )
        finally:
            self.tally = {name: counts.pop(name) for name in removals.TALLY}
        return counts


@pytest.fixture
def world(tmp_path, monkeypatch):
    monkeypatch.setattr(config, "CACHE_DIR", tmp_path / "cache")
    monkeypatch.setattr(config, "LOCAL_DB", tmp_path / "cache" / "pipeline.sqlite")
    monkeypatch.setattr(config, "load_env", lambda: {})  # the real .env is never read here
    monkeypatch.setattr(sqlite3, "connect", unsynced)
    monkeypatch.setattr(time, "sleep", lambda seconds: None)  # what discover.Pace waits with

    def no_network(*args, **kwargs):
        raise AssertionError("a real connection was attempted")

    monkeypatch.setattr(socket.socket, "connect", no_network)
    monkeypatch.setattr(socket, "create_connection", no_network)
    database = RemovalsDb()
    monkeypatch.setattr(push, "db", database)
    monkeypatch.setattr(removals, "db", database)
    made = World(database)
    monkeypatch.setattr(discover, "now", lambda: made.clock)
    return made


def gone(world, handle):
    return not any(row["slug"] == handle for row in world.db.artists.values())


# On DeviantArt: the bio and the tagline of the account the library keeps, and nowhere else.


@pytest.mark.parametrize("field", ["bio", "tagline"])
def test_the_code_in_the_profiles_own_text_removes_the_artist_there_at_once_and_bars_them_here(world, field):
    world.list(ALPHA)
    world.list(BRAVO, site=f"https://{BRAVO}.art/")
    world.da.profiles[ALPHA] = profile(ALPHA, bio="Concept artist.")
    # Long words get split by markup, and the code is in a sentence of the artist's own.
    split = code(BRAVO)[:30] + "<wbr>" + code(BRAVO)[30:]
    said = {field: f"<p>Please take me off Artist Search: {split}</p>"}
    world.da.profiles[BRAVO] = profile(BRAVO, website=f"https://{BRAVO}.art/", **said)

    assert world.check() == expected(
        requests_due=2, proven_on_deviantart=1, artists_removed=1, left_for_owner_no_code_found=1
    )
    # There: gone, the hashes of their account and site on the list, the request answered. Not their handle by name:
    # the account bars them under it and under any other. The other artist's request waits for the owner.
    assert [row["slug"] for row in world.db.artists.values()] == [ALPHA] and world.db.requests == [ALPHA]
    account = library.account(f"FAKE-USER-{BRAVO}")
    assert set(world.db.removed) == {account, digest(f"site:{BRAVO}.art")}
    # Here at once, the same two: no stage collects the account, under any handle, or the site again.
    assert library.account_removed(f"FAKE-USER-{BRAVO}") and store.is_removed(f"https://www.{BRAVO}.art/")
    assert not store.is_removed(ALPHA) and not library.account_removed(f"FAKE-USER-{ALPHA}")
    assert world.sites.urls == []  # proven on DeviantArt: the site is not asked
    assert world.da.log_levels == {logging.CRITICAL + 1}  # no request URL, which holds a handle, was logged


SPELLINGS = {
    "upper": lambda c: c.upper(),
    "capitalised": lambda c: c[0].upper() + c[1:],  # a phone's keyboard
    "spaced": lambda c: f"\n   {c}   \n",
    "entity_for_a_hyphen": lambda c: c.replace("-", "&#45;", 1),
    "nbsp_around": lambda c: f"code:&nbsp;{c}&nbsp;thanks",
    "zero_width_space_inside": lambda c: c[:30] + "&#8203;" + c[30:],
    "soft_hyphen_inside": lambda c: c[:30] + "&shy;" + c[30:],
    "bold_part": lambda c: f"<b>{c[:10]}</b>{c[10:]}",
    "in_a_link": lambda c: f'<a href="https://www.deviantart.com/{ALPHA}">{c}</a>',
}


@pytest.mark.parametrize("spelling", SPELLINGS)
def test_the_code_however_written_in_the_bio_is_found(world, spelling):
    world.list(ALPHA)
    world.da.profiles[ALPHA] = profile(ALPHA, bio=f"<p>{SPELLINGS[spelling](code(ALPHA))}</p>")

    assert world.check() == expected(requests_due=1, proven_on_deviantart=1, artists_removed=1)


@pytest.mark.parametrize("where", ["real_name", "website", "last_status", "a_watchers_comment"])
def test_the_code_anywhere_else_on_deviantart_proves_nothing(world, where):
    world.list(ALPHA)
    said = {
        "real_name": code(ALPHA),
        "website": f"https://{BRAVO}.art/?{code(ALPHA)}",
        "last_status": {"body": code(ALPHA)},
        "a_watchers_comment": [{"user": {"username": BRAVO}, "body": code(ALPHA)}],
    }
    world.da.profiles[ALPHA] = profile(ALPHA, bio="Concept artist.", tagline="Open for work.", **{where: said[where]})

    assert world.check() == expected(requests_due=1, left_for_owner_no_code_found=1)
    assert world.db.confirmed == [] and len(world.db.artists) == 1 and world.db.requests == [ALPHA]


def test_a_code_on_another_artists_page_proves_nothing_for_either(world):
    world.list(ALPHA, site=f"https://{ALPHA}.art/")
    world.list(BRAVO)
    world.da.profiles[ALPHA] = profile(ALPHA, bio=code(BRAVO), website=f"https://{ALPHA}.art/")
    world.da.profiles[BRAVO] = profile(BRAVO, tagline=code(ALPHA) + " " + code(ALPHA).upper())
    world.sites.pages[f"https://{ALPHA}.art/"] = page(meta(code(BRAVO)))
    world.sites.pages[f"https://{ALPHA}.art/{FILE}"] = code(BRAVO)

    assert world.check() == expected(requests_due=2, left_for_owner_no_code_found=2)
    assert world.db.confirmed == [] and len(world.db.artists) == 2 and world.db.removed == []
    # The eighth critic's uncaught breaks: confirm_removal taking any code, and a code made without its slug.
    # RemovalsDb follows the statements read here (tests/test_db.py runs them).
    assert (
        "if not coalesce(p_code = (select public.removal_code(r.artist_slug) from public.removal_requests r "
        "where r.id = p_id and r.outcome is null), false) then return null; end if;" in function_body("confirm_removal")
    )
    assert "hmac('removal code ' || coalesce(p_slug, ''), secret, 'sha256')" in function_body("removal_code")


@pytest.mark.parametrize(
    "answer", ["another_handle", "another_account", "not_found", "not_a_profile", "an_error_with_a_profile_in_it"]
)
def test_a_profile_that_is_not_the_artists_proves_nothing_and_its_site_is_not_asked(world, answer):
    site = f"https://{ALPHA}.art/"
    world.list(ALPHA, site=site)
    world.sites.pages[site] = page(meta(code(ALPHA)))  # their site, which their own profile would name
    world.da.profiles[ALPHA] = {
        # A profile under another name, for the account the library keeps: it says it is somebody else.
        "another_handle": profile("yy-fake-zulu", user="yy-fake-zulu", bio=code(ALPHA), website=site),
        # The handle has passed to somebody else, whose profile names the artist's site.
        "another_account": profile(ALPHA, user="yy-fake-zulu", bio=code(ALPHA), website=site),
        "not_found": None,
        "not_a_profile": httpx.Response(200, json=[code(ALPHA), site]),
        "an_error_with_a_profile_in_it": httpx.Response(404, json=profile(ALPHA, bio=code(ALPHA), website=site)),
    }[answer]

    assert world.check() == expected(requests_due=1, left_for_owner_not_checked=1)
    assert world.db.confirmed == [] and len(world.db.artists) == 1 and world.sites.urls == []


def test_an_answer_from_the_same_account_under_its_new_handle_is_the_artists(world):
    artist(BRAVO)
    world.list(BRAVO)
    world.da.profiles[BRAVO] = profile(CHARLIE, user=BRAVO, bio=code(BRAVO))  # renamed since it was collected

    assert world.check() == expected(requests_due=1, proven_on_deviantart=1, artists_removed=1)


@pytest.mark.parametrize("spelling", ["uid", "da-user"])
def test_the_account_is_known_under_either_spelling_of_its_hash(world, spelling):
    artist(ALPHA)
    with sqlite3.connect(config.LOCAL_DB) as conn:  # how a profile written before the 'uid' spelling holds it
        held = library.account(f"FAKE-USER-{ALPHA}", spelling)
        conn.execute("UPDATE profiles SET user_hash = ? WHERE artist_key = ?", (held, f"da:{ALPHA}"))
    world.list(ALPHA)  # the push sends the hash as the library holds it, and the request is bound to it
    assert world.db.queue[ALPHA]["da_account"] == held
    world.da.profiles[ALPHA] = profile(ALPHA, bio=code(ALPHA))

    assert world.check() == expected(requests_due=1, proven_on_deviantart=1, artists_removed=1)
    # And here, under that spelling: the collect stage does not take the account back under another handle.
    assert library.account_removed(f"FAKE-USER-{ALPHA}")


@pytest.mark.parametrize("holder", ["the_artist_still", "somebody_who_took_the_handle"])
def test_without_an_account_kept_the_bio_proves_nothing_and_the_site_still_can(world, holder):
    """The library keeps no account for the handle (a profile stored before ids were kept, say). Whoever answers to
    the handle now could be anyone, so the bio is not read: the site, or the owner, is the way."""
    site = f"https://{BRAVO}.art/"
    world.list(BRAVO, account=False)
    user = BRAVO if holder == "the_artist_still" else "yy-fake-squatter"
    world.da.profiles[BRAVO] = profile(BRAVO, user=user, bio=code(BRAVO))

    assert world.check() == expected(requests_due=1, left_for_owner_not_checked=1)
    assert world.db.confirmed == [] and not gone(world, BRAVO)

    # Listed with their site, which the handle's own profile names, and the code in its head: that proves it.
    world.list(CHARLIE, site=site.replace(BRAVO, CHARLIE), account=False)
    world.da.profiles[CHARLIE] = profile(CHARLIE, website=site.replace(BRAVO, CHARLIE))
    world.sites.pages[site.replace(BRAVO, CHARLIE)] = page(meta(code(CHARLIE)))
    world.later(hours=1)
    assert world.check() == expected(
        requests_due=2, proven_on_own_site=1, artists_removed=1, left_for_owner_not_checked=1
    )
    assert gone(world, CHARLIE) and not gone(world, BRAVO)


# On their own site: a <meta> in the front page's head, or the proof file, and nowhere else.


SITE_CASES = [
    "proven_in_the_head",
    "proven_by_the_file",
    "the_profile_names_a_page_of_the_site",
    "robots_txt_shuts_us_out",
    "profile_names_another_site",
    "a_lapsed_domain_the_profile_names_no_more",
    "not_on_record",
    "the_deviantart_account_is_gone",
]


@pytest.mark.parametrize("case", SITE_CASES)
def test_the_own_site_counts_only_on_record_named_by_the_profile_and_read_politely(world, case):
    site, other = f"https://{ALPHA}.art/", f"https://{BRAVO}.art/"
    world.list(ALPHA, site=None if case == "not_on_record" else site)
    website = {
        "profile_names_another_site": other,
        "a_lapsed_domain_the_profile_names_no_more": None,
        "the_profile_names_a_page_of_the_site": f"https://www.{ALPHA}.art/portfolio/",
    }.get(case, site)
    if case != "the_deviantart_account_is_gone":
        world.da.profiles[ALPHA] = profile(ALPHA, bio="Concept artist.", website=website)
    if case == "proven_by_the_file":
        world.sites.pages[f"{site}{FILE}"] = f"{code(ALPHA)}\n"
    else:  # whoever holds the domain now can put a tag in its head
        world.sites.pages[site] = page(meta(code(ALPHA)))
    world.sites.pages[other] = page(meta(code(ALPHA)))  # a site that holds the code: not theirs
    if case == "robots_txt_shuts_us_out":
        world.sites.pages[f"{site}robots.txt"] = "User-agent: ArtistSearchBot\nDisallow: /\n"

    counts = world.check()

    if case.startswith("proven") or case == "the_profile_names_a_page_of_the_site":
        assert counts == expected(requests_due=1, proven_on_own_site=1, artists_removed=1)
        # Through the crawler: robots.txt, then the file, then (when the file is not there) the front page.
        file_only = case == "proven_by_the_file"
        assert world.sites.urls == [f"{site}robots.txt", f"{site}{FILE}", *([] if file_only else [site])]
        assert world.db.artists == {} and store.is_removed(f"{ALPHA}.art")
        return
    left = "not_checked" if case in ("robots_txt_shuts_us_out", "the_deviantart_account_is_gone") else "no_code_found"
    assert counts == expected(requests_due=1, **{f"left_for_owner_{left}": 1})
    assert world.db.confirmed == [] and len(world.db.artists) == 1
    assert other not in world.sites.urls  # a site of somebody else's is never read for them
    # robots.txt keeps us from both places, so nothing else is asked; otherwise the site is not asked at all.
    assert world.sites.urls == ([f"{site}robots.txt"] if case == "robots_txt_shuts_us_out" else [])


VISITORS = {
    # A self-hosted blog whose front page shows its latest comments, a guestbook, a comment's author link.
    "a_recent_comment": ("", f'<aside class="recent-comments"><li>troll on Hello: {code(ALPHA)}</li></aside>'),
    "a_comment_authors_link": ("", f'<a href="https://x.example/" rel="nofollow">{code(ALPHA)}</a> says hi'),
    "a_tag_a_visitor_got_into_the_body": ("", meta(code(ALPHA))),
    "an_html_comment_in_the_body": ("", f"<!-- {code(ALPHA)} -->"),
    "the_code_in_another_tags_content": (f'<meta name="description" content="{code(ALPHA)}">', ""),
    # The critic's two: visitor text that reaches the head with its "<" kept. HTML asks only for the quote to be
    # escaped inside a quoted attribute, and JSON never escapes "<".
    "a_comment_quoted_in_og_description": (
        '<meta property="og:description" content="Latest comment: '
        f'<meta name={removals.PROOF_NAME} content={code(ALPHA)}>">',
        "",
    ),
    "a_review_the_head_carries_as_json_ld": (
        '<script type="application/ld+json">'
        + json.dumps(
            {"review": [{"reviewBody": f"Great prints! <meta name={removals.PROOF_NAME} content={code(ALPHA)}>"}]}
        )
        + "</script>",
        "",
    ),
    "the_title": (f"<title>{meta(code(ALPHA))}</title>", ""),
    "an_html_comment_in_the_head": (f"<!-- {meta(code(ALPHA))} -->", ""),
}


@pytest.mark.parametrize("place", [*VISITORS, "a_site_that_answers_every_address_with_its_front_page"])
def test_only_the_head_and_the_file_count_never_what_a_visitor_can_write(world, place):
    site = f"https://{ALPHA}.art/"
    world.list(ALPHA, site=site)
    world.da.profiles[ALPHA] = profile(ALPHA, website=site)
    if place in VISITORS:
        world.sites.pages[site] = page(*VISITORS[place])
    else:  # the file's address answers with the front page, which holds a visitor's comment
        world.sites.pages[site] = world.sites.pages[f"{site}{FILE}"] = page(body=f"<p>{code(ALPHA)}</p>")

    assert world.check() == expected(requests_due=1, left_for_owner_no_code_found=1)
    assert not gone(world, ALPHA) and world.db.confirmed == []


@pytest.mark.parametrize(
    ("spelling", "placed"),
    [
        ("upper", lambda c: page(meta(c.upper()))),
        ("spaced_in_the_tag", lambda c: page(meta(f"  {c} "))),
        ("entity_in_the_tag", lambda c: page(meta(c.replace("-", "&#45;")))),
        ("single_quotes_and_capitals", lambda c: page(f"<META NAME='Artist-Search-Removal' CONTENT='{c}'/>")),
        ("among_other_tags", lambda c: page(f'<meta charset="utf-8"><meta name="robots" content="index">{meta(c)}')),
        # The critic's shapes of real pages.
        (
            "head_end_only_in_capitals",
            lambda c: f'<HTML><HEAD><META NAME="{removals.PROOF_NAME}" CONTENT="{c}"></HEAD></HTML>',
        ),
        (
            "content_first_and_self_closing",
            lambda c: f'<html><head><meta content="{c}" name="{removals.PROOF_NAME}" /></head>',
        ),
        (
            "a_byte_order_mark_and_crlf",
            lambda c: chr(0xFEFF) + f"<!DOCTYPE html>\r\n<html>\r\n<head>\r\n{meta(c)}\r\n</head>\r\n</html>",
        ),
        (
            "after_a_pixel_in_noscript",
            lambda c: page(f'<noscript><img src="https://x.example/p.gif"></noscript>{meta(c)}'),
        ),
        ("before_a_declaration_the_parser_cannot_read", lambda c: page(f"{meta(c)}<![ broken")),
        # A browser keeps the first of an attribute written twice, and so does the reader.
        (
            "an_attribute_written_twice",
            lambda c: page(f'<meta name="{removals.PROOF_NAME}" content="{c}" content="x">'),
        ),
    ],
)
def test_the_code_however_written_in_the_head_is_found(world, spelling, placed):
    site = f"https://{ALPHA}.art/"
    world.list(ALPHA, site=site)
    world.da.profiles[ALPHA] = profile(ALPHA, website=site)
    world.sites.pages[site] = placed(code(ALPHA))

    assert world.check() == expected(requests_due=1, proven_on_own_site=1, artists_removed=1)


NO_HEAD_END = {
    # html-minifier drops the optional <head>, </head> and <body>: the tag is still in the head, before any content.
    "minified": (
        lambda c: (
            f"<!doctype html><html lang=en><meta charset=utf-8><title>Jane</title>"
            f"<meta name={removals.PROOF_NAME} content={c}><link rel=stylesheet href=/s.css><h1>Jane</h1>"
        ),
        True,
    ),
    # The same kind of page, with a visitor's comment that kept its tag: past the first content the head has ended.
    "a_comment_after_the_content": (
        lambda c: f"<!doctype html><html><title>Jane</title><h1>Jane</h1><p>{meta(c)}</p>",
        False,
    ),
    "after_the_first_words": (lambda c: f"<html><title>Jane</title>Latest comment: {meta(c)}", False),
    # Where the page says its head ends, it ends: the page tells the artist to put the tag between the two.
    "after_the_head_end": (lambda c: f"<html><head><title>Jane</title></head>{meta(c)}<body><p>Jane</p></body>", False),
    "after_the_first_element_of_the_body": (
        lambda c: f'<html><title>Jane</title><img src="/jane.png"><div class="comments">{meta(c)}</div>',
        False,
    ),
}


@pytest.mark.parametrize("shape", NO_HEAD_END)
def test_a_page_that_never_writes_where_its_head_ends_is_read_as_a_browser_reads_it(world, shape):
    site = f"https://{ALPHA}.art/"
    world.list(ALPHA, site=site)
    world.da.profiles[ALPHA] = profile(ALPHA, website=site)
    written, proven = NO_HEAD_END[shape]
    world.sites.pages[site] = written(code(ALPHA))

    found = expected(proven_on_own_site=1, artists_removed=1) if proven else expected(left_for_owner_no_code_found=1)
    assert world.check() == found | {"requests_due": 1}


def test_without_an_account_kept_a_profile_answering_under_another_name_vouches_for_no_site(world):
    """The handle's own answer is the only one that may vouch for a site: one under another name (the handle passed to
    somebody, the API answering for some other account) names a site that proves nothing, and it is not asked."""
    site = f"https://{BRAVO}.art/"
    world.list(BRAVO, site=site, account=False)
    world.da.profiles[BRAVO] = profile(BRAVO, name="yy-fake-someone-else", website=site)
    world.sites.pages[site] = page(meta(code(BRAVO)))

    assert world.check() == expected(requests_due=1, left_for_owner_not_checked=1)
    assert world.sites.urls == [] and not gone(world, BRAVO)


@pytest.mark.parametrize("handle", ["yy-fake-a/../../oauth2/token", "yy fake", "", None])
def test_a_request_whose_handle_is_not_a_handle_asks_deviantart_nothing(world, handle):
    world.list(ALPHA)
    world.db.queue[ALPHA]["da_username"] = handle
    world.da.profiles[ALPHA] = profile(ALPHA, bio=code(ALPHA))

    assert world.check() == expected(requests_due=1, left_for_owner_not_checked=1)
    assert world.da.requests == []


def test_a_site_is_asked_eight_times_at_most_for_one_proof(world):
    """However its addresses move about within it: robots.txt, then the file and the front page, each through three
    redirects. The ninth address, which holds the code, is not asked, and the request waits for its next look."""
    site = f"https://{ALPHA}.art/"
    world.list(ALPHA, site=site)
    world.da.profiles[ALPHA] = profile(ALPHA, website=site)
    for path in (FILE, ""):
        hops = [f"{site}{path}", *(f"{site}{'file' if path else 'page'}-{n}" for n in (1, 2, 3))]
        for here, there in itertools.pairwise(hops):
            world.sites.raw[here] = (302, {"location": there})
    world.sites.pages[f"{site}page-3"] = page(meta(code(ALPHA)))  # the file's last hop is not there: a 404

    assert world.check() == expected(requests_due=1, left_for_owner_not_checked=1)
    assert len(world.sites.urls) == 8 and world.sites.urls[-1] == f"{site}page-2"


@pytest.mark.parametrize("written", ["as_is", "upper_with_a_newline", "with_a_byte_order_mark", "with_more_text"])
def test_the_file_holds_the_code_and_nothing_else(world, written):
    site = f"https://{ALPHA}.art/"
    world.list(ALPHA, site=site)
    world.da.profiles[ALPHA] = profile(ALPHA, website=site)
    world.sites.pages[f"{site}{FILE}"] = {
        "as_is": code(ALPHA),
        "upper_with_a_newline": code(ALPHA).upper() + "\r\n",
        "with_a_byte_order_mark": "﻿" + code(ALPHA),
        "with_more_text": f"Remove me please: {code(ALPHA)}",
    }[written]

    counts = world.check()
    if written == "with_more_text":
        assert counts == expected(requests_due=1, left_for_owner_no_code_found=1)
    else:
        assert counts == expected(requests_due=1, proven_on_own_site=1, artists_removed=1)


REDIRECTS = ["http_to_https", "www_to_the_bare_domain", "the_bare_domain_to_www", "off_the_site", "too_many_hops"]


@pytest.mark.parametrize("case", REDIRECTS)
def test_a_redirect_is_followed_within_the_site_only_and_a_few_hops_at_most(world, case):
    bare, www, evil = f"https://{ALPHA}.art/", f"https://www.{ALPHA}.art/", "https://yy-fake-evil.art/"
    on_record, lands = {
        "http_to_https": (f"http://{ALPHA}.art/", bare),
        "www_to_the_bare_domain": (www, bare),
        "the_bare_domain_to_www": (bare, www),
        "off_the_site": (bare, evil),
        "too_many_hops": (bare, f"{bare}c"),
    }[case]
    world.list(ALPHA, site=on_record)
    world.da.profiles[ALPHA] = profile(ALPHA, website=on_record)
    # Every address of a site that has moved answers with its new one, as most servers do it. The last two cases
    # move only the front page.
    for path in ("",) if case in ("off_the_site", "too_many_hops") else ("robots.txt", FILE, ""):
        world.sites.raw[on_record + path] = (301, {"location": lands + path})
    if case == "too_many_hops":
        for hop in "cde":
            world.sites.raw[f"{bare}{hop}"] = (302, {"location": f"{bare}{chr(ord(hop) + 1)}"})
        world.sites.pages[f"{bare}f"] = page(meta(code(ALPHA)))
    world.sites.pages[lands] = page(meta(code(ALPHA)))

    counts = world.check()

    if case in ("off_the_site", "too_many_hops"):
        assert counts == expected(requests_due=1, left_for_owner_not_checked=1) and not gone(world, ALPHA)
        assert not any(url.startswith(evil) for url in world.sites.urls) and f"{bare}f" not in world.sites.urls
    else:
        assert counts == expected(requests_due=1, proven_on_own_site=1, artists_removed=1)
        assert len(world.sites.urls) <= crawl.PROOF_REQUEST_BUDGET


OPT_OUTS = {
    # Owner's lead: this one file or tag, put there for us at the artist's own request, is not AI use.
    "noai_in_the_head": {"head": '<meta name="robots" content="noai, noimageai">'},
    "noai_in_a_header": {"header": {"x-robots-tag": "noai", "content-type": "text/html"}},
    "gptbot_shut_out": {"robots": "User-agent: GPTBot\nDisallow: /\n\nUser-agent: *\nAllow: /\n"},
    "content_signal": {"robots": "User-agent: *\nContent-Signal: ai-train=no, ai-input=no\nAllow: /\n"},
    "tdm_reservation": {"tdmrep": '[{"location": "/", "tdm-reservation": 1}]'},
    "robots_keeps_us_from_the_file_only": {"robots": "User-agent: ArtistSearchBot\nDisallow: /.well-known/\n"},
    # robots.txt for our own crawler is still obeyed.
    "robots_shuts_us_out": {"robots": "User-agent: ArtistSearchBot\nDisallow: /\n"},
    "robots_shuts_everyone_out": {"robots": "User-agent: *\nDisallow: /\n"},
}


@pytest.mark.parametrize("case", OPT_OUTS)
def test_an_ai_opt_out_does_not_stop_the_proof_and_robots_txt_for_us_does(world, case):
    site = f"https://{ALPHA}.art/"
    world.list(ALPHA, site=site)
    world.da.profiles[ALPHA] = profile(ALPHA, website=site)
    said = OPT_OUTS[case]
    body = page(said.get("head", "") + meta(code(ALPHA)))
    if "header" in said:
        world.sites.raw[site] = (200, said["header"], body)
    else:
        world.sites.pages[site] = body
    if "robots" in said:
        world.sites.pages[f"{site}robots.txt"] = said["robots"]
    if "tdmrep" in said:
        world.sites.pages[f"{site}.well-known/tdmrep.json"] = said["tdmrep"]

    counts = world.check()

    assert f"{site}.well-known/tdmrep.json" not in world.sites.urls  # not even asked
    if case.startswith("robots_shuts"):
        assert counts == expected(requests_due=1, left_for_owner_not_checked=1)
        assert world.sites.urls == [f"{site}robots.txt"]
    else:
        assert counts == expected(requests_due=1, proven_on_own_site=1, artists_removed=1)


@pytest.mark.parametrize(
    "proof", ["made_with_an_old_secret", "no_code_from_the_database", "an_empty_code", "a_code_of_another_shape"]
)
def test_only_the_slugs_code_as_the_database_makes_it_now_proves_anything(world, proof):
    world.list(ALPHA)
    shape = "artist-search-removal-0123"  # in the bio, and too short to be anybody's code
    world.da.profiles[ALPHA] = profile(ALPHA, bio=f"Old code: {code(ALPHA, 'FAKE-OLD-SECRET')} {shape}")
    if proof != "made_with_an_old_secret":  # the secret's row is missing, say: removal_code() answers null
        world.db.proofs[ALPHA] = {"no_code_from_the_database": None, "an_empty_code": ""}.get(proof, shape)

    left = "no_code_found" if proof == "made_with_an_old_secret" else "not_checked"
    assert world.check() == expected(requests_due=1, **{f"left_for_owner_{left}": 1})
    assert world.db.confirmed == [] and len(world.db.artists) == 1
    assert (world.da.requests == []) == (left == "not_checked")  # a code of no use is not looked for


def test_a_code_the_database_refuses_removes_nobody_there_or_here(world):
    """The owner replaced the secret between the read of the code and the removal: the database is the judge."""
    world.list(ALPHA)
    world.db.proofs[ALPHA] = code(ALPHA, "FAKE-REPLACED-SECRET")
    world.da.profiles[ALPHA] = profile(ALPHA, bio=code(ALPHA, "FAKE-REPLACED-SECRET"))

    assert world.check() == expected(requests_due=1, proven_on_deviantart=1, proofs_refused_by_database=1)
    assert len(world.db.artists) == 1 and world.db.removed == [] and not store.is_removed(ALPHA)


def test_the_api_client_follows_no_redirect(world):
    world.list(ALPHA)
    elsewhere = "https://yy-fake-evil.example/profile.json"
    world.da.profiles[ALPHA] = httpx.Response(302, headers={"location": elsewhere})

    assert world.check() == expected(requests_due=1, left_for_owner_not_checked=1)
    assert elsewhere not in world.da.requests


# The queue: requests a code was given out for, the one due longest first, less often as they age, a few a run.


TALLY = dict.fromkeys(removals.TALLY, 0)


def test_a_request_without_a_code_is_the_owners_alone_never_looked_at_and_counted(world):
    world.list(ALPHA, code=False)  # filed with request_removal only
    world.da.profiles[ALPHA] = profile(ALPHA, bio=code(ALPHA))

    assert world.check() == expected()
    assert world.tally == TALLY | {"requests_open_without_code": 1}
    assert world.da.requests == [] and world.db.requests == [ALPHA] and world.db.moved == []

    world.db.file(ALPHA, with_code=True)  # the artist asks for their code on the site
    assert world.check() == expected(requests_due=1, proven_on_deviantart=1, artists_removed=1)
    assert world.tally == TALLY | {"requests_open_with_code": 1}  # as the run began
    world.check()
    assert world.tally == TALLY | {"requests_proven": 1}


def test_every_run_counts_the_requests_in_every_state_and_names_nobody(world, capsys, monkeypatch):
    """The critic's case: a request without proof was in no count. Each state is counted, and nothing else is said."""
    for handle in (ALPHA, BRAVO, CHARLIE, DELTA):
        world.list(handle, code=handle in (ALPHA, BRAVO))
        world.da.profiles[handle] = profile(handle, bio="Concept artist.")
    world.db.rpc("reject_removal", {"p_id": world.db.queue[DELTA]["id"]}, "secret")
    world.da.profiles[BRAVO]["bio"] = code(BRAVO)
    world.check()
    monkeypatch.setattr(config, "load_env", lambda: {"DEVIANT_ART_ID": "FAKE-ID", "DEVIANT_ART_SECRET": "FAKE-KEY"})

    assert removals.main([]) == 0  # nothing due: DeviantArt and the sites are not asked
    out = capsys.readouterr().out
    assert json.loads(out) == dict.fromkeys(removals.COUNTS, 0) | {
        "requests_open_with_code": 1,  # ALPHA
        "requests_open_without_code": 1,  # CHARLIE
        "requests_proven": 1,  # BRAVO
        "requests_closed": 1,  # DELTA, rejected
    }
    assert not any(handle in out for handle in (ALPHA, BRAVO, CHARLIE, DELTA))


def test_the_one_due_longest_comes_first_and_asking_again_puts_nobody_ahead(world, monkeypatch):
    monkeypatch.setattr(removals, "MAX_REQUESTS", 2)
    for handle in (ALPHA, BRAVO, CHARLIE):
        world.list(handle, account=False)
        world.da.profiles[handle] = profile(handle)
        world.later(minutes=1)
    for _ in range(5):  # asked for again, and again, with a code and without: none of it moves CHARLIE up
        world.db.file(CHARLIE, with_code=True)
        world.db.file(CHARLIE, with_code=False)

    assert world.check() == expected(requests_due=2, left_for_owner_not_checked=2)
    assert [slug for slug, _ in world.db.moved] == [ALPHA, BRAVO]
    world.later(minutes=10)
    assert world.check() == expected(requests_due=1, left_for_owner_not_checked=1)
    assert [slug for slug, _ in world.db.moved] == [ALPHA, BRAVO, CHARLIE]


def test_a_request_is_looked_at_less_often_as_it_ages(world):
    world.list(ALPHA)
    world.da.profiles[ALPHA] = profile(ALPHA, bio="Concept artist.")
    looked = []
    for hour in range(30 * 24):  # a run every hour for thirty days
        if world.check()["requests_due"]:
            looked.append(hour)
        world.later(hours=1)
    # Hourly for the first day, every six hours for the rest of the first two weeks, weekly after that.
    assert looked == [*range(24), *range(24, 14 * 24, 6), *range(14 * 24, 30 * 24, 7 * 24)]


def test_asking_for_the_code_again_starts_the_checks_over(world):
    """The critic's case: a code put up after the daily checks have ended. The page tells the artist to ask for the
    code again first, which makes the request due at once and hourly again. The seventh critic's uncaught breaks: the
    page's advice taken out, and start_removal_proof keeping the due time a re-ask found (RemovalsDb follows the
    statement read here)."""
    page = " ".join((config.ROOT / "web" / "src" / "pages.tsx").read_text(encoding="utf-8").split())
    advice = "Putting the code up more than two weeks after you got it? Come back to this page and press “Get my code”"
    assert f"{advice} again first: the code is the same, and the checks start over." in page
    assert "code_at = now(), due_at = least(r.due_at, now())," in function_body("start_removal_proof")
    world.list(ALPHA)
    world.da.profiles[ALPHA] = profile(ALPHA, bio="Concept artist.")
    for _ in range(20 * 24):  # twenty days of hourly runs: looked at weekly by now
        world.check()
        world.later(hours=1)
    world.db.file(ALPHA, with_code=True)  # the artist asks again, on the site
    world.da.profiles[ALPHA]["bio"] = code(ALPHA)
    assert world.check() == expected(requests_due=1, proven_on_deviantart=1, artists_removed=1)


def test_asking_for_the_code_again_while_a_run_looks_at_it_makes_it_due_at_once(world):
    """The seventh critic's race: "Get my code" pressed again, as the page asks after two weeks, while a run was
    looking at the request. The run then set its next look from the code_at it had read, a week on, over the reset,
    and the code, put up at once, was found 168 hours later. A run moves a request on only while its code_at is still
    the one it read."""
    world.list(ALPHA)
    world.da.profiles[ALPHA] = profile(ALPHA, bio="Concept artist.")
    world.later(days=15)
    world.check()  # past two weeks: looked at weekly from here
    world.later(days=7)
    served = world.da._serve

    def pressed_during_the_look(request):
        if request.url.path.endswith(f"/user/profile/{ALPHA}"):
            world.later(seconds=5)
            world.db.file(ALPHA, with_code=True)
        return served(request)

    world.da.transport = httpx.MockTransport(pressed_during_the_look)
    assert world.check() == expected(requests_due=1, left_for_owner_no_code_found=1)
    world.da.transport = httpx.MockTransport(served)
    assert when(world.db.queue[ALPHA]["due_at"]) <= when(world.db.queue[ALPHA]["code_at"])  # the reset stands
    world.da.profiles[ALPHA]["bio"] = code(ALPHA)  # put up right after the press
    world.later(seconds=removals.EVERY.total_seconds())
    assert world.check() == expected(requests_due=1, proven_on_deviantart=1, artists_removed=1)


PROMISE = "we usually find it within a few hours, and at most within a day"  # web/src/pages.tsx, step 3


@pytest.mark.parametrize("posted_after_hours", [0.1, 0.5, 5, 23.9, 24.05, 24.2, 50, 150, 200, 330, 335.9])
def test_a_code_put_up_in_the_first_two_weeks_is_found_as_the_page_promises(world, posted_after_hours):
    """The page: once the code is up, "we usually find it within a few hours, and at most within a day". The fifth
    critic's: it promised a look every hour, which nothing kept, and a page made to say "daily" went uncaught. It is
    held to the schedule here. With a run every removals.EVERY, the owner's schedule or a long run's own, at times
    that are not the code's, a code put up at any moment of its first two weeks is found within an hour and a run
    in its first day, and within six hours and a run after it."""
    assert PROMISE in " ".join((config.ROOT / "web" / "src" / "pages.tsx").read_text(encoding="utf-8").split())
    world.list(ALPHA)  # asked for now
    world.da.profiles[ALPHA] = profile(ALPHA, bio="Concept artist.")
    up = world.clock + posted_after_hours * 3600
    world.later(minutes=7)  # the runs fall where they fall
    while not gone(world, ALPHA):
        if world.clock >= up:
            world.da.profiles[ALPHA]["bio"] = f"Concept artist. {code(ALPHA)}"
        world.check()
        world.later(seconds=removals.EVERY.total_seconds())
    waited = (world.clock - removals.EVERY.total_seconds() - up) / 3600
    assert 0 <= waited <= (1 if posted_after_hours < 23 else 6) + removals.EVERY.total_seconds() / 3600 < 24


def test_the_page_tells_the_artist_the_tag_and_the_file_this_stage_reads():
    """The fifth critic's uncaught breaks: the page telling artists to write <meta property=...>, or to put the file
    at another address, would send them to put the code where it is never read. The page's own words are read here,
    against the name this stage reads and the tag and file that the tests above show it finds."""
    page = (config.ROOT / "web" / "src" / "pages.tsx").read_text(encoding="utf-8")
    assert f"const PROOF = '{removals.PROOF_NAME}'" in page
    assert 'copyRow(`<meta name="${PROOF}" content="${code}">`)' in page
    assert "copyRow(new URL(`/.well-known/${PROOF}.txt`, site).href)" in page
    assert meta("${code}") == '<meta name="artist-search-removal" content="${code}">'
    assert FILE == ".well-known/artist-search-removal.txt" and removals.PROOF_NAME == "artist-search-removal"
    # The eighth critic's: the page promising the tagline alone where the bio (the About text) is read too, and the
    # page no longer saying that robots.txt for our crawler keeps the check out (as the opt-out test above shows).
    said = " ".join(page.split())
    assert "paste it into the About text or the tagline of" in said and removals.PROFILE_FIELDS == ("bio", "tagline")
    assert f"and unless your robots.txt keeps out our crawler, {config.ROBOTS_TOKEN}." in said


def test_a_run_looks_at_no_more_than_its_share_and_the_rest_next_run(world):
    for n in range(40):
        world.list(f"yy-fake-flood{n:02d}", account=False)
        world.da.profiles[f"yy-fake-flood{n:02d}"] = profile(f"yy-fake-flood{n:02d}")
        world.later(seconds=1)

    assert world.check() == expected(requests_due=removals.MAX_REQUESTS, left_for_owner_not_checked=30)
    assert len(world.da.requests) == 1 + removals.MAX_REQUESTS  # the token, and one lookup each
    world.later(minutes=15)
    assert world.check() == expected(requests_due=10, left_for_owner_not_checked=10)


def test_a_flood_filed_ahead_of_a_real_request_holds_it_up_one_run_at_most(world):
    for n in range(40):  # filed by a script before the artist came
        world.list(f"yy-fake-flood{n:02d}", account=False)
        world.da.profiles[f"yy-fake-flood{n:02d}"] = profile(f"yy-fake-flood{n:02d}")
    world.later(minutes=5)
    world.list(ALPHA)
    world.da.profiles[ALPHA] = profile(ALPHA, bio=code(ALPHA))

    assert world.check() == expected(requests_due=30, left_for_owner_not_checked=30)
    world.later(minutes=15)
    assert world.check() == expected(
        requests_due=11, left_for_owner_not_checked=10, proven_on_deviantart=1, artists_removed=1
    )
    assert gone(world, ALPHA)


def test_codes_asked_for_from_a_few_networks_hold_back_only_their_own(world):
    """The final critic's flood: a real code behind 600 asked for from a few networks was looked at after 4.75 hours
    of 15-minute runs, and the more codes, the later. The checks take turns between the networks the codes were
    asked from (due_removals): the real one is in the first run, however many the others asked for."""
    for n in range(120):  # filed by a script on four networks, before the artist came
        handle = f"yy-fake-flood{n:03d}"
        world.list(handle, account=False, code=False)
        world.db.file(handle, with_code=True, network=f"FAKE-NETWORK-{n % 4}")
        world.da.profiles[handle] = profile(handle)
    world.later(minutes=5)
    world.list(ALPHA, code=False)
    world.db.file(ALPHA, with_code=True, network="FAKE-NETWORK-OF-ALPHA")
    world.da.profiles[ALPHA] = profile(ALPHA, bio=code(ALPHA))

    assert world.check() == expected(
        requests_due=30, left_for_owner_not_checked=29, proven_on_deviantart=1, artists_removed=1
    )
    assert gone(world, ALPHA)
    looked = [slug for slug, _ in world.db.moved]
    assert looked[:4] == [f"yy-fake-flood{n:03d}" for n in range(4)]  # each network's one due longest, in turn
    assert {int(slug[-3:]) % 4 for slug in looked if slug} == {0, 1, 2, 3}


def test_newer_requests_filed_again_and_a_throttled_api_do_not_starve_the_oldest(world):
    """The critic's case: 40 requests filed after the real one, and filed again; DeviantArt answers 429 after ten
    lookups. The real request is the one due longest, so it is looked at first."""
    world.list(ALPHA)
    world.da.profiles[ALPHA] = profile(ALPHA, bio=code(ALPHA))
    others = [f"yy-fake-other{n:02d}" for n in range(40)]
    for handle in others:
        world.later(seconds=1)
        world.list(handle, account=False)
        world.da.profiles[handle] = profile(handle)
    for handle in others:
        world.db.file(handle, with_code=True)
    served = world.da._serve

    def throttled(request):
        if "/user/profile/" in request.url.path and len(world.da.requests) > 10:
            world.da.requests.append(str(request.url))
            return httpx.Response(429)
        return served(request)

    world.da.transport = httpx.MockTransport(throttled)
    with pytest.raises(discover.Stop, match="rate limited"):
        world.check()
    assert gone(world, ALPHA)


def test_a_request_without_proof_waits_and_a_code_posted_after_a_look_is_found_by_the_next(world):
    world.list(ALPHA)
    world.da.profiles[ALPHA] = profile(ALPHA, bio="Concept artist.")

    assert world.check() == expected(requests_due=1, left_for_owner_no_code_found=1)
    assert world.db.requests == [ALPHA] and len(world.db.artists) == 1 and not store.is_removed(ALPHA)
    assert world.check() == expected()  # looked at a moment ago: not due again for an hour

    world.da.profiles[ALPHA]["bio"] += f" {code(ALPHA)}"
    world.later(hours=1)
    assert world.check() == expected(requests_due=1, proven_on_deviantart=1, artists_removed=1)
    assert world.db.requests == [] and world.db.artists == {} and library.account_removed(f"FAKE-USER-{ALPHA}")
    world.later(hours=1)
    assert world.check() == expected()


def hold_back(world, handle):
    """A push that holds the artist back: their row goes from the database, as push.run deletes it."""
    [artist_id] = [key for key, row in world.db.artists.items() if row["slug"] == handle]
    row = world.db.artists[artist_id]
    world.db.sql(f"delete from public.artists where id in ({artist_id})")
    return row


@pytest.mark.parametrize("site", [None, f"https://{ALPHA}.art/"])
def test_a_request_outlives_its_artists_row_and_their_code_is_still_looked_for(world, site):
    """The critic's case: the artist asks for their code, a push holds them back for a day, and the code goes up the
    day after. The request was made for their handle and site, which it keeps: the code is found while they are not
    listed, they are removed for good, and the push that would list them again cannot."""
    world.list(ALPHA, site=site)
    world.da.profiles[ALPHA] = profile(ALPHA, bio="Concept artist.", website=site)
    world.check()  # looked at once: no code yet
    world.later(hours=20)
    row = hold_back(world, ALPHA)
    assert world.db.requests == [ALPHA]  # nothing a push does touches it
    world.later(hours=10)
    if site:
        world.sites.pages[site] = page(meta(code(ALPHA)))
    else:
        world.da.profiles[ALPHA]["bio"] = f"Concept artist. {code(ALPHA)}"

    where = "own_site" if site else "deviantart"
    assert world.check() == expected(requests_due=1, artists_removed=1, **{f"proven_on_{where}": 1})
    assert world.db.requests == [] and world.tally == TALLY | {"requests_open_with_code": 1}
    assert library.account(f"FAKE-USER-{ALPHA}") in world.db.removed and library.account_removed(f"FAKE-USER-{ALPHA}")
    world.db.load([row], [], [])  # the next push lists them again, as it would have
    assert gone(world, ALPHA)


def test_a_push_that_holds_the_artist_back_leaves_their_request_and_its_place(world):
    artist(ALPHA, angle=0.0)
    artist(BRAVO, angle=0.5)
    push.run(False)
    world.db.file(BRAVO, with_code=True)
    world.db.file(ALPHA, with_code=False, note="It is me.")
    library.mark_artist(f"da:{BRAVO}", "account gone")  # held back from now on: the push deletes their row
    before = json.dumps(world.db.queue)

    push.run(False, allow_delete=1)
    assert gone(world, BRAVO) and json.dumps(world.db.queue) == before


def test_a_renamed_account_that_answers_only_to_its_new_handle_is_still_found(world):
    """The request was made for the old handle. DeviantArt no longer answers to it, or somebody else took it: the
    account's other handles kept here are asked, and the profile of the account the library keeps is read."""
    artist(BRAVO)
    library.mark_artist(f"da:{BRAVO}", "account gone")
    artist(CHARLIE, user=BRAVO, old_keys=[f"da:{BRAVO}"])  # the collect stage links the two handles
    world.list(BRAVO)
    hold_back(world, BRAVO)  # the push lists them under the new handle only
    world.da.profiles[CHARLIE] = profile(CHARLIE, user=BRAVO, bio=code(BRAVO))

    assert world.check() == expected(requests_due=1, proven_on_deviantart=1, artists_removed=1)
    assert [url.rsplit("/", 1)[1] for url in world.da.requests[1:]] == [BRAVO, CHARLIE]

    world.list(DELTA, account=True)  # a squatter holds the old handle now: its answer is not the artist's
    world.da.profiles[DELTA] = profile(DELTA, user="yy-fake-squatter", bio=code(DELTA))
    assert world.check() == expected(requests_due=1, left_for_owner_not_checked=1)


@pytest.mark.parametrize("place", ["deviantart", "own_site"])
def test_a_proof_bars_the_account_and_no_handle_by_name_not_the_old_one_somebody_else_holds(world, place):
    """The eighth critic's: BRAVO's account renamed itself CHARLIE, and DeviantArt gave BRAVO to a stranger. The artist
    proved under CHARLIE, and the proof barred BRAVO too, the request's handle: the stranger was barred there and here
    for good. A proof bars the account the request is bound to and no handle by name, on DeviantArt or on the artist's
    own site (the ninth critic's uncaught break: an own-site proof that bars the request's handle). The account bars
    the artist under every handle, so the pushes after it purge both handles' data here and bar neither handle by
    name: the account may leave CHARLIE too, before the collect stage sees it, and DeviantArt give it to anybody."""
    site = f"https://{CHARLIE}.art/" if place == "own_site" else None
    artist(ALPHA, angle=0.0)
    artist(BRAVO, angle=0.2)
    library.mark_artist(f"da:{BRAVO}", "account gone")
    artist(CHARLIE, angle=0.21, user=BRAVO, old_keys=[f"da:{BRAVO}"], website=site)  # the collect stage links them
    world.list(BRAVO, site=site)
    hold_back(world, BRAVO)
    world.da.profiles[BRAVO] = profile(BRAVO, user="yy-fake-stranger")  # somebody else holds the old handle
    world.da.profiles[CHARLIE] = profile(CHARLIE, user=BRAVO, bio="" if site else code(BRAVO), website=site)
    if site:
        world.sites.pages[site] = page(meta(code(BRAVO)))

    assert world.check() == expected(requests_due=1, artists_removed=1, **{f"proven_on_{place}": 1})
    account = library.account(f"FAKE-USER-{BRAVO}")
    barred = {account, *([digest(f"site:{CHARLIE}.art")] if site else [])}
    assert set(world.db.removed) == barred == store.removed_hashes()
    for run in range(2):  # the pushes add nothing: the account bars them, there and here
        assert push.run(False)["artists_purged_here"] == (2 if run == 0 else 0)
        assert set(world.db.removed) == barred == store.removed_hashes()
    assert library.artist_keys() == [f"da:{ALPHA}"] and sorted(sent_artists(world.db)) == [ALPHA]
    # Never taken in again under any handle: the collect stage asks every profile's account of the list, and the
    # database refuses a row with it.
    assert library.account_removed(f"FAKE-USER-{BRAVO}")
    row = {"slug": DELTA, "name": DELTA, "da_username": DELTA, "site_url": None, "site_domain": None}
    world.db.load([row | {"da_account": account}], [], [])  # the account under yet another handle
    assert gone(world, DELTA)
    # And whoever holds either handle later is listed like anybody, here and there.
    assert store.upsert_artist(BRAVO, f"https://www.deviantart.com/{BRAVO}", None, 9)
    assert store.upsert_artist(CHARLIE, f"https://www.deviantart.com/{CHARLIE}", None, 9)
    for handle in (BRAVO, CHARLIE):
        row = {"slug": handle, "name": handle, "da_username": handle, "site_url": None, "site_domain": None}
        world.db.load([row | {"da_account": library.account(f"FAKE-USER-{STRANGER}-{handle}")}], [], [])
        assert not gone(world, handle)


LEFT, STRANGER = "yy-fake-newname", "yy-fake-stranger"  # a handle one account left and DeviantArt gave to another


def test_a_proof_under_a_handle_another_kept_artist_left_takes_the_prover_and_nobody_else(world):
    """The ninth critic's probe 2: S was kept here as LEFT, renamed DELTA (the collect stage linked the two), and
    DeviantArt gave LEFT to ALPHA's account, which answers under it and proves with its own bio. The proof barred
    LEFT by name, and the next push, and every collect run's sweep, read that hash as S's old key, took S's account,
    and purged and barred S under every handle, here and there. The proof bars ALPHA's account alone, and the sweep
    and the push take ALPHA, and nobody else."""
    artist(LEFT, angle=0.4, user=STRANGER)
    library.mark_artist(f"da:{LEFT}", "account gone")
    artist(DELTA, angle=0.41, user=STRANGER, old_keys=[f"da:{LEFT}"])
    artist(ALPHA, angle=0.0)
    artist(CHARLIE, angle=0.9)
    push.run(False)
    world.db.file(ALPHA, with_code=True)
    world.da.profiles[ALPHA] = profile(LEFT, user=ALPHA, bio=f"Please remove me. {code(ALPHA)}")

    assert world.check() == expected(requests_due=1, proven_on_deviantart=1, artists_removed=1)
    assert world.db.removed == [library.account(f"FAKE-USER-{ALPHA}")] and store.removed_hashes() == {
        library.account(f"FAKE-USER-{ALPHA}")
    }
    counts = dict.fromkeys(collect._COUNTS, 0)
    collect._sweep(counts, None)  # what every collect run does at both ends: the account, under the key it holds
    assert counts[collect.PURGED] == 1 and f"da:{ALPHA}" not in library.artist_keys()
    for _ in range(2):
        assert push.run(False)["artists_purged_here"] == 0
        assert sorted(sent_artists(world.db)) == [CHARLIE, DELTA]
    theirs = library.account(f"FAKE-USER-{STRANGER}")
    assert theirs not in world.db.removed and not library.account_removed(f"FAKE-USER-{STRANGER}")
    assert not store.is_removed(DELTA) and not store.is_removed(LEFT)
    assert {f"da:{DELTA}", f"da:{LEFT}"} <= set(library.artist_keys())


def test_the_owners_grant_bars_the_account_and_not_the_old_handle_a_stranger_now_holds(world):
    """The ninth critic's probe 1: grant_request fell back to the request's own handle, and the owner's --grant sends
    none. BRAVO asked without proof, then renamed to CHARLIE (the collect stage linked the two), and DeviantArt gave
    BRAVO to a stranger, whom a push listed under the slug. The owner's grant deleted the stranger and barred them
    for good. It bars the account the request is bound to, BRAVO's under any handle, and no handle by name."""
    artist(BRAVO, angle=0.0)
    artist(ALPHA, angle=0.8)  # somebody else, so the library is never empty
    world.list(BRAVO, code=False)
    request = world.db.queue[BRAVO]["id"]
    library.mark_artist(f"da:{BRAVO}", "account gone")
    artist(CHARLIE, angle=0.2, user=BRAVO, old_keys=[f"da:{BRAVO}"])
    theirs = library.account(f"FAKE-USER-{STRANGER}")
    row = {"slug": BRAVO, "name": BRAVO, "da_username": BRAVO, "site_url": None, "site_domain": None}
    world.db.load([row | {"da_account": theirs}], [], [])  # the stranger, listed under the slug

    assert f"handle {BRAVO}" in removals.pending()[0]
    assert removals.answer([request], []) == {"granted": 1, "rejected": 0, "not_granted": 0}  # bound: no look
    account = library.account(f"FAKE-USER-{BRAVO}")
    assert world.db.removed == [account] and not gone(world, BRAVO)  # the stranger stays
    assert library.account_removed(f"FAKE-USER-{BRAVO}") and not store.is_removed(BRAVO)
    push.run(False)  # the push after it: the artist here under both handles, by the account; the stranger nowhere
    assert world.db.removed == [account] and library.artist_keys() == [f"da:{ALPHA}"]
    assert store.upsert_artist(BRAVO, f"https://www.deviantart.com/{BRAVO}", None, 9)
    # The statement RemovalsDb.grant follows (tests/test_db.py runs it): the account, and without one the request's
    # own handle only when the caller says the profile answers under it now. Nothing to bar, nothing done.
    granting = function_body("grant_request")
    assert (
        "lateral (select coalesce(r.da_account, case when public.key_hash('da:' || lower(p_handle)) = r.da_key "
        "then r.da_key end) as who) w" in granting
    )
    assert "if keys[1] is null then return null; end if;" in granting


def test_the_owners_grant_bars_a_handle_only_while_its_profile_answers_under_it(world, capsys):
    """For a request bound to no account the owner's grant can only bar a handle, which DeviantArt gives to somebody
    else once an account has left it. It says so, looks at the profile afresh, and bars the handle only while the
    profile answers under it, with the account kept here for it if one is. Otherwise, or when it cannot look, it
    grants nothing, leaves the request open, and names it to the owner by its id."""
    for handle in (ALPHA, BRAVO, CHARLIE):
        world.list(handle, account=False, code=False)
    artist(DELTA)  # kept here with its account, though its request was filed without it
    world.list(DELTA, code=False)
    world.db.queue[DELTA]["da_account"] = None
    world.da.profiles[ALPHA] = profile(ALPHA)  # still theirs, as far as anything here can tell
    world.da.profiles[BRAVO] = profile("yy-fake-renamed", user=BRAVO)  # renamed: the handle is free for anybody
    world.da.profiles[DELTA] = profile(DELTA, user=STRANGER)  # the handle passed to another account
    ids = {handle: world.db.queue[handle]["id"] for handle in (ALPHA, BRAVO, CHARLIE, DELTA)}  # CHARLIE: no answer
    capsys.readouterr()

    answered = removals.answer(list(ids.values()), [], transport=world.da.transport)
    assert answered == {"granted": 1, "rejected": 0, "not_granted": 3}
    assert world.db.removed == [digest(f"da:{ALPHA}")] and world.db.requests == [BRAVO, CHARLIE, DELTA]
    said = capsys.readouterr().err.splitlines()
    assert said[0] == f"request {ids[ALPHA]}: bound to no account, so its handle alone is barred."
    assert [line.split(":")[0] for line in said[1:]] == [f"request {ids[handle]}" for handle in (BRAVO, CHARLIE, DELTA)]
    assert all("not granted, and left open" in line for line in said[1:]) and BRAVO not in "".join(said)

    world.da.profiles[BRAVO] = profile(BRAVO)
    with discover.Pace(discover.BASE_INTERVAL, "the API"):  # another run holds the API: no look can be made
        assert removals.answer([ids[BRAVO]], [], transport=world.da.transport)["not_granted"] == 1
    assert removals.answer([ids[BRAVO]], [], transport=world.da.transport)["granted"] == 1
    assert digest(f"da:{BRAVO}") in world.db.removed and world.db.requests == [CHARLIE, DELTA]


def test_a_request_whose_artist_is_removed_another_way_is_closed_and_keeps_no_handle(world):
    world.list(ALPHA, code=False)
    world.db.sql(push._removed_sql({digest(f"da:{ALPHA}")}))  # removed by the owner's hand, or under another handle

    assert world.db.requests == [] and world.check() == expected()
    assert world.tally == TALLY | {"requests_closed": 1}
    assert ALPHA not in json.dumps(world.db.queue)
    # The eighth critic's uncaught break: a closed request that keeps its notes (RemovalsDb.close follows this).
    assert (
        "set artist_slug = 'closed:' || gen_random_uuid(), outcome = p_outcome, note = null, notes = '[]', "
        "da_username = null, site_url = null, site_domain = null, da_account = null, due_at = null, networks = '{}'"
        in function_body("close_request")
    )


def test_the_owner_lists_the_open_requests_by_handle_and_answers_them(world, capsys):
    world.list(ALPHA, code=False)
    world.later(minutes=5)
    world.db.file(ALPHA, with_code=False, note="It is me, please take it down.\x1b[2J")  # a note can hold anything
    world.list(BRAVO, site=f"https://{BRAVO}.art/")
    world.list(CHARLIE, code=False)
    hold_back(world, BRAVO)  # not listed at the moment: answered all the same
    before = json.dumps([world.db.queue, world.db.artists, world.db.removed])

    assert removals.main(["--pending"]) == 0
    lines = capsys.readouterr().out.splitlines()
    ids = {handle: world.db.queue[handle]["id"] for handle in (ALPHA, BRAVO, CHARLIE)}
    heads = [line for line in lines if not line.startswith(" ")]
    assert [line.split()[:3] for line in heads] == [[handle, "id", str(ids[handle])] for handle in ids]
    assert "2 times  code given out: no  notes: 1" in lines[0] and "code given out: yes  notes: 0" in lines[2]
    # The owner reads the note with its time, and nothing in it reaches the terminal as a control character.
    assert lines[1] == "    2026-10-14T12:05  'It is me, please take it down.\\x1b[2J'"
    assert json.dumps([world.db.queue, world.db.artists, world.db.removed]) == before

    answers = ["--grant", str(ids[BRAVO]), "999999", "--reject", str(ids[CHARLIE]), str(ids[ALPHA]), "999999"]
    assert removals.main(answers) == 0
    # No open request: not counted.
    assert json.loads(capsys.readouterr().out) == {"granted": 1, "rejected": 2, "not_granted": 0}
    assert world.db.requests == [] and not gone(world, ALPHA) and not gone(world, CHARLIE)
    account = library.account(f"FAKE-USER-{BRAVO}")
    assert set(world.db.removed) == {account, digest(f"site:{BRAVO}.art")}  # the account the request is bound to
    assert library.account_removed(f"FAKE-USER-{BRAVO}") and store.is_removed(f"{BRAVO}.art")
    assert not store.is_removed(ALPHA) and not library.account_removed(f"FAKE-USER-{ALPHA}")
    assert world.check() == expected() and world.tally == TALLY | {"requests_closed": 3}
    assert not any(handle in json.dumps(world.db.queue) for handle in (ALPHA, BRAVO, CHARLIE))


def test_a_dry_run_says_what_a_run_would_remove_and_changes_nothing(world):
    world.list(ALPHA)
    world.da.profiles[ALPHA] = profile(ALPHA, bio=code(ALPHA))

    def there():
        return json.dumps([world.db.artists, world.db.removed, world.db.queue])

    before = local_state(), there()
    assert world.check(dry_run=True) == expected(requests_due=1, proven_on_deviantart=1, artists_removed=1)
    assert (local_state(), there()) == before and world.db.confirmed == [] and world.db.moved == []


def test_the_api_is_held_one_request_at_a_time_and_a_collect_run_that_takes_it_stops_this_one(world, monkeypatch):
    for handle in (ALPHA, BRAVO, CHARLIE):
        world.list(handle)
        world.da.profiles[handle] = profile(handle)
        world.later(seconds=1)
    turns, wait = [], discover.Pace.wait

    def waited(pace):
        turns.append(len(world.da.requests))  # how many requests had gone before this wait
        wait(pace)

    monkeypatch.setattr(discover.Pace, "wait", waited)
    world.check()
    assert turns == [0, 1, 2, 3] and len(world.da.requests) == 4  # the token, then one profile each, in turn

    # A collect run starts while this run reads the database between two lookups: it gets the API, and this run
    # stops before its next lookup. What it looked at is moved on; the rest stays due.
    world.later(hours=1)
    world.da.requests.clear()
    world.db.moved.clear()
    collect, took = discover.Pace(discover.BASE_INTERVAL, "the API"), []
    rpc = world.db.rpc

    def collect_starts_after_the_first_lookup(name, args, key):
        if name == "removal_code" and len(world.da.requests) == 2:  # the token, and the first lookup
            collect.__enter__()
            took.append("the API")
        return rpc(name, args, key)

    monkeypatch.setattr(world.db, "rpc", collect_starts_after_the_first_lookup)
    try:
        with pytest.raises(discover.Stop, match="another run is talking to the API"):
            world.check()
    finally:
        collect.__exit__(None, None, None)
    assert took == ["the API"]  # the collect run got it: this run held nothing between two lookups
    assert len(world.da.requests) == 2 and [slug for slug, _ in world.db.moved] == [ALPHA]

    # And a run that cannot have the API at all asks it nothing, and reads no site.
    world.da.requests.clear()
    with discover.Pace(discover.BASE_INTERVAL, "the API"):
        with pytest.raises(discover.Stop, match="another run is talking to the API"):
            world.check()
    assert world.da.requests == [] and world.sites.urls == [] and world.db.confirmed == []


def test_a_proof_found_then_a_push_purges_them_here_and_never_sends_them_again(world):
    artist(ALPHA, angle=0.0)
    removed = artist(BRAVO, angle=0.01, website=f"https://{BRAVO}.art/")
    artist(CHARLIE, angle=0.5)
    push.run(False)
    assert sorted(sent_artists(world.db)) == [ALPHA, BRAVO, CHARLIE]
    world.db.file(BRAVO, with_code=True)
    world.da.profiles[BRAVO] = profile(BRAVO, website=f"https://{BRAVO}.art/")
    world.sites.pages[f"https://{BRAVO}.art/"] = page(meta(code(BRAVO)))

    assert world.check() == expected(requests_due=1, proven_on_own_site=1, artists_removed=1)
    assert not any(word in world.db.held() for word in of_artist(BRAVO))

    for run in range(2):  # the push takes it as the database's own removal: no guard holds it up
        counts = push.run(False)
        assert counts["artists_purged_here"] == (1 if run == 0 else 0) and counts["artists_sent"] == 2
        assert sorted(sent_artists(world.db)) == [ALPHA, CHARLIE]
        assert not any(word in world.db.held() for word in of_artist(BRAVO))
    assert library.artist_keys() == [f"da:{ALPHA}", f"da:{CHARLIE}"]
    assert not any(path.name.startswith(work["id"]) for work in removed for path in library.image_dir().iterdir())
    assert library.account(f"FAKE-USER-{BRAVO}") in world.db.removed  # their account, under any handle it takes


def test_a_proof_bars_its_site_and_nobody_else_who_names_it_here_or_in_the_database(world):
    """The final critic's: ALPHA, listed with shared.art, asks for a code. BRAVO, collected later, names the same site,
    and the push gives it to neither. ALPHA proves in their own bio, so shared.art is barred with them. The store then
    took the two who name the site for one person and barred BRAVO's handle, which the next push wrote to the
    database with BRAVO's account, for good. A proof bars only what its request is bound to, and the fifth critic's:
    a site barred holds back nobody who names it. BRAVO is sent without it, and nothing of theirs is barred."""
    shared = "https://yy-fake-shared.art/"
    artist(ALPHA, angle=0.0, website=shared)
    artist(CHARLIE, angle=0.5)
    push.run(False)
    assert sent_artists(world.db)[ALPHA]["site_domain"] == "yy-fake-shared.art"
    world.db.file(ALPHA, with_code=True)
    artist(BRAVO, angle=0.3, website=shared)
    push.run(False)
    assert sent_artists(world.db)[BRAVO]["site_domain"] is None  # a site two artists give is neither's
    world.da.profiles[ALPHA] = profile(ALPHA, bio=code(ALPHA), website=shared)

    assert world.check() == expected(requests_due=1, proven_on_deviantart=1, artists_removed=1)
    assert digest("site:yy-fake-shared.art") in world.db.removed and barred(world, ALPHA)
    for _ in range(2):
        counts = push.run(False)
        assert not barred(world, BRAVO) and not store.is_removed(BRAVO)
        assert not library.account_removed(f"FAKE-USER-{BRAVO}") and sorted(sent_artists(world.db)) == [BRAVO, CHARLIE]
        assert sent_artists(world.db)[BRAVO]["site_url"] is None and "yy-fake-shared" not in world.db.held()
        assert counts["site_links_left_out_removed"] == 1
    assert library.artist_keys() == [f"da:{BRAVO}", f"da:{CHARLIE}"]


def test_an_artist_who_names_a_site_barred_for_another_is_sent_without_it_and_with_the_next_site_they_name(world):
    """The fifth critic's: ALPHA, listed with shared.art, asks for a code; BRAVO, collected later, names the same
    site, which the collect stage writes into BRAVO's row; ALPHA proves on DeviantArt. Every push after it dropped
    BRAVO, counted under no reason: the store's row, which links the barred site, kept BRAVO out of the list the push
    and the collect stage read. So BRAVO's next website was never read, and BRAVO stayed out for good. BRAVO is
    sent without the site from the next push, read again, and sent with the next site BRAVO names."""
    shared, elsewhere = "https://yy-fake-shared.art/", "https://yy-fake-bravo-new.art/"
    artist(ALPHA, website=shared, angle=0.0)
    artist(CHARLIE, angle=0.8)
    push.run(False)
    world.db.file(ALPHA, with_code=True)
    made = artist(BRAVO, website=shared, angle=0.4)
    assert store.keep_own_site(BRAVO, shared)  # what the collect stage writes for a site of their own
    push.run(False)
    world.da.profiles[ALPHA] = profile(ALPHA, bio=f"Leaving. {code(ALPHA)}", website=shared)
    assert world.check() == expected(requests_due=1, proven_on_deviantart=1, artists_removed=1)

    counts = push.run(False)
    assert counts["site_links_left_out_removed"] == 1 and sorted(sent_artists(world.db)) == [BRAVO, CHARLIE]
    assert sent_artists(world.db)[BRAVO]["site_url"] is None and not barred(world, BRAVO)
    assert f"da:{BRAVO}" in [key for key, _, _ in store.all_artists()]  # so the collect stage reads them again
    collected_again(BRAVO, made, website=elsewhere)  # and keeps their new website (tests/test_collect.py)
    counts = push.run(False)
    assert counts["site_links_left_out_removed"] == 0 and sent_artists(world.db)[BRAVO]["site_url"] == elsewhere


def test_every_note_is_kept_with_its_time_and_a_stranger_cannot_take_the_artists_away(world):
    """The fifth critic's: the first request's note won, so a stranger's first try, with no note, hid the artist's
    own words from the owner. The sixth's: then the latest won, so a stranger's later note took the artist's place.
    The seventh's: then the first and the latest four were kept, so a stranger who wrote first owned the one kept for
    good, and four more notes of theirs pushed the artist's out. A note that says something takes the place of its
    own network's earlier one, cut to 300 characters, with when it came: the latest from each network, of the ten
    heard from last (request_removal in sql/002_functions.sql, which RemovalsDb follows: the statement is read
    below). So a stranger needs ten networks to put the artist's note out, and the artist writing again brings it
    back."""
    world.list(ALPHA, code=False)  # a stranger's, with no note
    world.db.file(ALPHA, with_code=False, note="Please keep this profile up, I love it.", network="stranger")  # first
    world.later(hours=3)
    words = f"This is my work and I would like it taken down. {'x' * 400}"
    for note in ("   ", words, None, ""):
        world.db.file(ALPHA, with_code=False, note=note, network="artist")
    for n in range(6):  # the stranger again, hour after hour
        world.later(hours=1)
        world.db.file(ALPHA, with_code=False, note=f"Keep my profile up! {n}", network="stranger")
    kept = world.db.queue[ALPHA]["notes"]
    assert [note["note"] for note in kept] == [words[:300], "Keep my profile up! 5"]
    assert kept[0]["at"] == datetime.fromtimestamp(START + 3 * 3600, UTC).isoformat()
    assert world.db.queue[ALPHA]["requests"] == 12
    for n in range(8):  # eight networks more: ten heard from, the artist's among them
        world.db.file(ALPHA, with_code=False, note=f"From elsewhere {n}", network=f"elsewhere {n}")
    lines = removals.pending()[0].splitlines()
    assert lines[0].endswith("notes: 10") and lines[1] == f"    2026-10-14T15:00  {words[:300]!r}" and len(lines) == 11
    world.db.file(ALPHA, with_code=False, note="From elsewhere 8", network="elsewhere 8")  # an eleventh
    assert words[:300] not in [note["note"] for note in world.db.queue[ALPHA]["notes"]]
    world.db.file(ALPHA, with_code=False, note=words, network="artist")  # the artist writes again
    assert [note["note"] for note in world.db.queue[ALPHA]["notes"]][-1:] == [words[:300]]
    assert len(world.db.queue[ALPHA]["notes"]) == 10
    body = function_body("request_removal")
    assert "left(nullif(btrim(p_note), ''), 300)" in body
    assert "jsonb_build_object('at', now(), 'note', said, 'network', heard)" in body
    assert "max(i) over (partition by coalesce(e -> 'network', to_jsonb(i))) as latest" in body
    assert "where n.i = n.latest order by n.i desc limit 10" in body


@pytest.mark.parametrize("asked", ["after_the_artist", "before_the_artist"])
def test_a_code_asked_for_from_another_network_takes_nothing_from_the_artist(world, asked):
    """The sixth critic's: asking for a code again moved the request into the asker's network's turn, so ten networks
    flooding for two weeks, each asking for the artist's code every hour, held a real proof 18.5 hours. A request is
    in the turns of every network that asked for it and is taken in the first it comes to (due_removals, which
    RemovalsDb follows: the statements are read below). Asked for by the flood before the artist or after, it is
    looked at in the artist's network's first turn, ahead of the flood's backlog."""
    for n in range(2 * removals.MAX_REQUESTS):  # one network's backlog, all of it due before the artist's request
        world.list(f"yy-fake-f{n:02d}", account=False, code=False)
        world.db.file(f"yy-fake-f{n:02d}", with_code=True, network="flood")
    world.later(seconds=1)
    world.list(ALPHA, code=False)
    for network in ("artist", "flood") if asked == "after_the_artist" else ("flood", "artist"):
        world.db.file(ALPHA, with_code=True, network=network)
    world.da.profiles[ALPHA] = profile(ALPHA, bio=code(ALPHA))

    assert world.check() == expected(
        requests_due=removals.MAX_REQUESTS,
        left_for_owner_not_checked=removals.MAX_REQUESTS - 1,
        proven_on_deviantart=1,
        artists_removed=1,
    )
    sql = " ".join((config.ROOT / "sql" / "002_functions.sql").read_text(encoding="utf-8").split())
    assert "else r.networks || excluded.networks end;" in sql and "select q.id, min(q.turn) as turn" in sql
    # The eighth critic's uncaught breaks: turns taken by request, not by network, and fewer networks kept than a run
    # looks at.
    turns = function_body("due_removals")
    assert "left join lateral unnest(r.networks) as n (network) on true" in turns
    assert "row_number() over (partition by n.network order by r.due_at, r.code_at, r.id) as turn" in turns
    kept = f"networks = case when cardinality(r.networks) >= {removals.MAX_REQUESTS} or excluded.networks"
    assert kept in function_body("start_removal_proof")


def test_an_account_hash_two_unlinked_keys_hold_is_sent_for_neither_and_a_proof_takes_nobody_else(world):
    """The sixth critic's: a broken store gives BRAVO's profile ALPHA's account hash, and no rename mark links the two.
    Push sent both rows with it; ALPHA's own proof barred it, and the database deleted BRAVO too, for good, before
    the next push could refuse the store. That account is sent for neither now (the rule _removal holds the store
    to), and counted. An account kept under two handles a rename mark links is one artist's, and is sent. ALPHA's
    code in the bio is then not read (a request bound to no account), and the owner's grant bars ALPHA's handle
    alone, once ALPHA's profile is seen to answer under it still."""
    artist(ALPHA, angle=0.0)
    artist(BRAVO, user=ALPHA, angle=0.4)
    artist(CHARLIE, angle=0.8)
    library.mark_artist(f"da:{CHARLIE}", "account gone")
    artist(DELTA, angle=0.9, user=CHARLIE, old_keys=[f"da:{CHARLIE}"])  # the collect stage links the two handles
    assert library.account_hash(f"da:{BRAVO}") == library.account_hash(f"da:{ALPHA}")
    assert push.run(False)["accounts_left_out_shared"] == 2
    sent = sent_artists(world.db)
    assert [sent[handle]["da_account"] for handle in (ALPHA, BRAVO)] == [None, None]
    assert sent[DELTA]["da_account"] == library.account(f"FAKE-USER-{CHARLIE}")
    world.db.file(ALPHA, with_code=True)
    world.da.profiles[ALPHA] = profile(ALPHA, bio=f"Leaving. {code(ALPHA)}")

    assert world.check() == expected(requests_due=1, left_for_owner_not_checked=1)
    granted = removals.answer([world.db.queue[ALPHA]["id"]], [], transport=world.da.transport)
    assert granted == {"granted": 1, "rejected": 0, "not_granted": 0}
    assert world.db.removed == [digest(f"da:{ALPHA}")] and not gone(world, BRAVO)
    with pytest.raises(push.Stop, match="broken local store"):  # what is kept here still ties BRAVO to ALPHA
        push.run(False)
    assert not gone(world, BRAVO) and world.db.removed == [digest(f"da:{ALPHA}")]


def test_a_request_filed_while_its_artist_is_removed_is_never_left_open():
    """The fifth critic's race: "Get my code" pressed while confirm_removal commits. request_binding read the
    removal list as it was before the proof committed, and ON CONFLICT, finding the open request closed by then,
    filed a new one for the artist just removed, looked at for good. Whatever files a request and whatever puts keys
    on the list take the removals' lock first and hold it until they commit, so each sees what the other did: the
    request files nothing, or the removal closes it. Only Postgres can run the race; here the functions are read,
    so that taking the lock out of any of them, or putting it after a read, fails. They are plpgsql, whose every
    statement reads the database afresh: the statements of an SQL function read it as the first one did. The
    seventh critic's: a load rebuilt the word list without the lock and a removal under it, each locking the words in
    its own order, so Postgres aborted one. refresh_search_words takes it, whoever calls it."""
    sql = re.sub(r"--[^\n]*", "", (config.ROOT / "sql" / "002_functions.sql").read_text(encoding="utf-8"))
    for name in (
        "request_removal",
        "start_removal_proof",
        "grant_request",
        "removed_after_insert",
        "refresh_search_words",
    ):
        body = sql[sql.index(f"function public.{name}(") :]
        body = body[: body.index("$$;")]
        assert "language plpgsql" in body, name
        first = body[body.index("begin") :].split(";")[0].split()
        assert first == ["begin", "perform", "pg_advisory_xact_lock(4207)"], name


def test_every_write_to_the_artists_and_what_names_them_takes_the_removals_lock_before_a_row():
    """The eighth critic's deadlock: a removal locked the artist and deleted it, and the cascade waited for a works or
    neighbour row that a load's batch, the sweep after a load or a push's delete had locked, while that one's
    foreign-key check waited for the artist. Postgres aborted one. Every statement that writes the artists, their
    works, their neighbour rows or the removal list takes the removals' lock first, by a statement trigger, which runs
    before the statement touches a row. Only Postgres can run the race (tests/test_db.py); here the statements are
    read, so that taking the lock out of any table fails without one."""
    sql = re.sub(r"--[^\n]*", "", (config.ROOT / "sql" / "002_functions.sql").read_text(encoding="utf-8"))
    made = re.findall(
        r"create or replace trigger removals_lock (before [a-z ]+) on public\.(\w+)\s+"
        r"for each statement execute function public\.removals_lock\(\);",
        sql,
    )
    every = "before insert or update or delete"
    assert sorted(made) == [
        ("before insert", "removed"),
        *((every, table) for table in ("artist_neighbors", "artists", "works")),
    ]
    assert function_body("removals_lock").endswith(
        "language plpgsql set search_path = '' as $$ begin perform pg_advisory_xact_lock(4207); return null; end"
    )


def test_a_batch_that_named_an_artist_removed_while_it_waited_is_written_once_more(monkeypatch):
    """A batch reads its artists before the removals' lock lets it in. One a removal deleted meanwhile fails the
    batch on its foreign key (23503); written again, the batch reads the artists anew and leaves them out. Once more
    only: a second failure is no race."""
    monkeypatch.setattr(time, "sleep", lambda seconds: None)
    refused = RuntimeError('HTTP 409: {"code":"23503","message":"FAKE violates foreign key constraint"}')
    tries = []

    def write(batch):
        tries.append(batch)
        if len(tries) == 1:
            raise refused
        return 1

    assert db._paced([["FAKE-BATCH"]], write) == 1 and tries == [["FAKE-BATCH"]] * 2

    def always(batch):
        raise refused

    with pytest.raises(RuntimeError, match="23503"):
        db._paced([["FAKE-BATCH"]], always)


def test_a_code_put_up_on_the_site_late_in_a_collect_run_is_read_at_the_next_look(world, monkeypatch):
    """The fifth critic's: the crawler's budget for a proof was the process's, and a collect run is one process for
    days. A site was read on two hourly looks, and a code put up after them waited for the collect run to end. Each
    run of the checks has a fetcher, and a budget, of its own (crawl.read_proof)."""
    site = f"https://{ALPHA}.art/"
    world.list(ALPHA, site=site)
    world.da.profiles[ALPHA] = profile(ALPHA, website=site)
    real, clock = crawl.PoliteFetcher, Clock()
    transport = httpx.MockTransport(world.sites._serve)
    monkeypatch.setattr(
        crawl,
        "PoliteFetcher",
        lambda: real(transport=transport, clock=clock, sleep=clock.sleep, resolver=lambda _: ["93.184.216.34"]),
    )
    world.sites.clock = clock
    for state in (crawl._LAST, crawl._DELAY, crawl._SPENT, crawl._OWNERS):
        state.clear()  # the collect process begins, and nothing clears them again
    http = httpx.Client(headers={"User-Agent": config.USER_AGENT}, transport=world.da.transport, follow_redirects=False)
    api, counts = discover._Api(http, "FAKE-ID", "FAKE-SECRET"), {discover.PROVEN: 0, discover.UNCHECKED: 0}
    looks = 0
    with api.pace:  # the collect run holds the API from start to end
        while not gone(world, ALPHA) and looks < 12:
            if looks == 6:
                world.sites.pages[f"{site}{FILE}"] = code(ALPHA)
            world.later(hours=1)
            clock.now += 3600
            discover.look_between(api, counts, None)  # between two artists
            looks += 1
    assert looks == 7 and counts == {discover.PROVEN: 1, discover.UNCHECKED: 0}
    assert len(world.sites.urls) == 3 * 6 + 2  # robots.txt, the file and the front page each look; then the file


@pytest.mark.parametrize("with_code", [True, False])
def test_a_request_from_the_page_is_filed_for_whom_it_showed_while_a_push_holds_them_back(world, with_code):
    """The final critic's: a push held the artist back while their claim-or-remove page was open. "Get my code" showed
    a code and "Send removal request" said it was sent, and nothing was filed: both filed only the artist listed at
    that moment. The page's ticket binds the request to whom it showed, listed or not (request_binding)."""
    artist(ALPHA)
    listed_as(world, ALPHA, ALPHA)
    shown = hold_back(world, ALPHA)  # what the page showed, and its ticket; then the push holds them back
    world.db.file(ALPHA, with_code=with_code)  # without the ticket: nobody is listed under the slug now
    assert world.db.requests == []
    world.db.file(ALPHA, with_code=with_code, shown=shown)
    assert world.db.requests == [ALPHA] and world.db.queue[ALPHA]["da_account"] == library.account(f"FAKE-USER-{ALPHA}")
    if with_code:  # and the code they put up is found while they are not listed
        world.da.profiles[ALPHA] = profile(ALPHA, bio=code(ALPHA))
        assert world.check() == expected(requests_due=1, proven_on_deviantart=1, artists_removed=1)
        assert barred(world, ALPHA)
    world.db.file(ALPHA, with_code=True, shown=shown)  # a ticket of an artist barred already files nothing
    assert world.db.requests == ([] if with_code else [ALPHA])
    # The seventh critic's uncaught break: request_binding with its look at the removal list taken out (RemovalsDb
    # follows the statement read here; tests/test_db.py runs it).
    barred_already = "(public.key_hash('da:' || lower(b.da_username)), b.da_account))"
    assert (
        f"and not exists (select 1 from public.removed gone where gone.key_hash in {barred_already}"
        in function_body("request_binding")
    )
    # The eighth critic's: whoever is listed now put before the ticket, and a ticket read before its signature holds.
    assert "order by b.pick limit 1" in function_body("request_binding")
    binding = function_body("ticket_binding")
    assert binding[binding.index(" declare ") :] == (
        " declare body constant text := split_part(p_ticket, '.', 1); begin if exists (select 1 from "
        "public.removal_secret s where split_part(p_ticket, '.', 2) = encode(extensions.hmac('claim ticket ' || body, "
        "s.secret, 'sha256'), 'hex')) then return convert_from(decode(body, 'base64'), 'UTF8')::jsonb; end if; "
        "return null; end"
    )


@pytest.mark.parametrize("posted", ["both_codes", "the_new_handles_code"])
def test_two_requests_for_one_account_under_two_handles(world, posted):
    artist(ALPHA, angle=0.0)
    artist(BRAVO, angle=0.2)
    library.mark_artist(f"da:{BRAVO}", "account gone")
    artist(CHARLIE, angle=0.21, user=BRAVO, old_keys=[f"da:{BRAVO}"])  # the collect stage links the two handles
    world.list(BRAVO)
    world.list(CHARLIE)  # the database lists both, as before the push that would drop the old one
    codes = [code(BRAVO), code(CHARLIE)] if posted == "both_codes" else [code(CHARLIE)]
    # One account, one profile, which answers for either handle under its new one.
    world.da.profiles[BRAVO] = world.da.profiles[CHARLIE] = profile(CHARLIE, user=BRAVO, bio=" ".join(codes))

    counts = world.check()
    # The first proof bars the account, which deletes both rows and closes the other request: its proof, seen in
    # the same run, finds nothing left to remove.
    found = {"proven_on_deviantart": 2, "proofs_refused_by_database": 1} if posted == "both_codes" else {}
    assert counts == expected(
        requests_due=2, artists_removed=1, **(found or {"proven_on_deviantart": 1, "left_for_owner_no_code_found": 1})
    )
    assert world.db.artists == {} and world.db.requests == []
    # The push that follows takes the account under both handles here too.
    counts = push.run(False)
    assert counts["artists_purged_here"] == 2 and sorted(sent_artists(world.db)) == [ALPHA]
    assert not any(word in world.db.held() for word in of_artist(BRAVO) + of_artist(CHARLIE))
    assert world.db.requests == [] and library.artist_keys() == [f"da:{ALPHA}"]


# A request is bound to the artist it was filed for, and a proof removes them and nobody else (the third critic).

# Two DeviantArt accounts whose handles push._slug makes into one slug. The first sorts first, so a push lists it
# and holds the second back (slug_unusable) while both are kept.
TWIN, TWIN_SECOND = "yy-fake-echo", "yy-Fake_Echo"
TWIN_SLUG = push._slug(TWIN_SECOND)


def listed_as(world, handle, slug, site=None):
    """What a push leaves in the database for the artist: listed under `slug`, which may not be their handle's."""
    domain = urlsplit(site).hostname.removeprefix("www.") if site else None
    world.db.load(
        [{"slug": slug, "name": handle, "da_username": handle, "site_url": site, "site_domain": domain}], [], []
    )


def barred(world, handle):
    return (
        digest(f"da:{handle.lower()}") in world.db.removed or library.account(f"FAKE-USER-{handle}") in world.db.removed
    )


def test_a_slug_that_passes_to_another_handle_takes_nobody_else_into_the_request(world):
    """The slug's first artist asks for its code, then holds himself back, and the next push lists the second under
    the slug. His code in his own bio removes him alone. A visitor's later request for the slug is the second
    artist's own; it re-points nothing, and her code in her own bio removes her alone."""
    assert push._slug(TWIN) == TWIN_SLUG
    artist(TWIN)
    artist(TWIN_SECOND)
    listed_as(world, TWIN, TWIN_SLUG)
    world.db.file(TWIN_SLUG, with_code=True)
    listed_as(world, TWIN_SECOND, TWIN_SLUG)
    world.db.file(TWIN_SLUG, with_code=False)  # anyone asks about the slug now: a request for whoever it lists
    assert [world.db.queue[name]["da_username"] for name in world.db.requests] == [TWIN, TWIN_SECOND]
    world.da.profiles[TWIN] = profile(TWIN, bio=f"bye {code(TWIN_SLUG)}")
    world.da.profiles[TWIN_SECOND] = profile(TWIN_SECOND, bio="Concept artist. Not leaving.")

    assert world.check() == expected(requests_due=1, proven_on_deviantart=1, artists_removed=1)
    assert barred(world, TWIN) and not barred(world, TWIN_SECOND)
    assert [row["da_username"] for row in world.db.artists.values()] == [TWIN_SECOND]
    assert [world.db.queue[name]["da_username"] for name in world.db.requests] == [TWIN_SECOND]

    world.db.file(TWIN_SLUG, with_code=True)  # she asks for the code: her request, its checks start
    world.da.profiles[TWIN_SECOND]["bio"] = f"Please remove me. {code(TWIN_SLUG)}"
    world.later(hours=1)
    assert world.check() == expected(requests_due=1, proven_on_deviantart=1, artists_removed=1)
    assert barred(world, TWIN_SECOND) and world.db.artists == {}


def test_a_slug_twin_through_the_real_push_is_not_purged_for_the_other_s_proof(world):
    artist(TWIN, angle=0.0)
    artist(TWIN_SECOND, angle=0.4)
    artist(ALPHA, angle=0.8)  # somebody else, so the library is never empty
    assert push.run(False)["artists_held_slug_unusable"] == 1
    world.db.file(TWIN_SLUG, with_code=True)
    library.mark_artist(store.artist_key(TWIN), "account gone")  # the first holds himself back
    push.run(False)
    world.da.profiles[TWIN] = profile(TWIN, bio=code(TWIN_SLUG))
    world.da.profiles[TWIN_SECOND] = profile(TWIN_SECOND, bio="Concept artist.")

    assert world.check() == expected(requests_due=1, proven_on_deviantart=1, artists_removed=1)
    push.run(False)
    assert sorted(row["da_username"] for row in world.db.artists.values()) == sorted([ALPHA, TWIN_SECOND])
    assert not library.account_removed(f"FAKE-USER-{TWIN_SECOND}") and library.account_removed(f"FAKE-USER-{TWIN}")


@pytest.mark.parametrize("proof", ["in_the_bio", "on_the_new_site"])
def test_a_site_the_artist_has_left_is_not_barred_and_whoever_holds_it_now_stays(world, proof):
    """The critic's stale site: ALPHA asked while listed with old.art, then moved to new.art; BRAVO lists old.art now.
    ALPHA's proof bars ALPHA's account, and not old.art, which ALPHA's profile no longer names."""
    old, new = f"https://{ALPHA}-old.art/", f"https://{ALPHA}-new.art/"
    world.list(ALPHA, site=old)
    listed_as(world, ALPHA, ALPHA, site=new)
    listed_as(world, BRAVO, BRAVO, site=old)
    world.da.profiles[ALPHA] = profile(ALPHA, bio=code(ALPHA) if proof == "in_the_bio" else "", website=new)
    world.sites.pages[new] = world.sites.pages[old] = page(meta(code(ALPHA)))

    if proof == "in_the_bio":
        assert world.check() == expected(requests_due=1, proven_on_deviantart=1, artists_removed=1)
    else:
        # The request is bound to old.art, which ALPHA's profile no longer names: no site is read for it.
        assert world.check() == expected(requests_due=1, left_for_owner_no_code_found=1) and world.sites.urls == []
        world.db.file(ALPHA, with_code=True)  # ALPHA asks again as listed now: a request of its own, for new.art
        world.later(hours=1)
        assert world.check() == expected(
            requests_due=2, left_for_owner_no_code_found=1, proven_on_own_site=1, artists_removed=1
        )
        assert digest(f"site:{ALPHA}-new.art") in world.db.removed
    assert gone(world, ALPHA) and not gone(world, BRAVO) and world.db.requests == []
    assert digest(f"site:{ALPHA}-old.art") not in world.db.removed and f"{old}robots.txt" not in world.sites.urls
    # The eighth critic's uncaught break: the site barred whoever lists it now. RemovalsDb.grant follows this: barred
    # only while nobody else is listed with it, by account, or for a request bound to none, by handle.
    assert (
        "case when p_site and not exists ( select 1 from public.artists a where a.site_key = r.site_key "
        "and w.who is distinct from case when r.da_account is null then a.da_key else a.da_account end) "
        "then r.site_key end" in function_body("grant_request")
    )


def test_a_site_the_same_account_is_listed_with_under_its_old_handle_is_barred_with_it(world):
    """The ninth critic's minor finding: the guard above compared handles, so a site the artist's own account was
    still listed with under its old handle counted as somebody else's, and was not barred. It compares accounts."""
    site = f"https://{BRAVO}.art/"
    artist(BRAVO)
    world.list(BRAVO, site=site)  # listed with their site, and their request filed, under BRAVO
    library.mark_artist(f"da:{BRAVO}", "account gone")
    artist(CHARLIE, user=BRAVO, old_keys=[f"da:{BRAVO}"])  # then renamed: the collect stage links the two handles
    world.da.profiles[CHARLIE] = profile(CHARLIE, user=BRAVO, bio=code(BRAVO), website=site)  # BRAVO answers 404

    assert world.check() == expected(requests_due=1, proven_on_deviantart=1, artists_removed=1)
    assert set(world.db.removed) == {library.account(f"FAKE-USER-{BRAVO}"), digest(f"site:{BRAVO}.art")}
    assert world.db.artists == {} and store.is_removed(site)


def test_a_proof_on_deviantart_bars_the_site_only_while_the_profile_names_it(world):
    site = f"https://{ALPHA}.art/"
    world.list(ALPHA, site=site)
    world.da.profiles[ALPHA] = profile(ALPHA, bio=code(ALPHA), website=None)  # a site it no longer names

    assert world.check() == expected(requests_due=1, proven_on_deviantart=1, artists_removed=1)
    assert set(world.db.removed) == {library.account(f"FAKE-USER-{ALPHA}")}
    assert not store.is_removed(site)


def test_a_request_stays_bound_to_the_account_it_was_filed_for(world):
    """The library comes to hold another account for the handle after the request was filed (the handle passed on and
    was collected again). The request is the first account's: the new holder's code proves nothing for it."""
    world.list(ALPHA)
    with sqlite3.connect(config.LOCAL_DB) as conn:
        conn.execute(
            "UPDATE profiles SET user_hash = ? WHERE artist_key = ?",
            (library.account("FAKE-USER-yy-fake-newcomer"), f"da:{ALPHA}"),
        )
    world.da.profiles[ALPHA] = profile(ALPHA, user="yy-fake-newcomer", bio=code(ALPHA))

    assert world.check() == expected(requests_due=1, left_for_owner_not_checked=1)
    assert not gone(world, ALPHA) and world.db.removed == []


def test_another_artists_removal_by_a_site_leaves_this_artists_request_open(world):
    """The critic's case: ALPHA's request was made with old.art, ALPHA moved on, BRAVO lists old.art now and proves
    his own removal. old.art is barred with BRAVO; ALPHA, still listed, keeps the request for the owner."""
    old, new = f"https://{ALPHA}-old.art/", f"https://{ALPHA}-new.art/"
    world.list(ALPHA, site=old, code=False)
    listed_as(world, ALPHA, ALPHA, site=new)
    world.list(BRAVO, site=old)
    world.da.profiles[BRAVO] = profile(BRAVO, bio=code(BRAVO), website=old)

    assert world.check() == expected(requests_due=1, proven_on_deviantart=1, artists_removed=1)
    assert gone(world, BRAVO) and digest(f"site:{ALPHA}-old.art") in world.db.removed
    assert not gone(world, ALPHA) and world.db.requests == [ALPHA]
    # By the handle or the account, never the site alone (removed_after_insert, which RemovalsDb.sql follows).
    closing = "and (r.da_key in (select key_hash from added) or r.da_account in (select key_hash from added));"
    assert closing in function_body("removed_after_insert")


def test_the_owner_sees_every_open_request_however_many_the_data_api_answers_at_once(world, monkeypatch):
    """The critic's case: the Data API answers 1,000 rows at most and says nothing of the rest."""
    for n in range(1000):
        world.list(f"yy-fake-f{n:04d}", account=False, code=False)
    world.later(minutes=1)
    world.list(ALPHA, code=False)
    served = world.db.data_api

    def at_most_a_thousand(method, path, key, params, json=None):
        answer = served(method, path, key, params, json)
        return httpx.Response(200, json=answer.json()[:1000]) if method == "GET" else answer

    monkeypatch.setattr(world.db, "data_api", at_most_a_thousand)
    lines = removals.pending()
    assert len(lines) == 1001 and lines[-1].startswith(f"{ALPHA}  id ")
    assert len({line.split()[2] for line in lines}) == 1001  # every request once


@pytest.mark.parametrize("slow", ["robots.txt", FILE])
def test_a_site_that_takes_too_long_costs_one_look_and_the_queue_moves_on(world, slow):
    """The critic's tarpit, as a run meets it: a site that answers in an hour. Its look is cut off at the first
    deadline, and the site is asked nothing more this run: a proof file that takes too long closes the site before
    its front page is asked (the sixth critic's: nothing failed when it did not, and a site could then cost a run
    eight deadlines). The request is moved on, and the next one is looked at."""
    site = f"https://{ALPHA}.art/"
    world.list(ALPHA, site=site)
    world.da.profiles[ALPHA] = profile(ALPHA, website=site)
    world.sites.slow.add(f"{site}{slow}")
    world.later(seconds=1)
    world.list(BRAVO)
    world.da.profiles[BRAVO] = profile(BRAVO, bio=code(BRAVO))

    assert world.check() == expected(
        requests_due=2, left_for_owner_not_checked=1, proven_on_deviantart=1, artists_removed=1
    )
    assert world.sites.urls == [f"{site}robots.txt", f"{site}{FILE}"][: 1 if slow == "robots.txt" else 2]
    assert [slug for slug, _ in world.db.moved] == [ALPHA, None]
    world.later(minutes=15)
    assert world.check() == expected()  # not due again for an hour: the next run meets other requests first


def test_the_database_failing_one_request_leaves_it_due_and_the_queue_moves_on(world, monkeypatch, capsys):
    """The ninth critic's: confirm_removal rebuilds the word list within the Data API's 8 s, and a run had no catch
    for one request, so one slow rebuild ended the run and held up every request behind it. The failure is counted,
    that request is not moved on (it stays due, first in the next run), and the next one is looked at."""
    world.list(ALPHA)
    world.later(seconds=1)
    world.list(BRAVO)
    for handle in (ALPHA, BRAVO):
        world.da.profiles[handle] = profile(handle, bio=code(handle))
    failing, rpc = [world.db.queue[ALPHA]["id"]], world.db.rpc

    def times_out(name, args, key):
        if name == "confirm_removal" and args["p_id"] in failing:
            raise RuntimeError("HTTP 500: FAKE canceling statement due to statement timeout")
        return rpc(name, args, key)

    monkeypatch.setattr(world.db, "rpc", times_out)
    assert world.check() == expected(
        requests_due=2, proven_on_deviantart=2, artists_removed=1, left_due_database_failed=1
    )
    assert gone(world, BRAVO) and not gone(world, ALPHA) and world.db.requests == [ALPHA]
    assert [slug for slug, _ in world.db.moved] == [None]  # BRAVO's, closed by the proof; ALPHA's not moved on
    failing.clear()
    assert world.check() == expected(requests_due=1, proven_on_deviantart=1, artists_removed=1)  # due still
    assert gone(world, ALPHA) and world.db.requests == []

    def fails(counts, *args, **kwargs):  # and a run that leaves any due so says so, and ends with a status
        counts["left_due_database_failed"] = 2

    monkeypatch.setattr(config, "load_env", lambda: {"DEVIANT_ART_ID": "FAKE-ID", "DEVIANT_ART_SECRET": "FAKE-KEY"})
    monkeypatch.setattr(removals, "run", fails)
    assert removals.main([]) == 1
    out, err = capsys.readouterr()
    assert json.loads(out)["left_due_database_failed"] == 2 and "failed for 2 requests, which stay due" in err


def test_six_proofs_between_two_pushes_stop_nothing_and_take_nobody_else(world):
    """The lead's, after round 10: a proof bars an account, and the collect stage, purging the artist before the
    next push, barred their handle here by name, a hash that only this machine's list then held. Six of those were
    more than the push believes of a store (DELETE_FLOOR) and stopped it as a broken one. No handle goes on a list
    by name while the account is known, so six proofs go through as one does, whether the push or a collect run
    purges them first. A store that adds hashes that name nobody still stops the run (tests/test_push.py:
    test_many_hashes_that_name_nobody_kept_here_stop_the_run_and_whom_the_two_lists_name_goes_all_the_same)."""
    proven = [f"yy-fake-p{n}" for n in range(12)]
    for n, handle in enumerate([*proven, ALPHA]):
        artist(handle, angle=0.07 * n)
    push.run(False)
    accounts = {library.account(f"FAKE-USER-{handle}") for handle in proven}
    for batch, sweep_first in ((proven[:6], True), (proven[6:], False)):
        for handle in batch:
            world.db.file(handle, with_code=True)
            world.da.profiles[handle] = profile(handle, bio=f"Please remove me. {code(handle)}")
        world.later(seconds=1)
        assert world.check() == expected(requests_due=6, proven_on_deviantart=6, artists_removed=6)
        if sweep_first:  # a collect run ends between the proofs and the push, and purges them itself
            swept = dict.fromkeys(collect._COUNTS, 0)
            collect._sweep(swept, None)
            assert swept[collect.PURGED] == 6
        counts = push.run(False)  # no Stop
        assert counts["removals_added_to_database"] == 0 and counts["artists_purged_here"] == (0 if sweep_first else 6)
    assert set(world.db.removed) == accounts == store.removed_hashes()  # the accounts, and no handle
    assert sorted(sent_artists(world.db)) == [ALPHA] and library.artist_keys() == [f"da:{ALPHA}"]
    assert [key for key, _, _ in store.all_artists()] == [f"da:{ALPHA}"]  # their store rows went with the purge


def test_an_artist_newly_listed_with_a_site_barred_during_a_push_is_counted_and_sent_without_it_next(
    world, monkeypatch
):
    """The ninth critic's minor finding: a proof bars a site after a push has read the removal list and before its
    load (the removals' lock lets them take turns, no more). An artist the load lists with that site for the first
    time is then skipped by the database (artists_before_write), and nothing said so. It is counted, and the next
    push, which reads the list again, sends them without the site."""
    site = f"https://{BRAVO}.art/"
    artist(ALPHA, angle=0.0)
    push.run(False)
    artist(BRAVO, angle=0.3, website=site)  # new, with a site of their own
    load = world.db.load

    def barred_meanwhile(artists, works, neighbors):
        world.db.sql(push._removed_sql({digest(f"site:{BRAVO}.art")}))  # the site's own artist proves, say
        return load(artists, works, neighbors)

    monkeypatch.setattr(world.db, "load", barred_meanwhile)
    counts = push.run(False)
    assert counts["artists_skipped_by_database"] == 1 and counts["artists_sent"] == 2 and gone(world, BRAVO)
    monkeypatch.setattr(world.db, "load", load)
    counts = push.run(False)
    assert counts["artists_skipped_by_database"] == 0 and counts["site_links_left_out_removed"] == 1
    assert not gone(world, BRAVO) and sent_artists(world.db)[BRAVO]["site_url"] is None
    assert not barred(world, BRAVO) and not store.is_removed(BRAVO)


def test_the_statements_the_live_tests_run_are_as_written():
    """The ninth critic's eleven SQL breaks that only tests/test_db.py caught, against the owner's database. Each is
    caught here too, without it: the statement that does it, as sql/ makes it."""
    # The open-request index: one request for a handle and site, a null key (no site) included.
    schema = " ".join((config.ROOT / "sql" / "001_schema.sql").read_text(encoding="utf-8").split())
    assert "'(artist_slug, da_key, site_key) nulls not distinct where outcome is null'" in schema
    # The verifier's turns: each network's request due longest, then each one's next (RemovalsDb.rpc follows it).
    assert "order by t.turn, r.due_at, r.code_at, r.id limit p_limit" in function_body("due_removals")
    # The counts: a day's window, kept for every one of the three rows (the visitor, the network, everyone).
    counting = function_body("client_wait")
    assert "wait := greatest(wait, greatest(held[i], now_s) + cost * 86400 / per_day[i] - 86400 - now_s);" in counting
    assert counting.count("for i in 1 .. 3 loop continue when per_day[i] is null;") == 3
    # A removed account bars its artist as a handle or a site does: on the way in, at commit, and when it joins
    # the list; and the commit-time check is a deferred constraint trigger.
    written = "where key_hash = any (public.artist_keys(new.da_username, new.site_domain) || new.da_account)"
    assert written in function_body("artists_before_write")
    committed = "if exists (select 1 from public.removed where key_hash in (new.da_key, new.site_key, new.da_account))"
    assert f"{committed} then raise exception" in function_body("artists_not_removed")
    sql = " ".join(
        re.sub(r"--[^\n]*", "", (config.ROOT / "sql" / "002_functions.sql").read_text(encoding="utf-8")).split()
    )
    assert (
        "create constraint trigger artists_not_removed after insert or update on public.artists deferrable initially "
        "deferred for each row execute function public.artists_not_removed();" in sql
    )
    deleting = "where a.da_key = any (keys) or a.site_key = any (keys) or a.da_account = any (keys) for update"
    assert deleting in function_body("delete_removed")
    # Never shown meanwhile (a row a removal had to skip): by any function the public reaches, or in the word list.
    unseen = (
        "not exists (select 1 from public.removed gone where gone.key_hash in (a.da_key, a.site_key, a.da_account))"
    )
    for name in ("get_artist", "site_claim", "search_artists", "refresh_search_words"):
        assert unseen in function_body(name), name
    both = "gone.key_hash in (seed.da_key, seed.site_key, seed.da_account, a.da_key, a.site_key, a.da_account))"
    assert both in function_body("similar_artists")
    # A grant closes its request first, so the removal it makes does not close it as 'removed'; and a removal
    # rebuilds the word list, so none of the removed artist's words is left to steer a search.
    granting = function_body("grant_request")
    assert granting.index("perform public.close_request(p_id, p_outcome);") < granting.index(
        "insert into public.removed"
    )
    assert "perform public.refresh_search_words(); return null;" in function_body("removed_after_insert")


def test_the_page_sends_its_ticket_with_either_button():
    """The ninth critic's uncaught break: the page dropping its ticket, so that a request sent while a push holds the
    artist back files nothing (request_binding: without a ticket, only whoever the slug lists now). The page's words
    are read here, as above; tests/test_removals.py and web/tests show what a ticket and its request do."""
    page = " ".join((config.ROOT / "web" / "src" / "pages.tsx").read_text(encoding="utf-8").split())
    assert "const { slug, name, site_url, deviantart_proof, ticket } = artist.data" in page
    assert "setCode(await removalCode(slug, ticket))" in page and "await requestRemoval(slug, note, ticket)" in page


def test_the_sweep_never_waits_on_a_row_a_load_holds():
    """delete_removed skips a row another transaction holds (a load holds none by then: removals_lock, so it is one
    locked by hand), and so does client_wait's clean-up. Only Postgres can show that for real (tests/test_db.py,
    against the owner's database); here the statements are read, so that taking SKIP LOCKED out of either fails
    without one."""
    sql = (config.ROOT / "sql" / "002_functions.sql").read_text(encoding="utf-8")
    limits = (config.ROOT / "sql" / "003_limits.sql").read_text(encoding="utf-8")
    sweep = sql[sql.index("function public.delete_removed") :]
    clean_up = limits[limits.index("A row back to nothing says nothing") :]
    for body in (sweep[: sweep.index("$$;")], clean_up[: clean_up.index("$$;")]):
        assert re.search(r"for\s+update\s+skip\s+locked", body, re.IGNORECASE)


def test_the_public_role_may_run_the_sites_functions_alone_and_each_checks_the_token_first():
    """The eighth critic's uncaught breaks: site_check granted to the public role too, and a site function that does
    not check the token. Only the owner's database can show what the public may call (tests/test_db.py); here every
    grant in sql/ is read."""
    sql = re.sub(
        r"--[^\n]*",
        "",
        "\n".join(
            (config.ROOT / "sql" / f).read_text(encoding="utf-8")
            for f in sorted(("001_schema.sql", "002_functions.sql", "003_limits.sql"))
        ),
    )
    grants = re.findall(r"\bgrant\s+([^;]*?)\s+to\s+(\w+)\s*;", sql)
    assert sorted(role for _, role in grants) == ["anon", "service_role", "service_role"]
    [public] = [what for what, role in grants if role == "anon"]
    site = ["site_search", "site_artist", "site_similar", "site_claim", "site_removal_code", "site_request_removal"]
    assert re.findall(r"public\.(\w+)\(", public) == ["client_wait", *site]
    assert "begin perform public.site_check(p_token);" in function_body("client_wait")
    for name in site:
        assert " as $$ select public.site_check(p_token); select " in function_body(name), name
    # And how much: reads 100 a visitor, 1,000 a network and 3,000 in all; removals 10 and 20, and no total (owner
    # decision 15). tests/test_db.py holds the database to them.
    budgets = "then '{100, 1000, 3000}'::float8[] else '{10, 20, null}'::float8[] end;"
    assert budgets in function_body("client_wait")


def test_what_the_secret_signs_begins_with_words_of_its_own_for_each_use():
    """The seventh critic's uncaught breaks: 'removal code ' or 'claim ticket ' taken off what the secret signs. A code
    is anybody's for any slug they name, so a code for a slug spelled as a ticket's body would then be half of that
    ticket's signature. Each use begins with words of its own (removal_code in sql/002_functions.sql), and every
    signature in sql/ is one of these."""
    begins = re.compile(r"hmac\(([^|,]*) \|\|")
    for name, words in {
        "removal_code": ["'removal code '"],
        "claim_ticket": ["'claim ticket '"],
        "ticket_binding": ["'claim ticket '"],
        "start_removal_proof": ["'removal network '"],
        "request_removal": ["'removal network '"],
        "client_wait": ["budget", "budget"],  # '<budget> visitor ' and '<budget> network '
    }.items():
        assert begins.findall(function_body(name)) == words, name
    sql = "\n".join(
        (config.ROOT / "sql" / f).read_text(encoding="utf-8") for f in ("002_functions.sql", "003_limits.sql")
    )
    assert len(begins.findall(sql)) == sql.count("hmac(") == 7
    assert "hmac(budget || ' visitor ' ||" in sql and "hmac(budget || ' network ' ||" in sql


def test_migrate_brings_what_an_earlier_version_left_to_the_files_or_stops_naming_it(monkeypatch):
    """The seventh critic's: "create index if not exists" and the named constraints left an earlier version's
    definition as it was, and the database holds one. Only Postgres can show the swap (tests/test_db.py runs it);
    here what migrate sends is read. Every index goes through pg_temp.index_as_written, which compares and replaces
    it; the constraints' loop compares too; and the last check knows every function the files make, so it stops at
    one they do not, and only at that."""
    sent = []
    monkeypatch.setattr(db, "load_env", lambda: {})
    monkeypatch.setattr(db, "sql", lambda query: sent.append(query) or [])
    monkeypatch.setattr(db, "data_api", lambda *args, **kwargs: httpx.Response(200, json={"paths": {}}))
    db.migrate()
    text = re.sub(r"--[^\n]*", "", sent[0])
    indexes = re.findall(
        r"select pg_temp\.index_as_written\(\s*'(\w+)', '(create (?:unique )?index %I on [^']*)'", text
    )
    assert len(indexes) == len(re.findall(r"create (?:unique )?index", text)) == len({name for name, _ in indexes}) == 9
    assert "pg_get_constraintdef" in text and "add constraint constraint_as_written" in text
    made = re.findall(r"create or replace function public\.(\w+)\(", text)
    check = text[text.rindex("do $$") :]
    assert re.findall(r"'(\w+)'", check[check.index("array[") : check.index("]::text[]")]) == made
    assert "raise exception 'migrate stopped, nothing applied: sql/ does not make %" in check


def test_migrate_gives_the_database_the_site_tokens_hash_and_never_the_token(monkeypatch):
    """Without the owner's database: what db.migrate sends. The critic's two breaks were no token, and the token."""
    sent = []
    monkeypatch.setattr(db, "load_env", lambda: {"SITE_TOKEN": "FAKE-SITE-TOKEN-0123456789"})
    monkeypatch.setattr(db, "sql", lambda query: sent.append(query) or [])
    monkeypatch.setattr(db, "data_api", lambda *args, **kwargs: httpx.Response(200, json={"paths": {}}))
    db.migrate()
    hashed = hashlib.sha256(b"FAKE-SITE-TOKEN-0123456789").hexdigest()
    assert sent[0].endswith(
        f"values ('{hashed}') on conflict (only_row) do update set token_hash = excluded.token_hash;"
    )
    assert not any("FAKE-SITE-TOKEN" in query for query in sent)


def test_stdout_is_counts_only_and_a_failure_is_named_by_its_kind(world, monkeypatch, capsys):
    monkeypatch.setattr(config, "load_env", lambda: {"DEVIANT_ART_ID": "FAKE-ID", "DEVIANT_ART_SECRET": "FAKE-KEY"})

    def fails(counts, *args, **kwargs):
        counts["artists_removed"] = 1
        raise RuntimeError(f"HTTP 500: {BRAVO} FAKE-KEY")

    monkeypatch.setattr(removals, "run", fails)
    assert removals.main([]) == 1
    out, err = capsys.readouterr()
    assert json.loads(out) == dict.fromkeys(removals.COUNTS, 0) | {"artists_removed": 1}
    assert err.startswith("stopped: RuntimeError at removals.py line") and BRAVO not in err + out
    assert "FAKE-KEY" not in err + out
