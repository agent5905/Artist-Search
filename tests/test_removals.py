"""Tests for pipeline.removals.

The public database is the push tests' FakeDb, with what this stage reads and calls besides: the removal requests
with their queue (code_at, due_at), artists by slug, start_removal_proof and confirm_removal, as sql/003_limits.sql
and sql/002_functions.sql have them. (tests/test_db.py runs the real ones against the owner's database.) It is put
in the place of pipeline.db for both stages, so a run of this one and a push that follows it meet the same
database. DeviantArt's API and the artists' sites are httpx.MockTransports: no network, no real sleeping. The time
is the world's clock, which only a test moves. The local stores are real ones in pytest's temporary directory,
filled by the push tests' own helper. Every handle, site, code and text below is invented.
"""

import hashlib
import hmac
import json
import logging
import socket
import sqlite3
import time
from datetime import UTC, datetime, timedelta
from urllib.parse import urlsplit

import httpx
import pytest
from test_push import ALPHA, BRAVO, CHARLIE, FakeDb, artist, digest, local_state, of_artist, sent_artists, unsynced

from pipeline import config, crawl, discover, library, push, removals, store

SECRET = "FAKE-REMOVAL-SECRET"
START = datetime(2026, 10, 14, 12, tzinfo=UTC).timestamp()
FILE = f".well-known/{removals.PROOF_NAME}.txt"


def code(slug, secret=SECRET):
    """removal_code() in sql/002_functions.sql: the slug's code, made with the database's secret."""
    return "artist-search-removal-" + hmac.new(secret.encode(), slug.encode(), hashlib.sha256).hexdigest()[:32]


def expected(**counts):
    return dict.fromkeys(removals.COUNTS, 0) | counts


def when(text):
    return datetime.fromisoformat(text)


class RemovalsDb(FakeDb):
    """FakeDb, with the removal requests and the functions this stage calls. A request is filed by request_removal
    (no code) or start_removal_proof (a code given out: due at once); filing again changes nothing of its place.
    confirm_removal removes as remove_artists does: the artist's two hashes join the list, which deletes whoever
    they bar. Whenever an artist goes, by a removal or a push, their request goes too, as delete_removed has it."""

    def __init__(self):
        super().__init__()
        self.queue = {}  # slug -> {"code_at", "due_at"}: ISO times, or None for a request no code was given for
        self.now = lambda: datetime.fromtimestamp(START, UTC).isoformat()  # the database's clock: World sets it
        self.secret = SECRET
        self.proofs = {}  # slug -> what start_removal_proof answers instead of the slug's code
        self.confirmed = []  # (slug, code) of every call to confirm_removal
        self.moved = []  # (slug, due_at) of every request moved on

    _checked = staticmethod(lambda response: response.json() if response.content else None)

    @property
    def requests(self):
        return list(self.queue)  # slugs, in the order filed

    def file(self, slug, with_code):
        """request_removal (without a code) or start_removal_proof (with one), as the database does it."""
        if any(row["slug"] == slug for row in self.artists.values()):
            row = self.queue.setdefault(slug, {"code_at": None, "due_at": None})
            if with_code and row["code_at"] is None:
                row["code_at"] = row["due_at"] = self.now()

    def data_api(self, method, path, key, params, json=None):
        assert key == "secret"  # the service key, and nothing else
        if method == "PATCH":
            assert path == "removal_requests" and set(params) == {"artist_slug"} and set(json) == {"due_at"}
            slug = params["artist_slug"].removeprefix("eq.")
            self.moved.append((slug, json["due_at"]))
            if slug in self.queue:
                self.queue[slug]["due_at"] = json["due_at"]
            return httpx.Response(204)
        assert method == "GET"
        if path == "removal_requests":
            now = params["due_at"].removeprefix("lte.")
            assert params == {
                "select": "artist_slug,code_at",
                "code_at": "not.is.null",
                "due_at": f"lte.{now}",
                "order": "due_at.asc,code_at.asc",
                "limit": str(removals.MAX_REQUESTS),
            }
            due = sorted(
                (when(row["due_at"]), when(row["code_at"]), slug)
                for slug, row in self.queue.items()
                if row["code_at"] and when(row["due_at"]) <= when(now)
            )
            listed = [{"artist_slug": slug, "code_at": self.queue[slug]["code_at"]} for _, _, slug in due]
            return httpx.Response(200, json=listed[: int(params["limit"])])
        assert path == "artists" and params["select"] == removals.ARTIST_COLUMNS
        rows = [row for row in self.artists.values() if f"eq.{row['slug']}" == params["slug"]]
        columns = ("slug", "da_username", "site_url", "site_domain")
        return httpx.Response(200, json=[{name: row.get(name) for name in columns} | self.hashes(row) for row in rows])

    def hashes(self, row):
        return dict(zip(("da_key", "site_key"), self.keys(row), strict=True))

    def rpc(self, name, args, key):
        assert key == "secret"
        slug = args["p_slug"]
        if name == "start_removal_proof":
            self.file(slug, with_code=True)
            return self.proofs.get(slug, code(slug, self.secret))
        assert name == "confirm_removal"
        self.confirmed.append((slug, args["p_code"]))
        row = next((row for row in self.artists.values() if row["slug"] == slug), None)
        if row is None or args["p_code"] != code(slug, self.secret):
            return False
        self.sql(push._removed_sql({key for key in self.keys(row) if key}))
        return True

    def sql(self, query):
        answer = super().sql(query)
        slugs = {row["slug"] for row in self.artists.values()}
        self.queue = {slug: row for slug, row in self.queue.items() if slug in slugs}
        return answer


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
        self.urls = []

    def fetcher(self):
        clock = Clock()
        transport = httpx.MockTransport(self._serve)
        return crawl.PoliteFetcher(
            transport=transport, clock=clock, sleep=clock.sleep, resolver=lambda _: ["93.184.216.34"]
        )

    def _serve(self, request):
        url = str(request.url.copy_with(host=request.headers["host"]))
        self.urls.append(url)
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

    def list(self, handle, site=None, *, account=True):
        """An artist the database lists, as a push leaves them, and their request, filed now with a code given out.
        `account`: the library keeps their DeviantArt account (they are collected here, unless they already were)."""
        if account and library.account_hash(store.artist_key(handle)) is None:
            artist(handle)
        domain = urlsplit(site).hostname.removeprefix("www.") if site else None
        self.db.load(
            [{"slug": handle, "name": handle, "da_username": handle, "site_url": site, "site_domain": domain}], [], []
        )
        self.db.file(handle, with_code=True)

    def later(self, **delta):
        self.clock += timedelta(**delta).total_seconds()

    def check(self, dry_run=False):
        """One run of the stage, in a process of its own: the crawler's state of the run starts afresh."""
        for state in (crawl._LAST, crawl._DELAY, crawl._SPENT, crawl._OWNERS):
            state.clear()
        counts = dict.fromkeys(removals.COUNTS, 0)
        fetcher = self.sites.fetcher()
        removals.run(
            counts, dry_run, "FAKE-CLIENT-ID", "FAKE-CLIENT-SECRET", transport=self.da.transport, fetcher=fetcher
        )
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
    world.da.profiles[BRAVO] = profile(BRAVO, **{field: f"<p>Please take me off Artist Search: {split}</p>"})

    assert world.check() == expected(
        requests_due=2, proven_on_deviantart=1, artists_removed=1, left_for_owner_no_code_found=1
    )
    # There: gone, both hashes on the list, the request answered. The other artist's request waits for the owner.
    assert [row["slug"] for row in world.db.artists.values()] == [ALPHA] and world.db.requests == [ALPHA]
    assert set(world.db.removed) == {digest(f"da:{BRAVO}"), digest(f"site:{BRAVO}.art")}
    # Here at once, the same two hashes: no stage collects the handle or the site again.
    assert store.is_removed(BRAVO) and store.is_removed(f"https://www.{BRAVO}.art/") and not store.is_removed(ALPHA)
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
    world.list(ALPHA)
    with sqlite3.connect(config.LOCAL_DB) as conn:  # how a profile written before the 'uid' spelling holds it
        held = library.account(f"FAKE-USER-{ALPHA}", spelling)
        conn.execute("UPDATE profiles SET user_hash = ? WHERE artist_key = ?", (held, f"da:{ALPHA}"))
    world.da.profiles[ALPHA] = profile(ALPHA, bio=code(ALPHA))

    assert world.check() == expected(requests_due=1, proven_on_deviantart=1, artists_removed=1)


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
    ],
)
def test_the_code_however_written_in_the_head_is_found(world, spelling, placed):
    site = f"https://{ALPHA}.art/"
    world.list(ALPHA, site=site)
    world.da.profiles[ALPHA] = profile(ALPHA, website=site)
    world.sites.pages[site] = placed(code(ALPHA))

    assert world.check() == expected(requests_due=1, proven_on_own_site=1, artists_removed=1)


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


@pytest.mark.parametrize("proof", ["made_with_an_old_secret", "no_code_from_the_database", "an_empty_code"])
def test_only_the_slugs_code_as_the_database_makes_it_now_proves_anything(world, proof):
    world.list(ALPHA)
    world.da.profiles[ALPHA] = profile(ALPHA, bio=f"Old code: {code(ALPHA, 'FAKE-OLD-SECRET')}")
    if proof != "made_with_an_old_secret":  # the secret's row is missing, say: removal_code() answers null
        world.db.proofs[ALPHA] = None if proof == "no_code_from_the_database" else ""

    left = "no_code_found" if proof == "made_with_an_old_secret" else "not_checked"
    assert world.check() == expected(requests_due=1, **{f"left_for_owner_{left}": 1})
    assert world.db.confirmed == [] and len(world.db.artists) == 1


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


def test_a_request_without_a_code_is_the_owners_alone_and_never_looked_at(world):
    world.list(ALPHA)
    world.db.queue[ALPHA] = {"code_at": None, "due_at": None}  # filed with request_removal only
    world.da.profiles[ALPHA] = profile(ALPHA, bio=code(ALPHA))

    assert world.check() == expected()
    assert world.da.requests == [] and world.db.requests == [ALPHA] and world.db.moved == []

    world.db.file(ALPHA, with_code=True)  # the artist asks for their code on the site
    assert world.check() == expected(requests_due=1, proven_on_deviantart=1, artists_removed=1)


def test_the_one_due_longest_comes_first_and_asking_again_moves_nothing(world, monkeypatch):
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
    # Hourly for the first day, daily for the rest of the first week, weekly after that.
    assert looked == [*range(24), *range(24, 7 * 24, 24), *range(7 * 24, 30 * 24, 7 * 24)]


@pytest.mark.parametrize("posted_after_hours", [0.5, 5, 50, 150])
def test_a_code_put_up_in_the_first_week_is_found_within_a_day(world, posted_after_hours):
    world.list(ALPHA)
    world.da.profiles[ALPHA] = profile(ALPHA, bio="Concept artist.")
    hours = 0.0
    while not gone(world, ALPHA):
        if hours >= posted_after_hours:
            world.da.profiles[ALPHA]["bio"] = f"Concept artist. {code(ALPHA)}"
        world.check()
        world.later(minutes=15)  # the schedule the README asks for
        hours += 0.25
    assert hours - posted_after_hours <= 24


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
    assert world.db.requests == [] and world.db.artists == {} and store.is_removed(ALPHA)
    world.later(hours=1)
    assert world.check() == expected()


def test_a_request_for_nobody_in_the_database_is_counted_moved_on_and_nothing_is_asked(world):
    at = datetime.fromtimestamp(START, UTC).isoformat()
    world.db.queue["yy-fake-gone"] = {"code_at": at, "due_at": at}  # an artist gone before the sweep took this

    assert world.check() == expected(requests_due=1, requests_for_no_artist=1)
    assert world.da.requests == [] and world.sites.urls == [] and world.db.confirmed == []
    assert [slug for slug, _ in world.db.moved] == ["yy-fake-gone"]
    assert not config.CACHE_DIR.exists()  # the API was not even taken


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
        if name == "start_removal_proof" and len(world.da.requests) == 2:  # the token, and the first lookup
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
    proven = 2 if posted == "both_codes" else 1
    assert counts == expected(
        requests_due=2, proven_on_deviantart=proven, artists_removed=proven, left_for_owner_no_code_found=2 - proven
    )
    # The verifier removes only the slugs whose code it saw. The push that follows takes the account under both.
    counts = push.run(False)
    assert counts["artists_purged_here"] == 2 and sorted(sent_artists(world.db)) == [ALPHA]
    assert not any(word in world.db.held() for word in of_artist(BRAVO) + of_artist(CHARLIE))
    assert world.db.requests == [] and library.artist_keys() == [f"da:{ALPHA}"]


def test_stdout_is_counts_only_and_a_failure_is_named_by_its_kind(world, monkeypatch, capsys):
    monkeypatch.setattr(config, "load_env", lambda: {"DEVIANT_ART_ID": "FAKE-ID", "DEVIANT_ART_SECRET": "FAKE-KEY"})

    def fails(counts, *args, **kwargs):
        counts["artists_removed"] = 1
        raise RuntimeError(f"HTTP 500: {BRAVO} FAKE-KEY")

    monkeypatch.setattr(removals, "run", fails)
    assert removals.main([]) == 1
    out, err = capsys.readouterr()
    assert json.loads(out) == expected(artists_removed=1)
    assert err.startswith("stopped: RuntimeError at removals.py line") and BRAVO not in err + out
    assert "FAKE-KEY" not in err + out
