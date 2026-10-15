"""Tests for pipeline.removals.

The public database is the push tests' FakeDb, with what this stage reads and calls besides: the pending requests,
artists by slug, start_removal_proof and confirm_removal. It is put in the place of pipeline.db for both stages, so
a run of this one and a push that follows it meet the same database. DeviantArt's API and the artists' sites are
httpx.MockTransports: no network, no real sleeping. The local stores are real ones in pytest's temporary directory,
filled by the push tests' own helper. Every handle, site, code and text below is invented.
"""

import hashlib
import hmac
import json
import socket
import sqlite3
import time
from urllib.parse import urlsplit

import httpx
import pytest
from test_push import ALPHA, BRAVO, CHARLIE, FakeDb, artist, digest, local_state, of_artist, sent_artists, unsynced

from pipeline import config, crawl, discover, library, push, removals, store

SECRET = "FAKE-REMOVAL-SECRET"


def code(slug, secret=SECRET):
    """removal_code() in sql/002_functions.sql: the slug's code, made with the database's secret."""
    return "artist-search-removal-" + hmac.new(secret.encode(), slug.encode(), hashlib.sha256).hexdigest()[:32]


def expected(**counts):
    return dict.fromkeys(removals.COUNTS, 0) | counts


class RemovalsDb(FakeDb):
    """FakeDb, with the pending requests (newest first) and the two removal functions. confirm_removal removes as
    remove_artists does: the artist's two hashes join the list, which deletes whoever they bar. Whenever an artist
    goes, by a removal or a push, the requests for nobody go too, as delete_removed has it."""

    def __init__(self):
        super().__init__()
        self.requests = []  # slugs, newest first
        self.secret = SECRET
        self.proofs = {}  # slug -> what start_removal_proof answers instead of the slug's code
        self.confirmed = []  # (slug, code) of every call to confirm_removal

    _checked = staticmethod(lambda response: response.json())

    def data_api(self, method, path, key, params):
        assert (method, key) == ("GET", "secret")  # the service key, for reading only
        if path == "removal_requests":
            assert params == {"select": "artist_slug", "order": "last_at.desc"}
            return httpx.Response(200, json=[{"artist_slug": slug} for slug in self.requests])
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
        self.requests = [slug for slug in self.requests if slug in slugs]
        return answer


def profile(handle, *, user=None, name=None, **fields):
    """A profile as the API answers it: the account's id and its handle now, and the fields given."""
    return {"user": {"userid": f"FAKE-USER-{user or handle}", "username": name or handle}, "website": None} | fields


def front(handle, *more):
    """A front page that links back to the handle's DeviantArt profile, with whatever else it is given."""
    return f'<html><body><a href="https://www.deviantart.com/{handle}">my DeviantArt</a>{"".join(more)}</body></html>'


class DeviantArt:
    """The API: a token for anybody, and each handle's profile as `profiles` has it. A handle it lacks is 404."""

    def __init__(self):
        self.profiles = {}
        self.requests = []
        self.transport = httpx.MockTransport(self._serve)

    def _serve(self, request):
        self.requests.append(str(request.url))
        assert request.url.host == "www.deviantart.com"  # nothing else goes through the API's client
        if request.url.path == "/oauth2/token":
            return httpx.Response(200, json={"access_token": "FAKE-TOKEN"})
        assert request.headers["authorization"] == "Bearer FAKE-TOKEN"
        answer = self.profiles.get(request.url.path.removeprefix("/api/v1/oauth2/user/profile/"))
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
    """Artists' own sites: `pages` maps a URL to its body. Anything else is 404, so no robots.txt and no
    tdmrep.json unless one is given."""

    def __init__(self):
        self.pages = {}
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
        body = self.pages.get(url)
        return (
            httpx.Response(404)
            if body is None
            else httpx.Response(200, text=body, headers={"content-type": "text/html"})
        )


class World:
    def __init__(self, database):
        self.db, self.da, self.sites = database, DeviantArt(), Sites()

    def list(self, handle, site=None):
        """An artist the database lists, as a push leaves them, and a request to remove them, the newest."""
        domain = urlsplit(site).hostname.removeprefix("www.") if site else None
        self.db.load(
            [{"slug": handle, "name": handle, "da_username": handle, "site_url": site, "site_domain": domain}], [], []
        )
        self.db.requests.insert(0, handle)

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
    return World(database)


# Where the code must be, and nowhere else.


@pytest.mark.parametrize("field", ["bio", "tagline"])
def test_the_code_in_the_profiles_own_text_removes_the_artist_there_at_once_and_bars_them_here(world, field):
    world.list(ALPHA)
    world.list(BRAVO, site=f"https://{BRAVO}.art/")
    world.da.profiles[ALPHA] = profile(ALPHA, bio="Concept artist.")
    # Long words get split by markup, and the code is in a sentence of the artist's own.
    split = code(BRAVO)[:30] + "<wbr>" + code(BRAVO)[30:]
    world.da.profiles[BRAVO] = profile(BRAVO, **{field: f"<p>Please take me off Artist Search: {split}</p>"})

    assert world.check() == expected(
        requests_pending=2, proven_on_deviantart=1, artists_removed=1, left_for_owner_no_code_found=1
    )
    # There: gone, both hashes on the list, the request answered. The other artist's request waits for the owner.
    assert [row["slug"] for row in world.db.artists.values()] == [ALPHA] and world.db.requests == [ALPHA]
    assert set(world.db.removed) == {digest(f"da:{BRAVO}"), digest(f"site:{BRAVO}.art")}
    # Here at once, the same two hashes: no stage collects the handle or the site again.
    assert store.is_removed(BRAVO) and store.is_removed(f"https://www.{BRAVO}.art/") and not store.is_removed(ALPHA)
    assert world.sites.urls == []  # proven on DeviantArt: the site is not asked


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

    assert world.check() == expected(requests_pending=1, left_for_owner_no_code_found=1)
    assert world.db.confirmed == [] and len(world.db.artists) == 1 and world.db.requests == [ALPHA]


def test_a_code_on_another_artists_page_proves_nothing_for_either(world):
    world.list(ALPHA, site=f"https://{ALPHA}.art/")
    world.list(BRAVO)
    world.da.profiles[ALPHA] = profile(ALPHA, bio=code(BRAVO), website=f"https://{ALPHA}.art/")
    world.da.profiles[BRAVO] = profile(BRAVO, tagline=code(ALPHA))
    world.sites.pages[f"https://{ALPHA}.art/"] = front(ALPHA, code(BRAVO))

    assert world.check() == expected(requests_pending=2, left_for_owner_no_code_found=2)
    assert world.db.confirmed == [] and len(world.db.artists) == 2 and world.db.removed == []


@pytest.mark.parametrize("answer", ["another_handle", "another_account", "not_found", "not_a_profile"])
def test_a_profile_that_is_not_the_artists_proves_nothing_and_its_site_is_not_asked(world, answer):
    site = f"https://{ALPHA}.art/"
    artist(ALPHA)  # the library keeps their account
    world.list(ALPHA, site=site)
    world.list(BRAVO)  # the library keeps no account for this one: their own handle's profile is what counts
    world.sites.pages[site] = front(ALPHA, code(ALPHA))  # their site, which their own profile would name
    world.da.profiles = {
        "another_handle": {ALPHA: profile(ALPHA), BRAVO: profile("yy-fake-zulu", user=BRAVO, bio=code(BRAVO))},
        # The handle has passed to somebody else, whose profile names the artist's site.
        "another_account": {
            ALPHA: profile(ALPHA, user="yy-fake-zulu", bio=code(ALPHA), website=site),
            BRAVO: profile(BRAVO),
        },
        "not_found": {},
        "not_a_profile": {ALPHA: [code(ALPHA), site], BRAVO: [code(BRAVO)]},
    }[answer]

    refused = 2 if answer in ("not_found", "not_a_profile") else 1
    assert world.check() == expected(
        requests_pending=2, left_for_owner_not_checked=refused, left_for_owner_no_code_found=2 - refused
    )
    assert world.db.confirmed == [] and len(world.db.artists) == 2 and world.sites.urls == []


def test_an_answer_from_the_same_account_under_its_new_handle_is_the_artists(world):
    artist(BRAVO)
    world.list(BRAVO)
    world.da.profiles[BRAVO] = profile(CHARLIE, user=BRAVO, bio=code(BRAVO))  # renamed since it was collected

    assert world.check() == expected(requests_pending=1, proven_on_deviantart=1, artists_removed=1)


SITE_CASES = [
    "proven",
    "no_link_back",
    "robots_txt_shuts_us_out",
    "profile_names_another_site",
    "profile_names_no_site",
    "not_on_record",
]


@pytest.mark.parametrize("case", SITE_CASES)
def test_the_own_site_counts_only_on_record_named_by_the_profile_linking_back_and_read_politely(world, case):
    site, other = f"https://{ALPHA}.art/", f"https://{BRAVO}.art/"
    world.list(ALPHA, site=None if case == "not_on_record" else site)
    website = {"profile_names_another_site": other, "profile_names_no_site": None}.get(case, site)
    world.da.profiles[ALPHA] = profile(ALPHA, bio="Concept artist.", website=website)
    proof = f'<meta name="artist-search-removal" content="{code(ALPHA)}">'
    world.sites.pages[site] = f"<html>{proof}</html>" if case == "no_link_back" else front(ALPHA, proof)
    world.sites.pages[other] = front(ALPHA, proof)  # a site that links to them and holds the code: not theirs
    if case == "robots_txt_shuts_us_out":
        world.sites.pages[f"{site}robots.txt"] = "User-agent: *\nDisallow: /\n"

    counts = world.check()

    if case == "proven":
        assert counts == expected(requests_pending=1, proven_on_own_site=1, artists_removed=1)
        # Through the crawler: robots.txt, then tdmrep.json, then the front page, and nothing else.
        assert world.sites.urls == [f"{site}robots.txt", f"{site}.well-known/tdmrep.json", site]
        assert world.db.artists == {} and store.is_removed(f"{ALPHA}.art")
        return
    left = "not_checked" if case in ("no_link_back", "robots_txt_shuts_us_out") else "no_code_found"
    assert counts == expected(requests_pending=1, **{f"left_for_owner_{left}": 1})
    assert world.db.confirmed == [] and len(world.db.artists) == 1
    assert other not in world.sites.urls  # a site of somebody else's is never read for them
    if case == "robots_txt_shuts_us_out":
        assert world.sites.urls == [f"{site}robots.txt"]
    elif case != "no_link_back":
        assert world.sites.urls == []


@pytest.mark.parametrize("proof", ["made_with_an_old_secret", "no_code_from_the_database", "an_empty_code"])
def test_only_the_slugs_code_as_the_database_makes_it_now_proves_anything(world, proof):
    world.list(ALPHA)
    world.da.profiles[ALPHA] = profile(ALPHA, bio=f"Old code: {code(ALPHA, 'FAKE-OLD-SECRET')}")
    if proof != "made_with_an_old_secret":  # the secret's row is missing, say: removal_code() answers null
        world.db.proofs[ALPHA] = None if proof == "no_code_from_the_database" else ""

    left = "no_code_found" if proof == "made_with_an_old_secret" else "not_checked"
    assert world.check() == expected(requests_pending=1, **{f"left_for_owner_{left}": 1})
    assert world.db.confirmed == [] and len(world.db.artists) == 1


def test_a_code_the_database_refuses_removes_nobody_there_or_here(world):
    """The owner replaced the secret between the read of the code and the removal: the database is the judge."""
    world.list(ALPHA)
    world.db.proofs[ALPHA] = code(ALPHA, "FAKE-REPLACED-SECRET")
    world.da.profiles[ALPHA] = profile(ALPHA, bio=code(ALPHA, "FAKE-REPLACED-SECRET"))

    assert world.check() == expected(requests_pending=1, proven_on_deviantart=1, proofs_refused_by_database=1)
    assert len(world.db.artists) == 1 and world.db.removed == [] and not store.is_removed(ALPHA)


def test_a_request_for_nobody_in_the_database_is_counted_and_nothing_is_asked(world):
    world.db.requests = ["yy-fake-gone"]  # removed since, or no longer qualifying

    assert world.check() == expected(requests_pending=1, requests_for_no_artist=1)
    assert world.da.requests == [] and world.sites.urls == [] and world.db.confirmed == []
    assert not config.CACHE_DIR.exists()  # the API was not even taken


# Sequences.


def test_a_request_without_proof_waits_and_a_code_posted_after_a_check_is_found_by_the_next_run(world):
    world.list(ALPHA)
    world.da.profiles[ALPHA] = profile(ALPHA, bio="Concept artist.")

    assert world.check() == expected(requests_pending=1, left_for_owner_no_code_found=1)
    assert world.db.requests == [ALPHA] and len(world.db.artists) == 1 and not store.is_removed(ALPHA)

    world.da.profiles[ALPHA]["bio"] += f" {code(ALPHA)}"
    assert world.check() == expected(requests_pending=1, proven_on_deviantart=1, artists_removed=1)
    assert world.db.requests == [] and world.db.artists == {} and store.is_removed(ALPHA)
    assert world.check() == expected()


def test_a_dry_run_says_what_a_run_would_remove_and_changes_nothing(world):
    artist(ALPHA)
    world.list(ALPHA)
    world.da.profiles[ALPHA] = profile(ALPHA, bio=code(ALPHA))

    def there():
        return json.dumps([world.db.artists, world.db.removed, world.db.requests])

    before = local_state(), there()
    assert world.check(dry_run=True) == expected(requests_pending=1, proven_on_deviantart=1, artists_removed=1)
    assert (local_state(), there()) == before and world.db.confirmed == []


def test_every_request_to_deviantart_waits_its_turn_and_a_run_that_cannot_have_the_api_asks_nothing(world, monkeypatch):
    for handle in (ALPHA, BRAVO, CHARLIE):
        world.list(handle)
        world.da.profiles[handle] = profile(handle)
    turns, wait = [], discover.Pace.wait

    def waited(pace):
        turns.append(len(world.da.requests))  # how many requests had gone before this wait
        wait(pace)

    monkeypatch.setattr(discover.Pace, "wait", waited)
    world.check()
    assert turns == [0, 1, 2, 3] and len(world.da.requests) == 4  # the token, then one profile each, in turn

    world.da.requests.clear()
    with discover.Pace(discover.BASE_INTERVAL, "the API"):  # another stage has the API
        with pytest.raises(discover.Stop, match="another run is talking to the API"):
            world.check()
    assert world.da.requests == [] and world.sites.urls == [] and world.db.confirmed == []


def test_a_proof_found_then_a_push_purges_them_here_and_never_sends_them_again(world):
    artist(ALPHA, angle=0.0)
    removed = artist(BRAVO, angle=0.01, website=f"https://{BRAVO}.art/")
    artist(CHARLIE, angle=0.5)
    push.run(False)
    assert sorted(sent_artists(world.db)) == [ALPHA, BRAVO, CHARLIE]
    world.db.requests = [BRAVO]
    world.da.profiles[BRAVO] = profile(BRAVO, website=f"https://{BRAVO}.art/")
    world.sites.pages[f"https://{BRAVO}.art/"] = front(BRAVO, f"<p>{code(BRAVO)}</p>")

    assert world.check() == expected(requests_pending=1, proven_on_own_site=1, artists_removed=1)
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
        requests_pending=2, proven_on_deviantart=proven, artists_removed=proven, left_for_owner_no_code_found=2 - proven
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
