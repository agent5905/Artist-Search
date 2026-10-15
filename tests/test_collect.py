"""Tests for pipeline.collect.

Every handle, address, title, tag, text and byte below is invented for these tests. Nothing came from DeviantArt
and nothing here calls it: all traffic goes to an httpx.MockTransport. Nothing really sleeps either: the tests
replace time.sleep, the one thing the code waits with, and the fake clock moves only when it is called. The
cache directory and the database are moved into pytest's temporary directory.
"""

import base64
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
from datetime import UTC, datetime, timedelta
from unittest import mock

import httpx
import pytest

from pipeline import collect, config, crawl, discover, library, store

CLIENT_ID = "FAKE-CLIENT-ID-0002"
CLIENT_SECRET = "FAKE-CLIENT-SECRET-0002"
API_HOST = "www.deviantart.com"
CDN_HOST = "images-wixmp-zzcdn.wixmp.com"  # a made-up name under the domain DeviantArt serves images from
CDN = f"https://{CDN_HOST}"
TOKEN_PATH = "/oauth2/token"
PROFILE_PATH = "/api/v1/oauth2/user/profile/"
GALLERY_PATH = "/api/v1/oauth2/gallery/all"
METADATA_PATH = "/api/v1/oauth2/deviation/metadata"

ALPHA, BRAVO, CHARLIE, DELTA = "zz-fake-alpha", "ZZ-Fake-Bravo", "zz-fake-charlie", "zz-fake-delta"
ECHO, FOXTROT, RENAMED = "zz-fake-echo", "zz-fake-foxtrot", "zz-fake-renamed"

# What a response carries that must never be kept, printed or logged (hard rule 6), and the numbers that go with it.
PRIVATE = (
    "FAKE-EMAIL",
    "REALNAME",
    "FAKE-COUNTRY",
    "FAKE-GEO",
    "FAKE-SEX",
    "FAKE-TIMEZONE",
    "FAKE-JOINDATE",
    "FAKE-BIO",
)
PRIVATE += ("FAKE-USERID", "FAKE-ICON", "FAKE-COVER", "FAKE-STATUS", "FAKE-DESCRIPTION", "FAKE-LICENSE", "FAKE-EXCERPT")
PRIVATE += ("987654321", "555000111", "777000333", "444000")  # age, two country ids, the profile's statistics

EXPECTED_COUNTS = """\
artists processed: 5
profiles stored: 3
of those, with an own-site link: 1
of those, no website given: 0
of those, website on a listed platform: 2
of those, website on another domain, not a bare front page: 0
of those, website names no public host: 0
works stored: 13
images downloaded: 13
images already cached: 0
purged artists (removed): 0
deleted cached files (no row for them): 0
deletions that failed: 0
rows with an unreadable artist key: 0
skipped artists (the database refused the profile): 0
skipped works (the database refused the row, or another artist has it): 0
skipped artists (unexpected error): 0
skipped artists (the API failed for them): 0
skipped artists (unreadable handle): 0
skipped artists (removed): 0
skipped artists (a removed account under a new handle): 0
skipped artists (none of their images was served): 0
marked artists (account suspended, inactive or gone): 2
marked artists (their profile names a removed site): 0
marked artists (an answer not kept, and what was kept stays): 0
marked works (stored, and no longer qualifying): 0
skipped works (malformed entry): 0
skipped works (another author's): 0
skipped works (not a public image): 2
skipped works (mature or unrated): 3
skipped works (no tag data): 0
skipped works (contact details in the title or an address): 0
skipped works (image not served): 0
skipped works (image host failing or rate limiting): 0
skipped works (image not an accepted type): 0
skipped works (image over the size cap): 0
"""
# After the counts, what each host's pace did: in a run that met no trouble, a request every 2 seconds to the API
# and an image every half second. The seconds are whole ones, rounded up.
EXPECTED_STDOUT = (
    EXPECTED_COUNTS
    + """\
requests to the API: 11
rate limit answers from the API: 0
server errors or no answer from the API: 0
longest interval between requests to the API, in seconds: 2
interval between requests to the API now, in seconds: 2
seconds spent waiting between requests to the API: 20
requests to the image host: 13
rate limit answers from the image host: 0
server errors or no answer from the image host: 0
longest interval between requests to the image host, in seconds: 1
interval between requests to the image host now, in seconds: 1
seconds spent waiting between requests to the image host: 6
"""
)
BASE_REQUESTS = 24  # 1 token, 10 to the API (3 + 3 + 1 + 1 + 2 for the five artists), 13 images
KEPT = "Everything stored is kept: run again later"
WAITS = "60 minutes of waits, each 5 minutes or as long as it asked"  # an hour of them stops a run: discover.Pace
LIMITED = f"stopped: rate limited: the API kept refusing through {WAITS}. {KEPT}\n"
ALPHA_KEPT = [12, 11, 10, 9, 8, 7, 6, 5, 4, 3]  # the ten newest of twelve
BRAVO_KEPT = [23, 22, 21]
PNG_START = b"\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR"
WEBP_START = b"RIFF\x24\x00\x00\x00WEBPVP8 "


def uuid(n):
    return f"00000000-0000-4000-A000-{n:012d}"


def key(username):
    return f"da:{username.lower()}"


def iso(seconds):
    return datetime.fromtimestamp(seconds, UTC).isoformat(timespec="seconds")


def jpeg(n):
    return b"\xff\xd8\xff" + f"FAKE-IMAGE-BYTES-{n}-".encode() * 40


def image_path(n, kind, size):
    """A path of the shape DeviantArt gives an image's address: the file, how it is cut, and a name made of the
    title, the artist and DeviantArt's own id for the work."""
    file = f"/f/{uuid(n).lower()}/d{n:06d}-{uuid(n).lower()}.jpg"
    return f"{file}/v1/fit/w_{size},h_{size},q_70,strp/fake_{kind}_{n}_by_zz_fake_d{n:06d}-{size}w.jpg"


def preview_path(n):
    return image_path(n, "preview", 800)


def thumb(n, size=300):
    return f"{CDN}{image_path(n, 'thumb', size)}?{signed(n)}"


HEADER = base64.urlsafe_b64encode(b'{"typ":"JWT","alg":"HS256"}').decode().rstrip("=")


def signed(n):
    """The query DeviantArt puts on an image address: a signature in three parts, the first a small JSON header."""
    return f"token={HEADER}.FAKE-CLAIMS-{n}.FAKE-SIGNATURE"


def author(username):
    return {
        "userid": f"FAKE-USERID-{username}",
        "username": username,
        "usericon": f"{CDN}/avatars/FAKE-ICON-{username}.gif",
        "type": "FAKE-USERTYPE",
        "email": f"FAKE-EMAIL-{username}@mail.invalid",
    }


def user_hash(username):
    return hashlib.sha256(f"uid:FAKE-USERID-{username}".encode()).hexdigest()


def deviation(n, username, drop=(), **changes):
    """A made-up gallery entry: every field the API documents for an image. Work n was published in hour n."""
    entry = {
        "deviationid": uuid(n),
        "printid": None,
        "url": f"https://www.deviantart.com/{username.lower()}/art/FAKE-TITLE-{n}-{n}",
        "title": f"FAKE-TITLE-{n}",
        "is_favourited": False,
        "is_deleted": False,
        "is_published": True,
        "is_blocked": False,
        "is_mature": False,
        "is_downloadable": True,
        "download_filesize": 2345678,
        "allows_comments": True,
        "author": author(username),
        "stats": {"comments": 1, "favourites": 2},
        "published_time": str(1_700_000_000 + n * 3600),
        "preview": {"src": f"{CDN}{preview_path(n)}?{signed(n)}", "height": 600, "width": 800},
        "content": {"src": f"{CDN}/f/FAKE-CONTENT-{n}.png?{signed(n)}", "height": 1200, "width": 1600},
        "thumbs": [{"src": thumb(n, size), "height": size, "width": size} for size in (150, 200, 300)],
    }
    entry.update(changes)
    return {name: value for name, value in entry.items() if name not in drop}


def metadata(n, username, tags, drop=(), **changes):
    """A made-up metadata item, with everything the API documents beside the tags."""
    item = {
        "deviationid": uuid(n),
        "printid": None,
        "author": author(username),
        "is_watching": False,
        "title": f"FAKE-TITLE-{n}",
        "description": f"FAKE-DESCRIPTION-{n}",
        "license": "FAKE-LICENSE",
        "allows_comments": True,
        "tags": [{"tag_name": tag, "sponsored": False, "sponsor": ""} for tag in tags],
        "is_favourited": False,
        "is_mature": False,
        "can_post_comment": False,
    }
    item.update(changes)
    return {name: value for name, value in item.items() if name not in drop}


def profile(username, **changes):
    """A made-up profile response, private fields included (the real API can return all of these)."""
    body = {
        "user": {
            **author(username),
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
        "website": f"https://www.artstation.com/{username.lower()}",
        "bio": f"FAKE-BIO-{username}",
        "cover_photo": f"{CDN}/f/FAKE-COVER-{username}.jpg",
        "last_status": {"body": f"FAKE-STATUS-{username}"},
        "stats": {"user_deviations": 444000111, "user_favourites": 444000222, "profile_pageviews": 444000333},
    }
    body.update(changes)
    return body


def refusal(status=400, **more):
    return httpx.Response(status, json={"error": "invalid_request", "error_description": "FAKE-ERROR", **more})


class World:
    """A made-up DeviantArt: an API, an image host and a clock that moves only when the code sleeps.

    The API answers exactly the requests the documentation describes for these three endpoints with the
    parameters this project is meant to send. Any other parameter is refused, the way a stricter API would,
    so a request that asks for more than it should (a profile's "expand", say) cannot pass unnoticed.
    """

    def __init__(self):
        self.now = 0.0
        self.waits = []
        self.slept = None  # the last wait, until the request it was for arrives
        self.log = []  # (fake time, request), in order
        self.threads = set()
        self.profiles = {}  # lowercased handle -> a profile body, an HTTP status, or a response
        self.galleries = {}  # lowercased handle -> gallery entries, in the order the API lists them
        self.metadata = {}  # deviation id -> metadata item
        self.images = {}  # path on the image host -> the bytes served there
        self.detours = []  # a test's own answers, tried first: request -> a response, or None to pass
        self.tokens = itertools.count(1)
        self.token = None
        self.transport = httpx.MockTransport(self.handle)

    def sleep(self, seconds):
        self.waits.append(seconds)
        self.now += seconds
        self.slept = seconds

    def waited(self, where):
        """The seconds slept right before each request to the API ("api") or anywhere else ("images"), in order.
        None for a request that nothing was waited for. The two hosts share one clock, so the gap between two
        requests to one of them has the other's waits in it: this is the wait itself."""
        return [request.extensions["waited"] for request in self.requests(where)]

    def artist(self, username, links=(), **changes):
        """An artist discovery has stored, with a profile and an empty gallery on the fake API."""
        assert store.upsert_artist(username, f"https://www.deviantart.com/{username}", list(links), 3)
        self.profiles[username.lower()] = profile(username, **changes)
        self.galleries[username.lower()] = []

    def work(self, username, n, tags=("fakepainting",), drop=(), meta=None, **changes):
        """One gallery entry with its metadata and its preview image. `meta` changes the metadata item."""
        self.galleries[username.lower()].append(deviation(n, username, drop, **changes))
        self.metadata[uuid(n)] = metadata(n, username, list(tags), **(meta or {}))
        self.images[preview_path(n)] = jpeg(n)

    def handle(self, request):
        self.log.append((self.now, request))
        request.extensions["waited"], self.slept = self.slept, None
        self.threads.add(threading.get_ident())
        for detour in self.detours:
            response = detour(request)
            if response is not None:
                return response
        path, asked = request.url.path, dict(request.url.params)
        if request.url.host != API_HOST:
            data = self.images.get(path)
            if request.url.host != CDN_HOST or data is None:
                return httpx.Response(404, text="FAKE-NOT-FOUND")
            return httpx.Response(200, headers={"content-type": "image/jpeg"}, content=data)
        if path == TOKEN_PATH:
            self.token = f"FAKE-TOKEN-{next(self.tokens)}"
            return httpx.Response(200, json={"access_token": self.token, "token_type": "Bearer", "expires_in": 3600})
        if request.headers.get("authorization") != f"Bearer {self.token}":
            return httpx.Response(401, json={"error": "invalid_token", "error_description": "FAKE-ERROR"})
        if path.startswith(PROFILE_PATH):
            body = self.profiles.get(path.removeprefix(PROFILE_PATH).lower(), 400)
            if asked:  # no parameter is asked for: "expand" would bring age, sex and location
                return refusal()
            if isinstance(body, httpx.Response):
                return body
            return refusal(body) if isinstance(body, int) else httpx.Response(200, json=body)
        if path == GALLERY_PATH:
            entries = self.galleries.get(asked.get("username", "").lower())
            if entries is None or set(asked) != {"username", "limit"} or asked["limit"] != "24":
                return refusal()
            more = len(entries) > 24
            return httpx.Response(
                200, json={"has_more": more, "next_offset": 24 if more else None, "results": entries[:24]}
            )
        if path == METADATA_PATH:
            if not asked or not all(re.fullmatch(r"deviationids\[\d+\]", name) for name in asked):
                return refusal()
            ids = [value for _, value in request.url.params.multi_items()]
            return httpx.Response(200, json={"metadata": [self.metadata[id_] for id_ in ids if id_ in self.metadata]})
        return refusal(404)

    def requests(self, where=None):
        """The requests so far, in order: to the API ("api"), to anywhere else ("images"), or all of them."""
        wanted = {"api": [True], "images": [False], None: [True, False]}[where]
        return [request for _, request in self.log if (request.url.host == API_HOST) in wanted]

    def since(self, seen, where=None):
        """The requests made after the first `seen` ones."""
        earlier, self.log = self.log, self.log[seen:]
        try:
            return self.requests(where)
        finally:
            self.log = earlier

    def gaps(self, where):
        times = [at for at, request in self.log if (request.url.host == API_HOST) == (where == "api")]
        return [later - earlier for earlier, later in itertools.pairwise(times)]

    def concerns(self, request, username):
        """True for a request that names this artist: by handle, or by one of their works or images."""
        ids = [entry["deviationid"] for entry in self.galleries.get(username.lower(), [])]
        theirs = [username.lower(), *ids, *(preview_path(int(id_[-12:])) for id_ in ids)]
        return any(part.lower() in str(request.url).lower() for part in theirs)

    def asked_about(self, username):
        """Every request so far that names this artist."""
        return [request for request in self.requests() if self.concerns(request, username)]


def base():
    """The base world. See EXPECTED_STDOUT, ALPHA_KEPT and BRAVO_KEPT for what a correct run makes of it."""
    world = World()
    # ALPHA: twelve good images, listed out of order, the oldest first as if pinned. The ten newest are kept.
    world.artist(ALPHA, links=["https://zz-fake-alpha.art/"], website="https://zz-fake-alpha.art")
    for n in (1, 12, 3, 7, 5, 10, 2, 11, 4, 9, 6, 8):
        world.work(ALPHA, n)
    # BRAVO: a handle with capitals, three good images and one of every kind of work that does not qualify.
    world.artist(BRAVO)
    world.work(BRAVO, 21)
    world.work(BRAVO, 22, tags=["fakecreature", "FAKE_Armor", "fake-knight"])
    world.work(BRAVO, 23)
    world.work(BRAVO, 24, is_mature=True)
    world.work(BRAVO, 25, drop=("is_mature",))
    world.work(
        BRAVO, 26, drop=("content", "preview"), excerpt="FAKE-EXCERPT-26", text_content={"excerpt": "FAKE-EXCERPT"}
    )
    world.work(
        BRAVO, 27, videos=[{"src": f"{CDN}/f/FAKE-VIDEO-27.mp4", "quality": "FAKE", "filesize": 1, "duration": 1}]
    )
    world.work(BRAVO, 29, meta={"is_mature": True})
    # CHARLIE: stored by discovery, but the profile lookup says, in its documented code, that there is no such user.
    assert store.upsert_artist(CHARLIE, f"https://www.deviantart.com/{CHARLIE}", [], 3)
    world.profiles[CHARLIE] = refusal(404, error_code=2)
    # DELTA: stored by discovery too, and suspended since: the documented code for that.
    assert store.upsert_artist(DELTA, f"https://www.deviantart.com/{DELTA}", [], 3)
    world.profiles[DELTA] = refusal(403, error_code=0)
    # ECHO: collected by nobody, because they are on the removal list.
    world.artist(ECHO)
    world.work(ECHO, 51)
    assert store.mark_removed(ECHO) == 1
    # FOXTROT: a profile and an empty gallery.
    world.artist(FOXTROT)
    return world


def run(world, max_artists=200, db=None):
    with mock.patch.object(time, "sleep", world.sleep):
        return collect.collect(CLIENT_ID, CLIENT_SECRET, max_artists=max_artists, db=db, transport=world.transport)


def summary(out):
    """The printed counts as a dict. It also checks that every line is a label and a number, and nothing more."""
    assert all(re.fullmatch(r"[A-Za-z ,()'-]+: \d+", line) for line in out.splitlines()), out
    counts = {name: int(count) for name, count in (line.rsplit(": ", 1) for line in out.splitlines())}
    assert list(counts) == list(collect._COUNTS)
    return counts


def counts_of(world, capsys, expect_status=0, max_artists=200):
    """Run, and return the printed counts. The run must end with this exit code and, on 0, an empty stderr."""
    capsys.readouterr()
    assert run(world, max_artists) == expect_status
    captured = capsys.readouterr()
    assert expect_status or captured.err == ""
    return summary(captured.out)


def stored():
    """What is kept, as four sets: the artists with a profile, the works, the cached files, the store's queue."""
    return (
        {row["artist_key"] for row in rows("profiles")},
        {row["deviation_id"] for row in rows("works")},
        set(cached()),
        {artist[0] for artist in store.all_artists()},
    )


def nothing_lost(since):
    """True when everything that was kept still is: each of the four sets has only grown."""
    return all(now >= before for now, before in zip(stored(), since, strict=True))


def work_marks():
    return {int(work_id[-12:]): reason for work_id, (reason, _) in library.work_marks().items()}


def artist_marks():
    return {artist: mark[0] for artist, mark in library.artist_marks().items()}


def rows(table, where=None):
    library.artist_keys()
    with closing(sqlite3.connect(config.LOCAL_DB)) as conn:
        conn.row_factory = sqlite3.Row
        found = [dict(row) for row in conn.execute(f"SELECT * FROM {table} ORDER BY 1")]
    return [row for row in found if where is None or row["artist_key"] == key(where)]


def kept(username):
    """The numbers of the artist's stored works, newest first."""
    stored = sorted(rows("works", username), key=lambda row: row["published_at"], reverse=True)
    return [int(row["deviation_id"][-12:]) for row in stored]


def files(root):
    """Every file under a folder, but for the two a run keeps for each host it talks to: its lock and its pace,
    which hold numbers only (discover.Pace) and have tests of their own at the end of this file."""
    kept = (path for path in root.rglob("*") if path.is_file() and path.suffix not in (".lock", ".pace"))
    return sorted(path.relative_to(root).as_posix() for path in kept)


def cached():
    return sorted(path.name for path in library.image_dir().glob("*")) if library.image_dir().exists() else []


def image_files(numbers):
    return sorted(f"{uuid(n)}.jpg" for n in numbers)


def work_row(n, username, tags=("fakepainting",)):
    return {
        "deviation_id": uuid(n),
        "artist_key": key(username),
        "page_url": f"https://www.deviantart.com/{username.lower()}/art/FAKE-TITLE-{n}-{n}",
        "title": f"FAKE-TITLE-{n}",
        "tags": json.dumps(list(tags)),
        "thumb_url": thumb(n),
        "published_at": iso(1_700_000_000 + n * 3600),
        "image_file": f"{uuid(n)}.jpg",
    }


def profile_row(username, website):
    return {
        "artist_key": key(username),
        "tagline": f"FAKE-TAGLINE-{username}",
        "specialty": "FAKE-SPECIALTY",
        "level": "FAKE-LEVEL",
        "website": website,
        "user_hash": user_hash(username),
    }


def without_time(found):
    assert all(row["fetched_at"].endswith("+00:00") for row in found)
    return [{name: value for name, value in row.items() if name != "fetched_at"} for row in found]


def solo(n_works=4, **artist):
    """A world with ALPHA alone and this many good works, numbered from 1."""
    world = World()
    world.artist(ALPHA, **artist)
    for n in range(1, n_works + 1):
        world.work(ALPHA, n)
    return world


def locked(*names):
    """A stand-in for Path.unlink that cannot delete the files of these names, as if another program held them."""
    real = type(config.ROOT).unlink

    def unlink(path, *args, **kwargs):
        if path.name in names:
            raise PermissionError(f"FAKE-LOCKED {path}")
        real(path, *args, **kwargs)

    return unlink


def unsynced(*args, connect=sqlite3.connect, **kwargs):
    """sqlite3.connect that does not wait for the disk after each commit. Nothing else about the database changes."""
    conn = connect(*args, **kwargs)
    conn.execute("PRAGMA synchronous = OFF")
    return conn


@pytest.fixture(autouse=True)
def sandbox(tmp_path, monkeypatch, caplog):
    """In every test: the cache and the database live in the temporary directory, a real sleep is an error, and
    no log record may carry a handle, a credential, a token or anything else made up here."""
    monkeypatch.setattr(config, "CACHE_DIR", tmp_path / "cache")
    monkeypatch.setattr(config, "LOCAL_DB", tmp_path / "cache" / "pipeline.sqlite")
    monkeypatch.setattr(sqlite3, "connect", unsynced)  # the tests commit thousands of times
    monkeypatch.setattr(time, "sleep", lambda seconds: pytest.fail("the code slept for real"))
    monkeypatch.chdir(tmp_path)
    caplog.set_level(logging.DEBUG)
    yield
    logged = " ".join(f"{record.name} {record.getMessage()} {record.exc_text or ''}" for record in caplog.records)
    assert not re.search(r"FAKE|zz-fake|deviantart|wixmp", logged, re.IGNORECASE), logged


# --- the whole run ---


def test_the_base_world_gives_exactly_these_counts_rows_and_files(capsys, tmp_path):
    world = base()
    assert run(world) == 0
    captured = capsys.readouterr()
    assert (captured.out, captured.err) == (EXPECTED_STDOUT, "")
    assert len(world.log) == BASE_REQUESTS
    assert without_time(rows("profiles")) == [
        profile_row(ALPHA, "https://zz-fake-alpha.art/"),
        profile_row(BRAVO, "https://www.artstation.com/zz-fake-bravo"),
        profile_row(FOXTROT, "https://www.artstation.com/zz-fake-foxtrot"),
    ]
    assert without_time(rows("works")) == [
        *(work_row(n, ALPHA) for n in sorted(ALPHA_KEPT)),
        work_row(21, BRAVO),
        work_row(22, BRAVO, ["fakecreature", "FAKE_Armor", "fake-knight"]),
        work_row(23, BRAVO),
    ]
    assert kept(ALPHA) == ALPHA_KEPT and kept(BRAVO) == BRAVO_KEPT
    assert files(tmp_path) == [
        *(f"cache/images/{name}" for name in image_files(ALPHA_KEPT + BRAVO_KEPT)),
        "cache/pipeline.sqlite",
    ]
    for n in ALPHA_KEPT + BRAVO_KEPT:
        assert (config.CACHE_DIR / "images" / f"{uuid(n)}.jpg").read_bytes() == jpeg(n)


def test_the_three_requests_carry_exactly_the_documented_parameters_and_no_others():
    """Pinned, because a parameter too many is how private data would arrive: the profile endpoint's "expand"
    adds age, sex and location, and the gallery's "mature_content" adds mature works."""
    world = base()
    assert run(world) == 0
    api = world.requests("api")
    assert [(request.method, request.url.path) for request in api[:4]] == [
        ("POST", TOKEN_PATH),
        ("GET", PROFILE_PATH + ALPHA),
        ("GET", GALLERY_PATH),
        ("GET", METADATA_PATH),
    ]
    profiles = [request for request in api if request.url.path.startswith(PROFILE_PATH)]
    assert [(request.url.path.removeprefix(PROFILE_PATH), request.url.query) for request in profiles] == [
        (username, b"")  # the handle as DeviantArt spells it, capitals and all, and not one parameter
        for username in (ALPHA, BRAVO, CHARLIE, DELTA, FOXTROT)
    ]
    galleries = [request for request in api if request.url.path == GALLERY_PATH]
    assert [request.url.params.multi_items() for request in galleries] == [
        [("username", username), ("limit", "24")] for username in (ALPHA, BRAVO, FOXTROT)
    ]  # one page each, the documented maximum, and nobody is asked twice
    lookups = [request for request in api if request.url.path == METADATA_PATH]
    # One request an artist, naming every entry that passed the gallery's own checks: not the mature ones, not
    # the text or the film. FOXTROT has no works, so nothing is asked.
    assert [request.url.params.multi_items() for request in lookups] == [
        [(f"deviationids[{at}]", uuid(n)) for at, n in enumerate(numbers)]
        for numbers in ((1, 12, 3, 7, 5, 10, 2, 11, 4, 9, 6, 8), (21, 22, 23, 29))
    ]
    assert {request.url.host for request in api} == {API_HOST}
    assert {request.url.scheme for request in world.requests()} == {"https"}
    assert all(request.content == b"" for request in world.requests()[1:])  # and nothing rides in a body either


@pytest.mark.parametrize(
    ("path", "extra"),
    [
        (PROFILE_PATH + ALPHA, {"expand": "user.details,user.geo"}),
        (PROFILE_PATH + ALPHA, {"ext_galleries": "true"}),
        (GALLERY_PATH, {"username": ALPHA, "limit": "24", "mature_content": "true"}),
        (GALLERY_PATH, {"username": ALPHA, "limit": "50"}),
        (METADATA_PATH, {"deviationids[0]": uuid(1), "ext_submission": "true"}),
    ],
)
def test_the_fake_api_refuses_a_request_that_asks_for_more(path, extra):
    """The check on the checker: were collect ever to send one of these, every test above would see a refusal."""
    world = solo()
    world.token = "FAKE-TOKEN-0"
    request = httpx.Request(
        "GET", f"https://{API_HOST}{path}", params=extra, headers={"authorization": "Bearer FAKE-TOKEN-0"}
    )
    assert world.handle(request).status_code == 400


# --- 1. hard rule 6: no private data ---


def test_nothing_private_is_stored_printed_or_logged(capsys, tmp_path):
    world = base()
    assert run(world) == 0
    captured = capsys.readouterr()
    everything = (
        config.LOCAL_DB.read_bytes().decode("latin-1") + captured.out + captured.err + " ".join(files(tmp_path))
    )
    for marker in PRIVATE:
        assert marker not in everything
    assert "FAKE-TAGLINE-zz-fake-alpha" in everything  # the search does find what is there


def marked():
    """A profile response in which every value says where it sits, with fields the docs do not list thrown in."""

    def leaves(names, prefix):
        return {name: f"FAKE-LEAF-{prefix}{name}" for name in names}

    private = ("email", "real_name", "country", "countryid", "phone", "birthday", "age", "sex", "location", "geo")
    body = leaves((*profile(ALPHA), *private, "username", "userid", "usericon", "about", "contact", "address"), "top_")
    body["user"] = {
        **leaves(("userid", "username", "usericon", "type", "email", "utid", *private), "user_"),
        "details": leaves(("sex", "age", "joindate"), "user_details_"),
        "geo": leaves(("country", "countryid", "timezone"), "user_geo_"),
        "profile": leaves(
            (
                "real_name",
                "tagline",
                "website",
                "artist_level",
                "artist_speciality",
                "artist_specialty",
                "bio",
                "cover_photo",
            ),
            "user_profile_",
        ),
        "stats": leaves(("watchers", "friends"), "user_stats_"),
    }
    body["last_status"] = leaves(("body", "statusid", "url"), "last_status_")
    body["stats"] = leaves(("user_deviations", "profile_pageviews"), "stats_")
    body["session"] = {"user": leaves(("userid", "username", "usericon"), "session_user_")}
    return body


def leaves_of(node):
    return [leaf for value in node.values() for leaf in leaves_of(value)] if isinstance(node, dict) else [node]


def test_of_a_whole_profile_response_only_the_public_fields_are_kept_and_the_bio_is_not_one(capsys):
    """Every field of the response is labelled with its own name, the nested and the undocumented ones too.
    No label but those of the allowed fields may turn up in the database, the output or a file name. (The
    website's label is not a site, so it is not kept either.) The user id is there only as a hash."""
    world = solo()
    world.profiles[ALPHA] = marked()
    world.profiles[ALPHA]["user"]["username"] = ALPHA.upper()
    assert run(world) == 0
    captured = capsys.readouterr()
    everywhere = config.LOCAL_DB.read_bytes().decode("latin-1") + captured.out + captured.err + " ".join(cached())
    allowed = {f"FAKE-LEAF-top_{name}" for name in ("tagline", "artist_specialty", "artist_level")}
    labels = set(leaves_of(marked()))
    assert allowed < labels and len(labels) > 60 and "FAKE-LEAF-top_bio" in labels - allowed
    for label in labels - allowed:
        assert label.lower() not in everywhere.lower(), label
    assert without_time(rows("profiles")) == [
        {
            "artist_key": key(ALPHA),
            "tagline": "FAKE-LEAF-top_tagline",
            "specialty": "FAKE-LEAF-top_artist_specialty",
            "level": "FAKE-LEAF-top_artist_level",
            "website": None,
            "user_hash": hashlib.sha256(b"uid:FAKE-LEAF-user_userid").hexdigest(),
        }
    ]


@pytest.mark.parametrize(
    "tagline",
    [
        "FAKE-TAGLINE write to zz.fake@mail.invalid",
        "FAKE-TAGLINE write to zz.fa\u200bke&amp;#64;mäil.invalid",
        "FAKE-TAGLINE write to <b>zz.fake</b>@<wbr>mail.invalid",
        "FAKE-TAGLINE call 555 0199",
        "FAKE-TAGLINE call 5550-1999",
        "FAKE-TAGLINE wa.me/zzhandle",
        # What a critic got past the second version, which looked for these one spelling at a time.
        "FAKE-TAGLINE zzhandle at outlook.com",
        "FAKE-TAGLINE Tel. 555 0199",
        "FAKE-TAGLINE call 9123 4567",
        "FAKE-TAGLINE t.me/zzhandle",
        "FAKE-TAGLINE see zzhandle.art",
    ],
)
def test_a_tagline_with_an_address_a_service_or_a_number_in_it_is_not_kept_at_all(tagline):
    """Hard rule 6, the lead's change of approach: nothing is cut out of a free text. In doubt the field is dropped."""
    world = solo(tagline=tagline, artist_level="None", artist_specialty=7)
    assert run(world) == 0
    assert without_time(rows("profiles")) == [
        {**dict.fromkeys(profile_row(ALPHA, None)), "artist_key": key(ALPHA), "user_hash": user_hash(ALPHA)}
        | {"website": "https://www.artstation.com/zz-fake-alpha"}
    ]
    stored = config.LOCAL_DB.read_bytes()
    assert b"mail" not in stored and b"zz.fa" not in stored and b"0199" not in stored and b"zzhandle" not in stored
    assert b"outlook" not in stored and b"4567" not in stored


CONTACT_TITLES = [
    "FAKE-TITLE zz.fake@mail.invalid",
    "FAKE-TITLE zz.fake&amp;#64;mail.invalid",
    "FAKE-TITLE call +00 555 010 9999",
    "FAKE-TITLE zzhandle at mailhost.invalid",
    # The critic's four of round 4: they stayed in the title and in the page address made from it.
    "FAKE-TITLE call 9123 4567",
    "FAKE-TITLE Tel. 555 0199",
    "FAKE-TITLE t.me/zzhandle",
    "FAKE-TITLE wa.me/zzhandle",
]


@pytest.mark.parametrize("title", CONTACT_TITLES)
def test_a_work_whose_title_holds_contact_details_is_not_kept_at_all(title, capsys, tmp_path):
    """Hard rule 6, the critic's finding: the title was dropped and the work kept, and the page address, which
    DeviantArt makes from the title, still carried the email address or the number. The whole work is left out
    and its image is never fetched."""
    world = solo()
    made_from_the_title = "FAKE-TITLE-zz-fake-mail-invalid-555-010-9999-zzhandle-2"
    world.galleries[ALPHA][1].update(title=title, url=f"https://www.deviantart.com/{ALPHA}/art/{made_from_the_title}")
    counts = counts_of(world, capsys)
    assert counts[collect.TITLE] == 1 and counts[collect.WORKS] == 3 and kept(ALPHA) == [4, 3, 1]
    assert files(tmp_path) == [*(f"cache/images/{name}" for name in image_files([1, 3, 4])), "cache/pipeline.sqlite"]
    assert preview_path(2) not in [request.url.path for request in world.requests("images")]
    stored = config.LOCAL_DB.read_bytes()
    assert b"mail" not in stored and b"9999" not in stored and b"zzhandle" not in stored


@pytest.mark.parametrize("title", CONTACT_TITLES)
def test_a_kept_work_whose_title_comes_to_hold_contact_details_is_marked_and_what_was_kept_is_not_changed(
    title, capsys
):
    world = solo()
    assert run(world) == 0
    before = rows("works")
    made_from_the_title = "FAKE-TITLE-zz-fake-mail-invalid-555-010-9999-zzhandle-2"
    world.galleries[ALPHA][1].update(title=title, url=f"https://www.deviantart.com/{ALPHA}/art/{made_from_the_title}")
    counts = counts_of(world, capsys)
    assert counts[collect.TITLE] == 1 and counts[collect.MARKED] == 1 and work_marks() == {2: "contact details"}
    assert [row for row in rows("works") if row["deviation_id"] == uuid(2)] == [before[1]]  # as it was kept
    stored = config.LOCAL_DB.read_bytes()
    assert b"mail" not in stored and b"9999" not in stored and b"zzhandle" not in stored


def test_a_page_or_thumbnail_address_that_carries_what_the_title_does_not_costs_the_work_its_place(capsys):
    """The critic's finding: a clean title, and the address beside it carrying the email address or the number."""
    world = solo(4)
    world.galleries[ALPHA][0]["url"] = f"https://www.deviantart.com/{ALPHA}/art/FAKE-zz.fake%40mail.invalid-1"
    world.galleries[ALPHA][1]["url"] = f"https://www.deviantart.com/{ALPHA}/art/FAKE-call-555-0199-2"
    world.galleries[ALPHA][2]["thumbs"] = [{"src": thumb(3).replace("fake_thumb_3_by", "call_555_0199_by")}]
    counts = counts_of(world, capsys)
    assert counts[collect.TITLE] == 3 and kept(ALPHA) == [4]
    stored = config.LOCAL_DB.read_bytes()
    assert b"mail.invalid" not in stored and b"0199" not in stored


def test_a_title_with_a_date_or_a_serial_number_in_it_is_kept():
    """Seven and eight digits were taken for phone numbers, and 15 real titles of the first 762 were dropped."""
    world = solo(3)
    for entry, title in zip(
        world.galleries[ALPHA], ("FAKE 2019-2023", "FAKE 20231105", "FAKE No. 1234567"), strict=True
    ):
        entry["title"] = title
    assert run(world) == 0
    assert [row["title"] for row in rows("works")] == ["FAKE 2019-2023", "FAKE 20231105", "FAKE No. 1234567"]


@pytest.mark.parametrize(
    "website",
    [
        "https://wa.me/15550109999",
        "https://t.me/+15550109999",
        "https://t.me/zzhandle",
        "https://discord.gg/zzhandle",
        # What a critic got past the second version's list of messaging services.
        "https://wa.link/zzhandle",
        "https://ig.me/m/zzhandle",
        "https://dsc.gg/zzhandle",
        "https://discord.me/zzhandle",
        "https://telegram.im/zzhandle",
        "https://www.facebook.com/messages/t/zzhandle",
        "https://www.instagram.com/direct/zzhandle",
        "https://zz-fake-alpha.art/?mail=zz.fake@mail.invalid",
        "https://zz-fake-alpha.art/zzhandle",
        "https://www.artstation.com/zzhandle-5550199",
        "https://www.artstation.com/zzhandle/data:image/png;base64,RkFLRQ==",
        "https://www.artstation.com/zzhandle/data%3Aimage/png;base64,RkFLRQ==",
    ],
)
def test_a_website_that_is_not_the_artists_own_site_or_a_profile_on_a_listed_host_is_not_kept(website):
    world = solo(1, website=website)
    assert run(world) == 0
    assert rows("profiles")[0]["website"] is None and rows("profiles")[0]["tagline"] == f"FAKE-TAGLINE-{ALPHA}"
    stored = config.LOCAL_DB.read_bytes()
    assert b"zzhandle" not in stored and b"0199" not in stored and b"mail" not in stored and b"wa.me" not in stored


# --- 2. which works qualify ---

NOT_QUALIFYING = {
    "mature": ({"is_mature": True}, collect.MATURE),
    "maturity not stated": ({"drop": ("is_mature",)}, collect.MATURE),
    "maturity null": ({"is_mature": None}, collect.MATURE),
    "maturity as a word": ({"is_mature": "false"}, collect.MATURE),
    "maturity as a number": ({"is_mature": 0}, collect.MATURE),
    "mature by its metadata": ({"meta": {"is_mature": True}}, collect.MATURE),
    "maturity not stated in its metadata": ({"meta": {"drop": ("is_mature",)}}, collect.MATURE),
    "a text": ({"excerpt": "FAKE-EXCERPT"}, collect.NOT_IMAGE),
    "a text with a preview": ({"text_content": {"excerpt": "FAKE-EXCERPT", "body": {}}}, collect.NOT_IMAGE),
    "a film": ({"videos": [{"src": f"{CDN}/f/FAKE-VIDEO-2.mp4"}]}, collect.NOT_IMAGE),
    "a flash file": ({"flash": {"src": f"{CDN}/f/FAKE-FLASH-2.swf", "height": 1, "width": 1}}, collect.NOT_IMAGE),
    "no content": ({"drop": ("content",)}, collect.NOT_IMAGE),
    "no preview": ({"drop": ("preview",)}, collect.NOT_IMAGE),
    "a preview with no address": ({"preview": {"height": 600, "width": 800}}, collect.NOT_IMAGE),
    "a preview that is not an object": ({"preview": f"{CDN}/f/FAKE-PREVIEW-2.jpg"}, collect.NOT_IMAGE),
    "locked behind a tier": ({"tier_access": "locked"}, collect.NOT_IMAGE),
    "locked for subscribers": ({"tier_access": "locked-subscribed"}, collect.NOT_IMAGE),
    "deleted": ({"is_deleted": True}, collect.NOT_IMAGE),
    "deletion not stated": ({"drop": ("is_deleted",)}, collect.NOT_IMAGE),
    "another author's": ({"author": author("zz-fake-other")}, collect.NOT_OWN),
    "no author": ({"drop": ("author",)}, collect.NOT_OWN),
    "an author with no name": ({"author": {"userid": "FAKE-USERID"}}, collect.NOT_OWN),
    "no metadata": ({"meta": None}, collect.NO_METADATA),
    "metadata with no tags": ({"meta": {"drop": ("tags",)}}, collect.NO_METADATA),
    "metadata with tags that are no list": ({"meta": {"tags": "fakepainting"}}, collect.NO_METADATA),
    # The next four are where the first version failed open: the tags could not be read, and the work was kept
    # with no AI check at all. "midjourney" is in each of them, in a shape that version did not look at.
    "tags that are words, not objects": ({"meta": {"tags": ["fakepainting", "midjourney"]}}, collect.NO_METADATA),
    "a tag that is not an object": (
        {"meta": {"tags": [{"tag_name": "fakepainting"}, ["midjourney"]]}},
        collect.NO_METADATA,
    ),
    "a tag with no name": (
        {"meta": {"tags": [{"tag_name": "fakepainting"}, {"name": "midjourney"}]}},
        collect.NO_METADATA,
    ),
    "a tag whose name is no word": ({"meta": {"tags": [{"tag_name": ["midjourney"]}]}}, collect.NO_METADATA),
    "an id that is no UUID": ({"deviationid": "../../FAKE-ID"}, collect.MALFORMED),
    "no id": ({"drop": ("deviationid",)}, collect.MALFORMED),
    "a page that is not on DeviantArt": (
        {"url": "https://zz-fake-evil.invalid/zz-fake-alpha/art/FAKE-2"},
        collect.MALFORMED,
    ),
    "a page over plain http": ({"url": "http://www.deviantart.com/zz-fake-alpha/art/FAKE-2"}, collect.MALFORMED),
    "a page with an embedded image in its query": (
        {"url": "https://www.deviantart.com/zz-fake-alpha/art/FAKE-2?x=data:image/png;base64,RkFLRQ=="},
        collect.MALFORMED,
    ),
    "a page with a query": (
        {"url": "https://www.deviantart.com/zz-fake-alpha/art/FAKE-2?FAKE-QUERY=1"},
        collect.MALFORMED,
    ),
    "a page that is not a work's page": (
        {"url": "https://www.deviantart.com/zz-fake-alpha/journal/FAKE-2"},
        collect.MALFORMED,
    ),
    "no page": ({"drop": ("url",)}, collect.MALFORMED),
    # A thumbnail of DeviantArt's shape that is not acceptable. One of any other shape is no thumbnail: see below.
    "a thumbnail with an embedded image in its query": (
        {"thumbs": [{"src": f"{thumb(2).partition('?')[0]}?x=data:image/png;base64,RkFLRQ=="}]},
        collect.MALFORMED,
    ),
    # The critic's two probes for an image kept as text: in the token of a signature, and as a page's title part.
    "a thumbnail with an image riding in its signature": (
        {"thumbs": [{"src": f"{thumb(2).partition('?')[0]}?token={HEADER}.{'F' * 1200}.FAKE-SIGNATURE"}]},
        collect.MALFORMED,
    ),
    "a thumbnail whose signature has no header": (
        {"thumbs": [{"src": f"{thumb(2).partition('?')[0]}?token=FAKE-HEADER.FAKE-CLAIMS.FAKE-SIGNATURE"}]},
        collect.MALFORMED,
    ),
    "a page whose title part is an encoded file": (
        {"url": f"https://www.deviantart.com/zz-fake-alpha/art/{'F' * 201}"},
        collect.MALFORMED,
    ),
}


@pytest.mark.parametrize("case", NOT_QUALIFYING)
def test_a_work_that_does_not_qualify_is_not_stored_and_its_image_is_never_fetched(case, capsys):
    """Requirement 2: only an image, explicitly not mature, the artist's own, with tags that can be read and a
    page and a thumbnail on DeviantArt. Work 2 of four is spoiled in one way; the other three are kept."""
    changes, label = NOT_QUALIFYING[case]
    changes = dict(changes)
    meta = changes.pop("meta", {})
    world = World()
    world.artist(ALPHA)
    for n in (1, 3, 4):
        world.work(ALPHA, n)
    world.work(ALPHA, 2, **changes)
    if meta is None:
        del world.metadata[uuid(2)]
    else:
        item = metadata(2, ALPHA, ["fakepainting"], drop=meta.get("drop", ()))
        world.metadata[uuid(2)] = {**item, **{name: value for name, value in meta.items() if name != "drop"}}
    counts = counts_of(world, capsys)
    assert kept(ALPHA) == [4, 3, 1]
    assert cached() == image_files([1, 3, 4])
    assert [request.url.path for request in world.requests("images")] == [preview_path(n) for n in (4, 3, 1)]
    assert counts[label] == 1 and counts[collect.WORKS] == 3 and counts[collect.PROFILES] == 1
    assert sum(count for name, count in counts.items() if name.startswith("skipped")) == 1


@pytest.mark.parametrize("good_one_first", [True, False])
@pytest.mark.parametrize("said", [True, None, "missing"])
@pytest.mark.parametrize("where", ["gallery", "metadata"])
def test_a_work_listed_twice_is_not_mature_only_if_every_listing_of_it_says_so(where, said, good_one_first, capsys):
    """Round 7: one id listed twice, once as mature and once as not, was stored as a new work, whichever of the
    answer's two lists it was in. Work 2 of three is listed twice here, and one of the two does not say "false"."""
    world = solo(3)
    changes = {"drop": ("is_mature",)} if said == "missing" else {"is_mature": said}
    if where == "gallery":
        pair = [world.galleries[ALPHA][1], deviation(2, ALPHA, **changes)]
        world.galleries[ALPHA][1:2] = pair if good_one_first else pair[::-1]
    else:
        pair = [world.metadata[uuid(2)], metadata(2, ALPHA, ["fakepainting"], **changes)]
        items = [world.metadata[uuid(1)], *(pair if good_one_first else pair[::-1]), world.metadata[uuid(3)]]
        world.detours.append(
            lambda request: httpx.Response(200, json={"metadata": items}) if request.url.path == METADATA_PATH else None
        )
    counts = counts_of(world, capsys)
    assert kept(ALPHA) == [3, 1] and cached() == image_files([1, 3]) and counts[collect.MATURE] == 1
    assert [request.url.path for request in world.requests("images")] == [preview_path(3), preview_path(1)]


def test_the_ten_most_recent_qualifying_works_are_kept_whatever_order_the_gallery_lists_them_in():
    world = World()
    world.artist(ALPHA)
    order = [5, 30, 12, 7, 19, 2, 25, 14, 9, 21, 3, 28, 17, 11, 23, 6, 16, 1, 27, 8, 13, 20, 4, 29]  # a full page of 24
    for n in order:
        world.work(ALPHA, n, **({"is_mature": True} if n in (30, 28) else {}))
    world.work(ALPHA, 99)  # newer than all of them, but on the gallery's second page: see the ponytail note
    assert run(world) == 0
    assert kept(ALPHA) == [29, 27, 25, 23, 21, 20, 19, 17, 16, 14]
    assert len(kept(ALPHA)) == collect.MAX_WORKS == 10
    assert len([request for request in world.requests("api") if request.url.path == GALLERY_PATH]) == 1


def test_a_work_with_no_usable_date_is_kept_but_ranks_last():
    world = solo(3)
    world.galleries[ALPHA][0]["published_time"] = "FAKE-TIME"
    del world.galleries[ALPHA][1]["published_time"]
    assert run(world) == 0
    assert [(int(row["deviation_id"][-2:]), row["published_at"]) for row in rows("works")] == [
        (1, None),
        (2, None),
        (3, iso(1_700_000_000 + 3 * 3600)),
    ]


# --- 3. hard rules 1 and 2: where image bytes go ---


def test_image_bytes_are_written_under_the_cache_directory_and_nowhere_else(tmp_path):
    """The whole temporary directory, which is also the working directory, holds the database and the images."""
    world = base()
    assert run(world) == 0
    assert files(tmp_path) == [
        *(f"cache/images/{name}" for name in image_files(ALPHA_KEPT + BRAVO_KEPT)),
        "cache/pipeline.sqlite",
    ]
    assert library.image_dir() == config.CACHE_DIR / "images" == tmp_path / "cache" / "images"
    assert not any("dir" in name or "path" in name for name in inspect.signature(collect.collect).parameters)


def test_nothing_image_like_is_written_to_the_database():
    world = base()
    assert run(world) == 0
    stored = config.LOCAL_DB.read_bytes()
    assert b"FAKE-IMAGE-BYTES" not in stored and b"base64" not in stored and b"data:" not in stored
    with closing(sqlite3.connect(config.LOCAL_DB)) as conn:
        for table in ("profiles", "works"):
            names = [name for _, name, *_ in conn.execute(f"PRAGMA table_info({table})")]
            kinds = conn.execute(f"SELECT DISTINCT {', '.join(f'typeof({name})' for name in names)} FROM {table}")
            assert {kind for row in kinds for kind in row} <= {"text", "null"}
    for row in rows("works"):  # addresses and a file name, and that is all there is about the image
        assert row["thumb_url"].startswith(f"{CDN}/f/") and "fake_thumb_" in row["thumb_url"]
        assert row["page_url"].startswith("https://www.deviantart.com/")
        assert re.fullmatch(r"[0-9A-F-]{36}\.jpg", row["image_file"])


def test_only_the_preview_is_downloaded_never_the_original_or_a_thumbnail():
    world = base()
    assert run(world) == 0
    paths = [request.url.path for request in world.requests("images")]
    assert sorted(paths) == sorted(preview_path(n) for n in ALPHA_KEPT + BRAVO_KEPT)
    assert not any("CONTENT" in str(request.url) or "fake_thumb" in str(request.url) for request in world.requests())
    assert {request.url.host for request in world.requests("images")} == {CDN_HOST}


@pytest.mark.parametrize(
    ("content_type", "body"),
    [
        ("text/html", b"<html>FAKE</html>"),
        ("image/svg+xml", b"<svg>FAKE</svg>"),
        ("application/octet-stream", jpeg(2)),
        ("application/json", b'{"FAKE": 1}'),
        ("image/bmp", b"BMFAKE"),
        (None, jpeg(2)),
        ("image/jpeg", b"<html>FAKE error page</html>"),  # the right label on the wrong bytes
        ("image/png", jpeg(2)),
        ("image/png", b"\x89PNG\r\n\x1a\n" + b"FAKE-NO-HEADER" * 20),
        ("image/gif", b"GIF8" + b"FAKE" * 20),
        ("image/jpeg", b""),
        # What a critic got past the first version, which looked at four bytes: sound and film are RIFF files too.
        ("image/webp", b"RIFF\x24\x00\x00\x00WAVEfmt " + b"FAKE-SOUND" * 20),
        ("image/webp", b"RIFF\x24\x00\x00\x00AVI LIST" + b"FAKE-FILM" * 20),
        ("image/webp", b"RIFF"),
    ],
)
def test_a_download_that_is_not_an_accepted_image_type_is_not_written(content_type, body, capsys, tmp_path):
    world = solo()
    headers = {"content-type": content_type} if content_type else {}
    world.detours.append(
        lambda request: (
            httpx.Response(200, headers=headers, content=body) if request.url.path == preview_path(2) else None
        )
    )
    counts = counts_of(world, capsys)
    assert counts[collect.IMAGE_WRONG_TYPE] == 1 and counts[collect.DOWNLOADED] == 3
    assert kept(ALPHA) == [4, 3, 1] and cached() == image_files([1, 3, 4])
    assert files(tmp_path) == [*(f"cache/images/{name}" for name in image_files([1, 3, 4])), "cache/pipeline.sqlite"]


@pytest.mark.parametrize(
    ("content_type", "start", "extension"),
    [
        ("image/jpeg; charset=binary", b"\xff\xd8\xff", "jpg"),
        ("IMAGE/JPEG", b"\xff\xd8\xff", "jpg"),
        ("image/png", PNG_START, "png"),
        ("image/gif", b"GIF89a", "gif"),
        ("image/gif", b"GIF87a", "gif"),
        ("image/webp", WEBP_START, "webp"),
    ],
)
def test_each_accepted_image_type_is_written_under_its_own_extension(content_type, start, extension):
    world = solo(1)
    body = start + b"FAKE-IMAGE-BYTES"
    world.detours.append(
        lambda request: (
            httpx.Response(200, headers={"content-type": content_type}, content=body)
            if request.url.host == CDN_HOST
            else None
        )
    )
    assert run(world) == 0
    assert cached() == [f"{uuid(1)}.{extension}"] and rows("works")[0]["image_file"] == f"{uuid(1)}.{extension}"
    assert (library.image_dir() / f"{uuid(1)}.{extension}").read_bytes() == body


def test_an_image_over_the_size_cap_is_given_up_on_without_reading_it_all(capsys, tmp_path):
    """The body is 64 MB, a megabyte at a time. The download stops at the first megabyte past the cap."""
    world = solo()
    served = []

    def megabytes():
        for _ in range(64):
            served.append(1)
            yield b"\xff\xd8\xff" + b"\x00" * (1024 * 1024)

    world.detours.append(
        lambda request: (
            httpx.Response(200, headers={"content-type": "image/jpeg"}, content=megabytes())
            if request.url.path == preview_path(2)
            else None
        )
    )
    counts = counts_of(world, capsys)
    assert counts[collect.IMAGE_TOO_LARGE] == 1 and counts[collect.DOWNLOADED] == 3
    assert library.MAX_IMAGE_BYTES == 10 * 1024 * 1024 and len(served) == 10
    assert kept(ALPHA) == [4, 3, 1]
    assert files(tmp_path) == [*(f"cache/images/{name}" for name in image_files([1, 3, 4])), "cache/pipeline.sqlite"]


JPEG_START = bytes([0xFF, 0xD8, 0xFF])


def test_images_over_the_size_cap_do_not_stop_the_run_however_many_come_in_a_row(capsys):
    """The first run at scale stopped on three of them in a row from one artist. The host served each one whole;
    our cap refused it. That says nothing of the host, so it is not a refusal toward MAX_REFUSALS."""
    world = solo(n_works=5)

    def huge(request):
        if request.url.path in {preview_path(n) for n in (1, 2, 3, 4)}:
            body = JPEG_START + bytes(11 << 20)  # past the 10 MB cap
            return httpx.Response(200, headers={"content-type": "image/jpeg"}, content=body)
        return None

    world.detours.append(huge)
    counts = counts_of(world, capsys)  # exit 0 and nothing on stderr: the run was not stopped
    assert counts[collect.IMAGE_TOO_LARGE] == 4 > collect.MAX_REFUSALS and counts[collect.IMAGE_LIMITED] == 0
    assert counts[collect.DOWNLOADED] == 1 and kept(ALPHA) == [5]


def test_the_body_of_a_download_of_the_wrong_type_is_not_read_at_all(capsys):
    """The type is checked on the headers. Not one byte of a page, a script or an archive is pulled in."""
    world = solo()
    served = []

    def megabytes():
        for _ in range(64):
            served.append(1)
            yield b"<html>FAKE</html>" * 60_000

    world.detours.append(
        lambda request: (
            httpx.Response(200, headers={"content-type": "text/html"}, content=megabytes())
            if request.url.path == preview_path(2)
            else None
        )
    )
    assert counts_of(world, capsys)[collect.IMAGE_WRONG_TYPE] == 1
    assert served == [] and kept(ALPHA) == [4, 3, 1]


def test_an_image_exactly_at_the_cap_is_kept_and_one_just_over_is_not(monkeypatch):
    monkeypatch.setattr(library, "MAX_IMAGE_BYTES", len(jpeg(1)))
    world = solo(2)
    world.images[preview_path(2)] = jpeg(2) + b"\x00" * 10
    assert run(world) == 0
    assert kept(ALPHA) == [1]


HOSTILE_PREVIEWS = [
    "http://images-wixmp-zzcdn.wixmp.com/f/FAKE-PREVIEW-2.jpg",
    "https://zz-fake-evil.invalid/f/FAKE-PREVIEW-2.jpg",
    "https://wixmp.com.zz-fake-evil.invalid/f/FAKE-PREVIEW-2.jpg",
    "https://zz-fake-evilwixmp.com/f/FAKE-PREVIEW-2.jpg",
    "https://images-wixmp-zzcdn.wixmp.com@zz-fake-evil.invalid/f/FAKE-PREVIEW-2.jpg",
    "https://zz-fake-evil.invalid\\@images-wixmp-zzcdn.wixmp.com/f/FAKE-PREVIEW-2.jpg",
    "https://images-wixmp-zzcdn.wixmp.com:8443/f/FAKE-PREVIEW-2.jpg",
    "https://images-wixmp-zzcdn.wixmp.com/f/FAKE-PREVIEW-2.jpg?FAKE-QUERY=1",
    "https://images-wixmp-zzcdn.wixmp.com/f/FAKE-PREVIEW-2.jpg?x=data:image/png;base64,RkFLRQ==",
    "https://images-wixmp-zzcdn.wixmp.com/f/FAKE-PREVIEW-2.jpg#FAKE-FRAGMENT",
    "https://images-wixmp-zzcdn.wixmp.com/f/FAKE-PREVIEW-2.jpg?token=FAKE-HEADER.FAKE-CLAIMS.FAKE-SIGNATURE",
    f"https://images-wixmp-zzcdn.wixmp.com/f/FAKE-PREVIEW-2.jpg?token={HEADER}.{'F' * 1200}.FAKE-SIGNATURE",
    "https://127.0.0.1/f/FAKE-PREVIEW-2.jpg",
    "https://localhost/f/FAKE-PREVIEW-2.jpg",
    "http://169.254.169.254/latest/FAKE",
    "file:///FAKE/PREVIEW-2.jpg",
    "//images-wixmp-zzcdn.wixmp.com/f/FAKE-PREVIEW-2.jpg",
    "/f/FAKE-PREVIEW-2.jpg",
    "data:image/jpeg;base64,RkFLRQ==",
    "",
]


@pytest.mark.parametrize("address", HOSTILE_PREVIEWS)
def test_an_image_is_only_ever_fetched_over_https_from_deviantarts_image_hosts(address, capsys):
    world = solo()
    world.galleries[ALPHA][1]["preview"]["src"] = address
    counts = counts_of(world, capsys)
    assert counts[collect.MALFORMED] == 1 and kept(ALPHA) == [4, 3, 1]
    assert {(request.url.scheme, request.url.host) for request in world.requests()} == {
        ("https", API_HOST),
        ("https", CDN_HOST),
    }
    assert [str(request.url).partition(CDN)[2] for request in world.requests("images")] == [
        f"{preview_path(n)}?{signed(n)}" for n in (4, 3, 1)
    ]


def test_an_embedded_image_in_a_text_field_is_not_stored():
    embedded = "data:image/png;base64," + "FAKEBASE64" * 20_000
    chunked = "data:image/jpeg;FAKE-PARAMETER=" + "x" * 150 + ";base64," + " ".join(["FAKEBASE64" * 7] * 30)
    website = f"https://www.artstation.com/{chunked}"
    world = solo(3, tagline=chunked, artist_specialty=embedded, artist_level=chunked, website=website)
    world.galleries[ALPHA][0]["title"] = f"FAKE-TITLE {embedded}"
    world.galleries[ALPHA][1]["title"] = "FAKE-TITLE data:image/gif;base64,R0lGODlhAQABAAAAACw="
    world.metadata[uuid(1)] = metadata(1, ALPHA, ["fakepainting", embedded, chunked, "FAKEBASE64" * 7])
    assert run(world) == 0
    stored = rows("profiles")[0]
    assert [stored[name] for name in ("tagline", "specialty", "level", "website")] == [None] * 4
    assert [(row["title"], row["tags"]) for row in rows("works")] == [
        (None, '["fakepainting"]'),
        (None, '["fakepainting"]'),
        ("FAKE-TITLE-3", '["fakepainting"]'),
    ]
    assert b"FAKEBASE64" not in config.LOCAL_DB.read_bytes() and b"base64" not in config.LOCAL_DB.read_bytes()


# --- 4. running it again ---


def test_a_second_run_makes_no_duplicates_and_downloads_nothing_that_is_cached(capsys, tmp_path):
    world = base()
    assert run(world) == 0
    first = (without_time(rows("profiles")), without_time(rows("works")), files(tmp_path), store.all_artists())
    stamps = {path.name: path.stat().st_mtime_ns for path in library.image_dir().iterdir()}
    capsys.readouterr()
    seen = len(world.log)

    assert run(world) == 0
    counts = summary(capsys.readouterr().out)
    assert (without_time(rows("profiles")), without_time(rows("works")), files(tmp_path), store.all_artists()) == first
    assert artist_marks() == dict.fromkeys((key(CHARLIE), key(DELTA)), "account gone")
    assert world.since(seen, "images") == [] and len(world.since(seen)) == BASE_REQUESTS - 13
    assert counts[collect.DOWNLOADED] == 0 and counts[collect.CACHED] == 13 and counts[collect.WORKS] == 13
    assert counts[collect.ORPHANS] == 0
    assert {path.name: path.stat().st_mtime_ns for path in library.image_dir().iterdir()} == stamps  # not rewritten
    with closing(sqlite3.connect(config.LOCAL_DB)) as conn:
        assert conn.execute("SELECT count(*), count(DISTINCT deviation_id) FROM works").fetchone() == (13, 13)
        assert conn.execute("SELECT count(*), count(DISTINCT artist_key) FROM profiles").fetchone() == (3, 3)


def test_a_missing_image_is_downloaded_again_and_only_that_one():
    world = base()
    assert run(world) == 0
    (library.image_dir() / f"{uuid(7)}.jpg").unlink()
    seen = len(world.log)
    assert run(world) == 0
    assert [request.url.path for request in world.since(seen, "images")] == [preview_path(7)]
    assert cached() == image_files(ALPHA_KEPT + BRAVO_KEPT)


SAID = {  # what an entry, or its metadata, can say against a work: (the change, the mark)
    "deleted": (lambda entry, meta: entry.update(is_deleted=True), "not a public image"),
    "locked": (lambda entry, meta: entry.update(tier_access="locked"), "not a public image"),
    "a film": (lambda entry, meta: entry.update(videos=[{"src": "FAKE"}]), "not a public image"),
    "another author's": (lambda entry, meta: entry.update(author=author(BRAVO)), "another author's"),
    "mature in the gallery": (lambda entry, meta: entry.update(is_mature=True), "mature"),
    "mature in the metadata": (lambda entry, meta: meta.update(is_mature=True), "mature"),
}


@pytest.mark.parametrize("said", SAID)
def test_a_kept_work_is_marked_for_what_its_entry_says_against_it_and_stays_as_it_was_kept(said, capsys):
    change, reason = SAID[said]
    world = solo(4)
    assert run(world) == 0
    before = rows("works")
    change(world.galleries[ALPHA][1], world.metadata[uuid(2)])
    counts = counts_of(world, capsys)
    assert work_marks() == {2: reason} and counts[collect.MARKED] == 1 and counts[collect.WORKS] == 3
    assert [row for row in rows("works") if row["deviation_id"] == uuid(2)] == [before[1]] and len(cached()) == 4


def test_a_kept_work_that_qualifies_again_loses_its_mark_though_it_is_no_longer_among_the_newest(capsys):
    world = solo(10)
    assert run(world) == 0
    world.galleries[ALPHA][0]["is_mature"] = True  # work 1, the oldest
    assert counts_of(world, capsys)[collect.MARKED] == 1 and work_marks() == {1: "mature"}
    world.galleries[ALPHA][0]["is_mature"] = False
    world.work(ALPHA, 11)  # ten newer ones now: work 1 is seen, and is not one of the ten taken
    counts = counts_of(world, capsys)
    assert counts[collect.WORKS] == 10 and work_marks() == {} and len(rows("works")) == 11


def test_a_removed_artist_of_whom_only_a_mark_is_kept_loses_that_too(capsys):
    world = base()
    library.mark_artist(key(ECHO), "account gone")  # ECHO is on the removal list, and nothing else names them here
    counts_of(world, capsys)
    assert key(ECHO) not in library.artist_marks() and b"zz-fake-echo" not in config.LOCAL_DB.read_bytes()


def test_a_work_is_collected_whatever_its_tags_say(capsys):
    """Round 6: the first-pass AI rule is gone from this stage, list, marks and all. In every round it failed in
    both directions, and the direction that marks a human artist is the one that must not happen. The analysis
    stage reads each work's tags and title with a language model, and the push stage holds back what it flags."""
    tags = [
        "ai",
        "AI_Generated",
        "midjourney",
        "aiart",
        "dreamup",
        "no_ai",
        "AI学習禁止",
        "ai_art",
        "synthography",
        "AI",
    ]
    world = solo(10)
    for n in range(1, 11):
        world.metadata[uuid(n)] = metadata(n, ALPHA, ["fakeportrait", tags[n - 1]])
    counts = counts_of(world, capsys)
    assert counts[collect.WORKS] == counts[collect.DOWNLOADED] == 10 and artist_marks() == work_marks() == {}
    assert json.loads(rows("works")[0]["tags"]) == ["fakeportrait", "ai"]  # kept as the artist wrote them
    assert not hasattr(collect, "AI_TAGS") and not hasattr(collect, "_is_ai")


def test_a_work_that_left_the_gallery_stays_and_one_that_stopped_qualifying_is_marked(monkeypatch):
    """Round 5. Missing from a later answer: kept, with the time it was last seen. Said to be mature: kept, and
    marked with the reason. Later stages read both and decide what is published."""
    ticking(monkeypatch)
    world = base()
    assert run(world) == 0
    before = {row["deviation_id"]: row for row in rows("works")}
    world.galleries[ALPHA] = [entry for entry in world.galleries[ALPHA] if entry["deviationid"] != uuid(12)]  # deleted
    world.galleries[ALPHA][1]["is_mature"] = True  # work 3
    world.work(ALPHA, 13)  # and a new one
    assert run(world) == 0
    after = {row["deviation_id"]: row for row in rows("works")}
    assert kept(ALPHA) == [13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2] and kept(BRAVO) == BRAVO_KEPT
    assert cached() == image_files([*range(2, 14), *BRAVO_KEPT])
    assert work_marks() == {3: "mature"}
    assert all(after[uuid(n)] == before[uuid(n)] for n in (12, 3))  # as they were kept
    assert after[uuid(12)]["fetched_at"] < after[uuid(10)]["fetched_at"]  # last seen a run ago: it was missing
    world.galleries[ALPHA][1]["is_mature"] = False
    assert run(world) == 0 and work_marks() == {}  # and a mark goes when the reason does


GIVEN = {  # what an entry of the documented shape can give for a field, when it gives nothing that can be kept
    "title": ["missing", None, 7, ["FAKE"], "", "  "],
    "published_time": ["missing", None, "FAKE", ["FAKE"]],
    "thumbs": ["missing", None, 7, [], ["FAKE"], [{"FAKE": 1}]],
}


@pytest.mark.parametrize(("field", "value"), [(field, value) for field, values in GIVEN.items() for value in values])
def test_a_field_an_entry_does_not_give_leaves_what_is_kept_for_the_work(field, value, capsys):
    """The critic's finding: entries with no title set the kept title to NULL on 24 of 24 works, the same for the
    publish time, and with no thumbnails the kept thumbnail was replaced by the preview address. Exit 0, nothing
    counted. Round 7: an empty title did the same, and so did an empty tag list (see the test after this one)."""
    world = solo(3)
    assert run(world) == 0
    before, sizes = rows("works"), library.thumb_sizes()
    for entry in world.galleries[ALPHA]:
        entry[field] = value
        if value == "missing":
            del entry[field]
    counts = counts_of(world, capsys)
    assert counts[collect.WORKS] == 3 and library.thumb_sizes() == sizes == dict.fromkeys(
        map(uuid, (1, 2, 3)), (300, 300)
    )
    assert [{**row, "fetched_at": None} for row in rows("works")] == [{**row, "fetched_at": None} for row in before]
    assert all(row["title"] and row["published_at"] and "fake_thumb" in row["thumb_url"] for row in rows("works"))


def test_an_empty_title_or_tag_list_leaves_what_is_kept_for_the_work_and_a_new_work_has_neither(capsys):
    world = solo(3)
    assert run(world) == 0
    before = rows("works")
    for n, entry in enumerate(world.galleries[ALPHA], 1):
        entry["title"] = ""
        world.metadata[uuid(n)]["tags"] = []
    world.work(ALPHA, 4, tags=[], title="")
    assert counts_of(world, capsys)[collect.WORKS] == 4
    after = rows("works")
    assert [{**row, "fetched_at": None} for row in after[:3]] == [{**row, "fetched_at": None} for row in before]
    assert (after[3]["title"], after[3]["tags"]) == (None, "[]")


def test_a_new_work_without_thumbnails_is_shown_by_its_preview_and_a_kept_one_takes_a_thumbnail_when_given(capsys):
    world = solo(2)
    world.galleries[ALPHA][0].pop("thumbs")
    assert run(world) == 0
    assert [row["thumb_url"] for row in rows("works")] == [f"{CDN}{preview_path(1)}?{signed(1)}", thumb(2)]
    assert library.thumb_sizes() == {uuid(2): (300, 300)}  # the size the API gave for the thumbnail that is kept
    world.galleries[ALPHA][0]["thumbs"] = [{"src": thumb(1, 400), "width": 400, "height": 250}]
    world.galleries[ALPHA][1]["thumbs"] = [{"src": thumb(2), "width": "FAKE"}]
    assert run(world) == 0
    assert rows("works")[0]["thumb_url"] == thumb(1, 400)
    assert library.thumb_sizes() == {uuid(1): (400, 250), uuid(2): (300, 300)}  # both or neither: the old one stays


NO_THUMBNAIL = {  # what an entry can give for a thumbnail that is not of the shape DeviantArt's own have
    "the critic's first": f"{CDN}/f/abc/whatsapp-5550199123",
    "the critic's second": f"{CDN}/f/call-5550199123/t_by_zz_d1.jpg?{signed(2)}",
    "a short path": f"{CDN}/f/FAKE-THUMB-2.jpg?{signed(2)}",
    "a path with one part too many": thumb(2).replace("/v1/", "/v1/call-5550199123/"),
    "another host": "https://zz-fake-evil.invalid/FAKE.jpg",
    "another of DeviantArt's hosts": thumb(2).replace(CDN_HOST, "zzcdn00.deviantart.net"),
    "an embedded image": "data:image/png;base64,RkFLRQ==",
    "no address": 5550199123,
}


@pytest.mark.parametrize("case", NO_THUMBNAIL)
def test_an_entry_whose_thumbnail_is_not_of_deviantarts_shape_has_no_thumbnail(case, capsys):
    """Round 7: new works were stored with thumbnail paths such as /f/abc/whatsapp-5550199123. Such an entry is
    taken as having no thumbnail. A kept work keeps the one it has, a new work is shown by its preview, and a
    new work whose preview has not the shape either is not kept."""
    world = solo(1)
    assert run(world) == 0
    world.work(ALPHA, 2)
    world.work(ALPHA, 3, preview={"src": f"{CDN}/f/FAKE-PREVIEW-3.jpg?{signed(3)}", "height": 600, "width": 800})
    for entry in world.galleries[ALPHA]:
        entry["thumbs"] = [{"src": NO_THUMBNAIL[case], "height": 77, "width": 77}]
    counts = counts_of(world, capsys)
    assert [row["thumb_url"] for row in rows("works")] == [thumb(1), f"{CDN}{preview_path(2)}?{signed(2)}"]
    assert counts[collect.MALFORMED] == 1 and counts[collect.WORKS] == 2 and cached() == image_files([1, 2])
    assert library.thumb_sizes() == {uuid(1): (300, 300)}  # nor is its size taken for the thumbnail that is kept
    assert not any("PREVIEW-3" in str(request.url) for request in world.requests("images"))
    assert b"5550199" not in config.LOCAL_DB.read_bytes() and b"FAKE-THUMB" not in config.LOCAL_DB.read_bytes()


def test_a_refreshed_profile_is_brought_up_to_date_where_the_answer_says_something():
    world = solo(1)
    assert run(world) == 0
    world.profiles[ALPHA] = profile(ALPHA, tagline="FAKE-TAGLINE-NEW", artist_level=None, website="")
    del world.profiles[ALPHA]["artist_specialty"]
    assert run(world) == 0
    # A new tagline. A website that is an empty text, a level that is null and a specialty that is not there say
    # nothing, and what is kept stays: the critic's findings were a 200 that blanked every field, and in round 7
    # an empty text that blanked the one it was given for.
    kept_as = {**profile_row(ALPHA, "https://www.artstation.com/zz-fake-alpha"), "tagline": "FAKE-TAGLINE-NEW"}
    assert without_time(rows("profiles")) == [kept_as]
    world.profiles[ALPHA] = profile(ALPHA, tagline="", artist_level=" ", artist_specialty="None", website="")
    assert run(world) == 0 and without_time(rows("profiles")) == [kept_as]


REFUSED_ANSWERS = {  # an answer that says something, and that the library's rules refuse: the critic's four
    "a tagline that names a domain": {"tagline": "FAKE-TAGLINE see zz-fake-alpha.art"},
    "a website that is none": {"website": "N/A"},
    "a level of seven digits": {"artist_level": "5550199"},
    "a specialty with an address in it": {"artist_specialty": "FAKE write to zz.fake@mail.invalid"},
    "a title that is a run of a hundred letters": {"title": "F" * 100},
}


@pytest.mark.parametrize("case", REFUSED_ANSWERS)
def test_an_answer_the_rules_refuse_leaves_what_is_kept_and_marks_the_artist_or_the_work(case, capsys):
    """Round 8: each of these replaced the kept value with nothing, and the run exited 0 with nothing counted. Now
    it is treated as a kept work's title always was: what is kept stays, and the artist or the work is marked,
    so that the push stage holds them back and the owner sees it. The mark goes when the answer can be kept."""
    answer = dict(REFUSED_ANSWERS[case])
    world = solo(2)
    assert run(world) == 0
    before = (without_time(rows("profiles")), [{**row, "fetched_at": None} for row in rows("works")])
    world.galleries[ALPHA][0]["title"] = answer.pop("title", "FAKE-TITLE-1")
    world.profiles[ALPHA].update(answer)
    for _ in range(2):
        counts = counts_of(world, capsys)
        assert (without_time(rows("profiles")), [{**row, "fetched_at": None} for row in rows("works")]) == before
        if answer:
            assert artist_marks() == {key(ALPHA): "answer refused"} and work_marks() == {}
            assert (counts[collect.ANSWER_REFUSED], counts[collect.MARKED]) == (1, 0)
        else:
            assert work_marks() == {1: "answer refused"} and artist_marks() == {}
            assert (counts[collect.ANSWER_REFUSED], counts[collect.MARKED]) == (0, 1)
        assert counts[collect.PROFILES] == 1 and counts[collect.WORKS] == 2
    world.galleries[ALPHA][0]["title"] = "FAKE-TITLE-NEW"
    world.profiles[ALPHA] = profile(ALPHA, tagline="FAKE-TAGLINE-NEW")
    counts = counts_of(world, capsys)
    assert artist_marks() == work_marks() == {} and counts[collect.ANSWER_REFUSED] == counts[collect.MARKED] == 0
    assert (rows("profiles")[0]["tagline"], rows("works")[0]["title"]) == ("FAKE-TAGLINE-NEW", "FAKE-TITLE-NEW")


def test_an_answer_the_rules_refuse_for_an_artist_of_whom_nothing_is_kept_is_left_out_and_nobody_is_marked(capsys):
    world = solo(1, tagline="FAKE-TAGLINE see zz-fake-alpha.art", website="N/A", artist_level="5550199")
    world.galleries[ALPHA][0]["title"] = "F" * 100
    counts = counts_of(world, capsys)
    assert without_time(rows("profiles")) == [{**profile_row(ALPHA, None), "tagline": None, "level": None}]
    assert rows("works")[0]["title"] is None and artist_marks() == work_marks() == {}
    assert counts[collect.ANSWER_REFUSED] == counts[collect.MARKED] == 0


def test_max_artists_bounds_a_run_and_the_next_run_takes_up_where_it_stopped(capsys):
    world = base()
    first = counts_of(world, capsys, max_artists=2)
    assert first[collect.PROCESSED] == 2 and library.artist_keys() == [key(ALPHA), key(BRAVO)]
    assert world.asked_about(CHARLIE) == world.asked_about(DELTA) == world.asked_about(FOXTROT) == []
    seen = len(world.log)
    second = counts_of(world, capsys, max_artists=3)  # the three with nothing collected come before ALPHA and BRAVO
    assert second[collect.PROCESSED] == 3
    lookups = [request.url.path for request in world.since(seen, "api") if request.url.path.startswith(PROFILE_PATH)]
    assert [path.removeprefix(PROFILE_PATH) for path in lookups] == [CHARLIE, DELTA, FOXTROT]
    assert library.artist_keys() == [key(ALPHA), key(BRAVO), key(FOXTROT)]
    assert counts_of(world, capsys, max_artists=0)[collect.PROCESSED] == 0
    assert counts_of(world, capsys, max_artists=-1)[collect.PROCESSED] == 0


def test_an_artist_is_written_to_the_database_in_one_transaction(monkeypatch):
    """Commits are what is slow on a loaded disk. A profile and ten works are one, however many images came in."""
    world = solo(10)
    statements = []
    real = sqlite3.connect

    def traced(*args, **kwargs):
        conn = real(*args, **kwargs)
        conn.set_trace_callback(statements.append)
        return conn

    monkeypatch.setattr(sqlite3, "connect", traced)
    assert run(world) == 0
    transactions = [[]]  # the statements of each transaction, in order
    for statement in statements:
        transactions[-1].append(statement.split()[:5])
        if statement.strip() == "COMMIT":
            transactions.append([])
    ours = [block for block in transactions if any(words[-1] in ("profiles", "works") for words in block)]
    assert len(ours) == 1  # the one transaction that writes to the two tables
    assert ours[0].count(["INSERT", "OR", "REPLACE", "INTO", "works"]) == 10
    assert ours[0].count(["INSERT", "OR", "REPLACE", "INTO", "profiles"]) == 1
    assert kept(ALPHA) == list(range(10, 0, -1))


# --- 5. hard rule 7: removal ---


def test_a_removed_artist_is_never_asked_about(capsys):
    world = base()
    counts = counts_of(world, capsys)
    assert world.asked_about(ECHO) == [] and store.is_removed(ECHO)
    assert key(ECHO) not in library.artist_keys() and counts[collect.PROCESSED] == 5


def test_the_removal_list_is_asked_again_before_each_artist(monkeypatch, capsys):
    """A list of artists read at the start of a run can be stale by the time an artist's turn comes."""
    world = base()
    listed = [*store.all_artists(), (key(ECHO), ECHO, f"https://www.deviantart.com/{ECHO}")]
    monkeypatch.setattr(store, "all_artists", lambda db=None: sorted(listed))
    counts = counts_of(world, capsys)
    assert counts[collect.REMOVED] == 1 and counts[collect.PROCESSED] == 5
    assert world.asked_about(ECHO) == [] and key(ECHO) not in library.artist_keys()


def test_an_artist_removed_while_the_run_is_on_is_not_fetched(capsys):
    world = base()

    def removal_arrives(request):
        if request.url.path == PROFILE_PATH + ALPHA:
            store.mark_removed(BRAVO)

    world.detours.append(removal_arrives)
    counts = counts_of(world, capsys)
    assert counts[collect.REMOVED] == 1 and world.asked_about(BRAVO) == [] and rows("works", BRAVO) == []


def removal_during(world, username, path):
    """A detour: the artist goes on the removal list while this request is being answered, as if by another process."""

    def detour(request):
        if request.url.path == path and not store.is_removed(username):
            store.mark_removed(username)

    world.detours.append(detour)


@pytest.mark.parametrize("n", [12, 10, 3])
def test_a_removal_that_lands_between_two_images_stops_the_downloads_at_once(n, capsys, tmp_path):
    """The critic's finding: a removal mid-artist let eight more images in, and their files outlived the run.
    The list is asked before every download, so the one in flight is the last, and nothing of theirs is left."""
    world = base()
    removal_during(world, ALPHA, preview_path(n))
    counts = counts_of(world, capsys)
    fetched = [
        request.url.path for request in world.requests("images") if request.url.path in map(preview_path, range(1, 13))
    ]
    assert fetched == [preview_path(m) for m in ALPHA_KEPT[: ALPHA_KEPT.index(n) + 1]]  # none after the removal landed
    assert counts[collect.REMOVED] == 1 and counts[collect.ORPHANS] == len(fetched)
    assert rows("profiles", ALPHA) == rows("works", ALPHA) == [] and key(ALPHA) not in library.artist_keys()
    assert files(tmp_path) == [*(f"cache/images/{name}" for name in image_files(BRAVO_KEPT)), "cache/pipeline.sqlite"]
    assert b"zz-fake-alpha" not in config.LOCAL_DB.read_bytes()
    assert kept(BRAVO) == BRAVO_KEPT and counts[collect.PROCESSED] == 5  # and everybody else is done as usual


@pytest.mark.parametrize("path", [GALLERY_PATH, METADATA_PATH])
def test_a_removal_that_lands_before_the_first_image_means_no_image_is_fetched(path, capsys):
    world = solo()
    removal_during(world, ALPHA, path)
    counts = counts_of(world, capsys)
    assert world.requests("images") == [] and counts[collect.REMOVED] == 1 and library.artist_keys() == []
    # Their first turn, so no profile of theirs was ever stored: the account is taken from the one in hand.
    assert library.account_removed(f"FAKE-USERID-{ALPHA}")


@pytest.mark.parametrize(
    "website",
    [
        "https://zz-fake-gone.art/",
        "https://www.zz-fake-gone.art/folio?page=2",  # the own-site rule drops each of these shapes,
        "https://shop.zz-fake-gone.art",  # so the removal list is asked before it
        "https://zz-fake-gone.art/#/zz-fake-alpha",
        "http://zz-fake-gone.art:8080/",
    ],
)
@pytest.mark.parametrize("removed", ["before discovery", "between discovery and collection"])
def test_an_artist_of_whom_nothing_is_kept_and_who_alone_names_a_removed_site_is_barred_for_good(
    website, removed, capsys
):
    """The critic's finding, which broke hard rule 7. Discovery stores a handle without reading the profile, so
    a site removed while its artist was a bare handle matched nobody. The artist was marked and no more: the row
    stayed, the API was asked about them on every run, and once their website field emptied they were collected,
    profile, works and images, and handed on. The same removal after they were collected bars them for good.
    Now it does so here too, in a link of any shape: handle and account barred, the row and every note gone.
    Nothing that is kept is deleted for it, because nothing is kept, and the site is written into nobody's row."""
    if removed == "before discovery":
        assert store.mark_removed("zz-fake-gone.art") == 0
    world = solo(website=website)
    if removed != "before discovery":
        assert store.mark_removed("zz-fake-gone.art") == 0  # it matches nobody: the row has a handle and no link
    counts = counts_of(world, capsys)
    assert counts[collect.LISTS_REMOVED] == counts[collect.PURGED] == 1 and counts[collect.PROFILES] == 0
    assert store.is_removed(ALPHA) and library.account_removed(f"FAKE-USERID-{ALPHA}")  # the handle and the account
    assert store.all_artists() == [] and library.artist_keys() == [] and artist_marks() == {}
    assert library.last_attempts() == {} and len(raw("SELECT * FROM removed")) == 3  # the site, and those two
    assert [request.url.path for request in world.requests()] == [TOKEN_PATH, PROFILE_PATH + ALPHA]  # and no more
    kept_bytes = config.LOCAL_DB.read_bytes()
    assert b"zz-fake-gone" not in kept_bytes and b"zz-fake-alpha" not in kept_bytes and b"FAKE-USERID" not in kept_bytes
    world.profiles[ALPHA]["website"] = ""  # whatever the field says afterwards
    seen = len(world.log)
    assert counts_of(world, capsys)[collect.PROCESSED] == 0 and world.since(seen) == []  # never asked about again
    assert store.upsert_artist(ALPHA, f"https://www.deviantart.com/{ALPHA}", None, 9) is False  # nor found again


def test_a_removed_site_that_three_profiles_list_puts_nobody_on_the_removal_list_on_a_profiles_word(capsys):
    """The guard on the rule above: a site that several artists list must not turn one removal into many, and one
    answer given for every profile must not bar anybody. BRAVO and DELTA are collected, each with a page on the
    site as website: not a site of their own, so no removal of it finds them by their row. ALPHA is a bare handle
    whose profile names its front page. Three profiles list the site, one more than store.OWNERS: it is shared,
    and nobody's to answer for. All three are held for as long as their profile names it, keep everything, and
    are collected like anybody once it does not."""
    world = solo(2, website="https://zz-fake-shared.art/")
    for name, n in ((BRAVO, 21), (DELTA, 41)):
        world.artist(name, website=f"https://zz-fake-shared.art/folio/{name.lower()}")
        world.work(name, n)
    world.profiles[ALPHA] = refusal(503)  # ALPHA's turn comes to nothing in the first run: nothing of theirs is kept
    assert run(world) == 1 and library.artist_keys() == [key(BRAVO), key(DELTA)]
    assert store.mark_removed("zz-fake-shared.art") == 0
    world.profiles[ALPHA] = profile(ALPHA, website="https://zz-fake-shared.art/")
    was = stored()
    for _ in range(2):
        counts = counts_of(world, capsys)
        assert counts[collect.LISTS_REMOVED] == 3 and counts[collect.PURGED] == counts[collect.REMOVED] == 0
        assert artist_marks() == dict.fromkeys(map(key, (ALPHA, BRAVO, DELTA)), "lists a removed site")
        assert stored() == was and not any(store.is_removed(name) for name in (ALPHA, BRAVO, DELTA))
        assert len(raw("SELECT * FROM removed")) == 1  # the site
    world.profiles[ALPHA]["website"], world.profiles[BRAVO.lower()]["website"] = "", "https://zz-fake-bravo-new.art"
    world.profiles[DELTA]["website"] = ""
    assert counts_of(world, capsys)[collect.PROFILES] == 3 and artist_marks() == {} and kept(ALPHA) == [2, 1]


def test_a_removed_site_two_profiles_list_removes_both_for_good_and_emptying_the_fields_brings_neither_back(capsys):
    """The critic's counterexample: one person with two accounts, both bare handles, both listing their site.
    The site was removed, and neither was barred, since the site was not either's alone; once they emptied both
    website fields both were collected. Two profiles are as many as can list one site and it still be somebody's
    own (store.OWNERS): a run that read every stored artist removes both, handle, account and all."""
    world, names = many(2)
    for name in names:
        world.profiles[name]["website"] = "https://zz-fake-gone.art/"
    assert store.mark_removed("zz-fake-gone.art") == 0
    counts = counts_of(world, capsys)
    assert counts[collect.LISTS_REMOVED] == counts[collect.PURGED] == 2 and counts[collect.PROFILES] == 0
    assert all(store.is_removed(name) and library.account_removed(f"FAKE-USERID-{name}") for name in names)
    assert library.artist_keys() == store.all_artists() == [] and cached() == []
    for name in names:
        world.profiles[name]["website"] = ""  # the person takes the site off both profiles
    seen = len(world.log)
    assert counts_of(world, capsys)[collect.PROCESSED] == 0 and world.since(seen) == []  # and nobody comes back


@pytest.mark.parametrize("short", ["--max-artists 1", "stopped by the rate limit after the first artist"])
def test_one_removed_website_answered_for_every_profile_in_a_short_run_removes_nobody(short, capsys):
    """The critic's counterexample: one removed site answered for every profile, in a run that read one profile,
    barred that artist for good: theirs was the only listing of the site the run had seen. A run that read no
    more than store.OWNERS profiles, and not every stored artist's, removes nobody now. The artist it held stays
    held, and is read again; the run that reads the others too finds the site listed by three: shared."""
    world, names = many(3)
    for name in names:
        world.profiles[name]["website"] = "https://zz-fake-gone.art/"  # the API's one wrong answer, for everybody
    assert store.mark_removed("zz-fake-gone.art") == 0
    if short == "--max-artists 1":
        assert run(world, max_artists=1) == 0
    else:
        world.detours.append(rate_limited(PROFILE_PATH + names[1], 99))
        assert run(world) == 1
    capsys.readouterr()
    assert not any(store.is_removed(name) for name in names) and len(raw("SELECT * FROM removed")) == 1  # the site
    assert artist_marks() == {key(names[0]): "lists a removed site"} and len(store.all_artists()) == 3
    world.detours.clear()
    counts = counts_of(world, capsys)
    assert counts[collect.LISTS_REMOVED] == 3 and counts[collect.PURGED] == 0
    assert not any(store.is_removed(name) for name in names) and len(store.all_artists()) == 3


def test_a_removal_that_lands_after_the_artist_was_stored_is_honoured_before_the_run_ends(capsys, tmp_path):
    """Purged at the end of a run as well as at the start: the run does not exit 0 with a removed artist on disk."""
    world = base()
    removal_during(world, ALPHA, PROFILE_PATH + FOXTROT)  # the last request of the run, long after ALPHA's turn
    counts = counts_of(world, capsys)
    assert counts[collect.PURGED] == 1 and counts[collect.PROFILES] == 3
    assert rows("profiles", ALPHA) == rows("works", ALPHA) == []
    assert files(tmp_path) == [*(f"cache/images/{name}" for name in image_files(BRAVO_KEPT)), "cache/pipeline.sqlite"]


def test_a_run_that_is_stopped_still_purges_before_it_exits(capsys):
    world = base()
    removal_during(world, ALPHA, PROFILE_PATH + BRAVO)
    world.detours.append(rate_limited(PROFILE_PATH + DELTA, 99))
    assert run(world) == 1
    captured = capsys.readouterr()
    assert captured.err == LIMITED
    assert summary(captured.out)[collect.PURGED] == 1
    assert library.artist_keys() == [key(BRAVO)] and cached() == image_files(BRAVO_KEPT)


@pytest.mark.parametrize(
    "removal", [ALPHA, f"https://www.deviantart.com/{ALPHA}", "zz-fake-alpha.art", "https://www.zz-fake-alpha.art/"]
)
def test_an_artist_removed_after_collection_is_purged_on_the_next_run(removal, capsys, tmp_path):
    world = base()
    assert run(world) == 0
    assert kept(ALPHA) == ALPHA_KEPT
    assert store.mark_removed(removal) == 1
    seen = len(world.log)

    counts = counts_of(world, capsys)
    assert counts[collect.PURGED] == 1 and counts[collect.PROCESSED] == 4
    assert rows("profiles", ALPHA) == rows("works", ALPHA) == [] and key(ALPHA) not in library.artist_keys()
    assert files(tmp_path) == [*(f"cache/images/{name}" for name in image_files(BRAVO_KEPT)), "cache/pipeline.sqlite"]
    stored = config.LOCAL_DB.read_bytes()
    assert b"zz-fake-alpha" not in stored and uuid(12).encode() not in stored  # not even in the file's free pages
    assert kept(BRAVO) == BRAVO_KEPT
    world.log[:] = world.log[seen:]
    assert world.asked_about(ALPHA) == []
    assert counts_of(world, capsys)[collect.PURGED] == 0  # and it stays that way


def test_the_purge_comes_before_any_request_and_survives_a_run_that_fails(capsys, tmp_path):
    world = base()
    assert run(world) == 0
    store.mark_removed(BRAVO)
    purged_at_first_request = []

    def all_fail(request):
        purged_at_first_request.append(rows("works", BRAVO) == [] and cached() == image_files(ALPHA_KEPT))
        return httpx.Response(500, text="FAKE-SERVER-ERROR")

    world.detours.append(all_fail)
    seen = len(world.log)
    assert run(world) == 1
    # The token request, which failed, asked for again three times like any other.
    assert len(world.since(seen)) == 1 + collect.SERVER_RETRIES and purged_at_first_request == [True] * 4
    assert rows("profiles", BRAVO) == rows("works", BRAVO) == []
    assert cached() == image_files(ALPHA_KEPT)


def test_a_collected_artist_whose_profile_comes_to_name_a_page_on_a_removed_site_is_removed_for_good(capsys):
    """The critic's counterexample. A collected artist's website field is a page on their site, not its front
    page, so the store's row has no link and the removal of the site matched nobody. Their next profile named it,
    and they were marked and kept everything; once the field changed, the mark went and they were published
    again. Round 7 made it a mark because one answer given for every profile once purged and barred every
    artist. That is guarded now by the count of profiles that list the site, so the profile's word holds them at
    once, and the end of a run that read three profiles removes them: handle, account and everything kept."""
    world = base()
    assert run(world) == 0
    assert store.mark_removed("zz-fake-gone.art") == 0
    world.profiles[BRAVO.lower()] = profile(BRAVO, website="see WWW.ZZ-Fake-Gone.art/folio for more")
    seen = len(world.log)
    counts = counts_of(world, capsys)
    assert counts[collect.LISTS_REMOVED] == counts[collect.PURGED] == 1 and counts[collect.PROFILES] == 2
    assert rows("profiles", BRAVO) == rows("works", BRAVO) == [] and cached() == image_files(ALPHA_KEPT)
    assert (
        store.is_removed(BRAVO) and library.account_removed(f"FAKE-USERID-{BRAVO}") and key(BRAVO) not in artist_marks()
    )
    assert [request.url.path for request in world.since(seen) if "bravo" in str(request.url).lower()] == [
        PROFILE_PATH + BRAVO
    ]  # and nothing of theirs was asked for beyond the profile
    assert b"zz-fake-gone" not in config.LOCAL_DB.read_bytes().lower()
    world.profiles[BRAVO.lower()] = profile(BRAVO)  # whatever the field says afterwards
    seen = len(world.log)
    assert counts_of(world, capsys)[collect.PROCESSED] == 4  # barred, and out of the store: never asked about again
    assert not [request for request in world.since(seen) if "bravo" in str(request.url).lower()]


def test_one_website_answered_for_every_profile_and_then_one_removal_takes_only_the_removed(capsys):
    """The critic's finding, and the biggest of round 7: one run in which every profile answered the same website,
    then one legitimate removal, purged and barred six artists of six. One wrong website once did it to two, and
    so did honest data: two artists who list one site. A removal reaches the artist it names."""
    world, names = many(6)
    for name in names:
        world.profiles[name]["website"] = "https://zz-fake-shared.art/"
    assert run(world) == 0
    was = stored()
    store.mark_removed(names[0])
    assert counts_of(world, capsys)[collect.PURGED] == 1
    others = set(map(key, names[1:]))
    assert stored() == (others, was[1] - {uuid(1)}, was[2] - {f"{uuid(1)}.jpg"}, others)
    assert not any(store.is_removed(name) for name in names[1:]) and artist_marks() == {}
    assert not store.is_removed("zz-fake-shared.art") and len(raw("SELECT * FROM removed")) == 2  # handle and account
    assert counts_of(world, capsys)[collect.PROFILES] == 5 and stored()[0] == others  # and the run after it


def test_an_own_site_kept_with_the_profile_is_in_the_stores_row_too_and_its_removal_reaches_the_artist(capsys):
    """Round 8, with honest data: discovery saw no site, the artist added one, and a rerun kept it with the profile
    only. Removing the site then returned 0. She was marked and not purged, and once she changed the field she
    was stored again with no mark and not barred. One place holds an artist's own site now: the store's row."""
    world = solo(2)  # stored by discovery with no link
    world.artist(BRAVO)
    world.work(BRAVO, 21)
    assert run(world) == 0 and store.own_site_candidates() == []
    world.profiles[ALPHA]["website"] = "https://www.zz-fake-alpha.art"  # she adds a site of her own
    assert run(world) == 0 and rows("profiles", ALPHA)[0]["website"] == "https://www.zz-fake-alpha.art/"
    assert store.own_site_candidates() == [(key(ALPHA), "https://www.zz-fake-alpha.art/")]  # the row has it too
    assert store.mark_removed("site:zz-fake-alpha.art") == 1  # and so the removal finds her
    world.profiles[ALPHA]["website"] = ""  # whatever she does with the field afterwards
    seen = len(world.log)
    counts = counts_of(world, capsys)
    assert counts[collect.PURGED] == 1 and library.artist_keys() == [key(BRAVO)] and cached() == image_files([21])
    assert store.is_removed(ALPHA) and library.account_removed(f"FAKE-USERID-{ALPHA}")
    assert not [request for request in world.since(seen) if "alpha" in str(request.url).lower()]
    assert b"zz-fake-alpha" not in config.LOCAL_DB.read_bytes()


@pytest.mark.parametrize("website", ["https://zz-fake-alpha.art/", "", "N/A", "https://zz-fake-alpha-new.art"])
def test_the_website_kept_for_an_artist_holds_them_for_a_removed_site_whatever_their_profile_says_now(website, capsys):
    """Round 8. A profile an earlier version kept, whose own site the store's row does not have, and the site
    removed before her next turn: no removal found her. The website that is kept is what her profile has said,
    and emptying the field, or changing it to another site, takes none of it back: she is held as if the field
    still named the site, and removed for good when the run ends, since nobody else lists it."""
    world = solo(2, website="https://zz-fake-alpha.art/")
    assert run(world) == 0
    raw("UPDATE artists SET links = '[]'")  # as the earlier version left it: the site is with the profile only
    assert store.mark_removed("site:zz-fake-alpha.art") == 0
    world.profiles[ALPHA]["website"] = website  # it names the site, says nothing, is refused, or names another
    counts = counts_of(world, capsys)
    assert counts[collect.LISTS_REMOVED] == counts[collect.PURGED] == 1 and counts[collect.PROFILES] == 0
    assert store.is_removed(ALPHA) and library.account_removed(f"FAKE-USERID-{ALPHA}")
    assert library.artist_keys() == store.all_artists() == [] and cached() == []


def test_every_profile_naming_a_removed_site_in_one_run_purges_nobody_and_marks_them(capsys):
    world, names = many(6)
    assert run(world) == 0
    was, before = stored(), (rows("profiles"), rows("works"))
    assert store.mark_removed("zz-fake-gone.art") == 0
    for name in names:
        world.profiles[name]["website"] = "https://www.zz-fake-gone.art/"
    seen = len(world.log)
    counts = counts_of(world, capsys)
    assert counts[collect.LISTS_REMOVED] == 6 and counts[collect.PURGED] == counts[collect.REMOVED] == 0
    assert stored() == was and (rows("profiles"), rows("works")) == before and len(cached()) == 6
    assert artist_marks() == dict.fromkeys(map(key, names), "lists a removed site")
    assert not any(store.is_removed(name) for name in names) and len(raw("SELECT * FROM removed")) == 1  # the site
    assert len(world.since(seen)) == 1 + 6  # a token and six profiles: nothing more is asked of a marked artist
    for name in names:
        world.profiles[name]["website"] = ""
    assert counts_of(world, capsys)[collect.PROFILES] == 6 and artist_marks() == {} and nothing_lost(was)


def test_a_removal_noticed_in_the_middle_of_a_run_is_purged_at_once(capsys):
    """An artist collected before, every image already cached, removed while their turn is on. What was kept
    about them goes then and there, not when the run ends, which can be hours later."""
    world = base()
    assert run(world) == 0
    world.detours.append(
        lambda request: (
            store.mark_removed(ALPHA) and None
            if request.url.path == GALLERY_PATH and request.url.params["username"] == ALPHA
            else None
        )
    )
    left_when_the_next_artist_is_asked_for = []

    def look(request):
        if request.url.path == PROFILE_PATH + BRAVO:  # the artist after ALPHA
            left_when_the_next_artist_is_asked_for.extend(rows("profiles", ALPHA) + rows("works", ALPHA) + cached())

    world.detours.append(look)
    seen = len(world.log)
    counts = counts_of(world, capsys)
    assert counts[collect.REMOVED] == 1 and counts[collect.PURGED] == 0
    assert left_when_the_next_artist_is_asked_for == image_files(BRAVO_KEPT)  # nothing of ALPHA's, rows or files
    assert library.account_removed(f"FAKE-USERID-{ALPHA}")  # the account, and not only the handle
    assert world.since(seen, "images") == []


def test_removed_hashes_imported_from_the_list_of_record_purge_too(capsys):
    world = base()
    assert run(world) == 0
    store.import_removed([hashlib.sha256(key(ALPHA).encode()).hexdigest()])
    assert counts_of(world, capsys)[collect.PURGED] == 1
    assert library.artist_keys() == [key(BRAVO), key(FOXTROT)] and cached() == image_files(BRAVO_KEPT)


def test_a_removed_artists_rows_in_the_other_stages_tables_go_in_the_same_run(capsys):
    """The critic's finding: ten of ten rows for a removed artist's works stayed in the tagging stage's and the
    style stage's tables until those stages ran again. Here with tables this stage has never heard of."""
    world = base()
    assert run(world) == 0
    raw("CREATE TABLE fake_work_tags (deviation_id TEXT, model TEXT, result TEXT)")
    raw("CREATE TABLE fake_summaries (artist_key TEXT PRIMARY KEY, summary TEXT)")
    for row in rows("works"):
        raw("INSERT INTO fake_work_tags VALUES (?, 'fake-model', 'FAKE-RESULT')", row["deviation_id"])
    for name in (ALPHA, BRAVO):
        raw("INSERT INTO fake_summaries VALUES (?, 'FAKE-SUMMARY')", key(name))
    store.mark_removed(ALPHA)
    assert counts_of(world, capsys)[collect.PURGED] == 1
    left = sorted(work_id for (work_id,) in raw("SELECT deviation_id FROM fake_work_tags"))
    assert left == sorted(uuid(n) for n in BRAVO_KEPT) and raw("SELECT artist_key FROM fake_summaries") == [
        (key(BRAVO),)
    ]


def test_rows_another_stage_writes_about_a_removed_artist_after_the_purge_go_on_the_next_run(capsys):
    """The critic's finding: a row written late, by a stage that was still working on a removed artist, stayed
    forever. Every run sweeps them up at both ends. A row about an artist who is only not collected yet stays."""
    world = base()
    assert run(world) == 0
    store.mark_removed(ALPHA)
    assert counts_of(world, capsys)[collect.PURGED] == 1
    raw("CREATE TABLE fake_late_tags (deviation_id TEXT, result TEXT)")
    raw("CREATE TABLE fake_late_notes (artist_key TEXT, note TEXT)")
    for work_id in (uuid(12), uuid(21)):  # a purged work of ALPHA's, and a work BRAVO still has
        raw("INSERT INTO fake_late_tags VALUES (?, 'FAKE-RESULT')", work_id)
    for name in (ALPHA, BRAVO, CHARLIE):  # removed, collected, and listed with nothing collected
        raw("INSERT INTO fake_late_notes VALUES (?, 'FAKE-NOTE')", key(name))
    counts_of(world, capsys)
    assert raw("SELECT deviation_id FROM fake_late_tags") == [(uuid(21),)]
    assert raw("SELECT artist_key FROM fake_late_notes ORDER BY 1") == [(key(BRAVO),), (key(CHARLIE),)]
    assert b"zz-fake-alpha" not in config.LOCAL_DB.read_bytes()


def test_a_removal_deletes_every_contact_sheet_other_stages_made_from_cached_images(capsys, tmp_path):
    """The critic's finding: cache/review holds sheets of cached images, and a removed artist's work stayed on them."""
    world = base()
    assert run(world) == 0
    review = config.CACHE_DIR / "review" / "FAKE-RUN"
    review.mkdir(parents=True)
    (review / "FAKE-SHEET.jpg").write_bytes(jpeg(1))
    assert run(world) == 0 and (review / "FAKE-SHEET.jpg").exists()  # nobody was removed: the sheets are left alone
    store.mark_removed(ALPHA)
    assert counts_of(world, capsys)[collect.PURGED] == 1
    assert files(tmp_path) == [*(f"cache/images/{name}" for name in image_files(BRAVO_KEPT)), "cache/pipeline.sqlite"]


def test_cached_files_with_no_row_are_deleted_at_the_start_of_every_run(capsys, tmp_path):
    """The critic's finding: the database was lost, the images stayed, and ten files of a removed artist had no
    row for any purge to go by. Now a file no row names is deleted before the run asks for anything."""
    world = base()
    assert run(world) == 0
    config.LOCAL_DB.unlink()  # the database is lost, the removal list with it; the 13 images stay
    for username in (BRAVO, CHARLIE, DELTA, FOXTROT):  # discovery runs again, and ALPHA has asked to be removed
        store.upsert_artist(username, f"https://www.deviantart.com/{username}", [], 3)
    store.mark_removed(ALPHA)
    (library.image_dir() / f"{uuid(77)}.jpg.part").write_bytes(jpeg(77))  # and half a download from a run that died
    orphans_at_first_request = []
    world.detours.append(lambda request: orphans_at_first_request.append(cached()))
    seen = len(world.log)

    counts = counts_of(world, capsys)
    assert orphans_at_first_request[0] == []  # every one of them was gone before the token was asked for
    assert counts[collect.ORPHANS] == 14 and counts[collect.DOWNLOADED] == 3 and counts[collect.PURGED] == 0
    assert files(tmp_path) == [*(f"cache/images/{name}" for name in image_files(BRAVO_KEPT)), "cache/pipeline.sqlite"]
    assert kept(BRAVO) == BRAVO_KEPT
    world.log[:] = world.log[seen:]
    assert world.asked_about(ALPHA) == []  # and nothing of the removed artist's is asked for again


def test_an_account_under_a_new_handle_is_linked_once_the_old_handle_is_gone_and_a_removal_purges_both(capsys):
    """Collected as one handle, found again as another. While the old handle still answers, the same user id under
    a new one is the API failing, and nobody is linked: round 6, after one wrong id tied two artists together.
    Once the API calls the old handle gone, the two keys are linked by a mark, and nothing is deleted for it.
    Hard rule 7 is not weakened: when either handle is removed, everything under both goes."""
    world = solo(4)
    world.artist(BRAVO)
    world.work(BRAVO, 21)
    assert run(world) == 0
    was = stored()
    world.artist(RENAMED, user={**author(RENAMED), "userid": f"FAKE-USERID-{ALPHA}"})
    for n in (3, 4, 5):  # two works it had before, and a new one
        world.work(RENAMED, n)
    assert run(world) == 1
    assert (
        capsys.readouterr().err == "skipped an artist: profile lookup answered with a user id that another artist has\n"
    )
    assert artist_marks() == {} and stored()[:3] == was[:3] and kept(ALPHA) == [4, 3, 2, 1]

    world.profiles[ALPHA] = refusal(404, error_code=2)  # of the old handle the API now says: no such user
    counts = counts_of(world, capsys)
    assert counts[collect.UNAVAILABLE] == 1 and counts[collect.PROFILES] == 2 and nothing_lost(was)
    assert kept(ALPHA) == [2, 1] and kept(RENAMED) == [5, 4, 3]  # works 3 and 4 went with the account
    assert [request.url.path for request in world.requests("images")].count(preview_path(4)) == 1  # not fetched twice
    assert artist_marks() == {key(ALPHA): "renamed"} and library.artist_marks()[key(ALPHA)][2] == key(RENAMED)
    assert library.same_account(key(RENAMED)) == [key(ALPHA)]
    for _ in range(2):  # and however often the API says of the old handle that there is no such user
        counts_of(world, capsys)
    assert artist_marks() == {key(ALPHA): "renamed"} and kept(ALPHA) == [2, 1] and kept(RENAMED) == [5, 4, 3]

    store.mark_removed(RENAMED)
    assert counts_of(world, capsys)[collect.PURGED] == 1
    assert library.artist_keys() == [key(BRAVO)] and cached() == image_files([21])
    assert [artist[0] for artist in store.all_artists()] == [key(BRAVO)] and store.is_removed(ALPHA)
    assert rows("skipped") == rows("artist_marks") == [] and library.account_removed(f"FAKE-USERID-{ALPHA}")
    seen = len(world.log)
    assert counts_of(world, capsys)[collect.PROCESSED] == 1
    assert not [
        request for request in world.since(seen) if "alpha" in str(request.url) or "renamed" in str(request.url)
    ]
    stored_bytes = config.LOCAL_DB.read_bytes()
    assert b"zz-fake-alpha" not in stored_bytes and b"zz-fake-renamed" not in stored_bytes


def renamed_world(capsys):
    """ALPHA collected with four works, called gone by the API, and found again as RENAMED with a fifth: linked."""
    world = solo(4)
    assert run(world) == 0
    world.profiles[ALPHA] = refusal(404, error_code=2)
    counts_of(world, capsys)
    world.artist(RENAMED, user={**author(RENAMED), "userid": f"FAKE-USERID-{ALPHA}"})
    world.work(RENAMED, 5)
    assert run(world) == 0 and library.artist_keys() == [key(ALPHA), key(RENAMED)]
    assert library.same_account(key(RENAMED)) == [key(ALPHA)] and kept(ALPHA) == [4, 3, 2, 1]
    return world


def test_an_old_handle_that_answers_again_under_another_user_id_does_not_undo_the_link(capsys):
    """The critic's finding: a new account took the freed name, the old key was stored under the new id, and the link
    was wiped. Removing the new handle then left the removed person's four works and files under the old key. The
    hash kept for a key is never replaced, so the newcomer's answer is the API failing, and the link holds."""
    world = renamed_world(capsys)
    world.profiles[ALPHA] = profile(ALPHA, user={**author(ALPHA), "userid": "FAKE-USERID-SOMEBODY-NEW"})
    capsys.readouterr()
    for _ in range(2):
        assert run(world) == 1
        assert (
            capsys.readouterr().err == "skipped an artist: profile lookup answered with a user id that is not theirs\n"
        )
    assert artist_marks() == {key(ALPHA): "renamed"} and library.account_hash(key(ALPHA)) == user_hash(ALPHA)
    assert kept(ALPHA) == [4, 3, 2, 1]
    store.mark_removed(RENAMED)
    counts_of(world, capsys)
    assert library.artist_keys() == [] and cached() == [] and store.is_removed(ALPHA) and store.all_artists() == []
    stored_bytes = config.LOCAL_DB.read_bytes()
    assert b"zz-fake-alpha" not in stored_bytes and b"zz-fake-renamed" not in stored_bytes


def test_a_removal_of_the_old_handle_purges_what_is_kept_under_the_new_one_too(capsys):
    world = renamed_world(capsys)
    store.mark_removed(ALPHA)
    counts = counts_of(world, capsys)
    assert counts[collect.PURGED] == 2 and library.artist_keys() == [] and cached() == []  # each of its handles
    assert store.is_removed(RENAMED) and store.all_artists() == [] and rows("artist_marks") == []


def test_one_user_id_answered_for_every_profile_links_nobody_and_a_removal_then_takes_only_the_removed(capsys):
    """The critic's finding, and the biggest of round 6: one run in which every profile answered with the right
    username and one shared user id marked five of six artists "renamed" and exited 0. One legitimate removal then
    purged all six and put all six on the removal list."""
    world, names = many(6)
    assert run(world) == 0
    was = stored()
    for name in names:
        world.profiles[name]["user"]["userid"] = f"FAKE-USERID-{names[0]}"
    assert run(world) == 1
    captured = capsys.readouterr()
    assert captured.err == (
        "skipped an artist: profile lookup answered with a user id that is not theirs\n" * 5
        + "stopped: 5 artists in a row failed: the API seems to be down\n"
    )
    assert stored() == was and artist_marks() == {}
    for name in names:
        world.profiles[name]["user"]["userid"] = f"FAKE-USERID-{name}"
    store.mark_removed(names[0])
    assert counts_of(world, capsys)[collect.PURGED] == 1
    others = set(map(key, names[1:]))
    assert stored() == (others, was[1] - {uuid(1)}, was[2] - {f"{uuid(1)}.jpg"}, others)
    assert not any(store.is_removed(name) for name in names[1:]) and artist_marks() == {}


def test_an_artist_kept_under_the_old_spelling_of_the_account_hash_is_still_themselves(capsys):
    world = solo(2)
    assert run(world) == 0
    raw("UPDATE profiles SET user_hash = ?", hashlib.sha256(f"da-user:FAKE-USERID-{ALPHA}".encode()).hexdigest())
    assert counts_of(world, capsys)[collect.PROFILES] == 1 and kept(ALPHA) == [2, 1]


def test_one_wrong_user_id_once_ties_nobody_to_anybody(capsys):
    """The critic's finding: b's lookup answered once with a's id, a was removed, and b was purged and barred too.
    Without any removal the same answer moved works from one artist's key to another's."""
    world, names = many(3)
    world.artist("zz-fake-new")
    world.work("zz-fake-new", 9)
    world.profiles["zz-fake-new"]["user"]["userid"] = f"FAKE-USERID-{names[0]}"  # a new artist, with a's id
    assert run(world, max_artists=3) == 0
    was = stored()
    world.profiles[names[1]]["user"]["userid"] = f"FAKE-USERID-{names[0]}"  # and b, who is kept, with a's id
    assert run(world) == 1
    assert capsys.readouterr().err == (
        "skipped an artist: profile lookup answered with a user id that another artist has\n"
        "skipped an artist: profile lookup answered with a user id that is not theirs\n"
    )
    assert stored() == was and artist_marks() == {} and kept(names[1]) == [2] and kept("zz-fake-new") == []
    world.profiles[names[1]]["user"]["userid"] = f"FAKE-USERID-{names[1]}"
    world.profiles["zz-fake-new"]["user"]["userid"] = "FAKE-USERID-zz-fake-new"
    store.mark_removed(names[0])
    assert counts_of(world, capsys)[collect.PURGED] == 1
    assert library.artist_keys() == [key(names[1]), key(names[2]), key("zz-fake-new")] and kept(names[1]) == [2]
    assert not store.is_removed(names[1]) and not store.is_removed("zz-fake-new")


IDENTITIES = {  # what the API can get wrong, for one run, about whose account each handle is
    "honest": lambda names: {},
    "one id for everybody": lambda names: dict.fromkeys(names, f"FAKE-USERID-{names[0]}"),
    "each the id of the next": lambda names: {
        name: f"FAKE-USERID-{names[(at + 1) % len(names)]}" for at, name in enumerate(names)
    },
    "ids nobody has": lambda names: {name: f"FAKE-USERID-OTHER-{name}" for name in names},
}


@pytest.mark.parametrize("removed", [0, 2, 4])
@pytest.mark.parametrize("second", IDENTITIES)
@pytest.mark.parametrize("first", IDENTITIES)
def test_whatever_user_ids_the_api_answers_with_an_artist_is_gone_only_if_they_were_removed(first, second, removed):
    """The property of round 6: wrong, shared and changed user ids on any run, then a removal. Nobody is gone but
    the one who was removed, and nobody else is put on the removal list."""
    world, names = many(4)
    assert run(world) == 0
    names = [*names, "zz-fake-new"]  # four who are kept, and one whom discovery has just added
    world.artist("zz-fake-new")
    world.work("zz-fake-new", 9)
    was = stored()
    for kind in (first, second):
        wrong = IDENTITIES[kind](names)
        for name in names:
            world.profiles[name]["user"]["userid"] = wrong.get(name, f"FAKE-USERID-{name}")
        run(world)
        assert nothing_lost(was) and artist_marks() == {}, kind
    for name in names:
        world.profiles[name]["user"]["userid"] = f"FAKE-USERID-{name}"
    store.mark_removed(names[removed])
    run(world)
    run(world)
    gone, others = key(names[removed]), {key(name) for at, name in enumerate(names) if at != removed}
    now = stored()
    assert gone not in now[0] | now[3] and now[3] == others and now[0] >= was[0] - {gone}
    assert now[1] >= was[1] - {uuid(removed + 1)} and not any(
        store.is_removed(name) for name in names if key(name) != gone
    )


def test_an_account_removed_under_one_handle_is_not_collected_under_the_next(capsys, tmp_path):
    """The critic's finding: collected, removed and purged; then the same account under a new handle, which
    discovery had stored, was collected again: a profile, three works and three files for a removed artist.
    The purge now leaves the hash of the account's user id on the store's removal list, and a profile whose
    user id is on that list has its handle barred through the store and nothing collected."""
    world = solo(4)
    assert run(world) == 0
    store.mark_removed(ALPHA)
    assert counts_of(world, capsys)[collect.PURGED] == 1 and library.artist_keys() == []
    world.artist(RENAMED, user={**author(RENAMED), "userid": f"FAKE-USERID-{ALPHA}"})  # the same account, renamed
    for n in (3, 4, 5):
        world.work(RENAMED, n)
    seen = len(world.log)

    counts = counts_of(world, capsys)
    assert counts[collect.RENAMED] == 1 and counts[collect.PROFILES] == counts[collect.WORKS] == 0
    assert library.artist_keys() == [] and files(tmp_path) == ["cache/pipeline.sqlite"]
    assert [request.url.path for request in world.since(seen)] == [TOKEN_PATH, PROFILE_PATH + RENAMED]  # and no more
    assert store.is_removed(RENAMED) and store.all_artists() == []  # barred through the store
    stored = config.LOCAL_DB.read_bytes()
    assert b"FAKE-USERID" not in stored and b"zz-fake-renamed" not in stored and b"zz-fake-alpha" not in stored
    seen = len(world.log)
    assert counts_of(world, capsys)[collect.PROCESSED] == 0 and world.since(seen) == []  # and never asked about again


def test_an_account_that_goes_on_the_list_while_its_images_are_fetched_is_not_stored_under_any_handle(capsys, tmp_path):
    """The third critic of the push stage: an account removed under another handle, as the push stage brings it
    from the list of record, while this turn fetches images. The list is asked once more before anything is
    written, of the account as well as of the handle: nothing is stored, and the handle is barred too."""
    world = solo(4)

    def removal_arrives(request):
        if request.url.path == preview_path(4) and not library.account_removed(f"FAKE-USERID-{ALPHA}"):
            store.import_removed([library.account(f"FAKE-USERID-{ALPHA}")])

    world.detours.append(removal_arrives)
    counts = counts_of(world, capsys)
    assert counts[collect.REMOVED] == 1 and counts[collect.PROFILES] == counts[collect.WORKS] == 0
    assert library.artist_keys() == [] and files(tmp_path) == ["cache/pipeline.sqlite"]
    assert store.is_removed(ALPHA) and store.all_artists() == []


def trio():
    """ALPHA with four good works, and BRAVO and DELTA with one each: two the API goes on answering for."""
    world = solo(4)
    for name, n in ((BRAVO, 21), (DELTA, 41)):
        world.artist(name)
        world.work(name, n)
    return world


@pytest.mark.parametrize("code", [0, 1, 2])
def test_an_account_the_api_calls_gone_is_marked_and_nothing_kept_under_it_is_ever_deleted_for_that(code, capsys):
    """The profile endpoint's three documented codes: suspended, inactive, not found. Round 5: the API's word is
    written beside the artist, on every run it is given, and that is all. Twice a rule that purged on it also
    purged on an API that was merely failing. Whether a gone account is published is for a later stage."""
    world = trio()
    assert run(world) == 0
    was, before = stored(), (rows("profiles"), rows("works"))
    world.profiles[ALPHA] = refusal(404, error_code=code)
    for _ in range(4):
        counts = counts_of(world, capsys)
        assert counts[collect.UNAVAILABLE] == 1 and counts[collect.PROCESSED] == 3 and counts[collect.PURGED] == 0
        assert stored() == was and artist_marks() == {key(ALPHA): "account gone"}
        assert [row for row in rows("profiles") if row["artist_key"] == key(ALPHA)] == before[0][:1]
        assert [row for row in rows("works") if row["artist_key"] == key(ALPHA)] == before[1][:4]
    assert key(ALPHA) in library.last_attempts() and world.asked_about(ALPHA)  # still in the queue, at the back


def test_a_profile_that_answers_200_is_a_profile_whatever_else_it_says(capsys):
    world = solo(2, error_code=2)
    assert counts_of(world, capsys)[collect.PROFILES] == 1 and kept(ALPHA) == [2, 1]


def test_an_artist_whose_gallery_the_api_refuses_is_passed_over_and_keeps_what_they_had(capsys):
    """Whatever the refusal says: one of the profile endpoint's codes means nothing on another endpoint."""
    world = trio()
    assert run(world) == 0
    was = stored()
    world.detours.append(
        lambda request: (
            refusal(404, error_code=2)
            if request.url.path == GALLERY_PATH and request.url.params["username"] == ALPHA
            else None
        )
    )
    for _ in range(3):
        assert run(world) == 1
        captured = capsys.readouterr()
        assert captured.err == "skipped an artist: gallery lookup answered HTTP 404\n"
        counts = summary(captured.out)
        assert counts[collect.API_FAILED] == 1 and counts[collect.UNAVAILABLE] == 0 and counts[collect.PROFILES] == 2
        assert stored() == was and artist_marks() == {} and key(ALPHA) in library.last_attempts()


def test_a_removed_artist_who_was_only_ever_passed_over_is_forgotten_too(capsys):
    """Hard rule 7: the note that an artist was passed over names them. It goes when they are removed."""
    world = base()
    assert run(world) == 0
    assert key(CHARLIE) in library.last_attempts() and key(CHARLIE) not in library.artist_keys()
    store.mark_removed(CHARLIE)
    counts_of(world, capsys)
    assert key(CHARLIE) not in library.last_attempts() and b"zz-fake-charlie" not in config.LOCAL_DB.read_bytes()


def test_an_account_the_api_called_gone_and_then_answers_for_again_loses_the_mark(capsys):
    world = trio()
    assert run(world) == 0
    there, world.profiles[ALPHA] = world.profiles[ALPHA], refusal(404, error_code=2)
    assert counts_of(world, capsys)[collect.UNAVAILABLE] == 1 and artist_marks() == {key(ALPHA): "account gone"}
    world.profiles[ALPHA] = there
    assert counts_of(world, capsys)[collect.PROFILES] == 3 and artist_marks() == {} and rows("skipped") == []


def test_a_row_with_a_key_nobody_can_read_is_counted_and_keeps_nobody_from_being_purged(capsys):
    """The critic's finding: such a row made the purge at the start of a run raise, before it had purged anyone."""
    world = base()
    assert run(world) == 0
    raw("UPDATE works SET artist_key = '!! FAKE: not a key !!' WHERE deviation_id = ?", uuid(21))  # sorts first
    raw("INSERT INTO skipped VALUES ('!! FAKE: nor is this !!', '2026-10-12T00:00:00+00:00', 0)")
    store.mark_removed(ALPHA)
    counts = counts_of(world, capsys, expect_status=1)
    # Two such rows at the start of the run and one at its end: the note about a handle nobody asks about is swept
    # up. The same rows at both ends are counted once. The work is not taken from the key it is kept under.
    assert counts[collect.BAD_KEY] == 2 and counts[collect.PURGED] == 1 and counts[collect.PROCESSED] == 4
    assert counts[collect.ROW_REFUSED] == 1 and kept(BRAVO) == [23, 22]
    assert rows("profiles", ALPHA) == rows("works", ALPHA) == [] and cached() == image_files(BRAVO_KEPT)


@pytest.mark.parametrize(
    "more", [{}, {"error_code": 3}, {"error_code": "2"}, {"error_code": True}, {"error_code": None}]
)
def test_any_other_refusal_is_the_api_failing_and_says_nothing_about_the_account(more, capsys):
    world = trio()
    assert run(world) == 0
    was = stored()
    world.profiles[ALPHA] = refusal(**more)
    for _ in range(3):
        assert run(world) == 1
        captured = capsys.readouterr()
        assert captured.err == "skipped an artist: profile lookup answered HTTP 400\n"
        counts = summary(captured.out)
        assert counts[collect.API_FAILED] == 1 and counts[collect.UNAVAILABLE] == 0
        assert stored() == was and artist_marks() == {}


def test_one_file_that_cannot_be_deleted_does_not_keep_another_removed_artist_on_disk(monkeypatch, capsys, tmp_path):
    """The critic's finding: one undeletable file ended the purge loop, and the second removed artist kept
    everything. Now the loop carries on, the failure is counted, and the run exits non-zero until it is gone."""
    world = base()
    assert run(world) == 0
    store.mark_removed(ALPHA)
    store.mark_removed(BRAVO)
    capsys.readouterr()
    with monkeypatch.context() as patch:
        patch.setattr("pathlib.Path.unlink", locked(f"{uuid(7)}.jpg"))
        assert run(world) == 1
    captured = capsys.readouterr()
    counts = summary(captured.out)
    assert captured.err == "incomplete: something that had to be deleted could not be. Run again.\n"
    assert counts[collect.UNDELETED] == 2 and counts[collect.PURGED] == 1  # tried at the start and at the end
    assert rows("profiles", BRAVO) == rows("works", BRAVO) == [] and rows("profiles", ALPHA) == []
    assert [row["deviation_id"] for row in rows("works")] == [uuid(7)]  # the row stays for as long as its file does
    assert files(tmp_path) == [f"cache/images/{uuid(7)}.jpg", "cache/pipeline.sqlite"]
    assert counts[collect.PROCESSED] == 3  # and the artists who are not removed were collected all the same

    counts = counts_of(world, capsys)  # the file can be deleted now
    assert counts[collect.PURGED] == 1 and counts[collect.UNDELETED] == 0
    assert library.artist_keys() == [key(FOXTROT)] and files(tmp_path) == ["cache/pipeline.sqlite"]


def test_an_orphan_that_cannot_be_deleted_is_counted_and_fails_the_run(monkeypatch, capsys):
    world = solo(1)
    library.image_dir().mkdir(parents=True)
    (library.image_dir() / f"{uuid(77)}.jpg").write_bytes(jpeg(77))
    with monkeypatch.context() as patch:
        patch.setattr("pathlib.Path.unlink", locked(f"{uuid(77)}.jpg"))
        assert run(world) == 1
    assert summary(capsys.readouterr().out)[collect.UNDELETED] == 2 and kept(ALPHA) == [1]
    assert counts_of(world, capsys)[collect.ORPHANS] == 1 and cached() == image_files([1])


def test_an_error_outside_any_artists_turn_stops_the_run_and_is_named_by_type_and_line_only(monkeypatch, capsys):
    def broken(db=None):
        raise RuntimeError("FAKE-MESSAGE zz-fake-alpha")

    monkeypatch.setattr(library, "drop_orphans", broken)  # the sweep before the first artist
    world = base()
    assert run(world) == 1
    captured = capsys.readouterr()
    assert re.fullmatch(r"stopped: RuntimeError at collect\.py line \d+\n", captured.err), captured.err
    assert summary(captured.out)[collect.PROCESSED] == 0 and world.log == []  # the counts are printed all the same


# --- 6. politeness ---


def test_api_requests_are_two_seconds_apart_and_image_downloads_at_most_two_a_second():
    world = base()
    assert run(world) == 0
    assert len(world.gaps("api")) == 10 and min(world.gaps("api")) >= 2.0
    assert len(world.gaps("images")) == 12 and min(world.gaps("images")) >= 0.5
    times = [at for at, _ in world.log]
    # One at a time, each after a wait of its own: all but the first request to the image host, which had none.
    assert times == sorted(times) and len(set(times)) == len(times) - 1
    assert world.waited("api") == [None] + [2.0] * 10 and world.waited("images") == [None] + [0.5] * 12
    assert world.threads == {threading.get_ident()}  # sequential: no worker threads


def test_the_production_path_waits_before_every_request_but_the_first(monkeypatch):
    """Nothing injected: only time.sleep is replaced. One wait a request, 2 seconds for the API and half a second
    for an image, and none before the first request to either host."""
    waits = []
    monkeypatch.setattr(time, "sleep", waits.append)
    world = base()
    assert collect.collect(CLIENT_ID, CLIENT_SECRET, max_artists=200, transport=world.transport) == 0
    assert len(world.log) == BASE_REQUESTS and len(waits) == BASE_REQUESTS - 2
    first_image = world.requests("images")[0]
    expected = [
        2.0 if request.url.host == API_HOST else 0.5 for request in world.requests()[1:] if request is not first_image
    ]
    assert waits == expected and waits.count(0.5) == 12
    assert collect.IMAGE_INTERVAL == 0.5 and discover.BASE_INTERVAL == 2.0
    assert "sleep" not in inspect.signature(collect.collect).parameters


def test_every_request_carries_the_project_user_agent_and_the_token_goes_to_the_api_alone():
    world = base()
    world.detours.append(rate_limited(GALLERY_PATH, 1))  # a request that is asked for again is a request too
    world.detours.append(rate_limited(preview_path(12), 1))
    assert run(world) == 0 and len(world.log) == BASE_REQUESTS + 2
    assert {request.headers["user-agent"] for request in world.requests()} == {config.USER_AGENT}
    # The API docs ask for both of every client: a user agent, and compression accepted.
    assert all("gzip" in request.headers["accept-encoding"] for request in world.requests())
    assert config.CONTACT_URL in config.USER_AGENT
    for request in world.requests("images"):
        assert "authorization" not in request.headers and "cookie" not in request.headers
        assert "FAKE-TOKEN" not in str(request.url) + str(request.headers)
    assert all(request.headers["authorization"] == "Bearer FAKE-TOKEN-1" for request in world.requests("api")[1:])


def rate_limited(path, times):
    """A detour that answers 429 to the first `times` requests for a path."""
    left = [times]

    def detour(request):
        if request.url.path == path and left[0] > 0:
            left[0] -= 1
            return httpx.Response(429, json={"error": "FAKE-RATE-LIMIT"})

    return detour


@pytest.mark.parametrize("path", [TOKEN_PATH, PROFILE_PATH + ALPHA, GALLERY_PATH, METADATA_PATH])
def test_a_429_from_the_api_slows_every_request_that_follows_and_not_only_the_one_asked_again(path, capsys):
    """The flaw this replaces: a 429 was waited out for that one request, and the next request went back to one a
    second. The pace is the run's now (discover.Pace): doubled by each 429, and down a tenth with each answer."""
    world = base()
    world.detours.append(rate_limited(path, 3))
    assert run(world) == 0
    out = capsys.readouterr().out
    assert out.startswith(EXPECTED_COUNTS) and len(world.log) == BASE_REQUESTS + 3  # collected as ever
    counts = summary(out)
    at = next(index for index, request in enumerate(world.requests("api")) if request.url.path == path)
    waited = world.waited("api")[1:]  # before every request to the API but the first
    assert waited[:at] == [2.0] * at and waited[at : at + 3] == [4.0, 8.0, 16.0]  # before the three askings again
    eased = [16.0 * 0.9**n for n in range(1, 11 - at)]  # 14.4, 12.96 and so on: every request that is left
    assert waited[at + 3 :] == pytest.approx(eased) and min(eased) > discover.BASE_INTERVAL
    assert world.waited("images") == [None] + [0.5] * 12  # the image host, which said nothing, is asked as before
    assert counts["rate limit answers from the API"] == 3 and counts["rate limit answers from the image host"] == 0
    assert counts["longest interval between requests to the API, in seconds"] == 16
    assert counts["requests to the API"] == 14 and counts["requests to the image host"] == 13


def test_repeated_429s_stop_the_run_cleanly_with_what_was_finished_kept(capsys):
    world = base()
    world.detours.append(rate_limited(PROFILE_PATH + BRAVO, 99))
    assert run(world) == 1
    captured = capsys.readouterr()
    assert captured.err == LIMITED
    counts = summary(captured.out)
    assert counts[collect.PROCESSED] == 2 and counts[collect.PROFILES] == 1 and counts[collect.WORKS] == 10
    assert kept(ALPHA) == ALPHA_KEPT and cached() == image_files(ALPHA_KEPT)
    assert len([request for request in world.requests() if request.url.path == PROFILE_PATH + BRAVO]) == 20
    assert world.asked_about(DELTA) == world.asked_about(FOXTROT) == []
    # Eight minutes of doubling, and then an hour at the ceiling: twelve more, five minutes apart.
    assert world.waits[-19:] == [4.0, 8.0, 16.0, 32.0, 64.0, 128.0, 256.0] + [300.0] * 12
    assert counts["rate limit answers from the API"] == 20
    assert counts["longest interval between requests to the API, in seconds"] == 300
    assert key(BRAVO) in library.last_attempts()  # noted as passed over: the next run does not begin with them


def limited(*numbers):
    """A detour: the image host answers 429 for these works' previews, or for every image when none is named."""

    def detour(request):
        if request.url.host == CDN_HOST and (not numbers or request.url.path in map(preview_path, numbers)):
            return httpx.Response(429, text="FAKE-SLOW-DOWN")

    return detour


def test_an_image_the_host_refuses_for_the_rate_limit_is_asked_for_again_and_then_that_work_alone_is_left_out(capsys):
    """The critic's finding: one work's image always answered 429, and four runs in a row stopped at the same first
    artist with nobody behind them asked, the images before it downloaded again each time. Now it is asked for
    again three times, each after twice as long a wait, then that work is left out, and everybody gets their
    turn. The run says so with its exit code: the image host was failing. And each run begins at the pace the
    last one ended with, since the host is still refusing: half a second in the first, five minutes by the fourth."""
    world = trio()
    world.detours.append(limited(3))
    for nth, began in enumerate((0.5, 8 * 0.9**4, 16 * 8 * 0.9**4, 300.0)):
        seen = len(world.log)
        assert run(world) == 1
        captured = capsys.readouterr()
        assert captured.err == "incomplete: the image host was failing. See the counts.\n"
        counts = summary(captured.out)
        asked = [request.url.path for request in world.since(seen, "images")]
        others = [preview_path(n) for n in (4, 2, 1, 21, 41)] if nth == 0 else []  # downloaded once, and never again
        assert sorted(asked) == sorted([preview_path(3)] * (1 + collect.SERVER_RETRIES) + others)
        times = [at for at, request in world.log[seen:] if request.url.path == preview_path(3)]
        doubled = [min(began * 2**asking, 300.0) for asking in (1, 2, 3)]  # 1, 2 and 4 seconds in the first run
        assert [later - earlier for earlier, later in itertools.pairwise(times)] == pytest.approx(doubled)
        assert counts[collect.IMAGE_LIMITED] == 1 and counts[collect.PROFILES] == 3 and counts[collect.ORPHANS] == 0
        assert counts["rate limit answers from the image host"] == 4
        assert kept(ALPHA) == [4, 2, 1] and kept(BRAVO) == [21] and cached() == image_files([1, 2, 4, 21, 41])
    world.detours.clear()
    assert counts_of(world, capsys)[collect.DOWNLOADED] == 1 and kept(ALPHA) == [4, 3, 2, 1]


def test_an_image_host_that_keeps_refusing_stops_the_run_with_the_artist_noted_as_passed_over(capsys):
    """Three works in a row refused, retries and all: by then it is us the host is refusing, not these images.
    The run stops, and the artist is noted first, so that the next run begins with somebody else."""
    world = base()
    world.detours.append(limited())
    assert run(world) == 1
    captured = capsys.readouterr()
    assert captured.err == (
        "stopped: the image host is failing or rate limiting\nincomplete: the image host was failing. See the counts.\n"
    )
    counts = summary(captured.out)
    assert counts[collect.IMAGE_LIMITED] == collect.MAX_REFUSALS == 3 and counts[collect.PROCESSED] == 1
    assert len(world.requests("images")) == 3 * (1 + collect.SERVER_RETRIES) and world.asked_about(BRAVO) == []
    assert list(library.last_attempts()) == [key(ALPHA)] and library.artist_keys() == [] and cached() == []
    world.detours.clear()
    seen = len(world.log)
    assert run(world, max_artists=1) == 0
    lookups = [request.url.path for request in world.since(seen, "api") if request.url.path.startswith(PROFILE_PATH)]
    assert lookups == [PROFILE_PATH + BRAVO]  # not ALPHA again
    assert run(world) == 0 and kept(ALPHA) == ALPHA_KEPT and cached() == image_files(ALPHA_KEPT + BRAVO_KEPT)


def test_refusals_with_an_image_served_between_them_do_not_stop_the_run(capsys):
    """Refused, refused, served; not found, refused, served; refused: five works without an image, and never
    three running. Only an image that is served and kept starts the count again."""
    world = base()
    world.detours.append(limited(12, 11, 8, 6))
    del world.images[preview_path(9)]
    counts = counts_of(world, capsys, expect_status=1)
    assert counts[collect.IMAGE_LIMITED] == 4 and counts[collect.IMAGE_FAILED] == 1 and counts[collect.PROFILES] == 3
    assert kept(ALPHA) == [10, 7, 5, 4, 3, 2, 1] and kept(BRAVO) == BRAVO_KEPT


NOT_AN_IMAGE = {  # what an image host can answer every request with that is neither an image nor a 429 or a 5XX
    "forbidden": lambda: httpx.Response(403, text="FAKE-FORBIDDEN"),
    "not found": lambda: httpx.Response(404, text="FAKE-NOT-FOUND"),
    "a redirect": lambda: httpx.Response(302, headers={"location": "https://zz-fake-evil.invalid/FAKE.jpg"}),
    "an html page": lambda: httpx.Response(200, headers={"content-type": "text/html"}, text="<html>FAKE</html>"),
    "an empty body": lambda: httpx.Response(200, headers={"content-type": "image/jpeg"}, content=b""),
    "a page labelled as a jpeg": lambda: httpx.Response(200, headers={"content-type": "image/jpeg"}, content=b"<html>"),
}


@pytest.mark.parametrize("answer", NOT_AN_IMAGE)
def test_an_image_host_that_serves_no_image_stops_the_run_whatever_it_answers_instead(answer, capsys, tmp_path):
    """Round 8: only a 429, a 5XX or no answer counted. With twelve artists of two works each and every image
    answered 403, all 24 were asked for, the run exited 0 with nothing on stderr, and twelve profiles were stored
    with no works. A signed address that has expired looks just like that. Every work whose image could not be
    had counts now, and nobody is stored as an artist with no works because of it."""
    world, names = two_works_each(10)
    world.detours.append(lambda request: NOT_AN_IMAGE[answer]() if request.url.host == CDN_HOST else None)
    assert run(world) == 1
    captured = capsys.readouterr()
    assert captured.err == "stopped: the image host is failing or rate limiting\n"
    counts = summary(captured.out)
    assert len(world.requests("images")) == collect.MAX_REFUSALS == 3 and world.asked_about(names[2]) == []
    assert counts[collect.NO_IMAGE] == 1 and counts[collect.PROFILES] == counts[collect.WORKS] == 0
    assert library.artist_keys() == [] and files(tmp_path) == ["cache/pipeline.sqlite"]
    assert sorted(library.last_attempts()) == [key(names[0]), key(names[1])]  # both passed over: taken last next time
    world.detours.clear()
    assert run(world) == 0 and len(library.artist_keys()) == len(cached()) // 2 == 10  # and the next run has it all


def two_works_each(count):
    """A world of this many artists with two good works each: n, and the newer one n + 10."""
    world, names = many(count)
    for number, name in enumerate(names, 11):
        world.work(name, number)
    return world, names


@pytest.mark.parametrize("status", [429, 503])
def test_an_image_host_that_refuses_everybody_stops_the_run_though_no_artist_has_three_works(status, capsys):
    """Round 7: the refusals were counted from nothing for every artist, so with two works an artist the count never
    came to three: ten artists, 80 requests, and nothing stopped. It is one count for the run now."""
    world, names = two_works_each(10)
    world.detours.append(lambda request: httpx.Response(status) if request.url.host == CDN_HOST else None)
    assert run(world) == 1
    captured = capsys.readouterr()
    assert captured.err == (
        "stopped: the image host is failing or rate limiting\nincomplete: the image host was failing. See the counts.\n"
    )
    counts = summary(captured.out)
    assert counts[collect.IMAGE_LIMITED] == collect.MAX_REFUSALS == 3 and counts[collect.PROCESSED] == 2
    assert len(world.requests("images")) == 3 * (1 + collect.SERVER_RETRIES) and world.asked_about(names[2]) == []
    # Neither is stored: of the first no image was served, and the third work refused was the second's. Both are
    # noted as passed over, so that the next run does not begin with them.
    assert library.artist_keys() == [] and counts[collect.NO_IMAGE] == 1
    assert sorted(library.last_attempts()) == [key(names[0]), key(names[1])]
    world.detours.clear()
    seen = len(world.log)
    assert run(world, max_artists=1) == 0
    assert world.since(seen, "api")[1].url.path == PROFILE_PATH + names[2]


def test_refusals_count_across_artists_only_while_nothing_is_served_between_them(capsys):
    """Refused twice for the first artist, served for the second, refused twice for the third: never three running."""
    world, names = two_works_each(4)
    world.detours.append(limited(1, 11, 3, 13))
    counts = counts_of(world, capsys, expect_status=1)
    assert counts[collect.IMAGE_LIMITED] == 4 and counts[collect.PROCESSED] == 4
    assert counts[collect.PROFILES] == counts[collect.NO_IMAGE] == 2  # who got no image is passed over, not stored
    assert [kept(name) for name in names] == [[], [12, 2], [], [14, 4]]
    assert library.artist_keys() == [key(names[1]), key(names[3])]


@pytest.mark.parametrize("host", [API_HOST, "deviantart.com"])
def test_an_image_served_from_the_apis_own_site_waits_the_apis_interval(host):
    """It is one more request to that host, and requests to that host wait the pace of the API. The critic's note:
    a preview on deviantart.com, without the www., was paced as the image host, at half a second."""
    world = solo(3)
    for entry in world.galleries[ALPHA]:
        entry["preview"]["src"] = entry["preview"]["src"].replace(CDN, f"https://{host}")

    def served_by_the_api_host(request):
        if request.url.path in world.images:
            return httpx.Response(200, headers={"content-type": "image/jpeg"}, content=world.images[request.url.path])

    world.detours.append(served_by_the_api_host)
    assert run(world) == 0
    images = [request for request in world.requests() if request.url.path in world.images]
    assert kept(ALPHA) == [3, 2, 1] and [request.url.host for request in images] == [host] * 3
    assert [request.extensions["waited"] for request in world.requests()] == [None] + [discover.BASE_INTERVAL] * 6
    assert not any("authorization" in request.headers for request in images)  # and the token does not go with it


def test_an_expired_token_is_renewed_once_and_the_request_repeated():
    world = base()
    expired = []

    def expire(request):
        if request.url.path == GALLERY_PATH and not expired:
            expired.append(1)
            world.token = "FAKE-TOKEN-GONE"

    world.detours.append(expire)
    assert run(world) == 0
    assert [request.url.path for request in world.requests("api")].count(TOKEN_PATH) == 2
    assert kept(ALPHA) == ALPHA_KEPT and len(world.log) == BASE_REQUESTS + 2


def test_a_token_that_stays_refused_is_the_api_failing_for_each_artist_and_enough_of_them_stop_the_run(capsys):
    world, names = many(7)
    world.detours.append(
        lambda request: httpx.Response(401, json={"error": "FAKE"}) if request.url.path == GALLERY_PATH else None
    )
    assert run(world) == 1
    assert capsys.readouterr().err == (
        "skipped an artist: gallery lookup answered HTTP 401\n" * 5
        + "stopped: 5 artists in a row failed: the API seems to be down\n"
    )
    assert [request.url.path for request in world.requests("api")].count(GALLERY_PATH) == 10  # each asked for twice
    assert world.asked_about(names[5]) == [] and list(library.last_attempts()) == list(map(key, names[:5]))


def test_one_artist_whose_gallery_answers_401_does_not_stop_the_run_or_stand_in_the_way_of_the_next(capsys):
    """The critic's finding: with one artist's gallery answering 401, four runs in a row stopped on them, and
    nobody behind them was ever asked. Now it is the API failing for them: they are passed over and noted."""
    world = base()
    world.detours.append(
        lambda request: (
            httpx.Response(401, json={"error": "FAKE"})
            if request.url.path == GALLERY_PATH and request.url.params["username"] == ALPHA
            else None
        )
    )
    for _ in range(4):
        assert run(world) == 1
        captured = capsys.readouterr()
        assert captured.err == "skipped an artist: gallery lookup answered HTTP 401\n"
        counts = summary(captured.out)
        assert counts[collect.API_FAILED] == 1 and counts[collect.PROCESSED] == 5 and kept(BRAVO) == BRAVO_KEPT
    assert world.asked_about(FOXTROT) and key(ALPHA) in library.last_attempts()


def test_a_stop_for_the_rate_limit_notes_the_artist_it_stopped_at_first(capsys):
    """Whoever a run stops at goes to the back of the next one, whatever the stop was for."""
    world = base()
    world.detours.append(rate_limited(GALLERY_PATH, 99))
    assert run(world) == 1 and "stopped: rate limited" in capsys.readouterr().err
    assert list(library.last_attempts()) == [key(ALPHA)]
    world.detours.clear()
    seen = len(world.log)
    assert run(world, max_artists=1) == 0
    lookups = [request.url.path for request in world.since(seen, "api") if request.url.path.startswith(PROFILE_PATH)]
    assert lookups == [PROFILE_PATH + BRAVO]


def broken_body():
    yield b"\xff\xd8\xffFAKE-IMAGE-BYTES"
    raise httpx.ReadError("FAKE-NETWORK-TEXT")


def raises(error):
    def detour(request):
        raise error(f"FAKE-NETWORK-TEXT {request.url}", request=request)

    return detour


IMAGE_FAILURES = {
    "not found": lambda request: httpx.Response(404, text="FAKE-NOT-FOUND"),
    "forbidden": lambda request: httpx.Response(403, text="<html>FAKE</html>"),
    "server error": lambda request: httpx.Response(500, text="FAKE-SERVER-ERROR"),
    "an error page labelled as a jpeg": lambda request: httpx.Response(
        503, headers={"content-type": "image/jpeg"}, content=jpeg(9)
    ),
    "a redirect": lambda request: httpx.Response(302, headers={"location": "https://zz-fake-evil.invalid/FAKE.jpg"}),
    "a redirect to itself": lambda request: httpx.Response(301, headers={"location": str(request.url)}),
    "no connection": raises(httpx.ConnectError),
    "a timeout": raises(httpx.ReadTimeout),
    "a connection cut mid-body": lambda request: httpx.Response(
        200, headers={"content-type": "image/jpeg"}, content=broken_body()
    ),
}


@pytest.mark.parametrize("failure", IMAGE_FAILURES)
def test_a_failed_image_download_skips_that_work_and_the_run_carries_on(failure, capsys, tmp_path):
    """Requirement 6. Work 9's image fails. The next work in line takes its place, and every other artist is done."""
    world = base()
    world.detours.append(
        lambda request: IMAGE_FAILURES[failure](request) if request.url.path == preview_path(9) else None
    )
    not_served = failure in ("not found", "forbidden", "a redirect", "a redirect to itself")  # the host answered
    counts = counts_of(world, capsys, expect_status=int(not not_served))
    assert counts[collect.IMAGE_FAILED if not_served else collect.IMAGE_LIMITED] == 1 and counts[collect.PROCESSED] == 5
    assert kept(ALPHA) == [12, 11, 10, 8, 7, 6, 5, 4, 3, 2] and kept(BRAVO) == BRAVO_KEPT
    assert rows("works", ALPHA) and uuid(9) not in [row["deviation_id"] for row in rows("works")]
    assert files(tmp_path) == [
        *(f"cache/images/{name}" for name in image_files([12, 11, 10, 8, 7, 6, 5, 4, 3, 2] + BRAVO_KEPT)),
        "cache/pipeline.sqlite",
    ]
    assert len([request for request in world.requests("images") if request.url.path == preview_path(9)]) == (
        1 if not_served else 1 + collect.SERVER_RETRIES
    )  # asked for again only when it is the host that fails
    # And only then are the downloads after it slowed: a host that answers, whatever it answers, has not asked.
    longest = counts["longest interval between requests to the image host, in seconds"]
    assert longest == 1 if not_served else longest > 5
    assert {request.url.host for request in world.requests()} == {API_HOST, CDN_HOST}  # the redirect was not followed
    assert min(world.gaps("images")) >= 0.5


def test_an_image_host_that_answers_5xx_is_backed_off_from_and_then_the_run_stops(capsys):
    """The critic's finding: a host answering 5XX was neither backed off from nor stopped at: 48 of 48 requests,
    two a second. It is treated like a 429 now: asked again three times, every request after twice as long a
    wait as the one before, the next work's too, and three works in a row end the run, with the artist noted as
    passed over."""
    world = solo(6)
    world.detours.append(lambda request: httpx.Response(503) if request.url.host == CDN_HOST else None)
    assert run(world) == 1
    captured = capsys.readouterr()
    assert captured.err == (
        "stopped: the image host is failing or rate limiting\nincomplete: the image host was failing. See the counts.\n"
    )
    counts = summary(captured.out)
    assert counts[collect.IMAGE_LIMITED] == 3 and len(world.requests("images")) == 3 * (1 + collect.SERVER_RETRIES)
    assert world.gaps("images") == [1.0, 2.0, 4.0, 8.0, 16.0, 32.0, 64.0, 128.0, 256.0, 300.0, 300.0]
    assert counts["rate limit answers from the image host"] == 0  # a 5XX is not a 429, and is backed off from alike
    assert counts["longest interval between requests to the image host, in seconds"] == 300
    assert rows("works") == [] and list(library.last_attempts()) == [key(ALPHA)]


# --- when the API does not answer as documented ---


def failing(world, path, status, username, times=99):
    """A detour: this endpoint answers this artist's requests with this status, this many times."""
    left = [times]

    def detour(request):
        if request.url.path.startswith(path) and world.concerns(request, username) and left[0] > 0:
            left[0] -= 1
            headers = {"location": "https://zz-fake-evil.invalid/"}
            return httpx.Response(status, headers=headers, text="FAKE-BODY zz-fake-bravo")

    world.detours.append(detour)


@pytest.mark.parametrize("status", [500, 502, 503])
@pytest.mark.parametrize("path", [PROFILE_PATH, GALLERY_PATH, METADATA_PATH])
def test_a_5xx_answer_is_asked_for_again_after_a_doubling_wait(path, status, capsys):
    """The API docs say a 500 is worth retrying, up to three times, and call a 503 "try again later". Here the
    fourth answer is a good one. Each 5XX slows the pace of the run as a 429 does."""
    world = base()
    failing(world, path, status, BRAVO, times=3)
    assert run(world) == 0
    captured = capsys.readouterr()
    assert captured.out.startswith(EXPECTED_COUNTS) and captured.err == ""
    asked = [at for at, request in world.log if request.url.path.startswith(path) and world.concerns(request, BRAVO)]
    waits = [later - earlier for earlier, later in itertools.pairwise(asked)]
    assert waits == [4.0, 8.0, 16.0]  # the base doubled, and doubled again
    assert len(world.log) == BASE_REQUESTS + 3 and min(world.gaps("api")) >= 2.0
    mine = [world.concerns(request, BRAVO) and request.url.path.startswith(path) for request in world.requests("api")]
    following = world.waited("api")[len(mine) - mine[::-1].index(True)]  # before the next request, another one
    assert following == pytest.approx(14.4)
    counts = summary(captured.out)
    assert counts["rate limit answers from the API"] == 0
    assert counts["longest interval between requests to the API, in seconds"] == 16
    assert collect.SERVER_RETRIES == 3


FAILURES = {  # what the API answers for BRAVO: (endpoint, status, requests made for it, what is said)
    "a profile that answers 500": (PROFILE_PATH, 500, 4, "profile lookup answered HTTP 500"),
    "a profile that answers 503": (PROFILE_PATH, 503, 4, "profile lookup answered HTTP 503"),
    "a profile that redirects": (PROFILE_PATH, 302, 1, "profile lookup answered HTTP 302"),
    "a gallery that answers 500": (GALLERY_PATH, 500, 4, "gallery lookup answered HTTP 500"),
    "a gallery that answers 503": (GALLERY_PATH, 503, 4, "gallery lookup answered HTTP 503"),
    "metadata that answers 500": (METADATA_PATH, 500, 4, "metadata lookup answered HTTP 500"),
    "metadata that answers 400": (METADATA_PATH, 400, 1, "metadata lookup answered HTTP 400"),
    "metadata that answers 404": (METADATA_PATH, 404, 1, "metadata lookup answered HTTP 404"),
    "a profile that answers 403": (PROFILE_PATH, 403, 1, "profile lookup answered HTTP 403"),
    "a gallery that answers 404": (GALLERY_PATH, 404, 1, "gallery lookup answered HTTP 404"),
}


@pytest.mark.parametrize("case", FAILURES)
def test_an_artist_the_api_fails_for_is_skipped_and_counted_and_the_run_carries_on(case, capsys, tmp_path):
    """The critic's finding, and operationally the worst: when one artist's profile, gallery or metadata could
    not be had, the run stopped there, and so did every run after it. Now that artist is passed over, with a
    note for next time, and everybody behind them gets their turn."""
    path, status, requests, said = FAILURES[case]
    world = base()
    failing(world, path, status, BRAVO)
    assert run(world) == 1
    captured = capsys.readouterr()
    assert captured.err == f"skipped an artist: {said}\n"
    counts = summary(captured.out)
    assert counts[collect.API_FAILED] == 1 and counts[collect.PROCESSED] == 5 and counts[collect.PROFILES] == 2
    assert rows("profiles", BRAVO) == rows("works", BRAVO) == [] and kept(ALPHA) == ALPHA_KEPT
    assert world.asked_about(DELTA) and world.asked_about(FOXTROT)  # the artists after BRAVO
    theirs = [request for request in world.requests("api") if world.concerns(request, BRAVO)]
    assert len([request for request in theirs if request.url.path.startswith(path)]) == requests
    assert key(BRAVO) in library.last_attempts() and key(BRAVO) not in library.artist_keys()
    assert files(tmp_path) == [*(f"cache/images/{name}" for name in image_files(ALPHA_KEPT)), "cache/pipeline.sqlite"]
    assert {request.url.host for request in world.requests()} <= {API_HOST, CDN_HOST}  # no redirect was followed


def many(count):
    """A world of this many artists, each with one good work, and their handles in the order they are taken."""
    world = World()
    names = [f"zz-fake-{letter}" for letter in "abcdefghij"[:count]]
    for number, name in enumerate(names, 1):
        world.artist(name)
        world.work(name, number)
    return world, names


def test_several_artists_failing_in_a_row_stop_the_run_because_then_it_is_the_api(capsys):
    """With an answer that is not a refusal. Five artists of 5XX answers are twenty refusals, and the twentieth
    ends the run by the pace's hour first: see the next test but one."""
    world, names = many(7)
    world.detours.append(
        lambda request: httpx.Response(403, text="FAKE-DOWN") if request.url.path.startswith(PROFILE_PATH) else None
    )
    assert run(world) == 1
    captured = capsys.readouterr()
    assert captured.err == (
        "skipped an artist: profile lookup answered HTTP 403\n" * 5
        + "stopped: 5 artists in a row failed: the API seems to be down\n"
    )
    counts = summary(captured.out)
    assert counts[collect.API_FAILED] == collect.MAX_FAILURES == 5 and counts[collect.PROCESSED] == 5
    assert world.asked_about(names[4]) and world.asked_about(names[5]) == world.asked_about(names[6]) == []


def test_failures_that_are_not_in_a_row_do_not_stop_the_run(capsys):
    """Four artists fail, one is collected, four more fail: eight failures, and never five running. With an
    answer that is not a refusal: 5XX answers this many are an hour of refusals, which is the next test."""
    world, names = many(9)
    world.detours.append(
        lambda request: (
            httpx.Response(403, text="FAKE-DOWN")
            if request.url.path.startswith(PROFILE_PATH) and not world.concerns(request, names[4])
            else None
        )
    )
    assert run(world) == 1
    captured = capsys.readouterr()
    assert captured.err == "skipped an artist: profile lookup answered HTTP 403\n" * 8
    counts = summary(captured.out)
    assert counts[collect.API_FAILED] == 8 and counts[collect.PROFILES] == 1 and counts[collect.PROCESSED] == 9
    assert library.artist_keys() == [key(names[4])]


def test_an_api_that_answers_503_to_all_but_every_fifth_artist_stops_after_an_hour_of_it(capsys):
    """The critic's counterexample: every API request but the token answered 503, with no Retry-After, except
    every fifth artist's profile and gallery, answered at the fourth asking. 91.7% refused, and it never
    stopped: 60 artists took 23.3 hours, because a 5XX counted for nothing towards the hour, and every fifth
    artist ended the row of five. A 5XX at the ceiling counts as a 429 there does, and the hour ends the run in
    the fifth artist's turn, an answer among the refusals or not."""
    world = World()
    names = [f"zz-fake-c{n:02d}" for n in range(10)]
    for name in names:
        world.artist(name)  # an empty gallery: a profile and a gallery lookup each, when they are answered
    asked: dict[str, int] = {}

    def refused(request):
        if request.url.path == TOKEN_PATH:
            return None
        asked[str(request.url)] = asked.get(str(request.url), 0) + 1
        name = request.url.params.get("username") or request.url.path.removeprefix(PROFILE_PATH)
        return None if names.index(name) % 5 == 4 and asked[str(request.url)] == 4 else httpx.Response(503)

    world.detours.append(refused)
    assert run(world) == 1
    captured = capsys.readouterr()
    assert captured.err == "skipped an artist: profile lookup answered HTTP 503\n" * 4 + LIMITED
    climb = [2.0, 4.0, 8.0, 16.0, 32.0, 64.0, 128.0, 256.0]  # four artists' profiles, each asked four times
    assert world.gaps("api") == climb + [300.0] * 12 + [270.0]  # and the fifth's, answered; its gallery, refused
    counts = summary(captured.out)
    assert counts["server errors or no answer from the API"] == 20 and counts["rate limit answers from the API"] == 0
    assert counts[collect.PROCESSED] == 5 and counts[collect.API_FAILED] == 4 and world.now / 3600 < 1.5


def ticking(monkeypatch):
    """Make the clock that rows are stamped with tick a second at every reading, so that "when" has an order."""
    seconds = itertools.count()
    monkeypatch.setattr(store, "_now", lambda: iso(1_800_000_000 + next(seconds)))


def test_an_artist_passed_over_waits_at_the_back_of_the_next_run_not_at_the_front(monkeypatch):
    """The critic's finding: artists with nothing stored came first, so an artist the API failed for was first
    again the next time, and three runs in a row ended on them with nobody behind them asked. Now whoever was
    tried longest ago comes first, whether that try stored them or passed them over."""
    ticking(monkeypatch)
    world = base()
    failing(world, GALLERY_PATH, 500, ALPHA)  # first in line, and the API never answers for them

    def turns(max_artists):
        seen = len(world.log)
        run(world, max_artists)
        lookups = [request.url.path for request in world.since(seen, "api")]
        return [path.removeprefix(PROFILE_PATH) for path in lookups if path.startswith(PROFILE_PATH)]

    assert turns(2) == [ALPHA, BRAVO]
    assert turns(2) == [CHARLIE, DELTA]  # not ALPHA again: they have had their turn, and so has BRAVO
    assert turns(2) == [FOXTROT, ALPHA]  # everybody has been tried once; ALPHA was tried longest ago
    assert turns(5) == [BRAVO, CHARLIE, DELTA, FOXTROT, ALPHA]  # passed over, unavailable, AI-made, stored: one queue
    assert turns(1) == [BRAVO]


OUTAGES = {  # what every profile lookup answers while the API is down, or is refusing us
    "403 with a page of HTML": lambda: httpx.Response(403, text="<html>FAKE-BLOCKED</html>"),
    "403 with an error and no code": lambda: refusal(403),
    "404 with a code that is not one of the three": lambda: refusal(404, error_code=3),
    "400 with the code as a string": lambda: refusal(400, error_code="2"),
    "410 with nothing": lambda: httpx.Response(410),
}


@pytest.mark.parametrize("outage", OUTAGES)
def test_an_outage_that_refuses_every_profile_stops_the_run_and_changes_nothing_that_is_kept(outage, capsys, tmp_path):
    """The critic's finding of round 4, and a dangerous one: with the token still issued and every profile lookup
    answering 403, the first run exited 0 in silence and the second purged the whole library as "dead". An answer
    that is not one of the API's three documented codes is the API failing: a few in a row stop the run."""
    world = base()
    assert run(world) == 0
    before = (rows("profiles"), rows("works"), files(tmp_path), store.all_artists(), library.artist_marks())
    capsys.readouterr()
    world.detours.append(lambda request: OUTAGES[outage]() if request.url.path.startswith(PROFILE_PATH) else None)
    for _ in range(3):
        assert run(world) == 1
        captured = capsys.readouterr()
        assert re.fullmatch(
            r"(skipped an artist: profile lookup answered HTTP 4\d\d\n){5}"
            r"stopped: 5 artists in a row failed: the API seems to be down\n",
            captured.err,
        ), captured.err
        counts = summary(captured.out)
        assert counts[collect.API_FAILED] == counts[collect.PROCESSED] == 5
        assert counts[collect.PURGED] == counts[collect.UNAVAILABLE] == counts[collect.MARKED] == 0
        assert (rows("profiles"), rows("works"), files(tmp_path), store.all_artists(), library.artist_marks()) == before
    world.detours.clear()
    assert run(world) == 0 and kept(ALPHA) == ALPHA_KEPT and kept(BRAVO) == BRAVO_KEPT


def test_an_outage_that_calls_every_account_gone_marks_them_and_deletes_nothing(capsys, tmp_path):
    """The critic's finding of round 5: three of seven profiles answering 4XX with a small error code were purged
    after two runs. Here it is all of them, on three runs. A mark is all the API's word is worth."""
    world = base()
    assert run(world) == 0
    before = (rows("profiles"), rows("works"), files(tmp_path), store.all_artists())
    world.detours.append(
        lambda request: refusal(404, error_code=2) if request.url.path.startswith(PROFILE_PATH) else None
    )
    for _ in range(3):
        counts = counts_of(world, capsys)
        assert counts[collect.UNAVAILABLE] == 5 and counts[collect.PURGED] == 0
        assert (rows("profiles"), rows("works"), files(tmp_path), store.all_artists()) == before
    assert artist_marks() == dict.fromkeys(map(key, (ALPHA, BRAVO, CHARLIE, DELTA, FOXTROT)), "account gone")
    world.detours.clear()
    assert run(world) == 0 and artist_marks() == dict.fromkeys((key(CHARLIE), key(DELTA)), "account gone")


def test_a_partial_outage_that_calls_three_accounts_of_seven_gone_run_after_run_deletes_nothing(capsys, tmp_path):
    """The critic's finding: three of seven profiles answering 4XX with a small error code were purged after two
    back-to-back runs, and both runs exited 0."""
    world, names = many(7)
    assert run(world) == 0
    before = (rows("profiles"), files(tmp_path), store.all_artists())
    gone = [PROFILE_PATH + name for name in names[1:6:2]]
    world.detours.append(lambda request: refusal(404, error_code=1) if request.url.path in gone else None)
    for _ in range(3):
        counts = counts_of(world, capsys)
        assert counts[collect.UNAVAILABLE] == 3 and counts[collect.PURGED] == 0 and len(rows("works")) == 7
        assert ([row["artist_key"] for row in rows("profiles")], files(tmp_path), store.all_artists()) == (
            [row["artist_key"] for row in before[0]],
            *before[1:],
        )
    assert artist_marks() == dict.fromkeys(map(key, names[1:6:2]), "account gone")


def test_a_run_in_which_the_api_failed_for_anybody_does_not_exit_zero(capsys):
    """Fewer than MAX_FAILURES of them cannot make a row of that many, and the run still says so."""
    world, names = many(3)
    world.detours.append(lambda request: refusal(403) if request.url.path == PROFILE_PATH + names[1] else None)
    assert run(world) == 1
    captured = capsys.readouterr()
    assert captured.err == "skipped an artist: profile lookup answered HTTP 403\n"
    assert summary(captured.out)[collect.API_FAILED] == 1 and library.artist_keys() == [key(names[0]), key(names[2])]


def mostly_new():
    """The critic's world: three artists who are collected, and five whom discovery has since added."""
    world = World()
    for name, first in ((ALPHA, 1), (BRAVO, 21), (DELTA, 41)):
        world.artist(name)
        for n in range(first, first + 3):
            world.work(name, n)
    assert run(world) == 0 and len(rows("works")) == len(cached()) == 9
    for letter in "vwxyz":
        world.artist(f"zz-fake-new-{letter}")
        world.work(f"zz-fake-new-{letter}", 60 + ord(letter))
    return world


EMPTY_ANSWERS = {  # an API that answers every lookup with a 200, and with nothing in it: (endpoint, body, failing)
    "an empty gallery": (GALLERY_PATH, {"has_more": False, "next_offset": None, "results": []}, False),
    "a gallery that is an empty object": (GALLERY_PATH, {}, True),
    "an empty metadata list": (METADATA_PATH, {"metadata": []}, False),
    "metadata that is an empty object": (METADATA_PATH, {}, True),
    "an error body for the gallery": (GALLERY_PATH, {"error": "FAKE", "error_description": "FAKE-ERROR"}, True),
    "an error body for the profile": (PROFILE_PATH, {"error": "FAKE", "error_description": "FAKE-ERROR"}, True),
    "an empty profile": (PROFILE_PATH, {}, True),
}


@pytest.mark.parametrize("answer", EMPTY_ANSWERS)
def test_an_outage_that_answers_with_nothing_deletes_nothing_however_many_artists_are_new(answer, capsys, tmp_path):
    """The critic's finding of round 5: three collected artists and five new ones, and every gallery lookup answering
    200 with an empty list. After two runs all nine works, their files and the profiles were gone, and both runs
    exited 0: the guard counted artists, and the new ones outvoted the kept. There is no guard to outvote now."""
    path, body, failing = EMPTY_ANSWERS[answer]
    world = mostly_new()
    before = (rows("works"), files(tmp_path), {row["artist_key"] for row in rows("profiles")})
    capsys.readouterr()
    world.detours.append(lambda request: httpx.Response(200, json=body) if request.url.path.startswith(path) else None)
    for _ in range(3):
        assert run(world) == int(failing)
        captured = capsys.readouterr()
        assert ("the API seems to be down" in captured.err) is failing
        assert summary(captured.out)[collect.PURGED] == 0 and library.work_marks() == library.artist_marks() == {}
        assert rows("works") == before[0] and files(tmp_path) == before[1]
        assert {row["artist_key"] for row in rows("profiles")} >= before[2]


def test_an_artist_who_took_every_work_down_keeps_what_was_kept_with_the_time_it_was_last_seen(monkeypatch, capsys):
    ticking(monkeypatch)
    world = trio()
    assert run(world) == 0
    before = rows("works")
    world.galleries[ALPHA] = []
    for _ in range(3):
        counts = counts_of(world, capsys)
        assert counts[collect.PROFILES] == 3 and counts[collect.WORKS] == 2 and counts[collect.PURGED] == 0
        assert [row for row in rows("works") if row["artist_key"] == key(ALPHA)] == before[:4]
    profile_seen = rows("profiles", ALPHA)[0]["fetched_at"]
    assert all(row["fetched_at"] < profile_seen for row in rows("works", ALPHA))  # which is how a reader can tell
    assert work_marks() == artist_marks() == {} and len(cached()) == 6


# Answers for ALPHA that are in shape and have lost something: what each does to the gallery or the metadata.
PARTLY_BROKEN = {
    "a gallery cut short": lambda world: world.galleries.__setitem__(ALPHA, world.galleries[ALPHA][:2]),
    "entries that are null": lambda world: world.galleries.__setitem__(ALPHA, [None] * 6),
    "entries with no word on maturity": lambda world: [entry.pop("is_mature") for entry in world.galleries[ALPHA]],
    "entries with no author": lambda world: [entry.pop("author") for entry in world.galleries[ALPHA]],
    "entries with no preview": lambda world: [entry.pop("preview") for entry in world.galleries[ALPHA]],
    "entries with no word on being deleted": lambda world: [
        entry.pop("is_deleted") for entry in world.galleries[ALPHA]
    ],
    "metadata for the first work only": lambda world: [world.metadata.pop(uuid(n)) for n in range(2, 7)],
    "metadata with no tags": lambda world: [world.metadata[uuid(n)].pop("tags") for n in range(1, 7)],
    "metadata with no word on maturity": lambda world: [world.metadata[uuid(n)].pop("is_mature") for n in range(1, 7)],
    "previews that are not addresses": lambda world: [
        entry.__setitem__("preview", {"src": "FAKE-NOT-AN-ADDRESS"}) for entry in world.galleries[ALPHA]
    ],
}


@pytest.mark.parametrize("broken", PARTLY_BROKEN)
def test_a_partly_broken_answer_deletes_nothing_and_marks_nothing(broken, capsys, tmp_path):
    """The critic's finding: a gallery cut short, nulled entries, a missing field or metadata for only the first id
    each deleted kept works in one run. An answer that fails to say something is not an answer that says it."""
    world = solo(6)
    world.artist(BRAVO)
    world.work(BRAVO, 21)
    assert run(world) == 0
    before = (rows("works"), files(tmp_path))
    PARTLY_BROKEN[broken](world)
    for _ in range(2):
        counts_of(world, capsys)
        kept_before = [row for row in before[0] if row["artist_key"] == key(ALPHA)]
        assert [{**row, "fetched_at": None} for row in rows("works", ALPHA)] == [
            {**row, "fetched_at": None} for row in kept_before
        ]
        assert files(tmp_path) == before[1] and work_marks() == artist_marks() == {}


def test_one_user_object_given_for_every_profile_is_believed_for_nobody_else(capsys, tmp_path):
    """The critic's finding: the same `user` on every lookup made every artist the same account, and three of four
    were purged as its old handles. A profile is believed when it is the profile of the handle that was asked."""
    world, names = many(4)
    assert run(world) == 0
    was = stored()
    shared = profile(names[0])
    world.detours.append(
        lambda request: httpx.Response(200, json=shared) if request.url.path.startswith(PROFILE_PATH) else None
    )
    assert run(world) == 1
    captured = capsys.readouterr()
    assert captured.err == "skipped an artist: profile lookup answered with a profile that is not theirs\n" * 3
    assert summary(captured.out)[collect.API_FAILED] == 3 and stored() == was and artist_marks() == {}


@pytest.mark.parametrize("user_id", ["missing", None, "", 7, ["FAKE-USERID"]])
def test_a_profile_with_no_user_id_is_the_api_failing_for_that_artist_and_nothing_of_it_is_stored(user_id, capsys):
    """Round 7's test gap: the rule's "there is a user id" could be taken out and every test still passed. A profile
    stored without one has no account hash: a removal could not follow that account to its next handle (hard
    rule 7), and nothing would say that two handles are one account. Here for a new artist and for a kept one."""
    world = trio()
    assert run(world) == 0
    world.artist("zz-fake-new")
    world.work("zz-fake-new", 9)
    was = stored()
    for name in ("zz-fake-new", ALPHA):
        world.profiles[name]["user"]["userid"] = user_id
        if user_id == "missing":
            del world.profiles[name]["user"]["userid"]
    assert run(world) == 1
    captured = capsys.readouterr()
    assert captured.err == "skipped an artist: profile lookup answered with a profile that is not theirs\n" * 2
    assert summary(captured.out)[collect.API_FAILED] == 2 and stored() == was and artist_marks() == {}
    assert rows("profiles", "zz-fake-new") == [] and library.account_hash(key(ALPHA)) == user_hash(ALPHA)
    assert len(world.asked_about("zz-fake-new")) == 1  # the profile, and no more


SEQUENCE = [  # what one run's answers can be like, endpoint by endpoint: None is the honest answer
    None,
    (PROFILE_PATH, lambda: refusal(403)),
    (PROFILE_PATH, lambda: refusal(404, error_code=2)),
    (PROFILE_PATH, lambda: httpx.Response(200, json={})),
    (PROFILE_PATH, lambda: httpx.Response(200, json=profile(ALPHA))),
    (PROFILE_PATH, lambda: httpx.Response(200, json={"user": author(ALPHA)})),
    (GALLERY_PATH, lambda: httpx.Response(200, json={"has_more": False, "results": []})),
    (GALLERY_PATH, lambda: httpx.Response(200, json={"has_more": False, "results": [None, {}, {"deviationid": 7}]})),
    (GALLERY_PATH, lambda: httpx.Response(200, json={})),
    (GALLERY_PATH, lambda: httpx.Response(200, text="<html>FAKE</html>")),
    (GALLERY_PATH, lambda: httpx.Response(500)),
    (GALLERY_PATH, lambda: httpx.Response(401)),
    (METADATA_PATH, lambda: httpx.Response(200, json={"metadata": []})),
    (
        METADATA_PATH,
        lambda: httpx.Response(200, json={"metadata": [{"deviationid": uuid(1), "tags": [{"tag_name": "ai"}]}]}),
    ),
    (METADATA_PATH, lambda: httpx.Response(200, json={"metadata": None})),
    (METADATA_PATH, lambda: httpx.Response(404)),
    ("/f/", lambda: httpx.Response(404)),
    ("/f/", lambda: httpx.Response(429)),
    ("/f/", lambda: httpx.Response(503)),
    ("/f/", lambda: httpx.Response(200, headers={"content-type": "text/html"}, text="<html>FAKE</html>")),
]


@pytest.mark.parametrize("first", range(len(SEQUENCE)))
def test_whatever_the_api_and_the_image_host_answer_what_is_kept_only_grows(first, tmp_path):
    """The property round 5 asks for, and the whole of it: across any sequence of answers, the kept artists, works
    and files only ever grow or gain marks. Each kind of answer here is given to every request of its endpoint for
    a whole run, followed by each other kind for the next run, over a library with kept and new artists in it."""
    world = mostly_new()
    (library.image_dir() / f"{uuid(2)}.jpg").unlink()  # one kept work whose image has to be fetched again
    was, rows_then = stored(), rows("works")
    was = (was[0], was[1], was[2] - {f"{uuid(2)}.jpg"}, was[3])
    for kind in (SEQUENCE[first], *SEQUENCE):
        world.detours[:] = (
            [] if kind is None else [lambda request, kind=kind: kind[1]() if kind[0] in request.url.path else None]
        )
        run(world)
        assert nothing_lost(was), kind and kind[0]
        after = {row["deviation_id"]: row for row in rows("works")}
        for row in rows_then:  # and a kept work is the same work, under the same key
            assert {**after[row["deviation_id"]], "fetched_at": None, "image_file": None} == {
                **row,
                "fetched_at": None,
                "image_file": None,
            }
    assert len(files(tmp_path)) >= 9


MALFORMED_BODIES = {
    "a gallery that is a list": (GALLERY_PATH, [deviation(1, ALPHA)]),
    "a gallery with no results": (GALLERY_PATH, {"has_more": False}),
    "results that are null": (GALLERY_PATH, {"results": None}),
    "results that are an object": (GALLERY_PATH, {"results": {"FAKE": deviation(1, ALPHA)}}),
    "results that are a string": (GALLERY_PATH, {"results": "FAKE-RESULTS"}),
    "entries that are not objects": (GALLERY_PATH, {"results": [None, 7, "FAKE", [deviation(1, ALPHA)], True]}),
    "metadata that is a list": (METADATA_PATH, [metadata(1, ALPHA, ["fakepainting"])]),
    "metadata with no items": (METADATA_PATH, {"metadata": None}),
    "metadata items that are not objects": (
        METADATA_PATH,
        {"metadata": [None, 7, "FAKE", {"deviationid": 7}, {"tags": []}]},
    ),
    "a profile that is a list": (PROFILE_PATH + ALPHA, ["FAKE-PROFILE"]),
    "a profile of the wrong types": (
        PROFILE_PATH + ALPHA,
        {"tagline": 7, "bio": ["FAKE"], "website": {"FAKE": 1}, "artist_level": True, "user": "FAKE-USER"},
    ),
    "a profile whose user id is no word": (PROFILE_PATH + ALPHA, {"user": {"userid": ["FAKE-USERID"]}}),
    "a profile with the name and no user id": (PROFILE_PATH + ALPHA, {"user": {"username": ALPHA}, "tagline": "FAKE"}),
    "a profile with the name and an empty user id": (PROFILE_PATH + ALPHA, {"user": {"username": ALPHA, "userid": ""}}),
    "a profile with the name and a number for a user id": (
        PROFILE_PATH + ALPHA,
        {"user": {"username": ALPHA, "userid": 7}},
    ),
}


@pytest.mark.parametrize("case", MALFORMED_BODIES)
def test_a_body_of_the_wrong_shape_is_the_api_failing_for_that_artist_and_changes_nothing_kept(case, capsys, tmp_path):
    """A 200 is believed only in its documented shape: a gallery with a list of results and a has_more that is true
    or false, metadata with a list, a profile whose user has an id and the handle that was asked for."""
    path, body = MALFORMED_BODIES[case]
    world = solo(2)
    world.artist(BRAVO)
    world.work(BRAVO, 21)
    assert run(world) == 0
    before = (rows("profiles", ALPHA), rows("works", ALPHA), files(tmp_path))
    world.detours.append(
        lambda request: (
            httpx.Response(200, json=body)
            if request.url.path == path and BRAVO not in str(request.url) and uuid(21) not in str(request.url)
            else None
        )
    )
    believed = case == "metadata items that are not objects"  # a list, as documented: read for what it holds
    counts = counts_of(world, capsys, expect_status=int(not believed))
    assert counts[collect.API_FAILED] == int(not believed) and counts[collect.PROCESSED] == 2 and kept(BRAVO) == [21]
    assert (rows("works", ALPHA), files(tmp_path)) == before[1:] and work_marks() == artist_marks() == {}
    assert believed or rows("profiles", ALPHA) == before[0]


UNEXPECTED = {
    "not json": lambda request: httpx.Response(200, text=f"<html>FAKE-HTML-TEXT {ALPHA}</html>"),
    "results that cannot be read": lambda request: httpx.Response(200, json={"results": 7}),
}


def breaking(monkeypatch, *usernames):
    """Make this module's own code fail in these artists' turns, as a bug would: with a message full of their data."""
    real = library.account_removed

    def broken(user_id, db=None):
        if any(str(user_id).endswith(name) for name in usernames):
            raise RuntimeError(f"FAKE-MESSAGE {user_id} zz-fake")
        return real(user_id, db)

    monkeypatch.setattr(library, "account_removed", broken)


def test_an_unexpected_error_with_one_artist_skips_that_artist_and_the_run_carries_on(monkeypatch, capsys, tmp_path):
    """No single artist can end the run. What went wrong is counted and named by type and line, never by message."""
    world = base()
    breaking(monkeypatch, ALPHA)
    assert run(world) == 1
    captured = capsys.readouterr()
    assert re.fullmatch(r"skipped an artist: RuntimeError at collect\.py line \d+\n", captured.err), captured.err
    assert "FAKE" not in captured.err and "zz-fake" not in captured.err and "Traceback" not in captured.err
    counts = summary(captured.out)
    assert counts[collect.FAILED] == 1 and counts[collect.PROCESSED] == 5 and counts[collect.PROFILES] == 2
    assert rows("profiles", ALPHA) == rows("works", ALPHA) == [] and kept(BRAVO) == BRAVO_KEPT
    assert files(tmp_path) == [*(f"cache/images/{name}" for name in image_files(BRAVO_KEPT)), "cache/pipeline.sqlite"]


def test_five_artists_in_a_row_failing_for_any_reason_stop_the_run(capsys):
    """Five in a row is not five unlucky artists. Nor is the network going down."""
    world, names = many(6)
    world.detours.append(lambda request: UNEXPECTED["not json"](request) if request.url.path == GALLERY_PATH else None)
    assert run(world) == 1
    captured = capsys.readouterr()
    assert captured.err == (
        "skipped an artist: gallery lookup answered with a body of the wrong shape\n" * 5
        + "stopped: 5 artists in a row failed: the API seems to be down\n"
    )
    counts = summary(captured.out)
    assert counts[collect.API_FAILED] == 5 and counts[collect.PROCESSED] == 5 and world.asked_about(names[5]) == []

    world = base()
    world.detours.append(raises(httpx.ConnectError))  # every request, the one for a token included
    assert run(world) == 1
    captured = capsys.readouterr()
    assert re.fullmatch(
        r"(skipped an artist: ConnectError at collect\.py line \d+\n){5}"
        r"stopped: 5 artists in a row failed: the API seems to be down\n",
        captured.err,
    ), captured.err
    assert "FAKE" not in captured.err and summary(captured.out)[collect.FAILED] == 5


def test_unexpected_failures_with_a_good_answer_between_them_do_not_add_up(monkeypatch, capsys):
    world = base()  # ALPHA and BRAVO fail, of CHARLIE the API says there is no such user, DELTA and FOXTROT fail
    breaking(monkeypatch, ALPHA, BRAVO, DELTA, FOXTROT)
    assert run(world) == 1
    captured = capsys.readouterr()
    assert (
        captured.err.count("skipped an artist: RuntimeError at collect.py line ") == 3 and "stopped" not in captured.err
    )
    counts = summary(captured.out)
    assert counts[collect.FAILED] == 3 and counts[collect.PROCESSED] == 5 and counts[collect.UNAVAILABLE] == 2
    assert sorted(library.last_attempts()) == sorted(map(key, (ALPHA, BRAVO, CHARLIE, DELTA, FOXTROT)))  # all noted


def test_a_handle_that_is_not_a_plain_handle_never_reaches_a_url_and_stops_nobody(monkeypatch, capsys):
    """The store only holds plain handles. Were a row ever otherwise, it is skipped and counted before any request.
    The critic's finding: five such rows at the front of the queue stopped every run after no request at all,
    with "the API seems to be down". It is the store's row that is wrong, not the API."""
    world = solo(1)
    hostile = "../../../oauth2/revoke?FAKE=1"
    odd = [(f"da:zz-fake-{n}", hostile, "https://www.deviantart.com/FAKE") for n in range(6)]
    listed = [*odd, *store.all_artists()]
    monkeypatch.setattr(store, "all_artists", lambda db=None: listed)
    counts = counts_of(world, capsys)
    assert counts[collect.BAD_NAME] == 6 and counts[collect.PROCESSED] == counts[collect.PROFILES] == 1
    assert all("revoke" not in str(request.url) for request in world.requests()) and kept(ALPHA) == [1]


# --- when the database refuses a row ---
#
# The real run of 2026-10-12 stopped with "IntegrityError at collect.py line 221" after 151 artists. The profiles
# table had been changed under the running collector (tests/test_library.py tells the story and reproduces the
# statement). Whatever makes the database refuse a row, one row or one artist must not end the run.


def raw(statement, *values):
    """Run one statement on the database with no help from the library, as some other program would."""
    with closing(sqlite3.connect(config.LOCAL_DB)) as conn, conn:
        return conn.execute(statement, values).fetchall()


def refuse(table, column, value):
    """Make the database refuse one row, the way a constraint does: the statement fails with IntegrityError."""
    library.artist_keys()
    raw(
        f"CREATE TRIGGER fake_refusal BEFORE INSERT ON {table} WHEN NEW.{column} = '{value}' "
        "BEGIN SELECT RAISE(ABORT, 'FAKE-CONSTRAINT'); END"
    )


def test_a_work_row_the_database_refuses_is_skipped_and_counted_and_the_run_carries_on(capsys, tmp_path):
    world = base()
    refuse("works", "deviation_id", uuid(7))
    assert run(world) == 1
    captured = capsys.readouterr()
    assert captured.err == "incomplete: the database refused rows it was given. See the counts.\n"
    counts = summary(captured.out)
    assert counts[collect.ROW_REFUSED] == 1 and counts[collect.NOT_STORED] == 0
    assert counts[collect.WORKS] == 12 and counts[collect.PROFILES] == 3 and counts[collect.PROCESSED] == 5
    assert kept(ALPHA) == [n for n in ALPHA_KEPT if n != 7] and kept(BRAVO) == BRAVO_KEPT
    assert counts[collect.ORPHANS] == 1  # the image that was fetched for the refused row does not stay behind
    assert files(tmp_path) == [
        *(f"cache/images/{name}" for name in image_files([n for n in ALPHA_KEPT if n != 7] + BRAVO_KEPT)),
        "cache/pipeline.sqlite",
    ]


def test_an_artist_whose_profile_the_database_refuses_is_skipped_and_counted_and_the_run_carries_on(capsys, tmp_path):
    """What the real run should have done at its 152nd artist: count them, leave nothing half-written, and go on."""
    world = base()
    refuse("profiles", "artist_key", key(BRAVO))
    assert run(world) == 1
    captured = capsys.readouterr()
    assert captured.err == "incomplete: the database refused rows it was given. See the counts.\n"
    counts = summary(captured.out)
    assert counts[collect.NOT_STORED] == 1 and counts[collect.ROW_REFUSED] == 0
    assert counts[collect.PROFILES] == 2 and counts[collect.WORKS] == 10 and counts[collect.PROCESSED] == 5
    assert rows("profiles", BRAVO) == rows("works", BRAVO) == [] and counts[collect.ORPHANS] == 3
    assert kept(ALPHA) == ALPHA_KEPT and [row["artist_key"] for row in rows("profiles")] == [key(ALPHA), key(FOXTROT)]
    assert world.asked_about(DELTA) and world.asked_about(FOXTROT)  # the artists after BRAVO had their turn
    assert key(BRAVO) in library.last_attempts()  # and BRAVO is noted, for the back of the next run
    assert files(tmp_path) == [*(f"cache/images/{name}" for name in image_files(ALPHA_KEPT)), "cache/pipeline.sqlite"]


def test_a_run_rebuilds_the_table_the_incident_left_behind_and_fills_it_in_again(capsys):
    """The real cache as it stands: a profiles table with no bio and no CHECK, and rows the first version's
    collector wrote into the wrong columns. One run of this version puts it right."""
    world = base()
    raw(
        "CREATE TABLE profiles (artist_key TEXT PRIMARY KEY, tagline TEXT, specialty TEXT, level TEXT, website TEXT, "
        "fetched_at TEXT NOT NULL, user_hash TEXT) STRICT"
    )
    shifted = "INSERT INTO profiles VALUES (?, 'FAKE-TAGLINE call 555 0199', 'None', 'FAKE-LEVEL', NULL, ?, ?)"
    raw(shifted, key(ALPHA), "https://wa.me/15550109999", "2026-10-12T17:31:07+00:00")
    raw(shifted, key(BRAVO), "https://zz-fake-wrong-column.art/", "2026-10-12T17:31:19+00:00")
    assert run(world) == 0
    assert capsys.readouterr().out == EXPECTED_STDOUT
    assert without_time(rows("profiles")) == [
        profile_row(ALPHA, "https://zz-fake-alpha.art/"),
        profile_row(BRAVO, "https://www.artstation.com/zz-fake-bravo"),
        profile_row(FOXTROT, "https://www.artstation.com/zz-fake-foxtrot"),
    ]
    assert "CHECK" in raw("SELECT sql FROM sqlite_master WHERE name = 'profiles'")[0][0]
    stored = config.LOCAL_DB.read_bytes()
    assert b"wa.me" not in stored and b"555 0199" not in stored and b"zz-fake-wrong-column" not in stored


# --- 7. output and credentials ---


def test_stdout_is_counts_and_nothing_else(capsys):
    world = base()
    assert run(world) == 0
    out = capsys.readouterr().out
    assert summary(out) and len(out.splitlines()) == len(collect._COUNTS) == 48
    assert not re.search(r"FAKE|zz-fake|https?:|deviantart|wixmp|[0-9A-F]{8}-", out, re.IGNORECASE)
    for word in ("artists processed", "profiles stored", "works stored", "images downloaded", "skipped"):
        assert word in out


def test_credentials_and_tokens_are_never_in_a_url_in_the_output_or_in_a_log(capsys, caplog):
    world = base()
    world.detours.append(rate_limited(GALLERY_PATH, 1))
    assert run(world) == 0
    captured = capsys.readouterr()
    logged = " ".join(record.getMessage() for record in caplog.records)
    for secret in (CLIENT_ID, CLIENT_SECRET, "FAKE-TOKEN"):
        assert secret not in captured.out + captured.err + logged
        assert all(secret not in str(request.url) for request in world.requests())
        assert secret.encode() not in config.LOCAL_DB.read_bytes()
    token_request = world.requests("api")[0]
    assert (token_request.method, token_request.url.path, token_request.url.query) == ("POST", TOKEN_PATH, b"")
    assert CLIENT_SECRET in token_request.content.decode()  # in the body of the one request it belongs in
    assert all(CLIENT_SECRET not in str(request.headers) + request.content.decode() for request in world.requests()[1:])
    assert caplog.records == []


def test_main_reads_the_two_names_from_env_and_never_prints_a_value(monkeypatch, capsys):
    called = []
    monkeypatch.setattr(collect, "collect", lambda *args, **kwargs: called.append((args, kwargs)) or 0)
    env = {"DEVIANT_ART_ID": CLIENT_ID, "DEVIANT_ART_SECRET": CLIENT_SECRET, "OTHER": "FAKE-OTHER"}
    monkeypatch.setattr(config, "load_env", lambda: env)
    assert collect.main(["--max-artists", "5"]) == 0
    assert called == [((CLIENT_ID, CLIENT_SECRET), {"max_artists": 5})]
    assert collect.main([]) == 0 and called[1][1] == {"max_artists": 200}
    assert capsys.readouterr() == ("", "")

    monkeypatch.setattr(config, "load_env", lambda: {"DEVIANT_ART_ID": CLIENT_ID, "DEVIANT_ART_SECRET": ""})
    assert collect.main([]) == 1
    captured = capsys.readouterr()
    assert captured.err == "Missing in .env: DEVIANT_ART_SECRET (see .env.example)\n" and captured.out == ""

    def no_file():
        raise FileNotFoundError("FAKE-PATH")

    monkeypatch.setattr(config, "load_env", no_file)
    assert collect.main([]) == 1
    assert capsys.readouterr().err == "Missing in .env: DEVIANT_ART_ID, DEVIANT_ART_SECRET (see .env.example)\n"
    assert len(called) == 2


# --- 8. a work whose image the artist replaces: a run, a change, a run ---
#
# The first critic of the push stage: "Collect keeps the old cached image and stores the new thumbnail address."
# Every reading of the old image then stood for the new one.


def replace_image(world, username, n, m, served=True):
    """The artist puts another file under work n: its preview and its thumbnails are file m's from now on."""
    entry = next(entry for entry in world.galleries[username.lower()] if entry["deviationid"] == uuid(n))
    entry["preview"] = {"src": f"{CDN}{preview_path(m)}?{signed(m)}", "height": 600, "width": 800}
    entry["thumbs"] = [{"src": thumb(m, size), "height": size, "width": size} for size in (150, 200, 300)]
    if served:
        world.images[preview_path(m)] = jpeg(m)


def test_a_thumbnail_signed_again_and_cut_to_another_size_is_the_same_image_and_nothing_is_fetched(capsys):
    world = solo(3)
    assert run(world) == 0
    first, seen = library.image_prints(), len(world.log)
    assert first == {uuid(n): library.fingerprint(jpeg(n)) for n in (1, 2, 3)}
    for n, entry in enumerate(world.galleries[ALPHA], start=1):  # what every later answer of the real API does
        again = thumb(n, 400).replace("FAKE-CLAIMS-", "FAKE-CLAIMS-LATER-").replace("/f/", "/i/")
        entry["thumbs"] = [{"src": again, "height": 400, "width": 400}]
    counts = counts_of(world, capsys)
    assert counts[collect.CACHED] == 3 and counts[collect.DOWNLOADED] == 0 and world.since(seen, "images") == []
    assert all("FAKE-CLAIMS-LATER-" in row["thumb_url"] for row in rows("works"))  # the address is up to date
    assert library.image_prints() == first  # and every reading of these images stands


def test_an_artist_who_replaces_a_works_image_has_the_new_one_fetched_in_its_place(capsys):
    world = solo(3)
    assert run(world) == 0
    first = library.image_prints()
    replace_image(world, ALPHA, 2, 902)
    seen = len(world.log)

    counts = counts_of(world, capsys)

    assert counts[collect.DOWNLOADED] == 1 and counts[collect.CACHED] == 2 and counts[collect.WORKS] == 3
    assert [request.url.path for request in world.since(seen, "images")] == [preview_path(902)]
    assert (library.image_dir() / f"{uuid(2)}.jpg").read_bytes() == jpeg(902) and cached() == image_files([1, 2, 3])
    assert next(row for row in rows("works") if row["deviation_id"] == uuid(2))["thumb_url"] == thumb(902)
    # The image the work has now is another one: whatever was read from the old one is stale by itself.
    assert library.image_prints() == first | {uuid(2): library.fingerprint(jpeg(902))}
    assert rows("work_images")[1]["source"] == library.own_file(thumb(902)) != library.own_file(thumb(2))
    seen = len(world.log)
    assert counts_of(world, capsys)[collect.CACHED] == 3 and world.since(seen, "images") == []  # and once only


@pytest.mark.parametrize("answer", [404, 503, "not an image"])
def test_a_replaced_image_that_cannot_be_had_leaves_the_work_as_it_was_kept_image_and_all(capsys, answer):
    world = solo(3)
    assert run(world) == 0
    before, kept_rows, images = stored(), (without_time(rows("works")), rows("work_images")), library.image_prints()
    replace_image(world, ALPHA, 2, 902, served=answer == "not an image")
    if answer == 503:
        world.detours.append(lambda request: httpx.Response(503) if request.url.path == preview_path(902) else None)
    elif answer == "not an image":
        world.images[preview_path(902)] = b"FAKE-BYTES-THAT-BEGIN-LIKE-NO-IMAGE"

    counts = counts_of(world, capsys, expect_status=int(answer == 503))

    assert counts[collect.WORKS] == 2 and counts[collect.DOWNLOADED] == 0 and nothing_lost(before)
    assert (library.image_dir() / f"{uuid(2)}.jpg").read_bytes() == jpeg(2)  # the old image, whole
    # The work is as it was: its old thumbnail beside its old image, and the readings of that image still its own.
    assert (without_time(rows("works"))[1], rows("work_images"), library.image_prints()) == (
        kept_rows[0][1],
        kept_rows[1],
        images,
    )
    world.detours.clear()
    world.images[preview_path(902)] = jpeg(902)  # the host serves it after all: the next run has it
    assert counts_of(world, capsys)[collect.DOWNLOADED] == 1
    assert library.image_prints()[uuid(2)] == library.fingerprint(jpeg(902))


def test_a_store_made_before_readings_carried_fingerprints_stops_the_run_before_a_request_is_made():
    world = solo(2)
    assert run(world) == 0
    raw("PRAGMA user_version = 0")  # as every store was before the migration
    before, seen = config.LOCAL_DB.read_bytes(), len(world.log)
    with pytest.raises(SystemExit) as stopped:
        run(world)
    assert "uv run python -m pipeline.tagging --migrate --backup" in str(stopped.value)
    assert config.LOCAL_DB.read_bytes() == before and world.since(seen) == [] and cached() == image_files([1, 2])


# --- 9. what a profile's website field is taken for ---
#
# Discovery read every candidate's profile for this one field, and enforced these rules itself. It reads no
# profile now and stores a candidate straight from its tally, so they are enforced here, where the profile is
# read: the removal list is asked about every host the field names, every such host is recorded for the
# shared-domain rule, and a link reaches the store's row only when the field is, as written, the front page of a
# domain of the artist's own. The made-up domains sit under real suffixes (.art, .com, .co.uk) because the
# own-site rule reads the Public Suffix List and turns .example and .test away. Nothing ever fetches them.


def crowd(websites):
    """A world of one artist per website, each with a profile and no works, and their handles in key order."""
    world = World()
    names = [f"zz-fake-n{i:02d}" for i in range(len(websites))]
    for name, website in zip(names, websites, strict=True):
        world.artist(name, website=website)
    return world, names


def links():
    """The link in each stored artist's row, in key order: None for a row that has none."""
    return [(json.loads(found) or [None])[0] for (found,) in raw("SELECT links FROM artists ORDER BY key")]


# Every way of writing a host that is DeviantArt's, or an image host, a social network or a hosted platform.
NOT_THEIR_OWN_SITE = [
    "https://www.deviantart.com/zz-fake/gallery",
    "https://DeviantArt.com/zz-fake",
    "zz-fake.deviantart.com",
    "https://www.deviantart.com./zz-fake/art/FAKE-TITLE-99",  # a trailing dot
    "https://www.deviantart.com../zz-fake/art/FAKE-TITLE-99",
    "https://www.deviantart。com/zz-fake/art/FAKE-TITLE-98",  # an ideographic full stop
    "https://www.deviantart.com．/zz-fake/art/FAKE-TITLE-98",  # a fullwidth one, trailing
    "https://www.deviantart%2Ecom/zz-fake/art/FAKE-TITLE-97",  # a percent-encoded dot
    "https://www.deviantart.com\\.zz-fake-site.art/",  # a backslash, which browsers read as a slash
    "https://www.Ｄeviantart.com/zz-fake",  # a fullwidth letter
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
    assert collect._external_links(website) == []


def test_no_spelling_of_a_platform_host_reaches_the_stores_row(capsys):
    world, names = crowd(NOT_THEIR_OWN_SITE)
    counts = counts_of(world, capsys)
    assert counts[collect.PROFILES] == len(names) and links() == [None] * len(names)  # each stored, none with a link
    # All of them count as a platform but one: the backslash spelling also names a domain that is not one.
    assert (counts[collect.ON_PLATFORM], counts[collect.OTHER_SHAPE]) == (len(names) - 1, 1)
    assert counts[collect.OWN_SITE] == counts[collect.NO_WEBSITE] == counts[collect.NO_HOST] == 0
    assert store.own_site_candidates() == []


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
    assert collect._external_links(website) == []
    assert collect._external_links("https://zz-fake-site.art") == ["https://zz-fake-site.art/"]  # the shape decides


def test_profile_shaped_links_are_dropped_whole_and_never_trimmed_to_the_root(capsys):
    """The library keeps a website with its query, its fragment or its port cut off, and that is what this stage
    wrote into the store's row once discovery's own link was no longer the last word. Whose site a site is
    is decided on the field as it is written."""
    world, names = crowd(PROFILE_SHAPED)
    counts = counts_of(world, capsys)
    assert counts[collect.PROFILES] == counts[collect.OTHER_SHAPE] == len(names)
    assert links() == [None] * len(names)  # stored with no link: not the bare domain
    cut = [row["website"] for row in rows("profiles") if row["website"]]
    assert len(cut) > 3 and set(cut) <= {"https://zz-fake-site.art/", "http://zz-fake-site.art/"}  # not a hollow check


@pytest.mark.parametrize(
    ("website", "kept_links"),
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
        ("https://www.zz-fake-site.art。/", ["https://www.zz-fake-site.art/"]),  # an ideographic one
        ("https://zz-fake-site。art", ["https://zz-fake-site.art/"]),
        ("https://zz-fake-site.art．", ["https://zz-fake-site.art/"]),  # a fullwidth one
        ("https://zz-fäke-site.art", ["https://xn--zz-fke-site-o8a.art/"]),
        ("https://zz-fake-straße.de", ["https://xn--zz-fake-strae-mdb.de/"]),  # spelled as browsers spell it
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
        ("https://zz-fake‍-site.art", []),
        ("https://zz-fake-site.art/" + "x" * 300, []),
        ("zz-fake-site.art" + " " * 300, []),
    ],
)
def test_only_the_front_page_of_their_own_domain_is_kept(website, kept_links):
    assert collect._external_links(website) == kept_links


def test_a_lone_surrogate_in_the_website_field_is_dropped_without_raising():
    """JSON can carry half a surrogate pair, and URL parsers choke on it. One odd profile must not end a turn."""
    half = chr(0xD800)
    for website in (half + "zz-fake-site.art", "https://zz-fake-site.art/" + half, f"https://zz-fake{half}-site.art"):
        assert collect._external_links(website) == []


def test_whatever_the_website_field_holds_a_kept_link_is_a_canonical_front_page():
    """A seeded fuzz. No input may raise, and only scheme://host/ of an own site comes out."""
    odd = [chr(code) for code in (0x00, 0x09, 0x0A, 0xDF, 0xE4, 0x130, 0x200D, 0x3002, 0xFF0E, 0xFF24, 0xFF61, 0xD800)]
    plain = "https:// http:// HTTPS:// ftp:// // : / ? # @ . .. www. zz-fake-site art com co.uk github.io deviantart"
    plain += " example - _ %2E %40 \\ [ ] ::1 127.0.0.1 :443 :8443 a=b xn-- localhost ; zz-fake-site.art"
    pieces = [*plain.split(), " ", "www.zz-fake-site.art", "https://zz-fake-site.co.uk", *odd]
    rng = random.Random(20261012)
    found = 0
    for _ in range(20_000):
        website = "".join(rng.choice(pieces) for _ in range(rng.randint(1, 7)))
        for link in collect._external_links(website):
            found += 1
            host = link.removeprefix("https://").removeprefix("http://").removesuffix("/")
            assert re.fullmatch(r"https?://[a-z0-9.-]+/", link), ascii(website)
            assert store.canonical_host(host) == host, ascii(website)
            assert crawl.is_own_site(link), ascii(website)
            assert store.site_key(link) in (f"site:{host}", f"site:{host.removeprefix('www.')}"), ascii(website)
    assert found > 100  # the fuzz does reach the kept branch


@pytest.mark.parametrize(
    ("website", "count"),
    [
        ("", collect.NO_WEBSITE),
        ("   ", collect.NO_WEBSITE),
        (None, collect.NO_WEBSITE),
        (12345, collect.NO_WEBSITE),
        ("https://www.deviantart.com/zz-fake/gallery", collect.ON_PLATFORM),
        ("https://www.deviantart.com./zz-fake/art/FAKE-TITLE-99", collect.ON_PLATFORM),
        ("https://www.artstation.com/zz-fake", collect.ON_PLATFORM),  # a listed name, in any label
        ("zz-fake.tumblr.com", collect.ON_PLATFORM),
        ("https://www.pinterest.de/zz-fake", collect.ON_PLATFORM),
        ("https://zz-fake.github.io", collect.ON_PLATFORM),
        ("https://x.com/zz-fake", collect.ON_PLATFORM),  # a listed domain
        ("fav.me/dfake", collect.ON_PLATFORM),
        ("https://zz-fake.cargo.site", collect.ON_PLATFORM),
        ("https://zz-fake-site.art/folio", collect.OTHER_SHAPE),  # a path
        ("https://shop.zz-fake-site.art", collect.OTHER_SHAPE),  # a subdomain
        ("https://zz-fake-site.art/?u=zz-fake", collect.OTHER_SHAPE),  # a query
        ("https://zz-fake-site.art/#/zz-fake", collect.OTHER_SHAPE),  # a fragment
        ("https://zz-fake-site.co.uk:8443", collect.OTHER_SHAPE),
        ("<https://zz-fake-site.art>", collect.OTHER_SHAPE),
        ("zz-fake-site.art (commissions open)", collect.OTHER_SHAPE),
        ("FAKE-TAGLINE free text", collect.NO_HOST),
        ("https://localhost/x", collect.NO_HOST),
        ("https://127.0.0.1", collect.NO_HOST),
        ("https://zz-fake-site.example", collect.NO_HOST),  # no registrable domain, and on no list
        ("https://zz-fake-site.test/folio", collect.NO_HOST),
    ],
)
def test_a_profile_with_no_link_to_keep_is_counted_under_exactly_one_reason(website, count):
    assert collect._external_links(website) == []
    assert collect._why_no_link(website, store.hosts_in(website)) == count


def test_the_summary_says_which_of_five_kinds_each_stored_profiles_website_was_and_prints_counts_only(capsys):
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
    world, names = crowd(websites)
    assert run(world) == 0
    out, err = capsys.readouterr()
    counts = summary(out)
    assert err == ""
    assert counts[collect.NO_WEBSITE] == 1
    assert counts[collect.ON_PLATFORM] == 2
    assert counts[collect.OTHER_SHAPE] == 3
    assert counts[collect.NO_HOST] == 1
    assert counts[collect.OWN_SITE] == 1
    kinds = (collect.OWN_SITE, collect.NO_WEBSITE, collect.ON_PLATFORM, collect.OTHER_SHAPE, collect.NO_HOST)
    assert sum(counts[kind] for kind in kinds) == counts[collect.PROFILES] == len(names)  # each profile under one
    assert list(counts)[1:7] == [collect.PROFILES, *kinds]  # "of those" comes right after what it is a part of
    for told in ("zz-fake", "artstation", "folio", "free text", ".art", "http", "tagline"):
        assert told not in out.lower()  # a count says how many, never who or where
    assert links() == [None] * 7 + ["https://zz-fake-kept.art/"]  # all stored, one with a link
    kept_rows = str(raw("SELECT * FROM artists"))
    assert "fake-three" not in kept_rows and "fake-four" not in kept_rows and "fake-five" not in kept_rows


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


FRONT_PAGE_SPELLINGS = [  # each is the front page of the removed site, in a spelling the own-site rule accepts
    "https://zz-fake-d.art",
    "https://zz-fake-d.art./",  # a trailing dot
    "https://www.zz-fake-d.art.",
    "zz-fake-d.art.",
    "HTTPS://WWW.ZZ-Fake-D.Art/",
    "http://zz-fake-d.art:80",
    "https://zz-fake-d。art/",
    "https://www.zz-fake-d.art．",
]


@pytest.mark.parametrize("spellings", [FRONT_PAGE_SPELLINGS, odd_spellings("zz-fake-d.art")], ids=["plain", "odd"])
def test_a_removed_site_is_recognised_however_the_website_field_spells_it(spellings, capsys):
    """Hard rule 7. The removal list is asked about every host the field names, before the own-site rule gets to
    drop the link. An artist whose profile names a removed site is marked and held back, and nothing of theirs
    is collected. That is one answer's word, so nobody is barred for it, and the next answer is read afresh."""
    assert store.mark_removed("site:zz-fake-d.art") == 0
    world, names = crowd(spellings)
    counts = counts_of(world, capsys)
    assert counts[collect.LISTS_REMOVED] == len(names) and counts[collect.PROFILES] == 0  # in every spelling
    assert artist_marks() == dict.fromkeys(map(key, names), "lists a removed site") and library.artist_keys() == []
    assert len(world.requests()) == 1 + len(names)  # a token and a profile each: nothing more is asked of them
    assert not any(store.is_removed(name) for name in names) and len(raw("SELECT * FROM removed")) == 1
    assert links() == [None] * len(names) and b"zz-fake-d" not in config.LOCAL_DB.read_bytes().lower()
    for n, name in enumerate(names):  # the next run: each shows a clean site of their own, and is stored like anybody
        world.profiles[name]["website"] = f"https://zz-fake-new-{n:02d}.art"
    counts = counts_of(world, capsys)
    assert counts[collect.PROFILES] == len(names) and artist_marks() == {}
    assert links() == [f"https://zz-fake-new-{n:02d}.art/" for n in range(len(names))]


def test_a_removed_site_with_a_name_outside_ascii_is_recognised_too(capsys):
    name = "zz-f" + chr(0xE4) + "ke-d.art"  # a-umlaut: xn--zz-fke-d-... once encoded
    assert store.mark_removed(f"site:{name}") == 0
    spellings = [f"<https://{name}/folio>", f"{name.upper()} (commissions open)", f"https://www.{name}/?page=2"]
    world, names = crowd(spellings)
    counts = counts_of(world, capsys)
    assert counts[collect.LISTS_REMOVED] == 3 and library.artist_keys() == []
    assert not any(store.is_removed(name) for name in names)


def test_only_the_website_field_is_read_for_hosts_never_the_bio_or_the_rest_of_the_profile(capsys):
    """Reading hosts out of DeviantArt text is not allowed, so the removal of a site that a bio names touches
    nobody."""
    store.mark_removed("site:zz-fake-bio-link.art")
    world = base()
    for body in world.profiles.values():
        if isinstance(body, dict):
            body["bio"] = "FAKE-BIO with a link to a bare domain, https://zz-fake-bio-link.art which is never read"
            body["last_status"] = {"body": "FAKE-STATUS see zz-fake-bio-link.art"}
    assert run(world) == 0
    assert capsys.readouterr() == (EXPECTED_STDOUT, "")  # nobody marked for it, and everybody collected as ever
    assert "lists a removed site" not in artist_marks().values()


# The seeding list never offers the crawl stage a domain that several profiles list.
FRONT, PATH, SUBDOMAIN = "https://zz-fake-plat.art/", "https://zz-fake-plat.art/u2", "https://u3.zz-fake-plat.art"


def test_a_domain_listed_by_three_profiles_is_nobodys_own_and_the_two_listings_not_kept_are_hashes(capsys):
    """The front page is a bare domain and is kept. The other two listings have a profile's shape and are
    dropped, but they are counted: three profiles naming one domain make it a platform, whatever any list says."""
    world, names = crowd([FRONT, PATH, SUBDOMAIN, "https://zz-fake-solo.art"])
    counts_of(world, capsys)
    assert links() == [FRONT, None, None, "https://zz-fake-solo.art/"]
    assert store.own_site_candidates() == [(key(names[3]), "https://zz-fake-solo.art/")]
    kept_rows = str(raw("SELECT * FROM artists")) + str(rows("profiles"))
    assert "u2" not in kept_rows and "u3" not in kept_rows
    assert len(raw("SELECT * FROM listed")) == 4  # three for the one domain, and one for the other


@pytest.mark.parametrize("second_listing", [PATH, SUBDOMAIN, "https://www.zz-fake-plat.art/?u=u4", f"<{FRONT}>"])
def test_a_domain_stops_being_a_candidate_when_a_later_run_finds_a_second_listing(second_listing, capsys):
    world, names = crowd([FRONT, "", ""])
    counts_of(world, capsys)
    assert store.own_site_candidates() == [(key(names[0]), FRONT)]  # one artist, one domain: a candidate
    world.profiles[names[1]]["website"] = second_listing
    counts_of(world, capsys)
    assert store.own_site_candidates() == [] and links() == [FRONT, None, None]


def test_a_domain_listed_in_an_earlier_run_is_not_a_candidate_when_its_front_page_turns_up(capsys):
    world, names = crowd(["", PATH, SUBDOMAIN])
    counts_of(world, capsys)
    kept_rows = str(raw("SELECT * FROM artists")) + str(rows("profiles"))
    assert links() == [None] * 3 and "plat" not in kept_rows  # of the two listings nothing is kept but hashes
    world.profiles[names[0]]["website"] = FRONT
    counts_of(world, capsys)
    assert links()[0] == FRONT and store.own_site_candidates() == []


def test_the_same_artist_listing_their_domain_twice_is_still_one_listing(capsys):
    world, names = crowd([FRONT, ""])
    counts_of(world, capsys)
    world.profiles[names[0]]["website"] = "https://www.zz-fake-plat.art"  # another spelling of the one front page
    counts_of(world, capsys)
    assert store.own_site_candidates() == [(key(names[0]), "https://www.zz-fake-plat.art/")]


# --- 10. the image host has a pace of its own, and it is the run's ---


def test_the_image_host_asking_us_to_slow_down_slows_every_later_download_and_not_the_api(capsys):
    """The same flaw as the API's had, and the same rule in its place: a 429 from the image host was backed off
    from for that one image, and the next image was asked for half a second later, as if nothing had been said."""
    world, left = trio(), [2]

    def twice(request):  # the second image asked for is refused twice, and then served
        if request.url.path == preview_path(3) and left[0]:
            left[0] -= 1
            return httpx.Response(429, text="FAKE-SLOW-DOWN")

    world.detours.append(twice)
    counts = counts_of(world, capsys)
    assert kept(ALPHA) == [4, 3, 2, 1] and counts[collect.DOWNLOADED] == 6 and counts[collect.IMAGE_LIMITED] == 0
    waited = world.waited("images")
    assert waited[:4] == [None, 0.5, 1.0, 2.0]  # the first image, then work 3: asked for, and asked again twice
    assert waited[4:] == pytest.approx([1.8, 1.62, 1.458, 1.3122])  # the works after it, the other artists' too
    assert waited[4] > collect.IMAGE_INTERVAL  # the next image, which is another one
    assert world.waited("api") == [None] + [2.0] * 9  # and the API, which said nothing, is asked as before
    assert counts["rate limit answers from the image host"] == 2 and counts["rate limit answers from the API"] == 0
    assert counts["longest interval between requests to the image host, in seconds"] == 2
    assert counts["requests to the image host"] == 8 and counts["requests to the API"] == 10
    assert counts["seconds spent waiting between requests to the image host"] == round(sum(waited[1:]))


def test_the_image_hosts_interval_comes_down_a_tenth_an_image_to_half_a_second_and_no_lower():
    world = solo(10)
    world.detours.append(rate_limited(preview_path(9), 1))
    assert run(world) == 0 and kept(ALPHA) == list(range(10, 0, -1))
    waited = world.waited("images")
    assert waited[:3] == [None, 0.5, 1.0]  # work 10, work 9, and work 9 asked for again
    assert waited[3:] == pytest.approx([max(0.5, 0.9**n) for n in range(1, 9)]) and waited[-2:] == [0.5, 0.5]


def test_a_retry_after_from_the_image_host_is_waited_when_it_asks_for_longer():
    world, left = solo(3), [1]

    def once(request):
        if request.url.path == preview_path(2) and left[0]:
            left[0] = 0
            return httpx.Response(429, headers={"Retry-After": "30"}, text="FAKE-SLOW-DOWN")

    world.detours.append(once)
    assert run(world) == 0 and kept(ALPHA) == [3, 2, 1]
    assert world.waited("images") == [None, 0.5, 30.0, pytest.approx(0.9)]


def test_a_collection_run_says_where_it_is_in_a_file_while_it_runs_and_the_file_goes_when_it_ends(capsys, tmp_path):
    """Counts only, as the summary prints them, before every request to either host: the first asking and every
    asking again. Nothing of it is on standard error, so a clean run's is still empty."""
    world, said = base(), []
    told = config.CACHE_DIR / "collect.status"
    world.detours.append(lambda request: said.append(told.read_text(encoding="utf-8")))
    world.detours.append(rate_limited(GALLERY_PATH, 1))
    assert run(world) == 0
    assert capsys.readouterr().err == "" and len(said) == BASE_REQUESTS + 1
    assert not told.exists() and "cache/collect.status" not in files(tmp_path)  # there only while a run is on
    for text in said:
        when, *lines = text.splitlines()
        assert datetime.now(UTC) - datetime.fromisoformat(when) < timedelta(minutes=1)
        assert summary("\n".join(lines))  # the summary's own lines, each a label and a number
        assert not re.search(r"FAKE|zz-fake|https?:|deviantart|wixmp|[0-9A-F]{8}-", text, re.IGNORECASE)
    assert "artists processed: 1\n" in said[0] and "requests to the API: 0\n" in said[0]  # the first artist's turn
    now = "interval between requests to the API now, in seconds"  # collecting, or waiting: this is how to tell
    assert [int(re.search(f"{now}: (\\d+)", text)[1]) for text in said[1:5]] == [2, 2, 4, 4]
    assert "rate limit answers from the API: 1\n" in said[3] and "artists processed: 1\n" in said[3]
    assert "requests to the image host: 4\n" in said[9] and "images downloaded: 4\n" in said[9]  # between two images
    last = summary("\n".join(said[-1].splitlines()[1:]))
    assert (last[collect.PROCESSED], last[collect.PROFILES], last[collect.DOWNLOADED]) == (5, 2, 13)
    assert last["requests to the image host"] == 13 and last["requests to the API"] == 11


# --- 11. the pace outlives the run, one run talks to a host at a time, and an hour of refusals ends it ---


def on_one_clock(world):
    """The pace reads the world's clock as well as sleeping on it, so that the time between two runs is on it too."""
    return mock.patch.object(discover, "now", lambda: world.now)


def test_a_run_begins_at_the_pace_the_last_one_ended_with_for_the_api_and_for_the_image_host(capsys):
    """The pace was the memory of one process, so a second run began at the base whatever the hosts had said to
    the first. Each host's pace is a file now, and the next run's first request waits a whole interval of it."""
    world = solo(4)
    world.detours.append(rate_limited(GALLERY_PATH, 3))
    world.detours.append(rate_limited(preview_path(3), 2))
    with on_one_clock(world):
        assert run(world) == 0
        assert world.waited("api")[-2:] == pytest.approx([16.0, 14.4])  # where the API's pace was when the run ended
        assert world.waited("images")[-2:] == pytest.approx([1.8, 1.62])  # and the image host's
        world.artist(BRAVO)  # discovery finds another artist, and the next run begins straight away
        world.work(BRAVO, 21)
        seen = len(world.requests("api"))
        assert counts_of(world, capsys)[collect.DOWNLOADED] == 1
    eased = [12.96 * 0.9**answered for answered in range(7)]  # on from where it was, a tenth off with every answer
    assert world.waited("api")[seen:] == pytest.approx(eased) and min(eased) > discover.BASE_INTERVAL
    assert world.waited("images")[-1] == pytest.approx(1.458)  # 1.62 and a tenth off: not half a second


def test_collection_begins_at_the_pace_discovery_ended_with(capsys):
    """The critic's counterexample: discovery's last gap was 5.8 s, and collection, begun in the same process,
    sent its first request 0 s later and its second 2 s after that."""
    world, asked, left = solo(1), [], [3]

    def no_works_under_any_tag(request):  # discovery's API, on the world's clock: its last page is refused three times
        asked.append(world.now)
        if request.url.path == TOKEN_PATH:
            return httpx.Response(200, json={"access_token": "FAKE-TOKEN-0"})
        if request.url.params["tag"] == discover.NICHE_TAGS[-1] and left[0]:
            left[0] -= 1
            return httpx.Response(429, json={"error": "FAKE-RATE-LIMIT"})
        return httpx.Response(200, json={"has_more": False, "next_offset": None, "results": []})

    with on_one_clock(world):
        with mock.patch.object(time, "sleep", world.sleep):
            transport = httpx.MockTransport(no_works_under_any_tag)
            assert discover.discover(CLIENT_ID, CLIENT_SECRET, max_artists=5, transport=transport) == 0
        assert asked[-1] - asked[-2] == 16.0  # discovery's last gap, and its pace then comes down a tenth: 14.4
        assert run(world) == 0
    assert world.log[0][0] - asked[-1] == pytest.approx(14.4)  # collection's first request, after discovery's last
    assert world.waited("api") == pytest.approx([14.4, 12.96, 11.664, 10.4976])
    assert world.waited("images") == [None]  # the image host, which discovery never asks, is asked as ever


@pytest.mark.parametrize("host", ["the API", collect.IMAGE_HOST])
def test_a_run_started_while_another_has_a_host_is_refused_and_touches_nothing(host, capsys):
    """The critic's counterexample: collection started while discovery was in the middle of a run, and both ran,
    at one API request a second between them, which is the rate that was limited. The second one stops now,
    before a request and before a sweep: a sweep would take what the other has downloaded and not yet stored
    for files that no row accounts for, and the end of a run takes away the file that says a run is on."""
    world = base()
    assert run(world) == 0
    capsys.readouterr()
    unstored = library.image_dir() / f"{uuid(77)}.jpg"  # as the other run has it: downloaded, its row not written yet
    unstored.write_bytes(jpeg(77))
    discover.tell("collect", {collect.PROCESSED: 1})  # and where the other run says it is
    store.mark_removed(BRAVO)
    seen = len(world.log)
    with discover.Pace(collect.IMAGE_INTERVAL, host):  # the other run, which holds this host
        assert run(world) == 1
    captured = capsys.readouterr()
    assert captured.err == f"stopped: another run is talking to {host}, and one at a time is the rule\n"
    assert summary(captured.out)[collect.PURGED] == 0 and world.since(seen) == []  # no request, to either host
    assert unstored.exists() and (config.CACHE_DIR / "collect.status").exists() and kept(BRAVO) == BRAVO_KEPT
    discover.tell("collect")  # the other run ends, and the next one is let in and does it all
    counts = counts_of(world, capsys)
    assert counts[collect.PURGED] == 1 and counts[collect.ORPHANS] == 1 and kept(BRAVO) == []


@pytest.mark.parametrize(
    ("status", "counted"),
    [(429, "rate limit answers from the image host"), (503, "server errors or no answer from the image host")],
)
def test_an_image_host_that_serves_every_image_at_the_fourth_asking_is_stopped_at_after_an_hour_of_it(
    status, counted, capsys
):
    """The critic's counterexample: every image refused three times and served the fourth: twelve images in 3.0
    hours, no stop, exit 0 and nothing on standard error, because each image served set the count back to
    nothing. The image host's hour is counted as the API's is, and one image among the refusals does not end it.
    And the round after, with 503 for the refusal: a 5XX counted for nothing towards the hour, and the summary
    said "rate limit answers from the image host: 0". It counts as a 429 does now, and the summary has it."""
    world, names = two_works_each(6)
    left = {}

    def three_refusals_then_served(request):
        if request.url.host == CDN_HOST and left.setdefault(request.url.path, 3):
            left[request.url.path] -= 1
            return httpx.Response(status, text="FAKE-SLOW-DOWN")

    world.detours.append(three_refusals_then_served)
    assert run(world) == 1
    captured = capsys.readouterr()
    assert captured.err == f"stopped: rate limited: the image host kept refusing through {WAITS}. {KEPT}\n"
    climb = [None, 1.0, 2.0, 4.0, 3.6, 7.2, 14.4, 28.8, 25.92, 51.84, 103.68, 207.36, 186.624]  # three images
    at_the_ceiling = [300.0, 300.0, 300.0, 270.0]  # three refusals five minutes apart, and the image, served
    assert world.waited("images") == pytest.approx(climb + at_the_ceiling * 4)  # twelve waits for a refusal there
    counts = summary(captured.out)
    assert counts[collect.DOWNLOADED] == 7 and counts[counted] == 22
    assert sum(world.waited("images")[1:]) == pytest.approx(5316.4, abs=0.1)  # 89 minutes, and not 3 hours and on
    # Everything stored is kept: three artists whole. The fourth, whom the run stopped at, is noted as passed over.
    assert library.artist_keys() == list(map(key, names[:3])) and len(cached()) == 6
    assert key(names[3]) in library.last_attempts()


def test_a_retry_after_of_an_hour_on_every_429_from_the_image_host_stops_the_run_after_that_hour(capsys):
    """The critic's counterexample: with `Retry-After: 3600` on every 429 the image host was asked for 11.0 hours."""
    world = solo(4)
    world.detours.append(
        lambda request: (
            httpx.Response(429, headers={"Retry-After": "3600"}, text="FAKE-SLOW-DOWN")
            if request.url.host == CDN_HOST
            else None
        )
    )
    assert run(world) == 1
    assert capsys.readouterr().err == f"stopped: rate limited: the image host kept refusing through {WAITS}. {KEPT}\n"
    assert world.waited("images") == [None, 3600.0] and library.artist_keys() == []


def test_a_retry_after_on_a_5xx_from_the_image_host_is_waited_as_on_a_429():
    """The critic's thirty-fifth break, which no test caught: Retry-After read on a 429 and not on a 5XX."""
    world, left = solo(3), [1]

    def once(request):
        if request.url.path == preview_path(2) and left[0]:
            left[0] = 0
            return httpx.Response(503, headers={"Retry-After": "30"}, text="FAKE-DOWN")

    world.detours.append(once)
    assert run(world) == 0 and kept(ALPHA) == [3, 2, 1]
    assert world.waited("images") == [None, 0.5, 30.0, pytest.approx(0.9)]


def test_a_retry_after_too_long_to_be_a_number_ends_no_run_and_a_removal_during_it_is_honoured(capsys):
    """The critic's counterexample: a Retry-After of 400 digits became infinity, rounding it up for the summary
    raised inside the `finally`, and the sweep after it never ran: an artist removed during the run still had
    a profile, a work and an image when the run exited. Such a value says nothing now, and is not read."""
    world, left = trio(), [1]
    assert run(world) == 0

    def removed_then_refused(request):
        if request.url.path == PROFILE_PATH + ALPHA and left[0]:
            left[0] = 0
            store.mark_removed(BRAVO)  # a removal lands while the run is on
            return httpx.Response(429, json={"error": "FAKE-RATE-LIMIT"}, headers={"Retry-After": "9" * 400})

    world.detours.append(removed_then_refused)
    seen = len(world.requests("api"))
    counts = counts_of(world, capsys)  # which exits 0, with nothing on standard error
    assert world.waited("api")[seen : seen + 4] == pytest.approx([2.0, 2.0, 4.0, 3.6])  # the doubled interval
    assert counts[collect.REMOVED] == counts[collect.PURGED] == 1 and kept(BRAVO) == []
    assert library.artist_keys() == [key(ALPHA), key(DELTA)] and cached() == image_files([1, 2, 3, 4, 41])
    assert not (config.CACHE_DIR / "collect.status").exists()


def test_an_error_in_the_pace_itself_skips_neither_the_closing_sweep_nor_the_end_of_the_status_file(
    monkeypatch, capsys
):
    """Whatever goes wrong where the run's closing lines read the pace, the two things after them still happen:
    the file that says a run is on goes, and a removal that landed during the run is honoured before exit."""
    world = trio()
    assert run(world) == 0
    capsys.readouterr()
    counts, calls = discover.Pace.counts, []

    def breaks(pace):
        calls.append(pace)
        if len(calls) > 4:  # two requests in: the file that says where the run is has been written
            store.mark_removed(BRAVO)
            raise OverflowError("FAKE-OVERFLOW")
        return counts(pace)

    monkeypatch.setattr(discover.Pace, "counts", breaks)
    assert run(world) == 1
    assert "stopped: OverflowError at collect.py line " in capsys.readouterr().err
    assert not (config.CACHE_DIR / "collect.status").exists()
    assert key(BRAVO) not in library.artist_keys() and kept(BRAVO) == [] and cached() == image_files([1, 2, 3, 4, 41])
