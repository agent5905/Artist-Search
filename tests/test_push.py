"""Tests for pipeline.push.

The public database is FakeDb, put in the place of pipeline.db: it keeps what it is given and notes every call.
The local stores are real ones in pytest's temporary directory, filled the way the other stages fill them.
No model, no GPU, no network: opening a real connection fails the test, and the real .env is never read.
Every handle, title, tag, tagline, address and image below is invented.
"""

import hashlib
import itertools
import json
import math
import re
import socket
import sqlite3
from array import array
from contextlib import closing
from urllib.parse import urlsplit

import numpy as np
import pytest

from pipeline import config, crawl, library, profiles, push, store, style, tagging

ALPHA, BRAVO, CHARLIE, DELTA = "zz-fake-alpha", "zz-fake-bravo", "zz-fake-charlie", "zz-fake-delta"
VERIFIED = tagging.result_set(profiles.TAGGER, profiles.TAGGER)
JPEG = b"\xff\xd8\xff" + b"FAKE-IMAGE-BYTES-" * 20
AT = "2026-01-01T00:00:00+00:00"
PUBLISHED = 1_780_000_000  # work 0 of every artist; each later work is a day older
FACTS = {
    "specialties": ["character"],
    "own_words": ["dragons"],
    "mediums": ["digital painting"],
    "genres": ["fantasy"],
    "formats": ["portrait or bust"],
    "style_words": ["painterly"],
    "hedged": [],
}
ARTIST_FIELDS = {
    *("slug", "name", "da_username", "site_url", "site_domain", "tagline", "top_tags", "search_text"),
    *("profile", "specialties", "mediums", "open_for_work", "profile_embedding"),
}
WORK_FIELDS = {"artist_slug", "source", "page_url", "image_url", "title", "description", "tags", "position"}
NEIGHBOUR_FIELDS = {"artist_slug", "neighbor_slug", "rank", "score"}
COUNTS = [
    "artists_sent",
    *(f"artists_held_{reason}" for reason in push.ARTIST_REASONS),
    "works_sent",
    *(f"works_held_{reason}" for reason in push.WORK_REASONS),
    *("neighbours_sent", "artists_removed_from_database", "works_removed_from_database"),
    *("removals_applied_here", "artists_purged_here"),
]
_BASES: dict[str, int] = {}  # handle -> where its works' ids begin


class FakeDb:
    """Stands in for pipeline.db. It keeps every row it is given, as the real one would, and refuses nothing:
    whatever must not reach the database must not be sent to this."""

    def __init__(self):
        self.removed = []  # the key hashes in its `removed` table
        self.artists = {}  # id -> the row
        self.works = {}  # id -> the row, which names its artist by artist_slug
        self.neighbors = []
        self.calls = []  # "removed", "listing", "delete", "load", in the order they came
        self.loads = []  # what each load was given: [artists, works, neighbors]
        self.fail = None  # the call that fails
        self.error = RuntimeError("HTTP 500: FAKE-DATABASE-FAILURE")  # what pipeline.db raises
        self._ids = itertools.count(1)

    def _note(self, call):
        self.calls.append(call)
        if self.fail == call:
            raise self.error

    def sql(self, query):
        if query == push.REMOVED_SQL:
            self._note("removed")
            return [{"key_hash": key_hash} for key_hash in self.removed]
        if query == push.LISTING_SQL:
            self._note("listing")
            rows = []
            for artist_id, row in self.artists.items():
                mine = [(work_id, work) for work_id, work in self.works.items() if work["artist_slug"] == row["slug"]]
                rows += [
                    {"artist_id": artist_id, "slug": row["slug"], "work_id": work_id, "page_url": work["page_url"]}
                    for work_id, work in mine
                ] or [{"artist_id": artist_id, "slug": row["slug"], "work_id": None, "page_url": None}]
            return rows
        self._note("delete")
        statement = r"delete from public\.(works|artists) where id in \(([0-9, ]+)\)"
        assert re.fullmatch(f"{statement}(; {statement})?", query), "the only other SQL is a delete by id"
        for table, ids in re.findall(statement, query):
            for row_id in map(int, ids.split(", ")):
                if table == "works":
                    del self.works[row_id]
                else:  # an artist's works and neighbour rows go with them, as the foreign keys have it
                    slug = self.artists.pop(row_id)["slug"]
                    self.works = {key: work for key, work in self.works.items() if work["artist_slug"] != slug}
                    self.neighbors = [
                        row for row in self.neighbors if slug not in (row["artist_slug"], row["neighbor_slug"])
                    ]
        return []

    def load(self, artists, works, neighbors):
        self._note("load")
        self.loads.append(json.loads(json.dumps([artists, works, neighbors], allow_nan=False)))  # it must be JSON
        for row in artists:
            ids = [artist_id for artist_id, have in self.artists.items() if have["slug"] == row["slug"]]
            self.artists[ids[0] if ids else next(self._ids)] = dict(row)
        for row in works:
            key = (row["artist_slug"], row["page_url"])
            ids = [work_id for work_id, have in self.works.items() if (have["artist_slug"], have["page_url"]) == key]
            self.works[ids[0] if ids else next(self._ids)] = dict(row)
        named = {row["artist_slug"] for row in neighbors}
        self.neighbors = [row for row in self.neighbors if row["artist_slug"] not in named] + list(neighbors)

    def sent(self):
        """Everything any load was given, as one lowercase text to look for a word in."""
        return json.dumps(self.loads).lower()

    def held(self):
        """Everything the database holds now, as one lowercase text."""
        return json.dumps([list(self.artists.values()), list(self.works.values()), self.neighbors]).lower()


def unsynced(*args, connect=sqlite3.connect, **kwargs):
    """sqlite3.connect that does not wait for the disk after each commit. Nothing else about the database changes."""
    conn = connect(*args, **kwargs)
    conn.execute("PRAGMA synchronous = OFF")
    return conn


@pytest.fixture(autouse=True)
def fake(tmp_path, monkeypatch):
    monkeypatch.setattr(config, "CACHE_DIR", tmp_path / "cache")
    monkeypatch.setattr(config, "LOCAL_DB", tmp_path / "cache" / "pipeline.sqlite")
    monkeypatch.setattr(config, "load_env", lambda: {})  # the real .env is never read here
    monkeypatch.setattr(sqlite3, "connect", unsynced)

    def no_network(*args, **kwargs):
        raise AssertionError("a real connection was attempted")

    monkeypatch.setattr(socket.socket, "connect", no_network)
    monkeypatch.setattr(socket, "create_connection", no_network)
    database = FakeDb()
    monkeypatch.setattr(push, "db", database)
    return database


def digest(key):
    return hashlib.sha256(key.encode()).hexdigest()


def squashed(handle):
    return handle.replace("-", "").replace("_", "")


def style_vector(angle, n):
    numbers = np.zeros(style.DIMENSIONS, dtype=np.float32)
    numbers[:3] = math.cos(angle), math.sin(angle), 0.01 * n
    return (numbers / np.linalg.norm(numbers)).astype(np.float16).tobytes()


def artist(
    handle, count=5, *, profile="written", tagline_contact="no", website=None, links=None, angle=0.0, edits=None
):
    """Collect, tag, rate and profile one invented artist, and return their works, the most recent first.

    Every work is fit to send unless `edits` says otherwise for its place in the list: "result": None (no
    verified tags), "explicit" or "not_artwork": True, "declared" (AI: "yes", "unclear", None for not read),
    "contact" ("yes", None for not read), "rating" ("suggestive", "explicit", None for not rated), "thumb".
    profile: "written", "excluded" (the profile stage ruled them out) or None (no row there yet).
    Everything about the artist carries their handle, with or without its hyphens, so it can be looked for.
    """
    key, word = store.artist_key(handle), squashed(handle)
    base = _BASES.setdefault(handle, (len(_BASES) + 1) * 1000)
    edits = edits or {}
    made, rows = [], []
    for n in range(count):
        edit = edits.get(n, {})
        work = {
            "id": f"00000000-0000-4000-A000-{base + n:012d}",
            "page": f"https://www.deviantart.com/{handle}/art/FAKE-PAGE-{handle}-{n}",
            "title": f"FAKE-TITLE {handle} {n}",
            "tags": [f"zzown{word}{n}", f"zzall{word}"],
            "thumb": edit.get("thumb", f"https://images-wixmp-zzfake.wixmp.com/f/FAKE-THUMB-{handle}-{n}.jpg"),
            "description": f"FAKE-DESCRIPTION {handle} {n}.",
        }
        made.append(work)
        library.save_image(work["id"], "image/jpeg", JPEG + work["id"].encode())
        result = {
            "medium": "digital painting",
            "subjects": ["character"],
            "genres": ["fantasy", "realistic"],
            "style_words": ["painterly"],
            "description": work["description"],
            "explicit": edit.get("explicit", False),
            "not_artwork": edit.get("not_artwork", False),
        }
        if edit.get("result", result) is not None:
            rows.append(("work_tags", (work["id"], VERIFIED, tagging.PROMPT_VERSION, json.dumps(result), AT)))
        if declared := edit.get("declared", "no"):
            rows.append(("work_ai_declared", (work["id"], profiles.TAGGER, tagging.AI_PROMPT_VERSION, declared, AT)))
        if contact := edit.get("contact", "no"):
            rows.append(("work_contact", (work["id"], profiles.TAGGER, tagging.CONTACT_PROMPT_VERSION, contact, AT)))
        if rating := edit.get("rating", "safe"):
            sorted_as = json.dumps({"formats": ["portrait or bust"], "rating": rating})
            rows.append(("work_format", (work["id"], profiles.TAGGER, tagging.FORMAT_VERSION, sorted_as, AT)))
        rows.append(("style_vectors", (work["id"], "csd", style_vector(angle, n))))
    if tagline_contact:
        rows.append(("artist_contact", (key, profiles.TAGGER, tagging.CONTACT_PROMPT_VERSION, tagline_contact, AT)))
    if profile:
        vector = array("f", [1.0, *[0.0] * (profiles.DIMENSIONS - 1)]).tobytes()
        texts = (
            (None,) * 6 if profile == "excluded" else (f"FAKE-TEMPLATE {handle}.", 1, f"FAKE-PROFILE {handle}.", 1, 1)
        )
        texts = texts if profile == "excluded" else (*texts, vector)
        reason = "nothing_clear" if profile == "excluded" else None
        rows.append(("artist_profiles", (key, "FAKE-BASIS", reason, json.dumps(FACTS), "[]", *texts, AT)))

    own_site = bool(website) and crawl.is_own_site(website)
    assert store.upsert_artist(
        handle, f"https://www.deviantart.com/{handle}", [website] if links is None and own_site else links or [], 1
    )
    library.store_artist(
        key,
        user_id=f"FAKE-USER-{handle}",
        tagline=f"FAKE-TAGLINE {handle}",
        specialty="FAKE-SPECIALTY",
        level="Professional",
        website=website,
        works=[
            {
                "work_id": work["id"],
                "page_url": work["page"],
                "title": work["title"],
                "tags": work["tags"],
                "thumb_url": work["thumb"],
                "published": str(PUBLISHED - 86_400 * n),
            }
            for n, work in enumerate(made)
        ],
    )
    tagging.results(), profiles.profiles(), style.artist_vectors(key, "csd")  # so that their tables exist
    with closing(sqlite3.connect(config.LOCAL_DB)) as conn, conn:
        for table, row in rows:
            conn.execute(f"INSERT OR REPLACE INTO {table} VALUES ({', '.join('?' * len(row))})", row)
    return made


def rate(work, rating):
    """The tagging stage rates a work again."""
    sorted_as = json.dumps({"formats": [], "rating": rating})
    with closing(sqlite3.connect(config.LOCAL_DB)) as conn, conn:
        conn.execute("UPDATE work_format SET result = ? WHERE deviation_id = ?", (sorted_as, work["id"]))


def local_state():
    """Everything the local stores hold: every row of every table, and every cached image."""
    with closing(sqlite3.connect(config.LOCAL_DB)) as conn:
        rows = list(conn.iterdump())
    return rows, sorted((path.name, path.read_bytes()) for path in library.image_dir().iterdir())


def of_work(work):
    """Every text that is one work's own: none of them may be found where the work was held back."""
    return [text.lower() for text in (work["id"], work["page"], work["title"], work["tags"][0], work["thumb"])] + [
        work["description"].lower()
    ]


def of_artist(handle):
    """What everything about one invented artist carries: the handle, as it is and as their tags have it."""
    return [handle, squashed(handle)]


def sent_artists(fake):
    return {row["da_username"]: row for row in fake.loads[-1][0]}


def traced(fake, monkeypatch):
    """Note, among the fake database's calls, each time the push stage reads or changes a local store."""
    calls = (
        *((store, "import_removed"), (library, "purge"), (profiles, "profiles"), (profiles, "excluded")),
        *((tagging, "results"), (tagging, "contacts"), (push, "_collected"), (style, "neighbors")),
    )

    def note(module, name):
        real = getattr(module, name)

        def noted(*args, **kwargs):
            fake.calls.append(name)
            return real(*args, **kwargs)

        monkeypatch.setattr(module, name, noted)

    for module, name in calls:
        note(module, name)


BUILDS = {"profiles", "excluded", "results", "contacts", "_collected", "neighbors"}  # every read a row is built from


# Removal first (hard rule 7).


@pytest.mark.parametrize("dry_run", [False, True])
def test_a_removal_made_through_the_site_is_imported_before_any_row_is_built(fake, monkeypatch, dry_run):
    artist(ALPHA, angle=0.0)
    removed = artist(BRAVO, angle=0.01, website=f"https://{BRAVO}.art/")
    artist(CHARLIE, angle=0.5)
    fake.removed = [digest(f"da:{BRAVO}")]
    traced(fake, monkeypatch)

    counts = push.run(dry_run)

    assert fake.calls[:2] == ["removed", "import_removed"]
    first_build = min(fake.calls.index(name) for name in BUILDS)
    last_removal = max(place for place, name in enumerate(fake.calls) if name in ("import_removed", "purge"))
    assert "purge" in fake.calls and last_removal < first_build
    assert counts["removals_applied_here"] == 1 and counts["artists_purged_here"] == 1
    # Nothing of theirs is left here: no row, no image, and the account and the site they listed are barred.
    assert f"da:{BRAVO}" not in library.artist_keys()
    assert not any(path.name.startswith(work["id"]) for work in removed for path in library.image_dir().iterdir())
    assert store.is_removed(BRAVO) and store.is_removed(f"{BRAVO}.art")
    assert library.account_removed(f"FAKE-USER-{BRAVO}")
    assert [key for key, _, _ in store.all_artists()] == [f"da:{ALPHA}", f"da:{CHARLIE}"]
    if dry_run:
        assert fake.loads == [] and counts["artists_sent"] == 2
        return
    assert sorted(sent_artists(fake)) == [ALPHA, CHARLIE]
    assert not any(word in fake.sent() for word in of_artist(BRAVO))
    assert [row["neighbor_slug"] for row in fake.loads[-1][2] if row["artist_slug"] == ALPHA] == [CHARLIE]


@pytest.mark.parametrize("how", ["by_site", "site_only_the_library_knows", "here_before_the_push"])
def test_nothing_about_a_removed_artist_is_sent_and_what_was_sent_before_is_deleted(fake, how):
    site = f"https://www.{BRAVO}.art/"
    artist(ALPHA, angle=0.0)
    # The site is in the store's links, or it is only in the profile field the collect stage kept.
    artist(BRAVO, angle=0.01, website=site, links=[] if how == "site_only_the_library_knows" else None)
    artist(CHARLIE, angle=0.5)
    push.run(False)
    assert sorted(sent_artists(fake)) == [ALPHA, BRAVO, CHARLIE] and BRAVO in fake.held()

    if how == "here_before_the_push":
        store.mark_removed(BRAVO)
    else:
        fake.removed = [digest(f"site:{BRAVO}.art")]
    counts = push.run(False)

    assert sorted(sent_artists(fake)) == [ALPHA, CHARLIE]
    assert not any(word in json.dumps(fake.loads[-1]).lower() for word in of_artist(BRAVO))
    assert not any(word in fake.held() for word in of_artist(BRAVO))  # their row, their works, and as a neighbour
    assert counts["artists_removed_from_database"] == 1
    assert counts["artists_sent"] == 2 and counts["works_sent"] == 10 and counts["neighbours_sent"] == 2


def test_a_removal_list_that_cannot_be_applied_stops_the_run_before_anything_is_built(fake, monkeypatch, capsys):
    artist(ALPHA)
    fake.removed = ["FAKE-NOT-A-HASH"]
    traced(fake, monkeypatch)
    with pytest.raises(SystemExit) as stopped:
        push.main([])
    assert stopped.value.code not in (0, None)
    assert fake.calls == ["removed", "import_removed"] and capsys.readouterr().out == ""


def test_a_purge_that_fails_stops_the_run_and_nothing_is_sent(fake, monkeypatch):
    artist(ALPHA)
    artist(BRAVO)
    fake.removed = [digest(f"da:{BRAVO}")]

    def stuck(*args, **kwargs):
        raise OSError("a cached file could not be deleted")

    monkeypatch.setattr(library, "purge", stuck)
    with pytest.raises(SystemExit) as stopped:
        push.main([])
    assert stopped.value.code not in (0, None)
    assert fake.calls == ["removed"]


def test_a_removal_stays_applied_here_when_the_database_then_fails(fake):
    artist(ALPHA)
    artist(BRAVO)
    fake.removed, fake.fail = [digest(f"da:{BRAVO}")], "load"
    with pytest.raises(SystemExit):
        push.main([])
    assert store.is_removed(BRAVO) and library.artist_keys() == [f"da:{ALPHA}"]


# Which works go.

HELD = {
    "not_tagged": {"result": None},
    "not_artwork": {"not_artwork": True},
    "explicit": {"explicit": True},
    "explicit by rating": {"rating": "explicit"},
    "declared_ai": {"declared": "yes"},
    "words_not_read for AI": {"declared": None},
    "contact_in_words": {"contact": "yes"},
    "words_not_read": {"contact": None},
    "not_rated": {"rating": None},
    "suggestive": {"rating": "suggestive"},
    "bad_address": {"thumb": "https://zz-fake.deviantart.net/f/FAKE-THUMB-NOT-ON-WIXMP.jpg"},
}


@pytest.mark.parametrize("why", HELD)
def test_a_held_back_work_appears_nowhere_in_what_is_sent(fake, why):
    works = artist(ALPHA, 8, edits={2: HELD[why]})
    artist(BRAVO, angle=0.3)
    artist(CHARLIE, angle=0.6)

    counts = push.run(False)

    assert counts[f"works_held_{why.split()[0]}"] == 1 and counts["works_sent"] == 7 + 5 + 5
    mine = [row for row in fake.loads[-1][1] if row["artist_slug"] == ALPHA]
    assert [row["page_url"] for row in mine] == [work["page"] for n, work in enumerate(works) if n != 2]
    assert [row["position"] for row in mine] == list(range(7))  # the most recent first, with no gap
    assert not any(text in fake.sent() for text in of_work(works[2]))  # not in search_text or top_tags either
    row = sent_artists(fake)[ALPHA]
    assert works[2]["tags"][0] not in row["top_tags"] and works[2]["title"] not in row["search_text"]
    assert all(work["title"] in row["search_text"] for n, work in enumerate(works) if n != 2)


def test_a_work_declared_unclear_about_ai_is_sent_and_only_a_safe_rating_is(fake):
    artist(ALPHA, 6, edits={1: {"declared": "unclear"}, 4: {"rating": "FAKE-UNKNOWN-RATING"}})
    counts = push.run(False)
    assert counts["works_sent"] == 5 and counts["works_held_not_rated"] == 1


# Which artists go.

LEFT_OUT = {
    "not_profiled": {"profile": None},
    "profile_excluded": {"profile": "excluded"},
    "adult_portfolio": {"count": 10, "edits": {n: {"rating": "suggestive"} for n in (1, 4, 7)}},  # 3 of 10 rated
    "too_few_works": {"count": 5, "edits": {0: {"rating": None}, 3: {"declared": "yes"}}},  # 3 left
}


@pytest.mark.parametrize("why", LEFT_OUT)
def test_an_artist_left_out_appears_nowhere_in_what_is_sent_not_even_as_a_neighbour(fake, why):
    artist(ALPHA, angle=0.0)
    artist(BRAVO, angle=0.001, website=f"https://{BRAVO}.art/", **LEFT_OUT[why])  # the nearest in style to ALPHA
    artist(CHARLIE, angle=0.5)
    assert style.neighbors(f"da:{ALPHA}", "csd", 1, "local")[0][0] == f"da:{BRAVO}"

    counts = push.run(False)

    assert counts["artists_sent"] == 2 and counts[f"artists_held_{why}"] == 1
    assert sorted(sent_artists(fake)) == [ALPHA, CHARLIE]
    assert not any(word in fake.sent() for word in of_artist(BRAVO))
    assert [(row["artist_slug"], row["neighbor_slug"], row["rank"]) for row in fake.loads[-1][2]] == [
        (ALPHA, CHARLIE, 1),
        (CHARLIE, ALPHA, 1),
    ]


def test_the_thresholds_are_the_named_ones(fake):
    assert (push.MIN_WORKS, push.ADULT_PERCENT, push.NEIGHBOURS) == (4, 30, 24)
    # Exactly 4 works that may go, with 2 of 7 rated works suggestive (28.6 percent) and 3 not rated at all: sent.
    edits = {0: {"rating": "suggestive"}, 1: {"rating": "explicit"}, 2: {"rating": None}}
    edits |= {3: {"rating": None}, 4: {"rating": None}, 5: {"not_artwork": True}}
    artist(ALPHA, 10, edits=edits)
    # Fewer than 30 percent, and one work short.
    artist(BRAVO, 10, edits={n: {"rating": None} for n in range(7)})
    # 3 of 10 is 30 percent: out, though 7 works could go.
    artist(CHARLIE, 10, edits={n: {"rating": "explicit"} for n in (0, 1, 2)})
    # 30 percent of the rated works, whatever else holds them back: 3 of 10 again, two of them not artwork.
    artist(
        DELTA,
        10,
        edits={0: {"rating": "suggestive"}} | {n: {"rating": "explicit", "not_artwork": True} for n in (1, 2)},
    )
    counts = push.run(False)
    assert sorted(sent_artists(fake)) == [ALPHA]
    assert counts["works_sent"] == 4
    assert counts["artists_held_too_few_works"] == 1 and counts["artists_held_adult_portfolio"] == 2
    assert counts["works_held_artist_held_back"] == 3 + 7 + 7


def test_two_handles_that_make_one_slug_do_not_share_a_row(fake):
    artist("zz-Fake_Echo")
    artist("zz-fake-echo")
    counts = push.run(False)
    assert [(row["slug"], row["da_username"]) for row in fake.loads[-1][0]] == [("zz-fake-echo", "zz-fake-echo")]
    assert counts["artists_sent"] == 1 and counts["artists_held_slug_taken"] == 1
    assert {row["artist_slug"] for row in fake.loads[-1][1]} == {"zz-fake-echo"} and len(fake.loads[-1][1]) == 5
    assert push._slug("--zz__Fake--Echo-9-") == "zz-fake-echo-9" and push._slug("_-_") == ""


# An artist's own words, and private contacts (hard rule 6).


@pytest.mark.parametrize(("reading", "sent"), [("no", True), ("yes", False), (None, False)])
def test_a_tagline_is_sent_only_when_it_was_read_and_holds_no_contact(fake, reading, sent):
    artist(ALPHA, tagline_contact=reading)
    push.run(False)
    row = sent_artists(fake)[ALPHA]
    assert row["tagline"] == (f"FAKE-TAGLINE {ALPHA}" if sent else None)
    assert ("fake-tagline" in fake.sent()) is sent


@pytest.mark.parametrize("reading", ["yes", None])
def test_a_title_and_tags_with_a_contact_or_with_no_reading_are_not_sent(fake, reading):
    works = artist(ALPHA, 7, edits={0: {"contact": reading}, 5: {"contact": reading}})
    push.run(False)
    for held in (works[0], works[5]):
        # Not the title, not a tag, and not the page address either: DeviantArt makes that from the title.
        assert not any(text in fake.sent() for text in of_work(held))
    row = sent_artists(fake)[ALPHA]
    assert row["top_tags"] == [f"zzall{squashed(ALPHA)}", *(works[n]["tags"][0] for n in (1, 2, 3, 4, 6))]
    assert [work["title"] for work in fake.loads[-1][1]] == [works[n]["title"] for n in (1, 2, 3, 4, 6)]
    assert "fake-specialty" not in fake.sent()  # a field of the artist's own that nothing has read for a contact


# What a row is.


def test_the_rows_are_exactly_these(fake):
    works = artist(ALPHA, 5, website=f"https://www.{ALPHA}.art/", angle=0.0)
    artist(BRAVO, 5, angle=0.2)
    artist(CHARLIE, 5, angle=0.4)
    push.run(False)
    artists, sent_works, neighbours = fake.loads[-1]

    assert all(set(row) == ARTIST_FIELDS for row in artists)
    assert all(set(row) == WORK_FIELDS for row in sent_works)
    assert all(set(row) == NEIGHBOUR_FIELDS for row in neighbours)
    row = artists[0]
    embedding = row.pop("profile_embedding")
    assert embedding == [1.0, *[0.0] * 1023]
    word = squashed(ALPHA)
    assert row == {
        "slug": ALPHA,
        "name": ALPHA,
        "da_username": ALPHA,
        "site_url": f"https://www.{ALPHA}.art/",
        "site_domain": f"{ALPHA}.art",
        "tagline": f"FAKE-TAGLINE {ALPHA}",
        "top_tags": [f"zzall{word}", *(f"zzown{word}{n}" for n in range(5))],
        "search_text": " ".join(
            [
                *("character", "dragons", "digital painting", "fantasy", "portrait or bust", "painterly"),
                *(work["title"] for work in works),
                *(f"zzall{word}", *(f"zzown{word}{n}" for n in range(5))),
            ]
        ),
        "profile": f"FAKE-PROFILE {ALPHA}.",
        "specialties": ["character"],
        "mediums": ["digital painting"],
        "open_for_work": None,
    }
    assert sent_works[0] == {
        "artist_slug": ALPHA,
        "source": "deviantart",
        "page_url": works[0]["page"],
        "image_url": works[0]["thumb"],
        "title": works[0]["title"],
        "description": works[0]["description"],
        "tags": ["digital painting", "character", "fantasy", "realistic", "painterly", "portrait or bust"],
        "position": 0,
    }
    assert [(row["artist_slug"], row["neighbor_slug"], row["rank"]) for row in neighbours if row["rank"] == 1] == [
        (ALPHA, BRAVO, 1),
        (BRAVO, ALPHA, 1),
        (CHARLIE, BRAVO, 1),
    ]
    assert all(isinstance(row["score"], float) and math.isfinite(row["score"]) for row in neighbours)


def test_every_address_sent_is_a_deviantart_page_a_wixmp_thumbnail_or_an_own_site_with_its_domain(fake):
    works = artist(ALPHA, 6, website=f"https://www.{ALPHA}.art/", edits={1: {"thumb": HELD["bad_address"]["thumb"]}})
    artist(BRAVO, website="https://www.artstation.com/zzfakebravo")  # a profile on a platform is not a site
    artist(CHARLIE, website=f"http://{CHARLIE}.art/")
    artist(DELTA)
    push.run(False)
    artists, sent_works, neighbours = fake.loads[-1]

    sites = {row["da_username"]: (row["site_url"], row["site_domain"]) for row in artists}
    assert sites == {
        ALPHA: (f"https://www.{ALPHA}.art/", f"{ALPHA}.art"),
        BRAVO: (None, None),
        CHARLIE: (f"http://{CHARLIE}.art/", f"{CHARLIE}.art"),
        DELTA: (None, None),
    }
    for row in artists:
        if row["site_url"]:
            assert crawl.is_own_site(row["site_url"])
            assert store.site_key(row["site_url"]) == f"site:{row['site_domain']}"
    assert len(sent_works) == 5 + 5 + 5 + 5
    for row in sent_works:
        assert urlsplit(row["page_url"]).hostname == "www.deviantart.com" and "/art/" in row["page_url"]
        assert library.page_address(row["page_url"]) == row["page_url"]
        assert urlsplit(row["image_url"]).hostname.endswith(".wixmp.com") and row["image_url"].startswith("https://")

    # No address anywhere else, no local path, no file name, no deviation id, and nothing long enough for an image.
    def texts(value):
        if isinstance(value, dict | list):
            for inner in value.values() if isinstance(value, dict) else value:
                yield from texts(inner)
        else:
            assert value is None or isinstance(value, str | int | float | bool)  # never bytes
            if isinstance(value, str):
                yield value

    addresses = [row[name] for row in sent_works for name in ("page_url", "image_url")]
    addresses += [row["site_url"] for row in artists if row["site_url"]]
    others = [text for text in texts(fake.loads[-1]) if text not in addresses]
    assert not any("://" in text or "data:" in text.lower() for text in others)
    assert max(map(len, texts(fake.loads[-1]))) <= push.MAX_SEARCH_TEXT
    everything = fake.sent()
    assert str(config.CACHE_DIR).lower() not in everything and config.CACHE_DIR.name + "\\" not in everything
    assert "fake-image-bytes" not in everything and "pipeline.sqlite" not in everything
    for work in works + [{"id": path.stem} for path in library.image_dir().iterdir()]:
        assert work["id"].lower() not in everything  # an image's file name is its deviation id
    assert not any(path.name.lower() in everything for path in library.image_dir().iterdir())


@pytest.mark.parametrize(
    ("page", "thumb"),
    [
        ("https://www.deviantart.com/zz-fake/art/FAKE-1", "https://zz-fake.deviantart.net/f/FAKE.jpg"),
        ("https://www.deviantart.com/zz-fake/art/FAKE-1", "https://wixmp.com/f/FAKE.jpg"),
        ("https://www.deviantart.com/zz-fake/art/FAKE-1", "https://zz-fake.wixmp.com/f/FAKE[1].jpg"),
        ("https://www.deviantart.com/zz-fake/art/FAKE-1", "https://zz-fake.wixmp.com/f/FAKE.jpg?token=FAKE"),
        ("https://www.deviantart.com/zz-fake/art/FAKE-1", "http://zz-fake.wixmp.com/f/FAKE.jpg"),
        ("https://www.deviantart.com/zz-fake/art/FAKE-1", "data:image/jpeg;base64,RkFLRQ=="),
        ("https://www.deviantart.com/zz-fake/art/FAKE-1", "C:\\cache\\images\\FAKE.jpg"),
        ("https://www.deviantart.com/zz-fake/art/FAKE-1", "file:///C:/cache/images/FAKE.jpg"),
        ("https://deviantart.com/zz-fake/art/FAKE-1", "https://zz-fake.wixmp.com/f/FAKE.jpg"),
        ("https://sta.sh/zz-fake/art/FAKE-1", "https://zz-fake.wixmp.com/f/FAKE.jpg"),
        ("https://www.deviantart.com/zz-fake/gallery", "https://zz-fake.wixmp.com/f/FAKE.jpg"),
        ("https://www.deviantart.com/zz-fake-other/art/FAKE-1", "https://zz-fake.wixmp.com/f/FAKE.jpg"),
        ("https://www.deviantart.com/zz-fake-longer/art/FAKE-1", "https://zz-fake.wixmp.com/f/FAKE.jpg"),
        (None, "https://zz-fake.wixmp.com/f/FAKE.jpg"),
    ],
)
def test_an_address_the_database_would_not_take_holds_the_work_back(page, thumb):
    assert push._addresses(page, thumb, "zz-fake") is None
    good = ("https://www.deviantart.com/zz-Fake/art/FAKE-1", "https://zz-fake.wixmp.com/f/FAKE.jpg")
    assert push._addresses(*good, "ZZ-fake") == good  # a handle is one handle however its capitals are written


def test_a_second_work_with_the_same_page_is_not_sent(fake):
    works = artist(ALPHA, 6)
    with closing(sqlite3.connect(config.LOCAL_DB)) as conn, conn:
        conn.execute("UPDATE works SET page_url = ? WHERE deviation_id = ?", (works[0]["page"], works[3]["id"]))
    counts = push.run(False)
    pages = [row["page_url"] for row in fake.loads[-1][1]]
    assert len(pages) == len(set(pages)) == 5 and counts["works_held_bad_address"] == 1


@pytest.mark.parametrize(
    ("website", "site"),
    [
        ("https://www.zz-fake.art/", ("https://www.zz-fake.art/", "zz-fake.art")),
        ("https://zz-fake.co.uk/", ("https://zz-fake.co.uk/", "zz-fake.co.uk")),
        ("https://zz-fake.art/gallery", (None, None)),  # not the front page
        ("https://zz-fake.art/?ref=FAKE", (None, None)),
        ("https://shop.zz-fake.art/", (None, None)),  # not the bare domain
        ("https://www.artstation.com/zzfake", (None, None)),  # a profile on a platform
        ("https://zzfake.tumblr.com/", (None, None)),
        ("https://www.deviantart.com/zzfake", (None, None)),
        ("https://zzfake.github.io/", (None, None)),  # a subdomain a host hands out
        ("https://zz-fake.art/<FAKE>", (None, None)),
        ("C:\\cache\\images\\FAKE.jpg", (None, None)),
        ("", (None, None)),
        (None, (None, None)),
    ],
)
def test_a_site_is_sent_only_as_the_front_page_of_a_domain_of_the_artists_own(website, site):
    assert push._own_site(website) == site


def test_an_artist_the_store_no_longer_lists_is_never_built_whatever_another_stage_still_says(fake, monkeypatch):
    artist(ALPHA, angle=0.0)
    artist(BRAVO, angle=0.01)  # no site of their own: nothing but the store's list says they are gone
    artist(CHARLIE, angle=0.5)
    stale = (profiles.profiles(), tagging.results(profiles.TAGGER, verifier=profiles.TAGGER), tagging.contacts())
    store.mark_removed(BRAVO)  # removed here, and nothing purged yet: every other stage's rows are still there
    monkeypatch.setattr(profiles, "profiles", lambda db=None: stale[0])
    monkeypatch.setattr(tagging, "results", lambda *args, **kwargs: stale[1])
    monkeypatch.setattr(tagging, "contacts", lambda *args, **kwargs: stale[2])
    built = push.build()
    assert [row["da_username"] for row in built[0]] == [ALPHA, CHARLIE]
    assert not any(word in json.dumps(built[:3]).lower() for word in of_artist(BRAVO))


def test_a_site_two_artists_give_is_sent_for_neither(fake):
    artist(ALPHA, website="https://zz-fake-shared.art/", links=[])
    artist(BRAVO, website="https://www.zz-fake-shared.art/", links=[])
    push.run(False)
    assert [(row["site_url"], row["site_domain"]) for row in fake.loads[-1][0]] == [(None, None)] * 2
    assert "zz-fake-shared" not in fake.sent()


def test_each_artist_gets_the_24_nearest_of_those_who_go(fake):
    handles = [f"zz-fake-n{n:02d}" for n in range(28)]
    for n, handle in enumerate(handles):
        artist(handle, 4, angle=0.05 * n)
    artist(BRAVO, 4, angle=0.0501, profile=None)  # two who do not go, in the middle of the others
    artist(CHARLIE, 4, angle=0.4001, edits={0: {"rating": None}})
    counts = push.run(False)
    neighbours = fake.loads[-1][2]

    going = {f"da:{handle}": handle for handle in handles}
    assert counts["artists_sent"] == 28 and counts["neighbours_sent"] == len(neighbours) == 28 * 24
    for key, slug in going.items():
        mine = [row for row in neighbours if row["artist_slug"] == slug]
        wanted = [(going[other], score) for other, score in style.neighbors(key, "csd", 999, "local") if other in going]
        assert [row["rank"] for row in mine] == list(range(1, 25))
        assert [row["neighbor_slug"] for row in mine] == [other for other, _ in wanted[:24]]
        assert [row["score"] for row in mine] == [pytest.approx(score, abs=1e-4) for _, score in wanted[:24]]
        assert slug not in [row["neighbor_slug"] for row in mine]
    # Asking for the 24 nearest of everybody would have named the two who do not go, and left 23 or fewer.
    assert any(
        other in (f"da:{BRAVO}", f"da:{CHARLIE}")
        for key in going
        for other, _ in style.neighbors(key, "csd", 24, "local")
    )
    assert not any(word in fake.sent() for word in of_artist(BRAVO) + of_artist(CHARLIE))


# Running it again.


def test_running_twice_sends_the_same_rows_and_an_artist_who_stops_qualifying_is_deleted(fake):
    works = artist(ALPHA, 6, angle=0.0)
    gone = artist(BRAVO, 6, angle=0.1)
    artist(CHARLIE, 6, angle=0.2)
    first = push.run(False)
    held = fake.held()
    second = push.run(False)
    assert fake.loads[0] == fake.loads[1] and fake.held() == held
    assert fake.calls == ["removed", "listing", "load"] * 2  # nothing to delete the second time
    assert second == first and first["artists_removed_from_database"] == first["works_removed_from_database"] == 0
    assert len(fake.artists) == 3 and len(fake.works) == 18 and len(fake.neighbors) == 6

    for work in gone[:2]:  # 2 of 6 is a third: an adult portfolio now
        rate(work, "explicit")
    rate(works[3], "suggestive")  # and one work of an artist who stays
    third = push.run(False)

    assert fake.calls[-3:] == ["listing", "delete", "load"]
    assert third["artists_removed_from_database"] == 1 and third["works_removed_from_database"] == 1
    assert third["artists_held_adult_portfolio"] == 1 and third["works_held_suggestive"] == 1
    assert sorted(row["slug"] for row in fake.artists.values()) == [ALPHA, CHARLIE]
    assert not any(word in fake.held() for word in of_artist(BRAVO))  # their works, and their place as a neighbour
    assert not any(text in fake.held() for text in of_work(works[3]))
    # The database holds exactly what the last load was given.
    artists, sent_works, neighbours = fake.loads[-1]
    key = lambda row: json.dumps(row, sort_keys=True)  # noqa: E731
    assert sorted(fake.artists.values(), key=key) == sorted(artists, key=key)
    assert sorted(fake.works.values(), key=key) == sorted(sent_works, key=key)
    assert sorted(fake.neighbors, key=key) == sorted(neighbours, key=key)
    assert len(sent_works) == 5 + 6 and [row["position"] for row in sent_works[:5]] == list(range(5))


@pytest.mark.parametrize("error", [None, ConnectionError("FAKE"), KeyError("FAKE"), Exception("FAKE")])
@pytest.mark.parametrize("fail", ["removed", "listing", "delete", "load"])
def test_a_failure_part_way_leaves_the_local_stores_untouched_and_exits_non_zero(fake, capsys, fail, error):
    artist(ALPHA)
    artist(BRAVO)
    artist(CHARLIE, profile=None)
    push.run(False)
    with closing(sqlite3.connect(config.LOCAL_DB)) as conn, conn:  # BRAVO stops qualifying: something to delete
        conn.execute("DELETE FROM artist_profiles WHERE artist_key = ?", (f"da:{BRAVO}",))
    before, loads = local_state(), len(fake.loads)
    capsys.readouterr()

    fake.fail, fake.error = fail, error or fake.error
    with pytest.raises(SystemExit) as stopped:
        push.main([])

    assert stopped.value.code not in (0, None)
    assert local_state() == before
    output = capsys.readouterr()
    assert output.out == "" and "zz-fake" not in output.err
    assert fake.calls[-1] == fail and len(fake.loads) == loads
    if fail in ("removed", "listing"):
        assert BRAVO in fake.held()  # the database was not touched either


# What it prints.


def test_a_dry_run_writes_nothing_to_the_database_and_prints_the_same_counts(fake, capsys):
    artist(ALPHA, 6, edits={0: {"rating": None}})
    artist(BRAVO)
    artist(CHARLIE, profile=None)
    push.run(False)
    with closing(sqlite3.connect(config.LOCAL_DB)) as conn, conn:
        conn.execute("DELETE FROM artist_profiles WHERE artist_key = ?", (f"da:{BRAVO}",))
    held, calls = fake.held(), len(fake.calls)
    capsys.readouterr()

    push.main(["--dry-run"])
    dry = capsys.readouterr().out
    assert fake.calls[calls:] == ["removed", "listing"] and fake.held() == held and len(fake.loads) == 1
    push.main([])
    real = capsys.readouterr().out

    assert dry == real and fake.calls[-3:] == ["listing", "delete", "load"]
    counts = json.loads(real)
    assert real.count("\n") == 1 and list(counts) == COUNTS
    assert all(type(value) is int for value in counts.values())  # counts only: nothing about any artist or work
    assert counts == dict.fromkeys(COUNTS, 0) | {
        "artists_sent": 1,
        "artists_held_not_profiled": 2,
        "works_sent": 5,
        "works_held_not_rated": 1,
        "works_held_artist_held_back": 10,
        "artists_removed_from_database": 1,
    }
