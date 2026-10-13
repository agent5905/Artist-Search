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

ALPHA, BRAVO, CHARLIE, DELTA = "yy-fake-alpha", "yy-fake-bravo", "yy-fake-charlie", "yy-fake-delta"
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
WORK_FIELDS = {
    *("artist_slug", "source", "page_url", "image_url", "title", "description", "tags", "position"),
    *("width", "height"),
}
NEIGHBOUR_FIELDS = {"artist_slug", "neighbor_slug", "rank", "score"}
COUNTS = [
    "artists_sent",
    *(f"artists_held_{reason}" for reason in push.ARTIST_REASONS),
    "works_sent",
    *(f"works_held_{reason}" for reason in push.WORK_REASONS),
    *("neighbours_sent", "artists_sent_without_neighbours", "neighbours_skipped_by_database"),
    *("artists_deleted_from_database", "works_deleted_from_database", "removed_artists_deleted_from_database"),
    *("removals_added_to_database", "removals_applied_here", "artists_purged_here"),
]
NO_NEIGHBOUR = {"neighbor_slug": None, "rank": None, "score": None}  # beside artist_slug: "this artist has none"
_BASES: dict[str, int] = {}  # handle -> where its works' ids begin


def digest(key):
    return hashlib.sha256(key.encode()).hexdigest()


class FakeDb:
    """Stands in for pipeline.db. It keeps every row it is given and refuses none of them: whatever must not
    reach the database must not be sent to this.

    What the real one does by itself, this does too (sql/002_functions.sql): an artist whose hash joins the
    removal list is deleted there and then, with their works and their place as a neighbour; an artist named in
    a load's neighbour rows ends up with exactly the rows whose two artists it has; and load says how many
    neighbour rows it skipped.
    """

    def __init__(self):
        self.removed = []  # the key hashes in its `removed` table
        self.artists = {}  # id -> the row
        self.works = {}  # id -> the row, which names its artist by artist_slug
        self.neighbors = []
        self.calls = []  # "listing", "removed", "removal", "delete", "load", in the order they came
        self.loads = []  # what each load was given: [artists, works, neighbors]
        self.fail = None  # the call that fails
        self.error = RuntimeError("HTTP 500: FAKE-DATABASE-FAILURE")  # what pipeline.db raises
        self.skips = set()  # slugs it will not store: an artist removed there since this run read the list
        self._ids = itertools.count(1)

    def _note(self, call):
        self.calls.append(call)
        if self.fail == call:
            raise self.error

    @staticmethod
    def keys(row):
        """The two hashes the removal list knows an artist by, as the database works them out."""
        site = row.get("site_domain")
        return [digest(f"da:{row['da_username'].lower()}"), site and digest(f"site:{site}")]

    def _delete_artist(self, artist_id):
        """An artist's works and neighbour rows go with them, as the foreign keys have it."""
        if artist_id in self.artists:
            slug = self.artists.pop(artist_id)["slug"]
            self.works = {key: work for key, work in self.works.items() if work["artist_slug"] != slug}
            self.neighbors = [row for row in self.neighbors if slug not in (row["artist_slug"], row["neighbor_slug"])]

    def sql(self, query):
        if query == push.REMOVED_SQL:
            self._note("removed")
            return [{"key_hash": key_hash} for key_hash in self.removed]
        if query == push.LISTING_SQL:
            self._note("listing")
            rows = []
            for artist_id, row in self.artists.items():
                da_key, site_key = self.keys(row)
                artist = {"artist_id": artist_id, "slug": row["slug"], "da_key": da_key, "site_key": site_key}
                mine = [(work_id, work) for work_id, work in self.works.items() if work["artist_slug"] == row["slug"]]
                rows += [artist | {"work_id": work_id, "page_url": work["page_url"]} for work_id, work in mine] or [
                    artist | {"work_id": None, "page_url": None}
                ]
            return rows
        # The only other SQL: key hashes for the removal list, and deletes by id.
        hashes = r"((?:\('[0-9a-f]{64}'\)(?:, )?)+)"
        insert = rf"insert into public\.removed \(key_hash\) values {hashes} on conflict \(key_hash\) do nothing"
        delete = r"delete from public\.(works|artists) where id in \(([0-9, ]+)\)"
        statements = query.split("; ")
        assert all(re.fullmatch(f"{insert}|{delete}", statement) for statement in statements), query[:80]
        self._note("removal" if "insert" in query else "delete")
        for statement in statements:
            if added := re.fullmatch(insert, statement):
                new = [key_hash for key_hash in re.findall("[0-9a-f]{64}", added[1]) if key_hash not in self.removed]
                self.removed += new
                for artist_id in [key for key, row in self.artists.items() if set(self.keys(row)) & set(new)]:
                    self._delete_artist(artist_id)
            else:
                table, ids = re.fullmatch(delete, statement).groups()
                for row_id in map(int, ids.split(", ")):  # an id that is no longer there deletes nothing
                    self.works.pop(row_id, None) if table == "works" else self._delete_artist(row_id)
        return []

    def load(self, artists, works, neighbors):
        self._note("load")
        self.loads.append(json.loads(json.dumps([artists, works, neighbors], allow_nan=False)))  # it must be JSON
        for row in artists:
            if row["slug"] not in self.skips:
                ids = [artist_id for artist_id, have in self.artists.items() if have["slug"] == row["slug"]]
                self.artists[ids[0] if ids else next(self._ids)] = dict(row)
        slugs = {row["slug"] for row in self.artists.values()}
        for row in works:
            key = (row["artist_slug"], row["page_url"])
            ids = [work_id for work_id, have in self.works.items() if (have["artist_slug"], have["page_url"]) == key]
            if row["artist_slug"] in slugs:
                self.works[ids[0] if ids else next(self._ids)] = dict(row)
        named = {row["artist_slug"] for row in neighbors}
        known = [row for row in neighbors if row["artist_slug"] in slugs and row["neighbor_slug"] in slugs]
        self.neighbors = [row for row in self.neighbors if row["artist_slug"] not in named] + known
        return len(neighbors) - len(known)

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


def squashed(handle):
    return handle.replace("-", "").replace("_", "")


def style_vector(angle, n):
    numbers = np.zeros(style.DIMENSIONS, dtype=np.float32)
    numbers[:3] = math.cos(angle), math.sin(angle), 0.01 * n
    return (numbers / np.linalg.norm(numbers)).astype(np.float16).tobytes()


def artist(
    handle,
    count=5,
    *,
    profile="written",
    tagline_contact="no",
    website=None,
    links=None,
    angle=0.0,
    edits=None,
    user=None,
    old_keys=(),
    sheet=None,
):
    """Collect, tag, rate and profile one invented artist, and return their works, the most recent first.

    Every work is fit to send unless `edits` says otherwise for its place in the list: "result": None (no
    verified tags), "explicit" or "not_artwork": True, "declared" (AI: "yes", "unclear", None for not read),
    "contact" ("yes", None for not read), "rating" ("suggestive", "explicit", None for not rated), "thumb".
    And "size": its thumbnail's (width, height), as the API gave it: without one, none is kept for the work.
    profile: "written", "excluded" (the profile stage ruled them out) or None (no row there yet).
    user: the handle whose account this is, when it is an account that took another handle, and old_keys: the
    keys the collector found that account kept under, which it names when it stores the new one.
    sheet: (pictures on the sheet the profile stage looked at, how many of them it counted as unsuitable).
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
            "thumb": edit.get("thumb", f"https://images-wixmp-yyfake.wixmp.com/f/FAKE-THUMB-{handle}-{n}.jpg"),
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
        facts = FACTS | ({"sheet": {"pictures": sheet[0], "unsuitable": sheet[1]}} if sheet else {})
        rows.append(("artist_profiles", (key, "FAKE-BASIS", reason, json.dumps(facts), "[]", *texts, AT)))

    own_site = bool(website) and crawl.is_own_site(website)
    assert store.upsert_artist(
        handle, f"https://www.deviantart.com/{handle}", [website] if links is None and own_site else links or [], 1
    )
    library.store_artist(
        key,
        user_id=f"FAKE-USER-{user or handle}",
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
                "thumb_size": edits.get(n, {}).get("size"),
            }
            for n, work in enumerate(made)
        ],
        old_keys=old_keys,
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
        *((tagging, "results"), (tagging, "contacts"), (push, "_collected"), (style, "all_neighbors")),
    )

    def note(module, name):
        real = getattr(module, name)

        def noted(*args, **kwargs):
            fake.calls.append(name)
            return real(*args, **kwargs)

        monkeypatch.setattr(module, name, noted)

    for module, name in calls:
        note(module, name)


# Every read a row is built from.
BUILDS = {"profiles", "excluded", "results", "contacts", "_collected", "all_neighbors"}


def seed(fake, count, works=0, prefix="old-fake"):
    """Put artists straight into the fake database, each with `works` works, as an earlier push would have left
    them. Returns the rows, as a load takes them."""
    artists = [
        {"slug": f"{prefix}-{n}", "name": f"{prefix}-{n}", "da_username": f"{prefix}-{n}", "site_domain": None}
        for n in range(count)
    ]
    pages = [
        {"artist_slug": row["slug"], "page_url": f"https://www.deviantart.com/{row['slug']}/art/FAKE-{n}"}
        for row in artists
        for n in range(works)
    ]
    fake.load(artists, pages, [])
    fake.calls.clear(), fake.loads.clear()
    return artists, pages


# Removal first (hard rule 7).


@pytest.mark.parametrize("dry_run", [False, True])
def test_a_removal_made_through_the_site_is_imported_before_any_row_is_built(fake, monkeypatch, dry_run):
    artist(ALPHA, angle=0.0)
    removed = artist(BRAVO, angle=0.01, website=f"https://{BRAVO}.art/")
    artist(CHARLIE, angle=0.5)
    fake.removed = [digest(f"da:{BRAVO}")]
    traced(fake, monkeypatch)

    counts = push.run(dry_run)

    assert fake.calls[:3] == ["listing", "removed", "import_removed"]
    first_build = min(fake.calls.index(name) for name in BUILDS)
    last_removal = max(place for place, name in enumerate(fake.calls) if name in ("import_removed", "purge", "removal"))
    assert "purge" in fake.calls and last_removal < first_build
    assert ("removal" in fake.calls) is not dry_run
    assert counts["removals_applied_here"] == 1 and counts["artists_purged_here"] == 1
    # Nothing of theirs is left here: no row, no image, and the handle and account are barred. A handle removal
    # bars no site (it must not travel to other artists who list the same one).
    assert f"da:{BRAVO}" not in library.artist_keys()
    assert not any(path.name.startswith(work["id"]) for work in removed for path in library.image_dir().iterdir())
    assert store.is_removed(BRAVO) and not store.is_removed(f"{BRAVO}.art")
    assert library.account_removed(f"FAKE-USER-{BRAVO}")
    assert [key for key, _, _ in store.all_artists()] == [f"da:{ALPHA}", f"da:{CHARLIE}"]
    # What the removal reached here goes back to the list of record: their account under any handle.
    reached = {library.account(f"FAKE-USER-{BRAVO}")}
    assert counts["removals_added_to_database"] == 1
    if dry_run:
        assert fake.loads == [] and counts["artists_sent"] == 2 and fake.removed == [digest(f"da:{BRAVO}")]
        return
    assert set(fake.removed) == {digest(f"da:{BRAVO}")} | reached == push._removed_here(None)
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
    assert counts["removed_artists_deleted_from_database"] == 1 and counts["artists_deleted_from_database"] == 0
    assert counts["artists_sent"] == 2 and counts["works_sent"] == 10 and counts["neighbours_sent"] == 2


def test_a_removal_list_that_cannot_be_applied_stops_the_run_before_anything_is_built(fake, monkeypatch, capsys):
    artist(ALPHA)
    fake.removed = ["FAKE-NOT-A-HASH"]
    traced(fake, monkeypatch)
    with pytest.raises(SystemExit) as stopped:
        push.main([])
    assert stopped.value.code not in (0, None)
    assert fake.calls == ["listing", "removed", "import_removed"] and capsys.readouterr().out == ""


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
    assert fake.calls == ["listing", "removed"]


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
    "bad_address": {"thumb": "https://yy-fake.deviantart.net/f/FAKE-THUMB-NOT-ON-WIXMP.jpg"},
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
    assert (push.MIN_WORKS, push.MAX_WORKS, push.ADULT_PERCENT, push.NEIGHBOURS) == (4, 10, 30, 24)
    assert (push.DELETE_PERCENT, push.DELETE_FLOOR, push.TEST_SLUG) == (10, 5, "zz-")
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
    artist("yy-Fake_Echo")
    artist("yy-fake-echo")
    counts = push.run(False)
    assert [(row["slug"], row["da_username"]) for row in fake.loads[-1][0]] == [("yy-fake-echo", "yy-fake-echo")]
    assert counts["artists_sent"] == 1 and counts["artists_held_slug_unusable"] == 1
    assert {row["artist_slug"] for row in fake.loads[-1][1]} == {"yy-fake-echo"} and len(fake.loads[-1][1]) == 5
    assert push._slug("--yy__Fake--Echo-9-") == "yy-fake-echo-9" and push._slug("_-_") == ""


def test_a_handle_that_would_make_a_test_rows_slug_is_never_sent(fake):
    artist(ALPHA)
    artist("ZZ-fake-own")  # a real handle may begin like this; a database with such a slug would stop every push
    first = push.run(False)
    assert [row["slug"] for row in fake.loads[-1][0]] == [ALPHA] and first["artists_held_slug_unusable"] == 1
    assert "zz-fake-own" not in fake.sent()
    assert push.run(False)["artists_sent"] == 1  # and so the next run is not stopped by a row of its own


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
        "width": None,  # the library keeps no size yet: both keys, and no number
        "height": None,
    }
    assert [(row["artist_slug"], row["neighbor_slug"], row["rank"]) for row in neighbours if row["rank"] == 1] == [
        (ALPHA, BRAVO, 1),
        (BRAVO, ALPHA, 1),
        (CHARLIE, BRAVO, 1),
    ]
    assert all(isinstance(row["score"], float) and math.isfinite(row["score"]) for row in neighbours)


def test_every_address_sent_is_a_deviantart_page_a_wixmp_thumbnail_or_an_own_site_with_its_domain(fake):
    works = artist(ALPHA, 6, website=f"https://www.{ALPHA}.art/", edits={1: {"thumb": HELD["bad_address"]["thumb"]}})
    artist(BRAVO, website="https://www.artstation.com/yyfakebravo")  # a profile on a platform is not a site
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
        ("https://www.deviantart.com/yy-fake/art/FAKE-1", "https://yy-fake.deviantart.net/f/FAKE.jpg"),
        ("https://www.deviantart.com/yy-fake/art/FAKE-1", "https://wixmp.com/f/FAKE.jpg"),
        ("https://www.deviantart.com/yy-fake/art/FAKE-1", "https://yy-fake.wixmp.com/f/FAKE[1].jpg"),
        ("https://www.deviantart.com/yy-fake/art/FAKE-1", "https://yy-fake.wixmp.com/f/FAKE.jpg?token=FAKE"),
        ("https://www.deviantart.com/yy-fake/art/FAKE-1", "http://yy-fake.wixmp.com/f/FAKE.jpg"),
        ("https://www.deviantart.com/yy-fake/art/FAKE-1", "data:image/jpeg;base64,RkFLRQ=="),
        ("https://www.deviantart.com/yy-fake/art/FAKE-1", "C:\\cache\\images\\FAKE.jpg"),
        ("https://www.deviantart.com/yy-fake/art/FAKE-1", "file:///C:/cache/images/FAKE.jpg"),
        ("https://deviantart.com/yy-fake/art/FAKE-1", "https://yy-fake.wixmp.com/f/FAKE.jpg"),
        ("https://sta.sh/yy-fake/art/FAKE-1", "https://yy-fake.wixmp.com/f/FAKE.jpg"),
        ("https://www.deviantart.com/yy-fake/gallery", "https://yy-fake.wixmp.com/f/FAKE.jpg"),
        ("https://www.deviantart.com/yy-fake-other/art/FAKE-1", "https://yy-fake.wixmp.com/f/FAKE.jpg"),
        ("https://www.deviantart.com/yy-fake-longer/art/FAKE-1", "https://yy-fake.wixmp.com/f/FAKE.jpg"),
        (None, "https://yy-fake.wixmp.com/f/FAKE.jpg"),
    ],
)
def test_an_address_the_database_would_not_take_holds_the_work_back(page, thumb):
    assert push._addresses(page, thumb, "yy-fake") is None
    good = ("https://www.deviantart.com/yy-Fake/art/FAKE-1", "https://yy-fake.wixmp.com/f/FAKE.jpg")
    assert push._addresses(*good, "YY-fake") == good  # a handle is one handle however its capitals are written


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
        ("https://www.yy-fake.art/", ("https://www.yy-fake.art/", "yy-fake.art")),
        ("https://yy-fake.co.uk/", ("https://yy-fake.co.uk/", "yy-fake.co.uk")),
        ("https://yy-fake.art/gallery", (None, None)),  # not the front page
        ("https://yy-fake.art/?ref=FAKE", (None, None)),
        ("https://shop.yy-fake.art/", (None, None)),  # not the bare domain
        ("https://www.artstation.com/yyfake", (None, None)),  # a profile on a platform
        ("https://yyfake.tumblr.com/", (None, None)),
        ("https://www.deviantart.com/yyfake", (None, None)),
        ("https://yyfake.github.io/", (None, None)),  # a subdomain a host hands out
        ("https://yy-fake.art/<FAKE>", (None, None)),
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
    everybody = style.all_neighbors  # and a style stage that answers about everybody, whoever it was asked about
    monkeypatch.setattr(
        style, "all_neighbors", lambda model, top, rule, among, db: everybody(model, 999, rule, None, db)
    )
    assert f"da:{BRAVO}" in dict(style.all_neighbors("csd", 24, "local", {f"da:{ALPHA}"}, None)[f"da:{ALPHA}"])
    built = push.build()
    assert [row["da_username"] for row in built[0]] == [ALPHA, CHARLIE]
    assert not any(word in json.dumps(built[:3]).lower() for word in of_artist(BRAVO))


def test_a_site_two_artists_give_is_sent_for_neither(fake):
    artist(ALPHA, website="https://yy-fake-shared.art/", links=[])
    artist(BRAVO, website="https://www.yy-fake-shared.art/", links=[])
    push.run(False)
    assert [(row["site_url"], row["site_domain"]) for row in fake.loads[-1][0]] == [(None, None)] * 2
    assert "yy-fake-shared" not in fake.sent()


def test_each_artist_gets_the_24_nearest_of_those_who_go_from_one_pass(fake, monkeypatch):
    handles = [f"yy-fake-n{n:02d}" for n in range(28)]
    for n, handle in enumerate(handles):
        artist(handle, 4, angle=0.05 * n)
    artist(BRAVO, 4, angle=0.0501, profile=None)  # two who do not go, in the middle of the others
    artist(CHARLIE, 4, angle=0.4001, edits={0: {"rating": None}})
    going = {f"da:{handle}": handle for handle in handles}
    asked, one_pass = [], style.all_neighbors

    def noted(model, top, rule, among, db):
        asked.append((model, top, rule, set(among)))
        return one_pass(model, top, rule, among, db)

    def one_at_a_time(*args, **kwargs):
        raise AssertionError("one artist at a time: every pair is scored again for each")

    monkeypatch.setattr(style, "all_neighbors", noted)
    monkeypatch.setattr(style, "neighbors", one_at_a_time)
    counts = push.run(False)
    neighbours = fake.loads[-1][2]

    assert asked == [("csd", 24, "local", set(going))]  # once, and the pool is the artists who go
    assert counts["artists_sent"] == 28 and counts["neighbours_sent"] == len(neighbours) == 28 * 24
    assert counts["artists_sent_without_neighbours"] == counts["neighbours_skipped_by_database"] == 0
    wanted = one_pass("csd", 24, "local", set(going))
    for key, slug in going.items():
        mine = [row for row in neighbours if row["artist_slug"] == slug]
        assert [row["rank"] for row in mine] == list(range(1, 25))
        assert [row["neighbor_slug"] for row in mine] == [going[other] for other, _ in wanted[key]]
        assert [row["score"] for row in mine] == [pytest.approx(score, abs=1e-4) for _, score in wanted[key]]
        assert slug not in [row["neighbor_slug"] for row in mine]
    # With everybody in the pool, the 24 nearest would have named the two who do not go.
    everybody = one_pass("csd", 24, "local")
    assert any(other in (f"da:{BRAVO}", f"da:{CHARLIE}") for key in going for other, _ in everybody[key])
    assert not any(word in fake.sent() for word in of_artist(BRAVO) + of_artist(CHARLIE))
    # And 24 is the most that is sent, however many the style stage hands over.
    monkeypatch.setattr(
        style, "all_neighbors", lambda model, top, rule, among, db: one_pass(model, 999, rule, among, db)
    )
    assert sorted(row["rank"] for row in push.build()[2] if row["artist_slug"] == handles[0]) == list(range(1, 25))


def test_an_artist_who_goes_without_neighbours_loses_the_ones_they_had(fake):
    artist(ALPHA, angle=0.0)
    lost = artist(BRAVO, angle=0.2)
    artist(CHARLIE, angle=0.4)
    push.run(False)
    assert sorted(row["artist_slug"] for row in fake.neighbors) == sorted([ALPHA, BRAVO, CHARLIE] * 2)

    with closing(sqlite3.connect(config.LOCAL_DB)) as conn, conn:  # BRAVO's works have no style vectors any more
        conn.executemany("DELETE FROM style_vectors WHERE deviation_id = ?", [(work["id"],) for work in lost])
    counts = push.run(False)

    # One row says so: it names them and no neighbour, which is how replace_neighbors is told "none".
    assert [row for row in fake.loads[-1][2] if row["artist_slug"] == BRAVO] == [{"artist_slug": BRAVO} | NO_NEIGHBOUR]
    assert sorted((row["artist_slug"], row["neighbor_slug"]) for row in fake.neighbors) == [
        (ALPHA, CHARLIE),
        (CHARLIE, ALPHA),
    ]
    assert counts["artists_sent"] == 3 and counts["neighbours_sent"] == 2
    assert counts["artists_sent_without_neighbours"] == 1 and counts["neighbours_skipped_by_database"] == 0


def test_the_neighbour_rows_the_database_skips_are_counted(fake):
    artist(ALPHA, angle=0.0)
    artist(BRAVO, angle=0.2)
    alone = artist(CHARLIE, angle=0.4)
    assert push.run(False)["neighbours_skipped_by_database"] == 0
    fake.skips = {BRAVO}  # the database will not have them: removed there since this run read its list
    for artist_id in [key for key, row in fake.artists.items() if row["slug"] == BRAVO]:
        fake._delete_artist(artist_id)
    with closing(sqlite3.connect(config.LOCAL_DB)) as conn, conn:  # and one who is sent has no neighbours at all
        conn.executemany("DELETE FROM style_vectors WHERE deviation_id = ?", [(work["id"],) for work in alone])
    counts = push.run(False)
    # ALPHA's row for BRAVO and BRAVO's for ALPHA. The row that says CHARLIE has none is not one of them.
    assert counts["neighbours_skipped_by_database"] == 2 and counts["artists_sent_without_neighbours"] == 1
    assert counts["neighbours_sent"] == 2 and fake.neighbors == []


# Running it again.


def test_running_twice_sends_the_same_rows_and_an_artist_who_stops_qualifying_is_deleted(fake):
    works = artist(ALPHA, 6, angle=0.0)
    gone = artist(BRAVO, 6, angle=0.1)
    artist(CHARLIE, 6, angle=0.2)
    first = push.run(False)
    held = fake.held()
    second = push.run(False)
    assert fake.loads[0] == fake.loads[1] and fake.held() == held
    assert fake.calls == ["listing", "removed", "load"] * 2  # nothing to delete the second time
    assert second == first and first["artists_deleted_from_database"] == first["works_deleted_from_database"] == 0
    assert len(fake.artists) == 3 and len(fake.works) == 18 and len(fake.neighbors) == 6

    for work in gone[:2]:  # 2 of 6 is a third: an adult portfolio now
        rate(work, "explicit")
    rate(works[3], "suggestive")  # and one work of an artist who stays
    third = push.run(False)

    assert fake.calls[-4:] == ["listing", "removed", "delete", "load"]
    assert third["artists_deleted_from_database"] == 1 and third["works_deleted_from_database"] == 1
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
@pytest.mark.parametrize("fail", ["listing", "removed", "delete", "load"])
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
    assert output.out == "" and "yy-fake" not in output.err
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
    assert fake.calls[calls:] == ["listing", "removed"] and fake.held() == held and len(fake.loads) == 1
    push.main([])
    real = capsys.readouterr().out

    assert dry == real and fake.calls[-4:] == ["listing", "removed", "delete", "load"]
    counts = json.loads(real)
    assert real.count("\n") == 1 and list(counts) == COUNTS
    assert all(type(value) is int for value in counts.values())  # counts only: nothing about any artist or work
    assert counts == dict.fromkeys(COUNTS, 0) | {
        "artists_sent": 1,
        "artists_held_not_profiled": 2,
        "works_sent": 5,
        "works_held_not_rated": 1,
        "works_held_artist_held_back": 10,
        "artists_sent_without_neighbours": 1,
        "artists_deleted_from_database": 1,
    }


# What the collect stage marks, and what it no longer deletes (round 3).

# The collect stage's words for a work that no longer qualifies -> the count line. Nothing here knows the words:
# the two about AI are ones it gave until its round 6 and may still be in a file, the last one is of no round yet.
WORK_MARKS = {
    "mature": "works_held_marked_mature",
    "tagged AI-made": "works_held_marked_tagged_ai_made",
    "not a public image": "works_held_marked_not_a_public_image",
    "another author's": "works_held_marked_another_author_s",
    "contact details": "works_held_marked_contact_details",
    "FAKE reason of a later round": "works_held_marked_fake_reason_of_a_later_round",
}


@pytest.mark.parametrize("reason", WORK_MARKS)
def test_a_marked_work_is_not_sent_and_is_counted_under_its_mark(fake, reason):
    works = artist(ALPHA, 8, angle=0.0)
    artist(BRAVO, angle=0.3)
    artist(CHARLIE, angle=0.6)
    first = push.run(False)
    assert first["works_sent"] == 8 + 5 + 5 and not any("marked" in name for name in first)

    assert library.mark_works(f"da:{ALPHA}", {works[2]["id"]: reason}) == 1  # as the collect stage does
    counts = push.run(False)

    assert counts[WORK_MARKS[reason]] == 1 and counts["works_sent"] == 7 + 5 + 5
    assert [name for name in counts if "marked" in name] == [WORK_MARKS[reason]]
    assert list(counts).index(WORK_MARKS[reason]) == list(counts).index("works_held_over_limit") + 1
    assert counts["works_deleted_from_database"] == 1
    mine = [row for row in fake.loads[-1][1] if row["artist_slug"] == ALPHA]
    assert [row["page_url"] for row in mine] == [work["page"] for n, work in enumerate(works) if n != 2]
    assert [row["position"] for row in mine] == list(range(7))
    # Not in the last load, in its search_text or top_tags, nor left in the database from the load before.
    assert not any(text in json.dumps(fake.loads[-1]).lower() for text in of_work(works[2]))
    assert not any(text in fake.held() for text in of_work(works[2]))

    library.mark_works(f"da:{ALPHA}", {works[2]["id"]: None})  # seen again, and it qualifies: the mark is gone
    again = push.run(False)
    assert again["works_sent"] == 8 + 5 + 5 and not any("marked" in name for name in again)


ARTIST_MARKS = {
    "account gone": "artists_held_marked_account_gone",
    "mostly AI-made": "artists_held_marked_mostly_ai_made",
}


@pytest.mark.parametrize("reason", ARTIST_MARKS)
def test_a_marked_artist_is_not_sent_not_even_as_a_neighbour_and_is_counted_under_their_mark(fake, reason):
    artist(ALPHA, angle=0.0)
    artist(BRAVO, angle=0.001, website=f"https://{BRAVO}.art/")  # the nearest in style to ALPHA
    artist(CHARLIE, angle=0.5)
    push.run(False)
    assert BRAVO in fake.held()

    library.mark_artist(f"da:{BRAVO}", reason)  # as the collect stage does: nothing of theirs is deleted here
    counts = push.run(False)

    assert library.artist_keys() == [f"da:{ALPHA}", f"da:{BRAVO}", f"da:{CHARLIE}"]
    assert counts[ARTIST_MARKS[reason]] == 1 and counts["artists_sent"] == 2
    assert [name for name in counts if "marked" in name] == [ARTIST_MARKS[reason]]
    assert list(counts).index(ARTIST_MARKS[reason]) == list(counts).index("artists_held_slug_unusable") + 1
    assert counts["works_held_artist_held_back"] == 5 and counts["artists_deleted_from_database"] == 1
    assert not any(word in json.dumps(fake.loads[-1]).lower() for word in of_artist(BRAVO))
    assert not any(
        word in fake.held() for word in of_artist(BRAVO)
    )  # their row, their works, their place as a neighbour
    assert [(row["artist_slug"], row["neighbor_slug"]) for row in fake.loads[-1][2]] == [
        (ALPHA, CHARLIE),
        (CHARLIE, ALPHA),
    ]


def test_the_handle_an_account_had_before_is_not_sent_once_it_is_kept_under_the_new_one(fake):
    artist(ALPHA, angle=0.0)
    artist(BRAVO, angle=0.2)
    push.run(False)
    # As the collect stage does it. First the API calls the old handle gone, and that is all that is known.
    library.mark_artist(f"da:{BRAVO}", "account gone")
    assert push.run(False)["artists_held_marked_account_gone"] == 1 and sorted(sent_artists(fake)) == [ALPHA]
    # Then the same account answers under another handle. The collector finds the key that account is kept
    # under, believes it because that handle is gone, and names it when it stores the new one.
    account = f"FAKE-USER-{BRAVO}"
    old_keys = [key for key in library.holders(account) if key != f"da:{CHARLIE}"]
    assert old_keys == [f"da:{BRAVO}"] and library.artist_marks()[f"da:{BRAVO}"][0] == "account gone"
    artist(CHARLIE, angle=0.21, user=BRAVO, old_keys=old_keys)
    assert library.artist_marks()[f"da:{BRAVO}"][::2] == ("renamed", f"da:{CHARLIE}")

    counts = push.run(False)

    assert counts["artists_held_marked_renamed"] == 1 and "artists_held_marked_account_gone" not in counts
    assert sorted(sent_artists(fake)) == [ALPHA, CHARLIE]
    assert not any(word in json.dumps(fake.loads[-1]).lower() for word in of_artist(BRAVO))
    assert not any(word in fake.held() for word in of_artist(BRAVO))


def test_a_works_thumbnail_size_is_sent_when_the_library_has_it_both_numbers_or_neither(fake):
    edits = {0: {"size": (300, 420)}, 2: {"size": (1, 100_000)}, 4: {"size": (640, 360), "rating": None}}
    works = artist(ALPHA, 7, edits=edits)
    assert library.thumb_sizes() == {
        works[0]["id"]: (300, 420),
        works[2]["id"]: (1, 100_000),
        works[4]["id"]: (640, 360),
    }
    # And two sizes the database would refuse, as a later round's library might come to hold them.
    with closing(sqlite3.connect(config.LOCAL_DB)) as conn, conn:
        conn.execute(
            "INSERT INTO work_sizes (deviation_id, width, height) VALUES (?, ?, ?)", (works[5]["id"], 100_001, 50)
        )
        conn.execute(
            "INSERT INTO work_sizes (deviation_id, width, height) VALUES (?, ?, ?)", (works[6]["id"], 50, 100_001)
        )

    push.run(False)

    rows = fake.loads[-1][1]
    assert all(set(row) == WORK_FIELDS for row in rows)  # both keys on every work: one shape a load
    assert {row["page_url"]: (row["width"], row["height"]) for row in rows} == {
        works[0]["page"]: (300, 420),
        works[1]["page"]: (None, None),  # a work stored before sizes were kept
        works[2]["page"]: (1, 100_000),
        works[3]["page"]: (None, None),
        works[5]["page"]: (None, None),  # one number the database would not take: neither is sent
        works[6]["page"]: (None, None),
    }
    assert all(
        type(side) is int and side > 0 for row in rows for side in (row["width"], row["height"]) if side is not None
    )


@pytest.mark.parametrize(
    ("size", "sent"),
    [
        ((300, 420), (300, 420)),
        ([300, 420], (300, 420)),
        ((100_000, 1), (100_000, 1)),
        (None, (None, None)),
        ((0, 420), (None, None)),
        ((300, 0), (None, None)),
        ((-300, 420), (None, None)),
        ((300, 100_001), (None, None)),
        ((300, None), (None, None)),
        ((None, 420), (None, None)),
        ((300.0, 420), (None, None)),
        (("300", "420"), (None, None)),
        ((True, 420), (None, None)),
        ((300,), (None, None)),
        ((300, 420, 3), (None, None)),
        ("300x420", (None, None)),
    ],
)
def test_a_size_is_two_whole_numbers_the_database_takes_or_it_is_no_size(size, sent):
    assert push._size(size) == sent


def test_a_work_that_was_not_in_the_artists_latest_answer_is_not_sent(fake):
    works = artist(ALPHA, 8)
    artist(BRAVO)
    push.run(False)
    assert fake.held().count("fake-page-yy-fake-alpha") == 8
    # The artist took one down. The collect stage stored them again since: the others were seen then, this one
    # was not, and nothing is deleted or marked for it. All there is to go on is when it was last seen.
    with closing(sqlite3.connect(config.LOCAL_DB)) as conn, conn:
        conn.execute(
            "UPDATE works SET fetched_at = '2026-01-01T00:00:00+00:00' WHERE deviation_id = ?", (works[1]["id"],)
        )
    assert library.work_marks() == {}

    counts = push.run(False)

    assert counts["works_held_not_seen_lately"] == 1 and counts["works_sent"] == 7 + 5
    assert counts["works_deleted_from_database"] == 1
    assert not any(text in json.dumps(fake.loads[-1]).lower() for text in of_work(works[1]))
    assert not any(text in fake.held() for text in of_work(works[1]))
    assert [row["position"] for row in fake.loads[-1][1] if row["artist_slug"] == ALPHA] == list(range(7))


def test_at_most_the_ten_most_recently_published_works_that_may_go_are_sent(fake):
    works = artist(
        ALPHA, 14, edits={0: {"rating": None}, 3: {"contact": "yes"}}
    )  # the newest and one more are held back
    artist(BRAVO, 13)
    counts = push.run(False)

    sent = [row for row in fake.loads[-1][1] if row["artist_slug"] == ALPHA]
    assert [row["page_url"] for row in sent] == [works[n]["page"] for n in (1, 2, 4, 5, 6, 7, 8, 9, 10, 11)]
    assert [row["position"] for row in sent] == list(range(10))
    assert len([row for row in fake.loads[-1][1] if row["artist_slug"] == BRAVO]) == 10
    assert counts["works_sent"] == 20 and counts["works_held_over_limit"] == 2 + 3
    assert counts["works_held_not_rated"] == counts["works_held_contact_in_words"] == 1
    # The older ones feed nothing either: not search_text, not top_tags.
    row = sent_artists(fake)[ALPHA]
    for left in (works[12], works[13]):
        assert not any(text in fake.sent() for text in of_work(left))
        assert left["title"] not in row["search_text"] and left["tags"][0] not in row["search_text"]
    own = [works[n]["tags"][0] for n in (1, 2, 4, 5, 6, 7, 8, 9, 10, 11)]
    assert sorted(row["top_tags"]) == sorted([f"zzall{squashed(ALPHA)}", *own])


# An adult portfolio, by the profile stage's count over the artist's sheet (round 3).


def sheets(monkeypatch, counted):
    """Make the profile stage hand over these counts: artist key -> (pictures on the sheet, unsuitable ones), or
    the fields to set. An artist not named has neither field, like a profile made before the count existed."""
    real = profiles.profiles

    def handed_over(db=None):
        rows = [{name: value for name, value in row.items() if "pictures" not in name} for row in real(db)]
        for row in rows:
            fields = counted.get(row["artist_key"], {})
            row |= fields if isinstance(fields, dict) else {"pictures": fields[0], "unsuitable_pictures": fields[1]}
        return rows

    monkeypatch.setattr(profiles, "profiles", handed_over)


@pytest.mark.parametrize(
    ("counted", "goes"),
    [
        ((10, 7), False),  # the artist a critic found: every work rated safe, most of the sheet pin-ups
        ((10, 3), False),  # 30 percent
        ((10, 2), True),
        ((7, 2), True),  # 28.6 percent
        ((3, 1), False),
        ((10, 0), True),
        ((0, 0), True),  # an empty sheet says nothing
        ({}, True),  # a profile from before the count: no field at all
        ({"pictures": None, "unsuitable_pictures": None}, True),
        ({"pictures": 10}, True),
    ],
)
def test_an_artist_whose_sheet_is_mostly_unsuitable_is_left_out_though_every_work_is_rated_safe(
    fake, monkeypatch, counted, goes
):
    artist(ALPHA, angle=0.0)
    artist(BRAVO, 10, angle=0.001)  # every one of their works is rated safe, and they are the nearest to ALPHA
    artist(CHARLIE, angle=0.5)
    sheets(monkeypatch, {f"da:{BRAVO}": counted, f"da:{ALPHA}": (10, 0)})

    counts = push.run(False)

    assert counts["artists_held_adult_portfolio"] == (0 if goes else 1)
    assert sorted(sent_artists(fake)) == ([ALPHA, BRAVO, CHARLIE] if goes else [ALPHA, CHARLIE])
    if not goes:
        assert counts["works_held_artist_held_back"] == 10
        assert not any(word in fake.sent() for word in of_artist(BRAVO))
        assert {row["neighbor_slug"] for row in fake.loads[-1][2]} == {ALPHA, CHARLIE}


def test_either_measure_of_an_adult_portfolio_is_enough(fake, monkeypatch):
    artist(ALPHA, 10, edits={n: {"rating": "suggestive"} for n in (0, 1, 2)})  # by the ratings; a clean sheet
    artist(BRAVO, 10)  # by the sheet; every rating safe
    artist(CHARLIE, 10, edits={n: {"rating": "suggestive"} for n in (0, 1)})  # 2 of 10 by each: by neither
    sheets(monkeypatch, {f"da:{ALPHA}": (10, 0), f"da:{BRAVO}": (10, 3), f"da:{CHARLIE}": (10, 2)})
    counts = push.run(False)
    assert sorted(sent_artists(fake)) == [CHARLIE] and counts["artists_held_adult_portfolio"] == 2


def test_the_profile_stage_hands_over_the_two_numbers_this_stage_reads(fake):
    artist(ALPHA, sheet=(10, 7))
    row = profiles.profiles()[0]
    if "unsuitable_pictures" not in row:
        pytest.skip("pipeline/profiles.py does not count the unsuitable pictures of a sheet yet")
    assert (row["pictures"], row["unsuitable_pictures"]) == (10, 7)
    assert push.run(False)["artists_held_adult_portfolio"] == 1 and fake.loads[-1][0] == []


# What stops a run (round 2).


@pytest.mark.parametrize("dry_run", [False, True])
def test_a_database_with_another_builders_test_rows_is_left_alone_and_nothing_is_done(fake, capsys, dry_run):
    artist(ALPHA)
    artist(BRAVO)
    seed(fake, 1, works=2, prefix="zz-test")
    fake.removed = [digest(f"da:{BRAVO}")]  # even this waits: their tests put hashes of their own on that list
    before, held = local_state(), fake.held()

    with pytest.raises(SystemExit) as stopped:
        push.main(["--dry-run"] if dry_run else ["--allow-delete", "999"])

    assert stopped.value.code not in (0, None) and 'begin "zz-"' in str(stopped.value.code)
    assert fake.calls == ["listing"] and fake.held() == held and local_state() == before
    assert capsys.readouterr().out == ""


@pytest.mark.parametrize("dry_run", [False, True])
def test_an_empty_local_library_stops_the_run_and_nothing_is_deleted(fake, capsys, dry_run):
    seed(fake, 3, works=2)  # what an earlier push left; the local file is new, or lost
    held = fake.held()
    with pytest.raises(SystemExit) as stopped:
        push.main(["--dry-run"] if dry_run else ["--allow-delete", "999"])
    assert stopped.value.code not in (0, None) and "no artists" in str(stopped.value.code)
    assert fake.calls == ["listing", "removed"] and fake.held() == held and capsys.readouterr().out == ""


def test_a_removal_is_not_held_up_by_an_empty_library_and_a_new_local_file_hears_of_it(fake):
    store.mark_removed("yy-fake-gone")  # removed here, before anything of theirs was collected
    seed(fake, 3)
    with pytest.raises(push.Stop):
        push.run(False)
    assert fake.removed == [digest("da:yy-fake-gone")] and len(fake.artists) == 3

    config.LOCAL_DB.unlink()  # this machine's file is lost, and made again by the next run
    with pytest.raises(push.Stop):
        push.run(False)
    assert store.is_removed("yy-fake-gone")
    assert not store.upsert_artist("yy-fake-gone", "https://www.deviantart.com/yy-fake-gone", [], 1)


@pytest.mark.parametrize("dry_run", [False, True])
def test_a_removal_made_here_is_added_to_the_databases_list(fake, dry_run):
    artist(ALPHA, angle=0.0)
    artist(BRAVO, angle=0.2, website=f"https://{BRAVO}.art/")
    artist(CHARLIE, angle=0.4)
    assert push.run(False)["removals_added_to_database"] == 0 and fake.removed == []
    store.mark_removed(BRAVO)

    counts = push.run(dry_run)

    # The hashes the store holds, as it holds them: the artist and their account (a handle removal bars no site).
    wanted = {digest(f"da:{BRAVO}"), library.account(f"FAKE-USER-{BRAVO}")}
    assert push._removed_here(None) == wanted
    assert counts["removals_added_to_database"] == 2 and counts["removed_artists_deleted_from_database"] == 1
    assert counts["artists_deleted_from_database"] == 0
    if dry_run:
        assert fake.removed == [] and BRAVO in fake.held()
        return
    assert set(fake.removed) == wanted and len(fake.removed) == 2
    assert not any(word in fake.held() for word in of_artist(BRAVO))
    assert fake.calls[-4:] == ["listing", "removed", "removal", "load"]
    again = push.run(False)  # sent once: the next run finds nothing to add
    assert again["removals_added_to_database"] == 0 and len(fake.removed) == 2 and "removal" not in fake.calls[-3:]


def test_a_removal_that_could_not_be_added_is_added_by_the_next_run_and_nothing_is_built_meanwhile(fake, monkeypatch):
    artist(ALPHA)
    artist(BRAVO)
    push.run(False)
    store.mark_removed(BRAVO)
    fake.fail = "removal"
    traced(fake, monkeypatch)
    with pytest.raises(SystemExit) as stopped:
        push.main([])
    assert stopped.value.code not in (0, None) and fake.calls[-1] == "removal" and not BUILDS & set(fake.calls)
    assert fake.removed == [] and len(fake.loads) == 1
    fake.fail = None
    assert push.run(False)["removals_added_to_database"] == 2 and len(fake.removed) == 2


def test_only_sha256_digests_are_written_into_the_statement_for_the_removal_list():
    good = {digest("da:yy-fake"), digest("site:yy-fake.art")}
    assert push._removed_sql(good).count("('") == 2
    for bad in ("FAKE", digest("da:yy-fake").upper(), digest("da:yy-fake") + "'); drop table public.removed; --"):
        with pytest.raises(ValueError):
            push._removed_sql(good | {bad})


def kept(monkeypatch, artists, pages):
    """Make the next push build exactly these rows, whatever the local stores hold."""
    monkeypatch.setattr(push, "build", lambda local=None: (artists, pages, [], push.Counter()))


@pytest.mark.parametrize(
    ("have", "gone", "dropped", "allow", "stops"),
    [
        (60, 6, 0, None, False),  # 10 percent of the artists
        (60, 7, 0, None, True),
        (60, 7, 0, 7, False),  # told that 7 may go
        (60, 7, 0, 6, True),
        (60, 0, 12, None, False),  # 10 percent of the 120 works of the artists who stay
        (60, 0, 13, None, True),
        (60, 0, 13, 13, False),
        (60, 0, 13, 12, True),
        (60, 6, 10, None, False),  # each kind is held to its own share: 6 of 60 artists, 10 of the 108 works left
        (60, 6, 11, None, True),
        (8, 5, 0, None, False),  # the floor: 5 may always go
        (8, 6, 0, None, True),
        (8, 8, 0, None, True),  # everybody
        (8, 1, 0, 0, True),  # told that nobody may
    ],
)
def test_more_would_be_deleted_than_is_believed_so_nothing_is_written(
    fake, monkeypatch, capsys, have, gone, dropped, allow, stops
):
    artist(ALPHA)  # the local library is not empty
    artists, pages = seed(fake, have, works=2)
    staying = artists[gone:]
    pages = [row for row in pages if row["artist_slug"] in {row["slug"] for row in staying}][dropped:]
    kept(monkeypatch, staying, pages)
    held = fake.held()
    allowed = [] if allow is None else ["--allow-delete", str(allow)]

    outcomes = []
    for arguments in (["--dry-run", *allowed], allowed):  # a dry run stops where the real one would
        try:
            push.main(arguments)
            outcomes.append((0, capsys.readouterr()))
        except SystemExit as stopped:
            outcomes.append((stopped.code, capsys.readouterr()))
    (dry_code, dry), (code, real) = outcomes

    counts = json.loads(real.out)
    assert dry.out == real.out and real.out.count("\n") == 1 and list(counts) == COUNTS
    assert counts["artists_deleted_from_database"] == gone and counts["works_deleted_from_database"] == dropped
    assert bool(code) is stops and bool(dry_code) is stops
    if stops:
        # Nothing at all was written, and the way on is named.
        assert fake.calls == ["listing", "removed"] * 2 and fake.held() == held and fake.loads == []
        assert f"--allow-delete {max(gone, dropped)}" in str(code) and str(code) == str(dry_code)
    else:
        assert len(fake.artists) == have - gone and len(fake.works) == 2 * (have - gone) - dropped
        assert fake.calls[2:] == ["listing", "removed", *(["delete"] if gone or dropped else []), "load"]


def test_the_share_is_of_the_artists_the_removal_list_leaves(fake, monkeypatch):
    artist(ALPHA)
    artists, pages = seed(fake, 60)
    barred, _ = seed(fake, 60, prefix="left-fake")  # as many again that the removal list bars
    fake.removed = [digest(f"da:{row['slug']}") for row in barred]
    kept(monkeypatch, artists[7:], pages)
    with pytest.raises(push.Stop, match="7 of the 60 artists"):  # 7 of 60 is too many; 7 of 120 would not be
        push.run(True)
    counts = push.run(False, allow_delete=7)
    assert counts["removed_artists_deleted_from_database"] == 60 and counts["artists_deleted_from_database"] == 7
    assert len(fake.artists) == 53


def test_the_guard_never_holds_up_a_removal(fake, capsys):
    handles = [f"yy-fake-g{n}" for n in range(8)]
    for n, handle in enumerate(handles):
        artist(handle, 4, angle=0.1 * n, website=f"https://{handle}.art/")
    push.run(False)
    assert len(fake.artists) == 8
    # Seven more whose hashes are on the database's list already: rows a removal made through the site had to
    # leave, because a load held them at that moment. (This also empties the fake's record of its loads.)
    lingering, _ = seed(fake, 7, prefix="left-fake")
    fake.removed = [digest(f"da:{row['slug']}") for row in lingering]
    store.mark_removed(handles[0])  # and one is removed here
    with closing(sqlite3.connect(config.LOCAL_DB)) as conn, conn:  # and six stop qualifying: more than a run may delete
        conn.executemany("DELETE FROM artist_profiles WHERE artist_key = ?", [(f"da:{h}",) for h in handles[1:7]])
    capsys.readouterr()

    with pytest.raises(SystemExit) as stopped:
        push.main([])

    counts = json.loads(capsys.readouterr().out)
    assert stopped.value.code not in (0, None) and "--allow-delete 6" in str(stopped.value.code)
    assert counts["removed_artists_deleted_from_database"] == 8 and counts["artists_deleted_from_database"] == 6
    # Stopped, and every removal was carried out all the same: the hashes are on the list of record, and the
    # eight artists it bars are gone. The six who only stopped qualifying are still there, and nothing was loaded.
    assert digest(f"da:{handles[0]}") in set(fake.removed)
    assert sorted(row["slug"] for row in fake.artists.values()) == handles[1:]
    assert not any(word in fake.held() for word in of_artist(handles[0])) and "left-fake" not in fake.held()
    assert fake.calls == ["listing", "removed", "removal"] and fake.loads == []

    push.main(["--allow-delete", "6"])  # the six go when the run is told they may
    assert sorted(row["slug"] for row in fake.artists.values()) == [handles[7]]
