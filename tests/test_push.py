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
import pathlib
import re
import shutil
import socket
import sqlite3
from array import array
from contextlib import closing
from datetime import UTC, datetime, timedelta
from urllib.parse import urlsplit

import httpx
import numpy as np
import pytest

from pipeline import config, crawl, library, profiles, push, store, style, tagging
from pipeline import db as real_db

ALPHA, BRAVO, CHARLIE, DELTA = "yy-fake-alpha", "yy-fake-bravo", "yy-fake-charlie", "yy-fake-delta"
VERIFIED = tagging.result_set(profiles.TAGGER, profiles.TAGGER)
JPEG = b"\xff\xd8\xff" + b"FAKE-IMAGE-BYTES-" * 20
AT = "2026-01-01T00:00:00+00:00"
PUBLISHED = 1_780_000_000  # work 0 of every artist; each later work is a day older
FACTS = {
    "specialties": ["character"],
    "mediums": ["digital painting"],
    "genres": ["fantasy"],
    "formats": ["portrait or bust"],
    "style_words": ["painterly"],
    "hedged": [],
}
ARTIST_FIELDS = {
    *("slug", "name", "da_username", "da_account", "site_url", "site_domain", "tagline", "top_tags", "search_text"),
    *("profile", "specialties", "mediums", "open_for_work", "profile_embedding"),
}
WORK_FIELDS = {
    *("artist_slug", "source", "page_url", "image_url", "title", "description", "tags", "position"),
    *("width", "height"),
}
NEIGHBOUR_FIELDS = {"artist_slug", "neighbor_slug", "rank", "score", "picks"}
COUNTS = [
    "artists_sent",
    *(f"artists_held_{reason}" for reason in push.ARTIST_REASONS),
    "works_sent",
    *(f"works_held_{reason}" for reason in push.WORK_REASONS),
    *("texts_left_out_for_contact", "taglines_left_out_profane", "taglines_left_out_social", "sheets_sent_as_filler"),
    "site_links_left_out_removed",
    *("neighbours_sent", "artists_sent_without_neighbours", "artists_skipped_by_database"),
    "neighbours_skipped_by_database",
    *("artists_deleted_from_database", "works_deleted_from_database", "neighbours_deleted_from_database"),
    *("removed_artists_deleted_from_database", "removals_added_to_database", "removals_applied_here"),
    *("artists_purged_here", "artists_left_to_purge_here"),
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
    artists (`skips`) and neighbour rows it skipped.
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
                artist["da_account"] = row.get("da_account")
                artist["neighbours"] = sum(near["artist_slug"] == row["slug"] for near in self.neighbors)
                mine = [(work_id, work) for work_id, work in self.works.items() if work["artist_slug"] == row["slug"]]
                rows += [artist | {"work_id": work_id, "page_url": work["page_url"]} for work_id, work in mine] or [
                    artist | {"work_id": None, "page_url": None}
                ]
            return rows
        # The only other SQL: key hashes for the removal list, deletes by id, and an artist's own words emptied.
        hashes = r"((?:\('[0-9a-f]{64}'\)(?:, )?)+)"
        insert = rf"insert into public\.removed \(key_hash\) values {hashes} on conflict \(key_hash\) do nothing"
        delete = r"delete from public\.(works|artists) where id in \(([0-9, ]+)\)"
        blank = r"update public\.artists set search_text = '', top_tags = '\{\}' where id in \(([0-9, ]+)\)"
        statements = query.split("; ")
        assert all(re.fullmatch(f"{insert}|{delete}|{blank}", statement) for statement in statements), query[:80]
        self._note("removal" if "insert" in query else "delete")
        for statement in statements:
            if added := re.fullmatch(insert, statement):
                new = [key_hash for key_hash in re.findall("[0-9a-f]{64}", added[1]) if key_hash not in self.removed]
                self.removed += new
                for artist_id, row in list(self.artists.items()):  # by handle, site or account (delete_removed)
                    if {*self.keys(row), row.get("da_account")} & set(new):
                        self._delete_artist(artist_id)
            elif emptied := re.fullmatch(blank, statement):
                for row_id in set(map(int, emptied[1].split(", "))) & set(self.artists):
                    self.artists[row_id] |= {"search_text": "", "top_tags": []}
            else:
                table, ids = re.fullmatch(delete, statement).groups()
                for row_id in map(int, ids.split(", ")):  # an id that is no longer there deletes nothing
                    self.works.pop(row_id, None) if table == "works" else self._delete_artist(row_id)
        return []

    def load(self, artists, works, neighbors):
        self._note("load")
        # A row the schema refuses fails the whole batch, as artists_own_words_sizes does (sql/001_schema.sql).
        for row in artists:
            tags = row.get("top_tags") or []
            if len(row.get("tagline") or "") > 300 or len(row.get("search_text") or "") > 4000:
                raise RuntimeError("HTTP 400: FAKE violates check constraint artists_own_words_sizes")
            if len(tags) > 40 or any(tag is None or len(tag) > 60 for tag in tags):
                raise RuntimeError("HTTP 400: FAKE violates check constraint artists_own_words_sizes")
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
        return len({row["slug"] for row in artists} & self.skips), len(neighbors) - len(known)

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
    # The invented tags below, which begin "zz", are on the reviewed list (profiles.APPROVED) as far as these
    # tests go, so that every other rule is seen to hold them back. The list's own tests begin no tag so.
    reviewed = profiles.approved
    monkeypatch.setattr(profiles, "approved", lambda tag: tag.startswith("zz") or reviewed(tag))
    database = FakeDb()
    monkeypatch.setattr(push, "db", database)
    return database


def squashed(handle):
    return handle.replace("-", "").replace("_", "")


def seen(work):
    """The fingerprint of the image artist() puts in the cache for a work."""
    return library.fingerprint(JPEG + work["id"].encode())


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
    account="fine",
    website=None,
    links=None,
    angle=0.0,
    edits=None,
    user=None,
    old_keys=(),
    sheet=None,
    counted=None,
):
    """Collect, tag, rate and profile one invented artist, and return their works, the most recent first.

    Every work is fit to send unless `edits` says otherwise for its place in the list: "result": None (no
    verified tags), "explicit" or "not_artwork": True, "declared" (AI: "yes", "unclear", None for not read),
    "contact" ("yes", None for not read), "rating" ("suggestive", "explicit", None for not rated), "artwork"
    (False when the artwork check found it is not, None for not checked), "fit" (False when the content check
    found it unfit, None for not checked), "formats" (from tagging.FORMATS), "title", "tags" (more of the artist's
    own, after the two every work has), "description" (the verified one), "thumb", "blood_only" (True or
    False: why a work rated suggestive is so was asked; not asked without it) and "political" (True or False: the
    political check was asked; not asked without it, which for a title of no political word is as False).
    And "size": its thumbnail's (width, height), as the API gave it: without one, none is kept for the work.
    profile: "written", "excluded" (the profile stage ruled them out) or None (no row there yet).
    account: what their username and tagline were read to say (tagging.ACCOUNT_ANSWERS), or None for not read.
    user: the handle whose account this is, when it is an account that took another handle, and old_keys: the
    keys the collector found that account kept under, which it names when it stores the new one.
    sheet: (pictures on the sheet the profile stage looked at, how many of them it counted as unsuitable).
    counted: how many of the works, from the most recent, the profile was made from. Without it the profile
    rests on no work, so nothing that happens to a work makes it out of date.
    Every reading is of the work as it is collected here: it carries the fingerprint of the image, or of the
    title and tags, or of the tagline. Everything about the artist carries their handle, with or without its
    hyphens, so it can be looked for.
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
            "title": edit.get("title", f"FAKE-TITLE {handle} {n}"),
            "tags": [f"zzown{word}{n}", f"zzall{word}", *edit.get("tags", [])],
            "thumb": edit.get("thumb", f"https://images-wixmp-yyfake.wixmp.com/f/FAKE-THUMB-{handle}-{n}.jpg"),
            "description": edit.get("description", f"FAKE-DESCRIPTION {handle} {n}."),
        }
        made.append(work)
        library.save_image(work["id"], "image/jpeg", JPEG + work["id"].encode())
        # What each reading was made from: the image in the cache, or the title and tags.
        image, words = seen(work), tagging.words_print(work["title"], json.dumps(work["tags"]))
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
            rows.append(("work_tags", (work["id"], VERIFIED, tagging.PROMPT_VERSION, json.dumps(result), AT, image)))
        if declared := edit.get("declared", "no"):
            read = (work["id"], profiles.TAGGER, tagging.AI_PROMPT_VERSION, declared, AT, words)
            rows.append(("work_ai_declared", read))
        if contact := edit.get("contact", "no"):
            read = (work["id"], profiles.TAGGER, tagging.CONTACT_PROMPT_VERSION, contact, AT, words)
            rows.append(("work_contact", read))
        if rating := edit.get("rating", "safe"):
            sorted_as = json.dumps({"formats": edit.get("formats", ["portrait or bust"]), "rating": rating})
            rows.append(("work_format", (work["id"], profiles.TAGGER, tagging.FORMAT_VERSION, sorted_as, AT, image)))
        if (art := edit.get("artwork", True)) is not None:
            checked = json.dumps({"kind": tagging.ARTWORK if art else "screenshot", "artwork": art})
            rows.append(("work_artwork", (work["id"], profiles.TAGGER, tagging.ARTWORK_VERSION, checked, AT, image)))
        if (fit := edit.get("fit", True)) is not None:
            checked = json.dumps({"shows": tagging.FIT if fit else "fetish", "fit": fit})
            rows.append(("work_content", (work["id"], profiles.TAGGER, tagging.CONTENT_VERSION, checked, AT, image)))
        if "blood_only" in edit:
            why = json.dumps({"shows": tagging.BLOOD, "blood_only": edit["blood_only"]})
            rows.append(("work_suggestive", (work["id"], profiles.TAGGER, tagging.SUGGESTIVE_VERSION, why, AT, image)))
        if "political" in edit:
            checked = json.dumps({"shows": "none", "named": "no", "political": edit["political"]})
            both = tagging.political_print(image, work["title"], json.dumps(work["tags"]))
            rows.append(("work_political", (work["id"], profiles.TAGGER, tagging.POLITICAL_VERSION, checked, AT, both)))
        rows.append(("style_vectors", (work["id"], "csd", style_vector(angle, n), image)))
    if tagline_contact:
        read = tagging.tagline_print(f"FAKE-TAGLINE {handle}")
        rows.append(
            ("artist_contact", (key, profiles.TAGGER, tagging.CONTACT_PROMPT_VERSION, tagline_contact, AT, read))
        )
    if account:
        read = tagging.account_print(key, f"FAKE-TAGLINE {handle}")
        rows.append(("artist_account", (key, profiles.TAGGER, tagging.ACCOUNT_PROMPT_VERSION, account, AT, read)))

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
    if profile:  # last, from the readings there are now, as the profile stage makes a row
        vector = array("f", [1.0, *[0.0] * (profiles.DIMENSIONS - 1)]).tobytes()
        texts = (
            (None,) * 6
            if profile == "excluded"
            else (f"FAKE-TEMPLATE {handle}.", profiles.TEMPLATE_VERSION, f"FAKE-PROFILE {handle}.", 1, 1)
        )
        texts = texts if profile == "excluded" else (*texts, vector)
        reason = "nothing_clear" if profile == "excluded" else None
        facts = FACTS | ({"sheet": {"pictures": sheet[0], "unsuitable": sheet[1]}} if sheet else {})
        # The artist's own tags the site shows as theirs, as the profile stage picks them: the most used first,
        # of those on the reviewed list as it is (this file's fixture puts the invented ones on it), each as the
        # list spells it.
        own = [profiles._compared(tag) for tag in (f"zzall{word}", *(work["tags"][0] for work in made))]
        facts |= {"own_words": [tag for tag in own if profiles.approved(tag)]}
        # What the profile rests on, as the profile stage keeps it: the works it counted, each as it was read.
        facts |= counted_as(sorted(work["id"] for work in made[: counted or 0]))
        said = {work["id"]: edits.get(n, {}).get("declared", "no") for n, work in enumerate(made)}
        basis = "FAKE-BASIS" + tagging.read_as(facts["counted"], {work["id"]: seen(work) for work in made}, said)
        with closing(sqlite3.connect(config.LOCAL_DB)) as conn, conn:
            row = (key, basis, reason, json.dumps(facts), "[]", *texts, AT)
            conn.execute(f"INSERT OR REPLACE INTO artist_profiles VALUES ({', '.join('?' * len(row))})", row)
    return made


def counted_as(ids):
    """What the profile stage keeps of the works it counted, as their verified tags read now (profiles.current):
    which of them count, which values each has, and the counts."""
    results, declared = tagging.results(profiles.TAGGER, verifier=profiles.TAGGER), tagging.declared_ai(profiles.TAGGER)
    facts, _ = profiles.facts_of([{"id": i, "result": results.get(i), "declared": declared.get(i)} for i in ids])
    return {"counted": facts["counted"], "tagged": facts["tagged"], "counts": facts["counts"]}


def rate(work, rating):
    """The tagging stage rates a work again."""
    sorted_as = json.dumps({"formats": [], "rating": rating})
    with closing(sqlite3.connect(config.LOCAL_DB)) as conn, conn:
        conn.execute("UPDATE work_format SET result = ? WHERE deviation_id = ?", (sorted_as, work["id"]))


def read_again(*works, handle=None):
    """The tagging stage reads words that have changed, and answers what it answered before: its readings of
    these works' titles and tags, and of this artist's tagline (with their username), are now readings of the words
    as they are."""
    with closing(sqlite3.connect(config.LOCAL_DB)) as conn, conn:
        for work in works:
            words = conn.execute("SELECT title, tags FROM works WHERE deviation_id = ?", (work["id"],)).fetchone()
            for table in ("work_ai_declared", "work_contact"):
                read = (tagging.words_print(*words), work["id"])
                conn.execute(f"UPDATE {table} SET fingerprint = ? WHERE deviation_id = ?", read)
        if handle:
            key = store.artist_key(handle)
            (tagline,) = conn.execute("SELECT tagline FROM profiles WHERE artist_key = ?", (key,)).fetchone()
            read = (tagging.tagline_print(tagline), key)
            conn.execute("UPDATE artist_contact SET fingerprint = ? WHERE artist_key = ?", read)
            read = (tagging.account_print(key, tagline), key)
            conn.execute("UPDATE artist_account SET fingerprint = ? WHERE artist_key = ?", read)


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
        (style, "all_closest_works"),
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
BUILDS = {"profiles", "excluded", "results", "contacts", "_collected", "all_neighbors", "all_closest_works"}


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
def test_a_removal_made_through_the_site_is_written_down_and_purged_before_any_row_is_built(fake, monkeypatch, dry_run):
    artist(ALPHA, angle=0.0)
    removed = artist(BRAVO, angle=0.01, website=f"https://{BRAVO}.art/")
    artist(CHARLIE, angle=0.5)
    push.run(False)
    # A proof bars the account (sql/002_functions.sql: grant_request), and the database deletes the artist then, or
    # with the next sweep when a load held the row at that moment: here it is still there for this run.
    account = library.account(f"FAKE-USER-{BRAVO}")
    fake.removed, fake.calls, fake.loads = [account], [], []
    traced(fake, monkeypatch)
    before = local_state()

    counts = push.run(dry_run)

    # A dry run says what the run would do, to the number, and does none of it.
    assert counts["removed_artists_deleted_from_database"] == 1 and counts["artists_purged_here"] == 1
    assert counts["removals_added_to_database"] == 0 and counts["artists_sent"] == 2
    if dry_run:
        assert local_state() == before and fake.loads == [] and fake.removed == [account] and BRAVO in fake.held()
        assert not {"import_removed", "purge", "delete"} & set(fake.calls)
        return
    # Here, then there, and only then is a file deleted: all of it before the first row is built.
    assert fake.calls[:5] == ["listing", "removed", "import_removed", "delete", "purge"]
    assert not {"import_removed", "purge", "delete"} & set(fake.calls[5:]) and fake.calls[5] in BUILDS
    # Nothing of theirs is left here: no row, no image, and the account is barred, which bars them under any handle.
    # No handle goes on a list while the account is known, and a person's removal bars no site (it must not travel
    # to other artists who list the same one).
    assert f"da:{BRAVO}" not in library.artist_keys()
    assert not any(path.name.startswith(work["id"]) for work in removed for path in library.image_dir().iterdir())
    assert not store.is_removed(BRAVO) and not store.is_removed(f"{BRAVO}.art")
    assert library.account_removed(f"FAKE-USER-{BRAVO}")
    assert [key for key, _, _ in store.all_artists()] == [f"da:{ALPHA}", f"da:{CHARLIE}"]
    assert set(fake.removed) == {account} == store.removed_hashes()
    assert sorted(sent_artists(fake)) == [ALPHA, CHARLIE] and BRAVO not in fake.held()
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

    assert counts["removed_artists_deleted_from_database"] == 1 and counts["artists_deleted_from_database"] == 0
    if how != "here_before_the_push":
        # A removed site is no removed artist (the fifth critic's): the row that showed it goes, and BRAVO, who
        # merely names it, is sent again without it.
        assert sorted(sent_artists(fake)) == [ALPHA, BRAVO, CHARLIE] and sent_artists(fake)[BRAVO]["site_url"] is None
        assert f"{BRAVO}.art" not in fake.held() and counts["site_links_left_out_removed"] == 1
        return
    assert sorted(sent_artists(fake)) == [ALPHA, CHARLIE]
    assert not any(word in json.dumps(fake.loads[-1]).lower() for word in of_artist(BRAVO))
    assert not any(word in fake.held() for word in of_artist(BRAVO))  # their row, their works, and as a neighbour
    assert counts["artists_sent"] == 2 and counts["works_sent"] == 10 and counts["neighbours_sent"] == 2


def test_a_removal_list_that_cannot_be_applied_stops_the_run_before_anything_is_built(fake, monkeypatch, capsys):
    artist(ALPHA)
    fake.removed = ["FAKE-NOT-A-HASH"]
    traced(fake, monkeypatch)
    with pytest.raises(SystemExit) as stopped:
        push.main([])
    assert stopped.value.code not in (0, None)
    assert fake.calls == ["listing", "removed"] and capsys.readouterr().out == ""  # read, refused, nothing written


def held_open(monkeypatch, work):
    """Somebody holds a work's cached image open, so it cannot be deleted: an image viewer, or another stage.
    Returns the list to empty when they let go."""
    held, unlink = [work["id"]], type(library.image_dir()).unlink

    def refused(path, *args, **kwargs):
        if path.stem in held:
            raise PermissionError("FAKE: the file is in use")
        return unlink(path, *args, **kwargs)

    monkeypatch.setattr(type(library.image_dir()), "unlink", refused)
    return held


@pytest.mark.parametrize("where", ["here", "through_the_site"])
def test_a_file_that_cannot_be_deleted_never_holds_up_the_removal_and_is_tried_again(fake, monkeypatch, capsys, where):
    artist(ALPHA, angle=0.0)
    gone = artist(BRAVO, angle=0.2)
    artist(CHARLIE, angle=0.4)
    push.run(False)
    assert BRAVO in fake.held()
    if where == "here":
        store.mark_removed(BRAVO)
    else:  # a proof: the account
        fake.sql(push._removed_sql({library.account(f"FAKE-USER-{BRAVO}")}))
    held = held_open(monkeypatch, gone[0])
    wanted = {library.account(f"FAKE-USER-{BRAVO}"), *([digest(f"da:{BRAVO}")] if where == "here" else [])}

    for _ in range(2):  # run after run while the file is held: the removal stands, and the push is done
        capsys.readouterr()
        with pytest.raises(SystemExit) as stopped:
            push.main([])
        counts = json.loads(capsys.readouterr().out)
        assert "not finished" in str(stopped.value.code) and "yy-fake" not in str(stopped.value.code)
        assert counts["artists_left_to_purge_here"] == 1 and counts["artists_purged_here"] == 0
        assert set(fake.removed) == wanted == store.removed_hashes() and len(fake.removed) == len(wanted)
        assert not any(word in fake.held() for word in of_artist(BRAVO))
        assert sorted(sent_artists(fake)) == [ALPHA, CHARLIE] and counts["artists_sent"] == 2
        # Everything else of theirs went all the same: the one work whose file is held is all that is left.
        assert library.work_ids(f"da:{BRAVO}") == {gone[0]["id"]}
        assert [path.stem for path in library.image_dir().iterdir() if path.stem in {w["id"] for w in gone}] == held

    held.clear()  # they let go, and the next run finishes it
    counts = push.run(False)
    assert counts["artists_purged_here"] == 1 and counts["artists_left_to_purge_here"] == 0
    assert f"da:{BRAVO}" not in library.artist_keys() and counts["removals_added_to_database"] == 0
    assert not any(path.stem == gone[0]["id"] for path in library.image_dir().iterdir())
    assert set(fake.removed) == wanted and library.account_removed(f"FAKE-USER-{BRAVO}")
    again = push.run(False)
    assert again["artists_purged_here"] == again["artists_left_to_purge_here"] == 0


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
    "checked_not_artwork": {"artwork": False},
    "artwork_not_checked": {"artwork": None},
    "unfit_image": {"fit": False},
    "fit_not_checked": {"fit": None},
    "unfit_title": {"title": "FAKE-TITLE Vore-verse: zz"},
    "unfit_title glued to other words": {"title": "FAKE-TITLE spaceNaziMechs zz"},
    "unfit_title with a glued kkk": {"title": "FAKE-TITLE zztheKKKriders zz"},  # the eighth critic's uncaught break
    "unfit_tag": {"tags": ["NSFW"]},  # the artist's own tag, in any case
    "unfit_tag glued to other words": {"tags": ["zzHugeBoobs"]},
    "profane_title": {"title": "FAKE-TITLE Piss off zz"},
    "profane_title glued": {"title": "FAKE-TITLE zzfuckery zz"},
    "explicit": {"explicit": True},
    "explicit by rating": {"rating": "explicit"},
    "contact_in_words": {"contact": "yes"},
    "words_not_read": {"contact": None},
    "not_rated": {"rating": None},
    "suggestive": {"rating": "suggestive", "blood_only": False},
    "suggestive_not_checked": {"rating": "suggestive"},
    "political": {"political": True},
    "political_not_checked": {"title": "FAKE-TITLE Election poster zz"},  # a candidate nobody has asked about
    "bad_address": {"thumb": "https://yy-fake.deviantart.net/f/FAKE-THUMB-NOT-ON-WIXMP.jpg"},
    "service_in_title": {"title": "FAKE-TITLE zz now on Instagram"},
    "service_in_title spelled out": {"title": "FAKE-TITLE zz our Dis-cord server is back"},
    "service_in_title bluesky": {"title": "FAKE-TITLE zz now on Bluesky"},  # the eighth critic's
    "held_by_hand": {},  # its id in the owner's list (held_by_hand, below)
}  # and declared_ai, or no reading of it: one such work holds back its artist (ai_declared_work, ai_not_read, below)


def held_by_hand(*lines):
    """The owner's list of what is held back by hand, in the cache, as these lines."""
    (config.CACHE_DIR / push.HELD_BY_HAND).write_text("\n".join(lines), encoding="utf-8")


def test_a_tag_built_from_an_unfit_word_holds_its_work_and_a_word_inside_another_holds_nothing(fake):
    """The eighth critic's: 162 sent works carried a sexual or fetish tag the list of whole tags missed ("nude",
    "bigboobs", "hypnosismindcontrol"). Tags glue words together; a word that is inside other words too counts only
    at either end of a run of letters, or as the whole of one."""
    unfit = [
        *("nude", "Sexy", "boobs", "bigboobs", "hugeboobs", "boobsandbutts", "milf", "pinup", "pin_up", "bimbo"),
        *("bondagefetish", "stockstickling", "hypnosismindcontrol", "zzpornart", "nsfwzz", "zzfutanari", "futa"),
        *("ecchigirl", "nudity", "softvore", "vore_zz", "zzinflation", "bdsm", "lingerie", "bigbreasts", "bigbutt"),
        *("thiccthighs", "footfetish", "ass", "zz_ass", "R-18", "zznazizombies", "zzKKKriders"),
    ]  # and not "feet" alone, which held concept art of robot feet: a foot fetish says "fetish"
    fine = [
        *("carnivore", "omnivore", "herbivore", "glass", "class", "bass", "assassin", "badass", "therapist"),
        *("arlecchino", "futaba", "button", "headbutt", "butterfly", "menudesign", "snakedragon", "treeskink"),
        *("boobytrap", "breastplate", "lolita", "hypnos", "gothiccomic", "emilfanart", "foodporn", "inflatable"),
        *("photoshoppainting", "skyscraper", "sharkkkk", "inktober18", "feet", "robotfeet"),
    ]
    assert [tag for tag in unfit if not push.UNFIT_TAG.search(tag.casefold())] == []
    assert [tag for tag in fine if push.UNFIT_TAG.search(tag.casefold())] == []
    artist(ALPHA, 8, edits={2: {"tags": fine}})
    artist(BRAVO, angle=0.3)
    counts = push.run(False)
    assert counts["works_held_unfit_tag"] == 0 and counts["works_sent"] == 8 + 5


@pytest.mark.parametrize("why", HELD)
def test_a_held_back_work_appears_nowhere_in_what_is_sent(fake, why):
    works = artist(ALPHA, 8, edits={2: HELD[why]})
    if why == "held_by_hand":
        held_by_hand("# a comment, and a blank line", "", f"  {works[2]['id'].lower()}  # its id, in any case")
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


def test_a_work_rated_suggestive_for_blood_alone_is_sent_and_counts_as_safe_in_the_artists_share(fake):
    """The owner's (2026-10-20): blood shows, pin-ups do not. Three works of ten rated suggestive is 30 percent."""
    artist(ALPHA, 10, edits={n: {"rating": "suggestive", "blood_only": True} for n in (1, 4, 7)}, angle=0.0)
    artist(BRAVO, 10, edits={n: {"rating": "suggestive", "blood_only": False} for n in (1, 4, 7)}, angle=0.5)
    artist(CHARLIE, 10, edits={n: {"rating": "suggestive"} for n in (1, 4, 7)}, angle=1.0)  # not asked why yet
    counts = push.run(False)
    assert list(sent_artists(fake)) == [ALPHA] and counts["works_sent"] == 10
    assert counts["artists_held_adult_portfolio"] == 2
    assert counts["works_held_suggestive"] == counts["works_held_suggestive_not_checked"] == 3


def test_a_blood_only_reading_of_the_picture_a_work_showed_before_counts_for_nothing_in_the_share(fake):
    """The seventh critic's uncaught break: the share took why a work is suggestive from a reading of another
    picture. Three of ten rated suggestive, each read as blood alone, but of the picture they showed before."""
    artist(ALPHA, 10, edits={n: {"rating": "suggestive", "blood_only": True} for n in (1, 4, 7)}, angle=0.0)
    artist(BRAVO, 10, angle=0.5)
    with closing(sqlite3.connect(config.LOCAL_DB)) as conn, conn:
        conn.execute("UPDATE work_suggestive SET fingerprint = 'FAKE-FINGERPRINT-OF-THE-PICTURE-BEFORE'")
    counts = push.run(False)
    assert list(sent_artists(fake)) == [BRAVO] and counts["artists_held_adult_portfolio"] == 1
    assert counts["works_held_suggestive_not_checked"] == 3


def test_the_words_that_declare_a_work_ai_made_and_those_that_refuse_it():
    """The seventh critic's: a title saying "Ai realistic" and a tag "aicreature" were read "no", and a picture
    captioned "AI-assisted" was described so. The words say it, glued or spaced; words that refuse it say nothing,
    and neither does "ai" inside another word, or a tag that is "ai" and nothing else (a robot is tagged so)."""
    said = [
        ("FAKE zz (Ai realistic render)", []),
        ("FAKE", ["aicreature"]),
        ("FAKE", ["AIart"]),
        ("FAKE", ["ai_generated"]),
    ]
    said += [("FAKE AI-assisted", []), ("FAKE made with AI", []), ("FAKE", ["midjourneyart"]), ("FAKE (AI)", [])]
    said += [("FAKE", ["stable_diffusion"]), ("FAKE", ["novelai"]), ("FAKE DALL-E 3 test", []), ("FAKE ai art", [])]
    refused = ["no AI", "NOT AI generated", "anti-ai", "AI-free", "0% AI", "made without AI", "human made", "noai"]
    unsaid = [("FAKE Raijin", ["samurai", "kaidan", "aircraft", "aisha"]), ("FAKE android", ["ai", "robot"])]
    assert all(push._ai_words(title, json.dumps(tags), None) for title, tags in said)
    assert push._ai_words("FAKE", "[]", "FAKE a picture captioned AI-assisted.")
    assert not any(push._ai_words(f"FAKE {words}", json.dumps(["aiart"]), None) for words in refused)
    assert not any(push._ai_words(title, json.dumps(tags), "FAKE a robot.") for title, tags in unsaid)


def test_an_artist_whose_ai_declaring_work_changed_its_words_is_held_until_they_are_read_again(fake):
    """The seventh critic's: a work read as declaring AI holds its artist back; the artist edits its title, and
    until the tagging stage reads it again there is no reading of it as it is, and the artist was sent."""
    made = artist(ALPHA, 6, edits={0: {"declared": "yes"}}, angle=0.0)
    artist(BRAVO, 6, angle=0.3)
    assert push.run(False)["artists_held_ai_declared_work"] == 1
    collected_again(ALPHA, made, titles={0: "FAKE-TITLE zz the second version"})
    counts = push.run(False)
    assert counts["artists_held_ai_not_read"] == 1 and list(sent_artists(fake)) == [BRAVO]
    read_again(made[0])  # read again, and it says what it said
    assert push.run(False)["artists_held_ai_declared_work"] == 1 and list(sent_artists(fake)) == [BRAVO]


def test_no_list_held_by_hand_holds_nothing_back():
    assert not (config.CACHE_DIR / push.HELD_BY_HAND).exists() and push.held_by_hand() == set()


# Which artists go.

LEFT_OUT = {
    "not_profiled": {"profile": None},
    "profile_excluded": {"profile": "excluded"},
    "declares_ai": {"account": "ai"},
    "adult_account": {"account": "adult"},
    "account_not_read": {"account": None},
    "adult_portfolio": {"count": 10, "edits": {n: {"rating": "suggestive"} for n in (1, 4, 7)}},  # 3 of 10 rated
    "too_few_works": {"count": 5, "edits": {0: {"rating": None}, 3: {"contact": "yes"}}},  # 3 left
    "ai_declared_work": {"count": 10, "edits": {6: {"declared": "yes", "result": None}}},  # one of ten, untagged
    # The model read "no", and the words say so all the same (AI_WORDS), in a title, a tag or the description sent.
    "ai_declared_work by its title": {"count": 10, "edits": {6: {"title": "FAKE-TITLE zz (Ai realistic render)"}}},
    "ai_declared_work by a tag": {"count": 10, "edits": {6: {"tags": ["zzkaiju", "aicreature"]}}},
    "ai_declared_work by the description": {
        "count": 10,
        "edits": {6: {"description": "FAKE-DESCRIPTION a zz picture captioned AI-assisted."}},
    },
    "ai_not_read": {"count": 10, "edits": {6: {"declared": None}}},  # its words have no reading as they are
    "no_account": {},  # no account kept for them (below)
    "held_by_hand": {},  # their key in the owner's list (below)
}


@pytest.mark.parametrize("why", LEFT_OUT)
def test_an_artist_left_out_appears_nowhere_in_what_is_sent_not_even_as_a_neighbour(fake, why):
    artist(ALPHA, angle=0.0)
    artist(BRAVO, angle=0.001, website=f"https://{BRAVO}.art/", **LEFT_OUT[why])  # the nearest in style to ALPHA
    artist(CHARLIE, angle=0.5)
    if why == "no_account":  # the tenth removals critic's: a removal of theirs could bar only a handle
        with closing(sqlite3.connect(config.LOCAL_DB)) as conn, conn:
            conn.execute("UPDATE profiles SET user_hash = NULL WHERE artist_key = ?", (f"da:{BRAVO}",))
    if why == "held_by_hand":
        held_by_hand(f"DA:{BRAVO.upper()}", "00000000-0000-4000-A000-FAKE-NOBODYS")
    assert style.neighbors(f"da:{ALPHA}", "csd", 1, "local")[0][0] == f"da:{BRAVO}"

    counts = push.run(False)

    assert counts["artists_sent"] == 2 and counts[f"artists_held_{why.split()[0]}"] == 1
    assert sorted(sent_artists(fake)) == [ALPHA, CHARLIE]
    assert not any(word in fake.sent() for word in of_artist(BRAVO))
    assert [(row["artist_slug"], row["neighbor_slug"], row["rank"]) for row in fake.loads[-1][2]] == [
        (ALPHA, CHARLIE, 1),
        (CHARLIE, ALPHA, 1),
    ]


def test_an_artist_held_by_hand_stays_held_under_a_handle_their_account_takes(fake):
    """The eighth critic's: the owner's list named BRAVO's key, the account took the handle CHARLIE, and the list
    matched by key, so CHARLIE was sent."""
    artist(ALPHA, angle=0.0)
    artist(BRAVO, angle=0.3)
    held_by_hand(f"da:{BRAVO}  # the owner's")
    push.run(False)
    assert list(sent_artists(fake)) == [ALPHA]
    library.mark_artist(f"da:{BRAVO}", "account gone")  # as the collect stage finds the account under a new handle
    artist(CHARLIE, angle=0.31, user=BRAVO, old_keys=[f"da:{BRAVO}"])
    artist(DELTA, angle=0.6)  # and somebody else

    counts = push.run(False)

    assert sorted(sent_artists(fake)) == [ALPHA, DELTA] and counts["artists_held_held_by_hand"] == 1
    assert not any(word in fake.sent() for word in of_artist(CHARLIE))


def test_the_thresholds_are_the_named_ones(fake):
    assert (push.MIN_WORKS, push.MAX_WORKS, push.ADULT_PERCENT, push.NEIGHBOURS) == (4, 10, 30, 24)
    assert (push.DELETE_PERCENT, push.DELETE_FLOOR, push.TEST_SLUG) == (10, 5, "zz-")
    # Exactly 4 works that may go, with 2 of 7 suggestive or not rated yet (28.6 percent): sent.
    artist(ALPHA, 7, edits={0: {"rating": "suggestive"}, 1: {"rating": None}, 2: {"not_artwork": True}})
    # Fewer than 30 percent, and one work short.
    artist(BRAVO, 10, edits={0: {"rating": None}, 1: {"rating": None}} | {n: {"contact": "yes"} for n in range(2, 7)})
    # 3 of 10 is 30 percent: out, though 7 works could go.
    artist(CHARLIE, 10, edits={n: {"rating": "explicit"} for n in (0, 1, 2)})
    # 30 percent of the rated works, whatever else holds them back: 3 of 10 again, two of them not artwork.
    artist(
        DELTA,
        10,
        edits={0: {"rating": "suggestive"}} | {n: {"rating": "explicit", "not_artwork": True} for n in (1, 2)},
    )
    # And 3 of 10 where two are only not rated yet: out as well, and not called an adult portfolio.
    artist("yy-fake-echo", 10, edits={0: {"rating": "suggestive"}, 1: {"rating": None}, 2: {"rating": None}})
    counts = push.run(False)
    assert sorted(sent_artists(fake)) == [ALPHA]
    assert counts["works_sent"] == 4
    assert counts["artists_held_too_few_works"] == 1 and counts["artists_held_adult_portfolio"] == 2
    assert counts["artists_held_not_all_rated"] == 1
    assert counts["works_held_artist_held_back"] == 3 + 7 + 7 + 7


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


def test_a_handle_that_swears_holds_back_its_artist_and_mild_words_or_words_inside_others_hold_back_nothing(fake):
    """The sixth critic's: two real handles with "fuck" and "shit" were sent. The handle is the artist's name and
    slug on the site; it is words glued together, so a swear word counts at either end of a run of letters."""
    swearing = ["yy-fake-fuckit", "yy-fake-shitbird", "yy-fake-grimbastard"]
    fine = ["yy-fake-goshitsme", "yy-fake-damnit", "yy-fake-scunthorpe", "yy-fake-assassin"]
    for n, handle in enumerate(swearing + fine):
        artist(handle, 4, angle=0.2 * n)
    titles = ["Damn, that sucks", "Scunthorpe cocktail", "Ashitaka the assassin", "Shitake grove", "Crap weather"]
    artist(ALPHA, 5, edits={n: {"title": f"FAKE-TITLE {title} zz"} for n, title in enumerate(titles)}, angle=2.0)
    counts = push.run(False)
    assert sorted(sent_artists(fake)) == sorted([ALPHA, *fine]) and counts["artists_held_profane_handle"] == 3
    assert counts["works_sent"] == 5 + 4 * len(fine)  # each of ALPHA's titles too
    assert counts["works_held_profane_title"] == 4  # the invented titles that carry "yy-fake-fuckit"
    assert not any(word in fake.sent() for handle in swearing for word in of_artist(handle))


# An artist's own words, and private contacts (hard rule 6).


@pytest.mark.parametrize(("reading", "sent"), [("no", True), ("yes", False), (None, False)])
def test_a_tagline_is_sent_only_when_it_was_read_and_holds_no_contact(fake, reading, sent):
    artist(ALPHA, tagline_contact=reading)
    push.run(False)
    row = sent_artists(fake)[ALPHA]
    assert row["tagline"] == (f"FAKE-TAGLINE {ALPHA}" if sent else None)
    assert ("fake-tagline" in fake.sent()) is sent


@pytest.mark.parametrize("said", ["profane", "social"])
def test_a_tagline_that_swears_or_names_a_social_handle_is_left_out_and_the_artist_is_sent(fake, said):
    artist(ALPHA, account=said)
    artist(BRAVO, angle=0.3)
    counts = push.run(False)
    assert sorted(sent_artists(fake)) == [ALPHA, BRAVO] and counts["works_sent"] == 10
    assert sent_artists(fake)[ALPHA]["tagline"] is None and f"fake-tagline {ALPHA}" not in fake.sent()
    assert sent_artists(fake)[BRAVO]["tagline"] == f"FAKE-TAGLINE {BRAVO}"
    assert (
        counts[f"taglines_left_out_{said}"] == 1
        and sum(counts[f"artists_held_{why}"] for why in push.ARTIST_REASONS) == 0
    )


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


def test_a_tag_shown_as_the_artists_is_one_the_profile_stage_picked_on_a_work_that_goes(fake):
    """Round 8: the site's chips go through the profile stage's one rule (profiles._own_tags), and are always a
    tag the artist put on a work that is sent, as the reviewed list spells it ("Dungeons_And_Dragons" is "dnd")."""
    works, word = artist(ALPHA, 5, edits={4: {"rating": "suggestive"}}), squashed(ALPHA)
    picked = ["dnd", f"zzown{word}0", f"zzown{word}4", "zzinvented"]
    with closing(sqlite3.connect(config.LOCAL_DB)) as conn, conn:
        tags = json.dumps(["Dungeons_And_Dragons", "digitalart", f"zzall{word}"])
        conn.execute("UPDATE works SET tags = ? WHERE deviation_id = ?", (tags, works[1]["id"]))
        conn.execute(
            "UPDATE artist_profiles SET facts = ? WHERE artist_key = ?",
            (json.dumps(FACTS | {"own_words": picked}), f"da:{ALPHA}"),
        )
    read_again(works[1], handle=ALPHA)

    push.run(False)

    row = sent_artists(fake)[ALPHA]
    # not one on a work held back, not one on no work, and not one the rule left out however much it is used
    assert row["top_tags"] == ["dnd", f"zzown{word}0"]
    with closing(sqlite3.connect(config.LOCAL_DB)) as conn:
        query = "SELECT tags FROM works WHERE deviation_id IN (?, ?, ?, ?)"
        stored = [json.loads(tags) for (tags,) in conn.execute(query, [work["id"] for work in works[:4]])]
    assert set(row["top_tags"]) <= {profiles._compared(tag) for tags in stored for tag in tags}
    assert "dnd" in row["search_text"].split() and "Dungeons_And_Dragons" not in row["search_text"]  # as listed


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
        "da_account": library.account_hash(f"da:{ALPHA}"),  # the account a removal request is bound to
        "site_url": f"https://www.{ALPHA}.art/",
        "site_domain": f"{ALPHA}.art",
        "tagline": f"FAKE-TAGLINE {ALPHA}",
        "top_tags": [f"zzall{word}", *(f"zzown{word}{n}" for n in range(5))],
        "search_text": " ".join(
            [
                *("character", "digital painting", "fantasy", "portrait or bust", "painterly"),
                *(work["title"] for work in works),
                # a digit each: the ninth would make the whole a number, so the last two go without theirs
                *(f"zzall{word}", *(f"zzown{word}{n}" for n in range(3)), f"zzown{word}", f"zzown{word}"),
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
        artist(handle, 4, angle=0.02 * n)  # all close enough to each other (push.STYLE_FLOOR)
    artist(BRAVO, 4, angle=0.0201, profile=None)  # two who do not go, in the middle of the others
    artist(CHARLIE, 4, angle=0.1601, edits={0: {"rating": None}})
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

    # Once, and the pool is the artists who go. Deeper than 24, for those who are not close enough to be shown.
    assert asked == [("csd", push.DEPTH, "local", set(going))]
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


def test_a_neighbour_is_sent_with_their_works_closest_to_the_artists_and_close_enough_of_those_that_go(fake):
    mine = artist(ALPHA, 12)  # ten go: the two oldest are over the limit
    theirs = artist(BRAVO, 6)
    bare = artist(CHARLIE, 12)
    # Each work's style, as its first four numbers. BRAVO's third work is ALPHA's two oldest exactly.
    looks = {
        ALPHA: [(0, 0, 0, 1)] * 7 + [(0.8, 0, 0, 0.6), (1, 0, 0, 0), (0.6, 0, 0, 0.8)] + [(0, 1, 0, 0)] * 2,
        BRAVO: [(0, 0, 1, 0), (0, 0, 1, 0), (0, 1, 0, 0), (0.6, 0, 0.8, 0), (1, 0, 0, 0), (0.8, 0, 0.6, 0)],
    }
    with closing(sqlite3.connect(config.LOCAL_DB)) as conn, conn:
        for work, look in zip(mine + theirs, looks[ALPHA] + looks[BRAVO], strict=True):
            vector = np.array([*look, *[0] * (style.DIMENSIONS - 4)], dtype=np.float16).tobytes()
            conn.execute("UPDATE style_vectors SET vector = ? WHERE deviation_id = ?", (vector, work["id"]))
        # And an artist none of whose works that go has a vector: only the two that are over the limit have.
        conn.executemany("DELETE FROM style_vectors WHERE deviation_id = ?", [(work["id"],) for work in bare[:10]])

    push.run(False)
    picks = {(row["artist_slug"], row["neighbor_slug"]): row.get("picks") for row in fake.loads[-1][2]}

    # By position, best first: BRAVO's copy of ALPHA's ninth work, then 0.8 and 0.6 with it (the floor is
    # 0.575). Not the works like nothing of ALPHA's that goes, though four would fit: the copy of the two that
    # do not go is one of those.
    assert picks[ALPHA, BRAVO] == [4, 5, 3]
    # And of ALPHA's works, only those that go are picked from: the two oldest, BRAVO's third exactly, are not.
    assert picks[BRAVO, ALPHA] == [8, 7, 9]
    # Nothing to measure with, or nothing to measure: CHARLIE is nobody's neighbour, and has none.
    assert picks == {(ALPHA, BRAVO): [4, 5, 3], (BRAVO, ALPHA): [8, 7, 9], (CHARLIE, None): None}
    assert not any(work["id"].lower() in fake.sent() for work in mine + theirs + bare)  # a position is no id


def test_a_neighbour_without_enough_works_close_enough_makes_way_for_the_next(fake, monkeypatch):
    artist(ALPHA, angle=0.0)
    artist(BRAVO, angle=1.0)  # every work of theirs 0.54 from every one of ALPHA's: under the floor
    lone = artist(CHARLIE, angle=1.0)
    artist(DELTA, angle=0.3)  # 0.96: all of them close enough
    with closing(sqlite3.connect(config.LOCAL_DB)) as conn, conn:  # one work of CHARLIE's is ALPHA's own style
        conn.execute(
            "UPDATE style_vectors SET vector = ? WHERE deviation_id = ?", (style_vector(0.0, 0), lone[2]["id"])
        )
    # The style stage ranks them CHARLIE, BRAVO, DELTA for ALPHA, and only one is to be sent.
    ranked = [(f"da:{handle}", 1 - n / 10) for n, handle in enumerate((CHARLIE, BRAVO, DELTA))]
    monkeypatch.setattr(style, "all_neighbors", lambda model, top, rule, among, db: {f"da:{ALPHA}": ranked})
    monkeypatch.setattr(push, "NEIGHBOURS", 1)

    def sent():
        return [(row["neighbor_slug"], row.get("picks")) for row in push.build()[2] if row["artist_slug"] == ALPHA]

    # CHARLIE has one work close enough, which can be chance; BRAVO has none. DELTA takes the place, with four.
    [(slug, picks)] = sent()
    assert slug == DELTA and len(picks) == push.PICKS
    monkeypatch.setattr(push, "MIN_PICKS", 1)  # one is enough now: CHARLIE, with that work alone
    assert sent() == [(CHARLIE, [2])]
    monkeypatch.setattr(push, "STYLE_FLOOR", 0.5)  # and with a lower floor, all four of CHARLIE's closest
    assert [len(picks) for _, picks in sent()] == [push.PICKS]


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
    assert third["artists_held_adult_portfolio"] == 1 and third["works_held_suggestive_not_checked"] == 1
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


QUOTED = RuntimeError("HTTP 400: FAKE " + "-" * 120 + f" the row it refused: {ALPHA}")  # a database error quotes rows


@pytest.mark.parametrize("error", [None, ConnectionError("FAKE"), KeyError("FAKE"), Exception("FAKE"), QUOTED])
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

    assert stopped.value.code not in (0, None) and "yy-fake" not in str(stopped.value.code)  # the kind, and a start
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
    assert list(counts).index(ARTIST_MARKS[reason]) == list(counts).index(f"artists_held_{push.ARTIST_REASONS[-1]}") + 1
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


@pytest.mark.parametrize("seen", ["2026-01-01T00:00:00+00:00", "earlier_the_same_day"])
def test_a_work_that_was_not_in_the_artists_latest_answer_is_not_sent(fake, seen):
    works = artist(ALPHA, 8)
    artist(BRAVO)
    push.run(False)
    assert fake.held().count("fake-page-yy-fake-alpha") == 8
    # The artist took one down. The collect stage stored them again since: the others were seen then, this one
    # was not, and nothing is deleted or marked for it. All there is to go on is when it was last seen: long
    # before, or a second before the answer of the same day.
    with closing(sqlite3.connect(config.LOCAL_DB)) as conn, conn:
        if seen == "earlier_the_same_day":
            conn.execute("UPDATE profiles SET fetched_at = '2026-10-14T12:00:01+00:00'")
            conn.execute("UPDATE works SET fetched_at = '2026-10-14T12:00:01+00:00'")
            seen = "2026-10-14T12:00:00+00:00"
        conn.execute("UPDATE works SET fetched_at = ? WHERE deviation_id = ?", (seen, works[1]["id"]))
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


# Adoptable and for-sale sheets: the artist's own work, but never what leads (the owner, 2026-10-19).

SHEET = {"formats": ["character sheet or turnaround", push.SHEET]}


def test_an_artists_sheets_go_after_every_other_work_and_only_to_fill_their_ten(fake):
    full = artist(ALPHA, 12, edits={0: SHEET, 5: SHEET})  # ten others: no sheet goes
    few = artist(BRAVO, 6, edits={n: SHEET for n in (0, 2, 4)}, angle=0.3)  # three others lead, the sheets follow
    counts = push.run(False)
    sent = {
        slug: [row["page_url"] for row in fake.loads[-1][1] if row["artist_slug"] == slug] for slug in (ALPHA, BRAVO)
    }
    assert sent[ALPHA] == [full[n]["page"] for n in (1, 2, 3, 4, 6, 7, 8, 9, 10, 11)]  # by position, most recent first
    assert sent[BRAVO] == [few[n]["page"] for n in (1, 3, 5, 0, 2, 4)]
    assert counts["works_held_sheet_over_limit"] == 2 and counts["works_held_over_limit"] == 0
    assert counts["sheets_sent_as_filler"] == 3 and counts["works_sent"] == 16
    assert push.SHEET in tagging.FORMATS


def test_a_sheet_is_never_picked_to_show_beside_another_artist(fake):
    artist(ALPHA, 6)
    artist(BRAVO, 6, edits={n: SHEET for n in range(3)}, angle=0.001)  # alike: four of six would take in a sheet
    push.run(False)
    picks = {(row["artist_slug"], row["neighbor_slug"]): row.get("picks") for row in fake.loads[-1][2]}
    assert len(picks[ALPHA, BRAVO]) >= push.MIN_PICKS and all(pick < 3 for pick in picks[ALPHA, BRAVO])


@pytest.mark.parametrize(
    "title",
    # The sixth critic's, the seventh's sale words, and a finished commission ("YCH") with nothing else to say so.
    [
        "FAKE-TITLE Adopt auction [OPEN] zz",
        "FAKE zz design [SOLD]",
        "FAKE zz batch FFA sale (closed)",
        "FAKE zz Flatsale 3",
    ]
    + ["FAKE SB-45 AB-100 zz", "FAKE-TITLE zz YCH 3"],
)
def test_a_work_titled_as_an_adoption_or_a_sale_is_a_sheet_though_the_format_reading_did_not_say_so(fake, title):
    """The sixth critic's: 679 sent works titled adopt, auction, for sale or OTA were read as portraits and the
    like, and led their artist or were picked beside others. The seventh's: titled sold, sale, FFA, flat sale or with
    a starting bid, one led its artist and was picked in 80 rows. The title says it as well as the format reading."""
    made = artist(ALPHA, 6, edits={0: {"title": title}}, angle=0.0)
    artist(BRAVO, 6, angle=0.001)
    counts = push.run(False)
    mine = [row["page_url"] for row in fake.loads[-1][1] if row["artist_slug"] == ALPHA]
    assert mine == [made[n]["page"] for n in (1, 2, 3, 4, 5, 0)] and counts["sheets_sent_as_filler"] == 1
    picks = {(row["artist_slug"], row["neighbor_slug"]): row.get("picks") for row in fake.loads[-1][2]}
    assert len(picks[BRAVO, ALPHA]) >= push.MIN_PICKS and 5 not in picks[BRAVO, ALPHA]
    sheets = ["Adoptable #12", "OTA | Soda Fox", "zz YCH [open] 3 slots", "Dragon for sale", "Adopt - Open", "{open}"]
    sheets += ["{sold} zz", "zz FFA", "flat sale zz", "zz SB $10", "zz sb 15 usd", "Set price zz", "setprice zz"]
    sheets += ["zz design [closed]", "zz - CLOSED", "Big zz sale"]
    others = ["Open World", "Our commissions are open", "The open sea", "Opening night", "Rota", "Psyche"]
    others += ["zz soldier", "Salem zz", "zz SB-7x", "SB zz sketch", "zz enclosed", "Closet zz"]
    assert all(map(push.SHEET_TITLE.search, sheets)) and not any(map(push.SHEET_TITLE.search, others))


# An adult portfolio is by the ratings alone (the seventh critic's, owner decision 17): the profile stage's count over
# the artist's sheet counts gore with sex, and held 79 artists of ordinary game art. Each work's rating, the reason a
# suggestive one is so, and the content check read every work for the rest.


def test_the_profile_stages_count_over_a_sheet_holds_nobody_back_and_the_ratings_still_do(fake):
    artist(ALPHA, 10, edits={n: {"rating": "suggestive"} for n in (0, 1, 2)}, sheet=(10, 0), angle=0.0)  # 3 of 10
    artist(BRAVO, 10, sheet=(10, 7), angle=0.001)  # most of the sheet counted unsuitable, every work rated safe
    artist(CHARLIE, 10, edits={n: {"rating": "suggestive"} for n in (0, 1)}, sheet=(10, 3), angle=0.5)  # 2 of 10
    assert [(row["pictures"], row["unsuitable_pictures"]) for row in profiles.profiles()] == [(10, 0), (10, 7), (10, 3)]
    counts = push.run(False)
    assert sorted(sent_artists(fake)) == [BRAVO, CHARLIE] and counts["artists_held_adult_portfolio"] == 1


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
    assert config.LOCAL_DB.exists() is not dry_run  # a run makes the file it found missing; a dry run, nothing


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

    before = local_state()
    counts = push.run(dry_run)

    # The hashes the store holds, as it holds them: the artist and their account (a handle removal bars no site).
    wanted = {digest(f"da:{BRAVO}"), library.account(f"FAKE-USER-{BRAVO}")}
    assert counts["removals_added_to_database"] == 2 and counts["removed_artists_deleted_from_database"] == 1
    assert counts["artists_deleted_from_database"] == 0
    if dry_run:
        assert fake.removed == [] and BRAVO in fake.held() and local_state() == before
        return
    assert store.removed_hashes() == wanted
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
    monkeypatch.setattr(push, "build", lambda local=None, removed=(): (artists, pages, [], push.Counter()))


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


# What the first critic of this stage found (round 13). Each is a sequence: a run, a change, a run.


def rate_now(works, rating="safe"):
    """The tagging stage gets to works it had not rated."""
    sorted_as = json.dumps({"formats": [], "rating": rating})
    with closing(sqlite3.connect(config.LOCAL_DB)) as conn, conn:
        for work in works:
            row = (work["id"], profiles.TAGGER, tagging.FORMAT_VERSION, sorted_as, AT, seen(work))
            conn.execute("INSERT OR REPLACE INTO work_format VALUES (?, ?, ?, ?, ?, ?)", row)


def test_every_rating_counts_towards_the_share_and_an_image_nobody_has_rated_counts_against(fake):
    # The tagging stage rates every image, tags or none: 4 of these 10 are rated explicit and have no verified tags.
    artist(ALPHA, 10, edits={n: {"result": None, "rating": "explicit"} for n in range(4)})
    waiting = artist(BRAVO, 10, edits={n: {"rating": None} for n in range(3)})  # 3 of 10 are not rated yet
    artist(CHARLIE, 10, edits={n: {"rating": None} for n in range(2)})  # 2 of 10: under the share
    unseen = artist(DELTA, 10, edits={n: {"rating": None} for n in range(4)})
    with closing(sqlite3.connect(config.LOCAL_DB)) as conn, conn:  # 4 of 10 with no image: there is nothing to rate
        conn.executemany("UPDATE works SET image_file = NULL WHERE deviation_id = ?", [(w["id"],) for w in unseen[:4]])

    counts = push.run(False)

    assert sorted(sent_artists(fake)) == [CHARLIE, DELTA]
    assert counts["artists_held_adult_portfolio"] == 1 and counts["artists_held_not_all_rated"] == 1
    assert counts["works_held_artist_held_back"] == 6 + 7 and counts["works_sent"] == 8 + 6
    assert not any(word in fake.sent() for word in of_artist(ALPHA) + of_artist(BRAVO))

    rate_now(waiting[:3])  # rated, and safe: they go
    assert push.run(False)["artists_held_not_all_rated"] == 0 and sorted(sent_artists(fake)) == [BRAVO, CHARLIE, DELTA]
    rate_now(waiting[:3], "suggestive")  # rated again, and not safe: an adult portfolio, and they leave
    counts = push.run(False)
    assert counts["artists_held_adult_portfolio"] == 2 and counts["artists_deleted_from_database"] == 1
    assert not any(word in fake.held() for word in of_artist(BRAVO))


def renamed(fake):
    """BRAVO's account takes the handle CHARLIE, as the collect stage does it, and is public under the new one."""
    artist(ALPHA, angle=0.0)
    artist(BRAVO, angle=0.2)
    push.run(False)
    library.mark_artist(f"da:{BRAVO}", "account gone")
    artist(CHARLIE, angle=0.21, user=BRAVO, old_keys=[f"da:{BRAVO}"])
    push.run(False)
    assert sorted(sent_artists(fake)) == [ALPHA, CHARLIE] and BRAVO not in fake.held()


@pytest.mark.parametrize("asked", ["the_new_handle_here", "the_account_there"])
def test_a_removal_covers_the_account_under_every_handle_it_is_kept_under(fake, asked):
    renamed(fake)
    if asked == "the_account_there":  # as a proof (grant_request) and remove_artists do: the account alone
        fake.sql(push._removed_sql({library.account(f"FAKE-USER-{BRAVO}")}))
    else:
        store.mark_removed(CHARLIE)
    # The account goes on the lists, and no handle but one somebody named: the account bars the artist under every
    # handle, and DeviantArt can give one it has left to somebody else, the old one here, or the new one later
    # (the eighth critic's, and the lead's after round 10).
    wanted = {
        library.account(f"FAKE-USER-{BRAVO}"),
        *([] if asked == "the_account_there" else [digest(f"da:{CHARLIE}")]),
    }

    for run in range(3):  # purging one handle frees no other: it stays so, run after run
        counts = push.run(False)
        assert counts["artists_purged_here"] == (2 if run == 0 else 0)
        assert sorted(sent_artists(fake)) == [ALPHA] and counts["artists_sent"] == 1
        assert not any(word in fake.held() for word in of_artist(BRAVO) + of_artist(CHARLIE))
        assert set(fake.removed) == wanted == store.removed_hashes()
        assert library.artist_keys() == [f"da:{ALPHA}"] and library.artist_marks() == {}
    # The account is not taken in again under any handle (every profile the collect stage reads is asked). A handle
    # is barred only where somebody named it; the old one is free for whoever holds it now, and nobody else is touched.
    assert store.upsert_artist(CHARLIE, f"https://www.deviantart.com/{CHARLIE}", [], 1) == (
        asked == "the_account_there"
    )
    assert store.upsert_artist(BRAVO, f"https://www.deviantart.com/{BRAVO}", [], 1)
    assert library.account_removed(f"FAKE-USER-{BRAVO}") and not library.account_removed(f"FAKE-USER-{ALPHA}")
    assert not store.is_removed(ALPHA) and len(library.work_ids(f"da:{ALPHA}")) == 5


def test_an_account_that_joins_the_list_in_this_run_is_known_there_by_its_account(fake):
    """No handle goes on the lists by name while the account is known, so the database's row of an artist whose
    account joins its list in this run is known by the account (LISTING_SQL): deleted as a removal, and not counted
    with the deletions a run is believed about."""
    for n, handle in enumerate((ALPHA, BRAVO, CHARLIE)):
        artist(handle, angle=0.3 * n)
    push.run(False)
    store.import_removed([library.account(f"FAKE-USER-{BRAVO}")])  # here before there, as the collect stage leaves it

    counts = push.run(False)
    assert counts["removed_artists_deleted_from_database"] == 1 and counts["artists_deleted_from_database"] == 0
    assert counts["removals_added_to_database"] == 1 and fake.removed == [library.account(f"FAKE-USER-{BRAVO}")]
    assert sorted(sent_artists(fake)) == [ALPHA, CHARLIE] and counts["artists_purged_here"] == 1


@pytest.mark.parametrize("asked", ["there", "here"])
def test_a_handle_the_account_has_left_names_whoever_holds_it_now_and_never_the_account(fake, asked):
    """The ninth critic's probe 2: BRAVO's account took the handle CHARLIE, the collect stage linked the two, and
    DeviantArt gave BRAVO to somebody else, whose removal put BRAVO's hash on a list. The push read that hash as the
    key BRAVO's account left, took that key's account, and purged and barred the artist under CHARLIE, who asked for
    nothing. A handle's hash names whoever holds the handle now, which a key marked RENAMED is not: nobody kept here
    is purged or barred for it, and the hash stays on both lists for whoever holds BRAVO now."""
    renamed(fake)
    if asked == "there":
        fake.sql(push._removed_sql({digest(f"da:{BRAVO}")}))
    else:
        store.mark_removed(BRAVO)
    before = sorted(library.artist_keys())

    for _ in range(2):
        counts = push.run(False)
        assert counts["artists_purged_here"] == 0 and sorted(sent_artists(fake)) == [ALPHA, CHARLIE]
        assert set(fake.removed) == {digest(f"da:{BRAVO}")} == store.removed_hashes()
    assert sorted(library.artist_keys()) == before and library.artist_marks()[f"da:{BRAVO}"][0] == library.RENAMED
    assert not library.account_removed(f"FAKE-USER-{BRAVO}") and not store.is_removed(CHARLIE)


def test_neighbour_rows_going_all_at_once_are_not_believed(fake, capsys):
    for n in range(12):
        artist(f"yy-fake-s{n:02d}", 4, angle=0.05 * n)  # all close enough to each other (push.STYLE_FLOOR)
    push.run(False)
    before = list(fake.neighbors)
    assert len(before) == 12 * 11
    with closing(sqlite3.connect(config.LOCAL_DB)) as conn, conn:
        conn.execute("DELETE FROM style_vectors")  # a model renamed, a table lost, a style run begun again
    capsys.readouterr()

    for arguments in (["--dry-run"], [], ["--allow-delete", "131"]):
        with pytest.raises(SystemExit) as stopped:
            push.main(arguments)
        counts = json.loads(capsys.readouterr().out)
        assert "132 of their 132 neighbour rows" in str(stopped.value.code)
        assert "--allow-delete 132" in str(stopped.value.code) and counts["neighbours_deleted_from_database"] == 132
        assert fake.neighbors == before and fake.calls[-2:] == ["listing", "removed"] and len(fake.loads) == 1

    push.main(["--allow-delete", "132"])  # told that they may go
    assert fake.neighbors == [] and len(fake.artists) == 12


def test_a_neighbour_row_that_is_replaced_or_goes_with_its_artist_is_not_a_row_lost(fake):
    handles = [f"yy-fake-r{n:02d}" for n in range(27)]
    for n, handle in enumerate(handles):
        artist(handle, 4, angle=0.02 * n)  # all close enough to each other (push.STYLE_FLOOR)
    assert push.run(False)["neighbours_sent"] == 27 * 24
    with closing(sqlite3.connect(config.LOCAL_DB)) as conn, conn:  # one stops qualifying: 24 artists name them
        conn.execute("DELETE FROM artist_profiles WHERE artist_key = ?", (f"da:{handles[3]}",))
    counts = push.run(False)  # each of the 24 gets another in their place: no row is lost, and no flag is needed
    assert counts["artists_deleted_from_database"] == 1 and counts["neighbours_deleted_from_database"] == 0
    assert len(fake.neighbors) == 26 * 24
    with closing(sqlite3.connect(config.LOCAL_DB)) as conn, conn:  # and two more: now there are 23 others, not 24
        conn.executemany("DELETE FROM artist_profiles WHERE artist_key = ?", [(f"da:{h}",) for h in handles[4:6]])
    counts = push.run(False)
    assert counts["artists_deleted_from_database"] == 2 and counts["neighbours_deleted_from_database"] == 0
    assert len(fake.neighbors) == 24 * 23


@pytest.mark.parametrize("dry_run", [True, False])
def test_an_artist_the_stores_table_lacks_is_nobodys_removal(fake, dry_run):
    handles = [f"yy-fake-t{n}" for n in range(8)]
    for n, handle in enumerate(handles):
        artist(handle, 4, angle=0.1 * n)
    first = push.run(False)
    with closing(sqlite3.connect(config.LOCAL_DB)) as conn, conn:
        conn.execute("DELETE FROM artists")  # the discover stage's table is emptied; everything else is as it was
    before, held = local_state(), fake.held()

    for _ in range(2):
        with pytest.raises(push.Stop, match="8 of the 8 artists") as stopped:
            push.run(dry_run)
        # A run that would delete everybody, and is not believed. Nobody was taken for removed: nothing is purged,
        # nobody is barred, and neither removal list has a hash it did not have.
        assert stopped.value.counts["artists_purged_here"] == stopped.value.counts["removals_added_to_database"] == 0
        assert local_state() == before and fake.held() == held and len(fake.artists) == 8
        assert fake.removed == [] and store.removed_hashes() == set()
        assert fake.calls[-2:] == ["listing", "removed"]

    for handle in handles:  # discovery fills its table again, and everybody is as they were
        assert store.upsert_artist(handle, f"https://www.deviantart.com/{handle}", [], 1)
    again = push.run(False)
    assert again == first and fake.held() == held


def test_a_few_removals_made_here_go_through_at_once_with_one_made_through_the_site(fake):
    """The second critic's case: three here the same day (seven hashes with the one there), and every run stopped."""
    handles = [f"yy-fake-m{n}" for n in range(6)]
    for n, handle in enumerate(handles):
        artist(handle, 4, angle=0.1 * n)
    push.run(False)
    fake.sql(push._removed_sql({library.account(f"FAKE-USER-{handles[5]}")}))  # through the site: the account
    for handle in handles[:3]:  # and three here: three people, each a handle and an account
        store.mark_removed(handle)

    counts = push.run(False)

    assert counts["artists_purged_here"] == 4 and counts["removals_added_to_database"] == 3 * 2
    accounts = {library.account(f"FAKE-USER-{handle}") for handle in (*handles[:3], handles[5])}
    assert set(fake.removed) == {digest(f"da:{handle}") for handle in handles[:3]} | accounts
    assert library.artist_keys() == [f"da:{handle}" for handle in handles[3:5]]
    assert sorted(row["slug"] for row in fake.artists.values()) == handles[3:5]


def test_more_removals_made_here_than_a_run_believes_wait_for_the_owners_word_and_the_databases_go(fake, capsys):
    """The fourth critic's case was six handles removed here between two pushes. The seventh's: a removal table
    filled from a bad copy holds every artist's hash, and one run deleted them all there and barred them for good.
    The owner's removals here are a few at a time: more than DELETE_FLOOR of them wait for --allow-delete, and only
    the database's own list, which proofs and the owner's grants fill, goes through however long it is."""
    handles = [f"yy-fake-m{n}" for n in range(10)]
    for n, handle in enumerate(handles):
        artist(handle, 4, angle=0.1 * n)
    push.run(False)
    account = library.account(f"FAKE-USER-{handles[9]}")
    fake.sql(push._removed_sql({account}))  # through the site
    for handle in handles[:6]:  # and six here, one at a time
        store.mark_removed(handle)
    capsys.readouterr()

    for _ in range(2):
        with pytest.raises(SystemExit) as stopped:
            push.main([])
        said = str(stopped.value.code)
        assert "bar 6 people beyond" in said and "--allow-delete 6" in said and capsys.readouterr().out == ""
        assert set(fake.removed) == {account} and len(fake.loads) == 1  # nothing of the six, and nothing built
        assert sorted(row["slug"] for row in fake.artists.values()) == handles[:9]
        assert library.artist_keys() == [f"da:{handle}" for handle in handles[:9]]  # the database's went all the same

    counts = push.run(False, allow_delete=6)

    assert counts["artists_purged_here"] == 6 and counts["removals_added_to_database"] == 6 * 2
    accounts = {library.account(f"FAKE-USER-{handle}") for handle in (*handles[:6], handles[9])}
    assert set(fake.removed) == {digest(f"da:{handle}") for handle in handles[:6]} | accounts
    assert sorted(row["slug"] for row in fake.artists.values()) == handles[6:9]
    assert library.artist_keys() == [f"da:{handle}" for handle in handles[6:9]]


def test_many_hashes_that_name_nobody_kept_here_stop_the_run_and_whom_the_databases_list_names_goes_all_the_same(
    fake, capsys
):
    handles = [f"yy-fake-m{n}" for n in range(10)]
    for n, handle in enumerate(handles):
        artist(handle, 4, angle=0.1 * n)
    push.run(False)
    account = library.account(f"FAKE-USER-{handles[9]}")
    fake.sql(push._removed_sql({account}))  # removed through the site
    store.mark_removed(handles[0])  # one here, by name
    store.import_removed([digest(f"da:yy-fake-nobody-{n}") for n in range(6)])  # and six hashes of nobody here
    capsys.readouterr()

    done = False
    for arguments in (["--dry-run"], [], ["--allow-delete", "6"], ["--dry-run", "--allow-delete", "6"]):
        with pytest.raises(SystemExit) as stopped:
            push.main(arguments)
        said = str(stopped.value.code)
        assert "bar 7 people" in said and "add 8 key hashes" in said and "purge 1 artists here" in said
        assert "--allow-delete 7" in said and "yy-fake" not in said and capsys.readouterr().out == ""
        # What the database's list names is done here, from the first run that is not a dry one; nothing of the
        # store's own list goes on the database's, not even the one made by name, and nothing is built.
        done = done or "--dry-run" not in arguments
        assert set(fake.removed) == {account}
        assert library.artist_keys() == [f"da:{handle}" for handle in handles[: 9 if done else 10]]
        assert sorted(row["slug"] for row in fake.artists.values()) == handles[:9]
        assert len(fake.loads) == 1

    push.main(["--allow-delete", "7"])  # they are real: all of it goes through
    counts = json.loads(capsys.readouterr().out)
    assert counts["removals_added_to_database"] == 8 and counts["artists_purged_here"] == 1
    assert len(fake.removed) == 9 and sorted(row["slug"] for row in fake.artists.values()) == handles[1:9]

    store.mark_removed(handles[6])  # one more, as removals come: a few go through whatever a run is told
    push.main(["--allow-delete", "0"])
    assert len(fake.removed) == 11 and sorted(row["slug"] for row in fake.artists.values()) == [
        *handles[1:6],
        *handles[7:9],
    ]


def test_many_hashes_of_nobody_here_are_as_many_people(fake):
    """A broken store need not bar anybody kept here: hashes of handles nobody here has go on the list for good."""
    artist(ALPHA)
    push.run(False)
    store.import_removed([digest(f"da:yy-fake-nobody-{n}") for n in range(6)])
    with pytest.raises(push.Stop, match="bar 6 people"):
        push.run(False)
    assert fake.removed == [] and library.artist_keys() == [f"da:{ALPHA}"]


@pytest.mark.parametrize("dry_run", [False, True])
@pytest.mark.parametrize("what", ["handles", "accounts"])
def test_a_removal_table_filled_from_a_bad_copy_empties_nothing_and_bars_nobody(fake, what, dry_run):
    """The seventh critic's: the store's removal table gets every kept artist's handle hash, or account hash. Each was
    "somebody named it" to the push: every artist was deleted from the public database and barred there for good,
    in one run, with no stop. Only the database's own list goes through however long it is."""
    handles = [f"yy-fake-e{n:02d}" for n in range(12)]
    for n, handle in enumerate(handles):
        artist(handle, 4, angle=0.1 * n)
    push.run(False)
    keys = [digest(f"da:{h}") if what == "handles" else library.account(f"FAKE-USER-{h}") for h in handles]
    with closing(sqlite3.connect(config.LOCAL_DB)) as conn, conn:
        conn.executemany("INSERT INTO removed VALUES (?, ?)", [(key, AT) for key in keys])
    before, held = local_state(), fake.held()

    with pytest.raises(push.Stop, match="bar 12 people beyond") as stopped:
        push.run(dry_run)

    assert "--allow-delete 12" in str(stopped.value) and "yy-fake" not in str(stopped.value)
    assert fake.held() == held and fake.removed == [] and len(fake.artists) == 12 and local_state() == before


def test_a_handle_an_earlier_holder_left_on_the_list_bars_whoever_holds_it_now_and_not_their_account(fake):
    """The seventh critic's: remove_artists put a handle on the database's list with its holder's account (it puts
    the account alone now). DeviantArt gives the handle to somebody else, collected here before this file hears of the
    removal (a file made again). The push named the new holder by the handle, purged them, and put their account on
    the list for good, which barred them under any handle they took later. A handle on the database's list bars
    whoever holds it now, and only it: their account goes on no list unless a list holds it already, which is when
    the handle's holder is the removed account."""
    artist(ALPHA, angle=0.0)
    new_holder = artist(BRAVO, angle=0.3)
    artist(CHARLIE, angle=0.6, user=f"{CHARLIE}-account")
    push.run(False)
    earlier = library.account("FAKE-USER-yy-fake-an-earlier-holder")
    theirs = library.account(f"FAKE-USER-{CHARLIE}-account")
    fake.sql(push._removed_sql({digest(f"da:{BRAVO}"), earlier, digest(f"da:{CHARLIE}"), theirs}))  # and CHARLIE's own

    for _ in range(2):
        push.run(False)
        assert library.account(f"FAKE-USER-{BRAVO}") not in fake.removed and sorted(sent_artists(fake)) == [ALPHA]
        assert set(fake.removed) == {digest(f"da:{BRAVO}"), earlier, digest(f"da:{CHARLIE}"), theirs}
    assert not library.account_removed(f"FAKE-USER-{BRAVO}") and library.account_removed(f"FAKE-USER-{CHARLIE}-account")
    assert library.artist_keys() == [f"da:{ALPHA}"] and store.is_removed(BRAVO)  # barred under the handle alone
    assert not any(path.stem in {work["id"] for work in new_holder} for path in library.image_dir().iterdir())


def test_a_removal_by_account_alone_is_applied_here_too_when_the_guard_stops_for_something_else(fake):
    """The sixth critic's: a proof bars the account, so the database's list holds its hash and no handle. Six hashes
    of nobody here trip the guard, and then only whom the database's list names is removed: matched by handle alone,
    that was nobody here, and the artist's rows and images stayed run after run."""
    artist(ALPHA, angle=0.0)
    removed = artist(BRAVO, angle=0.3)
    artist(CHARLIE, angle=0.6)
    push.run(False)
    account = library.account(f"FAKE-USER-{BRAVO}")
    fake.sql(push._removed_sql({account}))
    store.import_removed([digest(f"da:yy-fake-nobody-{n}") for n in range(6)])

    with pytest.raises(push.Stop, match="bar 6 people beyond"):
        push.run(False)

    assert library.artist_keys() == [f"da:{ALPHA}", f"da:{CHARLIE}"] and library.account_removed(f"FAKE-USER-{BRAVO}")
    assert not any(path.stem in {work["id"] for work in removed} for path in library.image_dir().iterdir())
    assert BRAVO not in fake.held() and set(fake.removed) == {account}  # the six wait for --allow-delete
    with pytest.raises(push.Stop, match="bar 6 people beyond"):
        push.run(False)
    assert push.run(False, allow_delete=6)["artists_sent"] == 2


@pytest.mark.parametrize("what", ["sites", "an_account"])
def test_the_owners_site_removals_and_a_removal_by_account_made_here_go_through_at_once(fake, what):
    """The seventh critic's: six of the owner's site removals here tripped the guard on every run, and the sites
    stayed on the public rows until somebody passed --allow-delete. A site bars nobody (it keeps the site out of the
    rows), so it counts for nothing. And one removal by account that a run put on the store's list and could not
    take to the database's goes with the next run, as a few made here do."""
    for n in range(6):
        artist(f"yy-fake-s{n}", angle=0.1 * n, website=f"https://yy-fake-s{n}.art/")
    push.run(False)
    assert sum(bool(row["site_url"]) for row in fake.artists.values()) == 6
    if what == "sites":
        for n in range(6):
            store.mark_removed(f"yy-fake-s{n}.art")
    else:
        store.import_removed([library.account("FAKE-USER-yy-fake-s0")])

    counts = push.run(False)

    if what == "sites":
        assert counts["site_links_left_out_removed"] == 6 and counts["artists_sent"] == 6
        assert not any(row["site_url"] for row in fake.artists.values()) and ".art/" not in fake.held()
        assert set(fake.removed) == {digest(f"site:yy-fake-s{n}.art") for n in range(6)}
    else:
        assert counts["artists_purged_here"] == 1 and counts["artists_sent"] == 5 and "yy-fake-s0" not in fake.held()


@pytest.mark.parametrize("linked", ["in_the_stores_row", "in_the_profile_only"])
def test_a_removed_site_that_several_artists_name_bars_none_of_them_and_they_are_sent_without_it(fake, linked):
    """A site more than store.OWNERS artists name is shared (store._named_by): its removal never takes them all,
    and never stops a run. Whoever asked is honoured all the same: the site is on both lists for good and is
    never shown. And it holds nobody back (the fifth critic's): every artist who names it is collected and sent,
    without it, and counted."""
    hub = "https://yy-fake-hub.art/"
    artist(ALPHA, angle=0.0)
    named = [BRAVO, CHARLIE, DELTA]
    for n, handle in enumerate(named):
        artist(handle, angle=0.1 * (n + 1), website=hub, links=None if linked == "in_the_stores_row" else [])
    push.run(False)
    assert sorted(sent_artists(fake)) == [ALPHA, *named] and "yy-fake-hub" not in fake.sent()
    fake.sql(push._removed_sql({digest("site:yy-fake-hub.art")}))  # removed through the site, by its domain

    counts = push.run(False)

    assert fake.removed == [digest("site:yy-fake-hub.art")] and counts["removals_added_to_database"] == 0
    assert counts["artists_purged_here"] == 0 and library.artist_keys() == [f"da:{h}" for h in (ALPHA, *named)]
    assert not any(store.is_removed(handle) for handle in named) and store.is_removed("yy-fake-hub.art")
    assert sorted(sent_artists(fake)) == [ALPHA, *named] and "yy-fake-hub" not in fake.held()
    assert [key for key, _, _ in store.all_artists()] == [f"da:{handle}" for handle in (ALPHA, *named)]
    assert counts["site_links_left_out_removed"] == len(named)


def test_a_dry_run_changes_nothing_anywhere_and_says_what_the_run_then_does(fake, capsys):
    mine = artist(ALPHA, 6, angle=0.0)
    artist(BRAVO, angle=0.2)
    artist(CHARLIE, angle=0.4)
    artist(DELTA, angle=0.6)
    push.run(False)
    # Everything a run acts on, pending at once: a removal made through the site, one made here, a work that
    # stopped qualifying, an artist who did, and contact sheets a purge would delete.
    fake.sql(push._removed_sql({library.account(f"FAKE-USER-{BRAVO}")}))
    store.mark_removed(CHARLIE)
    library.mark_works(f"da:{ALPHA}", {mine[0]["id"]: "mature"})
    with closing(sqlite3.connect(config.LOCAL_DB)) as conn, conn:
        conn.execute("DELETE FROM artist_profiles WHERE artist_key = ?", (f"da:{DELTA}",))
    library.review_dir().mkdir(parents=True)
    (library.review_dir() / "FAKE-SHEET.jpg").write_bytes(JPEG)

    def files():
        return sorted((str(path), path.stat().st_mtime_ns) for path in config.CACHE_DIR.rglob("*") if path.is_file())

    before, there, on_disk = local_state(), (fake.held(), list(fake.removed)), files()
    capsys.readouterr()

    for _ in range(2):  # and a second dry run says the same: the first changed nothing it reads
        push.main(["--dry-run"])
        dry = capsys.readouterr().out
        assert local_state() == before and (fake.held(), list(fake.removed)) == there and files() == on_disk
        assert fake.calls[-2:] == ["listing", "removed"] and len(fake.loads) == 1
    push.main([])
    assert capsys.readouterr().out == dry

    counts = json.loads(dry)
    assert counts["removals_applied_here"] == 0 and counts["artists_purged_here"] == 2  # by account; and CHARLIE
    assert counts["removals_added_to_database"] == 2  # CHARLIE's handle and account
    assert counts["removed_artists_deleted_from_database"] == 1 and counts["artists_deleted_from_database"] == 1
    assert counts["works_deleted_from_database"] == 1 and counts["artists_sent"] == 1 and counts["works_sent"] == 5
    assert library.artist_keys() == [f"da:{ALPHA}", f"da:{DELTA}"] and not library.review_dir().exists()
    assert [row["slug"] for row in fake.artists.values()] == [ALPHA] and len(fake.removed) == 3


def edit_result(work, **fields):
    """Change what the tagging stage's verified result says about a work."""
    with closing(sqlite3.connect(config.LOCAL_DB)) as conn, conn:
        (result,) = conn.execute("SELECT result FROM work_tags WHERE deviation_id = ?", (work["id"],)).fetchone()
        changed = json.dumps(json.loads(result) | fields)
        conn.execute("UPDATE work_tags SET result = ? WHERE deviation_id = ?", (changed, work["id"]))


# What the collect stage's plain rule looks for: an "@", a domain, a link, nine digits, seven beside a word for calling.
CONTACTS = [
    "A knight in armor. A watermark reads fake.person@example.invalid.",
    "A knight in armor, signed yy-fake-painter.example in the corner.",
    "A poster with a knight on it. A line says tg://yyfakepainter.",
    "A card with a knight and the number 555 010 019 942.",
    "A knight on a flyer that says call 555 0199.",
]


@pytest.mark.parametrize("said", CONTACTS)
@pytest.mark.parametrize("where", ["description", "title"])
def test_a_work_whose_title_or_description_fails_the_plain_rule_is_not_sent(fake, where, said):
    works = artist(ALPHA, 6)
    artist(BRAVO, angle=0.3)
    assert push.run(False)["works_sent"] == 11
    # The tagging stage's reading says "no contact" all the while: a description is a model's own words about an
    # image, which nothing reads, and a model that reads a title can miss what the plain rule finds.
    if where == "description":
        edit_result(works[1], description=said)
    else:
        with closing(sqlite3.connect(config.LOCAL_DB)) as conn, conn:
            conn.execute("UPDATE works SET title = ? WHERE deviation_id = ?", (said, works[1]["id"]))
        read_again(works[1])

    counts = push.run(False)

    assert counts["works_held_contact_by_rule"] == 1 and counts["works_sent"] == 10
    assert counts["works_deleted_from_database"] == 1 and counts["texts_left_out_for_contact"] == 0
    assert said.lower() not in json.dumps(fake.loads[-1]).lower() and said.lower() not in fake.held()
    assert works[1]["page"].lower() not in fake.held() and works[1]["tags"][0] not in fake.held()
    assert [row["position"] for row in fake.loads[-1][1] if row["artist_slug"] == ALPHA] == list(range(5))


def test_a_text_that_fails_the_plain_rule_is_left_out_of_a_row_that_is_sent_and_counted(fake):
    works = artist(ALPHA, 5)
    assert push.run(False)["texts_left_out_for_contact"] == 0
    key, word = f"da:{ALPHA}", squashed(ALPHA)
    said = {
        "tagline": "FAKE-TAGLINE mail fake.person@example.invalid",
        "own tag": "zzcall5550199",
        "own word": "zz555010019942",  # as the reviewed list would spell it; the artist wrote "zz_555_010_019_942"
        "specialty": "zz@yy-fake",
        "style word": "tg://yyfakepainter",
    }
    own_words = [said["own word"], f"zzall{word}", *(work["tags"][0] for work in works)]
    facts = FACTS | {"own_words": own_words, "specialties": ["character", said["specialty"]]}
    tags = json.dumps([said["own tag"], "zz_555_010_019_942"])
    with closing(sqlite3.connect(config.LOCAL_DB)) as conn, conn:  # each read "no contact", or never read at all
        conn.execute("UPDATE profiles SET tagline = ? WHERE artist_key = ?", (said["tagline"], key))
        conn.execute("UPDATE works SET tags = ? WHERE deviation_id = ?", (tags, works[0]["id"]))
        conn.execute("UPDATE artist_profiles SET facts = ? WHERE artist_key = ?", (json.dumps(facts), key))
    read_again(works[0], handle=ALPHA)
    edit_result(works[0], style_words=["painterly", said["style word"]])

    counts = push.run(False)

    row, first = sent_artists(fake)[ALPHA], fake.loads[-1][1][0]
    assert counts["artists_sent"] == 1 and counts["works_sent"] == 5 and counts["works_held_contact_by_rule"] == 0
    assert row["tagline"] is None and row["profile"] and row["specialties"] == ["character"]
    assert row["top_tags"] == [f"zzall{word}", *(f"zzown{word}{n}" for n in range(1, 5))]
    assert row["search_text"].startswith("character digital painting")
    assert first["tags"] == ["digital painting", "character", "fantasy", "realistic", "painterly", "portrait or bust"]
    for text in said.values():
        assert text.lower() not in json.dumps(fake.loads[-1]).lower() and text.lower() not in fake.held()
    # Each text once; the specialty twice, a search filter and a search word; and the own word twice, a tag as
    # the list spells it and as the artist wrote it.
    assert counts["texts_left_out_for_contact"] == len(said) + 2


def test_an_artist_whose_profile_sentence_fails_the_plain_rule_waits_for_one_that_can_be_shown(fake):
    """The sentence is what a card says. With none to show, the artist is held like any artist without a profile."""
    artist(ALPHA, 5)
    artist(BRAVO, 5)
    assert push.run(False)["artists_sent"] == 2
    said = "FAKE-PROFILE, more at yy-fake-painter.example."
    with closing(sqlite3.connect(config.LOCAL_DB)) as conn, conn:
        conn.execute("UPDATE artist_profiles SET written = ? WHERE artist_key = ?", (said, f"da:{ALPHA}"))

    counts = push.run(False, allow_delete=1)

    assert counts["artists_sent"] == 1 and counts["artists_held_not_profiled"] == 1
    assert list(sent_artists(fake)) == [BRAVO] and said.lower() not in json.dumps(fake.loads[-1]).lower()


def test_a_database_error_is_scrubbed_of_every_key_before_it_is_cut(monkeypatch):
    key = "sb_" + "secret_FAKE4444SECRET5555KEY6666"  # invented, and put together here so the secret scan passes it
    monkeypatch.setattr(real_db, "load_env", lambda: {"SUPABASE_SECRET_KEY": key})
    for lead in (0, 470, 485, 492):  # wherever the cut falls: before the key, or in the middle of it
        with pytest.raises(RuntimeError) as raised:
            real_db._checked(httpx.Response(500, text="x" * lead + key + " FAKE-TAIL"))
        assert key[:6] not in str(raised.value) and len(str(raised.value)) <= 520


def test_a_database_error_shows_no_run_of_eight_characters_of_a_key(monkeypatch):
    """The seventh critic's: only a whole key was masked, so a reply that echoed a token but its last three characters
    printed the rest. Every run of db.SHOWN characters of one is masked; a shorter value masks nothing."""
    key = "sb_" + "secret_FAKE4444SECRET5555KEY6666"  # invented, and put together here so the secret scan passes it
    monkeypatch.setattr(real_db, "load_env", lambda: {"SUPABASE_SECRET_KEY": key, "SHORT_VALUE": "FAKE"})
    runs = [key[n : n + real_db.SHOWN] for n in range(len(key) - real_db.SHOWN + 1)]
    for echoed in (key[:-3], key[5:], key[10:18], f"{key[:12]}...{key[-12:]}"):
        with pytest.raises(RuntimeError) as raised:
            real_db._checked(httpx.Response(401, text=f"FAKE-HEAD {echoed} FAKE-TAIL"))
        said = str(raised.value)
        assert not any(run in said for run in runs), echoed
        assert said.startswith("HTTP 401: FAKE-HEAD ***") and said.endswith("*** FAKE-TAIL")
    assert real_db._masked(f"FAKE {key[:7]} FAKE", [key]) == f"FAKE {key[:7]} FAKE"


def test_an_artist_public_with_a_removed_site_is_sent_without_it_and_never_barred_for_it(fake):
    """The final critic's: a site barred for the artist who proved it theirs is anybody's to name, and the
    database's listing of whoever it showed with the site put their handle and account on the list for good. A
    site keeps out of every row and removes nobody: the row that showed it goes from the database. The fifth
    critic's: nor does it hold anybody back. The artist is sent again without it, and counted. What is kept here
    about them, their handle and their account stay."""
    artist(ALPHA)
    artist(BRAVO, website=f"https://www.{BRAVO}.art/", links=[])  # the site is in the library's profile only
    push.run(False)
    assert sent_artists(fake)[BRAVO]["site_domain"] == f"{BRAVO}.art"
    store.mark_removed(f"{BRAVO}.art")  # the request names the site, and the store's own links do not show whose it is
    library.mark_artist(f"da:{BRAVO}", library.LISTED)  # the mark an earlier collect stage left: it holds nobody back

    counts = push.run(False)

    assert counts["artists_purged_here"] == 0 and library.artist_keys() == [f"da:{ALPHA}", f"da:{BRAVO}"]
    assert set(fake.removed) == {digest(f"site:{BRAVO}.art")} == store.removed_hashes()
    assert counts["site_links_left_out_removed"] == 1 and sorted(sent_artists(fake)) == [ALPHA, BRAVO]
    assert counts["removed_artists_deleted_from_database"] == 1  # the row that showed the site
    assert sent_artists(fake)[BRAVO]["site_url"] is None and f"{BRAVO}.art" not in fake.held()
    assert not store.is_removed(BRAVO) and not library.account_removed(f"FAKE-USER-{BRAVO}")


def test_a_site_another_artist_kept_here_gives_too_is_nobodys_whether_or_not_they_go(fake):
    hub = "https://yy-fake-hub.art/"
    artist(ALPHA, website=hub)  # the only one of the two who goes: the other has no profile yet
    artist(BRAVO, website=hub, profile=None)
    artist(CHARLIE, website=f"https://{CHARLIE}.art/")
    push.run(False)
    assert {row["da_username"]: row["site_domain"] for row in fake.loads[-1][0]} == {
        ALPHA: None,
        CHARLIE: f"{CHARLIE}.art",
    }
    assert "yy-fake-hub" not in fake.sent()
    # So a removal ALPHA makes through the site names their handle and no site (remove_artists takes the keys
    # from the row), and it reaches nobody else who gives that site here.
    row = next(row for row in fake.artists.values() if row["slug"] == ALPHA)
    fake.sql(push._removed_sql({key for key in fake.keys(row) if key}))
    counts = push.run(False)
    assert counts["artists_purged_here"] == 1 and library.artist_keys() == [f"da:{BRAVO}", f"da:{CHARLIE}"]
    assert [key for key, _, _ in store.all_artists()] == [f"da:{BRAVO}", f"da:{CHARLIE}"]
    assert not store.is_removed(BRAVO) and not store.is_removed("yy-fake-hub.art")


def test_a_held_back_works_words_leave_the_artists_row_with_the_work_though_the_load_then_fails(fake):
    mine = artist(ALPHA, 6, angle=0.0)
    late = artist("yy-fake-z9", 6, angle=0.9)
    push.run(False)
    library.mark_works("da:yy-fake-z9", {late[2]["id"]: "contact details"})  # the collect stage finds one in a title
    whole = next(row for row in fake.artists.values() if row["slug"] == ALPHA)["search_text"]

    fake.fail = "load"  # the work's row is deleted, and the load fails before it writes the artist's row again
    with pytest.raises(SystemExit):
        push.main([])

    assert not any(text in fake.held() for text in of_work(late[2]))  # not the title, not a tag: nowhere
    rows = {row["slug"]: row for row in fake.artists.values()}
    assert (rows["yy-fake-z9"]["search_text"], rows["yy-fake-z9"]["top_tags"]) == ("", [])
    assert rows[ALPHA]["search_text"] == whole and mine[0]["title"] in whole  # nobody else's row is touched
    fake.fail = None
    push.run(False)  # and the next load writes the row again, with the words of the works that go
    row = next(row for row in fake.artists.values() if row["slug"] == "yy-fake-z9")
    assert late[1]["title"] in row["search_text"] and late[2]["title"] not in row["search_text"]
    assert late[1]["tags"][0] in row["top_tags"] and late[2]["tags"][0] not in row["top_tags"]


# A reading is tied to what was read. What the first critic of this stage called its biggest gap: "Every fixture
# is read once and never changes afterwards." Each of these is a sequence: read and push, a change, push again.

ANOTHER = "https://images-wixmp-yyfake.wixmp.com/f/FAKE-ANOTHER-FILE-NOBODY-RATED.jpg"  # what an artist puts in place
NEW_PICTURE = JPEG + b"FAKE-ANOTHER-PICTURE"
SIGNED_AGAIN = "?token=" + "eyJhbGciOiJIUzI1NiJ9" + ".FAKE-CLAIMS.FAKE-SIGNATURE"  # a header that names an algorithm


def collected_again(handle, made, *, tagline=None, titles=None, tags=None, thumbs=None, images=None, **answer):
    """The collect stage's next turn for an artist of artist()'s: the same works, with what the artist has changed
    since, each by its place in the list. images: the bytes of the file an artist put under a work, which the
    collector has fetched and says so. `answer`: the rest of what store_artist() is given, as the collector would
    give it (a website, how many works were flagged mature). Nothing here reads anything: that is the tagging
    stage's to do."""
    for n, data in (images or {}).items():
        library.save_image(made[n]["id"], "image/jpeg", data)
    assert library.store_artist(
        store.artist_key(handle),
        user_id=f"FAKE-USER-{handle}",
        tagline=tagline if tagline is not None else f"FAKE-TAGLINE {handle}",
        specialty="FAKE-SPECIALTY",
        level="Professional",
        **{"website": None} | answer,
        works=[
            {
                "work_id": work["id"],
                "page_url": work["page"],
                "title": (titles or {}).get(n, work["title"]),
                "tags": (tags or {}).get(n, work["tags"]),
                "thumb_url": (thumbs or {}).get(n, work["thumb"]),
                "published": str(PUBLISHED - 86_400 * n),
                "fetched": n in (images or {}),
            }
            for n, work in enumerate(made)
        ],
    ) == (0, 0, 0)


def image_read_again(work, data, description):
    """The tagging and style stages read the image a work has now: tags, a rating, the two checks and a vector."""
    read = (library.fingerprint(data), work["id"])
    edit_result(work, description=description)
    with closing(sqlite3.connect(config.LOCAL_DB)) as conn, conn:
        for table in ("work_tags", "work_format", "work_artwork", "work_content", "style_vectors"):
            conn.execute(f"UPDATE {table} SET fingerprint = ? WHERE deviation_id = ?", read)


def profiled_again(handle):
    """The profile stage makes the artist's row again, from the readings there are now."""
    images, declared, key = library.image_prints(), tagging.declared_ai(profiles.TAGGER), store.artist_key(handle)
    with closing(sqlite3.connect(config.LOCAL_DB)) as conn, conn:
        (facts,) = conn.execute("SELECT facts FROM artist_profiles WHERE artist_key = ?", (key,)).fetchone()
        rests = tagging.read_as(json.loads(facts)["counted"], images, declared)
        conn.execute("UPDATE artist_profiles SET basis = ? WHERE artist_key = ?", ("FAKE-BASIS" + rests, key))


def test_a_tagline_that_changed_after_it_was_read_is_not_sent_until_it_is_read_again(fake):
    """The critic's first counterexample."""
    made = artist(ALPHA, counted=5)
    push.run(False)
    assert sent_artists(fake)[ALPHA]["tagline"] == f"FAKE-TAGLINE {ALPHA}"
    said = "Commissions open, message me on Discord fakepainter_77"
    collected_again(ALPHA, made, tagline=said)

    counts = push.run(False)

    # Held back until it is read: an unread line may declare the account AI or adult (tagging.accounts).
    assert counts["artists_sent"] == 0 and counts["artists_held_account_not_read"] == 1
    assert "fakepainter_77" not in fake.sent() and "fakepainter_77" not in fake.held()
    read_again(handle=ALPHA)  # the tagging stage reads it, and what it answers of these words is what counts
    push.run(False)
    assert sent_artists(fake)[ALPHA]["tagline"] == said


def test_a_work_whose_title_or_tags_changed_after_they_were_read_is_held_until_they_are_read_again(fake):
    """The critic's second counterexample: the title, top_tags and search_text under the old reading."""
    made = artist(ALPHA, 6)
    artist(BRAVO, angle=0.3)
    assert push.run(False)["works_sent"] == 11
    title, tag = "Commission sheet - telegram fakepainter or whatsapp five five five", "discord_fakepainter77"
    collected_again(ALPHA, made, titles={1: title}, tags={2: [tag, made[2]["tags"][1]]})

    counts = push.run(False)

    # Words nobody has read may declare AI too: their artist waits with them (ai_not_read, the seventh critic's).
    assert counts["works_held_words_not_read"] == counts["works_held_service_in_title"] == 1
    assert counts["artists_held_ai_not_read"] == 1 and counts["works_sent"] == 5 and counts["artists_sent"] == 1
    assert counts["artists_deleted_from_database"] == 1
    for text in ("telegram fakepainter", tag):  # in no row of any load, and nowhere in the database
        assert text not in fake.sent() and text not in fake.held()
    for work in made[1:3]:  # and the works are gone from it, with their old words: a page is made from the title
        assert work["page"].lower() not in fake.held() and work["title"].lower() not in fake.held()
        assert work["tags"][0] not in fake.held()

    read_again(made[1], made[2])  # read, and found to hold no contact: now they are words that were read
    assert push.run(False)["works_sent"] == 10
    # The tag was read and holds no contact, but it is not on the reviewed list (profiles.approved), so it is
    # neither shown nor searched by. The title, read the same way, names two services: its work stays held.
    text = sent_artists(fake)[ALPHA]["search_text"]
    assert title not in [row["title"] for row in fake.loads[-1][1]] and title not in text and "discord" not in text
    assert all(work["title"] in text for n, work in enumerate(made) if n != 1)
    assert not library.holds_contact(text) and not library.names_service(text)


def test_a_work_whose_image_the_artist_replaced_is_held_until_the_new_image_is_fetched_and_read(fake):
    """The critic's third counterexample: the new thumbnail under the old image's rating and description."""
    made = artist(ALPHA, 6)
    artist(BRAVO, angle=0.3)
    assert push.run(False)["works_sent"] == 11
    collected_again(ALPHA, made, thumbs={0: ANOTHER, 1: made[1]["thumb"] + SIGNED_AGAIN})

    counts = push.run(False)

    assert counts["works_held_not_tagged"] == 1 and counts["works_sent"] == 10 and counts["artists_sent"] == 2
    assert ANOTHER.lower() not in fake.sent() and made[0]["page"].lower() not in fake.held()
    assert made[0]["description"].lower() not in fake.held()  # the old picture's description goes with it
    # A thumbnail that is only signed again shows the same image: its work goes, under the new address.
    assert [row["image_url"] for row in fake.loads[-1][1] if row["artist_slug"] == ALPHA][0] == (
        made[1]["thumb"] + SIGNED_AGAIN
    )

    collected_again(ALPHA, made, thumbs={0: ANOTHER}, images={0: NEW_PICTURE})  # fetched, and nobody has read it
    counts = push.run(False)
    assert counts["works_held_not_tagged"] == 1 and ANOTHER.lower() not in fake.sent()
    image_read_again(made[0], NEW_PICTURE, "FAKE-DESCRIPTION of another picture.")
    assert push.run(False)["works_sent"] == 11
    first = next(row for row in fake.loads[-1][1] if row["artist_slug"] == ALPHA)
    assert (first["image_url"], first["description"]) == (ANOTHER, "FAKE-DESCRIPTION of another picture.")


def test_a_replaced_image_counts_as_one_nobody_has_rated_and_its_old_vector_as_none(fake):
    made = artist(ALPHA, 10, angle=0.0)
    artist(BRAVO, 10, angle=0.1)
    push.run(False)
    picks = next(row["picks"] for row in fake.loads[-1][2] if row["artist_slug"] == BRAVO)
    collected_again(ALPHA, made, thumbs={n: f"{ANOTHER}-{n}.jpg" for n in picks[:1]})  # one that was shown beside BRAVO
    push.run(False)
    again = next(row["picks"] for row in fake.loads[-1][2] if row["artist_slug"] == BRAVO)
    assert len(fake.loads[-1][1]) == 19 and len(again) == push.PICKS  # nine of ALPHA's go, and four are still shown

    collected_again(ALPHA, made, thumbs={n: f"{ANOTHER}-{n}.jpg" for n in range(3)})  # three of ten: the share
    counts = push.run(False)
    assert counts["artists_held_not_all_rated"] == 1 and list(sent_artists(fake)) == [BRAVO]
    assert not any(word in fake.held() for word in of_artist(ALPHA))


def test_an_artist_whose_profile_rests_on_a_reading_that_went_stale_is_held_until_it_rests_on_what_is_there(fake):
    """A profile is made from readings. While one of them is not of the work as it is now, the profile says what
    nobody has read: the artist waits. A work the profile never counted cannot do that (see the tests above)."""
    made = artist(ALPHA, 6, counted=6)
    other = artist(BRAVO, 6, counted=6, angle=0.3)
    late = artist(CHARLIE, 6, counted=5, angle=0.6)  # the sixth was collected after the profile was made
    assert push.run(False)["artists_sent"] == 3
    collected_again(ALPHA, made, titles={1: "FAKE-TITLE that is another now"})  # words of a work it counted
    collected_again(BRAVO, other, thumbs={0: ANOTHER})  # the image of a work it counted
    collected_again(CHARLIE, late, thumbs={5: f"{ANOTHER}-late.jpg"})  # the image of a work it never counted

    counts = push.run(False)

    assert counts["artists_held_profile_out_of_date"] == 2 and counts["artists_deleted_from_database"] == 2
    assert list(sent_artists(fake)) == [CHARLIE] and counts["works_sent"] == 5
    assert not any(word in fake.held() for word in of_artist(ALPHA) + of_artist(BRAVO))

    read_again(made[1])  # ALPHA's words are read again and declare what they declared: the profile stands
    collected_again(BRAVO, other, thumbs={0: ANOTHER}, images={0: NEW_PICTURE})
    image_read_again(other[0], NEW_PICTURE, "FAKE-DESCRIPTION of another picture.")  # BRAVO's new picture is read,
    counts = push.run(False)
    assert sorted(sent_artists(fake)) == [ALPHA, CHARLIE]  # and their profile was still made from the old one
    assert counts["artists_held_profile_out_of_date"] == 1 and counts["works_held_artist_held_back"] == 6
    profiled_again(BRAVO)
    assert push.run(False)["artists_held_profile_out_of_date"] == 0
    assert sorted(sent_artists(fake)) == [ALPHA, BRAVO, CHARLIE]


# The store's shape, and the one command that changes it.


def before_fingerprints():
    """Take the store back to what every store was before readings carried fingerprints: no such column, no note
    of which image a work has, a basis that says nothing of what a profile rests on, and no mark. The readings
    that came after the fingerprints keep theirs."""
    with closing(sqlite3.connect(config.LOCAL_DB)) as conn:
        for table in {*tagging.TABLES, "style_vectors"} - {"work_content", "artist_account"}:
            conn.execute(f"ALTER TABLE {table} DROP COLUMN fingerprint")
        conn.execute("UPDATE artist_profiles SET basis = substr(basis, 1, length(basis) - 64)")
        conn.execute("DROP TABLE work_images")
        conn.execute("PRAGMA user_version = 0")
        conn.commit()


@pytest.mark.parametrize("dry_run", [False, True])
def test_a_store_made_before_readings_carried_fingerprints_stops_the_push_and_nothing_is_changed(fake, dry_run):
    artist(ALPHA)
    artist(BRAVO, angle=0.3)
    push.run(False)
    store.mark_removed(BRAVO)  # a removal is waiting too: not even that is half-done
    before_fingerprints()
    here, there = local_state(), fake.held()
    fake.calls.clear()
    for call in (lambda: push.run(dry_run), lambda: push.main(["--dry-run"] if dry_run else [])):
        with pytest.raises(SystemExit) as stopped:
            call()
        assert "uv run python -m pipeline.tagging --migrate --backup" in str(stopped.value)
    assert set(fake.calls) <= {"listing", "removed"} and (local_state(), fake.held()) == (here, there)


def test_the_migration_ties_a_whole_store_to_what_was_read_and_what_changed_since_is_stale_by_itself(fake, tmp_path):
    made = artist(ALPHA, 6, counted=6)
    spoken = artist(BRAVO, 6, counted=6, angle=0.3)
    shown = artist(CHARLIE, 6, counted=6, angle=0.6)
    artist(DELTA, 6, counted=6, angle=0.9)
    assert push.run(False)["artists_sent"] == 4
    untouched = sent_artists(fake)[DELTA]
    before_fingerprints()
    backup = tmp_path / "before.sqlite"
    backup.write_bytes(config.LOCAL_DB.read_bytes())
    # The collection run after the copy, by a collector of before: it brings words and a thumbnail up to date.
    said = "FAKE-TAGLINE message me on Discord fakepainter_77"
    with closing(sqlite3.connect(config.LOCAL_DB)) as conn, conn:
        retitled = ("FAKE-TITLE telegram fakepainter", made[1]["id"])
        conn.execute("UPDATE works SET title = ? WHERE deviation_id = ?", retitled)
        conn.execute("UPDATE profiles SET tagline = ? WHERE artist_key = ?", (said, f"da:{BRAVO}"))
        conn.execute("UPDATE works SET thumb_url = ? WHERE deviation_id = ?", (ANOTHER, shown[0]["id"]))
        resigned = (spoken[2]["thumb"] + SIGNED_AGAIN, spoken[2]["id"])
        conn.execute("UPDATE works SET thumb_url = ? WHERE deviation_id = ?", resigned)

    counts = tagging.migrate(backup)

    assert counts == {
        "stores migrated": 1,
        "images fingerprinted": 24,
        "readings fingerprinted": 24 * 4 + 24 * 2 + 4,  # tags, ratings, artwork checks, vectors; words twice; taglines
        "readings stale: title changed": 2,
        "readings stale: tags changed": 0,
        "readings stale: tagline changed": 1,
        "readings left stale: nothing says which words they read": 0,
        "works with a replaced image": 1,
        "profiles tied to what they rest on": 4,
    }
    with closing(sqlite3.connect(config.LOCAL_DB)) as conn, conn:  # the tagging stage reads BRAVO's new line: fine
        read = (tagging.account_print(f"da:{BRAVO}", said), f"da:{BRAVO}")
        conn.execute("UPDATE artist_account SET fingerprint = ? WHERE artist_key = ?", read)
    counts = push.run(False, allow_delete=6)  # the stages open the store again, and each change is stale by itself
    assert sorted(sent_artists(fake)) == [BRAVO, DELTA] and counts["artists_held_profile_out_of_date"] == 2
    assert sent_artists(fake)[BRAVO]["tagline"] is None and counts["works_sent"] == 12
    assert sent_artists(fake)[DELTA] == untouched  # what did not change keeps every reading it had
    assert "fakepainter" not in fake.sent() and ANOTHER.lower() not in fake.sent()
    assert style.vector(shown[0]["id"], "csd") is None and style.vector(shown[1]["id"], "csd") is not None
    assert profiles.current() == {f"da:{BRAVO}", f"da:{DELTA}"}

    settled = local_state()
    assert tagging.migrate(backup)["stores migrated"] == 0 and local_state() == settled  # again: nothing


# What the second critic of this stage found (round 14).

TITLE = "Commission sheet - telegram fakepainter or whatsapp five five five"
TAGLINE = "Commissions open, message me on Discord fakepainter_77"
# Every read build() makes of the store, through whatever module makes it.
READS = [
    *((store, "all_artists"), (library, "artist_marks"), (library, "work_marks"), (library, "thumb_sizes")),
    *((profiles, "profiles"), (profiles, "excluded"), (tagging, "results"), (tagging, "contacts")),
    (tagging, "accounts"),
    *((push, "_collected"), (style, "all_neighbors"), (style, "all_closest_works")),
]


def lands_during(monkeypatch, module, name, change):
    """A commit of another stage that lands while the rows are built: just before `module.name` reads the store,
    the first time it does. Nothing else about the read changes."""
    real, done = getattr(module, name), []

    def reading(*args, **kwargs):
        if not done:
            done.append(change())
        return real(*args, **kwargs)

    monkeypatch.setattr(module, name, reading)


def everything_at_once(made):
    """What collection does to ALPHA and BRAVO in one go: a new title, a new thumbnail and a new tagline, none of
    them read yet; a work marked mature; and BRAVO's account gone."""

    def change():
        collected_again(ALPHA, made, tagline=TAGLINE, titles={1: TITLE}, thumbs={0: ANOTHER})
        library.mark_works(f"da:{ALPHA}", {made[2]["id"]: "mature"})
        library.mark_artist(f"da:{BRAVO}", "account gone")

    return change


@pytest.mark.parametrize(("module", "name"), READS, ids=[name for _, name in READS])
def test_every_read_of_one_build_is_of_the_same_moment_of_the_store(fake, monkeypatch, module, name):
    """Built once, whatever lands meanwhile: what goes is the store as it was when the build began, every part of
    it. Never a new title under the reading of the old one, a new thumbnail under the old image's rating, a work
    or an artist marked since and sent all the same, or a card of works a mark has since taken away."""
    made = artist(ALPHA, 8)
    artist(BRAVO, angle=0.3)
    artist(CHARLIE, angle=0.6)
    push.run(False)
    first = fake.loads[-1]
    monkeypatch.setattr(push, "TRIES", 1)
    lands_during(monkeypatch, module, name, everything_at_once(made))

    push.run(False)

    assert fake.loads[-1] == first


@pytest.mark.parametrize(("module", "name"), READS, ids=[name for _, name in READS])
def test_a_change_that_lands_while_the_rows_are_built_is_in_what_goes(fake, monkeypatch, module, name):
    """The store changed under the build, so it is built again from what is there now."""
    made = artist(ALPHA, 8)
    artist(BRAVO, angle=0.3)
    artist(CHARLIE, angle=0.6)
    push.run(False)
    lands_during(monkeypatch, module, name, everything_at_once(made))

    counts = push.run(False)

    assert sorted(sent_artists(fake)) == [CHARLIE] and counts["artists_held_marked_account_gone"] == 1
    assert counts["artists_held_account_not_read"] == 1  # ALPHA's new tagline is not read yet
    assert counts["works_held_service_in_title"] == counts["works_held_not_tagged"] == 1  # the new title names two
    assert counts["works_held_marked_mature"] == 1 and counts["works_sent"] == 5
    for text in ("fakepainter", ANOTHER.lower(), *(made[n]["page"].lower() for n in range(3)), BRAVO):
        assert text not in json.dumps(fake.loads[-1]).lower() and text not in fake.held()


@pytest.mark.parametrize("how", ["marked", "taken_down"])
def test_a_card_counted_from_works_that_have_since_fallen_waits_for_one_counted_from_those_that_stand(fake, how):
    """The second critic's: a card counted from 8 works, 4 of which are another author's, or taken down, since."""
    made = artist(ALPHA, 8, counted=8)
    artist(BRAVO, angle=0.3)
    push.run(False)
    fallen, standing = made[4:], made[:4]
    if how == "marked":
        library.mark_works(f"da:{ALPHA}", {work["id"]: "another author" for work in fallen})
    else:  # missing from the artist's latest answer: last seen before it
        with closing(sqlite3.connect(config.LOCAL_DB)) as conn, conn:
            conn.executemany("UPDATE works SET fetched_at = ? WHERE deviation_id = ?", [(AT, w["id"]) for w in fallen])
    assert profiles.current() == {f"da:{BRAVO}"}

    counts = push.run(False)

    assert counts["artists_held_profile_out_of_date"] == 1 and list(sent_artists(fake)) == [BRAVO]
    assert not any(word in fake.held() for word in of_artist(ALPHA))
    # The profile stage makes the card again, from the works that stand (profiles.run counts no other).
    counted = sorted(work["id"] for work in standing)
    rests = tagging.read_as(counted, library.image_prints(), tagging.declared_ai(profiles.TAGGER))
    with closing(sqlite3.connect(config.LOCAL_DB)) as conn, conn:
        (facts,) = conn.execute("SELECT facts FROM artist_profiles WHERE artist_key = ?", (f"da:{ALPHA}",)).fetchone()
        made_again = (json.dumps(json.loads(facts) | counted_as(counted)), "FAKE-BASIS" + rests, f"da:{ALPHA}")
        conn.execute("UPDATE artist_profiles SET facts = ?, basis = ? WHERE artist_key = ?", made_again)
    assert push.run(False)["artists_sent"] == 2
    assert [row["page_url"] for row in fake.loads[-1][1] if row["artist_slug"] == ALPHA] == [
        w["page"] for w in standing
    ]


SERVICES_SAID = [
    "A knight in armour. Text in the corner reads Discord fakepainter#1234.",
    "A knight in armour, with a caption that says telegram: fakepainter.",
    "A knight in armour; the banner says add me on Snapchat fakepainter.",
    "A knight in armour, signed T E L E G R A M fakepainter.",
    "A knight in armour, and Ko-fi fakepainter in the corner.",
    "A knight in armour, with &#68;iscord fakepainter on a shield.",  # what the plain rule undoes, this undoes
    "A knight in armour, with ＷｈａｔｓＡｐｐ fakepainter on a banner.",  # fullwidth
    "A knight in armour, with e-mail fakepainter on a scroll.",
]


@pytest.mark.parametrize("said", SERVICES_SAID)
def test_a_description_that_names_a_messaging_social_or_payment_service_is_not_sent_and_its_work_is(fake, said):
    works = artist(ALPHA, 5)
    edit_result(works[0], description=said)
    assert not library.holds_contact(said)  # the plain rule sees nothing in it

    counts = push.run(False)

    assert counts["works_sent"] == 5 and counts["texts_left_out_for_contact"] == 1
    assert fake.loads[-1][1][0]["description"] is None and "fakepainter" not in fake.sent()
    assert fake.loads[-1][1][1]["description"] == works[1]["description"]  # the others go as they are


def test_what_else_a_model_wrote_is_left_out_when_it_names_a_service(fake):
    works = artist(ALPHA, 5)
    key = f"da:{ALPHA}"
    edit_result(works[0], style_words=["painterly", "patreon exclusive"])
    facts = FACTS | {"style_words": ["painterly", "discord style"], "specialties": ["character", "tiktok art"]}
    written = "FAKE-PROFILE, on Instagram too."
    with closing(sqlite3.connect(config.LOCAL_DB)) as conn, conn:
        conn.execute(
            "UPDATE artist_profiles SET facts = ?, written = ? WHERE artist_key = ?", (json.dumps(facts), written, key)
        )

    counts = push.run(False)

    row = sent_artists(fake)[ALPHA]
    assert counts["artists_sent"] == 1 and row["profile"] is None and row["specialties"] == ["character"]
    tags = ["digital painting", "character", "fantasy", "realistic", "painterly", "portrait or bust"]
    assert fake.loads[-1][1][0]["tags"] == tags
    for text in ("patreon", "discord", "tiktok", "instagram"):
        assert text not in fake.sent()


@pytest.mark.parametrize("state", ["collected_not_yet_tagged", "discovered_only", "of_an_old_shape"])
def test_a_dry_run_leaves_the_store_byte_for_byte_as_it_was_whatever_state_it_is_in(fake, state):
    """The second critic's: a dry run on a store collected and not yet tagged added eleven tables to it."""
    artist(ALPHA)
    artist(BRAVO, angle=0.3)
    push.run(False)
    gone = (*tagging.TABLES, "artist_profiles", "style_vectors")
    if state == "discovered_only":  # the library's tables too: only discovery has run
        gone += ("works", "profiles", "skipped", "work_marks", "artist_marks", "work_sizes", "work_images")
    with closing(sqlite3.connect(config.LOCAL_DB)) as conn:
        if state == "of_an_old_shape":  # readings without fingerprints: every stage stops on it
            conn.execute("PRAGMA user_version = 0")
        else:
            conn.executescript("".join(f"DROP TABLE {table};" for table in gone))
        conn.commit()
    before = config.LOCAL_DB.read_bytes()

    try:
        push.run(True, allow_delete=99)
    except (push.Stop, SystemExit):  # nobody to send, or a store of the old shape: it stops where a run would
        pass

    assert config.LOCAL_DB.read_bytes() == before and not config.LOCAL_DB.with_name("pipeline.sqlite-journal").exists()


# What the third critic of this stage found (round 23).

# Tags in common use on DeviantArt for a body, dress, sex, a fetish or an age, that a list of banned words let through.
UNSHOWN = ["loli", "lolita", "shota", "oppai", "pecs", "midriff", "fishnets", "hotpants", "yogapants", "bimbo"]
UNSHOWN += ["pregnant", "mpreg", "tickling", "damselindistress", "gagged", "tiedup", "hypnosis", "diaper", "abs"]
UNSHOWN += ["bodystocking", "bunnysuit", "croptop", "nylons", "negligee", "yaoi", "yuri", "bara", "booba", "catgirl"]


# Taken off the list after the fourth critic of the push stage read it: a platform, selling and merchandise,
# photography, characters and words whose fan art cannot be told safe by the tag, drugs, a real war, a claim to be
# official, and companies, which read as a claim to have worked for them.
OFF_THE_LIST = ["bluesky", "sticker", "stickers", "stickerdesign", "wallart", "handmade", "photography"]
OFF_THE_LIST += ["portraitphotography", "streetphotography", "pomni", "tadc", "amyrose", "bonniethebunny", "kitten"]
OFF_THE_LIST += ["plush", "psychedelic", "worldwar2", "ww2", "magicalgirl", "officialart", "ww1", "alternatehistory"]
OFF_THE_LIST += ["photomanipulation", "bokeh", "toy", "pony", "princesspeach", "sailormoon", "winxclub", "promoart"]
OFF_THE_LIST += ["actionfigure", "actionfigures", "blizzardentertainment", "capcom", "dreamworks", "fromsoftware"]
OFF_THE_LIST += ["gamefreak", "gamesworkshop", "hasbro", "konami", "mattel", "nasa", "paizo", "riotgames"]
OFF_THE_LIST += ["squareenix", "ubisoft", "valve", "wizardsofthecoast"]


def test_the_reviewed_list_is_one_plain_tag_a_line_sorted_and_holds_none_of_these():
    lines = pathlib.Path(profiles.__file__).with_name("approved_tags.txt").read_text(encoding="utf-8").split("\n")
    keys = [line.partition("\t")[0] for line in lines[:-1]]  # a line is a key, or a key, a tab and its display
    assert lines[-1] == "" and keys == sorted(set(keys)) and set(keys) == profiles.APPROVED
    assert all(tag == profiles._compared(tag) and re.fullmatch("[a-z0-9]+", tag) for tag in profiles.APPROVED)
    assert not {profiles._compared(tag) for tag in [*UNSHOWN, *OFF_THE_LIST]} & profiles.APPROVED


def test_a_display_on_the_reviewed_list_is_its_key_spaced_and_nothing_else():
    """The site prints the display (profiles.displayed) and nothing is approved or compared by it, so it may add
    single spaces to its key and change no letter, digit or order. A tag left run together has no display."""
    lines = pathlib.Path(profiles.__file__).with_name("approved_tags.txt").read_text(encoding="utf-8").split("\n")
    pairs = [line.split("\t") for line in lines[:-1] if "\t" in line]
    assert len(pairs) > 500 and all(len(pair) == 2 for pair in pairs)
    for key, display in pairs:
        assert re.fullmatch("[a-z0-9]+( [a-z0-9]+)+", display), display
        assert display.replace(" ", "").lower() == key, (key, display)
    assert dict(pairs) == profiles.DISPLAY and set(profiles.DISPLAY) <= profiles.APPROVED
    # What a split could make another word of is left run together, as it is written: never "tit an", "ass assin".
    assert not {"titan", "assassin"} & set(profiles.DISPLAY)
    assert [profiles.displayed(key) for key in ("titan", "assassin")] == ["titan", "assassin"]
    words = {word for display in profiles.DISPLAY.values() for word in display.split()}
    assert not words & {"ass", "tit", "tits", "anal", "sex", "cum", "nude", "porn", "rape", "fag", "hoe", "nazi"}


def test_only_a_tag_on_the_reviewed_list_is_shown_as_the_artists(fake):
    """The critic's: the site showed what a list of banned words missed. Now only a tag on the reviewed list goes,
    whether the profile stage picks it (profiles._own_tags) or a row made before the list says it did."""
    works = [
        {"id": f"w{n}", "result": None, "declared": "no", "tags": [*UNSHOWN, "Knight", "dragon"]} for n in range(5)
    ]
    assert list(profiles._own_tags(works, ALPHA, {*UNSHOWN, "knight", "dragon"})) == ["knight", "dragon"]
    made = artist(ALPHA, 6)
    sendable = [tag for tag in UNSHOWN if not push.UNFIT_TAG.search(tag)]  # the others hold their work back
    tags = {n: [*sendable, "Knight", "character_design", work["tags"][1]] for n, work in enumerate(made)}
    collected_again(ALPHA, made, tags=tags)
    read_again(*made)
    # A row made before the list, or before a tag was taken off it, or with the artist's spacing: its vector was
    # made from those words too (profiles.embedded_text), so the whole card waits to be made again.
    for words in ([*UNSHOWN, "knight", "characterdesign"], ["knight", "character design"]):
        own_words(ALPHA, words)
        assert push.run(False)["artists_held_profile_out_of_date"] == 1 and sent_artists(fake) == {}
    own_words(ALPHA, ["knight", "characterdesign"])

    push.run(False)

    # Printed as the list spells it, though the row and every comparison keep the key "characterdesign".
    sent = sent_artists(fake)[ALPHA]
    assert sent["top_tags"] == ["knight", "character design"]
    assert "character design" in sent["search_text"] and "characterdesign" not in sent["search_text"]


def own_words(handle, words):
    """The profile stage's row for the artist says these are the tags the site shows as theirs."""
    with closing(sqlite3.connect(config.LOCAL_DB)) as conn, conn:
        facts = json.dumps(FACTS | {"own_words": words})
        conn.execute("UPDATE artist_profiles SET facts = ? WHERE artist_key = ?", (facts, store.artist_key(handle)))


def renamed_into(handles):
    """A corrupt artist_marks table, as a bad restore or a collector's mistake could leave it: every other
    artist linked to the first as a handle the same account had before."""
    with closing(sqlite3.connect(config.LOCAL_DB)) as conn, conn:
        conn.executemany(
            "INSERT OR REPLACE INTO artist_marks (artist_key, reason, marked_at, same_as) VALUES (?, ?, ?, ?)",
            [(f"da:{handle}", library.RENAMED, AT, f"da:{handles[0]}") for handle in handles[1:]],
        )


@pytest.mark.parametrize("where", ["through_the_site", "here"])
def test_one_removal_that_reaches_many_artists_through_what_is_kept_here_stops_the_run(fake, where):
    """The critic's: one removal through the site, and rename links that a corrupt table made, deleted twelve
    artists there and put 23 hashes on the list for good. What a removal reaches through what is kept here counts
    against the guard, wherever the removal was made; whom the database's list names is removed all the same, and
    one made here waits with the rest."""
    handles = [f"yy-fake-c{n:02d}" for n in range(12)]
    for n, handle in enumerate(handles):
        artist(handle, 4, angle=0.1 * n)
    push.run(False)
    renamed_into(handles)
    account = library.account(f"FAKE-USER-{handles[0]}")
    if where == "through_the_site":  # a proof: the account
        fake.sql(push._removed_sql({account}))
    else:
        store.mark_removed(handles[0])

    with pytest.raises(push.Stop, match=f"bar {11 if where == 'through_the_site' else 12} people beyond"):
        push.run(False)

    if where == "here":
        assert sorted(row["slug"] for row in fake.artists.values()) == handles and fake.removed == []
        assert library.artist_keys() == [f"da:{handle}" for handle in handles]
        return
    assert sorted(row["slug"] for row in fake.artists.values()) == handles[1:]
    assert library.artist_keys() == [f"da:{handle}" for handle in handles[1:]]
    assert set(fake.removed) == {account}
    # the purge took the links with the artist they named, and the next run goes as runs do
    assert push.run(False)["artists_sent"] == 11 and len(fake.removed) == 1


@pytest.mark.parametrize("where", ["through_the_site", "here"])
def test_profiles_given_a_removed_accounts_hash_stop_the_run_and_nothing_bars_or_purges_them(fake, where):
    """The push critic's: one removal, and five profiles here whose account hash a corrupt table made the removed
    account's, put six artists on the list for good, purged them, and nothing stopped. A key the account's hash ties
    to it and no rename link does is a disagreement in what is kept here: whom the lists name goes, and the run
    stops with the rest untouched, until it is told."""
    handles = [f"yy-fake-h{n}" for n in range(6)]
    for n, handle in enumerate(handles):
        artist(handle, 4, angle=0.1 * n)
    artist(ALPHA, angle=0.9)  # and somebody else
    push.run(False)
    account = library.account(f"FAKE-USER-{handles[0]}")
    with closing(sqlite3.connect(config.LOCAL_DB)) as conn, conn:
        conn.executemany(
            "UPDATE profiles SET user_hash = ? WHERE artist_key = ?", [(account, f"da:{h}") for h in handles]
        )
    if where == "through_the_site":  # as a proof or the owner's grant leaves it: the account
        fake.sql(push._removed_sql({account}))
    else:
        store.mark_removed(handles[0])

    with pytest.raises(push.Stop, match="bar 6 people beyond"):
        push.run(False)

    # Here, which of the six the account really is cannot be told: nobody is purged or barred for it, and its hash
    # is left to the database's list. (The database deleted the row it lists under that account.)
    assert set(fake.removed) == ({account} if where == "through_the_site" else set())
    assert library.artist_keys() == sorted(f"da:{h}" for h in [ALPHA, *handles])
    assert not any(store.is_removed(h) for h in handles[1:])
    assert store.removed_hashes() == ({account} if where == "through_the_site" else {digest(f"da:{handles[0]}")})
    assert sorted(row["slug"] for row in fake.artists.values()) == sorted(
        [ALPHA, *handles[1 if where == "through_the_site" else 0 :]]
    )
    with pytest.raises(push.Stop, match="bar 6 people beyond"):  # every run stops until the owner says
        push.run(False)
    assert push.run(False, allow_delete=6)["artists_purged_here"] == 6
    assert library.artist_keys() == [f"da:{ALPHA}"]


def test_one_profile_given_a_removed_accounts_hash_stops_the_run_though_it_is_fewer_than_the_floor(fake):
    """The eighth critic's uncaught break: two people only a disagreement reaches are fewer than DELETE_FLOOR, and
    only that they are a disagreement stops the run. Nobody here is purged for it, and the stop says what to run."""
    artist(ALPHA, angle=0.0)
    artist(BRAVO, angle=0.3)
    other = artist(CHARLIE, angle=0.6)
    push.run(False)
    account = library.account(f"FAKE-USER-{BRAVO}")
    with closing(sqlite3.connect(config.LOCAL_DB)) as conn, conn:
        conn.execute("UPDATE profiles SET user_hash = ? WHERE artist_key = ?", (account, f"da:{CHARLIE}"))
    fake.sql(push._removed_sql({account}))

    with pytest.raises(push.Stop, match="bar 2 people") as stopped:
        push.run(False)

    assert "`uv run python -m pipeline.push --allow-delete 2`" in str(stopped.value)
    assert library.artist_keys() == [f"da:{handle}" for handle in (ALPHA, BRAVO, CHARLIE)]
    assert {work["id"] for work in other} <= {path.stem for path in library.image_dir().iterdir()}


def test_granted_removals_of_accounts_kept_under_an_older_handle_too_go_through_and_purge_both(fake):
    """The push critic's: six granted removals, each a handle and its account, each account kept here under an older
    handle too, stopped every run, and the older handles' works and images stayed here until --allow-delete. What is
    kept here agrees on whose they are, the rename links and the account hashes alike: they go, and nobody says so.
    The older handles are purged and barred nowhere: DeviantArt may have given them to somebody else."""
    olds, news = [f"yy-fake-o{n}" for n in range(6)], [f"yy-fake-n{n}" for n in range(6)]
    for n, (old, new) in enumerate(zip(olds, news, strict=True)):
        artist(old, 4, angle=0.1 * n)
        library.mark_artist(f"da:{old}", "account gone")
        artist(new, 4, angle=0.1 * n + 0.01, user=old, old_keys=[f"da:{old}"])
    artist(ALPHA, angle=0.9)
    push.run(False)
    for old, new in zip(olds, news, strict=True):  # granted (pipeline/removals.py): the handle and the account
        fake.sql(push._removed_sql({digest(f"da:{new}"), library.account(f"FAKE-USER-{old}")}))

    counts = push.run(False)

    assert counts["artists_purged_here"] == 12 and library.artist_keys() == [f"da:{ALPHA}"]
    assert not set(fake.removed) & {digest(f"da:{old}") for old in olds} and list(sent_artists(fake)) == [ALPHA]
    assert not any(store.is_removed(old) for old in olds)


def test_one_person_under_three_handles_is_removed_at_once_however_far_apart_the_links_are(fake):
    """A genuine removal reaches every handle of the account, each linked only to the next, and goes through. The
    two the account has left are purged here and barred nowhere by name."""
    artist(ALPHA, angle=0.0)
    artist(BRAVO, angle=0.2)  # the account's first handle
    push.run(False)
    library.mark_artist(f"da:{BRAVO}", "account gone")
    artist(CHARLIE, angle=0.21, user=BRAVO, old_keys=[f"da:{BRAVO}"])  # its second
    library.mark_artist(f"da:{CHARLIE}", "account gone")
    artist(DELTA, angle=0.22, user=BRAVO, old_keys=[f"da:{CHARLIE}"])  # and its third, linked to the second alone
    push.run(False)
    assert sorted(sent_artists(fake)) == [ALPHA, DELTA]
    fake.sql(push._removed_sql({library.account(f"FAKE-USER-{BRAVO}")}))  # removed through the site, by account

    counts = push.run(False)

    assert counts["artists_purged_here"] == 3 and library.artist_keys() == [f"da:{ALPHA}"]
    assert set(fake.removed) == {library.account(f"FAKE-USER-{BRAVO}")}
    assert list(sent_artists(fake)) == [ALPHA]


def test_an_account_on_the_databases_list_is_not_sent_under_the_handle_it_took_when_this_file_is_made_again(fake):
    """The critic's: ALPHA removed through the site, the store made again from nothing, and the same account
    collected under a new handle before any push. The account's hash on the list bars every handle it is kept
    under here, by itself: the new handle goes on no list by name, since the account may leave it too."""
    artist(ALPHA, angle=0.0)
    artist(BRAVO, angle=0.3)
    artist(CHARLIE, angle=0.6)
    push.run(False)
    fake.sql(push._removed_sql({library.account(f"FAKE-USER-{ALPHA}")}))  # a proof: the account
    push.run(False)
    assert library.account(f"FAKE-USER-{ALPHA}") in fake.removed
    config.LOCAL_DB.unlink()
    shutil.rmtree(library.image_dir())
    artist(BRAVO, angle=0.3)
    artist(CHARLIE, angle=0.6)
    artist("yy-fake-newname", angle=0.05, user=ALPHA)

    counts = push.run(False)

    assert sorted(sent_artists(fake)) == [BRAVO, CHARLIE] and counts["artists_purged_here"] == 1
    assert digest("da:yy-fake-newname") not in fake.removed and not store.is_removed("yy-fake-newname")
    assert "newname" not in fake.sent() and library.account_removed(f"FAKE-USER-{ALPHA}")
    assert "yy-fake-newname" not in json.dumps(local_state()[0])  # purged here


def test_an_account_hash_two_keys_hold_with_no_rename_link_between_them_is_sent_for_neither(fake):
    """The sixth critic's untested break: a broken store gave BRAVO's profile ALPHA's account hash, and no rename
    mark links the two. A proof bound to that account would bar both there, so neither is sent."""
    artist(ALPHA, angle=0.0)
    artist(BRAVO, angle=0.3)
    artist(CHARLIE, angle=0.6)
    account = library.account(f"FAKE-USER-{ALPHA}")
    with closing(sqlite3.connect(config.LOCAL_DB)) as conn, conn:
        conn.execute("UPDATE profiles SET user_hash = ? WHERE artist_key = ?", (account, f"da:{BRAVO}"))

    counts = push.run(False)

    assert list(sent_artists(fake)) == [CHARLIE] and counts["artists_held_no_account"] == 2
    assert account not in fake.sent() and not any(word in fake.sent() for word in of_artist(ALPHA) + of_artist(BRAVO))


@pytest.mark.parametrize("how", ["not_read_at_the_new_prompt_version_yet", "read_again_and_otherwise"])
def test_a_card_counted_from_tag_readings_that_no_longer_stand_waits_for_one_counted_from_those_that_do(fake, how):
    """The critic's: a card made from the verified tags of an older prompt version went as current."""
    made = artist(ALPHA, 6, counted=6)
    artist(BRAVO, 6, counted=6, angle=0.3)
    assert push.run(False)["artists_sent"] == 2
    if how == "not_read_at_the_new_prompt_version_yet":  # what stands is of the version before
        with closing(sqlite3.connect(config.LOCAL_DB)) as conn, conn:
            work_ids = [(work["id"],) for work in made]
            conn.executemany(
                "UPDATE work_tags SET prompt_version = prompt_version - 1 WHERE deviation_id = ?", work_ids
            )
    else:  # of the same images: no character, and a creature in each
        for work in made:
            edit_result(work, subjects=["creature"])

    counts = push.run(False, allow_delete=6)

    assert counts["artists_held_profile_out_of_date"] == 1 and list(sent_artists(fake)) == [BRAVO]
    assert not profiles.profiles()[0]["current"] and not any(word in fake.held() for word in of_artist(ALPHA))
    if how == "read_again_and_otherwise":  # the profile stage counts it again, and the card goes
        counted = [work["id"] for work in made]
        with closing(sqlite3.connect(config.LOCAL_DB)) as conn, conn:
            (facts,) = conn.execute(
                "SELECT facts FROM artist_profiles WHERE artist_key = ?", (f"da:{ALPHA}",)
            ).fetchone()
            again = (json.dumps(json.loads(facts) | counted_as(counted)), f"da:{ALPHA}")
            conn.execute("UPDATE artist_profiles SET facts = ? WHERE artist_key = ?", again)
        assert push.run(False)["artists_sent"] == 2


def test_search_text_passes_the_plain_rule_as_a_whole_and_gives_up_digits_before_words(fake):
    """The critic's: two titles that each hold no contact are a number to call side by side."""
    made = artist(ALPHA, 6)
    titles = {0: "Call anytime", 1: "5550199", 2: "FAKE-TITLE page 12", 3: "FAKE-TITLE 2024 study"}
    collected_again(ALPHA, made, titles=titles)
    read_again(*made)

    push.run(False)

    text = sent_artists(fake)[ALPHA]["search_text"]
    assert not library.holds_contact(text) and "5550199" not in text
    assert {word for title in titles.values() for word in re.sub(r"\d", " ", title).split()} <= set(text.split())
    assert [row["title"] for row in fake.loads[-1][1]][:4] == list(titles.values())  # each work's own row: read alone


def test_search_text_is_cut_to_what_the_database_takes(fake):
    """The database refuses a search_text over 4,000 characters, and the whole load with it (FakeDb does too)."""
    made = artist(ALPHA, 10)
    letters = "abcdefghijklmnopqrst"
    tags = {n: [f"zz{letters[n]}{letter}" + "x" * 50 for letter in letters] for n in range(10)}
    titles = {n: f"FAKE-TITLE {' '.join(['long'] * 30)} {letters[n]}" for n in range(10)}
    collected_again(ALPHA, made, titles=titles, tags=tags)
    read_again(*made)

    push.run(False)

    text = sent_artists(fake)[ALPHA]["search_text"]
    assert 3_900 < len(text) <= push.MAX_SEARCH_TEXT and not library.holds_contact(text)
    assert all(title in text for title in titles.values())  # what comes first goes whole
    assert len(push._search_text(["x" * 30] * 200)) == 30 * 129 + 128


def test_a_tag_is_shown_as_the_list_spells_it_which_is_never_longer_than_the_database_takes(fake):
    """A tag is 60 characters at most as the artist wrote it (library._tags). Printed with spaces put in it, it
    was longer than the 60 the database takes a tag, and a longer one fails the whole load (FakeDb does too). As
    the reviewed list spells it, it is never longer than the artist wrote it."""
    made = artist(ALPHA, 5)
    written = "zz_long_" + "x" * 52
    collected_again(ALPHA, made, tags={n: [written, work["tags"][1]] for n, work in enumerate(made)})
    read_again(*made)
    own_words(ALPHA, [profiles._compared(written), made[0]["tags"][1]])

    push.run(False)

    assert sent_artists(fake)[ALPHA]["top_tags"] == ["zzlong" + "x" * 52, made[0]["tags"][1]] and len(written) == 60


def test_a_neighbour_with_a_score_that_is_no_number_is_not_sent(fake, monkeypatch):
    artist(ALPHA, angle=0.0)
    artist(BRAVO, angle=0.2)
    artist(CHARLIE, angle=0.4)
    artist(DELTA, angle=0.6)
    found = style.all_neighbors
    odd = {f"da:{BRAVO}": math.nan, f"da:{CHARLIE}": math.inf}

    def scored(*args, **kwargs):
        near = found(*args, **kwargs)
        return {key: [(other, odd.get(other, score)) for other, score in others] for key, others in near.items()}

    monkeypatch.setattr(style, "all_neighbors", scored)

    push.run(False)

    rows = [row for row in fake.loads[-1][2] if row["neighbor_slug"] is not None]
    assert rows and all(math.isfinite(row["score"]) for row in rows)
    assert not any(row["neighbor_slug"] in (BRAVO, CHARLIE) for row in rows)


def test_neither_the_artists_handle_nor_a_tag_few_artists_use_is_shown_as_theirs():
    """The push stage shows what the profile stage's one rule picks (profiles._own_tags), and nothing else."""
    tags = ["zzfakealpha", "zzfake", "artofzzfakealpha", "zzsurname", "zzshared"]
    works = [{"id": f"w{n}", "result": None, "declared": "no", "tags": tags} for n in range(5)]
    shared = {"zzfakealpha", "zzfake", "artofzzfakealpha", "zzshared"}  # "zzsurname" is used by fewer than three
    assert list(profiles._own_tags(works, "zz-fake-alpha", shared)) == ["zzshared"]
    assert list(profiles._own_tags(works, "zz-fake-alpha", None)) == ["zzsurname", "zzshared"]  # no store to ask


def test_a_rating_of_the_picture_a_work_showed_before_is_no_rating_of_the_one_it_shows_now(fake):
    made = artist(ALPHA, 6)
    artist(BRAVO, angle=0.3)
    push.run(False)
    collected_again(ALPHA, made, thumbs={0: ANOTHER}, images={0: NEW_PICTURE})
    with closing(sqlite3.connect(config.LOCAL_DB)) as conn, conn:  # its tags read from the new picture, and no more
        read = (library.fingerprint(NEW_PICTURE), made[0]["id"])
        conn.execute("UPDATE work_tags SET fingerprint = ? WHERE deviation_id = ?", read)

    counts = push.run(False)

    assert counts["works_held_not_rated"] == 1 and ANOTHER.lower() not in fake.sent()
    assert tagging.results(profiles.TAGGER, verifier=profiles.TAGGER)[made[0]["id"]]["rating"] is None


@pytest.mark.parametrize(
    ("table", "reason"),
    [
        ("work_content", "fit_not_checked"),
        ("work_suggestive", "suggestive_not_checked"),
        ("work_political", "political_not_checked"),
    ],
)
def test_a_check_of_the_picture_a_work_showed_before_is_no_check_of_the_one_it_shows_now(fake, table, reason):
    """The sixth critic's untested break: the content check of the old picture, which said it was fit, taken for a
    check of the new one. Every other reading of the work is of the new picture; this one is not."""
    checked = {"rating": "suggestive", "blood_only": True, "title": "FAKE-TITLE Election poster zz", "political": False}
    made = artist(ALPHA, 6, edits={0: checked})
    artist(BRAVO, angle=0.3)
    assert push.run(False)["works_sent"] == 11  # each check made of this picture, and passed
    collected_again(ALPHA, made, thumbs={0: ANOTHER}, images={0: NEW_PICTURE})
    new = library.fingerprint(NEW_PICTURE)
    both = tagging.political_print(new, made[0]["title"], json.dumps(made[0]["tags"]))
    with closing(sqlite3.connect(config.LOCAL_DB)) as conn, conn:
        for other in ("work_tags", "work_format", "work_artwork", "work_content", "work_suggestive", "work_political"):
            if other != table:  # read again from the new picture, answering as before: every check but one
                read = both if other == "work_political" else new
                conn.execute(f"UPDATE {other} SET fingerprint = ? WHERE deviation_id = ?", (read, made[0]["id"]))

    counts = push.run(False)

    assert counts[f"works_held_{reason}"] == 1 and counts["works_sent"] == 10 and ANOTHER.lower() not in fake.sent()


def test_a_replaced_pictures_size_is_not_sent_with_the_new_picture(fake):
    made = artist(ALPHA, 6, edits={0: {"size": [300, 900]}})
    push.run(False)
    collected_again(ALPHA, made, thumbs={0: ANOTHER}, images={0: NEW_PICTURE})  # the answer gave no size for it
    image_read_again(made[0], NEW_PICTURE, "FAKE-DESCRIPTION of another picture.")

    push.run(False)

    first = fake.loads[-1][1][0]
    assert (first["image_url"], first["width"], first["height"]) == (ANOTHER, None, None)


# The fourth critic of the push stage.


def test_a_card_made_by_an_older_version_of_the_profile_stage_is_held_back_with_its_vector(fake):
    """Its biggest gap: 490 artists went with a vector made from "In the artist's words: ..." naming tags the
    list refuses, and search by meaning reads the vectors. A card is current only as this version makes them, and
    with own words the list takes as it spells them (test_only_a_tag_on_the_reviewed_list_is_shown_as_the_artists);
    the profile stage makes every card again when TEMPLATE_VERSION changes, as it has for this."""
    artist(ALPHA)
    artist(BRAVO, angle=0.3)
    with closing(sqlite3.connect(config.LOCAL_DB)) as conn, conn:
        older = (profiles.TEMPLATE_VERSION - 1, f"da:{ALPHA}")
        conn.execute("UPDATE artist_profiles SET template_version = ? WHERE artist_key = ?", older)

    counts = push.run(False)

    assert counts["artists_held_profile_out_of_date"] == 1 and list(sent_artists(fake)) == [BRAVO]
    assert squashed(ALPHA) not in fake.sent()


def set_evidence(handle, evidence, written=None):
    """The profile stage's row for the artist quotes this evidence that they take work, and says so in `written`."""
    with closing(sqlite3.connect(config.LOCAL_DB)) as conn, conn:
        conn.execute(
            "UPDATE artist_profiles SET open_evidence = ?, written = coalesce(?, written) WHERE artist_key = ?",
            (json.dumps(evidence), written, store.artist_key(handle)),
        )


def test_a_card_says_open_for_work_only_while_the_artists_own_words_still_say_so(fake):
    """The tagline quoted as evidence came to say "Commissions CLOSED", and the card went on with "Open for
    commissions." and open_for_work beside it. The claim rests on the artist's words as they are now: the tagline
    their latest answer gives, once it is read, and their own tags on works of the last RECENT_DAYS."""
    made = artist(ALPHA)
    artist(BRAVO, angle=0.3)
    collected_again(ALPHA, made, tagline="Commissions open")
    read_again(handle=ALPHA)
    said = profiles.open_evidence("Commissions open", [], datetime.now(UTC))
    set_evidence(ALPHA, said, f"FAKE-PROFILE {ALPHA}. Open for commissions.")
    push.run(False)
    assert sent_artists(fake)[ALPHA]["open_for_work"] is True

    for read in (False, True):  # the new tagline not read yet, and read: either way the old one stands no more
        collected_again(ALPHA, made, tagline="Commissions CLOSED, not taking any")
        if read:
            read_again(handle=ALPHA)
        assert push.run(False)["artists_held_profile_out_of_date"] == 1 and list(sent_artists(fake)) == [BRAVO]
    collected_again(ALPHA, made, tagline="Commissions open")  # as it was: the claim stands again
    read_again(handle=ALPHA)
    assert push.run(False)["artists_sent"] == 2 and sent_artists(fake)[ALPHA]["open_for_work"] is True
    collected_again(ALPHA, made, tagline="")  # emptied: kept here, and no longer the artist's word
    assert push.run(False)["artists_held_profile_out_of_date"] == 1

    # A tag on a work says so for RECENT_DAYS after the work was published, and no longer.
    collected_again(ALPHA, made, tags={0: ["commissionsopen", made[0]["tags"][1]]})
    read_again(*made)
    published = datetime.fromtimestamp(PUBLISHED, UTC)
    work = (made[0]["id"], ["commissionsopen"], published.isoformat(timespec="seconds"))
    set_evidence(ALPHA, profiles.open_evidence(None, [work], published))
    assert f"da:{ALPHA}" in profiles.current(now=published + timedelta(days=profiles.RECENT_DAYS - 1))
    assert f"da:{ALPHA}" not in profiles.current(now=published + timedelta(days=profiles.RECENT_DAYS + 1))


def test_a_tagline_and_a_website_the_artists_latest_answer_empties_are_not_sent_though_they_stay_kept(fake):
    """The artist emptied both on DeviantArt; the collect stage, which replaces nothing kept by nothing, kept them,
    and this stage sent them on. What is sent is what the latest answer that said something says: a field it
    gives as a text that shows nothing is not sent, and an answer that does not give the field changes nothing."""
    site = "https://yy-fake-alpha.art/"
    made = artist(ALPHA, website=site)
    push.run(False)
    row = sent_artists(fake)[ALPHA]
    assert (row["site_url"], row["site_domain"], row["tagline"]) == (site, "yy-fake-alpha.art", f"FAKE-TAGLINE {ALPHA}")

    for answer in ({"tagline": "", "website": ""}, {"tagline": 7}):  # emptied; then an answer without either
        collected_again(ALPHA, made, **answer)
        push.run(False)
        row = sent_artists(fake)[ALPHA]
        assert (row["site_url"], row["site_domain"], row["tagline"]) == (None, None, None)
        assert "fake-tagline" not in fake.held() and "yy-fake-alpha.art" not in fake.held()
        # Kept all the same: an answer that blanks a field by mistake loses nothing.
        assert library.website(f"da:{ALPHA}") == site

    collected_again(ALPHA, made, website=site)  # given again
    push.run(False)
    row = sent_artists(fake)[ALPHA]
    assert (row["site_url"], row["tagline"]) == (site, f"FAKE-TAGLINE {ALPHA}")

    # Emptied or not, a removed site the kept website is on is never sent, and holds the artist back no more.
    other = artist(BRAVO, angle=0.3, website=f"https://{BRAVO}.art/", links=[])  # a site only the library knows
    store.mark_removed(f"{BRAVO}.art")
    for website in ("", f"https://{BRAVO}.art/"):
        collected_again(BRAVO, other, website=website)
        counts = push.run(False)
        assert counts["site_links_left_out_removed"] == 1 and sorted(sent_artists(fake)) == [ALPHA, BRAVO]
        assert sent_artists(fake)[BRAVO]["site_url"] is None and f"{BRAVO}.art" not in fake.held()


def test_works_deviantart_flags_mature_count_towards_an_adult_portfolio_though_none_of_them_is_kept(fake):
    """20 works flagged mature and 4 safe among the newest 24 passed the adult check: the collect stage keeps none
    of the 20. It keeps how many there were (library.answers), and the share counts them. A store collected before
    it kept the number counts none until the artist's next turn."""
    made = artist(ALPHA, 4)
    artist(BRAVO, angle=0.3)
    for mature, sent in ((20, False), (2, False), (1, True), (0, True)):  # 2 in 6 is a third; 1 in 5 a fifth
        collected_again(ALPHA, made, mature=mature)
        counts = push.run(False)
        assert (ALPHA in sent_artists(fake)) is sent and counts["artists_held_adult_portfolio"] == (not sent)
    collected_again(ALPHA, made, mature=20)
    with closing(sqlite3.connect(config.LOCAL_DB)) as conn, conn:
        conn.execute("DELETE FROM answers")  # as a store collected before the count
    assert push.run(False)["artists_held_adult_portfolio"] == 0 and ALPHA in sent_artists(fake)


def test_a_listed_tag_in_the_artists_own_spacing_is_shown_and_searched_as_the_list_spells_it(fake):
    """The listed "titan" written once as "tit_an" was shown as "tit an" and searched as "tit_an", which Postgres
    reads as the word "tit"; and so for jap, coon and heroin."""
    made = artist(ALPHA, 6)
    written = ["tit_an", "Jap-Anese", "rac_coon", "heroin_e"]
    tags = {n: [*written, work["tags"][1]] for n, work in enumerate(made)}
    collected_again(ALPHA, made, tags=tags)
    read_again(*made)
    own_words(ALPHA, list(profiles._own_tags([{"tags": tags[n]} for n in tags], ALPHA, None)))  # as the stage picks

    push.run(False)

    row = sent_artists(fake)[ALPHA]
    assert row["top_tags"] == ["titan", "japanese", "raccoon", "heroine"]
    searched = set(re.split(r"[\W_]+", row["search_text"].lower()))
    assert {"titan", "japanese", "raccoon", "heroine"} <= searched and not {"tit", "jap", "coon", "heroin"} & searched


@pytest.mark.parametrize(
    "unusable",
    [bytes(2 * style.DIMENSIONS), np.full(style.DIMENSIONS, np.nan, np.float16).tobytes(), b"\x00\x3c" * 7],
    ids=["zero", "nan", "short"],
)
def test_a_work_whose_style_vector_is_unusable_is_left_out_of_the_ranking_and_holds_up_nothing(fake, unusable):
    """One zero vector made all 56 neighbour scores NaN, and every push stopped until it was fixed."""
    handles = [ALPHA, BRAVO, CHARLIE, DELTA]
    made = {handle: artist(handle, 5, angle=0.2 * n) for n, handle in enumerate(handles)}
    push.run(False)
    with closing(sqlite3.connect(config.LOCAL_DB)) as conn, conn:
        conn.execute("UPDATE style_vectors SET vector = ? WHERE deviation_id = ?", (unusable, made[BRAVO][0]["id"]))

    counts = push.run(False)

    rows = [row for row in fake.loads[-1][2] if row["neighbor_slug"]]
    assert counts["neighbours_sent"] == len(rows) == 12 and all(math.isfinite(row["score"]) for row in rows)
    assert all(0 not in (row["picks"] or []) for row in rows if row["neighbor_slug"] == BRAVO)


def test_style_vectors_stored_at_another_length_are_ranked_and_picked_as_at_unit_length(fake):
    """The sixth critic's untested break: vectors not made unit length before they are compared. A cosine is a dot
    product only between unit vectors, so at three times the length two artists 69 degrees apart (cosine 0.36,
    under STYLE_FLOOR) would pass the floor nine times over."""
    for handle, angle in ((ALPHA, 0.0), (BRAVO, 1.2)):
        artist(handle, 5, angle=angle)
    with closing(sqlite3.connect(config.LOCAL_DB)) as conn, conn:
        for work_id, vector in conn.execute("SELECT deviation_id, vector FROM style_vectors").fetchall():
            longer = (np.frombuffer(vector, dtype=np.float16) * 3).astype(np.float16).tobytes()
            conn.execute("UPDATE style_vectors SET vector = ? WHERE deviation_id = ?", (longer, work_id))

    push.run(False)

    assert fake.loads[-1][2] == [{"artist_slug": ALPHA} | NO_NEIGHBOUR, {"artist_slug": BRAVO} | NO_NEIGHBOUR]


def test_search_text_read_whole_names_no_service_and_loses_only_what_made_it_name_one(fake):
    """Two titles, "Telegram" and "fakepainter", each read alone as no contact, were "Telegram fakepainter" in
    search_text: read whole by the plain rule only, and never for the name of a service."""
    made = artist(ALPHA, 6)
    collected_again(ALPHA, made, titles={0: "Telegram", 1: "fakepainter"})
    read_again(*made)

    push.run(False)

    text = sent_artists(fake)[ALPHA]["search_text"]
    assert not library.names_service(text) and not library.holds_contact(text)
    assert "Telegram" not in text and "fakepainter" in text.split()
    assert all(work["title"] in text for work in made[2:])
    # A name set apart across two parts, and an address across three: what made it goes, and nothing else.
    assert push._search_text(["FAKE words", "tele", "gram", "more"]) == "FAKE words tele more"
    assert push._search_text(["dragon", "fakepainter", "dot", "com"]) == "dragon fakepainter dot"


def test_search_text_gives_up_digits_from_nine_of_them_side_by_side(fake):
    """The plain rule reads nine digits as a number (library.holds_contact): four in one title and five in another
    are nine in search_text, and eight at most are kept."""
    made = artist(ALPHA, 6)
    titles = {n: "FAKE-TITLE sketch" for n in range(6)} | {0: "FAKE-TITLE 1234", 1: "FAKE-TITLE 56789"}
    collected_again(ALPHA, made, titles=titles, tags={n: [work["tags"][1]] for n, work in enumerate(made)})
    read_again(*made)

    push.run(False)

    text = sent_artists(fake)[ALPHA]["search_text"]
    assert "1234" in text and "56789" not in text and sum(map(str.isdigit, text)) == 4
    assert library.holds_contact("FAKE-TITLE 1234 FAKE-TITLE 56789")
