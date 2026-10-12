"""Tests for pipeline.library.

Every handle, address, text and byte below is invented for these tests. Nothing came from DeviantArt and nothing
here fetches anything. The cache directory and the database are moved into pytest's temporary directory.
"""

import hashlib
import inspect
import json
import sqlite3
import subprocess
import time
from contextlib import closing

import pytest

from pipeline import config, library, store

REAL_CACHE_DIR = config.CACHE_DIR  # as the project sets it, before the fixture below moves it

ALPHA, BRAVO = "da:zz-fake-alpha", "da:zz-fake-bravo"
ID1, ID2, ID3, ID4 = (f"00000000-0000-4000-A000-{n:012d}" for n in (1, 2, 3, 4))
PAGE = "https://www.deviantart.com/zz-fake-alpha/art/FAKE-TITLE-1-1001"
SIGNED = "token=FAKE-CDN-TOKEN-1.FAKE-PAYLOAD.FAKE-SIGNATURE"  # the shape of DeviantArt's signature on an image address
THUMB = f"https://images-wixmp-zzcdn.wixmp.com/f/FAKE-THUMB-1.jpg?{SIGNED}"
JPEG = b"\xff\xd8\xff" + b"FAKE-IMAGE-BYTES-1" * 50
PNG = b"\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR" + b"FAKE-IMAGE-BYTES-2" * 50
GIF = b"GIF89a" + b"FAKE-IMAGE-BYTES-3" * 50
WEBP = b"RIFF\x24\x00\x00\x00WEBPVP8 " + b"FAKE-IMAGE-BYTES-4" * 50
# A 200 KB "embedded image", the kind an earlier critic got into a text column of the remote database.
EMBEDDED = "data:image/png;base64," + "FAKEBASE64" * 20_000
# The one a critic got past the first version of this file: a long parameter, and the payload in short chunks.
CHUNKED = "data:image/jpeg;FAKE-PARAMETER=" + "x" * 150 + ";base64," + " ".join(["FAKEBASE64" * 7] * 30)
ZWSP, SHY, FILLER = "\u200b", "\u00ad", "\u3164"  # a zero-width space, a soft hyphen, a Hangul filler: all invisible


def short(value):
    """A test id. The default one is the whole value, and Windows cannot hold 200 KB in an environment variable."""
    return repr(value[:40] if isinstance(value, str | bytes) else value)


def unsynced(*args, connect=sqlite3.connect, **kwargs):
    """sqlite3.connect that does not wait for the disk after each commit. Nothing else about the database changes."""
    conn = connect(*args, **kwargs)
    conn.execute("PRAGMA synchronous = OFF")
    return conn


@pytest.fixture(autouse=True)
def cache(tmp_path, monkeypatch):
    monkeypatch.setattr(config, "CACHE_DIR", tmp_path / "cache")
    monkeypatch.setattr(config, "LOCAL_DB", tmp_path / "cache" / "pipeline.sqlite")
    monkeypatch.setattr(sqlite3, "connect", unsynced)  # the tests commit hundreds of times
    monkeypatch.chdir(tmp_path)


def rows(table, db=None):
    library.artist_keys(db)  # so the tables exist, even when the call under test refused before opening the file
    with closing(sqlite3.connect(db or config.LOCAL_DB)) as conn:
        conn.row_factory = sqlite3.Row
        return [dict(row) for row in conn.execute(f"SELECT * FROM {table} ORDER BY 1")]


def fields(work_id=ID1, **changes):
    """The keyword arguments of one acceptable work."""
    given = {"page_url": PAGE, "title": "FAKE-TITLE-1", "tags": ["faketag"], "thumb_url": THUMB, "published": "1"}
    return {"work_id": work_id, **given, **changes}


def keep(key=ALPHA, works=(), db=None, **profile_fields):
    blank = dict.fromkeys(("user_id", "tagline", "specialty", "level", "website"))
    return library.store_artist(key, **{**blank, **profile_fields}, works=list(works), db=db)


def profile(key=ALPHA, **profile_fields):
    keep(key, **profile_fields)
    return next(row for row in rows("profiles") if row["artist_key"] == key)


def work(key=ALPHA, work_id=ID1, **changes):
    keep(key, [fields(work_id, **changes)])
    return next(row for row in rows("works") if row["deviation_id"] == work_id.upper())


def files(root):
    return sorted(path.relative_to(root).as_posix() for path in root.rglob("*") if path.is_file())


def database_bytes():
    return config.LOCAL_DB.read_bytes()


# --- the tables ---


def test_the_tables_hold_what_was_asked_for_and_nothing_else():
    library.artist_keys()
    with closing(sqlite3.connect(config.LOCAL_DB)) as conn:
        columns = {
            table: [(name, kind) for _, name, kind, *_ in conn.execute(f"PRAGMA table_info({table})")]
            for table in ("profiles", "works")
        }
        strict = conn.execute("SELECT strict FROM pragma_table_list WHERE name IN ('profiles', 'works')").fetchall()
    assert columns["profiles"] == [
        ("artist_key", "TEXT"),
        ("tagline", "TEXT"),
        ("specialty", "TEXT"),
        ("level", "TEXT"),
        ("website", "TEXT"),
        ("fetched_at", "TEXT"),
        ("user_hash", "TEXT"),
    ]  # no bio: free text an artist wrote about themselves is not kept at all
    assert columns["works"] == [
        ("deviation_id", "TEXT"),
        ("artist_key", "TEXT"),
        ("page_url", "TEXT"),
        ("title", "TEXT"),
        ("tags", "TEXT"),
        ("thumb_url", "TEXT"),
        ("published_at", "TEXT"),
        ("image_file", "TEXT"),
        ("fetched_at", "TEXT"),
    ]
    assert strict == [(1,), (1,)]
    assert "bio" not in inspect.signature(library.store_artist).parameters


@pytest.mark.parametrize("column", ["tagline", "specialty", "level", "website", "user_hash"])
def test_a_profile_column_refuses_bytes_even_from_raw_sql(column):
    """Hard rule 1 held by the database itself: the tables are STRICT, so a text column cannot take a blob."""
    profile()
    with closing(sqlite3.connect(config.LOCAL_DB)) as conn, pytest.raises(sqlite3.IntegrityError):
        conn.execute(f"UPDATE profiles SET {column} = ?", (JPEG,))


@pytest.mark.parametrize("column", ["page_url", "title", "tags", "thumb_url", "published_at", "image_file"])
def test_a_work_column_refuses_bytes_even_from_raw_sql(column):
    work()
    with closing(sqlite3.connect(config.LOCAL_DB)) as conn, pytest.raises(sqlite3.IntegrityError):
        conn.execute(f"UPDATE works SET {column} = ?", (JPEG,))


def test_the_database_path_can_be_given(tmp_path):
    other = tmp_path / "elsewhere" / "other.sqlite"
    keep(ALPHA, [fields()], db=other, tagline="FAKE-TAGLINE")
    assert [row["tagline"] for row in rows("profiles", other)] == ["FAKE-TAGLINE"]
    assert library.artist_keys(other) == [ALPHA] and library.cached_image(ID1, other) is False
    library.purge(ALPHA, other)
    assert library.artist_keys(other) == [] and not config.LOCAL_DB.exists()


# --- a table from an earlier version, and the incident that one left behind ---
#
# What happened to the real cache, 2026-10-12. The first version's collector was running. It wrote a profile by
# position: INSERT OR REPLACE INTO profiles VALUES (key, tagline, specialty, level, bio, website, fetched_at).
# The second version of this module, opened on the same file by another stage for a table of its own, dropped
# the bio column and added user_hash at the end. From then on the collector's seven values landed one column
# to the right: seven artists' websites were stored as fetched_at, and the eighth artist had no website, which
# put NULL into fetched_at. NOT NULL refused it, and the run stopped with IntegrityError.

_FIRST_FOUR = "artist_key TEXT PRIMARY KEY, tagline TEXT, specialty TEXT, level TEXT"
FIRST_VERSION = f"{_FIRST_FOUR}, bio TEXT, website TEXT, fetched_at TEXT NOT NULL"
AFTER_THE_INCIDENT = f"{_FIRST_FOUR}, website TEXT, fetched_at TEXT NOT NULL, user_hash TEXT"
BY_POSITION = "INSERT OR REPLACE INTO profiles VALUES (?, ?, ?, ?, ?, ?, ?)"  # how the first version wrote a profile
WHEN = "2026-10-12T00:00:00+00:00"


def first_version_row(key, website):
    """What the first version passed for one profile: key, tagline, specialty, level, bio, website, fetched_at."""
    return (key, "FAKE-TAGLINE call 555 0199", "FAKE-SPECIALTY", "FAKE-LEVEL", "FAKE-BIO-TEXT", website, WHEN)


def earlier_database(profiles):
    """A database with a profiles table of an earlier shape, and works kept under the first version's looser rules."""
    config.LOCAL_DB.parent.mkdir(parents=True)
    with closing(sqlite3.connect(config.LOCAL_DB)) as conn, conn:
        conn.execute(f"CREATE TABLE profiles ({profiles}) STRICT")
        conn.execute(
            "CREATE TABLE works (deviation_id TEXT PRIMARY KEY, artist_key TEXT NOT NULL, page_url TEXT NOT NULL, "
            "title TEXT, tags TEXT NOT NULL, thumb_url TEXT NOT NULL, published_at TEXT, image_file TEXT, "
            "fetched_at TEXT NOT NULL) STRICT"
        )
        titles = ("FAKE-TITLE-PLAIN 2026", "FAKE-TITLE zz.fake@mail", "FAKE-TITLE 555 0199", "FAKE-TITLE data:image/x")
        for work_id, title in zip((ID1, ID2, ID3, ID4), titles, strict=True):
            row = (work_id, ALPHA, PAGE, title, '["ai", "faketag"]', THUMB, None, f"{work_id}.jpg", WHEN)
            conn.execute("INSERT INTO works VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", row)


def raw(statement, *values):
    """Run one statement on the database with no help from the library, as some other program would."""
    with closing(sqlite3.connect(config.LOCAL_DB)) as conn, conn:
        return conn.execute(statement, values).fetchall()


def test_the_real_run_stopped_on_a_not_null_constraint_after_the_table_changed_under_it():
    """The failure itself, statement for statement. See the comment above for the story."""
    earlier_database(AFTER_THE_INCIDENT)  # the table as the second version had just left it
    raw(BY_POSITION, *first_version_row(ALPHA, "https://zz-fake-alpha.art/"))  # goes in, one column to the right
    assert raw("SELECT website, fetched_at, user_hash FROM profiles") == [
        ("FAKE-BIO-TEXT", "https://zz-fake-alpha.art/", WHEN)
    ]
    with pytest.raises(sqlite3.IntegrityError, match="NOT NULL constraint failed: profiles.fetched_at"):
        raw(BY_POSITION, *first_version_row(BRAVO, None))  # the eighth artist: no website


def test_opening_the_file_never_changes_a_table_that_exists():
    """The cause, fixed: other stages open the file through library._open() for tables of their own, and that
    must leave a collector of another version, still writing, undisturbed."""
    earlier_database(FIRST_VERSION)
    raw(BY_POSITION, *first_version_row(ALPHA, "https://zz-fake-alpha.art/"))
    made = raw("SELECT sql FROM sqlite_master WHERE name IN ('profiles', 'works') ORDER BY name")
    with library._open(None) as conn:  # what pipeline/style.py and pipeline/tagging.py do
        conn.execute("CREATE TABLE IF NOT EXISTS fake_stage_table (work TEXT)")
    library.artist_keys()
    library.cached_image(ID1)
    library.drop_orphans()
    library.purge(BRAVO)
    assert raw("SELECT sql FROM sqlite_master WHERE name IN ('profiles', 'works') ORDER BY name") == made
    raw(BY_POSITION, *first_version_row(BRAVO, None))  # the old collector's next row still lands where it belongs
    assert raw("SELECT artist_key, bio, website, fetched_at FROM profiles ORDER BY 1") == [
        (ALPHA, "FAKE-BIO-TEXT", "https://zz-fake-alpha.art/", WHEN),
        (BRAVO, "FAKE-BIO-TEXT", None, WHEN),
    ]
    library.purge(ALPHA)  # and a purge works on a table of any version
    assert library.artist_keys() == [BRAVO] and rows("works") == []


@pytest.mark.parametrize("earlier", [FIRST_VERSION, AFTER_THE_INCIDENT])
def test_storing_an_artist_rebuilds_a_table_from_an_earlier_version(earlier):
    """Only the collector, about to write, upgrades. The rebuilt table lists the same artists and when each was
    fetched, and nothing else: the bio, the values in the wrong columns and what looser rules kept are gone."""
    earlier_database(earlier)
    raw(BY_POSITION, *first_version_row(ALPHA, "https://wa.me/15550109999"))
    raw(BY_POSITION, *first_version_row(BRAVO, "https://zz-fake-bravo.art/"))
    assert keep("da:zz-fake-charlie", tagline="FAKE-TAGLINE-NEW", user_id="FAKE-USERID") == (0, 0)

    made = raw("SELECT sql FROM sqlite_master WHERE name = 'profiles'")[0][0]
    assert "bio" not in made and "CHECK" in made and "STRICT" in made
    assert raw("SELECT name FROM sqlite_master WHERE name LIKE 'profiles%' AND type = 'table'") == [("profiles",)]
    blank = {"tagline": None, "specialty": None, "level": None, "website": None, "user_hash": None}
    when = WHEN if earlier == FIRST_VERSION else "1970-01-01T00:00:00+00:00"  # the incident's rows had lost theirs
    assert rows("profiles")[:2] == [{"artist_key": key, **blank, "fetched_at": when} for key in (ALPHA, BRAVO)]
    assert rows("profiles")[2]["tagline"] == "FAKE-TAGLINE-NEW" and len(rows("profiles")[2]["user_hash"]) == 64
    assert [row["title"] for row in rows("works")] == ["FAKE-TITLE-PLAIN 2026", None, None, None]
    assert [row["image_file"] for row in rows("works")] == [f"{work_id}.jpg" for work_id in (ID1, ID2, ID3, ID4)]
    stored = database_bytes()
    for gone in (
        b"FAKE-BIO-TEXT",
        b"wa.me",
        b"15550109999",
        b"555 0199",
        b"zz.fake@mail",
        b"data:image",
        b"zz-fake-bravo.art",
    ):
        assert gone not in stored, gone
    assert keep(ALPHA, [fields()], tagline="FAKE-TAGLINE-ALPHA") == (0, 0)  # and it is rebuilt once, not every time
    assert rows("profiles")[2]["tagline"] == "FAKE-TAGLINE-NEW" and len(rows("works")) == 1


def test_todays_table_refuses_a_row_written_by_position_instead_of_storing_it_in_the_wrong_columns():
    """Were a collector of the first version to meet today's table, every row would fail at once, and loudly:
    nothing would be stored under the wrong name for a later stage to read as true."""
    profile(BRAVO, tagline="FAKE-TAGLINE")
    for website in ("https://zz-fake-alpha.art/", None):
        with pytest.raises(sqlite3.IntegrityError):
            raw(BY_POSITION, *first_version_row(ALPHA, website))
    assert [row["artist_key"] for row in rows("profiles")] == [BRAVO]


@pytest.mark.parametrize(
    ("column", "value"),
    [
        ("fetched_at", "https://zz-fake-alpha.art/"),
        ("fetched_at", "FAKE"),
        ("fetched_at", ""),
        ("fetched_at", None),
        ("user_hash", WHEN),
        ("user_hash", "f" * 63),
        ("user_hash", "g" * 64),
        ("user_hash", "F" * 64),
        ("website", "FAKE-BIO-TEXT"),
        ("website", "zz-fake-alpha.art"),
        ("website", "ftp://zz-fake-alpha.art/"),
    ],
)
def test_a_profile_column_refuses_a_value_of_the_wrong_shape_even_from_raw_sql(column, value):
    profile(tagline="FAKE-TAGLINE", website="https://zz-fake-alpha.art", user_id="FAKE-USERID")
    with pytest.raises(sqlite3.IntegrityError):
        raw(f"UPDATE profiles SET {column} = ?", value)
    raw("UPDATE profiles SET fetched_at = ?, user_hash = ?, website = ?", WHEN, "f" * 64, "http://zz-fake-alpha.art/x")
    raw("UPDATE profiles SET user_hash = NULL, website = NULL")


def refuse(table, column, value):
    """Make the database refuse one row, the way a constraint does: the statement fails with IntegrityError."""
    library.artist_keys()
    raw(
        f"CREATE TRIGGER fake_refusal BEFORE INSERT ON {table} WHEN NEW.{column} = '{value}' "
        "BEGIN SELECT RAISE(ABORT, 'FAKE-CONSTRAINT'); END"
    )


def test_a_work_row_the_database_refuses_is_left_out_and_the_rest_of_the_artist_is_stored(tmp_path):
    two_artists()
    refuse("works", "deviation_id", ID2)
    library.save_image(ID4, "image/jpeg", JPEG)
    assert keep(ALPHA, [fields(ID1), fields(ID2), fields(ID4)], tagline="FAKE-TAGLINE-NEW") == (1, 0)
    assert [(row["artist_key"], row["deviation_id"]) for row in rows("works")] == [
        (ALPHA, ID1),
        (BRAVO, ID3),
        (ALPHA, ID4),
    ]
    assert rows("profiles")[0]["tagline"] == "FAKE-TAGLINE-NEW"
    # The refused work is not kept in an older form either, and its file goes with it.
    assert files(tmp_path) == [*(f"cache/images/{work_id}.jpg" for work_id in (ID1, ID3, ID4)), "cache/pipeline.sqlite"]


def test_a_profile_row_the_database_refuses_leaves_the_artist_exactly_as_they_were(tmp_path):
    two_artists()
    before = (rows("profiles"), rows("works"), files(tmp_path))
    refuse("profiles", "artist_key", ALPHA)
    with pytest.raises(sqlite3.IntegrityError):
        keep(ALPHA, [fields(ID4)], tagline="FAKE-TAGLINE-NEW")
    assert (rows("profiles"), rows("works"), files(tmp_path)) == before
    assert keep(BRAVO, [fields(ID3)], tagline="FAKE-TAGLINE-NEW") == (0, 0)  # and the next artist is stored as usual


# --- storing ---


def test_storing_a_profile_twice_keeps_one_row_with_the_latest_fields():
    profile(tagline="FAKE-TAGLINE-OLD", website="https://zz-fake-alpha.art", user_id="FAKE-USERID-1")
    stored = profile(tagline="FAKE-TAGLINE-NEW", specialty="FAKE-SPECIALTY", level="FAKE-LEVEL")
    assert len(rows("profiles")) == 1
    assert {name: value for name, value in stored.items() if name != "fetched_at"} == {
        "artist_key": ALPHA,
        "tagline": "FAKE-TAGLINE-NEW",
        "specialty": "FAKE-SPECIALTY",
        "level": "FAKE-LEVEL",
        "website": None,
        "user_hash": None,
    }
    assert stored["fetched_at"].endswith("+00:00")


def test_storing_a_work_twice_keeps_one_row_that_names_its_cached_image():
    assert work(title="FAKE-TITLE-OLD", tags=["fakeold"])["image_file"] is None
    library.save_image(ID1, "image/jpeg", JPEG)
    stored = work(title="FAKE-TITLE-NEW", tags=["fakenew", "fake-two"], published="1700000000")
    assert len(rows("works")) == 1
    assert {name: value for name, value in stored.items() if name != "fetched_at"} == {
        "deviation_id": ID1,
        "artist_key": ALPHA,
        "page_url": PAGE,
        "title": "FAKE-TITLE-NEW",
        "tags": '["fakenew", "fake-two"]',
        "thumb_url": THUMB,
        "published_at": "2023-11-14T22:13:20+00:00",
        "image_file": f"{ID1}.jpg",
    }
    assert work()["image_file"] == f"{ID1}.jpg"


def test_a_deviation_id_is_one_row_however_it_is_cased():
    work(work_id=ID1.lower())
    work(work_id=ID1)
    assert [row["deviation_id"] for row in rows("works")] == [ID1]


def test_storing_an_artist_replaces_their_works_and_deletes_the_files_of_those_that_went(tmp_path):
    keep(ALPHA, [fields(ID1), fields(ID2)], tagline="FAKE-TAGLINE")
    keep(BRAVO, [fields(ID3)])
    for work_id in (ID1, ID2, ID3):
        library.save_image(work_id, "image/jpeg", JPEG)
    keep(ALPHA, [fields(ID2), fields(ID4)])
    assert [(row["artist_key"], row["deviation_id"], row["image_file"]) for row in rows("works")] == [
        (ALPHA, ID2, f"{ID2}.jpg"),
        (BRAVO, ID3, None),  # stored before its image was: only storing the artist again names it
        (ALPHA, ID4, None),
    ]
    assert files(tmp_path) == [f"cache/images/{ID2}.jpg", f"cache/images/{ID3}.jpg", "cache/pipeline.sqlite"]
    assert len(rows("profiles")) == 2


def test_rows_are_written_by_column_name_not_by_position():
    """The first version wrote profiles by position, which is how a changed table came to hold websites as
    fetch times. Here both tables have their columns in another order, and every value still lands in its own."""
    config.LOCAL_DB.parent.mkdir(parents=True)
    raw(
        "CREATE TABLE profiles (user_hash TEXT, fetched_at TEXT NOT NULL CHECK (fetched_at != ''), website TEXT, "
        "level TEXT, specialty TEXT, tagline TEXT, artist_key TEXT PRIMARY KEY) STRICT"
    )
    raw(
        "CREATE TABLE works (fetched_at TEXT NOT NULL, image_file TEXT, published_at TEXT, thumb_url TEXT NOT NULL, "
        "tags TEXT NOT NULL, title TEXT, page_url TEXT NOT NULL, artist_key TEXT NOT NULL, "
        "deviation_id TEXT PRIMARY KEY) STRICT"
    )
    library.save_image(ID1, "image/jpeg", JPEG)
    given = {"tagline": "FAKE-TAGLINE", "specialty": "FAKE-SPECIALTY", "level": "FAKE-LEVEL"}
    profile_fields = {**given, "website": "https://zz-fake-alpha.art", "user_id": "FAKE-USERID"}
    assert keep(ALPHA, [fields(published="1700000000")], **profile_fields) == (0, 0)
    stored = rows("profiles")[0]
    assert {name: stored[name] for name in ("artist_key", *given, "website")} == {
        "artist_key": ALPHA,
        **given,
        "website": "https://zz-fake-alpha.art/",
    }
    assert stored["fetched_at"].endswith("+00:00") and len(stored["user_hash"]) == 64
    stored = rows("works")[0]
    assert {name: value for name, value in stored.items() if name != "fetched_at"} == {
        "deviation_id": ID1,
        "artist_key": ALPHA,
        "page_url": PAGE,
        "title": "FAKE-TITLE-1",
        "tags": '["faketag"]',
        "thumb_url": THUMB,
        "published_at": "2023-11-14T22:13:20+00:00",
        "image_file": f"{ID1}.jpg",
    }
    assert stored["fetched_at"].endswith("+00:00")


def test_an_artist_is_stored_whole_or_not_at_all():
    """One transaction: when one of the works is refused, neither the profile nor any other work is written."""
    keep(ALPHA, [fields(ID1)], tagline="FAKE-TAGLINE-OLD")
    with pytest.raises(ValueError):
        keep(ALPHA, [fields(ID2), fields(ID3, page_url="https://zz-fake-evil.invalid/x")], tagline="FAKE-TAGLINE-NEW")
    assert [row["deviation_id"] for row in rows("works")] == [ID1]
    assert rows("profiles")[0]["tagline"] == "FAKE-TAGLINE-OLD"


def test_an_artist_is_written_with_one_commit(monkeypatch):
    """Commits are what is slow on a loaded disk: a profile and ten works are one, not eleven."""
    library.artist_keys()
    statements = []
    real = sqlite3.connect

    def traced(*args, **kwargs):
        conn = real(*args, **kwargs)
        conn.set_trace_callback(statements.append)
        return conn

    monkeypatch.setattr(sqlite3, "connect", traced)
    keep(ALPHA, [fields(f"00000000-0000-4000-A000-{n:012d}") for n in range(10)], tagline="FAKE-TAGLINE")
    assert [statement.strip() for statement in statements if statement.strip() in ("BEGIN", "COMMIT")] == [
        "BEGIN",
        "COMMIT",
    ]
    assert len([statement for statement in statements if statement.startswith("INSERT")]) == 11
    assert len(rows("works")) == 10


# --- hard rules 1 and 6: what a text may hold ---

KEPT = [
    ("  FAKE  words\n\tover   lines ", "FAKE words over lines"),
    ("<p>FAKE <b>bold</b> words</p>", "FAKE bold words"),
    ("FAKE &amp; words &lt;3", "FAKE & words <3"),
    ("FAKE &amp;amp;amp; words", "FAKE & words"),  # entities are decoded until nothing changes
    (f"FA{ZWSP}KE wo{SHY}rds{FILLER}\x00\ufeff\U000e0041", "FAKE words"),  # invisible characters are taken out
    ("ＦＡＫＥ words", "FAKE words"),  # fullwidth letters become plain ones
    (f'FAKE before <img src="{EMBEDDED[:5000]}"> after', "FAKE before after"),  # the markup goes, the picture with it
    ("FAKE before &lt;img src=data:image/png;base64,RkFLRQ==&gt; after", "FAKE before after"),
    ('FAKE <a href="mailto:zz.fake@mail.invalid">write</a> now', "FAKE write now"),
    ("FAKE 12 x 34 px, 26", "FAKE 12 x 34 px, 26"),  # six digits
    ("FAKE dot and a hat", "FAKE dot and a hat"),
    ("FAKE platemail, ringmail and a female knight", "FAKE platemail, ringmail and a female knight"),
    ("FAKE atlas of dotted lines", "FAKE atlas of dotted lines"),
    ("FAKE at the dotted line", "FAKE at the dotted line"),  # "at" is a word here, "dot" is not
    ("FAKE atlas with a dot", "FAKE atlas with a dot"),  # and here it is the other way round
    ("FAKE " + "FAKEBASE" * 12 + "FAK", "FAKE " + "FAKEBASE" * 12 + "FAK"),  # a run of 99: one short of looking encoded
    ("フェイク" * 40, ("フェイク" * 40)),  # 160 letters with no space: words, not a file
]
DROPPED = [
    # An embedded file, in any spelling.
    f"FAKE before {EMBEDDED} after",
    f'FAKE before <img src="{EMBEDDED}"> after',  # over 10,000 characters: not read at all
    CHUNKED,
    "FAKE before data:image/gif;base64,R0lGODlhAQABAAAAACw= after",
    "FAKE DATA:;base64,RkFLRQ==",
    f"FAKE da{ZWSP}ta:image/gif;base64,R0lGODlhAQABAAAAACw=",
    "FAKE da<b></b>ta:image/gif;base64,R0lGODlhAQABAAAAACw=",
    "FAKE d a t a : image/gif;base64,R0lGODlhAQABAAAAACw=",
    "FAKE &#100;ata&colon;image/gif;base64,R0lGODlhAQABAAAAACw=",
    "FAKE data%3Aimage/gif;base64,R0lGODlhAQABAAAAACw=",
    "FAKE ｄａｔａ：image/gif;base64,R0lGODlhAQABAAAAACw=",
    "FAKE metadata: the word is not worth the doubt",
    "FAKE before " + "FAKEBASE" * 12 + "FAKE after",  # a run of 100 that looks encoded, with not a digit in it
    "FAKE before " + "A" * 5000 + " after",
    # An email address, in every disguise a critic got past the first version.
    "FAKE write to zz.fake+tag@mail.invalid now",
    "FAKE write to zz.fake&#64;mail.invalid now",
    "FAKE write to zz.fake&amp;#64;mail.invalid now",
    "FAKE write to zz.fake&amp;amp;commat;mail.invalid now",
    "FAKE write to zz.fake＠mail.invalid now",  # a fullwidth @
    "FAKE write to zz.fake﹫mail.invalid now",  # a small @
    "FAKE write to zz.fake%40mail.invalid now",
    "FAKE write to zz.fake%2540mail.invalid now",
    # An "@" under six layers of entity and percent-encoding, one more than are undone. As written it holds
    # nothing suspect; it is dropped because it would not settle.
    "FAKE write to zz.fake&commat" + "&percnt" * 5 + "%3B" + "3B" * 5 + "mail.invalid now",
    f"FAKE write to zz.fa{ZWSP}ke@ma{SHY}il.invalid now",
    "FAKE write to <b>zz.fake</b>@mail.invalid now",
    "FAKE write to zz.fake@<wbr>mail.invalid now",
    "FAKE write to zz.fake@mäil.invalid now",  # a domain that is not ASCII
    "FAKE write to zz.fake@フェイク.invalid now",
    "FAKE write to zz.fake (at) mail (dot) invalid now",
    "FAKE write to zz.fake[at]mail[dot]invalid now",
    "FAKE write to zz.fake at mail dot invalid now",
    "FAKE @zzfake",
    # A number to call. Seven digits is where a phone number starts.
    "FAKE call +00 (555) 010-9999 now",
    "FAKE call 555 0199 now",
    "FAKE call 5550-1999 now",
    "FAKE call ５５５ ０１９９ now",  # fullwidth digits
    f"FAKE call 555{ZWSP}0{SHY}199 now",
    "FAKE call 555<b></b>0199 now",
    "FAKE call 5&#53;5 0199 now",
    "FAKE call ٥٥٥ ٠١٩٩ now",  # Arabic-Indic digits
    "FAKE years 2019-2023",  # eight digits: not worth the doubt
    "FAKE 10%25 20%25 off",  # eight digits as written, four once decoded: it is what is written that would be kept
    # A link, or a messaging service.
    "FAKE see https://zz-fake-alpha.art",
    "FAKE see www.zz-fake-alpha.art",
    "FAKE wa.me/zzfake",
    "FAKE t.me/zzfake",
    "FAKE WhatsApp zzfake",
    "FAKE Whats App zzfake",
    "FAKE telegram zzfake",
    "FAKE Discord zzfake#0001",
    "FAKE tel: 555",
    "FAKE skype:zzfake",
    "FAKE mailto:zzfake",
    *(f"FAKE {scheme}: zzfake" for scheme in ("sms", "callto", "facetime", "TEL")),
    *(f"FAKE {name}.me/zzfake" for name in ("m", "line", "vb", "kik", "vk", "zalo", "signal", "WA", "T")),
    *(f"FAKE {service} zzfake" for service in ("Viber", "WeChat", "weixin", "KakaoTalk", "whatsapp", "TELEGRAM")),
    "FAKE zz.fake {at} mail {dot} invalid",
    "FAKE zz.fake ( AT ) mail",
    "FAKE mail [ dot ] invalid",
    "FAKE zzfake_at_mail_dot_invalid",
    "FAKE zzfake.AT.mail.DOT.invalid",
    "FAKE zzfake gmail com",
    "FAKE e-mail zzfake",
    "FAKE my Email is zzfake",
    "FAKE zzfake on ProtonMail",
    # Not a text, or nothing left of it, or something that will not settle.
    "FAKE &" + "amp;" * 7 + "lt;b&gt;",  # seven layers deep: whatever is under them is not looked for
    "FAKE word " * 1001,  # over 10,000 characters
    "   ",
    "<br>",
    f"{ZWSP}{SHY}{FILLER}",
    None,
    12345,
    ["FAKE"],
    b"FAKE",
]


@pytest.mark.parametrize(("given", "kept"), KEPT, ids=short)
def test_a_plain_text_is_kept_as_plain_words(given, kept):
    assert profile(tagline=given)["tagline"] == kept
    assert work(title=given)["title"] == kept
    if kept and len(kept) <= 100:
        assert (profile(specialty=given)["specialty"], profile(level=given)["level"]) == (kept, kept)


@pytest.mark.parametrize("given", DROPPED, ids=short)
def test_a_text_that_may_hold_an_address_a_number_or_a_file_is_dropped_whole(given):
    """Hard rules 1 and 6. Nothing is cut out of a suspect text and the rest kept: the field stays empty."""
    stored = profile(tagline=given, specialty=given, level=given)
    assert (stored["tagline"], stored["specialty"], stored["level"]) == (None, None, None)
    assert work(title=given)["title"] is None
    assert work(tags=["fakeone", given, "faketwo"])["tags"] == '["fakeone", "faketwo"]'
    stored = database_bytes()
    assert b"mail" not in stored and b"0199" not in stored and b"base64" not in stored and b"zzfake" not in stored


def test_an_embedded_image_of_any_size_never_reaches_a_text_column():
    """The round-2 database finding, on this store: 200 KB of image in every text field, and none of it is kept."""
    for picture in (EMBEDDED, CHUNKED):
        profile(tagline=picture, specialty=picture, level=picture, website=picture, user_id=picture)
        work(title=picture, tags=[picture, "faketag"])
        stored = rows("profiles")[0]
        assert [stored[name] for name in ("tagline", "specialty", "level", "website")] == [None] * 4
        assert (rows("works")[0]["title"], rows("works")[0]["tags"]) == (None, '["faketag"]')
        assert b"FAKEBASE64" not in database_bytes() and b"base64" not in database_bytes()
    assert config.LOCAL_DB.stat().st_size < 100_000  # the empty tables and their indexes are about half of that


@pytest.mark.parametrize(
    "hostile",
    ["data:" * 1999, "<" * 9999, "<a" * 4999, "at " * 3333, "@a" * 4999, "1." * 4999, "&amp;" * 1999, "%25" * 3333],
    ids=short,
)
def test_checking_a_text_written_to_be_slow_is_still_quick(hostile):
    """A text over 10,000 characters is not read at all, and one just under it takes a few milliseconds."""
    started = time.perf_counter()
    profile(tagline=hostile, specialty=hostile, level=hostile, website=hostile)
    work(title=hostile, tags=[hostile] * 100)
    assert time.perf_counter() - started < 5


def test_every_text_is_cut_to_its_limit():
    long = "FAKE word " * 900
    stored = profile(tagline=long, specialty=long, level=long)
    assert [len(stored[name]) for name in ("tagline", "specialty", "level")] == [199, 99, 99]
    assert len(work(title=long)["title"]) == 199


def test_the_word_none_for_a_specialty_or_a_level_is_stored_as_nothing():
    """The API spells an unset specialty or level as the word None."""
    stored = profile(specialty="None", level="None", tagline="None")
    assert (stored["specialty"], stored["level"], stored["tagline"]) == (None, None, "None")


WEBSITES = [
    ("https://zz-fake-alpha.art", "https://zz-fake-alpha.art/"),
    ("  ZZ-Fake-Alpha.ART.  ", "https://zz-fake-alpha.art/"),
    ("http://www.zz-fake-alpha.com/folio/", "http://www.zz-fake-alpha.com/folio/"),
    ("https://www.artstation.com/zz-fake-alpha?FAKE-QUERY=1#FAKE-FRAGMENT", "https://www.artstation.com/zz-fake-alpha"),
    ("https://zz-fake-alpha.art/folio-123456", "https://zz-fake-alpha.art/folio-123456"),  # six digits
    # An "@", however it is written.
    ("zz.fake@mail.invalid", None),
    ("mailto:zz.fake@mail.invalid", None),
    ("https://zz.fake@mail.invalid", None),
    ("https://FAKE-USER:FAKE-PASSWORD@zz-fake-alpha.art/", None),
    ("https://zz-fake-alpha.art/write-to/zz.fake@mail.invalid", None),
    ("https://zz-fake-alpha.art/write-to/zz.fake%40mail.invalid", None),
    ("https://zz-fake-alpha.art/write-to/zz.fake%2540mail.invalid", None),
    ("https://zz-fake-alpha.art/write-to/zz.fake&#64;mail.invalid", None),
    ("https://zz-fake-alpha.art/write-to/zz.fake＠mail.invalid", None),
    ("https://zz-fake-alpha.art/?mail=zz.fake@mail.invalid", None),
    # A messaging service, with a number or without.
    ("https://wa.me/15550109999", None),
    ("https://wa.me/zzfake", None),
    ("wa.me/zzfake", None),
    ("https://api.whatsapp.com/send", None),
    ("https://t.me/+15550109999", None),
    ("https://t.me/zzfake", None),
    ("https://T.ME./zzfake", None),
    ("https://telegram.me/zzfake", None),
    ("https://discord.gg/zzfake", None),
    ("https://m.me/zzfake", None),
    ("https://line.me/ti/p/zzfake", None),
    ("https://signal.me/#p/zzfake", None),
    # Seven digits or more, anywhere in it.
    ("https://zz-fake-alpha.art/5550199", None),
    ("https://zz-fake-alpha.art/555/01/99", None),
    ("https://zz-fake-alpha.art/555%30199", None),
    ("https://zz-fake-alpha.art/５５５０１９９", None),
    ("https://zz-fake-5550199.art/", None),
    # An embedded file.
    ("https://zz-fake-alpha.art/data:image/png;base64,RkFLRQ==", None),
    ("https://zz-fake-alpha.art/DATA:image/png;base64,RkFLRQ==", None),
    ("https://zz-fake-alpha.art/data%3Aimage/png;base64,RkFLRQ==", None),
    ("https://zz-fake-alpha.art/%64ata:image/png;base64,RkFLRQ==", None),
    ("https://zz-fake-alpha.art/data%253Aimage/png;base64,RkFLRQ==", None),
    ("https://zz-fake-alpha.art/?x=data:image/png;base64,RkFLRQ==", None),
    ("https://zz-fake-alpha.art/" + "FAKEBASE" * 13, None),  # a run of over 100 that looks encoded, with no digit
    ("https://zz-fake-alpha.art/" + "FAKEBASE" * 11, "https://zz-fake-alpha.art/" + "FAKEBASE" * 11),
    (EMBEDDED, None),
    # Not an address.
    ("FAKE words that are not an address", None),
    ("see zz-fake-alpha.art for more", None),
    ("https://zz-fake-alpha.art/ and FAKE words after it", None),
    ("https://zz-fake-alpha.art/folio<br>FAKE words", None),
    ("javascript:alert(1)", None),
    ("ftp://zz-fake-alpha.art/", None),
    ("https://127.0.0.1/", None),
    ("https://localhost/", None),
    ("https://zz-fake-alpha.art/" + "a" * 300, None),
    ("", None),
    (None, None),
    (12345, None),
]


@pytest.mark.parametrize(("given", "kept"), WEBSITES, ids=short)
def test_a_website_is_kept_only_as_scheme_host_and_path_and_only_when_it_is_a_site(given, kept):
    assert profile(website=given)["website"] == kept
    stored = database_bytes()
    assert (
        b"mail.invalid" not in stored and b"zzfake" not in stored and b"0199" not in stored and b"data:" not in stored
    )


@pytest.mark.parametrize("domain", library.MESSAGING_DOMAINS)
def test_no_address_on_a_messaging_service_is_kept_as_a_website(domain):
    for address in (
        f"https://{domain}/zzfake",
        f"http://www.{domain}/zzfake",
        f"{domain.upper()}/zzfake",
        f"zzfake.{domain}",
    ):
        assert profile(website=address)["website"] is None, address
    assert (
        profile(website=f"https://zz-fake-{domain}/")["website"] == f"https://zz-fake-{domain}/"
    )  # only the service itself


def test_tags_are_kept_as_a_json_list_of_plain_tag_names():
    given = ["fakeone", "FAKE_Two", "fake-three", "フェイク", "fake four", "zz.fake@mail.invalid"]
    given += [EMBEDDED, "<b>fakefive</b>", "", None, 7, ["fakenested"], "x" * 60, "y" * 61, "5550199", "fake5550199"]
    given += ["zzfake_at_mail_dot_invalid", "zzfake-AT-mail-DOT-invalid", "zzfake_gmail_com", "email_zzfake", "e-mail"]
    given += ["platemail", "ringmail", "chainmail", "at_the_gates", "polkadot", "data", "no_ai"]
    kept = ["fakeone", "FAKE_Two", "fake-three", "フェイク", "fakefive", "x" * 60]
    kept += ["platemail", "ringmail", "chainmail", "at_the_gates", "polkadot", "data", "no_ai"]
    assert json.loads(work(tags=given)["tags"]) == kept
    assert b"zzfake" not in database_bytes()
    assert len(json.loads(work(tags=[f"fake{n}" for n in range(500)])["tags"])) == 60
    assert [work(tags=tags)["tags"] for tags in (None, "faketag", {"faketag": 1}, 5)] == ["[]"] * 4


NOT_ADDRESSES = [
    "http://www.deviantart.com/zz-fake-alpha/art/FAKE-1",
    "//www.deviantart.com/zz-fake-alpha/art/FAKE-1",
    "www.deviantart.com/zz-fake-alpha/art/FAKE-1",
    "https://www.deviantart.com.zz-fake-evil.invalid/zz-fake-alpha/art/FAKE-1",
    "https://zz-fake-evildeviantart.com/zz-fake-alpha/art/FAKE-1",
    "https://zz-fake-evil.invalid/www.deviantart.com/zz-fake-alpha/art/FAKE-1",
    "https://zz-fake-evil.invalid/?https://www.deviantart.com/zz-fake-alpha/art/FAKE-1",
    "https://www.deviantart.com@zz-fake-evil.invalid/zz-fake-alpha/art/FAKE-1",
    "https://FAKE-USER:FAKE-PASSWORD@www.deviantart.com/zz-fake-alpha/art/FAKE-1",
    "https://zz-fake-evil.invalid\\@www.deviantart.com/zz-fake-alpha/art/FAKE-1",
    "https://www.deviantart.com\\@zz-fake-evil.invalid/zz-fake-alpha/art/FAKE-1",
    "https://www.deviantart.com:8443/zz-fake-alpha/art/FAKE-1",
    "https://www.deviantart.com./zz-fake-alpha/art/FAKE-1",
    "https://www.deviantart.com/zz-fake-alpha/art/FAKE 1",
    "https://www.deviantart.com/zz-fake-alpha/art/FAKE-1\n",
    "https://www.deviantart.com/zz-fake-alpha/art/FAKE-フ",
    "https://www.deviantart.com/zz-fake-alpha/art/" + "a" * 2000,
    "https://www.deviantart.com/zz-fake-alpha/art/FAKE-1#FAKE-FRAGMENT",
    "https://www.deviantart.com/zz-fake-alpha/art/FAKE-1#data:image/png;base64,RkFLRQ==",
    "https://www.deviantart.com/zz-fake-alpha/art/data:image/png;base64,RkFLRQ==",
    "https://www.deviantart.com/zz-fake-alpha/art/data%3Aimage/png;base64,RkFLRQ==",
    "https://www.deviantart.com/zz-fake-alpha/art/FAKE-1?x=data:image/png;base64,RkFLRQ==",
    "https://www.deviantart.com/zz-fake-alpha/art/FAKE-1?x=DATA%3Aimage/png;base64,RkFLRQ==",
    "https://www.deviantart.com/zz-fake-alpha/art/FAKE-1?x=%2564ata%253Aimage/png;base64,RkFLRQ==",
    "https://www.deviantart.com/zz-fake-alpha/art/FAKE-1?x=&#100;ata:image/png;base64,RkFLRQ==",
    f"https://www.deviantart.com/zz-fake-alpha/art/FAKE-1?{SIGNED}&x=FAKE",
    "https://127.0.0.1/zz-fake-alpha/art/FAKE-1",
    "https://localhost/zz-fake-alpha/art/FAKE-1",
    "file:///zz-fake-alpha/art/FAKE-1.jpg",
    "javascript:alert(1)",
    EMBEDDED,
    "data:image/png;base64,RkFLRQ==",
    "",
    None,
    12345,
    b"https://www.deviantart.com/zz-fake-alpha/art/FAKE-1",
]


@pytest.mark.parametrize("address", NOT_ADDRESSES, ids=short)
def test_an_address_that_is_not_https_on_deviantart_is_refused_and_nothing_is_stored(address):
    for check in (library.page_address, library.image_address):
        with pytest.raises(ValueError):
            check(address)
    with pytest.raises(ValueError):
        work(page_url=address)
    with pytest.raises(ValueError):
        work(thumb_url=address)
    assert rows("works") == [] and rows("profiles") == []


@pytest.mark.parametrize(
    "address",
    [
        "https://www.deviantart.com/",
        "https://www.deviantart.com/zz-fake-alpha",
        "https://www.deviantart.com/zz-fake-alpha/art/",
        "https://www.deviantart.com/zz-fake-alpha/art/FAKE-1/more",
        "https://www.deviantart.com/zz-fake-alpha/journal/FAKE-1",
        "https://www.deviantart.com/zz-fake-alpha/art/FAKE-1?FAKE-QUERY=1",
        f"https://www.deviantart.com/zz-fake-alpha/art/FAKE-1?{SIGNED}",
        "https://www.deviantart.com/zz-fake-alpha/art/FAKE-1;base64,RkFLRQ==",
        "https://www.deviantart.com/zz-fake-alpha/art/" + "FAKEBASE64" * 31,
        THUMB,
    ],
    ids=short,
)
def test_a_works_page_is_handle_art_title_on_deviantart_and_nothing_else(address):
    with pytest.raises(ValueError):
        library.page_address(address)
    with pytest.raises(ValueError):
        work(page_url=address)


@pytest.mark.parametrize(
    "address",
    [
        "https://images-wixmp-zzcdn.wixmp.com/f/FAKE-THUMB-1.jpg?FAKE-QUERY=1",
        "https://images-wixmp-zzcdn.wixmp.com/f/FAKE-THUMB-1.jpg?token=FAKE",
        "https://images-wixmp-zzcdn.wixmp.com/f/FAKE-THUMB-1.jpg?token=FAKE.FAKE.FAKE=",
        f"https://images-wixmp-zzcdn.wixmp.com/f/FAKE-THUMB-1.jpg?{SIGNED}&x=FAKEBASE64",
        f"https://images-wixmp-zzcdn.wixmp.com/f/FAKE-THUMB-1.jpg?x=FAKEBASE64&{SIGNED}",
        f"https://images-wixmp-zzcdn.wixmp.com/f/FAKE-THUMB-1.jpg?{SIGNED}#FAKE-FRAGMENT",
        f"https://images-wixmp-zzcdn.wixmp.com/f/data:image/png;base64,RkFLRQ==?{SIGNED}",
        "https://images-wixmp-zzcdn.wixmp.com/f/" + "a" * 2000,
        PAGE.replace("www.deviantart.com", "zz-fake-evil.invalid"),
    ],
    ids=short,
)
def test_an_image_address_carries_no_query_but_deviantarts_own_signature(address):
    with pytest.raises(ValueError):
        library.image_address(address)
    with pytest.raises(ValueError):
        work(thumb_url=address)


def test_the_addresses_deviantart_really_gives_are_accepted():
    assert library.page_address(PAGE) == PAGE
    assert library.page_address("HTTPS://WWW.DeviantArt.com/zz-fake-alpha/art/FAKE-1") == (
        "https://www.deviantart.com/zz-fake-alpha/art/FAKE-1"
    )
    odd = (
        "https://www.deviantart.com/zz_fake-Alpha/art/FAKE.Title-%E3%83%95%E3%82%A7-1001"  # a dot, and encoded letters
    )
    assert library.page_address(odd) == odd and work(page_url=odd)["page_url"] == odd
    assert library.image_address(THUMB) == THUMB and work()["thumb_url"] == THUMB
    plain = "https://zzcdn00.deviantart.net/f/FAKE,w_300,q_70,strp/FAKE-1.jpg"  # no signature at all
    assert library.image_address(plain) == plain


@pytest.mark.parametrize(
    "work_id",
    [
        "",
        "FAKE",
        "../../FAKE",
        ID1 + "\n",
        ID1 + ".jpg",
        ID1.replace("-", ""),
        ID1.replace("A", "G"),
        "*",
        None,
        7,
        [ID1],
    ],
    ids=short,
)
def test_an_id_that_is_not_a_uuid_is_refused(work_id):
    """The id becomes a file name, so nothing but a UUID gets as far as the disk or the database."""
    with pytest.raises(ValueError):
        work(work_id=work_id)
    with pytest.raises(ValueError):
        library.save_image(work_id, "image/jpeg", JPEG)
    with pytest.raises(ValueError):
        library.cached_image(work_id)
    assert rows("works") == [] and not library.image_dir().exists()


@pytest.mark.parametrize(
    "key", ["zz-fake-alpha", "da:ZZ-Fake-Alpha", "da:", "da:zz fake", "site:zz-fake.art", "", None, 7]
)
def test_an_artist_key_must_be_spelled_the_way_the_store_spells_it(key):
    """Otherwise a removal, which goes by the store's key, would not find what was kept under it."""
    for call in (profile, work, library.purge):
        with pytest.raises(ValueError):
            call(key)
    assert library.artist_keys() == []
    assert ALPHA == store.artist_key("ZZ-Fake-Alpha")


@pytest.mark.parametrize(
    ("given", "kept"),
    [
        ("1700000000", "2023-11-14T22:13:20+00:00"),
        (1700000000, "2023-11-14T22:13:20+00:00"),
        ("0", "1970-01-01T00:00:00+00:00"),
        ("FAKE", None),
        ("", None),
        (None, None),
        ("9" * 30, None),
        ("9" * 5000, None),
        (float("inf"), None),
        (EMBEDDED, None),
    ],
    ids=short,
)
def test_a_published_time_is_stored_as_a_utc_timestamp_or_not_at_all(given, kept):
    assert work(published=given)["published_at"] == kept


# --- hard rule 2: image bytes ---


def test_an_image_is_written_into_the_cache_directory_and_nowhere_else(tmp_path):
    assert library.save_image(ID1, "image/jpeg", JPEG) == f"{ID1}.jpg"
    assert files(tmp_path) == [f"cache/images/{ID1}.jpg"]  # no database is opened to write an image
    assert (config.CACHE_DIR / "images" / f"{ID1}.jpg").read_bytes() == JPEG
    assert work()["image_file"] == f"{ID1}.jpg"
    assert files(tmp_path) == [f"cache/images/{ID1}.jpg", "cache/pipeline.sqlite"]
    # Nothing image-like in the database: not the bytes, and no column of any row that is not text or empty.
    assert b"FAKE-IMAGE-BYTES" not in database_bytes()
    with closing(sqlite3.connect(config.LOCAL_DB)) as conn:
        names = [name for _, name, *_ in conn.execute("PRAGMA table_info(works)")]
        kinds = conn.execute(f"SELECT DISTINCT {', '.join(f'typeof({name})' for name in names)} FROM works").fetchall()
    assert set(kinds[0]) <= {"text", "null"}


def test_the_cache_directory_is_the_git_ignored_one_and_cannot_be_passed_in():
    """Hard rule 2. The directory comes from config alone: no function here takes one."""
    assert library.image_dir() == config.CACHE_DIR / "images"
    assert REAL_CACHE_DIR == config.ROOT / "cache"
    ignored = subprocess.run(
        ["git", "check-ignore", "-q", str(REAL_CACHE_DIR / "images" / f"{ID1}.jpg")], cwd=config.ROOT, check=False
    )
    assert ignored.returncode == 0
    assert set(inspect.signature(library.save_image).parameters) == {"work_id", "content_type", "data"}
    for function in (library.cached_image, library.purge, library.drop_orphans, library.store_artist):
        assert not any("dir" in name or "path" in name for name in inspect.signature(function).parameters)
    assert inspect.signature(library.image_dir).parameters == {}


NOT_IMAGES = [
    ("image/svg+xml", b"<svg>FAKE</svg>"),
    ("text/html", b"<html>FAKE</html>"),
    ("application/octet-stream", JPEG),
    ("image/bmp", b"BMFAKE"),
    ("", JPEG),
    ("IMAGE/JPEG", JPEG),
    ("image/jpeg", b"<html>FAKE error page</html>"),  # the right label on the wrong bytes
    ("image/jpeg", PNG),
    ("image/png", JPEG),
    ("image/jpeg", b""),
    ("image/webp", b"RIFF\x24\x00\x00\x00WAVEfmt " + b"FAKE-SOUND" * 50),  # RIFF is also what a sound file is
    ("image/webp", b"RIFF\x24\x00\x00\x00AVI LIST" + b"FAKE-FILM" * 50),  # and a film
    ("image/webp", b"RIFF"),
    ("image/webp", b"WEBP" + WEBP),
    ("image/png", b"\x89PNG\r\n\x1a\n" + b"FAKE" * 50),  # the eight bytes, and then no image header
    ("image/gif", b"GIF8" + b"FAKE" * 50),
    ("image/gif", b"GIF90a" + b"FAKE" * 50),
    ("image/jpeg", JPEG + b"\x00" * (library.MAX_IMAGE_BYTES - len(JPEG) + 1)),  # one byte over the cap
]


@pytest.mark.parametrize(("content_type", "data"), NOT_IMAGES, ids=short)
def test_bytes_that_are_not_an_accepted_image_are_not_written(content_type, data, tmp_path):
    with pytest.raises(ValueError):
        library.save_image(ID1, content_type, data)
    assert files(tmp_path) == []


def test_an_image_exactly_at_the_cap_and_each_accepted_type_are_written(tmp_path):
    images = {"image/jpeg": JPEG, "image/png": PNG, "image/gif": GIF, "image/webp": WEBP}
    for work_id, (content_type, data) in zip((ID1, ID2, ID3, ID4), images.items(), strict=True):
        library.save_image(work_id, content_type, data)
    assert library.save_image(ID3, "image/gif", b"GIF87a" + b"FAKE") == f"{ID3}.gif"
    library.save_image(ID1, "image/jpeg", JPEG + b"\x00" * (library.MAX_IMAGE_BYTES - len(JPEG)))
    assert [name.rsplit(".", 1)[1] for name in files(tmp_path)] == ["jpg", "png", "gif", "webp"]
    assert set(images) == set(library.IMAGE_TYPES)


def test_a_cached_image_is_one_that_its_row_names_and_that_is_there():
    work()
    assert library.cached_image(ID1) is False
    library.image_dir().mkdir(parents=True)
    (library.image_dir() / f"{ID1}.png.part").write_bytes(PNG)  # half a download is not a cached image
    assert work()["image_file"] is None and library.cached_image(ID1) is False
    library.save_image(ID1, "image/png", PNG)
    assert library.cached_image(ID1) is False  # in the cache, but its row does not name it yet
    assert work()["image_file"] == f"{ID1}.png"
    assert library.cached_image(ID1) is True and library.cached_image(ID1.lower()) is True
    (library.image_dir() / f"{ID1}.png").unlink()
    assert library.cached_image(ID1) is False and library.cached_image(ID2) is False


# --- hard rule 7: purge, orphans, a changed handle ---


def two_artists():
    for key, ids in ((ALPHA, (ID1, ID2)), (BRAVO, (ID3,))):
        handle = key.removeprefix("da:")
        for work_id in ids:
            library.save_image(work_id, "image/jpeg", JPEG)
        page = f"https://www.deviantart.com/{handle}/art/FAKE"
        works = [fields(work_id, title=f"FAKE-TITLE-{handle}", page_url=page) for work_id in ids]
        keep(
            key,
            works,
            tagline=f"FAKE-TAGLINE-{handle}",
            website=f"https://{handle}.art",
            user_id=f"FAKE-USERID-{handle}",
        )


def test_purge_leaves_no_row_and_no_file_for_the_artist_and_touches_nobody_else(tmp_path):
    two_artists()
    (library.image_dir() / f"{ID1}.png").write_bytes(PNG)  # a second file of the same work, under another type
    (library.image_dir() / f"{ID2}.jpg.part").write_bytes(JPEG)  # and half a download
    library.purge(ALPHA)
    assert [row["artist_key"] for row in rows("profiles")] == [BRAVO]
    assert [(row["artist_key"], row["deviation_id"]) for row in rows("works")] == [(BRAVO, ID3)]
    assert files(tmp_path) == ["cache/images/" + f"{ID3}.jpg", "cache/pipeline.sqlite"]
    assert library.artist_keys() == [BRAVO]
    # Not left behind in the file either: the store opens it with secure_delete on.
    assert b"zz-fake-alpha" not in database_bytes() and ID1.encode() not in database_bytes()
    assert b"zz-fake-bravo" in database_bytes()
    library.purge(ALPHA)  # safe to repeat
    library.purge(BRAVO)
    assert rows("profiles") == rows("works") == [] and files(tmp_path) == ["cache/pipeline.sqlite"]


def test_purge_removes_an_artist_who_has_only_a_profile_or_only_works():
    profile(ALPHA, tagline="FAKE-TAGLINE")
    work(BRAVO, ID3)
    with closing(sqlite3.connect(config.LOCAL_DB)) as conn, conn:
        conn.execute("DELETE FROM profiles WHERE artist_key = ?", (BRAVO,))
    assert library.artist_keys() == [ALPHA, BRAVO]
    library.purge(ALPHA)
    library.purge(BRAVO)
    assert library.artist_keys() == []


def test_purge_does_not_take_a_file_name_from_a_row_on_trust(tmp_path):
    """A row somebody tampered with must not make purge delete another artist's images, or a file elsewhere."""
    two_artists()
    outside = tmp_path / "FAKE-OUTSIDE.jpg"
    outside.write_bytes(JPEG)
    with closing(sqlite3.connect(config.LOCAL_DB)) as conn, conn:
        conn.execute(
            "UPDATE works SET deviation_id = '*', image_file = '../../FAKE-OUTSIDE.jpg' WHERE deviation_id = ?", (ID1,)
        )
        conn.execute("UPDATE works SET deviation_id = '../../FAKE-OUTSIDE' WHERE deviation_id = ?", (ID2,))
    library.purge(ALPHA)
    assert outside.read_bytes() == JPEG
    assert (library.image_dir() / f"{ID3}.jpg").read_bytes() == JPEG
    assert [row["artist_key"] for row in rows("works")] == [BRAVO]


def locked(*names):
    """A stand-in for Path.unlink that cannot delete the files of these names, as if another program held them."""
    real = type(config.ROOT).unlink

    def unlink(path, *args, **kwargs):
        if path.name in names:
            raise PermissionError("FAKE-LOCKED")
        real(path, *args, **kwargs)

    return unlink


def test_a_file_that_cannot_be_deleted_does_not_keep_the_rest_of_the_purge_from_happening(monkeypatch, tmp_path):
    """Everything else goes. The stuck work keeps its row, so the artist is still listed and the purge can be rerun."""
    two_artists()
    with monkeypatch.context() as patch:
        patch.setattr("pathlib.Path.unlink", locked(f"{ID1}.jpg"))
        with pytest.raises(OSError):
            library.purge(ALPHA)
    assert [(row["artist_key"], row["deviation_id"]) for row in rows("works")] == [(ALPHA, ID1), (BRAVO, ID3)]
    assert [row["artist_key"] for row in rows("profiles")] == [BRAVO]
    assert files(tmp_path) == [f"cache/images/{ID1}.jpg", f"cache/images/{ID3}.jpg", "cache/pipeline.sqlite"]
    assert library.artist_keys() == [ALPHA, BRAVO]
    library.purge(ALPHA)
    assert library.artist_keys() == [BRAVO] and files(tmp_path) == [f"cache/images/{ID3}.jpg", "cache/pipeline.sqlite"]


def test_storing_an_artist_reports_a_stale_file_it_could_not_delete_after_storing_everything(monkeypatch):
    two_artists()
    with monkeypatch.context() as patch:
        patch.setattr("pathlib.Path.unlink", locked(f"{ID1}.jpg"))
        assert keep(ALPHA, [fields(ID2), fields(ID4)], tagline="FAKE-TAGLINE-NEW") == (0, 1)
    assert [row["deviation_id"] for row in rows("works") if row["artist_key"] == ALPHA] == [ID1, ID2, ID4]
    assert rows("profiles")[0]["tagline"] == "FAKE-TAGLINE-NEW"


def test_an_account_that_changed_its_handle_does_not_leave_its_old_key_behind(tmp_path):
    """Hard rule 7 across a rename. The same user id under a new handle takes over the works it still has; the
    old key's profile and its other works go, so removing the new handle later leaves nothing of the old one."""
    two_artists()
    library.save_image(ID4, "image/jpeg", JPEG)
    page = "https://www.deviantart.com/zz-fake-renamed/art/FAKE"
    works = [fields(ID2, page_url=page), fields(ID4, page_url=page)]
    keep("da:zz-fake-renamed", works, tagline="FAKE-TAGLINE-RENAMED", user_id="FAKE-USERID-zz-fake-alpha")
    assert [row["artist_key"] for row in rows("profiles")] == [BRAVO, "da:zz-fake-renamed"]
    assert [(row["artist_key"], row["deviation_id"], row["image_file"]) for row in rows("works")] == [
        ("da:zz-fake-renamed", ID2, f"{ID2}.jpg"),
        (BRAVO, ID3, f"{ID3}.jpg"),
        ("da:zz-fake-renamed", ID4, f"{ID4}.jpg"),
    ]
    assert files(tmp_path) == [*(f"cache/images/{work_id}.jpg" for work_id in (ID2, ID3, ID4)), "cache/pipeline.sqlite"]
    assert b"zz-fake-alpha" not in database_bytes()
    library.purge("da:zz-fake-renamed")
    assert library.artist_keys() == [BRAVO] and b"zz-fake-renamed" not in database_bytes()


def test_the_user_id_is_kept_only_as_a_hash_and_only_a_real_one_links_two_keys():
    keep(ALPHA, [fields(ID1)], user_id="FAKE-USERID-1")
    keep(BRAVO, [fields(ID3)], user_id="FAKE-USERID-2")
    assert [row["user_hash"] for row in rows("profiles")] == [
        hashlib.sha256(f"da-user:{user_id}".encode()).hexdigest() for user_id in ("FAKE-USERID-1", "FAKE-USERID-2")
    ]
    assert b"FAKE-USERID" not in database_bytes()
    for missing in (None, "", 7, ["FAKE-USERID-1"], {"FAKE": 1}):  # no id is no link: nobody is purged for it
        keep(ALPHA, [fields(ID1)], user_id=missing)
        keep(BRAVO, [fields(ID3)], user_id=missing)
        assert [row["user_hash"] for row in rows("profiles")] == [None, None] and len(rows("works")) == 2


def test_drop_orphans_deletes_every_cached_file_that_no_row_names(tmp_path):
    """A lost database, a run cut short between a download and its row, half a download, a stray file: no purge
    could ever find these, because a purge goes by rows."""
    two_artists()
    orphans = [f"{ID4}.jpg", f"{ID1}.png", f"{ID2}.jpg.part", "FAKE-STRAY.txt", "FAKE-FOLDER/FAKE-NESTED.jpg"]
    orphans.append(f"FAKE-FOLDER/{ID1}.jpg")  # named like a file a row does name, but it is not that file
    for name in orphans:
        (library.image_dir() / name).parent.mkdir(exist_ok=True)
        (library.image_dir() / name).write_bytes(JPEG)
    with closing(sqlite3.connect(config.LOCAL_DB)) as conn, conn:
        conn.execute("UPDATE works SET image_file = NULL WHERE deviation_id = ?", (ID3,))  # a row that names no file
    assert library.drop_orphans() == (7, 0)
    assert files(tmp_path) == [f"cache/images/{ID1}.jpg", f"cache/images/{ID2}.jpg", "cache/pipeline.sqlite"]
    assert library.drop_orphans() == (0, 0)
    config.LOCAL_DB.unlink()  # the database is lost and the images stay
    assert library.drop_orphans() == (2, 0)
    assert files(tmp_path) == ["cache/pipeline.sqlite"]


def test_drop_orphans_counts_what_it_could_not_delete_and_carries_on(monkeypatch, tmp_path):
    for work_id in (ID1, ID2, ID3):
        library.save_image(work_id, "image/jpeg", JPEG)
    with monkeypatch.context() as patch:
        patch.setattr("pathlib.Path.unlink", locked(f"{ID2}.jpg"))
        assert library.drop_orphans() == (2, 1)
    assert files(tmp_path) == [f"cache/images/{ID2}.jpg", "cache/pipeline.sqlite"]
    assert library.drop_orphans() == (1, 0)


def test_drop_orphans_leaves_the_cache_alone_when_the_database_is_not_the_one_the_cache_belongs_to(tmp_path):
    """Measured against some other database, every image in the cache would look like an orphan."""
    two_artists()
    other = tmp_path / "elsewhere" / "other.sqlite"
    keep(BRAVO, [fields(ID3)], db=other)
    assert library.drop_orphans(other) == (0, 0)
    assert len(files(tmp_path / "cache" / "images")) == 3
    assert library.drop_orphans(config.LOCAL_DB) == (0, 0) and library.drop_orphans(str(config.LOCAL_DB)) == (0, 0)
    library.purge(ALPHA)
    (library.image_dir() / f"{ID4}.jpg").write_bytes(JPEG)
    assert library.drop_orphans(config.LOCAL_DB) == (1, 0)
    assert files(tmp_path / "cache" / "images") == [f"{ID3}.jpg"]


def test_drop_orphans_with_no_cache_directory_does_nothing(tmp_path):
    assert library.drop_orphans() == (0, 0) and files(tmp_path) == []
