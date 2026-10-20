"""Tests for pipeline.library.

Every handle, address, text and byte below is invented for these tests. Nothing came from DeviantArt and nothing
here fetches anything. The cache directory and the database are moved into pytest's temporary directory.
"""

import base64
import hashlib
import inspect
import json
import sqlite3
import subprocess
import time
from contextlib import closing, suppress

import pytest

from pipeline import config, library, store

REAL_CACHE_DIR = config.CACHE_DIR  # as the project sets it, before the fixture below moves it

ALPHA, BRAVO = "da:zz-fake-alpha", "da:zz-fake-bravo"
ID1, ID2, ID3, ID4 = (f"00000000-0000-4000-A000-{n:012d}" for n in (1, 2, 3, 4))
PAGE = "https://www.deviantart.com/zz-fake-alpha/art/FAKE-TITLE-1-1001"


def encoded(value):
    """What a signature's parts are made of: URL-safe base64 with the padding left off."""
    data = value if isinstance(value, bytes) else json.dumps(value, separators=(",", ":")).encode()
    return base64.urlsafe_b64encode(data).decode().rstrip("=")


HEADER = encoded({"typ": "JWT", "alg": "HS256"})  # the header every real signature seen so far begins with
SIGNED = f"token={HEADER}.FAKE-CLAIMS-1.FAKE-SIGNATURE"  # DeviantArt's signature on an image address
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
    ours = ("profiles", "works", "skipped", "work_marks", "artist_marks")
    with closing(sqlite3.connect(config.LOCAL_DB)) as conn:
        columns = {
            table: [(name, kind) for _, name, kind, *_ in conn.execute(f"PRAGMA table_info({table})")] for table in ours
        }
        strict = conn.execute(f"SELECT strict FROM pragma_table_list WHERE name IN {ours}").fetchall()
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
    assert columns["skipped"] == [("artist_key", "TEXT"), ("skipped_at", "TEXT"), ("unavailable", "INTEGER")]
    # Round 5: what an answer says against a kept work or artist is written beside it. In tables of their own,
    # so that the two tables other stages read are as they were, and opening the file still changes no table.
    assert columns["work_marks"] == [("deviation_id", "TEXT"), ("reason", "TEXT"), ("marked_at", "TEXT")]
    assert columns["artist_marks"] == [
        ("artist_key", "TEXT"),
        ("reason", "TEXT"),
        ("marked_at", "TEXT"),
        ("same_as", "TEXT"),
    ]
    assert strict == [(1,)] * 5
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
    """A database with a profiles table of an earlier shape, and works kept under the first version's looser rules.
    It has the mark of the readings migration, which leaves these two tables as they are: without the mark
    nothing opens it at all (see test_a_store_made_before_readings_carried_fingerprints_is_not_opened)."""
    config.LOCAL_DB.parent.mkdir(parents=True)
    with closing(sqlite3.connect(config.LOCAL_DB)) as conn, conn:
        conn.execute(f"PRAGMA user_version = {library.SHAPE}")
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
    assert library.last_attempts() == {ALPHA: WHEN}
    assert library.account_hash(ALPHA) is None and library.account_removed("FAKE-USERID") is False
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
    assert keep("da:zz-fake-charlie", tagline="FAKE-TAGLINE-NEW", user_id="FAKE-USERID") == (0, 0, 0)

    made = raw("SELECT sql FROM sqlite_master WHERE name = 'profiles'")[0][0]
    assert "bio" not in made and "CHECK" in made and "STRICT" in made
    assert raw("SELECT name FROM sqlite_master WHERE name LIKE 'profiles%' AND type = 'table'") == [("profiles",)]
    blank = {"tagline": None, "specialty": None, "level": None, "website": None, "user_hash": None}
    when = WHEN if earlier == FIRST_VERSION else "1970-01-01T00:00:00+00:00"  # the incident's rows had lost theirs
    assert rows("profiles")[:2] == [{"artist_key": key, **blank, "fetched_at": when} for key in (ALPHA, BRAVO)]
    assert rows("profiles")[2]["tagline"] == "FAKE-TAGLINE-NEW" and len(rows("profiles")[2]["user_hash"]) == 64
    assert [row["title"] for row in rows("works")] == ["FAKE-TITLE-PLAIN 2026", None, "FAKE-TITLE 555 0199", None]
    assert [row["image_file"] for row in rows("works")] == [f"{work_id}.jpg" for work_id in (ID1, ID2, ID3, ID4)]
    stored = database_bytes()
    for gone in (b"FAKE-BIO-TEXT", b"FAKE-TAGLINE call", b"wa.me", b"15550109999", b"zz.fake@mail", b"data:image"):
        assert gone not in stored, gone
    assert b"zz-fake-bravo.art" not in stored
    assert keep(ALPHA, [fields()], tagline="FAKE-TAGLINE-ALPHA") == (0, 0, 0)  # and it is rebuilt once, not every time
    assert rows("profiles")[2]["tagline"] == "FAKE-TAGLINE-NEW" and len(rows("works")) == 4  # and nothing is dropped


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
    before = rows("works")[1]
    assert keep(ALPHA, [fields(ID1), fields(ID2), fields(ID4)], tagline="FAKE-TAGLINE-NEW") == (1, 0, 0)
    assert [(row["artist_key"], row["deviation_id"]) for row in rows("works")] == [
        (ALPHA, ID1),
        (ALPHA, ID2),
        (BRAVO, ID3),
        (ALPHA, ID4),
    ]
    assert rows("profiles")[0]["tagline"] == "FAKE-TAGLINE-NEW"
    assert rows("works")[1] == before  # the refused work stays as it was kept, with its file
    assert files(tmp_path) == [
        *(f"cache/images/{work_id}.jpg" for work_id in (ID1, ID2, ID3, ID4)),
        "cache/pipeline.sqlite",
    ]


def test_a_profile_row_the_database_refuses_leaves_the_artist_exactly_as_they_were(tmp_path):
    two_artists()
    before = (rows("profiles"), rows("works"), files(tmp_path))
    refuse("profiles", "artist_key", ALPHA)
    with pytest.raises(sqlite3.IntegrityError):
        keep(ALPHA, [fields(ID4)], tagline="FAKE-TAGLINE-NEW")
    assert (rows("profiles"), rows("works"), files(tmp_path)) == before
    assert keep(BRAVO, [fields(ID3)], tagline="FAKE-TAGLINE-NEW") == (0, 0, 0)  # and the next artist is stored as usual


# --- storing ---


def test_storing_a_profile_twice_keeps_one_row_and_brings_up_to_date_what_was_said():
    """A field is brought up to date when the answer says something in it. Not given, given as anything but a
    text, or given empty, it says nothing: the critic's findings were a 200 with nothing in it that blanked what
    was kept, and in round 7 an empty text that did."""
    profile(
        tagline="FAKE-TAGLINE-OLD", level="FAKE-LEVEL-OLD", website="https://zz-fake-alpha.art", user_id="FAKE-USERID-1"
    )
    stored = profile(tagline="FAKE-TAGLINE-NEW", specialty="FAKE-SPECIALTY", level="")
    assert len(rows("profiles")) == 1
    assert {name: value for name, value in stored.items() if name != "fetched_at"} == {
        "artist_key": ALPHA,
        "tagline": "FAKE-TAGLINE-NEW",
        "specialty": "FAKE-SPECIALTY",
        "level": "FAKE-LEVEL-OLD",  # given as an empty text: nothing was said about it
        "website": "https://zz-fake-alpha.art/",  # not given: the same
        "user_hash": account("FAKE-USERID-1"),  # nor about whose account it is
    }
    assert stored["fetched_at"].endswith("+00:00")
    # No text, an empty one, one that shows nothing, and the API's word for a specialty or a level that is not given.
    for nothing in (None, 7, ["FAKE"], {"FAKE": 1}, b"FAKE", "", "  \n", ZWSP, "&nbsp;", "<br>", "None"):
        choice = {} if nothing == "None" else {"tagline": nothing, "website": nothing}
        again = profile(**choice, specialty=nothing, level=nothing, user_id=nothing)
        assert {name: again[name] for name in stored if name != "fetched_at"} == {
            name: stored[name] for name in stored if name != "fetched_at"
        }
    assert library.artist_marks() == {} and library.website(ALPHA) == "https://zz-fake-alpha.art/"
    # Round 8. An answer these rules refuse blanked what was kept, and the run exited 0: a tagline that names a
    # domain, a website answered "N/A", a level of seven digits. What is kept stays, and the artist is marked
    # for it, the way a kept work is when its new title is not acceptable.
    kept = {name: stored[name] for name in stored if name != "fetched_at"}
    refused = {"tagline": "FAKE see zz-fake-alpha.art", "website": "N/A", "level": "5550199"}
    refused["specialty"] = "FAKE write to zz.fake@mail.invalid"
    for name, answer in refused.items():
        assert keep(ALPHA, **{name: answer}) == (0, 0, 1)
        assert {name: rows("profiles")[0][name] for name in kept} == kept
        assert library.artist_marks()[ALPHA][0] == library.REFUSED == "answer refused"
        assert keep(ALPHA) == (0, 0, 0) and library.artist_marks() == {}  # and the mark goes with the answer
    assert keep(ALPHA, **refused) == (0, 0, 4)
    # With nothing kept there is nothing to hold on to: the field stays empty, and nobody is marked for that.
    assert keep(BRAVO, **refused) == (0, 0, 0) and library.artist_marks().keys() == {ALPHA}
    assert [rows("profiles")[1][name] for name in refused] == [None] * 4
    assert library.website(BRAVO) is None and library.website("da:zz-fake-nobody") is None


def test_a_field_the_artist_emptied_stays_kept_and_is_written_down_and_one_not_answered_changes_nothing():
    """The fourth critic of the push stage: an artist who emptied their tagline and website was still shown with
    them. What is kept stays, against an answer that blanks a field by mistake, and the answer is written down
    beside it (library.answers): the push stage sends what the latest answer that said something says."""
    profile(tagline="FAKE-TAGLINE-OLD", website="https://zz-fake-alpha.art")
    kept = ("FAKE-TAGLINE-OLD", "https://zz-fake-alpha.art/")
    assert library.answers() == {ALPHA: (False, False, 0)}
    for empty in ("", "  \n", ZWSP, "<br>"):  # a text that shows nothing: the artist emptied the field
        stored = profile(tagline=empty, website=empty, mature=3)
        assert (stored["tagline"], stored["website"]) == kept and library.answers() == {ALPHA: (True, True, 3)}
    for nothing in (None, 7, ["FAKE"], b"FAKE", "FAKE see zz-fake-alpha.art"):  # not answered, or refused
        stored = profile(tagline=nothing, website=nothing)
        assert (stored["tagline"], stored["website"]) == kept and library.answers() == {ALPHA: (True, True, 0)}
    profile(tagline="FAKE-TAGLINE-NEW")  # given again: it is theirs again
    assert library.answers() == {ALPHA: (False, True, 0)}
    with pytest.raises(sqlite3.IntegrityError):  # a count is a number of works, and nothing else
        profile(mature=-1)


def test_a_kept_value_that_todays_rules_refuse_goes_and_nobody_is_marked_for_it():
    """Round 8, and counted in the real store: 62 websites and 4 taglines kept on 2026-10-12 under an earlier,
    looser rule, which today's rule refuses. Answered again as they are, each would be an answer refused over a
    kept value, and 66 artists would be marked and held back for a rule of ours, not for anything they said."""
    profile(
        tagline="FAKE-TAGLINE-OLD", specialty="FAKE-SPECIALTY", website="https://zz-fake-alpha.art", user_id="FAKE-1"
    )
    looser = {"website": "https://www.facebook.com/zzfake/about", "tagline": "FAKE see zz-fake-alpha.art"}
    for answer in (looser, {}, {"website": "", "tagline": None}):  # the same again, nothing, or an empty answer
        raw("UPDATE profiles SET website = ?, tagline = ?", *looser.values())  # as the earlier version kept them
        assert keep(ALPHA, **answer) == (0, 0, 0)
        again = rows("profiles")[0]
        assert (again["website"], again["tagline"], again["specialty"]) == (None, None, "FAKE-SPECIALTY")
        assert library.artist_marks() == {}


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


def test_storing_an_artist_again_adds_and_brings_up_to_date_and_deletes_nothing(tmp_path, monkeypatch):
    """The lead's decision in round 5. A work that is kept and is missing from a later answer stays, with the time
    it was last seen. Two rounds of guards on when a missing work may be dropped each let an outage drop them."""
    clock(monkeypatch)
    for work_id in (ID1, ID2, ID3):
        library.save_image(work_id, "image/jpeg", JPEG)
    keep(ALPHA, [fields(ID1), fields(ID2)], tagline="FAKE-TAGLINE")
    keep(BRAVO, [fields(ID3)])
    before = {row["deviation_id"]: row for row in rows("works")}
    assert keep(ALPHA, [fields(ID2, title="FAKE-TITLE-NEW"), fields(ID4)]) == (0, 0, 0)
    after = {row["deviation_id"]: row for row in rows("works")}
    assert sorted(after) == [ID1, ID2, ID3, ID4]
    assert after[ID1] == before[ID1] and after[ID3] == before[ID3]  # not in the answer: as they were kept
    assert after[ID2]["title"] == "FAKE-TITLE-NEW" and after[ID2]["fetched_at"] > before[ID2]["fetched_at"]
    assert after[ID1]["fetched_at"] < after[ID2]["fetched_at"]  # which is how a reader knows ID1 was missing
    assert [after[work_id]["image_file"] for work_id in (ID1, ID2, ID3, ID4)] == [
        f"{ID1}.jpg",
        f"{ID2}.jpg",
        f"{ID3}.jpg",
        None,
    ]
    assert files(tmp_path) == [*(f"cache/images/{work_id}.jpg" for work_id in (ID1, ID2, ID3)), "cache/pipeline.sqlite"]
    assert keep(ALPHA, []) == (0, 0, 0) and len(rows("works")) == 4 and len(rows("profiles")) == 2  # an empty answer


def test_a_work_kept_under_another_artists_key_is_not_taken_from_them():
    two_artists()
    before = rows("works")
    assert keep(ALPHA, [fields(ID1), fields(ID3)]) == (1, 0, 0)  # ID3 is kept for BRAVO
    assert [(row["artist_key"], row["deviation_id"]) for row in rows("works")] == [
        (ALPHA, ID1),
        (ALPHA, ID2),
        (BRAVO, ID3),
    ]
    assert rows("works")[2] == before[2]


def test_rows_are_written_by_column_name_not_by_position():
    """The first version wrote profiles by position, which is how a changed table came to hold websites as
    fetch times. Here both tables have their columns in another order, and every value still lands in its own."""
    config.LOCAL_DB.parent.mkdir(parents=True)
    raw(f"PRAGMA user_version = {library.SHAPE}")  # a store with works in it is opened only with this mark
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
    assert keep(ALPHA, [fields(published="1700000000")], **profile_fields) == (0, 0, 0)
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
    # the profile, what the answer said beside it (library.answers), and the ten works
    assert len([statement for statement in statements if statement.startswith("INSERT")]) == 12
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
    ("FAKE a hat and a dot", "FAKE a hat and a dot"),
    ("FAKE platemail, ringmail and a female knight", "FAKE platemail, ringmail and a female knight"),
    ("FAKE atlas of dotted lines", "FAKE atlas of dotted lines"),
    ("FAKE at the dotted line", "FAKE at the dotted line"),  # "at" is a word here, "dot" is not
    ("FAKE atlas with a dot", "FAKE atlas with a dot"),  # and here it is the other way round
    ("FAKE concept artist. Illustrator, e.g. of U.S. armour", "FAKE concept artist. Illustrator, e.g. of U.S. armour"),
    ("FAKE signals, lines and messengers", "FAKE signals, lines and messengers"),  # not the services of those names
    ("FAKE " + "FAKEBASE" * 12 + "FAK", "FAKE " + "FAKEBASE" * 12 + "FAK"),  # a run of 99: one short of looking encoded
    ("フェイク" * 40, ("フェイク" * 40)),  # 160 letters with no space: words, not a file
]
EMBEDDINGS = [  # an embedded file, in any spelling
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
    # As written it holds the file. Once its percent-encoding is undone it is one piece of markup, and holds nothing.
    "FAKE <data:image/gif;base64,R0lGODlhAQABAAAAACw=%3E",
]
CONTACTS = [  # the plain cases this stage looks for: refused in a tagline and in a title alike
    # An "@", in every disguise a critic got past an earlier version.
    "FAKE write to zz.fake+tag@mail.invalid now",
    "FAKE write to zz.fake&#64;mail.invalid now",
    "FAKE write to zz.fake&amp;#64;mail.invalid now",
    "FAKE write to zz.fake&amp;amp;commat;mail.invalid now",
    "FAKE write to zz.fake＠mail.invalid now",  # a fullwidth @
    "FAKE write to zz.fake﹫mail.invalid now",  # a small @
    "FAKE write to zz.fake%40mail.invalid now",
    "FAKE write to zz.fake%2540mail.invalid now",
    # An "@" under six layers of entity and percent-encoding, one more than are undone. As written it holds
    # nothing suspect; it is refused because it would not settle.
    "FAKE write to zz_fake&commat" + "&percnt" * 5 + "%3B" + "3B" * 5 + "mailhost now",
    f"FAKE write to zz.fa{ZWSP}ke@ma{SHY}il.invalid now",
    "FAKE write to <b>zz.fake</b>@mail.invalid now",
    "FAKE write to zz.fake@<wbr>mail.invalid now",
    "FAKE write to zz.fake@mäil.invalid now",  # a domain that is not ASCII
    "FAKE write to zz.fake@フェイク.invalid now",
    "FAKE @zzfake",
    "FAKE write to zz_fake%40zzhost now",  # an "@" only once its percent-encoding is undone, and nothing else to see
    # Anything shaped like a domain: an address, a link, a messaging link.
    "FAKE zz_fake at mailhost.invalid",
    "FAKE zz-fake-alpha.art",
    "FAKE see https://zz-fake-alpha.art",
    "FAKE see www.zz-fake-alpha.art",
    "FAKE wa.me/zzfake",  # the critic's two: a title kept them, and the page address made from it carried them
    "FAKE t.me/zzfake",
    *(f"FAKE {name}.me/zzfake" for name in ("m", "line", "vb", "kik", "vk", "zalo", "signal", "WA", "T")),
    "FAKE discord.gg/zzfake",
    "FAKE zzfake%2Eme/zzfake",  # the dot only once its percent-encoding is undone
    "FAKE Mr.Fox and Vol.II",  # shaped like two domains, and neither is one: in doubt it goes
    # A link, whatever it links to: the critic's, which have no host with a dot in them.
    "FAKE skype:zzfake",
    "FAKE SKYPE:zzfake",
    "FAKE tg://resolve?domain=zzfake",
    "FAKE sms:zzfake",
    "FAKE mailto:zzfake",
    "FAKE http://10.0.0.7/zzfake",
    "FAKE skype%3Azzfake",  # the colon only once its percent-encoding is undone
    "FAKE zzfake&colon;//zzfake",
    "FAKE ://zzfake",
    # Nine digits or more: no date and no serial number is that long.
    "FAKE +00 (555) 010-9999 now",
    "FAKE 555 010 999",
    "FAKE ５５５０１０９９９",
    f"FAKE 555{ZWSP}010<b></b>9&#57;9",
    "FAKE 555%30%31%30999",
    "FAKE 10%25 20%25 30%25 off",  # twelve digits as written, six once decoded: it is what is written that is kept
    # Seven or eight digits beside a word for calling, or a plus sign.
    "FAKE call 555 0199 now",
    "FAKE call 9123 4567",  # the critic's two
    "FAKE Tel. 555 0199",
    "FAKE call 5550-1999 now",
    "FAKE call ５５５ ０１９９ now",  # fullwidth digits
    f"FAKE call 555{ZWSP}0{SHY}199 now",
    "FAKE call 555<b></b>0199 now",
    "FAKE call 5&#53;5 0199 now",
    "FAKE call ٥٥٥ ٠١٩٩ now",  # Arabic-Indic digits
    "FAKE CALL 555 0199",
    "FAKE phone 5550199",
    "FAKE WhatsApp 555 0199",
    "FAKE +555 0199",
    "FAKE ＋555 0199",  # a fullwidth plus sign
    "FAKE %2B555 0199",  # a plus sign only once its percent-encoding is undone
    "FAKE &" + "amp;" * 7 + "lt;b&gt;",  # seven layers deep: whatever is under them is not looked for
]
# Kept here on purpose, by the lead's decision in round 4: this stage stopped chasing spellings. The analysis
# stage reads every title and tagline with a language model and flags contact details before anything is published.
LEFT_FOR_ANALYSIS = [
    "FAKE zz_fake ( AT ) zzhost",
    "FAKE zz_fake(a)zzhost",
    "FAKE zzfake_at_zzhost_dot_invalid",
    "FAKE write name at host com",
    "FAKE zzfake arroba zzhost punto com",
    "FAKE telegram zzfake",
    "FAKE Discord zzfake",
    "FAKE my Email is zzfake",
    "FAKE The Messenger and the Signal",
    "FAKE years 2019-2023",  # eight digits and no word for calling: a date
    "FAKE commission 20231105",
    "FAKE piece No. 1234567",  # seven digits: a serial number
    "FAKE call sheet 123456",  # a word for calling, and six digits
    "FAKE Hotel: the lobby",
    "FAKE at the gates",
    "FAKE Sheet (A)",
    "FAKE Chapter: the gates",  # a colon and a space: a subtitle, not a link
    "FAKE 12:30 at night",
    # Seven digits beside a word the short list for calling does not have: kept in a title, dropped from a tagline.
    "FAKE cell 555 0199",
    "FAKE fax 5550199",
    "FAKE text me 555 0199",
]
NOT_TEXTS = ["   ", "<br>", f"{ZWSP}{SHY}{FILLER}", "FAKE word " * 1001, None, 12345, ["FAKE"], b"FAKE"]


@pytest.mark.parametrize(("given", "kept"), KEPT, ids=short)
def test_a_plain_text_is_kept_as_plain_words(given, kept):
    assert profile(tagline=given)["tagline"] == kept
    assert work(title=given)["title"] == kept
    if kept and len(kept) <= 100:
        assert (profile(specialty=given)["specialty"], profile(level=given)["level"]) == (kept, kept)


@pytest.mark.parametrize("given", [*EMBEDDINGS, *CONTACTS, *NOT_TEXTS], ids=short)
def test_a_text_that_may_hold_contact_details_or_a_file_is_dropped_whole(given):
    """Hard rules 1 and 6. Nothing is cut out of a suspect text and the rest kept: the field stays empty."""
    stored = profile(tagline=given, specialty=given, level=given)
    assert (stored["tagline"], stored["specialty"], stored["level"]) == (None, None, None)
    assert work(tags=["fakeone", given, "faketwo"])["tags"] == '["fakeone", "faketwo"]'
    stored = database_bytes()
    assert b"mail" not in stored and b"0199" not in stored and b"base64" not in stored and b"zzfake" not in stored
    assert b"zz_fake" not in stored and b"zz-fake-alpha.art" not in stored


@pytest.mark.parametrize("title", CONTACTS, ids=short)
def test_a_work_whose_title_holds_contact_details_is_not_kept_at_all(title):
    """Hard rule 6, the critic's finding: with only the title dropped, the page address still carried it, because
    DeviantArt makes that address from the title. So the whole work is refused, and nothing of it is stored."""
    with pytest.raises(library.ContactInTitle):
        library.checked_work(**fields(title=title))
    with pytest.raises(ValueError):
        keep(ALPHA, [fields(ID1), fields(ID2, title=title)], tagline="FAKE-TAGLINE")
    assert rows("works") == rows("profiles") == []


@pytest.mark.parametrize("given", LEFT_FOR_ANALYSIS)
def test_what_is_not_a_plain_case_is_kept_and_left_for_the_analysis_stage(given):
    """Round 4, both ways. Seven and eight digits alone are dates and serial numbers, and were being refused. And
    addresses spelled out in words, service names and other languages are no longer chased here at all."""
    assert work(title=given)["title"] == given
    # A line an artist writes about themselves is held to more: seven digits in it are a number to call.
    assert profile(tagline=given)["tagline"] == (given if sum(map(str.isdigit, given)) < 7 else None)


@pytest.mark.parametrize("title", [*(text for text in EMBEDDINGS if text is not CHUNKED), *NOT_TEXTS], ids=short)
def test_a_title_that_could_hold_a_file_is_left_out_and_the_work_is_kept(title):
    """No address is made from such a title that could carry the file: a page's title part is short and plain."""
    assert work(title=title)["title"] is None
    assert b"base64" not in database_bytes()


def test_an_embedded_image_of_any_size_never_reaches_a_text_column():
    """The round-2 database finding, on this store: 200 KB of image in every text field, and none of it is kept."""
    for picture in (EMBEDDED, CHUNKED):
        profile(tagline=picture, specialty=picture, level=picture, website=picture, user_id=picture)
        work(title=EMBEDDED, tags=[picture, "faketag"])
        stored = rows("profiles")[0]
        assert [stored[name] for name in ("tagline", "specialty", "level", "website")] == [None] * 4
        assert (rows("works")[0]["title"], rows("works")[0]["tags"]) == (None, '["faketag"]')
        assert b"FAKEBASE64" not in database_bytes() and b"base64" not in database_bytes()
    assert work(title=CHUNKED)["title"] is None
    assert config.LOCAL_DB.stat().st_size < 120_000  # the empty tables and their indexes are most of that


@pytest.mark.parametrize(
    "hostile",
    ["data:" * 1999, "<" * 9999, "<a" * 4999, "at " * 3333, "a." * 4999, "1." * 4999, "&amp;" * 1999, "%25" * 3333],
    ids=short,
)
def test_checking_a_text_written_to_be_slow_is_still_quick(hostile):
    """A text over 10,000 characters is not read at all, and one just under it takes a few milliseconds."""
    started = time.perf_counter()
    profile(tagline=hostile, specialty=hostile, level=hostile, website=hostile)
    with suppress(library.ContactInTitle):
        work(title=hostile, tags=[hostile] * 100)
    assert time.perf_counter() - started < 5


def test_a_tag_that_is_long_as_it_is_written_is_not_read():
    """A hundred tags of 10,000 characters took seconds to check. A tag is a name: 300 characters as written."""
    assert work(tags=["&#102;" * 50, "&#102;" * 51])["tags"] == json.dumps(["f" * 50])


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
    # A site of the artist's own: its front page, and nothing under it.
    ("https://zz-fake-alpha.art", "https://zz-fake-alpha.art/"),
    ("  ZZ-Fake-Alpha.ART.  ", "https://zz-fake-alpha.art/"),
    ("http://www.zz-fake-alpha.com", "http://www.zz-fake-alpha.com/"),
    ("https://zz-fake-alpha.art/?FAKE-QUERY=1#FAKE-FRAGMENT", "https://zz-fake-alpha.art/"),
    ("http://www.zz-fake-alpha.com/zzfake/", None),
    ("https://zz-fake-alpha.art/zzfake-123456", None),
    ("https://zzfake.zz-fake-alpha.art/", None),
    # A profile on one of the hosts listed: host/<name> or <name>.host/, and nothing after it.
    ("https://www.artstation.com/zzfolio?FAKE-QUERY=1#FAKE-FRAGMENT", "https://www.artstation.com/zzfolio"),
    ("https://zzfolio.artstation.com", "https://zzfolio.artstation.com/"),
    ("instagram.com/zz_folio/", "https://instagram.com/zz_folio/"),
    ("https://www.patreon.com/c/zzfake", None),
    ("https://bsky.app/profile/zzfake.bsky.social", None),
    # The critic's two, and their like: what comes after the name is how one writes to them.
    ("https://www.tumblr.com/zzfake/ask", None),
    ("https://www.artstation.com/zzfake/contact", None),
    ("https://zzfake.tumblr.com/ask", None),
    ("https://zzfake.artstation.com/contact", None),
    ("https://www.artstation.com/zzfolio-123456", "https://www.artstation.com/zzfolio-123456"),  # six digits
    ("https://www.artstation.com/" + "FOLIOBASE" * 10, "https://www.artstation.com/" + "FOLIOBASE" * 10),
    ("https://www.artstation.com/", None),  # the platform itself is nobody's profile
    ("https://www.artstation.com/zzfake/albums/zzfake", None),
    # A way to message a person: every one of these was kept by the list of services to refuse.
    ("https://wa.link/zzfake", None),
    ("https://ig.me/m/zzfake", None),
    ("https://dsc.gg/zzfake", None),
    ("https://discord.me/zzfake", None),
    ("https://telegram.im/zzfake", None),
    ("https://www.facebook.com/messages/t/zzfake", None),
    ("https://www.facebook.com/zzfake", None),
    ("https://wa.me/15550109999", None),
    ("wa.me/zzfake", None),
    ("https://api.whatsapp.com/send", None),
    ("https://t.me/+15550109999", None),
    ("https://T.ME./zzfake", None),
    ("https://discord.gg/zzfake", None),
    ("https://line.me/ti/p/zzfake", None),
    ("https://vgen.co/zzfake", None),  # and anything else that is not on the list
    ("https://zzfake.wixsite.com/folio", None),
    # On a listed host, a path that is a way to write to somebody.
    ("https://www.instagram.com/direct/zzfake", None),
    ("https://x.com/messages/zzfake", None),
    ("https://twitter.com/intent/zzfake", None),
    ("https://www.patreon.com/messages/zzfake", None),
    ("https://www.tumblr.com/message/zzfake", None),
    ("https://www.twitch.tv/zzfake/chat", None),
    # An "@", however it is written.
    ("zz.fake@mail.invalid", None),
    ("mailto:zz.fake@mail.invalid", None),
    ("https://zz.fake@mail.invalid", None),
    ("https://FAKE-USER:FAKE-PASSWORD@zz-fake-alpha.art/", None),
    ("https://www.youtube.com/@zzfake", None),
    ("https://www.artstation.com/zz.fake%40mail.invalid", None),
    ("https://www.artstation.com/zz.fake%2540mail.invalid", None),
    ("https://www.artstation.com/zz.fake&#64;mail.invalid", None),
    ("https://www.artstation.com/zz.fake＠mail.invalid", None),
    ("https://zz-fake-alpha.art/?mail=zz.fake@mail.invalid", None),
    # Seven digits or more, anywhere in it.
    ("https://www.artstation.com/5550199", None),
    ("https://www.artstation.com/555/0199", None),
    ("https://www.artstation.com/555%30199", None),
    ("https://www.artstation.com/５５５０１９９", None),
    ("https://zz-fake-5550199.art/", None),
    # An embedded file.
    ("https://www.artstation.com/data:image/png;base64,RkFLRQ==", None),
    ("https://www.artstation.com/DATA:image/png;base64,RkFLRQ==", None),
    ("https://www.artstation.com/data%3Aimage/png;base64,RkFLRQ==", None),
    ("https://www.artstation.com/%64ata:image/png;base64,RkFLRQ==", None),
    ("https://www.artstation.com/data%253Aimage/png;base64,RkFLRQ==", None),
    ("https://www.artstation.com/" + "FAKEBASE" * 13, None),  # a run of over 100 that looks encoded, with no digit
    (EMBEDDED, None),
    # Not an address.
    ("FAKE words that are not an address", None),
    ("see zz-fake-alpha.art for more", None),
    ("https://www.artstation.com/zzfake and FAKE words after it", None),
    ("https://www.artstation.com/zzfake<br>FAKE words", None),
    ("javascript:alert(1)", None),
    ("ftp://zz-fake-alpha.art/", None),
    ("https://127.0.0.1/", None),
    ("https://localhost/", None),
    ("https://www.artstation.com/" + "a" * 300, None),
    ("", None),
    (None, None),
    (12345, None),
]


@pytest.mark.parametrize(("given", "kept"), WEBSITES, ids=short)
def test_a_website_is_kept_only_when_it_is_the_artists_own_site_or_a_profile_on_a_listed_host(given, kept):
    """Hard rule 6, the lead's change of approach: a list of what is kept, since a list of messaging services to
    refuse can never be finished."""
    assert profile(website=given)["website"] == kept
    stored = database_bytes()
    assert b"mail.invalid" not in stored and b"zzfake" not in stored and b"0199" not in stored
    assert b"data:" not in stored and b"FAKEBASE" not in stored


def test_a_website_is_300_characters_at_most():
    long = "https://www.tumblr.com/" + "fake." * 55 + "fak"
    assert len(long) == 301 and profile(website=long)["website"] is None
    assert profile(website=long[:300])["website"] == long[:300]


def test_the_listed_hosts_are_the_ones_the_lead_named():
    assert sorted(library.PROFILE_DOMAINS) == sorted(
        "artstation.com behance.net instagram.com x.com twitter.com tumblr.com patreon.com "
        "ko-fi.com linktr.ee carrd.co youtube.com twitch.tv bsky.app".split()
    )


@pytest.mark.parametrize("domain", library.PROFILE_DOMAINS)
def test_each_listed_host_keeps_a_profile_and_nothing_that_is_not_one(domain):
    for address in (f"https://{domain}/zzfolio", f"http://www.{domain}/zzfolio/", f"https://zzfolio.{domain}/"):
        assert profile(website=address)["website"] == address
    assert profile(website=f"{domain.upper()}/zzfolio")["website"] == f"https://{domain}/zzfolio"
    for address in (
        f"https://{domain}/",
        f"https://www.{domain}",
        f"https://{domain}/messages/zzfake",
        f"https://{domain}/zzfake/direct",
        f"https://{domain}/zzfake/contact",
        f"https://zzfake.{domain}/ask",
        f"https://{domain}/zzfake/zzfake/zzfake",
        f"https://zz-fake-{domain}/zzfake",  # a name that only ends like it
        f"https://{domain}.zz-fake-evil.invalid/zzfake",
    ):
        assert profile(BRAVO, website=address)["website"] is None, address  # of whom no website is kept
    assert b"zzfake" not in database_bytes()


def test_tags_are_kept_as_a_json_list_of_plain_tag_names():
    given = ["fakeone", "FAKE_Two", "fake-three", "フェイク", "fake four", "zz.fake@mail.invalid"]
    given += [EMBEDDED, "<b>fakefive</b>", "", None, 7, ["fakenested"], "x" * 60, "y" * 61, "555010999", "call5550199"]
    given += ["fake5550199", "platemail", "at_the_gates", "polkadot", "data", "no_ai", "fakename_at_host_com"]
    kept = ["fakeone", "FAKE_Two", "fake-three", "フェイク", "fakefive", "x" * 60]
    kept += ["platemail", "at_the_gates", "polkadot", "data", "no_ai", "fakename_at_host_com"]
    assert json.loads(work(tags=given)["tags"]) == kept
    assert b"zzfake" not in database_bytes()
    assert len(json.loads(work(tags=[f"fake{n}" for n in range(500)])["tags"])) == 60
    # What is no list of tags: a new work has none, and a kept one keeps what it has.
    assert [work(work_id=ID2, tags=tags)["tags"] for tags in (None, "faketag", {"faketag": 1}, 5)] == ["[]"] * 4
    assert [len(json.loads(work(tags=tags)["tags"])) for tags in (None, "faketag", {"faketag": 1}, 5, [])] == [60] * 5


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
        # The critic's probe: an encoded file as the title part. It may be 200 characters and no more.
        "https://www.deviantart.com/zz-fake-alpha/art/" + encoded(GIF),
        "https://www.deviantart.com/zz-fake-alpha/art/" + "F" * 201,
        THUMB,
    ],
    ids=short,
)
def test_a_works_page_is_handle_art_title_on_deviantart_and_nothing_else(address):
    with pytest.raises(ValueError):
        library.page_address(address)
    with pytest.raises(ValueError):
        work(page_url=address)


# The critic's probe for the thumbnail: 1.7 KB of encoded image riding in the token of a signature.
SMUGGLED = encoded(b"GIF89a" + b"FAKE-IMAGE-BYTES" * 80)
NOT_SIGNATURES = [
    "FAKE-QUERY=1",
    "token=FAKE",
    "token=FAKE.FAKE.FAKE=",
    f"{SIGNED}&x=FAKEBASE64",
    f"x=FAKEBASE64&{SIGNED}",
    f"token={HEADER}.{SMUGGLED}.FAKE-SIGNATURE",  # over 1,000 characters
    f"token={HEADER}.FAKE-CLAIMS.{'F' * 990}",
    f"token={SMUGGLED[:96]}.FAKE-CLAIMS.FAKE-SIGNATURE",  # a first part that is no header: not JSON
    "token=FAKE-HEADER.FAKE-CLAIMS.FAKE-SIGNATURE",
    f"token={encoded(['alg', 'HS256'])}.FAKE-CLAIMS.FAKE-SIGNATURE",  # JSON, but no header
    f"token={encoded({'typ': 'JWT'})}.FAKE-CLAIMS.FAKE-SIGNATURE",  # a header that names no algorithm
    f"token={encoded({'alg': 256})}.FAKE-CLAIMS.FAKE-SIGNATURE",
    f"token={encoded({'alg': 'HS256', 'x': 'F' * 80})}.FAKE-CLAIMS.FAKE-SIGNATURE",  # a header of over 100 characters
    f"token={HEADER}..FAKE-SIGNATURE",
    f"token={HEADER}.FAKE-CLAIMS",
]


@pytest.mark.parametrize("query", NOT_SIGNATURES, ids=short)
def test_an_image_address_carries_no_query_but_deviantarts_own_signature(query):
    """A token of 1,000 characters at most, whose first part is a small JSON header naming an algorithm."""
    address = f"https://images-wixmp-zzcdn.wixmp.com/f/FAKE-THUMB-1.jpg?{query}"
    with pytest.raises(ValueError):
        library.image_address(address)
    with pytest.raises(ValueError):
        work(thumb_url=address)
    assert rows("works") == []


def test_a_signature_of_the_real_shape_is_accepted_up_to_the_cap():
    longest = f"token={HEADER}.{'F' * (library.MAX_TOKEN - len(HEADER) - 3)}.F"
    assert len(longest) == len("token=") + library.MAX_TOKEN == 1006
    for query in (SIGNED, longest, f"token={encoded({'alg': 'none'})}.FAKE-CLAIMS.FAKE-SIGNATURE"):
        address = f"https://images-wixmp-zzcdn.wixmp.com/f/FAKE-THUMB-1.jpg?{query}"
        assert library.image_address(address) == address
    with pytest.raises(ValueError):
        library.image_address(f"https://images-wixmp-zzcdn.wixmp.com/f/FAKE-THUMB-1.jpg?{longest}F")


CARRIED = [  # what a page address's title part may not carry, though the title beside it is clean
    "FAKE-zz.fake%40mail.invalid-1001",
    "FAKE-zz-fake-alpha.art-1001",
    "FAKE-call-555-0199-1001",
    "FAKE-5550109999-1001",
    "FAKE-skype%3Azzfake-1001",
    "FAKE-" + "%2525" * 7 + "40-1001",  # under more layers than are undone
]


@pytest.mark.parametrize("slug", CARRIED)
def test_a_page_address_that_carries_contact_details_under_a_clean_title_is_refused(slug):
    """The critic's finding: DeviantArt makes the address from the title, and an answer can give either alone."""
    address = f"https://www.deviantart.com/zz-fake-alpha/art/{slug}"
    with pytest.raises(library.ContactInTitle):
        library.page_address(address)
    with pytest.raises(library.ContactInTitle):
        library.checked_work(**fields(title="FAKE-TITLE-CLEAN", page_url=address))
    with pytest.raises(ValueError):
        keep(ALPHA, [fields(ID1), fields(ID2, page_url=address)])
    assert rows("works") == [] and b"zzfake" not in database_bytes() and b"0199" not in database_bytes()


def test_the_number_deviantart_ends_a_page_address_with_is_not_a_number_to_call():
    # Nor is a title with a colon in it a link: one real address in twenty-four is written like the last of these.
    for slug in (
        "FAKE-TITLE-1234567890",
        "FAKE-call-me-maybe-987654321",
        "FAKE-2019-2023-1234567890",
        "FAKE%3A-the-gates-1001",
    ):
        address = f"https://www.deviantart.com/zz-fake-alpha/art/{slug}"
        assert library.page_address(address) == address


@pytest.mark.parametrize(
    "path",
    [
        "/f/zz.fake%40mail.invalid.jpg",
        "/f/zz.fake@mail.invalid.jpg",
        "/f/skype%3Azzfake.jpg",
        "/f/FAKE/tg%3A%2F%2Fzzfake.jpg",
    ],
)
def test_an_image_address_that_carries_an_address_or_a_link_is_refused(path):
    """Of the title's rule only this much can be asked of an image path, which is all digits and dots."""
    address = f"https://images-wixmp-zzcdn.wixmp.com{path}?{SIGNED}"
    with pytest.raises(library.ContactInTitle):
        library.image_address(address)
    with pytest.raises(ValueError):
        keep(ALPHA, [fields(thumb_url=address)])
    assert rows("works") == []
    plain = (
        f"https://images-wixmp-zzcdn.wixmp.com/f/FAKE-5550109999/v1/fill/w_300,h_400,q_70,strp/fake_1-350t.jpg?{SIGNED}"
    )
    assert library.image_address(plain) == plain


def test_an_image_address_has_a_path_of_600_characters_at_most():
    """The critic's finding: a 1,288-byte PNG rode in as base64 in the path of a 1,761-character thumbnail
    address. Real paths are 252 characters at most. With the cap on the signature, the whole address is short."""
    host = "https://images-wixmp-zzcdn.wixmp.com"
    longest = "/f/" + "F" * (library.MAX_IMAGE_PATH - 7) + ".jpg"
    assert len(longest) == library.MAX_IMAGE_PATH == 600
    for address in (host + longest, f"{host}{longest}?{SIGNED}"):
        assert library.image_address(address) == address
    smuggled = f"{host}/f/{encoded(PNG * 2)[:1718]}.jpg"
    for address in (f"{host}/F{longest[1:]}", f"{host}/F{longest[1:]}?{SIGNED}", smuggled, f"{smuggled}?{SIGNED}"):
        with pytest.raises(ValueError):
            library.image_address(address)
        with pytest.raises(ValueError):
            keep(ALPHA, [fields(thumb_url=address)])
    assert len(smuggled) == 1761 and rows("works") == []


@pytest.mark.parametrize(
    "address",
    [
        f"https://images-wixmp-zzcdn.wixmp.com/f/FAKE-THUMB-1.jpg?{SIGNED}#FAKE-FRAGMENT",
        f"https://images-wixmp-zzcdn.wixmp.com/f/data:image/png;base64,RkFLRQ==?{SIGNED}",
        "https://images-wixmp-zzcdn.wixmp.com/f/" + "a" * 2000,
        PAGE.replace("www.deviantart.com", "zz-fake-evil.invalid"),
    ],
    ids=short,
)
def test_an_image_address_is_on_one_of_deviantarts_image_hosts_with_nothing_hidden_in_it(address):
    with pytest.raises(ValueError):
        library.image_address(address)
    with pytest.raises(ValueError):
        work(thumb_url=address)


def test_the_addresses_deviantart_really_gives_are_accepted():
    longest_title = "https://www.deviantart.com/zz-fake-alpha/art/" + "F" * 200
    assert library.page_address(longest_title) == longest_title
    assert library.page_address(PAGE) == PAGE
    assert library.page_address("HTTPS://WWW.DeviantArt.com/zz-fake-alpha/art/FAKE-1") == (
        "https://www.deviantart.com/zz-fake-alpha/art/FAKE-1"
    )
    odd = (
        "https://www.deviantart.com/zz_fake-Alpha/art/FAKE._Title-%E3%83%95%E3%82%A7-1001"  # a dot, and encoded letters
    )
    assert library.page_address(odd) == odd and work(page_url=odd)["page_url"] == odd
    assert library.image_address(THUMB) == THUMB and work()["thumb_url"] == THUMB
    plain = "https://zzcdn00.deviantart.net/f/FAKE,w_300,q_70,strp/FAKE-1.jpg"  # no signature at all
    assert library.image_address(plain) == plain


def test_a_thumbnail_is_one_only_in_the_shape_deviantart_makes_them():
    """Round 7: works were stored with thumbnail paths such as /f/abc/whatsapp-5550199123. These are the shapes
    of all 4,381 thumbnails kept on 2026-10-13, and the collect stage takes an entry with any other as having none."""
    uuid = "0a1b2c3d-4e5f-6071-8293-a4b5c6d7e8f9"
    file = f"https://images-wixmp-zzcdn.wixmp.com/f/{uuid}/dfake01-{uuid}"
    cut = "v1/fit/w_414,h_300,q_70,strp"
    for good in (
        f"{file}.jpg/{cut}/fake_title_1_by_zz_fake_dfake01-414w.jpg?{SIGNED}",
        f"{file}.png/v1/fit/w_414,h_300/fake_title_1_by_zz_fake_dfake01-414w.png?{SIGNED}",
        f"{file}.gif/{cut},blur_20/_fake__title_by_zzfake_dfake01-414w.jpg?{SIGNED}",
        f"{file.replace('/f/', '/i/')}.jpg/{cut}/fake_title_1_by_zz_fake_dfake01-414w.jpg",  # not signed
    ):
        assert library.is_thumbnail(good) and library.image_address(good) == good
    assert library.is_thumbnail(f"{file}.jpg?{SIGNED}")  # the file as it is, not cut to a size: one of the 4,381
    for other in (
        "https://images-wixmp-zzcdn.wixmp.com/f/abc/whatsapp-5550199123",  # the critic's two
        "https://images-wixmp-zzcdn.wixmp.com/f/call-5550199123/t_by_zz_d1.jpg",
        THUMB,  # an image address the library takes, and no thumbnail of DeviantArt's
        f"{file}.jpg/whatsapp-5550199123",
        f"{file}.jpg/{cut}/call-5550199123/fake_by_zz_dfake01-414w.jpg",
        f"{file}.jpg/{cut}/call-555-0199-414w.jpg",
        f"{file}.jpg/{cut}/Fake Title_by_zz_dfake01-414w.jpg",
        f"{file}.jpg/{cut}/fake%40mail_by_zz_dfake01-414w.jpg",
        f"{file}.jpg/{cut}/fake_by_zz_dfake01-414w.jpg/",
        f"{file}.jpg\n",
        f"{file.replace('wixmp.com', 'deviantart.net')}.jpg",
        f"{file.replace('images-wixmp-zzcdn.', '')}.jpg",
        f"{file.replace('wixmp.com', 'wixmp.com.zz-fake-evil.invalid')}.jpg",
        f"{file.replace('https:', 'http:')}.jpg",
        f"{file.upper()}.jpg",
        "",
        None,
        5550199123,
        [f"{file}.jpg"],
    ):
        assert not library.is_thumbnail(other)


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
    """Everything else goes. The stuck work keeps its row, so the artist is still listed and the purge can be rerun,
    and so does the profile, whose account hash is what names the artist for that purge (a removal bars the
    account, and no handle while the account is known)."""
    two_artists()
    with monkeypatch.context() as patch:
        patch.setattr("pathlib.Path.unlink", locked(f"{ID1}.jpg"))
        with pytest.raises(OSError):
            library.purge(ALPHA)
    assert [(row["artist_key"], row["deviation_id"]) for row in rows("works")] == [(ALPHA, ID1), (BRAVO, ID3)]
    assert [row["artist_key"] for row in rows("profiles")] == [ALPHA, BRAVO]
    assert library.account_hash(ALPHA) is not None
    assert files(tmp_path) == [f"cache/images/{ID1}.jpg", f"cache/images/{ID3}.jpg", "cache/pipeline.sqlite"]
    assert library.artist_keys() == [ALPHA, BRAVO]
    library.purge(ALPHA)
    assert library.artist_keys() == [BRAVO] and files(tmp_path) == [f"cache/images/{ID3}.jpg", "cache/pipeline.sqlite"]


def test_an_account_under_a_new_handle_is_linked_to_the_keys_its_caller_names_and_nothing_is_deleted(tmp_path):
    """The same user id under a new handle is written down as a mark on the old key and a link to the new one, so
    that a removal of either finds both. Round 6: only for the keys the caller names. An id the API answered with
    is not enough: one wrong id once tied two artists together, and a removal of one took the other."""
    renamed = "da:zz-fake-renamed"
    two_artists()
    library.save_image(ID4, "image/jpeg", JPEG)
    page = "https://www.deviantart.com/zz-fake-renamed/art/FAKE"
    works = [fields(ID2, page_url=page), fields(ID4, page_url=page)]
    same_id = {"tagline": "FAKE-TAGLINE-RENAMED", "user_id": "FAKE-USERID-zz-fake-alpha"}
    assert keep(renamed, works, **same_id) == (1, 0, 0)  # not linked: ID2 is ALPHA's, and is not taken from them
    assert library.artist_marks() == {} and library.same_account(renamed) == library.same_account(ALPHA) == []
    assert library.holders("FAKE-USERID-zz-fake-alpha") == [ALPHA, renamed] and library.holders(None) == []
    assert keep(renamed, works, old_keys=[ALPHA], **same_id) == (0, 0, 0)
    assert [row["artist_key"] for row in rows("profiles")] == [ALPHA, BRAVO, renamed]
    assert [(row["artist_key"], row["deviation_id"], row["image_file"]) for row in rows("works")] == [
        (ALPHA, ID1, f"{ID1}.jpg"),
        (renamed, ID2, f"{ID2}.jpg"),  # the work is the account's under either handle: it is brought up to date
        (BRAVO, ID3, f"{ID3}.jpg"),
        (renamed, ID4, f"{ID4}.jpg"),
    ]
    assert len(files(tmp_path)) == 5
    marks = library.artist_marks()
    assert marks.keys() == {ALPHA} and marks[ALPHA][0] == library.RENAMED == "renamed" and marks[ALPHA][2] == renamed
    assert library.same_account(ALPHA) == [renamed] and library.same_account(renamed) == [ALPHA]
    assert library.same_account(BRAVO) == [] and library.same_account("da:zz-fake-nobody") == []
    library.mark_artist(ALPHA, "account gone")  # of the old handle the API says again: no such user. The link stays.
    assert library.artist_marks()[ALPHA][0] == library.RENAMED and library.same_account(renamed) == [ALPHA]
    # Nor is the link dropped, or the hash that is kept replaced, when the old key is stored again under another id.
    keep(ALPHA, tagline="FAKE-TAGLINE-NEWCOMER", user_id="FAKE-USERID-SOMEBODY-NEW")
    assert library.same_account(renamed) == [ALPHA] and library.account_hash(ALPHA) == account(
        "FAKE-USERID-zz-fake-alpha"
    )
    with pytest.raises(ValueError):
        keep(renamed, old_keys=["zz-fake-alpha"])
    library.purge(renamed)
    assert library.artist_marks() == {} and library.same_account(ALPHA) == []  # a mark that names a purged key goes


def test_the_user_id_is_kept_only_as_a_hash_and_only_a_real_one_links_two_keys():
    keep(ALPHA, [fields(ID1)], user_id="FAKE-USERID-1")
    keep(BRAVO, [fields(ID3)], user_id="FAKE-USERID-2")
    assert [row["user_hash"] for row in rows("profiles")] == [account("FAKE-USERID-1"), account("FAKE-USERID-2")]
    assert b"FAKE-USERID" not in database_bytes()
    for missing in (None, "", 7, ["FAKE-USERID-1"], {"FAKE": 1}):  # no id says nothing: the hash that is kept stays
        keep(ALPHA, [fields(ID1)], user_id=missing)
        keep(BRAVO, [fields(ID3)], user_id=missing)
        assert [row["user_hash"] for row in rows("profiles")][:2] == [
            account("FAKE-USERID-1"),
            account("FAKE-USERID-2"),
        ]
        keep("da:zz-fake-charlie", user_id=missing)  # and two artists who never had one are not thereby one account
        keep("da:zz-fake-delta", user_id=missing)
        assert [row["user_hash"] for row in rows("profiles")][2:] == [None, None] and len(rows("works")) == 2
        assert library.artist_marks() == {} and library.same_account("da:zz-fake-charlie") == []


def account(user_id, spelling="uid"):
    """The hash an account is known by here and on the store's removal list: sha256 of 'uid:<the API's user id>'."""
    return hashlib.sha256(f"{spelling}:{user_id}".encode()).hexdigest()


def test_a_profile_from_before_the_hash_was_respelled_is_still_known_for_the_account_it_is():
    """The second version hashed 'da-user:<id>'. A row it wrote is found for the account all the same, and keeps
    the hash it has."""
    two_artists()
    old = account("FAKE-USERID-zz-fake-alpha", "da-user")
    raw("UPDATE profiles SET user_hash = ? WHERE artist_key = ?", old, ALPHA)
    assert library.holders("FAKE-USERID-zz-fake-alpha") == [ALPHA] and library.holders("FAKE-USERID-zz-fake-bravo") == [
        BRAVO
    ]
    keep(ALPHA, user_id="FAKE-USERID-zz-fake-alpha")
    assert library.account_hash(ALPHA) == old and len(rows("works")) == 3


def test_the_size_of_a_works_thumbnail_is_kept_when_both_numbers_are_given_and_stays_when_they_are_not():
    """For the site, which cropped every work to one shape because nothing said what shape it has."""
    keep(ALPHA, [fields(ID1, thumb_size=(300, 200)), fields(ID2, thumb_size=[640, 480]), fields(ID3)])
    assert library.thumb_sizes() == {ID1: (300, 200), ID2: (640, 480)}
    for nothing in (
        None,
        (300,),
        (300, None),
        ("300", "200"),
        (300.0, 200.0),
        (True, True),
        (300, True),
        (True, 200),
        (0, 200),
        (-1, 5),
        "FAKE",
    ):
        keep(ALPHA, [fields(ID1, thumb_size=nothing), fields(ID3, thumb_size=nothing)])
        assert library.thumb_sizes() == {ID1: (300, 200), ID2: (640, 480)}  # both or neither, and what is kept stays
    keep(ALPHA, [fields(ID1, thumb_size=(150, 100))])
    assert library.thumb_sizes()[ID1] == (150, 100)
    library.purge(ALPHA)
    assert library.thumb_sizes() == {} and rows("work_sizes") == []


def test_what_an_entry_does_not_give_for_a_kept_work_stays_as_it_is_kept():
    """Not given, or given empty: round 7's finding was an empty title and an empty tag list blanking what was kept."""
    work(title="FAKE-TITLE-KEPT", published="1700000000")
    kept = rows("works")[0]
    for title, tags in ((None, None), (7, "faketag"), (["FAKE"], {"FAKE": 1}), ("", []), (" ", []), (ZWSP, [])):
        keep(ALPHA, [fields(title=title, tags=tags, thumb_url=None, published=None)])
        assert {**rows("works")[0], "fetched_at": None} == {**kept, "fetched_at": None}
    keep(ALPHA, [fields(title="", published="FAKE", tags=["fakenew"])])
    assert {**rows("works")[0], "fetched_at": None} == {**kept, "fetched_at": None, "tags": '["fakenew"]'}
    # Round 8. A title that is a 100-letter run blanked the kept one, and so did tags these rules refuse. What is
    # kept stays, and the work is marked for it.
    kept = rows("works")[0]
    for refused in ({"title": EMBEDDED}, {"title": "F" * 100}, {"tags": ["not a tag name", 7]}):
        assert keep(ALPHA, [fields(**{"title": "", "tags": [], "published": None, **refused})]) == (0, 1, 0)
        assert {**rows("works")[0], "fetched_at": None} == {**kept, "fetched_at": None}
        assert {work_id: mark[0] for work_id, mark in library.work_marks().items()} == {ID1: library.REFUSED}
    assert keep(ALPHA, [fields(title="FAKE-TITLE-NEWER", tags=["fakenewer", "not a tag name"])]) == (0, 0, 0)
    assert (rows("works")[0]["title"], rows("works")[0]["tags"]) == ("FAKE-TITLE-NEWER", '["fakenewer"]')
    assert library.work_marks() == {}  # an answer that can be kept is taken, and the mark goes
    assert keep(ALPHA, [fields(ID2, title=EMBEDDED, tags=["not a tag name"])]) == (0, 0, 0)  # a new work: no mark
    assert (rows("works")[1]["title"], rows("works")[1]["tags"]) == (None, "[]")
    with pytest.raises(ValueError):  # and a work that is not kept is not taken without a thumbnail
        keep(BRAVO, [fields(ID3, thumb_url=None)])
    assert len(rows("works")) == 2 and len(rows("profiles")) == 1


def test_rows_about_nobody_who_is_kept_are_swept_from_every_table_and_nobody_else_loses_a_row():
    """The critic's finding: a row another stage writes about a removed artist or their work after the purge
    stayed forever. An artist who is only not collected yet, or collected and not listed, loses nothing."""
    other_stages()  # ALPHA and BRAVO are kept, with rows in tables of other stages
    listed, removed, nobody = "da:zz-fake-listed", "da:zz-fake-removed", "da:zz-fake-nobody"
    for handle in ("zz-fake-listed", "zz-fake-removed"):
        assert store.upsert_artist(handle, f"https://www.deviantart.com/{handle}", [], 3)
    store.mark_removed("zz-fake-removed")
    with closing(sqlite3.connect(config.LOCAL_DB)) as conn, conn:
        for key in (listed, removed, nobody):
            conn.execute("INSERT INTO fake_summaries VALUES (?, 'FAKE-SUMMARY')", (key,))
            conn.execute("INSERT INTO fake_both VALUES (?, ?, 'FAKE-NOTE')", (key, ID1))
        conn.execute("INSERT INTO fake_tags VALUES (?, 'fake-model-a', 'FAKE-RESULT')", (ID4,))  # no such work
    for key in (listed, removed, nobody):
        library.note_skip(key)
    library.mark_works(ALPHA, {ID1: "mature"})
    # Kept and not listed, by a profile alone or by a work alone: theirs to keep until a removal purges them.
    alone = ["da:zz-fake-profile-only", "da:zz-fake-work-only"]
    keep(alone[0], tagline="FAKE-TAGLINE")
    keep(alone[1], [fields("00000000-0000-4000-A000-000000000005")])
    raw("DELETE FROM profiles WHERE artist_key = ?", alone[1])
    for key in alone:
        raw("INSERT INTO fake_summaries VALUES (?, 'FAKE-SUMMARY')", key)
    before = (rows("profiles"), rows("works"), rows("fake_unrelated"), rows("work_marks"))
    library.drop_orphan_rows()
    assert left("fake_summaries", "artist_key") == [ALPHA, BRAVO, listed, *alone]
    assert left("fake_both", "artist_key") == [ALPHA, BRAVO, listed] and left("fake_tags", "deviation_id") == [
        ID1,
        ID2,
        ID3,
    ]
    assert [row["artist_key"] for row in rows("skipped")] == [listed]
    assert (rows("profiles"), rows("works"), rows("fake_unrelated"), rows("work_marks")) == before
    assert b"zz-fake-removed" not in database_bytes() and b"zz-fake-nobody" not in database_bytes()
    raw("INSERT INTO fake_summaries VALUES (?, 'FAKE-SUMMARY')", nobody)  # and a purge sweeps as well
    library.purge(BRAVO)
    assert left("fake_summaries", "artist_key") == [ALPHA, listed, *alone]


@pytest.mark.parametrize(
    "name",
    [
        "call_5550199000_by_zzfake_d1a2b3c-350t.jpg",
        "whatsapp_5550199_by_zzfake_d1a2b3c-350t.jpg",
        "visit_zz-fake-alpha.art_by_zzfake_d1a2b3c-350t.jpg",
        "FAKE-call-555-0199.jpg",
    ],
)
def test_an_image_address_whose_last_part_carries_what_a_title_may_not_is_refused(name):
    """The critic's finding: under a clean title, a thumbnail path ending like these was stored. The last part of
    the path is "<title>_by_<artist>_<id>-<size>.jpg": the title in it is held to the title's rule."""
    address = f"https://images-wixmp-zzcdn.wixmp.com/f/FAKE/v1/fill/w_300,h_400,q_70,strp/{name}?{SIGNED}"
    with pytest.raises(library.ContactInTitle):
        library.image_address(address)
    with pytest.raises(ValueError):
        keep(ALPHA, [fields(thumb_url=address)])
    assert rows("works") == []
    for fine in (
        "fake_title_2019_2023_by_zzfake_d1a2b3c4d5e6-350t.jpg",
        "fake_call_of_the_wild_by_zzfake_d9876543-414w-2x.jpg",
    ):
        ordinary = f"https://images-wixmp-zzcdn.wixmp.com/f/FAKE/v1/fill/w_300,h_400,q_70,strp/{fine}?{SIGNED}"
        assert (
            library.image_address(ordinary) == ordinary
        )  # the artist's handle and DeviantArt's own numbers are not it


def test_a_file_as_deviantart_names_it_is_not_read_as_a_number_to_call():
    """The first rerun on real data marked 177 kept works for this. A preview served as the file itself ends in
    <id>-<uuid>.<type>, a name DeviantArt made, and a UUID has more than nine digits in it."""
    uuid = "12345678-9012-4345-8678-901234567890"

    def address(name):
        return f"https://images-wixmp-zzcdn.wixmp.com/f/{uuid}/{name}?{SIGNED}"

    for name in (f"d1a2b3c-{uuid}.png", f"dfakeid-{uuid}.jpg", f"d999999-{uuid}.gif"):
        assert library.image_address(address(name)) == address(name)
    for name in (
        f"5550199000-{uuid}.png",  # an id that is itself a number
        f"call-5550199-{uuid}.png",
        f"d1a2b3c-{uuid}-5550199000.png",
        f"d1a2b3c-{uuid.replace('9', 'A')}.png",  # not the shape DeviantArt makes: read as any other name
        f"d1a2b3c-{uuid}.png.5550199000",
    ):
        with pytest.raises(library.ContactInTitle):
            library.image_address(address(name))


def test_a_removed_account_is_known_by_its_hash_on_the_stores_removal_list():
    """Hard rule 7 across a rename that comes after the removal. The purge deletes the profile and the hash with
    it, so the collector first hands the hash to the store's removal list; from then on the account is known
    there under any handle. Nothing but the hash is kept: it names nobody."""
    two_artists()
    assert library.account_hash(ALPHA) == account("FAKE-USERID-zz-fake-alpha")
    assert library.account_hash("da:zz-fake-nobody") is None
    assert library.account_removed("FAKE-USERID-zz-fake-alpha") is False
    store.import_removed([library.account_hash(ALPHA)])
    library.purge(ALPHA)
    assert library.account_removed("FAKE-USERID-zz-fake-alpha") is True
    assert library.account_removed("FAKE-USERID-zz-fake-bravo") is False
    assert [library.account_removed(missing) for missing in (None, "", 7, ["FAKE-USERID-zz-fake-alpha"])] == [False] * 4
    assert b"FAKE-USERID" not in database_bytes() and b"zz-fake-alpha" not in database_bytes()
    store.import_removed([account("FAKE-USERID-OLD", "da-user")])  # an account removed while the second version ran
    assert library.account_removed("FAKE-USERID-OLD") is True


def test_only_a_hash_is_ever_handed_to_the_removal_list():
    """A table an earlier version left can hold something else where the hash belongs. That is not passed on."""
    earlier_database(AFTER_THE_INCIDENT)
    raw(BY_POSITION, *first_version_row(ALPHA, "https://zz-fake-alpha.art/"))  # a time lands in the hash column
    assert raw("SELECT user_hash FROM profiles") == [(WHEN,)] and library.account_hash(ALPHA) is None
    raw("DROP TABLE profiles")
    raw(f"CREATE TABLE profiles ({FIRST_VERSION}) STRICT")  # and the first version's table had no such column
    raw(BY_POSITION, *first_version_row(ALPHA, None))
    assert library.account_hash(ALPHA) is None


def clock(monkeypatch):
    """Make the library's clock tick a second each time it is read, from midnight. Returns what second n reads as."""
    seconds = iter(range(3600))
    monkeypatch.setattr(store, "_now", lambda: at(next(seconds)))
    return at


def at(second):
    return f"2026-10-12T00:{second // 60:02d}:{second % 60:02d}+00:00"


def test_a_skip_is_noted_with_its_time_and_nothing_else(monkeypatch):
    """It puts the artist at the back of the next run. Nothing is ever decided on how often they were passed over."""
    clock(monkeypatch)
    for key in (ALPHA, BRAVO, BRAVO):
        assert library.note_skip(key) is None
    assert library.last_attempts() == {ALPHA: at(0), BRAVO: at(2)}
    assert rows("skipped") == [
        {"artist_key": ALPHA, "skipped_at": at(0), "unavailable": 0},
        {"artist_key": BRAVO, "skipped_at": at(2), "unavailable": 0},
    ]
    assert library.artist_keys() == []  # a skip is not something kept about an artist's work
    for key in ("zz-fake-alpha", "da:ZZ-Fake-Alpha", "*", None):
        with pytest.raises(ValueError):
            library.note_skip(key)


def test_storing_an_artist_ends_their_skip_and_the_last_attempt_is_then_when_they_were_stored(monkeypatch):
    clock(monkeypatch)
    library.note_skip(ALPHA)  # second 0
    library.note_skip(BRAVO)  # second 1
    keep(ALPHA, [fields()], tagline="FAKE-TAGLINE")  # the profile at second 2, the work at second 3
    assert [row["artist_key"] for row in rows("skipped")] == [BRAVO]
    assert library.last_attempts() == {ALPHA: at(2), BRAVO: at(1)}
    library.note_skip(ALPHA)
    assert library.last_attempts()[ALPHA] == at(4)  # whichever is later: stored, or passed over


def test_a_purge_forgets_the_skip_and_the_marks_too():
    """Hard rule 7: nothing stays, the notes that name them included."""
    two_artists()
    for key in (ALPHA, BRAVO):
        library.note_skip(key)
        library.mark_artist(key, "account gone")
    library.mark_works(ALPHA, {ID1: "mature"})
    library.mark_works(BRAVO, {ID3: "mature"})
    library.purge(ALPHA)
    assert library.artist_keys() == [BRAVO] and [row["artist_key"] for row in rows("skipped")] == [BRAVO]
    assert library.artist_marks().keys() == {BRAVO} and library.work_marks().keys() == {ID3}
    library.purge(BRAVO)
    assert rows("skipped") == rows("profiles") == rows("works") == rows("work_marks") == rows("artist_marks") == []
    assert b"zz-fake-alpha" not in database_bytes() and b"zz-fake-bravo" not in database_bytes()


def test_a_kept_work_that_no_longer_qualifies_is_marked_and_stays(monkeypatch):
    """Round 5: what the API says against a kept work is written beside it, with the time. That is all."""
    clock(monkeypatch)
    two_artists()
    before = rows("works")
    assert library.mark_works(ALPHA, {ID1: "mature", ID3: "mature", ID4: "mature"}) == 1  # BRAVO's, and nobody's
    marks = library.work_marks()
    assert marks.keys() == {ID1} and marks[ID1][0] == "mature" and marks[ID1][1].endswith("+00:00")
    assert rows("works") == before and len(rows("profiles")) == 2  # nothing about the work itself changes
    assert keep(ALPHA, [fields(ID2)], marks={ID1: "tagged AI-made", ID2: "mature"}) == (0, 1, 0)
    assert library.work_marks().keys() == {ID1} and library.work_marks()[ID1][0] == "tagged AI-made"  # ID2 was stored
    assert library.mark_works(ALPHA, {ID2: "mature"}) == 1 and library.work_marks().keys() == {ID1, ID2}
    seen = rows("works")[1]["fetched_at"]
    assert library.mark_works(ALPHA, {ID2: None, ID3: None}) == 0  # seen again, and it qualifies
    assert library.work_marks().keys() == {ID1} and rows("works")[1]["fetched_at"] > seen
    assert rows("works")[2] == before[2]  # not BRAVO's work: that was not ALPHA's turn to say
    keep(ALPHA, [fields(ID1)])
    assert library.work_marks() == {}  # stored again: the mark goes
    for reason in ("", "x" * 41, "FAKE <b>", "zz.fake@mail.invalid", 7):
        with pytest.raises(ValueError):
            library.mark_works(ALPHA, {ID1: reason})
    for key in ("zz-fake-alpha", "*", None):
        with pytest.raises(ValueError):
            library.mark_works(key, {ID1: "mature"})


def test_an_artist_is_marked_with_a_reason_and_a_time_and_storing_them_takes_the_mark_off():
    two_artists()
    library.mark_artist(ALPHA, "account gone")
    library.mark_artist("da:zz-fake-nobody", "mostly AI-made")  # an artist of whom nothing is kept can be marked too
    marks = library.artist_marks()
    assert {key: mark[0] for key, mark in marks.items()} == {
        ALPHA: "account gone",
        "da:zz-fake-nobody": "mostly AI-made",
    }
    assert all(mark[1].endswith("+00:00") and mark[2] is None for mark in marks.values())
    assert len(rows("profiles")) == 2 and len(rows("works")) == 3  # and that is all that happens
    library.mark_artist(ALPHA, "mostly AI-made")
    assert library.artist_marks()[ALPHA][0] == "mostly AI-made"
    keep(ALPHA, tagline="FAKE-TAGLINE")
    assert library.artist_marks().keys() == {"da:zz-fake-nobody"}
    for reason in ("", "FAKE <b>", 7):
        with pytest.raises(ValueError):
            library.mark_artist(ALPHA, reason)
    with pytest.raises(ValueError):
        library.mark_artist("zz-fake-alpha", "account gone")


def test_purge_deletes_every_contact_sheet_too(tmp_path, monkeypatch):
    """The critic's finding: other stages lay cached images out on sheets under cache/review, and a removed
    artist's work stayed on them. No row says which sheet shows whom, so a purge deletes all of them."""
    two_artists()
    review = config.CACHE_DIR / "review"
    (review / "FAKE-RUN").mkdir(parents=True)
    (review / "FAKE-SHEET-1.png").write_bytes(PNG)
    (review / "FAKE-RUN" / "FAKE-SHEET-2.jpg").write_bytes(JPEG)
    assert library.review_dir() == review
    library.purge(ALPHA)
    assert files(tmp_path) == [f"cache/images/{ID3}.jpg", "cache/pipeline.sqlite"] and not review.exists()
    library.purge(ALPHA)  # safe to repeat, with no sheets there
    (review / "FAKE-RUN").mkdir(parents=True)
    (review / "FAKE-RUN" / "FAKE-SHEET-3.png").write_bytes(PNG)
    with monkeypatch.context() as patch:  # a sheet that cannot be deleted is a purge that is not done
        patch.setattr("shutil.rmtree", lambda *args, **kwargs: None)
        with pytest.raises(OSError):
            library.purge(BRAVO)
    assert library.artist_keys() == []  # the rows went all the same
    library.purge(BRAVO)
    assert not review.exists()


OTHER_STAGES = """
CREATE TABLE fake_tags (deviation_id TEXT, model TEXT, result TEXT, PRIMARY KEY (deviation_id, model));
CREATE TABLE "fake vectors" (Deviation_ID TEXT PRIMARY KEY, vector BLOB) STRICT;
CREATE TABLE fake_summaries (artist_key TEXT PRIMARY KEY, summary TEXT);
CREATE TABLE fake_both (artist_key TEXT, deviation_id TEXT, note TEXT);
CREATE TABLE fake_unrelated (id TEXT PRIMARY KEY, note TEXT);
CREATE TABLE "fake ""quoted"" one" (deviation_id TEXT, artist_key TEXT);
"""


def other_stages():
    """Tables this module has never heard of, as the analysis stages keep them: rows by work and rows by artist."""
    two_artists()
    with closing(sqlite3.connect(config.LOCAL_DB)) as conn, conn:
        conn.executescript(OTHER_STAGES)
        for work_id, key in ((ID1, ALPHA), (ID2, ALPHA), (ID3, BRAVO)):
            conn.execute("INSERT INTO fake_tags VALUES (?, 'fake-model-a', 'FAKE-RESULT')", (work_id,))
            conn.execute("INSERT INTO fake_tags VALUES (?, 'fake-model-b', 'FAKE-RESULT')", (work_id,))
            conn.execute('INSERT INTO "fake vectors" VALUES (?, ?)', (work_id, b"FAKE-VECTOR"))
            conn.execute("INSERT INTO fake_both VALUES (?, ?, 'FAKE-NOTE')", (key, work_id))
            conn.execute('INSERT INTO "fake ""quoted"" one" VALUES (?, ?)', (work_id, key))
            conn.execute("INSERT INTO fake_unrelated VALUES (?, 'FAKE-NOTE')", (f"FAKE-ROW-{work_id[-1]}",))
        conn.execute("INSERT INTO fake_summaries VALUES (?, 'FAKE-SUMMARY'), (?, 'FAKE-SUMMARY')", (ALPHA, BRAVO))


def left(table, column):
    return sorted({row[column] for row in rows(f'"{table}"')})


def test_purge_deletes_the_artists_rows_from_every_table_whichever_stage_made_it():
    """The critic's finding: a purge left all ten rows of a removed artist's works in the tagging stage's and the
    style stage's tables until those stages ran again. There is no list of tables here to forget to add one to:
    whatever table has a deviation_id or an artist_key column loses the purged works' and the artist's rows."""
    other_stages()
    library.purge(ALPHA)
    assert left("fake_tags", "deviation_id") == left("fake vectors", "Deviation_ID") == [ID3]
    assert left("fake_both", "deviation_id") == [ID3] and left("fake_summaries", "artist_key") == [BRAVO]
    assert left('fake ""quoted"" one', "deviation_id") == [ID3]  # whatever the table is called
    assert len(rows("fake_tags")) == 2 and len(rows("fake_unrelated")) == 3  # BRAVO's two; and a table about nobody
    assert b"zz-fake-alpha" not in database_bytes() and ID1.encode() not in database_bytes()


def test_a_work_whose_file_is_stuck_keeps_its_rows_everywhere_until_the_purge_is_run_again(monkeypatch):
    other_stages()
    with monkeypatch.context() as patch:
        patch.setattr("pathlib.Path.unlink", locked(f"{ID1}.jpg"))
        with pytest.raises(OSError):
            library.purge(ALPHA)
    assert left("fake_tags", "deviation_id") == [ID1, ID3] and left("fake_summaries", "artist_key") == [BRAVO]
    library.purge(ALPHA)
    assert left("fake_tags", "deviation_id") == left("fake_both", "deviation_id") == [ID3]


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


def test_holds_contact_is_the_plain_rule_a_title_is_held_to_for_any_text_a_later_stage_sends():
    assert all(library.holds_contact(text) for text in CONTACTS)
    assert not any(library.holds_contact(text) for text in [*LEFT_FOR_ANALYSIS, *(given for given, _ in KEPT)])
    assert library.holds_contact("FAKE word " * 1001)  # too long to read, so not passed
    assert not any(library.holds_contact(value) for value in (None, 12345, ["FAKE"]))  # no text, nothing held


@pytest.mark.parametrize(
    ("text", "names"),
    [
        # the second critic of the push stage: what a model wrote reading the text in an image
        ("Text in the corner reads Discord fakepainter#1234.", True),
        ("A caption that says telegram: fakepainter.", True),
        ("The banner says add me on Snapchat fakepainter.", True),
        # letters set apart, and what the plain rule undoes: entities, percent-encoding, fullwidth, invisible marks
        ("Signed T E L E G R A M fakepainter.", True),
        ("Signed d.i.s.c.o.r.d fakepainter.", True),
        ("Support me on Ko-fi, or ko fi, or KOFI.", True),
        ("On a shield: &#68;iscord fakepainter.", True),
        ("On a shield: %57hatsApp fakepainter.", True),
        ("On a banner: Ｉｎｓｔａｇｒａｍ fakepainter.", True),
        ("On a banner: Pat​reon fakepainter.", True),
        ("Write by e-mail or by email.", True),
        ("A tip jar: PayPal, Venmo, Cash App.", True),
        ("FAKE word " * 1001, True),  # too long to read, so not passed
        # words a picture is described in, which only look like one
        ("Discordant colours, a knight in plate mail and a female figure.", False),
        ("A clear blue sky over an X-wing; a line of soldiers; a signal fire.", False),
        ("A twitching tail, a kick, and a youthful face.", False),
        (None, False),
    ],
)
def test_names_service_finds_a_messaging_social_or_payment_service_however_it_is_spelled(text, names):
    assert library.names_service(text) is names


# --- a reading is tied to what was read: which image a work has, and the store's shape ---

FILE = "d00000a-00000000-0000-4000-a000-00000000000a.jpg"  # a file as DeviantArt names it: <id>-<uuid>.<type>
OTHER_FILE = "d00000b-00000000-0000-4000-a000-00000000000b.jpg"  # what the artist puts in its place
SIGNED_AGAIN = SIGNED.replace("CLAIMS-1", "CLAIMS-2")


def shown(file=FILE, size=300, signed=SIGNED, host="images-wixmp-zzcdn", folder="f"):
    """A thumbnail's address as DeviantArt makes it: the file, how it is cut, a name made of the title, a signature."""
    cut = f"/v1/fit/w_{size},h_{size},q_70,strp/fake_title_by_zz_fake_d00000a-{size}w.jpg"
    return f"https://{host}.wixmp.com/{folder}/00000000-0000-4000-a000-0000000000ff/{file}{cut}?{signed}"


def test_own_file_is_the_file_an_address_shows_whatever_its_size_signature_or_host():
    same = [shown(), shown(size=150), shown(signed=SIGNED_AGAIN), shown(host="images-wixmp-yy"), shown(folder="i")]
    same += [shown().partition("/v1/")[0], shown().partition("?")[0]]
    assert {library.own_file(address) for address in same} == {FILE}
    assert all(library.is_thumbnail(address) for address in same)
    assert library.own_file(shown(OTHER_FILE)) == OTHER_FILE
    # An address of no such shape is known by its path up to the cut, and never by its signature.
    assert library.own_file(THUMB) == library.own_file(THUMB.partition("?")[0]) == "/f/FAKE-THUMB-1.jpg"
    assert library.own_file(None) is None and library.own_file(7) is None


def test_fingerprint_is_the_sha256_of_what_was_given():
    assert library.fingerprint(JPEG) == hashlib.sha256(JPEG).hexdigest() != library.fingerprint(PNG)
    assert library.fingerprint("FAKE-TEXT é") == hashlib.sha256("FAKE-TEXT é".encode()).hexdigest()


def test_a_work_first_stored_with_an_image_has_it_written_down_as_the_image_its_thumbnail_shows():
    library.save_image(ID1, "image/jpeg", JPEG)
    keep(ALPHA, [fields(thumb_url=shown()), fields(ID2, thumb_url=shown(OTHER_FILE))])  # ID2 has no cached image
    assert rows("work_images") == [{"deviation_id": ID1, "source": FILE, "fingerprint": library.fingerprint(JPEG)}]
    assert library.image_prints() == {ID1: library.fingerprint(JPEG)}
    assert library.cached_image(ID1) and library.cached_image(ID1, thumb=shown(size=150))
    assert not library.cached_image(ID1, thumb=shown(OTHER_FILE)) and not library.cached_image(ID2)


def test_a_thumbnail_signed_again_or_cut_to_another_size_leaves_the_image_as_it_is():
    library.save_image(ID1, "image/jpeg", JPEG)
    keep(ALPHA, [fields(thumb_url=shown())])
    before = rows("work_images")
    keep(ALPHA, [fields(thumb_url=shown(size=150, signed=SIGNED_AGAIN, folder="i"))])
    assert rows("works")[0]["thumb_url"] == shown(size=150, signed=SIGNED_AGAIN, folder="i")
    assert rows("work_images") == before and library.image_prints() == {ID1: library.fingerprint(JPEG)}


def test_a_thumbnail_that_comes_to_show_another_file_leaves_the_image_out_of_date_until_it_is_fetched():
    """The critic's third counterexample, at its root: the address is brought up to date and the image is not."""
    library.save_image(ID1, "image/jpeg", JPEG)
    keep(ALPHA, [fields(thumb_url=shown())])
    keep(ALPHA, [fields(thumb_url=shown(OTHER_FILE))])  # the answer names another file, and nobody fetched it
    assert rows("works")[0]["thumb_url"] == shown(OTHER_FILE) and rows("work_images")[0]["source"] == FILE
    assert library.image_prints() == {}  # out of date: no reading of it is current, and nothing reads it
    assert library.cached_image(ID1) and not library.cached_image(ID1, thumb=shown(OTHER_FILE))
    keep(ALPHA, [fields(thumb_url=shown())])  # the artist puts the first file back: the image is that one's
    assert library.image_prints() == {ID1: library.fingerprint(JPEG)}

    keep(ALPHA, [fields(thumb_url=shown(OTHER_FILE))])
    another = JPEG + b"FAKE-ANOTHER-PICTURE"
    library.save_image(ID1, "image/jpeg", another)  # the collect stage fetches it,
    keep(ALPHA, [fields(thumb_url=shown(OTHER_FILE), fetched=True)])  # and says so
    assert rows("work_images") == [
        {"deviation_id": ID1, "source": OTHER_FILE, "fingerprint": library.fingerprint(another)}
    ]
    assert library.image_prints() == {ID1: library.fingerprint(another)}
    assert library.cached_image(ID1, thumb=shown(OTHER_FILE))


def test_an_image_fetched_again_in_another_type_is_named_and_the_old_file_goes_only_after_that(tmp_path):
    library.save_image(ID1, "image/jpeg", JPEG)
    keep(ALPHA, [fields(thumb_url=shown())])
    library.save_image(ID1, "image/png", PNG)  # the new file is a PNG: both are in the cache, the row names the old
    assert files(tmp_path / "cache" / "images") == [f"{ID1}.jpg", f"{ID1}.png"]
    assert rows("works")[0]["image_file"] == f"{ID1}.jpg" and library.image_prints()[ID1] == library.fingerprint(JPEG)
    keep(ALPHA, [fields(thumb_url=shown(OTHER_FILE), fetched=True)])
    assert rows("works")[0]["image_file"] == f"{ID1}.png" and library.image_prints()[ID1] == library.fingerprint(PNG)
    assert library.drop_orphans() == (1, 0) and files(tmp_path / "cache" / "images") == [f"{ID1}.png"]
    keep(ALPHA, [fields(thumb_url=shown(OTHER_FILE))])  # and a later turn that fetches nothing keeps naming it
    assert rows("works")[0]["image_file"] == f"{ID1}.png"


def test_a_purge_takes_the_note_of_which_image_a_work_has():
    library.save_image(ID1, "image/jpeg", JPEG)
    keep(ALPHA, [fields(thumb_url=shown())])
    library.purge(ALPHA)
    assert rows("work_images") == [] and library.image_prints() == {}


def test_a_store_made_before_readings_carried_fingerprints_is_not_opened(tmp_path):
    """Every stage opens the file through library._open(). A store with works in it and without the mark of the
    migration stops each of them at once, with the command to run, and not a byte of it is changed."""
    library.save_image(ID1, "image/jpeg", JPEG)
    keep(ALPHA, [fields()])
    raw("DROP TABLE work_images")
    raw("PRAGMA user_version = 0")  # as every store was before the migration
    before = database_bytes()
    calls = [library.artist_keys, library.work_marks, library.image_prints, lambda: library.cached_image(ID1)]
    calls += [lambda: keep(ALPHA, [fields()]), lambda: library.purge(ALPHA), library.drop_orphan_rows]
    for call in calls:
        with pytest.raises(SystemExit) as stopped:
            call()
        assert "uv run python -m pipeline.tagging --migrate --backup" in str(stopped.value)
    assert database_bytes() == before and files(tmp_path / "cache" / "images") == [f"{ID1}.jpg"]


def test_a_new_store_is_made_in_the_shape_of_today_and_one_with_no_works_yet_is_opened(tmp_path):
    assert store.upsert_artist("zz-fake-alpha", "https://www.deviantart.com/zz-fake-alpha", [], 1)  # discovery's
    assert raw("PRAGMA user_version") == [(0,)] and library.artist_keys() == []
    assert raw("PRAGMA user_version") == [(library.SHAPE,)]
    other = tmp_path / "elsewhere" / "other.sqlite"
    assert library.artist_keys(other) == []
    with closing(sqlite3.connect(other)) as conn:
        assert conn.execute("PRAGMA user_version").fetchone() == (library.SHAPE,)
