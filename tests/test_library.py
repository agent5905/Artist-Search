"""Tests for pipeline.library.

Every handle, address, text and byte below is invented for these tests. Nothing came from DeviantArt and nothing
here fetches anything. The cache directory and the database are moved into pytest's temporary directory.
"""

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
ID1, ID2, ID3 = (f"00000000-0000-4000-A000-{n:012d}" for n in (1, 2, 3))
PAGE = "https://www.deviantart.com/zz-fake-alpha/art/FAKE-TITLE-1-1001"
THUMB = "https://images-wixmp-zzfake.wixmp.com/f/FAKE-THUMB-1.jpg?token=FAKE-CDN-TOKEN-1"
JPEG = b"\xff\xd8\xff" + b"FAKE-IMAGE-BYTES-1" * 50
PNG = b"\x89PNG\r\n\x1a\n" + b"FAKE-IMAGE-BYTES-2" * 50
# A 200 KB "embedded image", the kind an earlier critic got into a text column of the remote database.
EMBEDDED = "data:image/png;base64," + "FAKEBASE64" * 20_000


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


def profile(key=ALPHA, **fields):
    blank = dict.fromkeys(("tagline", "specialty", "level", "bio", "website"))
    library.upsert_profile(key, **{**blank, **fields})
    return next(row for row in rows("profiles") if row["artist_key"] == key)


def work(key=ALPHA, work_id=ID1, **fields):
    given = {"page_url": PAGE, "title": "FAKE-TITLE-1", "tags": ["faketag"], "thumb_url": THUMB, "published": "1"}
    library.upsert_work(key, work_id, **{**given, **fields})
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
        strict = [
            row[0] for row in conn.execute("SELECT strict FROM pragma_table_list WHERE name IN ('profiles', 'works')")
        ]
    assert columns["profiles"] == [
        ("artist_key", "TEXT"),
        ("tagline", "TEXT"),
        ("specialty", "TEXT"),
        ("level", "TEXT"),
        ("bio", "TEXT"),
        ("website", "TEXT"),
        ("fetched_at", "TEXT"),
    ]
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
    assert strict == [1, 1]


@pytest.mark.parametrize("column", ["tagline", "specialty", "level", "bio", "website"])
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
    library.upsert_profile(ALPHA, tagline="FAKE-TAGLINE", specialty=None, level=None, bio=None, website=None, db=other)
    assert [row["tagline"] for row in rows("profiles", other)] == ["FAKE-TAGLINE"]
    assert library.artist_keys(other) == [ALPHA]
    assert not config.LOCAL_DB.exists()


# --- upserts ---


def test_storing_a_profile_twice_keeps_one_row_with_the_latest_fields():
    profile(tagline="FAKE-TAGLINE-OLD", bio="FAKE-BIO-OLD", website="https://zz-fake-alpha.art")
    stored = profile(tagline="FAKE-TAGLINE-NEW", specialty="FAKE-SPECIALTY", level="FAKE-LEVEL")
    assert len(rows("profiles")) == 1
    assert {name: value for name, value in stored.items() if name != "fetched_at"} == {
        "artist_key": ALPHA,
        "tagline": "FAKE-TAGLINE-NEW",
        "specialty": "FAKE-SPECIALTY",
        "level": "FAKE-LEVEL",
        "bio": None,
        "website": None,
    }
    assert stored["fetched_at"].endswith("+00:00")


def test_storing_a_work_twice_keeps_one_row_and_its_cached_image():
    work(title="FAKE-TITLE-OLD", tags=["fakeold"])
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


def test_a_deviation_id_is_one_row_however_it_is_cased():
    work(work_id=ID1.lower())
    work(work_id=ID1)
    assert [row["deviation_id"] for row in rows("works")] == [ID1]


# --- hard rules 1 and 6: what a text may hold ---


@pytest.mark.parametrize(
    ("given", "kept"),
    [
        ("  FAKE  words\n\tover   lines ", "FAKE words over lines"),
        ("<p>FAKE <b>bold</b> words</p>", "FAKE bold words"),
        (f'FAKE before <img src="{EMBEDDED}"> after', "FAKE before after"),
        (f"FAKE before {EMBEDDED} after", "FAKE before after"),
        ("FAKE before data:image/gif;base64,R0lGODlhAQABAAAAACw= after", "FAKE before after"),
        ("FAKE before DATA:;base64,RkFLRQ== after", "FAKE before after"),
        ("FAKE before &lt;img src=data:image/png;base64,RkFLRQ==&gt; after", "FAKE before after"),
        ("FAKE before " + "A" * 5000 + " after", "FAKE before after"),
        ("FAKE metadata: kept, as are 12 x 34 px", "FAKE metadata: kept, as are 12 x 34 px"),
        ("FAKE write to zz.fake+tag@mail.invalid now", "FAKE write to now"),
        ("FAKE write to zz.fake&#64;mail.invalid now", "FAKE write to now"),
        ("FAKE write to zz.fake＠mail.invalid now", "FAKE write to now"),  # a fullwidth @
        ('FAKE <a href="mailto:zz.fake@mail.invalid">write</a> now', "FAKE write now"),
        ("FAKE call +00 (555) 010-9999 now", "FAKE call now"),
        ("FAKE call 555.010.9999x now", "FAKE call x now"),
        ("FAKE call ５５５ 010 999 now", "FAKE call now"),  # fullwidth digits
        ("FAKE years 2019-2023 and 1920x1080 stay", "FAKE years 2019-2023 and 1920x1080 stay"),
        ("FAKE\x00nul\x1b[31m and \ud800 surrogate", "FAKE nul [31m and surrogate"),
        ("フェイク" * 60, ("フェイク" * 60)),  # 240 letters with no space: a bio, not a file
        ("   ", None),
        ("<br>", None),
        (None, None),
        (12345, None),
        (["FAKE"], None),
        (b"FAKE", None),
    ],
    ids=short,
)
def test_a_text_is_stored_as_plain_words(given, kept):
    assert profile(bio=given)["bio"] == kept
    assert work(title=given)["title"] == (kept[:200] if kept else None)


def test_an_embedded_image_of_any_size_never_reaches_a_text_column():
    """The round-2 database finding, on this store: 200 KB of image in every text field, and none of it is kept."""
    profile(tagline=EMBEDDED, specialty=EMBEDDED, level=EMBEDDED, bio=EMBEDDED, website=EMBEDDED)
    work(title=EMBEDDED, tags=[EMBEDDED, "faketag"])
    assert [value for name, value in rows("profiles")[0].items() if name not in ("artist_key", "fetched_at")] == [
        None
    ] * 5
    assert (rows("works")[0]["title"], rows("works")[0]["tags"]) == (None, '["faketag"]')
    assert b"FAKEBASE64" not in database_bytes() and b"base64" not in database_bytes()
    assert config.LOCAL_DB.stat().st_size < 100_000  # the empty tables and their indexes are about half of that


@pytest.mark.parametrize(
    "hostile",
    [
        "data:" * 200_000,
        "<" * 1_000_000,
        "<a" * 500_000,
        "a" * 1_000_000 + "@",
        "@a" * 500_000,
        "a." * 500_000 + "@",
        "1." * 500_000,
        ("a" * 199 + " ") * 5_000,
        "&lt;" * 250_000,
    ],
    ids=short,
)
def test_cleaning_a_megabyte_written_to_be_slow_is_still_quick(hostile):
    """Each takes well under a second. A pattern that rereads the text from every position would take minutes."""
    started = time.perf_counter()
    profile(bio=hostile)
    assert time.perf_counter() - started < 10


def test_every_text_is_cut_to_its_limit():
    long = "FAKE word " * 2000
    stored = profile(tagline=long, specialty=long, level=long, bio=long)
    assert [len(stored[name]) for name in ("tagline", "specialty", "level", "bio")] == [199, 99, 99, 1999]
    assert len(work(title=long)["title"]) == 199


@pytest.mark.parametrize(
    ("given", "kept"),
    [
        ("https://zz-fake-alpha.art", "https://zz-fake-alpha.art/"),
        ("  ZZ-Fake-Alpha.ART.  ", "https://zz-fake-alpha.art/"),
        ("http://www.zz-fake-alpha.com/folio/", "http://www.zz-fake-alpha.com/folio/"),
        (
            "https://www.artstation.com/zz-fake-alpha?FAKE-QUERY=1#FAKE-FRAGMENT",
            "https://www.artstation.com/zz-fake-alpha",
        ),
        ("zz.fake@mail.invalid", None),
        ("mailto:zz.fake@mail.invalid", None),
        ("https://zz.fake@mail.invalid", None),
        ("https://FAKE-USER:FAKE-PASSWORD@zz-fake-alpha.art/", None),
        ("https://zz-fake-alpha.art/write-to/zz.fake@mail.invalid", None),
        ("https://zz-fake-alpha.art/write-to/zz.fake%40mail.invalid", None),
        ("https://zz-fake-alpha.art/write-to/zz.fake＠mail.invalid", None),
        ("https://zz-fake-alpha.art/?mail=zz.fake@mail.invalid", None),
        ("FAKE words that are not an address", None),
        ("javascript:alert(1)", None),
        ("ftp://zz-fake-alpha.art/", None),
        ("https://127.0.0.1/", None),
        ("https://localhost/", None),
        (EMBEDDED, None),
        ("https://zz-fake-alpha.art/" + "a" * 300, None),
        ("", None),
        (None, None),
        (12345, None),
    ],
    ids=short,
)
def test_a_website_is_kept_only_as_scheme_host_and_path(given, kept):
    assert profile(website=given)["website"] == kept


def test_tags_are_kept_as_a_json_list_of_plain_tag_names():
    given = ["fakeone", "FAKE_Two", "fake-three", "フェイク", "fake four", "zz.fake@mail.invalid"]
    given += [EMBEDDED, "<b>fakefive</b>", "", None, 7, ["fakenested"], "x" * 65, "5550109999"]
    assert json.loads(work(tags=given)["tags"]) == ["fakeone", "FAKE_Two", "fake-three", "フェイク", "fakefive"]
    assert len(json.loads(work(tags=[f"fake{n}" for n in range(500)])["tags"])) == 60
    assert [work(tags=tags)["tags"] for tags in (None, "faketag", {"faketag": 1}, 5)] == ["[]"] * 4


@pytest.mark.parametrize(
    "address",
    [
        "http://www.deviantart.com/zz-fake-alpha/art/FAKE-1",
        "//www.deviantart.com/zz-fake-alpha/art/FAKE-1",
        "www.deviantart.com/zz-fake-alpha/art/FAKE-1",
        "https://www.deviantart.com.zz-fake-evil.invalid/FAKE-1",
        "https://zz-fake-evildeviantart.com/FAKE-1",
        "https://zz-fake-evil.invalid/www.deviantart.com/FAKE-1",
        "https://zz-fake-evil.invalid/?https://www.deviantart.com/FAKE-1",
        "https://www.deviantart.com@zz-fake-evil.invalid/FAKE-1",
        "https://FAKE-USER:FAKE-PASSWORD@www.deviantart.com/FAKE-1",
        "https://zz-fake-evil.invalid\\@www.deviantart.com/FAKE-1",
        "https://www.deviantart.com\\@zz-fake-evil.invalid/FAKE-1",
        "https://www.deviantart.com:8443/FAKE-1",
        "https://www.deviantart.com./FAKE-1",
        "https://www.deviantart.com/FAKE 1",
        "https://www.deviantart.com/FAKE-1\n",
        "https://www.deviantart.com/FAKE-フ",
        "https://www.deviantart.com/" + "a" * 2000,
        "https://127.0.0.1/FAKE-1",
        "https://localhost/FAKE-1",
        "file:///FAKE/1.jpg",
        "javascript:alert(1)",
        EMBEDDED,
        "data:image/png;base64,RkFLRQ==",
        "",
        None,
        12345,
        b"https://www.deviantart.com/FAKE-1",
    ],
    ids=short,
)
def test_an_address_that_is_not_https_on_deviantart_is_refused_and_nothing_is_stored(address):
    with pytest.raises(ValueError):
        work(page_url=address)
    with pytest.raises(ValueError):
        work(thumb_url=address)
    with pytest.raises(ValueError):
        library.checked_url(address, library.IMAGE_DOMAINS)
    assert rows("works") == []


def test_a_page_must_be_on_deviantart_itself_and_an_image_on_one_of_its_image_hosts():
    assert library.checked_url(PAGE, library.PAGE_DOMAINS) == PAGE
    assert (
        library.checked_url("HTTPS://WWW.DeviantArt.com/FAKE-1", library.PAGE_DOMAINS)
        == "https://www.deviantart.com/FAKE-1"
    )
    assert library.checked_url(THUMB, library.IMAGE_DOMAINS) == THUMB
    assert library.checked_url("https://zzfake00.deviantart.net/FAKE-1.jpg", library.IMAGE_DOMAINS)
    with pytest.raises(ValueError):
        library.checked_url(THUMB, library.PAGE_DOMAINS)


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
    for call in (work, lambda work_id: library.save_image(work_id, "image/jpeg", JPEG), library.cached_image):
        with pytest.raises(ValueError):
            call(work_id=work_id) if call is work else call(work_id)
    assert rows("works") == [] and not library.image_dir().exists()


@pytest.mark.parametrize(
    "key", ["zz-fake-alpha", "da:ZZ-Fake-Alpha", "da:", "da:zz fake", "site:zz-fake.art", "", None, 7]
)
def test_an_artist_key_must_be_spelled_the_way_the_store_spells_it(key):
    """Otherwise a removal, which goes by the store's key, would not find what was kept under it."""
    for call in (profile, work, library.purge, lambda key: library.drop_other_works(key, ())):
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
    work()
    assert library.save_image(ID1, "image/jpeg", JPEG) == f"{ID1}.jpg"
    assert files(tmp_path) == ["cache/images/" + f"{ID1}.jpg", "cache/pipeline.sqlite"]
    assert (config.CACHE_DIR / "images" / f"{ID1}.jpg").read_bytes() == JPEG
    assert rows("works")[0]["image_file"] == f"{ID1}.jpg"
    # Nothing image-like in the database: not the bytes, and no column of any row that is not text or empty.
    assert b"FAKE-IMAGE-BYTES" not in database_bytes() and JPEG[:3] not in database_bytes()
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
    for function in (library.save_image, library.cached_image, library.purge, library.drop_other_works):
        assert set(inspect.signature(function).parameters) <= {
            "work_id",
            "content_type",
            "data",
            "artist_key",
            "keep",
            "db",
        }
    assert inspect.signature(library.image_dir).parameters == {}


@pytest.mark.parametrize(
    ("content_type", "data"),
    [
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
        ("image/jpeg", JPEG + b"\x00" * (library.MAX_IMAGE_BYTES - len(JPEG) + 1)),  # one byte over the cap
    ],
    ids=short,
)
def test_bytes_that_are_not_an_accepted_image_are_not_written(content_type, data, tmp_path):
    work()
    with pytest.raises(ValueError):
        library.save_image(ID1, content_type, data)
    assert files(tmp_path) == ["cache/pipeline.sqlite"]
    assert rows("works")[0]["image_file"] is None


def test_an_image_exactly_at_the_cap_and_each_accepted_type_are_written(tmp_path):
    types = {
        "image/jpeg": b"\xff\xd8\xff",
        "image/png": b"\x89PNG\r\n\x1a\n",
        "image/gif": b"GIF89a",
        "image/webp": b"RIFF",
    }
    for work_id, (content_type, start) in zip((ID1, ID2, ID3, ID3[:-1] + "4"), types.items(), strict=True):
        work(work_id=work_id)
        library.save_image(work_id, content_type, start + b"FAKE")
    work(work_id=ID1)
    library.save_image(ID1, "image/jpeg", JPEG + b"\x00" * (library.MAX_IMAGE_BYTES - len(JPEG)))
    assert [name.rsplit(".", 1)[1] for name in files(tmp_path)[:-1]] == ["jpg", "png", "gif", "webp"]
    assert set(types) == set(library.IMAGE_TYPES)


def test_an_image_is_not_written_for_a_work_that_has_no_row(tmp_path):
    """Every file in the cache has a row, so purge() can always find it."""
    library.artist_keys()
    with pytest.raises(ValueError):
        library.save_image(ID1, "image/jpeg", JPEG)
    assert files(tmp_path) == ["cache/pipeline.sqlite"]


def test_a_cached_image_is_found_by_its_file_and_named_on_the_row():
    work()
    assert library.cached_image(ID1) is False
    library.image_dir().mkdir(parents=True)
    (library.image_dir() / f"{ID1}.png.part").write_bytes(PNG)  # half a download is not a cached image
    assert library.cached_image(ID1) is False
    (library.image_dir() / f"{ID1}.png").write_bytes(PNG)  # as if the database had been deleted and refilled
    assert library.cached_image(ID1.lower()) is True
    assert rows("works")[0]["image_file"] == f"{ID1}.png"


# --- hard rule 7: purge ---


def two_artists():
    for key, ids in ((ALPHA, (ID1, ID2)), (BRAVO, (ID3,))):
        handle = key.removeprefix("da:")
        profile(key, tagline=f"FAKE-TAGLINE-{handle}", bio=f"FAKE-BIO-{handle}", website=f"https://{handle}.art")
        for work_id in ids:
            work(key, work_id, title=f"FAKE-TITLE-{handle}", page_url=f"https://www.deviantart.com/{handle}/art/FAKE")
            library.save_image(work_id, "image/jpeg", JPEG)


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


def test_a_file_that_cannot_be_deleted_keeps_its_row(monkeypatch):
    """A purge that fails says so and can be run again. It never reports an artist gone while a file remains."""
    two_artists()

    def locked(path):
        raise PermissionError("FAKE-LOCKED")

    with monkeypatch.context() as patch:
        patch.setattr("pathlib.Path.unlink", locked)
        with pytest.raises(PermissionError):
            library.purge(ALPHA)
    assert [row["deviation_id"] for row in rows("works") if row["artist_key"] == ALPHA] == [ID1, ID2]
    assert [row["artist_key"] for row in rows("profiles")] == [ALPHA, BRAVO]
    library.purge(ALPHA)
    assert library.artist_keys() == [BRAVO]


def test_drop_other_works_keeps_the_named_works_and_deletes_the_rest_with_their_files(tmp_path):
    two_artists()
    assert library.drop_other_works(ALPHA, [ID2, ID3]) == 1  # ID3 is BRAVO's: naming it changes nothing
    assert [(row["artist_key"], row["deviation_id"]) for row in rows("works")] == [(ALPHA, ID2), (BRAVO, ID3)]
    assert files(tmp_path) == [f"cache/images/{ID2}.jpg", f"cache/images/{ID3}.jpg", "cache/pipeline.sqlite"]
    assert library.drop_other_works(ALPHA, [ID2]) == 0
    assert len(rows("profiles")) == 2
