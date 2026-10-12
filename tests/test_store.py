"""Tests for pipeline.store. Every name and URL here is invented, and nothing here touches the network.

The made-up sites sit under real suffixes (.art, .com, .co.uk): what counts as somebody's own site is decided
with the Public Suffix List, which turns .example and .test away.
"""

import hashlib
import json
import sqlite3
from contextlib import closing
from datetime import datetime, timedelta

import pytest

from pipeline import config, crawl, store

PAINTER = "https://zz-fake-painter.art/"
SCULPTOR = "https://zz-fake-sculptor.art/"


@pytest.fixture
def db(tmp_path):
    return tmp_path / "store.sqlite"


def rows(db, table="artists"):
    with closing(sqlite3.connect(db)) as conn:
        return conn.execute(f"SELECT * FROM {table} ORDER BY 1").fetchall()


def keys(db):
    return [row[0] for row in rows(db)]


def sha(key):
    return hashlib.sha256(key.encode()).hexdigest()


def add(db, username="ZZ-Fake-Painter", links=(PAINTER,), hits=4):
    return store.upsert_artist(username, f"https://www.deviantart.com/{username}", list(links), hits, db)


# --- the artists table ---


def test_upsert_stores_exactly_the_agreed_columns(db):
    assert add(db) is True
    with closing(sqlite3.connect(db)) as conn:
        columns = [row[1] for row in conn.execute("PRAGMA table_info(artists)")]
        primary_key = [row[1] for row in conn.execute("PRAGMA table_info(artists)") if row[5]]
    assert columns == ["key", "da_username", "da_profile_url", "links", "niche_hits", "discovered_at"]
    assert primary_key == ["key"]
    key, username, profile_url, links, hits, discovered_at = rows(db)[0]
    assert key == "da:zz-fake-painter"  # lowercased
    assert username == "ZZ-Fake-Painter"  # as spelled
    assert profile_url == "https://www.deviantart.com/ZZ-Fake-Painter"
    assert json.loads(links) == [PAINTER]
    assert hits == 4
    stamp = datetime.fromisoformat(discovered_at)
    assert stamp.utcoffset() == timedelta(0)  # ISO-8601, UTC


def test_upsert_is_idempotent(db):
    add(db)
    first = rows(db)
    add(db)
    add(db, username="zz-fake-painter")  # the same artist in another case is the same row
    assert len(rows(db)) == 1
    assert rows(db)[0][0] == first[0][0]


def test_upsert_refreshes_the_row_but_keeps_discovered_at(db):
    add(db)
    with closing(sqlite3.connect(db)) as conn, conn:
        conn.execute("UPDATE artists SET discovered_at = '2020-01-01T00:00:00+00:00'")
    add(db, links=("https://zz-fake-painter-moved.art/",), hits=9)
    (row,) = rows(db)
    assert json.loads(row[3]) == ["https://zz-fake-painter-moved.art/"]
    assert row[4] == 9
    assert row[5] == "2020-01-01T00:00:00+00:00"


@pytest.mark.parametrize(
    ("username", "links"),
    [
        ("zz fake painter", [PAINTER]),
        ("zz-fake/../painter", [PAINTER]),
        ("", [PAINTER]),
        ("zz-fake-painter", ["not a link"]),
        ("zz-fake-painter", ["https://localhost/x"]),
        ("zz-fake-painter", [PAINTER, "https://zz-fake_painter.art"]),
    ],
)
def test_upsert_refuses_a_bad_handle_or_a_link_with_no_public_host(db, username, links):
    with pytest.raises(ValueError):
        store.upsert_artist(username, "https://www.deviantart.com/zz-fake-painter", links, 3, db)
    assert not db.exists() or rows(db) == []


def test_default_path_is_config_local_db(tmp_path, monkeypatch):
    target = tmp_path / "nested" / "pipeline.sqlite"
    monkeypatch.setattr(config, "LOCAL_DB", target)
    add(None)
    assert store.mark_removed("da:zz-fake-someone-else") == 0
    assert store.is_removed("da:zz-fake-someone-else")
    assert store.import_removed([sha("da:zz-fake-third")]) == 0
    assert store.own_site_candidates() == [("da:zz-fake-painter", PAINTER)]
    assert store.bar_if_site_removed("zz-fake-fourth", "zz-fake-fourth.art") is False
    assert target.exists()
    assert len(rows(target)) == 1


# --- one spelling per host ---


@pytest.mark.parametrize(
    ("host", "canonical"),
    [
        ("fake-site.example", "fake-site.example"),
        ("  WWW.Fake-Site.Example  ", "www.fake-site.example"),
        ("fake-site.example.", "fake-site.example"),  # a trailing dot
        ("fake-site.example...", "fake-site.example"),
        ("fake-site" + chr(0x3002) + "example", "fake-site.example"),  # an ideographic full stop
        ("fake-site.example" + chr(0xFF0E), "fake-site.example"),  # a fullwidth one, trailing
        ("fake-site" + chr(0xFF61) + "example" + chr(0x3002), "fake-site.example"),  # the halfwidth one
        (chr(0xFF26) + "ake-site.example", "fake-site.example"),  # a fullwidth letter
        ("f" + chr(0xE4) + "ke-site.example", "xn--fke-site-0za.example"),
        ("xn--fke-site-0za.example", "xn--fke-site-0za.example"),
        # characters the 2003 codec would respell into another name (sharp s into "ss"): dropped, not guessed at
        ("fake-stra" + chr(0xDF) + "e.example", None),
        ("FAKE-STRA" + chr(0x1E9E) + "E.example", None),
        ("fake-" + chr(0x3C3) + chr(0x3BF) + chr(0x3C2) + ".example", None),
        ("fake" + chr(0x200D) + "-site.example", None),
        ("fake" + chr(0x200C) + "-site.example", None),
        ("", None),
        (".", None),
        ("example", None),  # no dot: not a public name
        ("localhost", None),
        ("127.0.0.1", None),
        ("::1", None),
        ("fake-site%2Eexample", None),
        ("fake-site.example\\.other.example", None),
        ("fake site.example", None),
        ("fake_site.example", None),
        ("-fake-site.example", None),
        ("fake-site-.example", None),
        ("fake-site..example", None),
        ("fake-site.example/path", None),
        ("fake-site.example:8080", None),
        ("fake-person@fake-site.example", None),
        ("a" * 64 + ".example", None),
    ],
)
def test_canonical_host(host, canonical):
    assert store.canonical_host(host) == canonical


@pytest.mark.parametrize(
    ("host", "domain"),
    [
        ("zz-fake-site.art", "zz-fake-site.art"),
        ("www.zz-fake-site.art", "zz-fake-site.art"),
        ("shop.zz-fake-site.art", "zz-fake-site.art"),
        ("a.b.zz-fake-site.co.uk", "zz-fake-site.co.uk"),  # a two-part public suffix
        ("www.zz-fake-site.art.br", "zz-fake-site.art.br"),
        ("patreon.zz-fake-site.com", "zz-fake-site.com"),  # only the subdomain is named after a platform
        ("www.art", "www.art"),  # a domain that happens to be called www
        # a platform, a suffix some host hands out, a bare suffix, or no public suffix at all
        ("www.artstation.com", None),
        ("www.deviantart.com", None),
        ("twitter.com", None),
        ("www.instagram.com", None),
        ("zz-fake.tumblr.com", None),
        ("zz-fake.carrd.co", None),
        ("linktr.ee", None),
        ("fav.me", None),
        ("sta.sh", None),
        ("zz-fake.github.io", None),
        ("zz-fake.blogspot.com", None),
        ("co.uk", None),
        ("zz-fake-site.example", None),
        ("zz-fake-site.test", None),
    ],
)
def test_registrable_domain_is_the_crawl_stage_s_notion_of_one_site(host, domain):
    assert store.registrable_domain(host) == domain
    if domain:  # and that is exactly a domain whose front page the crawl stage calls an own site
        assert crawl.is_own_site(f"https://{domain}/")
        assert store.registrable_domain(domain) == domain


# --- what an operator may type, and what is refused ---


@pytest.mark.parametrize(
    ("typed", "key"),
    [
        ("da:zz-fake-g", "da:zz-fake-g"),
        ("zz-fake-g", "da:zz-fake-g"),  # a bare handle
        ("ZZ-Fake-G", "da:zz-fake-g"),
        ("@zz-fake-g", "da:zz-fake-g"),
        ("da: zz-fake-g", "da:zz-fake-g"),
        ("  DA : ZZ-FAKE-G  ", "da:zz-fake-g"),
        ("https://www.deviantart.com/zz-fake-g", "da:zz-fake-g"),  # a profile URL
        ("https://www.deviantart.com/ZZ-Fake-G/gallery?page=2", "da:zz-fake-g"),
        ("https://www.deviantart.com/zz-fake-g/art/FAKE-TITLE-123", "da:zz-fake-g"),  # one of their works
        ("http://deviantart.com./zz-fake-g/", "da:zz-fake-g"),
        ("www.deviantart.com/zz-fake-g", "da:zz-fake-g"),
        ("da:https://www.deviantart.com/zz-fake-g", "da:zz-fake-g"),
        ("site:zz-fake-g.art", "site:zz-fake-g.art"),
        ("zz-fake-g.art", "site:zz-fake-g.art"),  # a bare host
        ("site:https://zz-fake-g.art", "site:zz-fake-g.art"),
        ("site:zz-fake-g.art/", "site:zz-fake-g.art"),
        ("site: WWW.ZZ-Fake-G.Art. ", "site:zz-fake-g.art"),
        ("https://www.zz-fake-g.art./portfolio?x=1#y", "site:zz-fake-g.art"),
        ("http://zz-fake-g.art:8080/", "site:zz-fake-g.art"),
        ("zz-fake-g" + chr(0x3002) + "art", "site:zz-fake-g.art"),
        ("site:shop.zz-fake-g.art", "site:zz-fake-g.art"),  # any host of a site names the whole site
        ("https://a.b.zz-fake-g.art/x", "site:zz-fake-g.art"),
    ],
)
def test_every_plain_spelling_names_the_same_artist_or_site(db, typed, key):
    assert store.normalize(typed) == key
    add(db, username="ZZ-Fake-G", links=("https://www.zz-fake-g.art/",))
    add(db, username="zz-fake-other", links=("https://zz-fake-other.art/",))
    assert store.mark_removed(typed, db) == 1
    assert store.is_removed(typed, db) and store.is_removed(key, db)
    assert keys(db) == ["da:zz-fake-other"]  # the removal did something, and only to them


@pytest.mark.parametrize(
    "typed",
    [
        "",
        "   ",
        "da:",
        "site:",
        "zz fake g",
        "da:zz fake g",
        "site:zz-fake-g",  # a handle is not a site
        "da:zz-fake-g.art",  # a site is not a handle
        "site:https://www.deviantart.com/zz-fake-g",
        "https://www.deviantart.com/",  # no handle in it
        "https://www.deviantart.com",
        "zz-fake-g.deviantart.com",  # the old subdomain form: refused, not guessed at
        "fake.person@mail.invalid",  # an email address is neither
        "mailto:fake.person@mail.invalid",
        "fake.person@zz-fake-g.art",  # not even one at a domain that is somebody's own site
        "mailto:fake.person@zz-fake-g.art",
        "site:fake.person@zz-fake-g.art",
        "https://fake.person@zz-fake-g.art/",
        "site:localhost",
        "site:127.0.0.1",
        "https://[::1]/",
        "site:zz-fake_g.art",
        "site:zz-fake-g%2Eart",
        "site:zz-fake-g.art\\x",
        "https://",
        "z" * 41,
        # a host on a platform: discovery can never store it, so barring it would do nothing
        "https://www.artstation.com/zz-fake-g",
        "site:artstation.com",
        "https://twitter.com/zz-fake-g",
        "https://x.com/zz-fake-g",
        "https://www.instagram.com/zz-fake-g/",
        "https://linktr.ee/zz-fake-g",
        "zz-fake-g.tumblr.com",
        "https://zz-fake-g.carrd.co",
        "fav.me/dfake",
        "sta.sh/0fake",
        "https://zz-fake-g.github.io",
        "site:zz-fake-g.blogspot.com",
        # no registrable domain at all
        "site:co.uk",
        "site:zz-fake-g.example",
        "zz-fake-g.test",
        # a DeviantArt URL that is a site route, not a profile
        "https://www.deviantart.com/deviation/123456789",
        "https://www.deviantart.com/art/FAKE-TITLE-123",
        "https://www.deviantart.com/view/123456789",
        "https://www.deviantart.com/users/outgoing?https://zz-fake-g.art",
        "https://www.deviantart.com/tag/conceptart",
        "https://www.deviantart.com/search?q=zz-fake-g",
        "https://www.deviantart.com/gallery/zz-fake-g",
        "deviantart.com/watch",
        "da:https://www.deviantart.com/topic/zz-fake-g",
        "da:search",
    ],
)
def test_an_identifier_that_names_nothing_is_refused_loudly(db, typed):
    add(db, username="zz-fake-g", links=("https://zz-fake-g.art/",))
    with pytest.raises(ValueError):
        store.mark_removed(typed, db)
    with pytest.raises(ValueError):
        store.normalize(typed)
    assert rows(db, "removed") == []  # nothing was written
    assert keys(db) == ["da:zz-fake-g"]


def test_asking_about_a_platform_host_is_answered_not_refused(db):
    """mark_removed refuses a platform because barring it would do nothing. Asking whether it is removed is fine."""
    assert store.is_removed("site:www.artstation.com", db) is False
    assert store.is_removed("https://zz-fake-g.tumblr.com/post/1", db) is False
    for garbage in ("", "site:", "zz fake g", "site:zz-fake-g", "https://www.deviantart.com/tag/conceptart"):
        with pytest.raises(ValueError):
            store.is_removed(garbage, db)


# --- the removal list (hard rule 7) ---


def test_removed_list_holds_only_a_hash(db):
    store.mark_removed("da:ZZ-Fake-Painter", db)
    store.mark_removed("https://www.zz-fake-painter.art/", db)
    assert [row[0] for row in rows(db, "removed")] == sorted(
        [sha("da:zz-fake-painter"), sha("site:zz-fake-painter.art")]
    )
    assert all(datetime.fromisoformat(row[1]).utcoffset() == timedelta(0) for row in rows(db, "removed"))
    assert b"fake-painter" not in db.read_bytes().lower()


def test_is_removed_normalizes_the_identifier(db):
    assert store.is_removed("da:zz-fake-painter", db) is False
    store.mark_removed("  DA:ZZ-Fake-Painter ", db)
    assert store.is_removed("da:zz-fake-painter", db)
    assert store.is_removed("da:ZZ-FAKE-PAINTER", db)
    assert not store.is_removed("da:zz-fake-painter-2", db)

    store.mark_removed("site:WWW.ZZ-Fake-Painter.Art", db)
    assert store.is_removed("site:zz-fake-painter.art", db)
    assert store.is_removed(store.site_key("https://www.zz-fake-painter.art./gallery?page=2"), db)
    assert store.is_removed("https://shop.zz-fake-painter.art/x", db)  # any host of the removed site
    assert not store.is_removed("site:zz-fake-other-painter.art", db)
    assert not store.is_removed("site:notzz-fake-painter.art", db)  # another name that ends the same way


def test_mark_removed_is_idempotent_and_says_how_many_artists_it_deleted(db):
    add(db)
    add(db, username="zz-fake-sculptor", links=(SCULPTOR,))
    assert store.mark_removed("da:zz-fake-nobody", db) == 0  # matched nobody, and says so
    assert store.mark_removed("site:zz-fake-nowhere.art", db) == 0
    assert store.mark_removed("da:zz-fake-painter", db) == 1
    first = rows(db, "removed")
    assert store.mark_removed("da:ZZ-FAKE-PAINTER", db) == 0  # already gone
    assert store.mark_removed("https://www.deviantart.com/zz-fake-painter", db) == 0
    assert rows(db, "removed") == first
    assert keys(db) == ["da:zz-fake-sculptor"]


def test_removal_deletes_the_artist_and_blocks_every_later_upsert(db):
    add(db)
    add(db, username="zz-fake-sculptor", links=(SCULPTOR,))
    store.mark_removed("da:zz-fake-painter", db)
    assert keys(db) == ["da:zz-fake-sculptor"]
    assert add(db) is False
    assert add(db, username="ZZ-FAKE-PAINTER", links=("https://zz-fake-painter-new.art/",)) is False
    assert keys(db) == ["da:zz-fake-sculptor"]


def test_removal_leaves_no_trace_of_the_artist_in_the_file(db):
    add(db)
    store.mark_removed("da:zz-fake-painter", db)
    assert b"fake-painter" not in db.read_bytes().lower()


def test_removing_an_artist_by_handle_bars_the_site_in_their_row(db):
    """A renamed or second account that lists the same site must not bring the removed artist back."""
    add(db, username="zz-fake-old", links=("https://zz-fake-studio.art/",))
    assert store.mark_removed("da:zz-fake-old", db) == 1
    assert store.is_removed("site:zz-fake-studio.art", db)
    assert add(db, username="zz-fake-new", links=("https://www.zz-fake-studio.art/",)) is False
    assert store.is_removed("da:zz-fake-new", db)  # and the new handle is barred, whatever site it lists next
    assert add(db, username="zz-fake-new", links=("https://zz-fake-elsewhere.art/",)) is False
    assert rows(db) == []


def test_removing_one_of_two_artists_who_list_a_site_removes_both(db):
    add(db, username="zz-fake-one", links=("https://zz-fake-studio.art/",))
    add(db, username="zz-fake-two", links=("http://www.zz-fake-studio.art/",))
    add(db, username="zz-fake-other", links=("https://zz-fake-other.art/",))
    assert store.mark_removed("da:zz-fake-one", db) == 2
    assert keys(db) == ["da:zz-fake-other"]
    assert store.is_removed("da:zz-fake-two", db) and store.is_removed("site:zz-fake-studio.art", db)
    assert not store.is_removed("da:zz-fake-other", db) and not store.is_removed("site:zz-fake-other.art", db)


def test_a_removal_reaches_everything_connected_to_it(db):
    add(db, username="zz-fake-one", links=("https://zz-fake-first.art/",))
    add(db, username="zz-fake-two", links=("https://zz-fake-first.art/", "https://zz-fake-second.art/"))
    add(db, username="zz-fake-three", links=("https://zz-fake-second.art/",))
    add(db, username="zz-fake-other", links=("https://zz-fake-other.art/",))
    assert store.mark_removed("da:zz-fake-one", db) == 3
    assert keys(db) == ["da:zz-fake-other"]
    assert store.is_removed("site:zz-fake-second.art", db) and store.is_removed("da:zz-fake-three", db)


def test_removing_an_artist_never_bars_a_platform_they_linked_to(db):
    add(db, username="zz-fake-one", links=("https://www.artstation.com/zz-fake-one", "https://zz-fake-one.art/"))
    add(db, username="zz-fake-two", links=("https://www.artstation.com/zz-fake-two",))
    assert store.mark_removed("da:zz-fake-one", db) == 1
    assert keys(db) == ["da:zz-fake-two"]  # sharing a platform is not sharing a site
    assert not store.is_removed("site:www.artstation.com", db)
    assert [row[0] for row in rows(db, "removed")] == sorted([sha("da:zz-fake-one"), sha("site:zz-fake-one.art")])


def test_site_removal_deletes_and_blocks_artists_who_link_to_it(db):
    add(db, links=("https://www.zz-fake-painter.art/work",))
    add(db, username="zz-fake-sculptor", links=(SCULPTOR,))
    assert store.mark_removed("site:zz-fake-painter.art", db) == 1
    assert keys(db) == ["da:zz-fake-sculptor"]
    assert add(db, links=("http://ZZ-FAKE-PAINTER.art/",)) is False
    assert add(db, username="zz-fake-third", links=("https://zz-fake-ok.art", "https://zz-fake-painter.art/x")) is False
    assert keys(db) == ["da:zz-fake-sculptor"]


@pytest.mark.parametrize(
    "link",
    [
        "https://zz-fake-d.art./work",  # a trailing dot
        "https://www.zz-fake-d.art./work",
        "https://ZZ-FAKE-D.ART/work",
        "https://zz-fake-d" + chr(0x3002) + "art/work",
        "http://zz-fake-d.art:8080/",
        "https://shop.zz-fake-d.art/art",  # a subdomain
        "https://a.b.zz-fake-d.art",
    ],
)
def test_site_removal_matches_every_spelling_and_every_host_of_the_site(db, link):
    add(db, username="zz-fake-d", links=(link,))  # stored before the removal, in this spelling
    add(db, username="zz-fake-other", links=("https://notzz-fake-d.art",))  # another name that ends the same way
    assert store.mark_removed("site:zz-fake-d.art", db) == 1
    assert keys(db) == ["da:zz-fake-other"]
    assert add(db, username="zz-fake-second", links=(link,)) is False  # and after it
    assert keys(db) == ["da:zz-fake-other"]


def test_site_removal_bars_the_handle_of_every_artist_it_deletes(db):
    add(db, username="zz-fake-e", links=("https://zz-fake-e.art",))
    add(db, username="zz-fake-other", links=("https://zz-fake-other.art",))
    store.mark_removed("site:zz-fake-e.art", db)
    assert store.is_removed("da:zz-fake-e", db)
    assert not store.is_removed("da:zz-fake-other", db)
    assert add(db, username="zz-fake-e", links=("https://zz-fake-e-new.art",)) is False  # a new website
    assert keys(db) == ["da:zz-fake-other"]
    assert len(rows(db, "removed")) == 2  # the site and the handle, as hashes


def test_linking_to_a_removed_site_bars_the_handle_even_if_never_stored(db):
    store.mark_removed("site:zz-fake-e.art", db)
    assert add(db, username="zz-fake-e", links=("https://www.zz-fake-e.art./work",)) is False
    assert store.is_removed("da:zz-fake-e", db)
    assert add(db, username="zz-fake-e", links=("https://zz-fake-e-new.art",)) is False
    assert rows(db) == []


def test_an_artist_who_moves_to_a_removed_site_is_deleted_and_barred(db):
    add(db, username="zz-fake-e", links=("https://zz-fake-e-old.art",))
    store.mark_removed("site:zz-fake-e.art", db)
    assert keys(db) == ["da:zz-fake-e"]  # not affected yet
    assert add(db, username="zz-fake-e", links=("https://zz-fake-e.art",)) is False
    assert rows(db) == []
    assert store.is_removed("da:zz-fake-e", db)
    assert b"fake-e" not in db.read_bytes().lower()


def test_bar_if_site_removed_bars_whoever_points_at_a_removed_site(db):
    add(db, username="zz-fake-e", links=("https://zz-fake-e-other.art/",))
    assert store.bar_if_site_removed("zz-fake-e", "zz-fake-e.art", db) is False  # nothing removed yet
    assert keys(db) == ["da:zz-fake-e"] and rows(db, "removed") == []
    store.mark_removed("site:zz-fake-e.art", db)
    assert store.bar_if_site_removed("zz-fake-f", "www.artstation.com", db) is False  # a platform: never raises
    assert store.bar_if_site_removed("zz-fake-f", "zz-fake-f.art", db) is False
    assert not store.is_removed("da:zz-fake-f", db)
    assert store.bar_if_site_removed("ZZ-Fake-E", "shop.zz-fake-e.art", db) is True  # any host of the removed site
    assert store.is_removed("da:zz-fake-e", db)
    assert rows(db) == []  # the row they had under another site is gone too


def test_a_removal_survives_an_odd_row_written_around_upsert(db):
    add(db, username="zz-fake-e", links=("https://zz-fake-e.art",))
    with closing(sqlite3.connect(db)) as conn, conn:
        conn.execute("INSERT INTO artists VALUES ('da:zz-fake-odd', 'zz-fake-odd', '', '[\"not a link\"]', 3, '')")
    assert store.mark_removed("site:zz-fake-e.art", db) == 1
    assert keys(db) == ["da:zz-fake-odd"]
    assert store.mark_removed("da:zz-fake-odd", db) == 1  # and a row with such a link can still be removed
    assert rows(db) == []


# --- removals from the list of record ---


def test_an_artist_removed_only_through_imported_hashes_is_deleted_and_never_readded(db):
    add(db, username="zz-fake-gone", links=("https://zz-fake-gone.art/",))
    add(db, username="zz-fake-stays", links=("https://zz-fake-stays.art/",))
    assert store.import_removed([sha("da:zz-fake-gone")], db) == 1
    assert keys(db) == ["da:zz-fake-stays"]
    assert store.is_removed("da:zz-fake-gone", db)
    assert add(db, username="ZZ-Fake-Gone", links=("https://zz-fake-gone-new.art/",)) is False
    assert store.is_removed("site:zz-fake-gone.art", db)  # and their site went with them, as in mark_removed
    assert add(db, username="zz-fake-renamed", links=("https://zz-fake-gone.art/",)) is False
    assert keys(db) == ["da:zz-fake-stays"]
    assert b"fake-gone" not in db.read_bytes().lower()


@pytest.mark.parametrize(
    ("removed_key", "link"),
    [
        ("site:zz-fake-gone.art", "https://zz-fake-gone.art/"),  # the site key
        ("site:zz-fake-gone.art", "https://www.zz-fake-gone.art/"),
        ("site:www.zz-fake-gone.art", "https://www.zz-fake-gone.art/"),  # a key some other code made from the host
        ("site:zz-fake-gone.art", "https://shop.zz-fake-gone.art/art"),  # a parent-host key
        ("site:zz-fake-gone.co.uk", "https://a.b.zz-fake-gone.co.uk/"),
    ],
)
def test_an_imported_site_hash_removes_whoever_links_to_that_site(db, removed_key, link):
    add(db, username="zz-fake-gone", links=(link,))
    add(db, username="zz-fake-stays", links=("https://notzz-fake-gone.art/",))
    assert store.import_removed([sha(removed_key).upper()], db) == 1  # hex in either case
    assert keys(db) == ["da:zz-fake-stays"]
    assert store.is_removed("da:zz-fake-gone", db)  # their handle is barred too
    assert add(db, username="zz-fake-gone", links=("https://zz-fake-gone-new.art/",)) is False
    assert add(db, username="zz-fake-other", links=(link,)) is False


def test_importing_removals_is_idempotent_and_keeps_hashes_that_match_nobody(db):
    add(db)
    hashes = [sha("da:zz-fake-nobody"), sha("site:zz-fake-nowhere.art")]
    assert store.import_removed(hashes, db) == 0
    assert store.import_removed(iter(hashes), db) == 0  # any iterable, any number of times
    assert [row[0] for row in rows(db, "removed")] == sorted(hashes)
    assert keys(db) == ["da:zz-fake-painter"]
    assert add(db, username="zz-fake-nobody", links=("https://zz-fake-somewhere.art/",)) is False  # barred for later
    assert add(db, username="zz-fake-later", links=("https://zz-fake-nowhere.art/",)) is False


def test_a_deleted_cache_is_refilled_and_the_removal_still_holds(db):
    add(db, username="zz-fake-gone", links=("https://zz-fake-gone.art/",))
    store.mark_removed("da:zz-fake-gone", db)
    of_record = [row[0] for row in rows(db, "removed")]  # what the database holds: hashes only
    db.unlink()  # the local file is a cache
    assert add(db, username="zz-fake-gone", links=("https://zz-fake-gone.art/",)) is True  # nothing knows, yet
    assert store.import_removed(of_record, db) == 1
    assert rows(db) == []
    assert add(db, username="zz-fake-gone", links=("https://zz-fake-gone.art/",)) is False
    assert add(db, username="zz-fake-renamed", links=("https://zz-fake-gone.art/",)) is False


@pytest.mark.parametrize("bad", ["", "da:zz-fake-gone", "f" * 63, "f" * 65, "g" * 64, sha("da:x")[:-1] + " x"])
def test_only_sha256_digests_are_imported(db, bad):
    add(db, username="zz-fake-gone", links=("https://zz-fake-gone.art/",))
    with pytest.raises(ValueError):
        store.import_removed([sha("da:zz-fake-gone"), bad], db)
    assert rows(db, "removed") == [] and keys(db) == ["da:zz-fake-gone"]  # nothing half done


# --- the seeding rule for the crawl stage ---


def test_own_site_candidates_are_artists_with_one_front_page_of_their_own(db):
    add(db, username="zz-fake-b", links=("http://www.zz-fake-b.com/",))
    add(db, username="ZZ-Fake-A", links=("https://zz-fake-a.art/",))
    add(db, username="zz-fake-path", links=("https://zz-fake-path.art/folio",))  # not a front page
    add(db, username="zz-fake-sub", links=("https://shop.zz-fake-sub.art/",))  # a subdomain
    add(db, username="zz-fake-query", links=("https://zz-fake-query.art/?user=zz-fake",))
    add(db, username="zz-fake-platform", links=("https://www.artstation.com/zz-fake-platform",))
    add(db, username="zz-fake-two-links", links=("https://zz-fake-two.art/", "https://zz-fake-links.art/"))
    add(db, username="zz-fake-no-link", links=())
    assert store.own_site_candidates(db) == [
        ("da:zz-fake-a", "https://zz-fake-a.art/"),
        ("da:zz-fake-b", "http://www.zz-fake-b.com/"),
    ]
    assert all(crawl.is_own_site(url) for _, url in store.own_site_candidates(db))


def test_a_domain_listed_by_two_artists_is_a_platform_whatever_any_list_says(db):
    add(db, username="zz-fake-one", links=("https://zz-fake-shared.art/",))
    add(db, username="zz-fake-two", links=("http://www.zz-fake-shared.art",))  # the same registrable domain
    add(db, username="zz-fake-alone", links=("https://zz-fake-alone.art/",))
    assert store.own_site_candidates(db) == [("da:zz-fake-alone", "https://zz-fake-alone.art/")]


def test_a_domain_counts_as_shared_however_the_second_artist_links_to_it(db):
    add(db, username="zz-fake-front", links=("https://zz-fake-shared.art/",))
    add(db, username="zz-fake-deep", links=("https://users.zz-fake-shared.art/zz-fake-deep", "https://zz-fake-d.art/"))
    add(db, username="zz-fake-alone", links=("https://zz-fake-alone.co.uk/",))
    assert store.own_site_candidates(db) == [("da:zz-fake-alone", "https://zz-fake-alone.co.uk/")]


def test_nobody_on_the_removal_list_is_a_candidate_even_from_a_stale_row(db):
    add(db, username="zz-fake-a", links=("https://zz-fake-a.art/",))
    with closing(sqlite3.connect(db)) as conn, conn:  # rows that got in around the store, after their removal
        conn.execute("INSERT INTO removed VALUES (?, '')", (sha("da:zz-fake-stale"),))
        conn.execute("INSERT INTO removed VALUES (?, '')", (sha("site:zz-fake-barred.art"),))
        stale = "INSERT INTO artists VALUES (?, ?, '', ?, 3, '')"
        conn.execute(stale, ("da:zz-fake-stale", "zz-fake-stale", '["https://zz-fake-stale.art/"]'))
        conn.execute(stale, ("da:zz-fake-site", "zz-fake-site", '["https://www.zz-fake-barred.art/"]'))
    assert store.own_site_candidates(db) == [("da:zz-fake-a", "https://zz-fake-a.art/")]
