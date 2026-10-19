"""Tests for pipeline.store. Every name and URL here is invented, and nothing here touches the network.

The made-up sites sit under real suffixes (.art, .com, .co.uk): what counts as somebody's own site is decided
with the Public Suffix List, which turns .example and .test away.
"""

import hashlib
import json
import sqlite3
from contextlib import closing
from datetime import UTC, datetime, timedelta

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
    assert [key for key, _, _ in store.all_artists()] == ["da:zz-fake-painter"]
    assert store.record_listing("zz-fake-fourth", ["zz-fake-fourth.art"]) is False
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
        ("https://www.deviantart.com/ZZ-Fake-G/", "da:zz-fake-g"),
        ("https://www.deviantart.com/zz-fake-g?tab=about#top", "da:zz-fake-g"),  # the path is still /<handle>
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
    assert store.mark_removed(typed, db) == key.startswith("da:")  # a site deletes nobody, and holds nobody back
    assert store.is_removed(typed, db) and store.is_removed(key, db)
    listed = ["da:zz-fake-other"] if key.startswith("da:") else ["da:zz-fake-g", "da:zz-fake-other"]
    assert [key for key, _, _ in store.all_artists(db)] == listed
    other = ("da:zz-fake-other", "https://zz-fake-other.art/")
    assert store.own_site_candidates(db) == [other]  # it did something, only to them


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
        # a DeviantArt URL whose path is one of the site's own routes
        "https://www.deviantart.com/deviation/123456789",
        "https://www.deviantart.com/art/FAKE-TITLE-123",
        "https://www.deviantart.com/view/123456789",
        "https://www.deviantart.com/users/outgoing?https://zz-fake-g.art",
        "https://www.deviantart.com/tag/conceptart",
        "https://www.deviantart.com/gallery/zz-fake-g",
        "https://www.deviantart.com/notes/zz-fake-g",
        "https://www.deviantart.com/muro/zz-fake-g",
        "deviantart.com/commissions/zz-fake-g/",
        "da:https://www.deviantart.com/topic/zz-fake-g",
        # and any path longer than /<handle>, even one that starts with the handle: the operator types the handle
        "https://www.deviantart.com/zz-fake-g/gallery",
        "https://www.deviantart.com/zz-fake-g/art/FAKE-TITLE-123",
        "https://www.deviantart.com/zz-fake-g/gallery/?page=2",
        "https://www.deviantart.com//zz-fake-g",
        "https://www.deviantart.com/zz-fake-g//",
        "https://www.deviantart.com/zz%2Dfake%2Dg",
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


def test_removing_a_handle_bars_that_handle_and_no_site_and_touches_no_other_artist(db):
    """Round 7. A removal by handle used to bar the sites the artist listed, and whoever else listed one was then
    purged and barred: one website answered for every profile, then one removal, took six artists of six."""
    for name in ("one", "two", "three"):
        add(db, username=f"zz-fake-{name}", links=("https://zz-fake-studio.art/",))  # the same site, all three
    store.record_listing("zz-fake-two", ["users.zz-fake-studio.art", "zz-fake-second.art"], db)
    assert store.mark_removed("da:zz-fake-one", db) == 1
    assert keys(db) == ["da:zz-fake-three", "da:zz-fake-two"]
    assert [row[0] for row in rows(db, "removed")] == [sha("da:zz-fake-one")]  # the handle, and nothing else
    assert not store.is_removed("site:zz-fake-studio.art", db) and not store.is_removed("da:zz-fake-two", db)
    assert add(db, username="zz-fake-four", links=("https://www.zz-fake-studio.art/",)) is True
    assert [key for key, _, _ in store.all_artists(db)] == ["da:zz-fake-four", "da:zz-fake-three", "da:zz-fake-two"]


@pytest.mark.parametrize("how", ["mark_removed", "import_removed"])
def test_removing_a_site_removes_nobody_and_holds_back_nobody_who_links_to_it(db, how):
    """The final critic's: a site barred for the artist who proved it theirs, and named by one other, took that
    other's handle for good. A site is anybody's to name, so its removal bars the site and nobody: whoever links to
    it keeps their handle, is not deleted, and is not held back (the fifth critic's: held, they never got out). The
    site is nobody's to crawl. Another site in their row is not barred."""
    add(db, username="zz-fake-one", links=("https://zz-fake-first.art/",))
    add(db, username="zz-fake-two", links=("https://zz-fake-first.art/", "https://zz-fake-second.art/"))
    add(db, username="zz-fake-three", links=("https://zz-fake-second.art/",))
    if how == "mark_removed":
        assert store.mark_removed("site:zz-fake-first.art", db) == 0
    else:  # as a proof or the owner's grant hands it on (pipeline/removals.py)
        assert store.import_removed([sha("site:zz-fake-first.art")], db) == 0
    assert keys(db) == ["da:zz-fake-one", "da:zz-fake-three", "da:zz-fake-two"]
    assert [row[0] for row in rows(db, "removed")] == [sha("site:zz-fake-first.art")]
    assert [key for key, _, _ in store.all_artists(db)] == ["da:zz-fake-one", "da:zz-fake-three", "da:zz-fake-two"]
    assert not any(store.is_removed(f"da:zz-fake-{n}", db) for n in ("one", "two", "three"))
    assert not store.is_removed("site:zz-fake-second.art", db)


def test_site_removal_keeps_the_site_out_of_every_row_now_or_later_and_nobody_out(db):
    add(db, links=("https://www.zz-fake-painter.art/work",))
    add(db, username="zz-fake-sculptor", links=(SCULPTOR,))
    assert store.mark_removed("site:zz-fake-painter.art", db) == 0
    assert [key for key, _, _ in store.all_artists(db)] == ["da:zz-fake-painter", "da:zz-fake-sculptor"]
    assert add(db, links=("http://ZZ-FAKE-PAINTER.art/",)) is False
    assert add(db, username="zz-fake-third", links=("https://zz-fake-ok.art", "https://zz-fake-painter.art/x")) is False
    assert keys(db) == ["da:zz-fake-painter", "da:zz-fake-sculptor"]


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
    assert store.mark_removed("site:zz-fake-d.art", db) == 0
    assert [key for key, _, _ in store.all_artists(db)] == ["da:zz-fake-d", "da:zz-fake-other"]  # held back by nothing
    assert store.is_removed(link, db) and not store.keep_own_site("zz-fake-other", link, db)  # the site in no row
    assert add(db, username="zz-fake-second", links=(link,)) is False  # and after it
    assert keys(db) == ["da:zz-fake-d", "da:zz-fake-other"]


def test_site_removal_bars_no_handle_so_another_website_lists_the_artist_again(db):
    add(db, username="zz-fake-e", links=("https://zz-fake-e.art",))
    add(db, username="zz-fake-other", links=("https://zz-fake-other.art",))
    store.mark_removed("site:zz-fake-e.art", db)
    assert not store.is_removed("da:zz-fake-e", db) and not store.is_removed("da:zz-fake-other", db)
    assert add(db, username="zz-fake-e", links=("https://zz-fake-e-new.art",)) is True  # a new website
    assert [key for key, _, _ in store.all_artists(db)] == ["da:zz-fake-e", "da:zz-fake-other"]
    assert not store.is_removed("site:zz-fake-e-new.art", db)  # which is not barred for it
    assert len(rows(db, "removed")) == 1  # the site, as a hash


@pytest.mark.parametrize("stored_before", [False, True])
def test_naming_a_removed_site_changes_nothing_that_is_stored_and_bars_nobody(db, stored_before):
    """Round 7: it is one answer's word. It used to delete the artist's row and put their handle on the list."""
    if stored_before:
        add(db, username="zz-fake-e", links=("https://zz-fake-e-old.art",))
    store.mark_removed("site:zz-fake-e.art", db)
    before, barred = rows(db), rows(db, "removed")
    assert store.record_listing("zz-fake-f", ["www.artstation.com", "zz-fake-f.art"], db) is False  # never raises
    assert store.record_listing("ZZ-Fake-E", ["shop.zz-fake-e.art"], db) is True  # any host of the removed site
    assert add(db, username="zz-fake-e", links=("https://www.zz-fake-e.art./work",)) is False  # and it is not stored
    assert rows(db) == before and rows(db, "removed") == barred
    assert not store.is_removed("da:zz-fake-e", db)
    assert add(db, username="zz-fake-e", links=("https://zz-fake-e-new.art",)) is True  # an answer without it is


def test_what_profiles_list_is_kept_as_hashes_only(db):
    store.record_listing("ZZ-Fake-Lister", ["users.zz-fake-plat.art", "www.artstation.com"], db)
    store.record_listing("zz-fake-nothing", [], db)  # lists no site: nothing to keep
    assert rows(db, "listed") == [(sha("site:zz-fake-plat.art"), sha("da:zz-fake-lister"))]
    store.mark_removed("da:zz-fake-lister", db)  # a removed handle is not followed to what it listed
    assert rows(db, "removed") == [(sha("da:zz-fake-lister"), rows(db, "removed")[0][1])]
    assert b"fake-" not in db.read_bytes().lower()
    assert not hasattr(store, "site_unresolved") and not hasattr(store, "_seen")  # the cascade's machinery is gone


def test_a_hash_made_from_another_subdomain_covers_that_host_and_no_more(db):
    """What _host_keys promises, and its limit: a hash cannot be turned back into the site it belongs to."""
    add(db, username="zz-fake-front", links=("https://zz-fake-gone.art/",))
    assert store.import_removed([sha("site:shop.zz-fake-gone.art")], db) == 0  # nobody stored links to that host
    assert keys(db) == ["da:zz-fake-front"]
    assert store.is_removed("https://a.shop.zz-fake-gone.art/x", db)  # the host and what is under it
    assert not store.is_removed("site:zz-fake-gone.art", db) and not store.is_removed("site:blog.zz-fake-gone.art", db)
    assert store.import_removed([sha("site:www.zz-fake-gone.art")], db) == 0  # the www. form is the whole site
    assert store.is_removed("site:zz-fake-gone.art", db) and store.is_removed("https://blog.zz-fake-gone.art/", db)
    assert store.own_site_candidates(db) == [] and keys(db) == ["da:zz-fake-front"]


def test_a_removal_survives_an_odd_row_written_around_upsert(db):
    add(db, username="zz-fake-e", links=("https://zz-fake-e.art",))
    with closing(sqlite3.connect(db)) as conn, conn:
        conn.execute("INSERT INTO artists VALUES ('da:zz-fake-odd', 'zz-fake-odd', '', '[\"not a link\"]', 3, '')")
    assert store.mark_removed("site:zz-fake-e.art", db) == 0
    assert [key for key, _, _ in store.all_artists(db)] == ["da:zz-fake-e", "da:zz-fake-odd"]
    assert store.mark_removed("da:zz-fake-odd", db) == 1  # and a row with such a link can still be removed
    assert keys(db) == ["da:zz-fake-e"]


# --- removals from the list of record ---


def test_an_artist_removed_only_through_imported_hashes_is_deleted_and_never_readded(db):
    add(db, username="zz-fake-gone", links=("https://zz-fake-gone.art/",))
    add(db, username="zz-fake-stays", links=("https://zz-fake-stays.art/",))
    assert store.import_removed([sha("da:zz-fake-gone")], db) == 1
    assert keys(db) == ["da:zz-fake-stays"]
    assert store.is_removed("da:zz-fake-gone", db)
    assert add(db, username="ZZ-Fake-Gone", links=("https://zz-fake-gone-new.art/",)) is False
    assert not store.is_removed("site:zz-fake-gone.art", db)  # a handle bars no site, as in mark_removed
    assert keys(db) == ["da:zz-fake-stays"]
    assert b"fake-gone" not in db.read_bytes().lower()


@pytest.mark.parametrize(
    ("removed_key", "link"),
    [
        ("site:zz-fake-gone.art", "https://zz-fake-gone.art/"),  # the site key
        ("site:zz-fake-gone.art", "https://www.zz-fake-gone.art/"),
        ("site:www.zz-fake-gone.art", "https://www.zz-fake-gone.art/"),  # a key some other code made from the host
        ("site:www.zz-fake-gone.art", "https://zz-fake-gone.art/"),  # which bars the whole site, not just www.
        ("site:www.zz-fake-gone.art", "https://shop.zz-fake-gone.art/art"),
        ("site:zz-fake-gone.art", "https://shop.zz-fake-gone.art/art"),  # a parent-host key
        ("site:zz-fake-gone.co.uk", "https://a.b.zz-fake-gone.co.uk/"),
    ],
)
def test_an_imported_site_hash_bars_that_site_and_nobody_who_links_to_it(db, removed_key, link):
    add(db, username="zz-fake-gone", links=(link,))
    add(db, username="zz-fake-stays", links=("https://notzz-fake-gone.art/",))
    assert store.import_removed([sha(removed_key).upper()], db) == 0  # hex in either case
    assert store.is_removed(link, db) and len(store.all_artists(db)) == 2  # nobody held back
    assert not store.is_removed("https://notzz-fake-gone.art/", db)
    assert not store.is_removed("da:zz-fake-gone", db)  # their handle is not barred: anybody can name a site
    assert add(db, username="zz-fake-other", links=(link,)) is False
    assert add(db, username="zz-fake-gone", links=("https://zz-fake-gone-new.art/",)) is True


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
    add(db, username="zz-fake-sited", links=("https://zz-fake-sited.art/",))
    store.mark_removed("da:zz-fake-gone", db)
    store.mark_removed("site:zz-fake-sited.art", db)
    of_record = [row[0] for row in rows(db, "removed")]  # what the database holds: hashes only
    db.unlink()  # the local file is a cache
    assert add(db, username="zz-fake-gone", links=("https://zz-fake-gone.art/",)) is True  # nothing knows, yet
    assert store.import_removed(of_record, db) == 1
    assert rows(db) == []
    assert add(db, username="zz-fake-gone", links=("https://zz-fake-gone.art/",)) is False
    assert add(db, username="zz-fake-sited", links=("https://zz-fake-sited.art/",)) is False  # the site, still
    assert add(db, username="zz-fake-renamed", links=("https://zz-fake-sited.art/",)) is False


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


def test_a_domain_counts_as_shared_when_another_profile_lists_it_in_a_link_that_was_dropped(db):
    add(db, username="zz-fake-front", links=("https://zz-fake-shared.art/",))
    add(db, username="zz-fake-alone", links=("https://zz-fake-alone.co.uk/",))
    assert [key for key, _ in store.own_site_candidates(db)] == ["da:zz-fake-alone", "da:zz-fake-front"]
    store.record_listing("zz-fake-front", ["www.zz-fake-shared.art"], db)  # the same artist again: still one listing
    assert [key for key, _ in store.own_site_candidates(db)] == ["da:zz-fake-alone", "da:zz-fake-front"]
    store.record_listing("zz-fake-deep", ["users.zz-fake-shared.art"], db)  # what discovery records for a dropped link
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


# --- a cache written before listings were kept: artists, and no `listed` rows ---


def old_rows(db, *artists):
    """Rows as an earlier version of the store left them: put in directly, with nothing in `listed`."""
    add(db, username="zz-fake-new", links=("https://zz-fake-new.art/",))  # creates the tables
    with closing(sqlite3.connect(db)) as conn, conn:
        for name, link in artists:
            conn.execute("INSERT INTO artists VALUES (?, ?, '', ?, 3, '')", (f"da:{name}", name, json.dumps([link])))


def test_a_removal_reaches_the_row_of_an_artist_that_has_no_listing(db):
    old_rows(db, ("zz-fake-old-c", "https://zz-fake-old-c.art/"), ("zz-fake-old-d", "https://www.zz-fake-old-d.art/"))
    assert store.mark_removed("da:zz-fake-old-c", db) == 1
    assert not store.is_removed("site:zz-fake-old-c.art", db)  # removed by handle: the site in their row is not
    assert store.mark_removed("site:zz-fake-old-d.art", db) == 0
    assert not store.is_removed("da:zz-fake-old-d", db)  # removed by site: nobody's handle goes with it
    assert [key for key, _, _ in store.all_artists(db)] == ["da:zz-fake-new", "da:zz-fake-old-d"]  # nor anybody
    assert store.own_site_candidates(db) == [("da:zz-fake-new", "https://zz-fake-new.art/")]  # only the site goes


def test_a_domain_two_old_rows_share_is_not_a_candidate(db):
    old_rows(db, ("zz-fake-old-a", "https://zz-fake-old.art/"), ("zz-fake-old-b", "https://www.zz-fake-old.art/"))
    old_rows(db, ("zz-fake-old-c", "https://zz-fake-old-c.art/"))
    assert [key for key, _ in store.own_site_candidates(db)] == ["da:zz-fake-new", "da:zz-fake-old-c"]


# --- what the collect stage and discovery ask of the rows (round 8) ---


def test_an_own_site_the_collect_stage_keeps_is_written_into_the_row_where_a_removal_of_it_looks(db):
    """The critic's finding: discovery saw no site, the artist added one, and a rerun kept it with the profile
    only. The removal of that site then returned 0 and deleted nobody."""
    add(db, username="zz-fake-late", links=())
    add(db, username="zz-fake-other", links=("https://zz-fake-other.art/",))
    assert store.keep_own_site("ZZ-Fake-Late", "https://zz-fake-late.art/", db) is True
    assert [json.loads(row[3]) for row in rows(db)] == [["https://zz-fake-late.art/"], ["https://zz-fake-other.art/"]]
    assert [key for key, _ in store.own_site_candidates(db)] == ["da:zz-fake-late", "da:zz-fake-other"]
    profile_on_a_platform = "https://www.artstation.com/zz-fake-late"
    for no_own_site in (
        None,
        "",
        "FAKE",
        profile_on_a_platform,
        "https://zz-fake-b.art/folio",
        "https://shop.zz-fake-b.art/",
    ):
        assert store.keep_own_site("zz-fake-late", no_own_site, db) is False
    assert store.keep_own_site("zz-fake-nobody", "https://zz-fake-nobody.art/", db) is False  # no row, and none is made
    assert [json.loads(row[3]) for row in rows(db)] == [["https://zz-fake-late.art/"], ["https://zz-fake-other.art/"]]
    assert store.mark_removed("site:zz-fake-late.art", db) == 0 and not store.is_removed("da:zz-fake-late", db)
    assert [key for key, _, _ in store.all_artists(db)] == ["da:zz-fake-late", "da:zz-fake-other"]  # held by nothing
    assert [key for key, _ in store.own_site_candidates(db)] == ["da:zz-fake-other"]  # and the site crawled by nobody
    # A removed site is written into nobody's row, and nothing into the row of somebody who is removed.
    assert store.keep_own_site("zz-fake-other", "https://www.zz-fake-late.art/", db) is False
    store.mark_removed("da:zz-fake-late", db)
    assert store.keep_own_site("zz-fake-late", "https://zz-fake-late-new.art/", db) is False
    assert [(row[0], json.loads(row[3])) for row in rows(db)] == [("da:zz-fake-other", ["https://zz-fake-other.art/"])]
    with pytest.raises(ValueError):
        store.keep_own_site("zz fake", "https://zz-fake-late.art/", db)


def test_discovery_stores_an_artist_without_their_links_and_the_link_a_row_has_stays(db):
    """Discovery reads no profile, so it knows no link. It must not blank the one the collect stage keeps."""
    add(db, username="zz-fake-old", hits=3)  # with the painter's site as their link
    with closing(sqlite3.connect(db)) as conn, conn:
        conn.execute("UPDATE artists SET discovered_at = '2020-01-01T00:00:00+00:00'")
    assert store.upsert_artist("ZZ-Fake-Old", "https://www.deviantart.com/ZZ-Fake-Old", None, 9, db) is True
    assert store.upsert_artist("zz-fake-new", "https://www.deviantart.com/zz-fake-new", None, 3, db) is True
    assert [(row[0], row[1], json.loads(row[3]), row[4]) for row in rows(db)] == [
        ("da:zz-fake-new", "zz-fake-new", [], 3),
        ("da:zz-fake-old", "ZZ-Fake-Old", [PAINTER], 9),  # the handle and the hits brought up to date, the link kept
    ]
    assert rows(db)[1][5] == "2020-01-01T00:00:00+00:00"
    store.mark_removed("da:zz-fake-new", db)
    assert store.upsert_artist("zz-fake-new", "https://www.deviantart.com/zz-fake-new", None, 4, db) is False
    assert keys(db) == ["da:zz-fake-old"]


def test_the_tally_counts_each_work_once_and_holds_nothing_but_hashes(db):
    page = [
        ("FAKE-WORK-1", "ZZ-Fake-A"),
        ("FAKE-WORK-2", "zz-fake-a"),
        ("FAKE-WORK-3", None),
        ("FAKE-WORK-4", "zz-fake-b"),
    ]
    assert store.count_works(page, db) == {"ZZ-Fake-A": 2, "zz-fake-a": 2, "zz-fake-b": 1}  # a handle in any case
    # Work 1 is under another tag too, and work 3 was counted for nobody the first time it was seen.
    again = [("FAKE-WORK-1", "zz-fake-a"), ("FAKE-WORK-5", "zz-fake-a"), ("FAKE-WORK-3", "zz-fake-b")]
    assert store.count_works(again, db) == {"zz-fake-a": 3, "zz-fake-b": 1}
    assert store.count_works([], db) == {}
    assert rows(db, "counted") == sorted(
        [
            (sha("work:FAKE-WORK-1"), sha("da:zz-fake-a")),
            (sha("work:FAKE-WORK-2"), sha("da:zz-fake-a")),
            (sha("work:FAKE-WORK-3"), None),
            (sha("work:FAKE-WORK-4"), sha("da:zz-fake-b")),
            (sha("work:FAKE-WORK-5"), sha("da:zz-fake-a")),
        ]
    )
    assert b"fake" not in db.read_bytes().lower()  # no id and no handle: nobody is written down by name
    with pytest.raises(ValueError):
        store.count_works([("FAKE-WORK-9", "zz fake")], db)
    assert len(rows(db, "counted")) == 5


def test_the_tally_in_numbers_says_who_has_works_enough_and_what_became_of_them(db):
    """Pages, works counted, mature works, authors, and of the authors with works enough: all, the removed, the
    ones not stored yet, the stored. The last three add up to the one before them."""
    assert store.tally(3, db) == (0, 0, 0, 0, 0, 0, 0, 0)
    for author, works in (("zz-fake-a", 4), ("zz-fake-b", 3), ("zz-fake-c", 3), ("zz-fake-d", 2), (None, 2)):
        store.count_works([(f"FAKE-WORK-{author}-{n}", author) for n in range(works)], db)
    store.note_browsed("zz-fake-tag", 50, db)
    assert store.tally(3, db) == (1, 12, 2, 4, 3, 0, 3, 0)
    assert store.tally(4, db)[4:] == (1, 0, 1, 0) and store.tally(5, db)[4:] == (0, 0, 0, 0)
    add(db, username="ZZ-Fake-A", links=())
    add(db, username="zz-fake-d", links=())  # stored some other time: not one of this pass's candidates
    assert store.tally(3, db) == (1, 12, 2, 4, 3, 0, 2, 1)
    store.mark_removed("da:zz-fake-b", db)  # never stored
    store.mark_removed("da:zz-fake-a", db)  # stored, and gone with the removal
    assert store.tally(3, db) == (1, 12, 2, 4, 3, 2, 1, 0)


def test_a_browsing_pass_is_saved_tag_by_tag_and_forgotten_whole_when_its_first_page_is_too_old(db):
    now = datetime.now(UTC)
    assert store.browsing_pass(now, db) == {}
    store.note_browsed("zz-fake-tag", 40, db)
    store.note_browsed("zz-fake-tag", 80, db)
    store.note_browsed("zz-fake-other", None, db)  # the API said this tag has no more
    store.count_works([("FAKE-WORK-1", "zz-fake-a")], db)
    add(db, username="zz-fake-a", links=())
    saved = {"zz-fake-tag": (80, 2), "zz-fake-other": (None, 1)}
    assert store.browsing_pass(now - timedelta(days=1), db) == saved and store.tally(1, db)[:2] == (3, 1)
    with closing(sqlite3.connect(db)) as conn, conn:  # one tag was begun a week before the other
        conn.execute(
            "UPDATE browsed SET began_at = ? WHERE tag = 'zz-fake-other'", ((now - timedelta(days=7)).isoformat(),)
        )
    assert store.browsing_pass(now - timedelta(days=8), db) == saved
    assert store.browsing_pass(now - timedelta(days=6), db) == {}  # a pass is as old as its first page
    assert rows(db, "browsed") == rows(db, "counted") == [] and store.tally(1, db) == (0,) * 8
    assert keys(db) == ["da:zz-fake-a"]  # what is forgotten is the pass: nobody who is stored goes with it


def test_only_a_browsing_pass_adds_its_two_tables_to_a_store_and_it_changes_no_other(db):
    """No table of the store changes shape, and none is added, just because some other stage ran."""

    def made():
        with closing(sqlite3.connect(db)) as conn:
            return dict(conn.execute("SELECT name, sql FROM sqlite_master WHERE type = 'table'"))

    add(db)
    store.mark_removed("da:zz-fake-someone", db)
    store.import_removed([sha("da:zz-fake-other")], db)
    store.record_listing("zz-fake-painter", ["zz-fake-painter.art"], db)
    store.keep_own_site("zz-fake-painter", PAINTER, db)
    assert store.all_artists(db) and store.own_site_candidates(db) and store.is_removed("zz-fake-someone", db)
    before = made()
    assert set(before) == {"artists", "removed", "listed"}
    assert store.browsing_pass(datetime.now(UTC), db) == {}  # discovery, at the start of a run
    assert set(made()) == {"artists", "removed", "listed", "browsed", "counted"}
    assert {name: sql for name, sql in made().items() if name in before} == before


# --- the list for the collect stage ---


def test_all_artists_are_the_stored_artists_with_or_without_a_link_in_key_order(db):
    add(db, username="zz-fake-b", links=())  # an own site is optional
    add(db, username="ZZ-Fake-A", links=("https://zz-fake-a.art/",))
    add(db, username="zz-fake-c", links=("https://www.artstation.com/zz-fake-c",))
    assert store.all_artists(db) == [
        ("da:zz-fake-a", "ZZ-Fake-A", "https://www.deviantart.com/ZZ-Fake-A"),
        ("da:zz-fake-b", "zz-fake-b", "https://www.deviantart.com/zz-fake-b"),
        ("da:zz-fake-c", "zz-fake-c", "https://www.deviantart.com/zz-fake-c"),
    ]
    assert store.own_site_candidates(db) == [("da:zz-fake-a", "https://zz-fake-a.art/")]  # fewer rows than artists


def test_all_artists_leaves_out_whoever_is_removed_by_handle_and_nobody_for_a_site(db):
    """The fifth critic's: an artist whose row links a removed site was left out here, so the collect stage never
    read their profile again, and one who had changed their website stayed out for good."""
    add(db, username="zz-fake-a", links=("https://zz-fake-a.art/",))
    add(db, username="zz-fake-b", links=())
    add(db, username="zz-fake-c", links=())
    store.mark_removed("site:zz-fake-a.art", db)
    store.mark_removed("da:zz-fake-b", db)
    assert [key for key, _, _ in store.all_artists(db)] == ["da:zz-fake-a", "da:zz-fake-c"]
    assert add(db, username="zz-fake-b", links=()) is False  # and they cannot come back, link or no link
    assert store.keep_own_site("zz-fake-a", "https://zz-fake-a-new.art/", db)  # a new website, read on their turn
    assert store.own_site_candidates(db) == [("da:zz-fake-a", "https://zz-fake-a-new.art/")]


def test_all_artists_never_hands_on_a_row_that_outlived_its_removal(db):
    add(db, username="zz-fake-a", links=())
    with closing(sqlite3.connect(db)) as conn, conn:  # rows that got in around the store, after their removal
        conn.execute("INSERT INTO removed VALUES (?, '')", (sha("da:zz-fake-stale"),))
        conn.execute("INSERT INTO removed VALUES (?, '')", (sha("site:zz-fake-barred.art"),))
        stale = "INSERT INTO artists VALUES (?, ?, 'FAKE-URL', ?, 3, '')"
        conn.execute(stale, ("da:zz-fake-stale", "zz-fake-stale", "[]"))
        conn.execute(stale, ("da:zz-fake-site", "zz-fake-site", '["https://shop.zz-fake-barred.art/x"]'))
    assert [key for key, _, _ in store.all_artists(db)] == ["da:zz-fake-a", "da:zz-fake-site"]  # a site bars nobody


def test_what_a_removal_would_bar_can_be_asked_before_it_is_written_and_asking_changes_nothing(db):
    add(db, username="zz-fake-a", links=("https://shop.zz-fake-a.art/x",))
    add(db, username="zz-fake-b", links=())
    add(db, username="zz-fake-c", links=())
    hashes, before = {sha("site:zz-fake-a.art"), sha("da:zz-fake-b")}, db.read_bytes()
    assert store.named_by(hashes, db) == ["da:zz-fake-b"]  # a site names nobody, though zz-fake-a links to it
    assert store.covers(hashes, "da:ZZ-Fake-B") and store.covers(hashes, "https://www.zz-fake-a.art/")
    assert not store.covers(hashes, "da:zz-fake-a") and not store.covers(hashes, "zz-fake-c")  # a site names no handle
    assert not store.covers((), "da:zz-fake-b")
    with pytest.raises(ValueError):
        store.covers(hashes, "FAKE: not a handle and not a host")
    assert db.read_bytes() == before and not store.is_removed("da:zz-fake-b", db)
    assert store.import_removed(hashes, db) == 1 and keys(db) == ["da:zz-fake-a", "da:zz-fake-c"]  # what a removal does
    assert store.named_by(hashes, db) == [] and [key for key, _, _ in store.all_artists(db)] == keys(db)
