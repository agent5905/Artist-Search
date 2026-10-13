"""Live tests for the schema, the search functions and the removal flow.

    uv run pytest tests/test_db.py --basetemp=.pytest_tmp_db
    RUN_SCALE=1 uv run pytest tests/test_db.py --basetemp=.pytest_tmp_db -s -k full_scale

These run against the real Supabase project. Every artist they write has a slug starting with
zz-test-, and the module fixture deletes them before and after, pass or fail.
"""

import hashlib
import os
import random
import re
import secrets
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Any
from urllib.parse import urlsplit

import httpx
import pytest

from pipeline import db
from pipeline.config import ROOT, load_env

NEEDED = (
    "SUPABASE_PROJECT_REF",
    "SUPABASE_ACCESS_TOKEN",
    "VITE_SUPABASE_URL",
    "VITE_SUPABASE_PUBLISHABLE_KEY",
    "SUPABASE_SECRET_KEY",
)
if not (ROOT / ".env").exists() or not all(load_env().get(name) for name in NEEDED):
    pytest.skip(".env lacks the Supabase keys", allow_module_level=True)

TABLES = ("artists", "works", "artist_styles", "search_words", "removal_requests", "removal_secret", "removed")
PUBLIC_FUNCTIONS = ["get_artist", "request_removal", "search_artists", "similar_artists", "start_removal_proof"]
SERVICE_FUNCTIONS = [
    "artist_ids",
    "confirm_removal",
    "delete_removed",
    "purge_removed",
    "refresh_search_words",
    "refresh_style_centroids",
    "reject_removal",
    "remove_artists",
]
HELPER_FUNCTIONS = [
    "artist_card",
    "artist_fts",
    "artist_keys",
    "artists_before_write",
    "artists_not_removed",
    "clean_url",
    "host_under",
    "key_hash",
    "query_embedding",
    "removal_code",
    "removed_after_insert",
    "short_list",
    "url_host",
    "works_before_write",
]
FILTER_CAP = 20  # search_artists reads this many values of a filter list
OFFSET_CAP = 1000  # and starts a page no deeper than this
SIMILAR_CAP = 24  # similar_artists returns at most this many
PUBLIC_TIMEOUT_MS = 3000  # the anon role's statement_timeout
CODE = re.compile(r"artist-search-removal-[0-9a-f]{32}")  # what start_removal_proof hands out

WORD = "zzqxglorp"  # in no real profile, so keyword searches for it see only synthetic artists
WORD2 = "zzqxblend"  # the same, for the artists that are not analyzed yet
MEDIUM = "zz-test-medium"  # on every analyzed search artist; filtering on it keeps real artists out of a ranking
MIXED = "zz-test-mixed"  # on analyzed and unanalyzed artists together
TYPO = "zz-test-typo"  # on the two artists the misspelled query is tried on


def vec(size: int, *head: float) -> list[float]:
    return [*map(float, head), *[0.0] * (size - len(head))]


def digest(key: str) -> str:
    return hashlib.sha256(key.encode()).hexdigest()


def artist(key: str, **fields: Any) -> dict[str, Any]:
    return {
        "slug": f"zz-test-{key}",
        "name": key.title(),
        "da_username": f"zz-test-{key}",
        "site_url": None,
        "site_domain": None,
        "tagline": None,
        "top_tags": [],
        "search_text": None,
        "profile": None,
        "specialties": [],
        "mediums": [],
        "open_for_work": None,
        "profile_embedding": None,
        "hidden": False,
        **fields,
    }


def site(key: str) -> dict[str, str]:
    """An artist's own site and its registrable domain, as the push stage supplies them."""
    return {"site_url": f"https://www.zz-test-{key}.invalid/", "site_domain": f"zz-test-{key}.invalid"}


QUERY = vec(1024, 1)  # the search embedding. Cosine to it: beta 1.0, gamma 0.8, alpha 0.2.
ARTISTS = [
    artist(
        "alpha",
        name=f"{WORD} Alpha {WORD2}",  # a word in the name outranks the same word in a profile
        **site("alpha"),
        tagline="Armor and creatures",
        top_tags=["zz-armor", "zz-knight"],
        profile=f"{WORD} armor {WORD2}",
        mediums=[MEDIUM, MIXED, "zz-test-digital"],
        specialties=["zz-test-character"],
        open_for_work=True,
        profile_embedding=vec(1024, 0.2, 1),
    ),
    artist(
        "beta",
        profile=f"{WORD} creatures {WORD2}",
        mediums=[MEDIUM, MIXED, "zz-test-digital"],
        specialties=["zz-test-creature"],
        open_for_work=False,
        profile_embedding=vec(1024, 1),
    ),
    artist(
        "gamma",
        profile="environment paintings",
        mediums=[MEDIUM, MIXED, "zz-test-traditional"],
        specialties=["zz-test-character"],
        open_for_work=None,
        profile_embedding=vec(1024, 0.8, 0.6),
    ),
    artist("delta"),
    artist("epsilon"),
    artist(  # the best match for every search and every style, and it must never show
        "hidden",
        name=f"{WORD} {WORD} {WORD2} Hidden",
        profile=f"{WORD} {WORD} {WORD2}",
        mediums=[MEDIUM, MIXED, TYPO, "zz-test-digital", "zz-test-traditional"],
        specialties=["zz-test-character", "zz-test-creature"],
        tagline="character concept design artist zzqxsecretword",
        open_for_work=True,
        profile_embedding=QUERY,
        hidden=True,
    ),
    # Loaded but not analyzed yet: no profile vector, no style vectors. They are found by their own words.
    artist("dana", name=f"{WORD2} Dana", tagline=f"{WORD2} painter", mediums=[MIXED]),
    artist("drew", search_text=f"studies {WORD2} sketches", mediums=[MIXED]),
    # For the misspelled query: one artist it is really about, one who only matches the word spelled right.
    artist("cora", tagline="character concept design", mediums=[TYPO]),
    artist("arti", tagline="artist artist artist", mediums=[TYPO]),
]
BY_SLUG = {row["slug"]: row for row in ARTISTS}

E1, E2, E3, E4, BLEND = vec(768, 1), vec(768, 0, 1), vec(768, 0, 0, 1), vec(768, 0, 0, 0, 1), vec(768, 1, 1)
STYLES: dict[str, list[list[float] | None]] = {  # the style vector of each work, in display order
    "alpha": [E1, E2, *[None] * 8, E3],  # the seed. Its eleventh work is past the cap and must not count.
    "beta": [E1, E2],  # matches both seed works: 1.0
    "gamma": [BLEND],  # halfway to each seed work: 0.71
    "delta": [E1, E3],  # matches one seed work and misses the other: 0.5 (0.67 if the seed's eleventh counted)
    "epsilon": [*[E4] * 10, E1, E2],  # ten works that match nothing, then two perfect ones past the cap: 0.0
    "hidden": [E1, E2],
    "dana": [None, None],  # not analyzed yet
}
ON_OWN_SITE = {"alpha", "gone", "keep"}  # whose works are shown on their own site; the others' on DeviantArt

# The removal tests' artists. GONE is removed: a port and a path on a www host show what the site key is not made from.
GONE = artist(
    "gone",
    da_username="ZZ-Test-Gone",
    site_url="https://www.zz-test-gone.invalid:8443/art?p=1",
    site_domain="zz-test-gone.invalid",
    tagline="zzqxgoneword paintings",
)
GONE_HASHES = sorted(map(digest, ("da:zz-test-gone", "site:zz-test-gone.invalid")))
STAY = artist("stay", **site("stay"))
KEEP = artist("keep", tagline="zzqxkeepword studies", **site("keep"))
RACE = artist("race")
HELD = artist("held")
CROWD = [artist(f"crowd-{n:02d}") for n in range(30)]


def key_hashes(row: dict[str, Any]) -> list[str]:
    keys = [f"da:{row['da_username'].lower()}", *([f"site:{row['site_domain']}"] if row["site_domain"] else [])]
    return [digest(key) for key in keys]


# Every key a removal in these tests could leave behind, so cleaning up never depends on a test having passed.
FORGET = "delete from public.removed where key_hash in ({})".format(
    ", ".join(
        f"'{key_hash}'" for row in (*ARTISTS, GONE, STAY, KEEP, RACE, HELD, *CROWD) for key_hash in key_hashes(row)
    )
)

COUNTS = """
select (select count(*) from public.artists where slug like 'zz-test-%')::int as artists,
       (select count(*) from public.works w join public.artists a on a.id = w.artist_id
        where a.slug like 'zz-test-%')::int as works
"""
SEEDED = [{"artists": len(ARTISTS), "works": sum(map(len, STYLES.values()))}]


def thumbnail(name: str, position: int, token: str = "A") -> str:
    """A DeviantArt thumbnail URL in shape and length: a long signed token in the query (about 1,000 characters)."""
    signed = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9." + token * 760 + "." + "s" * 43
    return (
        "https://images-wixmp-ed30a86b8c4ca887773594c2.wixmp.com/f/0a1b2c3d-4e5f-6071-8293-a4b5c6d7e8f9/"
        f"dzz{position}-0a1b2c3d.jpg/v1/fill/w_300,h_300,q_70,strp/zz_test_{position}_by_zz_test_{name}.jpg?token={signed}"
    )


def works_of(name: str, styles: list[list[float] | None], **owner: Any) -> list[dict[str, Any]]:
    """An artist's works, each with `owner` added: artist_slug for db.load, artist_id for db.upsert."""
    # In reverse display order, so anything sorted by id instead of position would fail.
    own_site = name in ON_OWN_SITE
    return [
        {
            **owner,
            "source": "site" if own_site else "deviantart",
            "page_url": (
                f"https://zz-test-{name}.invalid/work/{position}"
                if own_site
                else f"https://www.deviantart.com/zz-test-{name}/art/Zz-Test-{position}-9000000{position}"
            ),
            "image_url": (
                f"https://zz-test-{name}.invalid/thumb/{position}.jpg" if own_site else thumbnail(name, position)
            ),
            "title": f"{name} {position}",
            "style_embedding": styles[position],
            "position": position,
        }
        for position in reversed(range(len(styles)))
    ]


WORKS = [row for name, styles in STYLES.items() for row in works_of(name, styles, artist_slug=f"zz-test-{name}")]


def seed() -> dict[str, int]:
    db.load(ARTISTS, WORKS)  # the push stage's way in
    return db.rpc("artist_ids", {"p_slugs": list(BY_SLUG)}, "secret")


def clean() -> None:
    db.sql(f"""
        delete from public.artists where slug like 'zz-test-%';
        delete from public.removal_requests where artist_slug like 'zz-test-%';
        {FORGET};
        select public.refresh_search_words();  -- so the synthetic artists' words go with them
    """)


@pytest.fixture(scope="module", autouse=True)
def seeded():
    db.migrate()
    clean()  # in case an earlier run was killed
    try:
        yield seed()
    finally:
        clean()


def slugs(rows: list[dict[str, Any]]) -> list[str]:
    return [row["slug"] for row in rows]


def search(**args: Any) -> list[str]:
    found = slugs(db.rpc("search_artists", {"q": "", **args}))
    assert "zz-test-hidden" not in found
    return found


def visible(row: dict[str, Any]) -> bool:
    return slugs(db.rpc("get_artist", {"p_slug": row["slug"]})) == [row["slug"]]


def denied(response: httpx.Response) -> bool:
    """Postgres refused for lack of privilege. A wrong name or signature would be a 404, which proves nothing."""
    return response.status_code in (401, 403) and response.json()["code"] == "42501"


def refused(response: httpx.Response, reason: str) -> bool:
    """The database refused the row, and for the reason the test means (a constraint's name, or the message)."""
    return response.status_code in (400, 409) and reason in response.text


# 1. migrate twice: no error, no change.

SNAPSHOT = """
select jsonb_build_object(
  'relations', (select jsonb_agg(jsonb_build_array(relname, oid, relrowsecurity, relacl::text) order by relname)
                from pg_class where relnamespace = 'public'::regnamespace and relkind in ('r', 'i', 'S')),
  'columns', (select jsonb_agg(jsonb_build_array(
                       c.relname, a.attnum, a.attname, format_type(a.atttypid, a.atttypmod),
                       a.attnotnull, a.attstorage, a.attgenerated, pg_get_expr(d.adbin, d.adrelid)
                     ) order by c.relname, a.attnum)
              from pg_attribute a
              join pg_class c on c.oid = a.attrelid
              left join pg_attrdef d on d.adrelid = a.attrelid and d.adnum = a.attnum
              where c.relnamespace = 'public'::regnamespace and c.relkind = 'r'
                and a.attnum > 0 and not a.attisdropped),
  'indexes', (select jsonb_agg(indexdef order by indexname) from pg_indexes where schemaname = 'public'),
  'constraints', (select jsonb_agg(jsonb_build_array(conname, oid, pg_get_constraintdef(oid)) order by conname)
                  from pg_constraint where connamespace = 'public'::regnamespace),
  'functions', (select jsonb_agg(jsonb_build_array(
                         proname, oid, pg_get_functiondef(oid), proacl::text
                       ) order by proname)
                from pg_proc where pronamespace = 'public'::regnamespace),
  'triggers', (select jsonb_agg(jsonb_build_array(t.oid, pg_get_triggerdef(t.oid)) order by t.tgname)
               from pg_trigger t join pg_class c on c.oid = t.tgrelid
               where c.relnamespace = 'public'::regnamespace and not t.tgisinternal),
  'policies', (select count(*) from pg_policies where schemaname = 'public'),
  'rows', jsonb_build_array((select count(*) from public.artists), (select count(*) from public.works),
                            (select count(*) from public.removal_requests), (select count(*) from public.removed),
                            (select count(*) from public.removal_secret))
) as snapshot
"""


def test_migrate_twice_changes_nothing():
    before = db.sql(SNAPSHOT)
    names = {function[0] for function in before[0]["snapshot"]["functions"]}
    assert names >= {*PUBLIC_FUNCTIONS, *SERVICE_FUNCTIONS, *HELPER_FUNCTIONS}  # the snapshot really sees the schema
    code = db.rpc("start_removal_proof", {"p_slug": "zz-test-alpha"})
    db.migrate()
    db.migrate()
    # Same objects (same oids, so nothing was dropped and made again) and same rows.
    assert db.sql(SNAPSHOT) == before
    # And the same secret: a code an artist was told before a migration is still their code after it.
    assert db.rpc("start_removal_proof", {"p_slug": "zz-test-alpha"}) == code


# 2. Load the same artists and works twice: no duplicates.

VERSIONS = """
select md5(string_agg(w.id || ':' || w.xmin::text || ':' || w.ctid::text, ',' order by w.id)) as of_every_work,
       (select md5(string_agg(id || ':' || xmin::text || ':' || ctid::text, ',' order by id))
        from public.artists where slug like 'zz-test-%') as of_every_artist,
       (select md5(string_agg(s.artist_id || ':' || s.xmin::text, ',' order by s.artist_id))
        from public.artist_styles s) as of_every_style_row
from public.works w join public.artists a on a.id = w.artist_id
where a.slug like 'zz-test-%'
"""


def test_loading_twice_makes_no_duplicates(seeded):
    assert db.sql(COUNTS) == SEEDED
    versions = db.sql(VERSIONS)
    assert seed() == seeded  # the same rows, under the same ids
    assert db.sql(COUNTS) == SEEDED
    assert db.sql(VERSIONS) == versions  # and none was rewritten: every row version is the one from before
    assert db.upsert("artists", ARTISTS, "slug", "slug") == []  # an upsert returns only what it wrote
    assert db.rpc("refresh_search_words", {}, "secret") == 0

    edited = {**BY_SLUG["zz-test-delta"], "profile": "edited"}
    changed = db.upsert("artists", [edited], "slug", "id,profile")
    assert changed == [{"id": seeded["zz-test-delta"], "profile": "edited"}]  # duplicates are merged, not skipped
    assert db.upsert("artists", [], "slug") == []

    # A work that did change is written and returned; sent again, it is left alone.
    [retitled, *_] = works_of("delta", STYLES["delta"], artist_id=seeded["zz-test-delta"])
    retitled["title"] = "retitled"
    assert db.upsert("works", [retitled], "artist_id,key", "title") == [{"title": "retitled"}]
    assert db.upsert("works", [retitled], "artist_id,key", "title") == []
    assert db.sql(VERSIONS) != versions
    assert db.sql(COUNTS) == SEEDED


def test_load_goes_batch_by_batch(monkeypatch):
    vacuums = []
    monkeypatch.setattr(db, "LOAD_BATCH", 7)
    monkeypatch.setattr(db, "VACUUM_EVERY", 2)
    monkeypatch.setattr(db, "LOAD_REST", 0.0)
    monkeypatch.setattr(db, "vacuum_analyze", lambda analyze=True: vacuums.append(analyze))
    crowd_works = [
        row
        for person in CROWD
        for row in works_of(person["slug"].removeprefix("zz-test-"), [E1], artist_slug=person["slug"])
    ]
    try:
        db.load(CROWD, crowd_works)
        # Thirty artists in batches of seven: five batches, a plain vacuum after the second and the fourth,
        # and one with statistics to finish.
        assert vacuums == [False, False, True]
        [loaded] = db.sql("""
            select count(*)::int as artists, count(s.artist_id)::int as with_style_rows,
                   (select count(*) from public.works w join public.artists c on c.id = w.artist_id
                    where c.slug like 'zz-test-crowd-%')::int as works
            from public.artists a left join public.artist_styles s on s.artist_id = a.id
            where a.slug like 'zz-test-crowd-%'
        """)
        assert loaded == {"artists": 30, "with_style_rows": 30, "works": 30}
        # Their style rows were made batch by batch, so they are found by style without any other step.
        assert "zz-test-crowd-01" in slugs(db.rpc("similar_artists", {"p_slug": "zz-test-crowd-00"}))
    finally:
        db.sql("delete from public.artists where slug like 'zz-test-crowd-%'")


def test_deviantart_work_is_the_same_work_under_a_new_thumbnail(seeded):
    # A DeviantArt thumbnail URL is long and carries a signed token that changes from one load to the
    # next. The deviation page is what identifies the work, so the row is updated, not doubled.
    dana = seeded["zz-test-dana"]
    [work] = [row for row in works_of("dana", STYLES["dana"], artist_id=dana) if row["position"] == 0]
    longer = (thumbnail("dana", 0, token="B") + "&pad=").ljust(2040, "x")  # near the 2,048 limit, and accepted
    assert db.upsert("works", [{**work, "image_url": longer}], "artist_id,key", "image_url") == [{"image_url": longer}]
    assert db.sql(f"select count(*)::int as n from public.works where artist_id = {dana}") == [{"n": 2}]
    [profile] = db.rpc("get_artist", {"p_slug": "zz-test-dana"})
    assert [(shown["source"], shown["image_url"] == longer) for shown in profile["works"]] == [
        ("deviantart", True),
        ("deviantart", False),
    ]
    assert profile["works"][0]["page_url"] == "https://www.deviantart.com/zz-test-dana/art/Zz-Test-0-90000000"


def test_one_row_per_handle_and_per_site(seeded):
    # No two rows can share a removal key, so a removal never leaves a second copy of the artist behind.
    def post(row: dict[str, Any]) -> httpx.Response:
        return db.data_api("POST", "artists", "secret", json=row)

    assert refused(post(artist("twin", da_username="ZZ-TEST-ALPHA")), "artists_da_key_idx")
    shop = {"site_url": "http://shop.zz-test-alpha.invalid:8080/me", "site_domain": "zz-test-alpha.invalid"}
    assert refused(post(artist("twin", **shop)), "artists_site_key_idx")
    assert db.sql(COUNTS) == SEEDED


# 3. search_artists.


def test_search_by_keyword_only():
    # Alpha has the word in its name and profile, beta in its profile, gamma nowhere.
    assert search(q=WORD) == ["zz-test-alpha", "zz-test-beta"]
    assert search(q=f"{WORD} armor painterly", p_mediums=[MEDIUM]) == ["zz-test-alpha", "zz-test-beta"]
    assert search(q="zzqxnothing") == []


def test_search_by_keyword_and_embedding():
    # Keywords rank alpha, beta. Meaning ranks beta, gamma, alpha. Reciprocal rank fusion:
    # beta 1/62 + 1/61, alpha 1/61 + 1/63, gamma 1/62 (found by meaning alone).
    ranked = search(q=WORD, q_embedding=QUERY, p_mediums=[MEDIUM])
    assert ranked == ["zz-test-beta", "zz-test-alpha", "zz-test-gamma"]
    # Without keywords the order is by meaning alone.
    assert search(q_embedding=QUERY, p_mediums=[MEDIUM]) == ["zz-test-beta", "zz-test-gamma", "zz-test-alpha"]


def test_unanalyzed_artists_rank_by_keywords_among_the_analyzed():
    mixed = {"q": WORD2, "p_mediums": [MIXED]}
    # By keywords alone, analyzed or not makes no difference: dana has the word in name and tagline,
    # alpha in name and profile, beta in the profile, drew only in their collected words.
    assert search(**mixed) == ["zz-test-dana", "zz-test-alpha", "zz-test-beta", "zz-test-drew"]
    # With an embedding, dana and drew have no place by meaning, so their keyword place counts twice.
    # Two of the four keyword matches are analyzed, so places by meaning are doubled: beta 2, gamma 4,
    # alpha 6. dana 2/61, beta 1/63 + 1/62, alpha 1/62 + 1/66, drew 2/64, gamma 1/64.
    # Scored the old way (no stand-in, no stretch) the two unanalyzed artists would sit below both
    # analyzed keyword matches: beta, alpha, dana, gamma, drew.
    assert search(**mixed, q_embedding=QUERY) == [
        "zz-test-dana",
        "zz-test-beta",
        "zz-test-alpha",
        "zz-test-drew",
        "zz-test-gamma",
    ]


def test_misspelled_words_are_put_right():
    # "concpet", "charcter" and "desing" are words nobody uses, so each is also searched for as its
    # nearest known word. cora is found by all three, and before arti, who only has "artist".
    typo = {"q": "concpet artist charcter desing", "p_mediums": [TYPO]}
    assert search(**typo) == ["zz-test-cora", "zz-test-arti"]
    assert search(q="concept character design", p_mediums=[TYPO]) == ["zz-test-cora"]  # spelled right: the same artist
    assert search(q="zzqxnothing desing", p_mediums=[MEDIUM]) == []  # nothing is made up for a word with no neighbour


def test_word_list_holds_only_what_the_public_can_see():
    # A hidden artist's words must not steer how a visitor's misspelling is put right.
    words = db.sql("select word from public.search_words where word like 'zzqx%' or word = 'concept' order by 1")
    assert [row["word"] for row in words] == ["concept", "zzqxblend", "zzqxglorp"]  # not zzqxsecretword


def test_search_filters_rule_out_before_ranking():
    top = {"q": WORD, "q_embedding": QUERY, "p_mediums": [MEDIUM], "p_limit": 1}
    assert search(**top) == ["zz-test-beta"]
    # Each filter rules out beta. The one slot then goes to the best of what is left,
    # which it could not if the filter ran after ranking and limiting.
    assert search(**top, p_open_only=True) == ["zz-test-alpha"]
    assert search(**top, p_specialties=["zz-test-character"]) == ["zz-test-alpha"]
    assert search(**{**top, "p_mediums": ["zz-test-traditional"]}) == ["zz-test-gamma"]

    everyone = {"q_embedding": QUERY, "p_mediums": [MEDIUM]}
    assert search(**everyone, p_open_only=True) == ["zz-test-alpha"]  # beta is closed, gamma unknown
    assert search(**everyone, p_specialties=["zz-test-creature"]) == ["zz-test-beta"]
    assert search(**everyone, p_specialties=["zz-test-character"]) == ["zz-test-gamma", "zz-test-alpha"]
    assert search(q_embedding=QUERY, p_mediums=["ZZ-Test-Traditional"]) == ["zz-test-gamma"]  # case does not matter
    assert search(**everyone, p_specialties=["zz-test-creature", "zz-test-character"]) == search(**everyone)
    assert search(**everyone, p_specialties=["zz-test-creature"], p_open_only=True) == []  # filters combine
    assert search(**everyone, p_specialties=[]) == search(**everyone)  # an empty list is no filter
    assert search(q=WORD, p_open_only=True) == ["zz-test-alpha"]


def test_search_with_no_query_browses_and_pages():
    everyone = search(p_mediums=[MEDIUM])
    assert everyone == ["zz-test-beta", "zz-test-gamma", "zz-test-alpha"]  # by name
    pages = search(p_mediums=[MEDIUM], p_limit=2) + search(p_mediums=[MEDIUM], p_limit=2, p_offset=2)
    assert pages == everyone
    assert len(search(p_mediums=[MEDIUM], p_limit=-5, p_offset=-5)) == 1  # out of range values are clamped
    assert search(q=None, p_mediums=[MEDIUM], p_limit=None, p_offset=None) == everyone
    assert search(p_mediums=[MEDIUM], p_offset=2_000_000_000) == []  # a page that deep is empty, not an error


def test_search_survives_odd_input():
    # Each odd query still finds the word that follows it, so it was parsed and searched, not swallowed.
    odd = ("hand-painted", "AT&T", "http://x.invalid/?a=1&b=2", "it's", "!!! | & <-> ()", "'; drop table artists; --")
    for q in (*odd, "the", "  ", "é漢字"):
        assert {"zz-test-alpha", "zz-test-beta"} <= set(search(q=f"{q} {WORD}", p_mediums=[MEDIUM])), q
    # An embedding of the wrong size is refused as the caller's mistake (400), whatever its size:
    # a list longer than a vector can be used to end in HTTP 500.
    for size in (2, 1023, 1025, 16_001):
        wrong = db.data_api("POST", "rpc/search_artists", "publishable", json={"q": "", "q_embedding": [0.5] * size})
        assert wrong.status_code == 400 and "1,024 numbers" in wrong.text, size


def test_search_reads_only_the_start_of_a_long_query():
    # A 20,000-word query used to end in HTTP 500. Now only its first 200 characters are parsed.
    filler = "qqxfiller "  # ten characters
    assert search(q=filler * 20_000) == []
    assert search(q=f"{WORD} " + filler * 2_000) == ["zz-test-alpha", "zz-test-beta"]
    assert search(q=filler * 19 + WORD) == ["zz-test-alpha", "zz-test-beta"]  # the word ends at character 199
    assert search(q=filler * 20 + WORD) == []  # the word starts at character 201


def test_search_filter_lists_are_capped():
    # A filter list is read up to its 20th value. 100,000 values used to end in HTTP 500 after 4 s.
    everyone = ["zz-test-beta", "zz-test-gamma", "zz-test-alpha"]
    junk = [f"zz-none-{n}" for n in range(FILTER_CAP)]
    assert search(q_embedding=QUERY, p_mediums=[*junk[:-1], MEDIUM]) == everyone  # the 20th value counts
    assert search(q_embedding=QUERY, p_mediums=[*junk, MEDIUM]) == []  # the 21st does not
    assert search(q_embedding=QUERY, p_mediums=[MEDIUM], p_specialties=[*junk, "zz-test-character"]) == []
    assert search(q_embedding=QUERY, p_mediums=[MEDIUM, *junk * 5_000]) == everyone  # 100,001 values, answered
    # A value longer than any stored one matches nothing, and still counts as a filter.
    assert search(q_embedding=QUERY, p_mediums=[MEDIUM], p_specialties=["x" * 200_000]) == []
    assert search(q_embedding=QUERY, p_mediums=[MEDIUM], p_specialties=["x" * 200_000, "ZZ-Test-Creature"]) == [
        "zz-test-beta"
    ]


def test_cards_and_the_profile_page():
    [card] = db.rpc("search_artists", {"q": WORD, "p_open_only": True})
    assert set(card) == {"slug", "name", "tagline", "profile", "specialties", "mediums", "open_for_work", "works"}
    assert (card["slug"], card["tagline"]) == ("zz-test-alpha", "Armor and creatures")
    assert [work["title"] for work in card["works"]] == ["alpha 0", "alpha 1", "alpha 2", "alpha 3"]
    assert card["works"][0] == {
        "image_url": "https://zz-test-alpha.invalid/thumb/0.jpg",
        "page_url": "https://zz-test-alpha.invalid/work/0",
        "title": "alpha 0",
        "source": "site",  # each work says where it is shown, so a DeviantArt thumbnail can be credited
    }
    [similar, *_] = db.rpc("similar_artists", {"p_slug": "zz-test-alpha"})
    assert set(similar) == set(card)  # the same card from every function
    assert {work["source"] for work in similar["works"]} == {"deviantart"}

    [profile] = db.rpc("get_artist", {"p_slug": "zz-test-alpha"})
    assert set(profile) == set(card) | {"top_tags", "site_url", "deviantart_url"}
    assert profile["top_tags"] == ["zz-armor", "zz-knight"]
    assert profile["site_url"] == "https://www.zz-test-alpha.invalid/"
    assert profile["deviantart_url"] == "https://www.deviantart.com/zz-test-alpha"
    assert [work["title"] for work in profile["works"]] == [f"alpha {n}" for n in range(11)]
    assert db.rpc("get_artist", {"p_slug": "zz-test-hidden"}) == []
    assert db.rpc("get_artist", {"p_slug": "zz-test-nobody"}) == []


# 4. similar_artists.


def test_similar_artists_ranks_by_style_on_the_first_ten_works():
    found = slugs(db.rpc("similar_artists", {"p_slug": "zz-test-alpha"}))
    # Mean over the seed's two counted works of the best match: beta 1.0, gamma 0.71, delta 0.5, epsilon 0.
    # The order tells the scoring apart from "best single pair" (delta would tie beta) and from
    # "mean of all pairs" (gamma would win). It also proves both caps: counting the seed's eleventh
    # work lifts delta to beta's score, and counting epsilon's last two works lifts epsilon to 1.0.
    assert found[:3] == ["zz-test-beta", "zz-test-gamma", "zz-test-delta"]
    assert "zz-test-alpha" not in found
    assert "zz-test-hidden" not in found
    assert slugs(db.rpc("similar_artists", {"p_slug": "zz-test-alpha", "p_limit": 1})) == ["zz-test-beta"]
    assert db.rpc("similar_artists", {"p_slug": "zz-test-hidden"}) == []  # a hidden seed gives nothing away
    assert db.rpc("similar_artists", {"p_slug": "zz-test-nobody"}) == []
    assert db.rpc("similar_artists", {"p_slug": "zz-test-dana"}) == []  # no style vectors yet: an empty answer


# 5. The publishable key.


def test_publishable_key_cannot_touch_tables(seeded):
    for table in TABLES:
        assert denied(db.data_api("GET", table, "publishable")), table
    # The same read with the secret key finds the row, so the check above is not empty.
    params = {"slug": "eq.zz-test-alpha", "select": "slug"}
    assert db.data_api("GET", "artists", "secret", params=params).json() == [{"slug": "zz-test-alpha"}]
    # The secret the proof codes are made from is not even the service role's to read.
    assert denied(db.data_api("GET", "removal_secret", "secret"))

    assert denied(db.data_api("POST", "artists", "publishable", json=artist("intruder")))
    hidden = {"slug": "eq.zz-test-hidden"}
    assert denied(db.data_api("PATCH", "artists", "publishable", params=hidden, json={"hidden": False}))
    assert denied(db.data_api("DELETE", "artists", "publishable", params={"slug": "like.zz-test-*"}))
    assert db.sql(COUNTS)[0]["artists"] >= len(ARTISTS)
    assert db.sql("select hidden from public.artists where slug = 'zz-test-hidden'") == [{"hidden": True}]


def test_publishable_key_runs_the_public_functions_only():
    assert slugs(db.rpc("search_artists", {"q": WORD}, "publishable")) == ["zz-test-alpha", "zz-test-beta"]
    assert slugs(db.rpc("similar_artists", {"p_slug": "zz-test-alpha", "p_limit": 1}, "publishable")) == [
        "zz-test-beta"
    ]
    assert slugs(db.rpc("get_artist", {"p_slug": "zz-test-alpha"}, "publishable")) == ["zz-test-alpha"]
    assert db.rpc("request_removal", {"p_slug": "zz-test-nobody"}, "publishable") is True
    assert CODE.fullmatch(db.rpc("start_removal_proof", {"p_slug": "zz-test-nobody"}, "publishable"))

    # Each of these exists and works for the service role or the owner; the public is refused.
    for name, args in (
        ("confirm_removal", {"p_slug": "zz-test-alpha", "p_code": "x"}),
        ("purge_removed", {"p_slugs": ["zz-test-alpha"]}),
        ("reject_removal", {"p_slug": "zz-test-alpha"}),
        ("remove_artists", {"p_slugs": ["zz-test-alpha"]}),
        ("delete_removed", {}),
        ("removal_code", {"p_slug": "zz-test-alpha"}),
        ("artist_ids", {}),
        ("refresh_style_centroids", {}),
        ("refresh_search_words", {}),
        ("artist_card", {"p_artist_id": 1}),
        ("artist_keys", {"p_da_username": "zz-test-alpha", "p_site_domain": None}),
    ):
        assert denied(db.data_api("POST", f"rpc/{name}", "publishable", json=args)), name
    # Nor can the service role read a proof code's making: only the two functions that need it can.
    assert denied(db.data_api("POST", "rpc/removal_code", "secret", json={"p_slug": "zz-test-alpha"}))
    assert visible(BY_SLUG["zz-test-alpha"])


def test_tables_are_locked_and_functions_are_pinned():
    [row] = db.sql("""
        with tables as (
          select oid, relrowsecurity from pg_class where relnamespace = 'public'::regnamespace and relkind = 'r'
        ),
        sequences as (
          select oid from pg_class where relnamespace = 'public'::regnamespace and relkind = 'S'
        ),
        functions as (
          select oid, proname::text as name, prosecdef, proconfig from pg_proc
          where pronamespace = 'public'::regnamespace
        ),
        public_roles as (select unnest(array['anon', 'authenticated']) as role)
        select
          (select count(*) from tables where not relrowsecurity)::int as tables_without_rls,
          (select count(*) from pg_policies where schemaname = 'public')::int as policies,
          (select count(*) from tables, public_roles
           where has_table_privilege(role, oid, 'select, insert, update, delete, truncate, references, trigger')
          )::int as tables_open_to_the_public,
          (select count(*) from sequences)::int as sequences,
          (select count(*) from sequences, public_roles
           where has_sequence_privilege(role, oid, 'usage, select, update'))::int as sequences_open_to_the_public,
          (select array_agg(name order by name) from functions
           where has_function_privilege('anon', oid, 'execute')) as anon_functions,
          (select count(*) from functions where has_function_privilege('authenticated', oid, 'execute')
          )::int as authenticated_functions,
          (select array_agg(name order by name) from functions where prosecdef) as definer_functions,
          (select array_agg(name order by name) from functions
           where not coalesce(proconfig @> array['search_path=""'], false)) as functions_without_fixed_path,
          to_regprocedure('public.purge_removed()') is null as purge_everything_is_gone
    """)
    assert row == {
        "tables_without_rls": 0,
        "policies": 0,
        "tables_open_to_the_public": 0,
        "sequences": 2,
        "sequences_open_to_the_public": 0,
        "anon_functions": PUBLIC_FUNCTIONS,
        "authenticated_functions": 0,
        # The public's five, and the two that read the proof secret (neither is the public's to call).
        "definer_functions": sorted([*PUBLIC_FUNCTIONS, "confirm_removal", "removal_code"]),
        "functions_without_fixed_path": None,
        "purge_everything_is_gone": True,
    }


def test_a_function_made_later_is_not_the_publics_to_call():
    # Until it is granted on purpose. (The project's defaults used to hand every new function to anon.)
    try:
        db.sql("create function public.zz_test_later() returns int language sql set search_path = '' as 'select 1'")
        [later] = db.sql("""
            select has_function_privilege('anon', 'public.zz_test_later()', 'execute') as anon,
                   has_function_privilege('authenticated', 'public.zz_test_later()', 'execute') as authenticated
        """)
        assert later == {"anon": False, "authenticated": False}
    finally:
        db.sql("drop function if exists public.zz_test_later()")


# 6. Removal. Nothing the public can call changes what is shown or undoes what an artist did;
#    proof of control removes at once.

REQUESTS = """
select r.artist_slug as slug, r.note, r.requests::int as requests, r.first_at <= r.last_at as in_order
from public.removal_requests r
where r.artist_slug like 'zz-test-%' order by r.artist_slug
"""
SHOWN = "select slug, hidden from public.artists where slug like 'zz-test-%' order by slug"
STATE = """
select (select count(*) from public.artists)::int as artists, (select count(*) from public.works)::int as works,
       (select count(*) from public.removed)::int as removed
"""


def fresh(*rows: dict[str, Any]) -> None:
    """Start a removal test: nothing pending for synthetic artists, and the given ones in the index."""
    db.sql(f"delete from public.removal_requests where artist_slug like 'zz-test-%'; {FORGET};")
    db.upsert("artists", list(rows), "slug", "slug")  # returns only the ones it had to write
    wanted = ", ".join(f"'{row['slug']}'" for row in rows)
    there = db.sql(f"select count(*)::int as n from public.artists where not hidden and slug in ({wanted})")
    assert there == [{"n": len(rows)}]


def ask(row_or_slug: dict[str, Any] | str, note: str | None = None) -> None:
    slug = row_or_slug if isinstance(row_or_slug, str) else row_or_slug["slug"]
    assert db.rpc("request_removal", {"p_slug": slug, "p_note": note}, "publishable") is True


def proof_code(row_or_slug: dict[str, Any] | str) -> str:
    slug = row_or_slug if isinstance(row_or_slug, str) else row_or_slug["slug"]
    code = db.rpc("start_removal_proof", {"p_slug": slug}, "publishable")
    assert CODE.fullmatch(code)
    return code


def confirm(row: dict[str, Any], code: str) -> bool:
    return db.rpc("confirm_removal", {"p_slug": row["slug"], "p_code": code}, "secret")


def test_an_artists_request_survives_whatever_is_asked_after_it(seeded):
    fresh(GONE, STAY)
    shown = db.sql(SHOWN)
    ask("zz-test-nobody", "x")  # the same answer for a slug that does not exist, and nothing logged
    assert db.sql(REQUESTS) == []

    # The artist asks, with a note. Then a script asks six more times, with notes of its own and without.
    ask(GONE, "It is me. Please take my work down.")
    for n in range(6):
        ask(GONE, f"junk {n}" if n % 2 else None)
    ask(STAY, "y" * 5000)  # a long note cannot fill the table
    ask("zz-test-hidden")  # a hidden artist's request is logged too
    # One row per artist: the first note is still there, and what came after only counted.
    assert db.sql(REQUESTS) == [
        {"slug": GONE["slug"], "note": "It is me. Please take my work down.", "requests": 7, "in_order": True},
        {"slug": "zz-test-hidden", "note": None, "requests": 1, "in_order": True},
        {"slug": STAY["slug"], "note": "y" * 300, "requests": 1, "in_order": True},
    ]
    # The service role can read it.
    mine = {"artist_slug": f"eq.{GONE['slug']}", "select": "note,requests"}
    assert db.data_api("GET", "removal_requests", "secret", params=mine).json() == [
        {"note": "It is me. Please take my work down.", "requests": 7}
    ]
    # And nobody's visibility changed.
    assert db.sql(SHOWN) == shown
    assert visible(GONE) and visible(STAY)


def test_no_public_call_changes_what_is_shown_or_spoils_a_code(seeded):
    fresh(*CROWD)
    shown, state = db.sql(SHOWN), db.sql(STATE)
    targets = [*[row["slug"] for row in CROWD], "zz-test-hidden", "zz-test-nobody"]
    asked = [targets[n % len(targets)] for n in range(1000)]
    before = {slug: proof_code(slug) for slug in targets}
    assert len(set(before.values())) == len(targets)  # a code of its own for every slug

    # A script asks for everyone, a thousand times over, twenty at a time, and for every code three times.
    with ThreadPoolExecutor(20) as pool:
        list(pool.map(ask, asked))
        again = list(pool.map(proof_code, targets * 3))
    assert again == [before[slug] for slug in targets] * 3  # every artist's code is still the one they were told

    # Guessing gets nowhere: the functions that act are not the public's to call, with a real code or without.
    for slug in targets[:5]:
        guess = {"p_slug": slug, "p_code": before[slug]}
        assert denied(db.data_api("POST", "rpc/confirm_removal", "publishable", json=guess))
    for name, args in (
        ("purge_removed", {"p_slugs": targets}),
        ("remove_artists", {"p_slugs": targets}),
        ("reject_removal", {"p_slug": targets[0]}),
    ):
        assert denied(db.data_api("POST", f"rpc/{name}", "publishable", json=args)), name

    # Every artist is exactly as visible as before, nothing was deleted, and nothing was barred.
    assert db.sql(SHOWN) == shown
    assert db.sql(STATE) == state
    # The log holds one row for each artist asked about, with how often, and none for the unknown slug.
    assert db.sql(REQUESTS) == [
        {"slug": slug, "note": None, "requests": asked.count(slug), "in_order": True} for slug in sorted(targets[:-1])
    ]


def test_proof_of_control_removes_at_once_and_for_good(seeded):
    fresh(GONE, STAY)
    [gone] = db.sql(f"select id from public.artists where slug = '{GONE['slug']}'")
    db.upsert("works", works_of("gone", [E1, E2], artist_id=gone["id"]), "artist_id,key", "id")
    db.rpc("refresh_search_words", {}, "secret")
    ask(GONE, "it is me")
    there = f"""
        select (select count(*) from public.artists where slug = '{GONE["slug"]}')::int as gone,
               (select count(*) from public.works where artist_id = {gone["id"]})::int as gone_works,
               (select count(*) from public.artists where slug = '{STAY["slug"]}')::int as stay,
               (select count(*) from public.search_words where word = 'zzqxgoneword')::int as gone_words
    """
    untouched = [{"gone": 1, "gone_works": 2, "stay": 1, "gone_words": 1}]
    hashes = ", ".join(f"'{key_hash}'" for key_hash in GONE_HASHES)
    remembered = f"select key_hash from public.removed where key_hash in ({hashes}) order by key_hash"

    # The artist is told their code. Whoever else asks for it afterwards, however often, changes nothing.
    code = proof_code(GONE)
    assert {proof_code(GONE) for _ in range(5)} == {code}
    # A code that is wrong, or somebody else's, does nothing, and says so. Nor can the public confirm.
    assert confirm(GONE, "artist-search-removal-" + "0" * 32) is False
    assert confirm(GONE, proof_code(STAY)) is False
    assert confirm(artist("nobody"), proof_code("zz-test-nobody")) is False  # a code for nobody removes nobody
    guess = {"p_slug": GONE["slug"], "p_code": code}
    assert denied(db.data_api("POST", "rpc/confirm_removal", "publishable", json=guess))
    assert db.sql(there) == untouched and db.sql(remembered) == [] and visible(GONE)

    # The verifier confirms the artist's code: the artist and their works are gone at once, with what was
    # pending for them and the words only they used. Only the hashed keys remain.
    assert confirm(GONE, code) is True
    assert db.sql(there) == [{"gone": 0, "gone_works": 0, "stay": 1, "gone_words": 0}]
    assert [row["key_hash"] for row in db.sql(remembered)] == GONE_HASHES
    assert db.sql(REQUESTS) == []
    assert visible(STAY)
    # The code opens nothing a second time, and the artist cannot be loaded again.
    assert confirm(GONE, code) is False
    assert db.upsert("artists", [GONE], "slug", "slug") == []
    db.load([GONE], works_of("gone", [E1], artist_slug=GONE["slug"]))  # nor by a load, which skips their works too
    assert db.sql(there) == [{"gone": 0, "gone_works": 0, "stay": 1, "gone_words": 0}]


def test_public_functions_answer_alike_for_unknown_hidden_and_visible(seeded):
    fresh(STAY)
    counts = """
        select (select count(*) from public.removal_requests)::int as requests,
               (select count(*) from public.removed)::int as removed,
               (select count(*) from public.artists)::int as artists
    """
    before = db.sql(counts)
    codes = [proof_code(slug) for slug in (STAY["slug"], "zz-test-hidden", "zz-test-nobody")]
    assert len(set(codes)) == 3  # three codes of one shape (proof_code checks the shape)
    assert db.sql(counts) == before  # and asking for a code writes nothing anywhere
    for slug in (STAY["slug"], "zz-test-hidden", "zz-test-nobody"):
        ask(slug)  # the same answer each time (ask checks it)


def test_the_owner_answers_unproven_requests(seeded):
    fresh(GONE, STAY)
    ask(GONE)
    ask(STAY, "not really me")
    assert visible(GONE) and visible(STAY)  # a request alone hides nobody
    hashes = ", ".join(f"'{key_hash}'" for key_hash in GONE_HASHES)
    remembered = f"select key_hash from public.removed where key_hash in ({hashes}) order by key_hash"

    # "Purge everything pending" does not exist, for anyone, and the public cannot purge or reject at all.
    everything = db.data_api("POST", "rpc/purge_removed", "secret", json={})
    assert everything.status_code == 404 and everything.json()["code"] == "PGRST202"
    assert denied(db.data_api("POST", "rpc/purge_removed", "publishable", json={"p_slugs": [GONE["slug"]]}))
    assert denied(db.data_api("POST", "rpc/reject_removal", "publishable", json={"p_slug": STAY["slug"]}))
    # Slugs without a request are left alone, so a typo cannot delete an artist who never asked.
    assert db.rpc("purge_removed", {"p_slugs": []}, "secret") == 0
    assert db.rpc("purge_removed", {"p_slugs": ["zz-test-alpha", "zz-test-nobody"]}, "secret") == 0
    assert visible(BY_SLUG["zz-test-alpha"]) and visible(GONE) and db.sql(remembered) == []

    # Turned down: the request goes, and nothing else changes.
    assert db.rpc("reject_removal", {"p_slug": STAY["slug"]}, "secret") is True
    assert db.rpc("reject_removal", {"p_slug": STAY["slug"]}, "secret") is False  # nothing left to reject
    assert slugs(db.sql(REQUESTS)) == [GONE["slug"]]
    assert visible(STAY)

    # Granted: that artist is removed for good, and only that one.
    assert db.rpc("purge_removed", {"p_slugs": [GONE["slug"]]}, "secret") == 1
    assert not visible(GONE) and visible(STAY)
    assert [row["key_hash"] for row in db.sql(remembered)] == GONE_HASHES
    assert db.sql(REQUESTS) == []
    assert db.rpc("purge_removed", {"p_slugs": [GONE["slug"]]}, "secret") == 0


def bar_gone() -> None:
    """Put GONE's keys on the removal list, as removing GONE leaves them (the two tests above prove the hashes)."""
    values = ", ".join(f"('{key_hash}')" for key_hash in GONE_HASHES)
    db.sql(f"""
        delete from public.artists where slug in ('zz-test-gone', 'zz-test-back');
        insert into public.removed (key_hash) values {values} on conflict do nothing;
    """)


def test_key_on_the_removal_list_deletes_the_artist_it_bars(seeded):
    fresh(KEEP)
    [keep] = db.sql(f"select id from public.artists where slug = '{KEEP['slug']}'")
    db.upsert("works", works_of("keep", [E1], artist_id=keep["id"]), "artist_id,key", "id")
    db.rpc("refresh_search_words", {}, "secret")
    ask(KEEP)
    left = f"""
        select (select count(*) from public.artists where slug = '{KEEP["slug"]}')::int as artists,
               (select count(*) from public.works where artist_id = {keep["id"]})::int as works,
               (select count(*) from public.removal_requests where artist_slug = '{KEEP["slug"]}')::int as requests,
               (select count(*) from public.search_words where word = 'zzqxkeepword')::int as words
    """
    assert db.sql(left) == [{"artists": 1, "works": 1, "requests": 1, "words": 1}] and visible(KEEP)

    # The owner removes a site by hand (what pipeline/store.py's mark_removed does) while its artist is here.
    site_key = key_hashes(KEEP)[1]
    assert db.upsert("removed", [{"key_hash": site_key}], "key_hash", "key_hash") == [{"key_hash": site_key}]
    # Nothing of that artist stays: not the row, not the works, not the request, not the words only they used.
    assert db.sql(left) == [{"artists": 0, "works": 0, "requests": 0, "words": 0}]
    assert visible(BY_SLUG["zz-test-alpha"])  # and nobody else went with them


def test_existing_row_cannot_move_onto_a_removed_key(seeded):
    fresh(STAY)
    bar_gone()
    row = f"select da_username, site_url, site_domain, hidden from public.artists where slug = '{STAY['slug']}'"
    before = db.sql(row)
    patch = {"params": {"slug": f"eq.{STAY['slug']}"}, "headers": {"Prefer": "return=representation"}}
    for move in (
        {"da_username": "zz-test-GONE"},
        {"site_url": "https://zz-test-gone.invalid/new", "site_domain": "zz-test-gone.invalid"},
        {"site_url": "https://m.zz-test-gone.invalid/", "site_domain": "zz-test-gone.invalid"},
    ):
        # An update is refused out loud; an upsert skips the row, as it does for any removed artist.
        response = db.data_api("PATCH", "artists", "secret", json=move, **patch)
        assert response.status_code == 400 and "removed artist" in response.text, move
        assert db.upsert("artists", [{**STAY, **move}], "slug", "slug") == [], move
        assert db.sql(row) == before, move

    # Every other change still goes through, hiding first of all.
    hide = db.data_api("PATCH", "artists", "secret", json={"hidden": True}, **patch)
    assert hide.status_code == 200 and [found["hidden"] for found in hide.json()] == [True]
    assert not visible(STAY)
    moved = db.data_api("PATCH", "artists", "secret", json={"site_url": "https://zz-test-stay.invalid/new"}, **patch)
    assert moved.status_code == 200 and len(moved.json()) == 1


def test_removal_does_not_bar_lookalikes(seeded):
    bar_gone()
    lookalike = {"site_url": "https://notzz-test-gone.invalid/", "site_domain": "notzz-test-gone.invalid"}
    near = artist("near", da_username="zz-test-gone2", **lookalike)  # ends like the removed domain, is not under it
    try:
        assert db.upsert("artists", [near], "slug", "slug") == [{"slug": "zz-test-near"}]
    finally:
        db.sql("delete from public.artists where slug = 'zz-test-near'")


def back(site_url: str | None = None, **fields: Any) -> dict[str, Any]:
    """GONE trying to come back under a new slug, by default with a new handle too."""
    on_the_site = {"site_url": site_url, "site_domain": "zz-test-gone.invalid"} if site_url else {}
    return artist("back", **{**on_the_site, **fields})


READDS = [
    ("the same row again", GONE),
    ("a new slug and the handle in another case", back(da_username="zz-test-GONE")),
    ("a new slug and the site without www", back("https://zz-test-gone.invalid/")),
    ("the site over http on another port", back("http://www.zz-test-gone.invalid:8080")),
    ("the site under www.www.", back("https://www.www.zz-test-gone.invalid/")),
    ("the site under shop.", back("https://shop.zz-test-gone.invalid/")),
    ("the site under m.", back("https://m.zz-test-gone.invalid/")),
    ("the site two levels down", back("https://a.b.zz-test-gone.invalid/")),
    ("a trailing dot on the host", back("https://zz-test-gone.invalid./")),
    ("a user in the URL", back("https://me@zz-test-gone.invalid/")),
    ("capitals in the host", back("https://ZZ-Test-Gone.INVALID/")),
    ("capitals in the domain", back("https://zz-test-gone.invalid/", site_domain="ZZ-Test-Gone.invalid")),
    ("a trailing dot on the domain", back("https://zz-test-gone.invalid/", site_domain="zz-test-gone.invalid.")),
    ("the site with no domain given", back("https://zz-test-gone.invalid/", site_domain=None)),
    ("the site with somebody else's domain", back("https://zz-test-gone.invalid/", site_domain="zz-test-back.invalid")),
    *[(f"the site followed by {extra!r}", back(f"https://zz-test-gone.invalid{extra}")) for extra in " \n\t\xa0​"],
    *[(f"the handle followed by {extra!r}", back(da_username=f"zz-test-gone{extra}")) for extra in " \n\xa0​　﻿\x0b"],
    ("the handle after a space", back(da_username=" zz-test-gone")),
    ("no handle and no site", back(da_username=None)),
]


@pytest.mark.parametrize(("label", "row"), READDS, ids=[label for label, _ in READDS])
def test_removed_artist_cannot_come_back(label: str, row: dict[str, Any]):
    bar_gone()
    response = db.data_api(
        "POST",
        "artists",
        "secret",
        params={"on_conflict": "slug", "select": "slug"},
        headers={"Prefer": "resolution=merge-duplicates,return=representation"},
        json=[row],
    )
    # Either the row is skipped as a removed artist, or it is refused as not canonical. It is never stored.
    skipped = response.is_success and response.json() == []
    not_canonical = response.status_code == 400 and response.json()["code"] in ("23514", "23502")  # check, not null
    assert skipped or not_canonical, response.text
    assert db.sql("select count(*)::int as n from public.artists where slug in ('zz-test-gone', 'zz-test-back')") == [
        {"n": 0}
    ]


def test_a_write_racing_a_removal_does_not_survive_it(seeded):
    fresh(RACE)
    outcome: dict[str, Any] = {}

    def remove_slowly() -> None:
        # The removal's transaction stays open for four seconds after it has done its work.
        db.sql(f"select public.remove_artists(array['{RACE['slug']}']); select pg_sleep(4);")

    removal = threading.Thread(target=remove_slowly)
    removal.start()
    time.sleep(1.5)  # the removal has run and not committed: its keys are not visible to anyone else yet
    racing = {**RACE, "tagline": "loaded while the removal was open"}
    outcome["write"] = db.data_api(
        "POST",
        "artists",
        "secret",
        params={"on_conflict": "slug", "select": "slug"},
        headers={"Prefer": "resolution=merge-duplicates,return=representation"},
        json=[racing],
    )
    removal.join()
    # The write waited for the removal and then failed (or wrote nothing). Either way the artist is gone.
    write = outcome["write"]
    assert (write.status_code == 400 and "removed artist" in write.text) or write.json() == [], write.text
    assert db.sql(f"select count(*)::int as n from public.artists where slug = '{RACE['slug']}'") == [{"n": 0}]
    assert not visible(RACE)


def test_a_removal_does_not_wait_for_a_load(seeded):
    fresh(HELD)
    code = proof_code(HELD)
    load_failed: list[str] = []

    def hold_the_row() -> None:
        # A load that has rewritten the artist's row and keeps its transaction open for eight seconds.
        try:
            db.sql(f"update public.artists set tagline = 'held' where slug = '{HELD['slug']}'; select pg_sleep(8);")
        except RuntimeError as error:
            load_failed.append(str(error))

    holder = threading.Thread(target=hold_the_row)
    holder.start()
    time.sleep(1.5)
    try:
        started = time.perf_counter()
        assert confirm(HELD, code) is True  # answered at once, not after the load, and not with a timeout
        assert time.perf_counter() - started < 3
        # The row is still there, held by the load, but the public no longer sees the artist.
        count = f"select count(*)::int as n from public.artists where slug = '{HELD['slug']}'"
        assert db.sql(count) == [{"n": 1}]
        assert not visible(HELD)
        assert HELD["slug"] not in slugs(db.rpc("search_artists", {"q": "held"}))
    finally:
        holder.join()
    # The load's write could not commit onto a removed artist, and the sweep after a load takes the row.
    assert len(load_failed) == 1 and "removed artist" in load_failed[0]
    assert db.rpc("delete_removed", {}, "secret") == 1
    assert db.sql(count) == [{"n": 0}]


# The removal list of record is the `removed` table, and pipeline/store.py imports it. These hosts are never
# stored or fetched: they need a real public suffix, because the store reads no site from a name like x.invalid.
STORE_CASES: list[tuple[str, str | None]] = [
    ("ZZ-Test_Jane", "https://www.zz-test-jane.com:8443/art?p=1"),
    ("Jane", "https://shop.zz-test-jane.com/x"),
    ("zz-9", "https://m.blog.zz-test-other.co.uk/?a=1"),
    ("a", None),
    ("z" * 40, "http://zz-test-plain.art"),
]


def test_removal_keys_are_the_pipeline_stores(seeded):
    from pipeline import store  # another builder's module: imported here so that only this test depends on it

    for handle, site_url in STORE_CASES:
        # The push stage supplies the registrable domain, worked out the way the store works it out.
        domain = store.registrable_domain(urlsplit(site_url).hostname or "") if site_url else None
        domain_sql = f"'{domain}'" if domain else "null"
        [row] = db.sql(f"select public.artist_keys('{handle}', {domain_sql}) as keys")
        theirs = [store.artist_key(handle), *([store.site_key(site_url)] if site_url else [])]
        assert row["keys"] == [digest(key) for key in theirs], (handle, site_url)
    # And a key the store puts on the list bars the artist here, whichever host of their site they are on.
    for site_url in ("https://www.zz-test-jane.com/", "https://shop.zz-test-jane.com/x"):
        jane = artist("jane", site_url=site_url, site_domain="zz-test-jane.com")
        assert digest(store.site_key(site_url)) in key_hashes(jane)


# 7. No image bytes, and under the size budget.

GOOD_WORK = {
    "source": "site",
    "page_url": "https://zz-test-alpha.invalid/work/99",
    "image_url": "https://img.zz-test-alpha.invalid/t/99.jpg",  # a subdomain of the artist's site is their site
}
DATA_URL = "data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNk"
IMAGE = "data:image/png;base64," + "A" * 200_000  # an image as text: what a critic stored in every text column
OWN_WORDS, SITE_URL, DOMAIN = "artists_own_words_sizes", "artists_site_url_clean", "artists_site_domain_clean"
BAD = "zz-test-bad.invalid"
BAD_ARTISTS = [
    ("a space in the handle", {"da_username": "zz test"}, "artists_da_username_clean"),
    ("an accent in the handle", {"da_username": "zz-tést"}, "artists_da_username_clean"),
    ("an empty handle", {"da_username": ""}, "artists_da_username_clean"),
    ("a handle of 41 characters", {"da_username": "z" * 41}, "artists_da_username_clean"),
    ("a data: URL as the site", {"site_url": DATA_URL, "site_domain": BAD}, SITE_URL),
    ("data: in the site's query", {"site_url": f"https://{BAD}/?x={DATA_URL}", "site_domain": BAD}, SITE_URL),
    ("a fragment on the site", {"site_url": f"https://{BAD}/#top", "site_domain": BAD}, SITE_URL),
    ("an IP address as the site", {"site_url": "https://192.0.2.7/", "site_domain": BAD}, DOMAIN),
    ("a host with no domain", {"site_url": "https://localhost/", "site_domain": BAD}, DOMAIN),
    ("an accent in the path", {"site_url": f"https://{BAD}/café", "site_domain": BAD}, SITE_URL),
    ("a site of 2,049 characters", {"site_url": f"https://{BAD}/".ljust(2049, "a"), "site_domain": BAD}, SITE_URL),
    ("a site with no domain given", {"site_url": f"https://{BAD}/"}, DOMAIN),
    ("a domain with no site", {"site_domain": BAD}, DOMAIN),
    ("a domain the site is not on", {"site_url": f"https://{BAD}/", "site_domain": "zz-test-other.invalid"}, DOMAIN),
    ("a domain the site only ends like", {"site_url": f"https://not{BAD}/", "site_domain": BAD}, DOMAIN),
    ("a domain in capitals", {"site_url": f"https://{BAD}/", "site_domain": BAD.upper()}, DOMAIN),
    ("a domain with a scheme", {"site_url": f"https://{BAD}/", "site_domain": f"https://{BAD}"}, DOMAIN),
    ("a bare top-level domain", {"site_url": f"https://{BAD}/", "site_domain": "invalid"}, DOMAIN),
    (
        "DeviantArt as the artist's own site",
        {"site_url": "https://www.deviantart.com/zz-test-bad", "site_domain": "deviantart.com"},
        DOMAIN,
    ),
    ("a list of links", {"links": [f"https://{BAD}/"]}, "links"),  # the column is gone: one site, one handle
    ("an image as the name", {"name": IMAGE}, "artists_sizes"),
    ("an image as the profile", {"profile": IMAGE}, "artists_sizes"),
    ("an image among the specialties", {"specialties": ["concept art", IMAGE]}, "artists_sizes"),
    ("an image among the mediums", {"mediums": [IMAGE]}, "artists_sizes"),
    ("an empty name", {"name": ""}, "artists_sizes"),
    ("a name of 121 characters", {"name": "n" * 121}, "artists_sizes"),
    ("a profile of 2,001 characters", {"profile": "p" * 2001}, "artists_sizes"),
    ("21 specialties", {"specialties": [f"s{n}" for n in range(21)]}, "artists_sizes"),
    ("a medium of 61 characters", {"mediums": ["m" * 61]}, "artists_sizes"),
    ("a slug of 81 characters", {"slug": "zz-test-bad-".ljust(81, "a")}, "artists_sizes"),
    ("an image as the tagline", {"tagline": IMAGE}, OWN_WORDS),
    ("an image as the search text", {"search_text": IMAGE}, OWN_WORDS),
    ("an image among the top tags", {"top_tags": ["fantasy", IMAGE]}, OWN_WORDS),
    ("a tagline of 301 characters", {"tagline": "t" * 301}, OWN_WORDS),
    ("a search text of 4,001 characters", {"search_text": "s" * 4001}, OWN_WORDS),
    ("41 top tags", {"top_tags": [f"t{n}" for n in range(41)]}, OWN_WORDS),
    ("a top tag of 61 characters", {"top_tags": ["t" * 61]}, OWN_WORDS),
]
URLS, SIZES, DA_HOSTS, OWN_SITE = "works_urls_clean", "works_sizes", "works_deviantart_hosts", "on that site"
DA_PAGE = "https://www.deviantart.com/zz-test-alpha/art/Zz-Test-99-900000099"
ON_DA = {"source": "deviantart", "page_url": DA_PAGE, "image_url": thumbnail("alpha", 99)}
BAD_WORKS = [
    ("a data: URL as the image", {"image_url": DATA_URL}, URLS),
    ("a data: fragment on the image", {"image_url": f"https://zz-test-alpha.invalid/t.jpg#{DATA_URL}"}, URLS),
    ("a data: fragment on the page", {"page_url": f"https://zz-test-alpha.invalid/work#{DATA_URL}"}, URLS),
    ("data: in the image's query", {"image_url": f"https://zz-test-alpha.invalid/t.jpg?{DATA_URL}"}, URLS),
    ("DATA: in the page's path", {"page_url": "https://zz-test-alpha.invalid/DATA:image/png;base64,AAAA"}, URLS),
    ("a space in the page URL", {"page_url": "https://zz-test-alpha.invalid/my work"}, URLS),
    ("an image URL of 2,049 characters", {"image_url": "https://zz-test-alpha.invalid/".ljust(2049, "a")}, URLS),
    ("a fragment on a DeviantArt thumbnail", {**ON_DA, "image_url": thumbnail("x", 1) + "#x"}, URLS),
    ("a thumbnail of 2,049 characters", {**ON_DA, "image_url": thumbnail("x", 1).ljust(2049, "a")}, URLS),
    # A work's URLs must be where its source says they are.
    ("a site work with its image elsewhere", {"image_url": "https://zz-test-beta.invalid/t.jpg"}, OWN_SITE),
    ("a site work with its page elsewhere", {"page_url": "https://www.deviantart.com/zz-test-alpha"}, OWN_SITE),
    (
        "a site work on a host that only ends like the site",
        {"image_url": "https://notzz-test-alpha.invalid/t.jpg"},
        OWN_SITE,
    ),
    ("a DeviantArt work with its page elsewhere", {**ON_DA, "page_url": "https://zz-test-alpha.invalid/w"}, DA_HOSTS),
    (
        "a DeviantArt page on a lookalike host",
        {**ON_DA, "page_url": "https://www.deviantart.com.zz-test.invalid/a"},
        DA_HOSTS,
    ),
    (
        "a DeviantArt thumbnail on the artist's site",
        {**ON_DA, "image_url": "https://zz-test-alpha.invalid/t.jpg"},
        DA_HOSTS,
    ),
    (
        "a DeviantArt thumbnail on a lookalike host",
        {**ON_DA, "image_url": "https://wixmp.com.zz-test.invalid/t.jpg"},
        DA_HOSTS,
    ),
    ("an image as the title", {"title": IMAGE}, SIZES),
    ("an image as the description", {"description": IMAGE}, SIZES),
    ("an image among the tags", {"tags": ["armor", IMAGE]}, SIZES),
    ("a title of 201 characters", {"title": "t" * 201}, SIZES),
    ("a description of 2,001 characters", {"description": "d" * 2001}, SIZES),
    ("41 tags", {"tags": [f"t{n}" for n in range(41)]}, SIZES),
    ("a tag of 61 characters", {"tags": ["t" * 61]}, SIZES),
    ("a source that is neither", {"source": "artstation"}, "works_source_known"),
]


def test_what_counts_as_a_clean_url():
    urls = {
        "https://zz-test-alpha.invalid": True,
        "http://www.zz-test-alpha.invalid:8443/art/x_1.jpg?w=300,h=300&t=a.b-c~d": True,
        thumbnail("alpha", 1): True,
        "https://192.0.2.7/": False,  # an IP address
        "https://localhost/": False,  # no domain
        "https://me@zz-test-alpha.invalid/": False,  # a user
        "https://zz-test-alpha.invalid./": False,  # a trailing dot
        "https://ZZ-test-alpha.invalid/": False,  # capitals in the host
        "https://zz-test-alpha.invalid/a b": False,  # a space
        "https://zz-test-alpha.invalid/#top": False,  # a fragment
        "https://zz-test-alpha.invalid/?next=data:text/plain,hello": False,  # data: anywhere
        "https://zz-test-alpha.invalid/Data:x": False,  # in any case
        "ftp://zz-test-alpha.invalid/": False,
        DATA_URL: False,
    }
    values = ", ".join(f"({n}, '{url}')" for n, url in enumerate(urls))
    rows = db.sql(f"select public.clean_url(url) as clean from (values {values}) v (n, url) order by n")
    assert [row["clean"] for row in rows] == list(urls.values())


def test_no_image_bytes_and_database_under_budget():
    [row] = db.sql("""
        select (select count(*) from information_schema.columns
                where table_schema = 'public' and udt_name in ('bytea', '_bytea'))::int as bytea_columns,
               (select count(*) from storage.objects)::int as stored_files,
               (pg_database_size(current_database()) / 1048576.0)::float as megabytes
    """)
    assert row["bytea_columns"] == 0
    assert row["stored_files"] == 0
    assert 0 < row["megabytes"] < 400


@pytest.mark.parametrize(("label", "fields", "reason"), BAD_ARTISTS, ids=[bad[0] for bad in BAD_ARTISTS])
def test_artist_must_be_canonical(label: str, fields: dict[str, Any], reason: str):
    response = db.data_api("POST", "artists", "secret", json=artist("bad", **fields))
    assert refused(response, reason), response.text[:300]
    assert db.sql("select count(*)::int as n from public.artists where slug like 'zz-test-bad%'") == [{"n": 0}]


def test_longest_clean_values_are_accepted(seeded):
    # The limits in the two tests around this one are exact: one character less is fine.
    longest = artist("long", da_username="z" * 40)
    longest |= {"site_url": "https://zz-test-long.invalid/".ljust(2048, "a"), "site_domain": "zz-test-long.invalid"}
    longest |= {"name": "n" * 120, "profile": "p" * 2000, "specialties": ["s" * 60] * 20, "mediums": ["m" * 60] * 20}
    longest |= {"tagline": "t" * 300, "search_text": "s" * 4000, "top_tags": ["t" * 60] * 40}
    work = {**GOOD_WORK, "artist_id": seeded["zz-test-alpha"]}
    work["image_url"] = "https://img.zz-test-alpha.invalid/".ljust(2048, "a")
    work |= {"title": "t" * 200, "description": "d" * 2000, "tags": ["t" * 60] * 40}
    try:
        assert db.upsert("artists", [longest], "slug", "slug") == [{"slug": "zz-test-long"}]
        assert len(db.upsert("works", [work], "artist_id,key", "id")) == 1
    finally:
        db.sql("""
            delete from public.artists where slug = 'zz-test-long';
            delete from public.works where page_url = 'https://zz-test-alpha.invalid/work/99';
        """)


@pytest.mark.parametrize(("label", "fields", "reason"), BAD_WORKS, ids=[bad[0] for bad in BAD_WORKS])
def test_work_must_be_clean_short_and_where_it_says(seeded, label: str, fields: dict[str, Any], reason: str):
    # A data: URL, data hidden in a fragment or a query, or an image pasted in as text would be image bytes
    # in a text column (hard rule 1); and a thumbnail is shown only from DeviantArt or the artist's own site.
    alpha = seeded["zz-test-alpha"]
    response = db.data_api("POST", "works", "secret", json={**GOOD_WORK, "artist_id": alpha, **fields})
    assert refused(response, reason), response.text[:300]
    assert db.sql(f"select count(*)::int as n from public.works where artist_id = {alpha}") == [
        {"n": len(STYLES["alpha"])}
    ]


def test_a_work_cannot_be_on_the_site_of_an_artist_who_has_none(seeded):
    dana = {**GOOD_WORK, "artist_id": seeded["zz-test-dana"]}
    assert refused(db.data_api("POST", "works", "secret", json=dana), OWN_SITE)
    assert db.sql(f"select count(*)::int as n from public.works where artist_id = {seeded['zz-test-dana']}") == [
        {"n": len(STYLES["dana"])}
    ]


def test_removal_tables_hold_no_long_text():
    def post(table: str, row: dict[str, Any]) -> httpx.Response:
        return db.data_api("POST", table, "secret", json=row)

    assert refused(post("removal_requests", {"artist_slug": "zz-test-alpha", "note": IMAGE}), "removal_requests_short")
    assert refused(
        post("removal_requests", {"artist_slug": "zz-test-alpha", "note": "n" * 301}), "removal_requests_short"
    )
    assert refused(post("removal_requests", {"artist_slug": "zz-test-".ljust(81, "a")}), "removal_requests_short")
    assert refused(post("removed", {"key_hash": IMAGE}), "removed_key_is_a_hash")
    assert refused(post("removed", {"key_hash": "da:zz-test-alpha"}), "removed_key_is_a_hash")  # a name, not a hash
    assert db.sql("""
        select (select count(*) from public.removal_requests where artist_slug like 'zz-test-a%')::int as requests,
               (select count(*) from public.removed where length(key_hash) <> 64)::int as keys
    """) == [{"requests": 0, "keys": 0}]


# Secrets: an error never repeats a value from .env, and the request URL holds two of them.


def test_errors_do_not_echo_secrets(monkeypatch):
    with pytest.raises(RuntimeError, match="42501") as refusal:
        db.rpc("purge_removed", {"p_slugs": []}, "publishable")

    def unreachable(method: str, url: str, **kwargs: Any):
        raise httpx.ConnectError(f"cannot reach {url}")

    monkeypatch.setattr(db._http, "request", unreachable)
    with pytest.raises(RuntimeError) as failed:
        db.sql("select 1")
    assert failed.value.__suppress_context__  # the httpx error, which names the URL, is not chained

    secrets_in_env = [value for value in load_env().values() if len(value) >= 8]
    leaked = any(secret in str(error.value) for error in (refusal, failed) for secret in secrets_in_env)
    assert not leaked


# Full scale, on request: RUN_SCALE=1. Loads 3,000 analyzed artists and 30,000 works, loads them all
# again with new vectors while a visitor keeps calling, deletes them and compacts the tables (a brief
# lock). About ten minutes and 200 MB of upload, so it is not part of the normal run.

BULK = """
insert into public.artists
  (slug, name, da_username, tagline, top_tags, search_text, profile, specialties, mediums, open_for_work,
   profile_embedding)
select 'zz-test-bulk-' || g, 'Bulk ' || g, 'zz-test-bulk-' || g,
       'Concept artist and illustrator ' || g,
       array(select 'zztag' || ((g * k) % 400) from generate_series(1, 20) k),
       -- About 600 characters of the artist's own words: thirty titles of three words each.
       (select string_agg('title' || ((g * k) % 5000) || ' fantasy piece', ' ') from generate_series(1, 30) k),
       'Character artist. Painterly fantasy armor and creature designs, mostly digital. ' || md5(g::text),
       array['character design', 'creature design'], array['digital'], g % 2 = 0,
       (select array_agg(random() - 0.5) from generate_series(1, 1024) where g = g)::extensions.halfvec(1024)
from generate_series(1, 3000) g;

-- Ten analyzed DeviantArt works each: a deviation page, and a thumbnail URL with a signed token of 800
-- characters that does not compress (as a real one does not).
insert into public.works
  (artist_id, source, page_url, image_url, title, description, tags, style_embedding, position)
select a.id, 'deviantart',
       'https://www.deviantart.com/' || a.slug || '/art/Work-' || n,
       'https://images-wixmp-ed30a86b8c4ca887773594c2.wixmp.com/f/0a1b2c3d-4e5f-6071-8293-a4b5c6d7e8f9/d' || a.id
         || n || '-pre.jpg/v1/fill/w_300,h_300,q_70,strp/work_' || n || '.jpg?token='
         || (select string_agg(md5(random()::text), '') from generate_series(1, 25) where a.id = a.id and n = n),
       'Work title ' || n,
       'A knight in ornate plate armor stands before a ruined cathedral, painted with loose, textured '
         || 'brushwork and a warm, muted palette. ' || md5(random()::text),
       array['digital painting', 'fantasy', 'armor', 'character', 'painterly', 'knight'],
       (select array_agg(random() - 0.5) from generate_series(1, 768) where a.id = a.id and n = n
       )::extensions.halfvec(768),
       n
from public.artists a cross join generate_series(0, 9) n
where a.slug like 'zz-test-bulk-%';
"""
SCALE = """
select (select count(*) from public.artists where slug like 'zz-test-bulk-%')::int as artists,
       (select count(*) from public.works w join public.artists a on a.id = w.artist_id
        where a.slug like 'zz-test-bulk-%')::int as works,
       (select count(*) from public.artist_styles)::int as style_rows,
       round(pg_database_size(current_database()) / 1048576.0)::int as megabytes
"""


def random_vector(numbers: random.Random, size: int) -> str:
    return "[" + ",".join(f"{numbers.uniform(-0.5, 0.5):.4f}" for _ in range(size)) + "]"


def reload_rows(numbers: random.Random) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Every bulk artist and work again, as the push stage would send them after analyzing everything anew:
    the same artists and deviation pages, with new vectors and new thumbnail tokens."""
    artists, works = [], []
    for g in range(1, 3001):
        slug = f"zz-test-bulk-{g}"
        artists.append(
            {
                "slug": slug,
                "name": f"Bulk {g}",
                "da_username": slug,
                "tagline": f"Concept artist and illustrator {g}",
                "top_tags": [f"zztag{(g * k) % 400}" for k in range(1, 21)],
                "search_text": " ".join(f"title{(g * k) % 5000} fantasy piece" for k in range(1, 31)),
                "profile": f"Character artist. Painterly fantasy armor and creature designs, again. {g}",
                "specialties": ["character design", "creature design"],
                "mediums": ["digital"],
                "open_for_work": g % 2 == 0,
                "profile_embedding": random_vector(numbers, 1024),
            }
        )
        for n in range(10):
            works.append(
                {
                    "artist_slug": slug,
                    "source": "deviantart",
                    "page_url": f"https://www.deviantart.com/{slug}/art/Work-{n}",
                    "image_url": (
                        "https://images-wixmp-ed30a86b8c4ca887773594c2.wixmp.com/f/0a1b2c3d-4e5f-6071-8293-a4b5c6d7e8f9/"
                        f"d{g}{n}-pre.jpg/v1/fill/w_300,h_300,q_70,strp/work_{n}.jpg?token={secrets.token_urlsafe(600)}"
                    ),
                    "title": f"Work title {n}",
                    "description": f"A knight in ornate plate armor, described again. {g} {n}",
                    "tags": ["digital painting", "fantasy", "armor", "character", "painterly", "knight"],
                    "style_embedding": random_vector(numbers, 768),
                    "position": n,
                }
            )
    return artists, works


def server_ms(query: str) -> list[float]:
    """Three runs in a row: the first is the cold one."""
    plans = [db.sql("explain (analyze, costs off, buffers off) " + query) for _ in range(3)]
    return [float(plan[-1]["QUERY PLAN"].split()[2]) for plan in plans]  # "Execution Time: 123.456 ms"


@pytest.mark.skipif(os.environ.get("RUN_SCALE") != "1", reason="set RUN_SCALE=1 to load 3,000 artists and 30,000 works")
def test_full_scale_fits_the_budget_and_the_public_timeout():
    limit = PUBLIC_TIMEOUT_MS / 1000
    numbers = random.Random(5)
    embedding = [round(numbers.uniform(-0.5, 0.5), 4) for _ in range(1024)]
    calls: dict[str, tuple[str, dict[str, Any]]] = {
        "search with an embedding": ("search_artists", {"q": "painterly fantasy character", "q_embedding": embedding}),
        "similar": ("similar_artists", {"p_slug": "zz-test-bulk-1"}),
        "search": ("search_artists", {"q": "painterly fantasy character artist"}),
        "misspelled search": ("search_artists", {"q": "concpet artist charcter desing"}),
        "browse": ("search_artists", {"q": "", "p_mediums": ["digital"], "p_open_only": True}),
        "profile": ("get_artist", {"p_slug": "zz-test-bulk-1"}),
        "removal request": ("request_removal", {"p_slug": "zz-test-bulk-3", "p_note": "scale test"}),
        "proof code": ("start_removal_proof", {"p_slug": "zz-test-bulk-3"}),
    }

    def as_the_public(label: str) -> float:
        """One call through the publishable key, timed. One that the 3 s limit cuts off raises."""
        name, args = calls[label]
        started = time.perf_counter()
        answer = db.rpc(name, args, "publishable")
        took = time.perf_counter() - started
        assert answer is True or CODE.fullmatch(str(answer)) or len(answer) > 0, label
        return took

    def megabytes() -> int:
        return db.sql("select round(pg_database_size(current_database()) / 1048576.0)::int as mb")[0]["mb"]

    try:
        # A first load.
        db.sql(BULK)
        db.after_load()
        [loaded] = db.sql(SCALE)
        print(f"\nloaded: {loaded}", flush=True)
        assert (loaded["artists"], loaded["works"], loaded["style_rows"] >= 3000) == (3000, 30_000, True)
        assert loaded["megabytes"] < 400
        after_load = {label: as_the_public(label) for label in calls}
        print(
            "as the public, after the load: " + ", ".join(f"{k} {v:.2f} s" for k, v in after_load.items()), flush=True
        )
        assert max(after_load.values()) < limit

        # Everything again, analyzed anew, through db.load, while a visitor keeps calling the public
        # functions and the database's size is watched. No call may fail or outlast the public's limit.
        artists, works = reload_rows(numbers)
        seen: list[tuple[str, float]] = []
        failed: list[str] = []
        sizes = [loaded["megabytes"]]
        loading = threading.Event()

        def visitor() -> None:
            while not loading.is_set():
                for label in ("similar", "search with an embedding", "search", "profile"):
                    try:
                        seen.append((label, as_the_public(label)))
                    except Exception as error:  # every failure is recorded, and the test fails on them below
                        failed.append(f"{label} at {time.perf_counter() - started:.0f} s: {error}")
                sizes.append(megabytes())

        # How long each of the load's own statements takes, to see how near the Data API's 8 s they come.
        statements: dict[str, list[float]] = {}
        real_upsert, real_rpc = db.upsert, db.rpc

        def timed(kind: str, call: Any, *args: Any) -> Any:
            began = time.perf_counter()
            try:
                return call(*args)
            finally:
                statements.setdefault(kind, []).append(time.perf_counter() - began)

        db.upsert = lambda table, *args: timed(f"upsert {table}", real_upsert, table, *args)  # type: ignore[assignment]
        db.rpc = lambda name, *args: timed(name, real_rpc, name, *args)  # type: ignore[assignment]
        started = time.perf_counter()
        watcher = threading.Thread(target=visitor)
        watcher.start()
        try:
            db.load(artists, works)
        finally:
            loading.set()
            watcher.join()
            db.upsert, db.rpc = real_upsert, real_rpc  # type: ignore[assignment]
        took = time.perf_counter() - started
        for kind in ("upsert artists", "upsert works", "refresh_style_centroids"):
            took_each = sorted(statements[kind])
            middle, longest = took_each[len(took_each) // 2], took_each[-1]
            print(
                f"the load's {len(took_each)} '{kind}' statements: median {middle:.2f} s, slowest {longest:.2f} s",
                flush=True,
            )
        slowest = {label: max(s for name, s in seen if name == label) for label in {name for name, _ in seen}}
        round_trips = sorted(s for _, s in seen)
        median, most = round_trips[len(round_trips) // 2], round_trips[int(len(round_trips) * 0.99)]
        print(
            f"reload through db.load: {took:.0f} s; a visitor made {len(seen) + len(failed)} calls meanwhile",
            flush=True,
        )
        print(f"of those, {len(failed)} failed: {failed[:3]}", flush=True)
        print(f"round trips during the reload: median {median:.2f} s, 99 in 100 under {most:.2f} s", flush=True)
        print("slowest of each: " + ", ".join(f"{k} {v:.2f} s" for k, v in slowest.items()), flush=True)
        print(f"database size during the reload: peak {max(sizes)} MB over {len(sizes)} readings", flush=True)
        # Every call was answered: one that the public's 3 s limit cut off would be among the failures.
        # (A round trip adds the network to the query, so it is held to the limit for 99 calls in 100.)
        assert failed == []
        assert len(seen) >= 40 and most < limit
        assert max(sizes) < 400
        [reloaded] = db.sql(SCALE)
        print(f"after the reload: {reloaded}", flush=True)
        assert (reloaded["artists"], reloaded["works"]) == (3000, 30_000) and reloaded["megabytes"] < 400
        changed = "select count(*)::int as n from public.works where description like '%described again%'"
        assert db.sql(changed) == [{"n": 30_000}]  # the reload really rewrote every work

        # And the limits that keep single calls cheap, at this size.
        after_reload = {label: as_the_public(label) for label in calls}
        print(
            "as the public, after the reload: " + ", ".join(f"{k} {v:.2f} s" for k, v in after_reload.items()),
            flush=True,
        )
        assert max(after_reload.values()) < limit
        assert len(db.rpc("similar_artists", {"p_slug": "zz-test-bulk-1", "p_limit": 100})) == SIMILAR_CAP
        deep = {"q": "", "p_limit": 100, "p_offset": OFFSET_CAP}
        assert len(db.rpc("search_artists", deep)) == 100  # the deepest page, built for its own rows only
        assert db.rpc("search_artists", {**deep, "p_offset": OFFSET_CAP + 1}) == []  # and past it, nothing
        huge = {"q": "", "p_mediums": ["digital", *["zz-none"] * 100_000]}
        assert len(db.rpc("search_artists", huge)) == 24

        timings = {
            "search_artists, keywords and embedding": server_ms(
                "select * from public.search_artists('painterly fantasy character', "
                "(select profile_embedding::real[] from public.artists where slug = 'zz-test-bulk-2'))"
            ),
            "search_artists, keywords": server_ms("select * from public.search_artists('painterly fantasy character')"),
            "search_artists, deepest page of 100": server_ms(
                "select * from public.search_artists('', null, null, null, false, 100, 1000)"
            ),
            "similar_artists": server_ms("select * from public.similar_artists('zz-test-bulk-1')"),
            "get_artist": server_ms("select * from public.get_artist('zz-test-bulk-1')"),
        }
        for name, runs in timings.items():
            print(f"{name}, on the server: first {runs[0]:.0f} ms, best {min(runs):.0f} ms", flush=True)
        assert max(max(runs) for runs in timings.values()) < PUBLIC_TIMEOUT_MS
    finally:
        db.sql("""
            delete from public.artists where slug like 'zz-test-bulk-%';
            delete from public.removal_requests where artist_slug like 'zz-test-bulk-%';
            select public.refresh_search_words();
        """)
        for table in ("works", "artists", "artist_styles", "search_words"):
            db.sql(f"vacuum full public.{table}")
