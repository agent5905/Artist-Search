"""Live tests for the schema, the search functions and the removal flow.

    uv run pytest tests/test_db.py --basetemp=.pytest_tmp_db
    RUN_SCALE=1 uv run pytest tests/test_db.py --basetemp=.pytest_tmp_db -s -k full_scale

These run against the real Supabase project. Every artist they write has a slug starting with
zz-test-, and the module fixture deletes them before and after, pass or fail.
"""

import hashlib
import json
import os
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

TABLES = ("artists", "works", "removal_requests", "removed", "artist_styles")
PUBLIC_FUNCTIONS = ["get_artist", "request_removal", "search_artists", "similar_artists"]
SERVICE_FUNCTIONS = ["purge_removed", "refresh_style_centroids", "reject_removal"]
HELPER_FUNCTIONS = [
    "artists_before_write",
    "barring_keys",
    "clean_links",
    "clean_url",
    "is_barred",
    "removal_keys",
    "removed_after_insert",
    "short_list",
    "site_host",
    "skip_unchanged",
    "words",
    "work_cards",
]
FILTER_CAP = 20  # search_artists reads this many values of a filter list
HIDE_CAP = 20  # request_removal hides at most this many artists in 24 hours
PUBLIC_TIMEOUT_MS = 3000  # the anon role's statement_timeout

WORD = "zzqxglorp"  # in no real profile, so keyword searches for it see only synthetic artists
MEDIUM = "zz-test-medium"  # on every synthetic search artist; filtering on it keeps real artists out of a ranking


def vec(size: int, *head: float) -> list[float]:
    return [*map(float, head), *[0.0] * (size - len(head))]


def artist(key: str, **fields: Any) -> dict[str, Any]:
    return {
        "slug": f"zz-test-{key}",
        "name": key.title(),
        "da_username": f"zz-test-{key}",
        "site_url": None,
        "links": [],
        "profile": None,
        "specialties": [],
        "mediums": [],
        "open_for_work": None,
        "profile_embedding": None,
        "hidden": False,
        **fields,
    }


QUERY = vec(1024, 1)  # the search embedding. Cosine to it: beta 1.0, gamma 0.8, alpha 0.2.
ARTISTS = [
    artist(
        "alpha",
        name=f"{WORD} Alpha",  # the word in the name outranks the word in the profile
        site_url="https://www.zz-test-alpha.invalid/",
        links=["https://zz-test-alpha.invalid/about?lang=en", "https://www.deviantart.com/zz-test-alpha"],
        profile=f"{WORD} armor",
        mediums=[MEDIUM, "zz-test-digital"],
        specialties=["zz-test-character"],
        open_for_work=True,
        profile_embedding=vec(1024, 0.2, 1),
    ),
    artist(
        "beta",
        profile=f"{WORD} creatures",
        mediums=[MEDIUM, "zz-test-digital"],
        specialties=["zz-test-creature"],
        open_for_work=False,
        profile_embedding=vec(1024, 1),
    ),
    artist(
        "gamma",
        profile="environment paintings",
        mediums=[MEDIUM, "zz-test-traditional"],
        specialties=["zz-test-character"],
        open_for_work=None,
        profile_embedding=vec(1024, 0.8, 0.6),
    ),
    artist("delta"),
    artist("epsilon"),
    artist(  # the best match for every search and every style, and it must never show
        "hidden",
        name=f"{WORD} {WORD} Hidden",
        profile=f"{WORD} {WORD}",
        mediums=[MEDIUM, "zz-test-digital", "zz-test-traditional"],
        specialties=["zz-test-character", "zz-test-creature"],
        open_for_work=True,
        profile_embedding=QUERY,
        hidden=True,
    ),
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
}

# The removal tests' artists. GONE is purged: www, a port and a path show what the site key is made from.
GONE = artist("gone", da_username="ZZ-Test-Gone", site_url="https://www.zz-test-gone.invalid:8443/art?p=1")
GONE_HASHES = sorted(
    hashlib.sha256(key.encode()).hexdigest() for key in ("da:zz-test-gone", "site:zz-test-gone.invalid")
)
STAY = artist("stay", site_url="https://zz-test-stay.invalid/")
COPY = artist("copy", site_url="https://shop.zz-test-gone.invalid/")  # GONE again, on a subdomain of GONE's site
KEEP = artist("keep", site_url="https://zz-test-keep.invalid/")
FAN = artist("fan", links=["https://www.zz-test-keep.invalid/about"])  # tied to KEEP's site by a link only
CROWD = [artist(f"crowd-{n:02d}") for n in range(HIDE_CAP + 10)]


def key_hashes(row: dict[str, Any]) -> list[str]:
    host = urlsplit(row["site_url"] or "").hostname
    keys = [f"da:{row['da_username'].lower()}", *([f"site:{host.removeprefix('www.')}"] if host else [])]
    return [hashlib.sha256(key.encode()).hexdigest() for key in keys]


# Every key a purge in these tests could leave behind, so cleaning up never depends on a test having passed.
FORGET = "delete from public.removed where key_hash in ({})".format(
    ", ".join(
        f"'{key_hash}'" for row in (*ARTISTS, GONE, STAY, COPY, KEEP, FAN, *CROWD) for key_hash in key_hashes(row)
    )
)

COUNTS = """
select (select count(*) from public.artists where slug like 'zz-test-%')::int as artists,
       (select count(*) from public.works w join public.artists a on a.id = w.artist_id
        where a.slug like 'zz-test-%')::int as works
"""
SEEDED = [{"artists": len(ARTISTS), "works": sum(map(len, STYLES.values()))}]


def works_of(name: str, artist_id: int, styles: list[list[float] | None]) -> list[dict[str, Any]]:
    # Inserted in reverse display order, so anything sorted by id instead of position would fail.
    return [
        {
            "artist_id": artist_id,
            "page_url": f"https://zz-test-{name}.invalid/work/{position}",
            "image_url": f"https://zz-test-{name}.invalid/thumb/{position}.jpg",
            "title": f"{name} {position}",
            "style_embedding": styles[position],
            "position": position,
        }
        for position in reversed(range(len(styles)))
    ]


def seed() -> dict[str, int]:
    ids = {row["slug"]: row["id"] for row in db.upsert("artists", ARTISTS, "slug", "id,slug")}
    works = [row for name, styles in STYLES.items() for row in works_of(name, ids[f"zz-test-{name}"], styles)]
    db.upsert("works", works, "artist_id,image_url", "id")
    db.rpc("refresh_style_centroids", {}, "secret")  # as the push stage does after loading works
    return ids


def clean() -> None:
    db.sql(f"""
        delete from public.artists where slug like 'zz-test-%';
        delete from public.removal_requests where artist_slug like 'zz-test-%';
        {FORGET};
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


def refused(response: httpx.Response, constraint: str) -> bool:
    """The database refused the row, and for the reason the test means."""
    return response.status_code in (400, 409) and constraint in response.text


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
  'triggers', (select jsonb_agg(pg_get_triggerdef(t.oid) order by t.tgname)
               from pg_trigger t join pg_class c on c.oid = t.tgrelid
               where c.relnamespace = 'public'::regnamespace and not t.tgisinternal),
  'policies', (select count(*) from pg_policies where schemaname = 'public'),
  'rows', jsonb_build_array((select count(*) from public.artists), (select count(*) from public.works),
                            (select count(*) from public.removal_requests), (select count(*) from public.removed))
) as snapshot
"""


def test_migrate_twice_changes_nothing():
    before = db.sql(SNAPSHOT)
    names = {function[0] for function in before[0]["snapshot"]["functions"]}
    assert names >= {*PUBLIC_FUNCTIONS, *SERVICE_FUNCTIONS, *HELPER_FUNCTIONS}  # the snapshot really sees the schema
    db.migrate()
    db.migrate()
    assert db.sql(SNAPSHOT) == before  # same objects (same oids, so nothing was dropped) and same rows


# 2. Upsert the same artist and works twice: no duplicates.


VERSIONS = """
select md5(string_agg(w.id || ':' || w.xmin::text || ':' || w.ctid::text, ',' order by w.id)) as of_every_work
from public.works w join public.artists a on a.id = w.artist_id
where a.slug like 'zz-test-%'
"""


def test_upsert_twice_makes_no_duplicates(seeded):
    assert db.sql(COUNTS) == SEEDED
    versions = db.sql(VERSIONS)
    assert seed() == seeded  # the same rows were updated in place
    assert db.sql(COUNTS) == SEEDED
    assert db.sql(VERSIONS) == versions  # and no work was rewritten: every row version is the one from before
    assert db.rpc("refresh_style_centroids", {}, "secret") == 0  # nor any centroid

    edited = {**BY_SLUG["zz-test-delta"], "profile": "edited"}
    changed = db.upsert("artists", [edited], "slug", "id,profile")
    assert changed == [{"id": seeded["zz-test-delta"], "profile": "edited"}]  # duplicates are merged, not skipped
    assert db.upsert("artists", [], "slug") == []

    # A work that did change is written and returned; sent again, it is left alone.
    retitled = {**works_of("delta", seeded["zz-test-delta"], STYLES["delta"])[0], "title": "retitled"}
    assert db.upsert("works", [retitled], "artist_id,image_url", "title") == [{"title": "retitled"}]
    assert db.upsert("works", [retitled], "artist_id,image_url", "title") == []
    assert db.sql(VERSIONS) != versions
    assert db.sql(COUNTS) == SEEDED


def test_one_row_per_handle_and_per_site(seeded):
    # No two rows can share a removal key, so a purge never leaves a second copy of the artist behind.
    def post(row: dict[str, Any]) -> httpx.Response:
        return db.data_api("POST", "artists", "secret", json=row)

    assert refused(post(artist("twin", da_username="ZZ-TEST-ALPHA")), "artists_da_username_idx")
    assert refused(post(artist("twin", site_url="http://zz-test-alpha.invalid:8080/me")), "artists_site_host_idx")
    assert db.sql(COUNTS) == SEEDED


# 3. search_artists.


def test_search_by_keyword_only():
    # Alpha has the word in its name and profile, beta in its profile, gamma nowhere.
    assert search(q=WORD) == ["zz-test-alpha", "zz-test-beta"]
    assert search(q=f"{WORD} armor painterly") == ["zz-test-alpha", "zz-test-beta"]  # any word may match
    assert search(q="zzqxnothing") == []


def test_search_by_keyword_and_embedding():
    # Keywords rank alpha, beta. Meaning ranks beta, gamma, alpha. Reciprocal rank fusion:
    # beta 1/62 + 1/61, alpha 1/61 + 1/63, gamma 1/62 (found by meaning alone).
    ranked = search(q=WORD, q_embedding=QUERY, p_mediums=[MEDIUM])
    assert ranked == ["zz-test-beta", "zz-test-alpha", "zz-test-gamma"]
    # Without keywords the order is by meaning alone.
    assert search(q_embedding=QUERY, p_mediums=[MEDIUM]) == ["zz-test-beta", "zz-test-gamma", "zz-test-alpha"]


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


def test_search_survives_odd_input():
    # Each odd query still finds the word that follows it, so it was parsed and searched, not swallowed.
    odd = ("hand-painted", "AT&T", "http://x.invalid/?a=1&b=2", "it's", "!!! | & <-> ()", "'; drop table artists; --")
    for q in (*odd, "the", "  ", "é漢字"):
        assert {"zz-test-alpha", "zz-test-beta"} <= set(search(q=f"{q} {WORD}", p_mediums=[MEDIUM])), q
    # An embedding of the wrong size is refused, not answered with a wrong ranking.
    wrong = db.data_api("POST", "rpc/search_artists", "publishable", json={"q": "", "q_embedding": [1.0, 0.0]})
    assert wrong.status_code == 400 and "dimensions" in wrong.text


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


def test_cards_show_four_works_in_display_order():
    [card] = db.rpc("search_artists", {"q": WORD, "p_open_only": True})
    assert set(card) == {"slug", "name", "profile", "specialties", "mediums", "open_for_work", "works"}
    assert [work["title"] for work in card["works"]] == ["alpha 0", "alpha 1", "alpha 2", "alpha 3"]
    assert card["works"][0] == {
        "image_url": "https://zz-test-alpha.invalid/thumb/0.jpg",
        "page_url": "https://zz-test-alpha.invalid/work/0",
        "title": "alpha 0",
    }

    [profile] = db.rpc("get_artist", {"p_slug": "zz-test-alpha"})
    assert set(profile) == set(card) | {"links"}
    assert profile["links"] == BY_SLUG["zz-test-alpha"]["links"]
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


# 5. The publishable key.


def test_publishable_key_cannot_touch_tables(seeded):
    for table in TABLES:
        assert denied(db.data_api("GET", table, "publishable")), table
    # The same read with the secret key finds the row, so the check above is not empty.
    params = {"slug": "eq.zz-test-alpha", "select": "slug"}
    assert db.data_api("GET", "artists", "secret", params=params).json() == [{"slug": "zz-test-alpha"}]

    assert denied(db.data_api("POST", "artists", "publishable", json=artist("intruder")))
    hidden = {"slug": "eq.zz-test-hidden"}
    assert denied(db.data_api("PATCH", "artists", "publishable", params=hidden, json={"hidden": False}))
    assert denied(db.data_api("DELETE", "artists", "publishable", params={"slug": "like.zz-test-*"}))
    assert db.sql(COUNTS) == SEEDED
    assert db.sql("select hidden from public.artists where slug = 'zz-test-hidden'") == [{"hidden": True}]


def test_publishable_key_runs_the_public_functions_only():
    assert slugs(db.rpc("search_artists", {"q": WORD}, "publishable")) == ["zz-test-alpha", "zz-test-beta"]
    assert slugs(db.rpc("similar_artists", {"p_slug": "zz-test-alpha", "p_limit": 1}, "publishable")) == [
        "zz-test-beta"
    ]
    assert slugs(db.rpc("get_artist", {"p_slug": "zz-test-alpha"}, "publishable")) == ["zz-test-alpha"]
    assert db.rpc("request_removal", {"p_slug": "zz-test-nobody"}, "publishable") is True

    # Each of these exists and works for the service role (see the removal tests); the public is refused.
    for name, args in (
        ("purge_removed", {"p_slugs": ["zz-test-alpha"]}),
        ("refresh_style_centroids", {}),
        ("reject_removal", {"p_slug": "zz-test-alpha"}),
        ("work_cards", {"p_artist_id": 1}),
        ("removal_keys", {"p_da_username": "zz-test-alpha", "p_site_url": None}),
    ):
        assert denied(db.data_api("POST", f"rpc/{name}", "publishable", json=args)), name
    assert db.sql(COUNTS) == SEEDED


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
        "definer_functions": PUBLIC_FUNCTIONS,
        "functions_without_fixed_path": None,
        "purge_everything_is_gone": True,
    }


# 6. Removal.

REQUESTS = """
select r.artist_slug as slug, r.note, r.hidden_at is not null as hid, a.hidden
from public.removal_requests r left join public.artists a on a.slug = r.artist_slug
where r.artist_slug like 'zz-test-%' order by 1
"""


def fresh(*rows: dict[str, Any], hides: int) -> None:
    """Start a removal test: no synthetic requests, the given artists visible, and room under the hide cap."""
    recent = db.sql(
        "select count(*)::int as n from public.removal_requests "
        "where hidden_at > now() - interval '24 hours' and artist_slug not like 'zz-test-%'"
    )
    if HIDE_CAP - recent[0]["n"] < hides:
        pytest.skip("real removal requests have used up the auto-hide cap for now")
    db.sql(f"delete from public.removal_requests where artist_slug like 'zz-test-%'; {FORGET};")
    assert len(db.upsert("artists", list(rows), "slug", "id")) == len(rows)


def ask(row: dict[str, Any], note: str | None = None) -> None:
    assert db.rpc("request_removal", {"p_slug": row["slug"], "p_note": note}, "publishable") is True


def test_removal_request_hides_at_once(seeded):
    fresh(GONE, STAY, hides=2)
    assert visible(GONE) and visible(STAY)
    assert db.rpc("request_removal", {"p_slug": "zz-test-nobody", "p_note": "x"}) is True  # same answer, nothing logged
    assert db.sql(REQUESTS) == []

    ask(GONE, "please remove me")
    ask(STAY)
    assert not visible(GONE) and not visible(STAY)
    assert db.sql(REQUESTS) == [
        {"slug": GONE["slug"], "note": "please remove me", "hid": True, "hidden": True},
        {"slug": STAY["slug"], "note": None, "hid": True, "hidden": True},
    ]
    assert GONE["slug"] not in slugs(db.rpc("similar_artists", {"p_slug": "zz-test-alpha"}))
    assert db.rpc("similar_artists", {"p_slug": GONE["slug"]}) == []

    ask(GONE, "x" * 5000)  # asking again changes nothing, and a long note cannot fill the table
    ask(BY_SLUG["zz-test-hidden"], "y" * 5000)  # already hidden by the pipeline: logged, and not counted as a hide
    assert db.sql(REQUESTS) == [
        {"slug": GONE["slug"], "note": "please remove me", "hid": True, "hidden": True},
        {"slug": "zz-test-hidden", "note": "y" * 1000, "hid": False, "hidden": True},
        {"slug": STAY["slug"], "note": None, "hid": True, "hidden": True},
    ]

    # A pipeline rerun before the owner has looked does not bring them back, even if it says hidden: false.
    db.upsert("artists", [GONE, STAY], "slug", "id")
    assert not visible(GONE) and not visible(STAY)


def test_removal_requests_cannot_hide_the_whole_index(seeded):
    fresh(*CROWD, hides=1)
    recent = "select count(*)::int as n from public.removal_requests where hidden_at > now() - interval '24 hours'"
    room = HIDE_CAP - db.sql(recent)[0]["n"]

    # An attacker's script asks for everyone. The last free slot is asked for by many at the same moment,
    # which is how a cap gets raced past.
    for row in CROWD[: room - 1]:
        ask(row)
    rest = CROWD[room - 1 :]
    with ThreadPoolExecutor(len(rest)) as pool:
        assert all(pool.map(visible, rest))  # this also opens one connection each, so the requests land together
        list(pool.map(ask, rest))

    requests = db.sql(REQUESTS)
    assert len(requests) == len(CROWD)  # every request is logged for the owner
    assert all(request["hid"] == request["hidden"] for request in requests)
    assert sum(request["hidden"] for request in requests) == room  # and only up to the cap were hidden
    assert db.sql(recent) == [{"n": HIDE_CAP}]
    still_shown = [row for row in CROWD if visible(row)]
    assert len(still_shown) == len(CROWD) - room >= 10

    ask(still_shown[0])  # asking again does not get past the cap
    assert visible(still_shown[0])
    assert db.sql(recent) == [{"n": HIDE_CAP}]


def test_purge_removes_only_the_slugs_the_owner_names(seeded):
    fresh(GONE, STAY, COPY, hides=2)
    [gone] = db.sql(f"select id from public.artists where slug = '{GONE['slug']}'")
    db.upsert("works", works_of("gone", gone["id"], [E1, E2]), "artist_id,image_url", "id")
    ask(GONE)
    ask(STAY)
    state = f"""
        select (select count(*) from public.artists where slug = '{GONE["slug"]}')::int as gone,
               (select count(*) from public.works where artist_id = {gone["id"]})::int as gone_works,
               (select count(*) from public.artists where slug = '{COPY["slug"]}')::int as copy,
               (select count(*) from public.artists where slug = '{STAY["slug"]}')::int as stay,
               (select count(*) from public.removal_requests where artist_slug like 'zz-test-%')::int as requests
    """
    pending = [{"gone": 1, "gone_works": 2, "copy": 1, "stay": 1, "requests": 2}]
    hashes = ", ".join(f"'{key_hash}'" for key_hash in GONE_HASHES)
    remembered = f"select key_hash from public.removed where key_hash in ({hashes}) order by key_hash"

    # "Purge everything pending" no longer exists, for anyone.
    everything = db.data_api("POST", "rpc/purge_removed", "secret", json={})
    assert everything.status_code == 404 and everything.json()["code"] == "PGRST202"
    # The public cannot purge at all.
    assert denied(db.data_api("POST", "rpc/purge_removed", "publishable", json={"p_slugs": [GONE["slug"]]}))
    # Slugs without a request are left alone, so a typo cannot delete an artist who never asked.
    assert db.rpc("purge_removed", {"p_slugs": []}, "secret") == 0
    assert db.rpc("purge_removed", {"p_slugs": ["zz-test-alpha", "zz-test-nobody"]}, "secret") == 0
    assert db.sql(state) == pending
    assert db.sql(remembered) == []
    assert visible(BY_SLUG["zz-test-alpha"])

    # The owner purges one reviewed slug. That artist and its works are gone, and so is the second copy
    # of the artist on a subdomain of the same site. Only the purged artist's hashed keys remain.
    assert db.rpc("purge_removed", {"p_slugs": [GONE["slug"]]}, "secret") == 2
    waiting = [{"gone": 0, "gone_works": 0, "copy": 0, "stay": 1, "requests": 1}]  # the other request still waits
    assert db.sql(state) == waiting
    assert [row["key_hash"] for row in db.sql(remembered)] == GONE_HASHES
    assert not visible(STAY)
    assert db.rpc("purge_removed", {"p_slugs": [GONE["slug"]]}, "secret") == 0


def test_reject_removal_restores_the_artist(seeded):
    fresh(STAY, hides=1)
    ask(STAY, "not really me")
    ask(BY_SLUG["zz-test-hidden"])
    assert not visible(STAY)

    assert denied(db.data_api("POST", "rpc/reject_removal", "publishable", json={"p_slug": STAY["slug"]}))
    assert not visible(STAY)

    assert db.rpc("reject_removal", {"p_slug": STAY["slug"]}, "secret") is True
    assert visible(STAY)
    assert db.rpc("reject_removal", {"p_slug": STAY["slug"]}, "secret") is False  # nothing left to reject
    # Rejecting a request that hid nobody does not show an artist the pipeline had hidden.
    assert db.rpc("reject_removal", {"p_slug": "zz-test-hidden"}, "secret") is True
    assert db.sql(REQUESTS) == []
    assert db.sql("select hidden from public.artists where slug = 'zz-test-hidden'") == [{"hidden": True}]

    db.upsert("artists", [STAY], "slug", "id")  # and the pipeline can update the artist again
    assert visible(STAY)


def bar_gone() -> None:
    """Put GONE's keys on the removal list, as a purge of GONE leaves them (the purge test proves the hashes)."""
    values = ", ".join(f"('{key_hash}')" for key_hash in GONE_HASHES)
    db.sql(f"""
        delete from public.artists where slug in ('zz-test-gone', 'zz-test-back');
        insert into public.removed (key_hash) values {values} on conflict do nothing;
    """)


def test_key_on_the_removal_list_deletes_every_row_it_bars(seeded):
    fresh(KEEP, FAN, hides=0)
    [keep] = db.sql(f"select id from public.artists where slug = '{KEEP['slug']}'")
    db.upsert("works", works_of("keep", keep["id"], [E1]), "artist_id,image_url", "id")
    assert visible(KEEP) and visible(FAN)

    # The owner removes a site by hand (what pipeline/store.py's mark_removed does) while its artist is here.
    site_key = key_hashes(KEEP)[1]
    assert db.upsert("removed", [{"key_hash": site_key}], "key_hash", "key_hash") == [{"key_hash": site_key}]
    # No row that key bars stays in the index, public or not: not the artist, not the one who links to the site.
    assert db.sql(f"""
        select (select count(*) from public.artists where slug in ('{KEEP["slug"]}', '{FAN["slug"]}'))::int as artists,
               (select count(*) from public.works where artist_id = {keep["id"]})::int as works
    """) == [{"artists": 0, "works": 0}]
    assert db.sql(COUNTS)[0]["artists"] >= len(ARTISTS)  # and nobody else went with them
    assert visible(BY_SLUG["zz-test-alpha"])


def test_existing_row_cannot_move_onto_a_removed_key(seeded):
    fresh(STAY, hides=0)
    bar_gone()
    row = f"select da_username, site_url, links, hidden from public.artists where slug = '{STAY['slug']}'"
    before = db.sql(row)
    patch = {"params": {"slug": f"eq.{STAY['slug']}"}, "headers": {"Prefer": "return=representation"}}
    for move in (
        {"da_username": "zz-test-GONE"},
        {"site_url": "https://zz-test-gone.invalid/new"},
        {"site_url": "https://m.zz-test-gone.invalid/"},
        {"links": ["https://www.zz-test-gone.invalid/"]},
        {"links": ["https://zz-test-stay.invalid/", "https://deviantart.com/ZZ-Test-Gone"]},
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
    near = artist(
        "near",
        da_username="zz-test-gone2",
        site_url="https://notzz-test-gone.invalid/",  # ends like the removed host, but is not under it
        links=["https://www.deviantart.com/zz-test-gone2", "https://zz-test-gone.invalid.example.org/"],
    )
    try:
        assert db.upsert("artists", [near], "slug", "slug") == [{"slug": "zz-test-near"}]
    finally:
        db.sql("delete from public.artists where slug = 'zz-test-near'")


READDS = [
    ("the same row again", GONE),
    ("a new slug and the handle in another case", artist("back", da_username="zz-test-GONE")),
    ("a new slug and the site without www", artist("back", site_url="https://zz-test-gone.invalid/")),
    ("the site over http on another port", artist("back", site_url="http://www.zz-test-gone.invalid:8080")),
    ("the site under www.www.", artist("back", site_url="https://www.www.zz-test-gone.invalid/")),
    ("the site under shop.", artist("back", site_url="https://shop.zz-test-gone.invalid/")),
    ("the site under m.", artist("back", site_url="https://m.zz-test-gone.invalid/")),
    ("the site two levels down", artist("back", site_url="https://a.b.zz-test-gone.invalid/")),
    ("the site only in the links", artist("back", links=["https://zz-test-gone.invalid/about"])),
    ("a subdomain of the site only in the links", artist("back", links=["https://blog.zz-test-gone.invalid/"])),
    (
        "the DeviantArt profile only in the links",
        artist("back", links=["https://zz-test-back.invalid/", "https://www.deviantart.com/ZZ-Test-Gone/gallery"]),
    ),
    ("a trailing dot on the host", artist("back", site_url="https://zz-test-gone.invalid./")),
    ("a user in the URL", artist("back", site_url="https://me@zz-test-gone.invalid/")),
    ("capitals in the host", artist("back", site_url="https://ZZ-Test-Gone.INVALID/")),
    *[
        (f"the site followed by {extra!r}", artist("back", site_url=f"https://zz-test-gone.invalid{extra}"))
        for extra in (" ", "\n", "\t", "\xa0", "​")
    ],
    *[
        (f"the handle followed by {extra!r}", artist("back", da_username=f"zz-test-gone{extra}"))
        for extra in (" ", "\n", "\xa0", "​", "　", "﻿", "\x0b")
    ],
    ("the handle after a space", artist("back", da_username=" zz-test-gone")),
    ("no handle and no site", artist("back", da_username=None)),
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


def digest(key: str) -> str:
    return hashlib.sha256(key.encode()).hexdigest()


# The removal list of record is the `removed` table, and pipeline/store.py imports it. These hosts are never
# stored or fetched: they need a real public suffix, because the store reads no site from a name like x.invalid.
STORE_CASES: list[tuple[str, str | None, list[str]]] = [
    ("ZZ-Test_Jane", "https://www.zz-test-jane.com:8443/art?p=1", []),
    (
        "Jane",
        "https://shop.zz-test-jane.com/x",
        [
            "https://www.deviantart.com/Old-Name/gallery",
            "https://deviantart.com/zz_under?tab=1",
            "https://www.deviantart.com/tag/fantasy",  # a DeviantArt route, not a profile
            "https://x.deviantart.com/y",
            "https://www.artstation.com/jane",  # a platform: nobody's own site
            "https://m.blog.zz-test-other.co.uk/?a=1",
        ],
    ),
    ("a", None, ["https://a.b.c.zz-test-deep.org:8443/p", "http://www.www.zz-test-twice.net"]),
    ("z" * 40, "http://zz-test-plain.art", ["https://notdeviantart.com/b"]),
]


def keys_in_the_database(function: str, handle: str, site: str | None, links: list[str] | None = None) -> list[str]:
    site_sql = f"'{site}'" if site else "null"
    links_sql = "" if links is None else f", '{json.dumps(links)}'"
    return db.sql(f"select public.{function}('{handle}', {site_sql}{links_sql}) as keys")[0]["keys"]


def test_keys_a_purge_stores_are_keys_the_pipeline_store_looks_for():
    from pipeline import store  # another builder's module: imported here so that only these two tests depend on it

    for handle, site, _ in STORE_CASES:
        stored = keys_in_the_database("removal_keys", handle, site)
        assert stored[0] == digest(store.artist_key(handle)), handle
        if site:
            # The store looks a site up under its host and under each parent domain.
            labels = (urlsplit(site).hostname or "").split(".")
            looked_for = {digest("site:" + ".".join(labels[n:])) for n in range(len(labels) - 1)}
            assert len(stored) == 2 and stored[1] in looked_for, site
        else:
            assert len(stored) == 1


def test_a_removal_made_by_the_pipeline_store_bars_the_same_rows_here():
    from pipeline import store

    checked = 0
    for handle, site, links in STORE_CASES:
        barring = set(keys_in_the_database("barring_keys", handle, site, links))
        for named in filter(None, [handle, site, *links]):
            try:
                key = store.normalize(named)  # what the store puts on the list for this handle, profile or site
            except ValueError:  # the store removes nothing for this one: a platform, or a DeviantArt route
                continue
            assert digest(key) in barring, (handle, named)
            checked += 1
    assert checked >= 10  # most of the handles, sites and links above are ones the store can remove


# 7. No image bytes, and under the size budget.

GOOD_WORK = {"page_url": "https://zz-test-alpha.invalid/work/99", "image_url": "https://zz-test-alpha.invalid/t/99.jpg"}
DATA_URL = "data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNk"
IMAGE = "data:image/png;base64," + "A" * 200_000  # an image as text: what a critic stored in every text column
BAD_ARTISTS = [
    ("a space in the handle", {"da_username": "zz test"}, "artists_da_username_clean"),
    ("an accent in the handle", {"da_username": "zz-tést"}, "artists_da_username_clean"),
    ("an empty handle", {"da_username": ""}, "artists_da_username_clean"),
    ("a handle of 41 characters", {"da_username": "z" * 41}, "artists_da_username_clean"),
    ("a data: URL as the site", {"site_url": DATA_URL}, "artists_site_url_clean"),
    ("a fragment on the site", {"site_url": "https://zz-test-bad.invalid/#top"}, "artists_site_url_clean"),
    ("an IP address as the site", {"site_url": "https://192.0.2.7/"}, "artists_site_url_clean"),
    ("a host with no domain", {"site_url": "https://localhost/"}, "artists_site_url_clean"),
    ("an accent in the path", {"site_url": "https://zz-test-bad.invalid/café"}, "artists_site_url_clean"),
    (
        "a site of 2,049 characters",
        {"site_url": "https://zz-test-bad.invalid/".ljust(2049, "a")},
        "artists_site_url_clean",
    ),
    ("links that are not a list", {"links": {"site": "https://zz-test-bad.invalid/"}}, "artists_links_clean"),
    ("a link that is not text", {"links": [{"url": "https://zz-test-bad.invalid/"}]}, "artists_links_clean"),
    ("a data: URL among the links", {"links": ["https://zz-test-bad.invalid/", DATA_URL]}, "artists_links_clean"),
    ("a data: fragment in a link", {"links": [f"https://zz-test-bad.invalid/#{DATA_URL}"]}, "artists_links_clean"),
    ("51 links", {"links": [f"https://zz-test-bad.invalid/{n}" for n in range(51)]}, "artists_links_clean"),
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
]
URLS, SIZES = "works_urls_clean", "works_sizes"
BAD_WORKS = [
    ("a data: URL as the image", {"image_url": DATA_URL}, URLS),
    ("a data: fragment on the image", {"image_url": f"https://zz-test-alpha.invalid/t.jpg#{DATA_URL}"}, URLS),
    ("a data: fragment on the page", {"page_url": f"https://zz-test-alpha.invalid/work#{DATA_URL}"}, URLS),
    ("a space in the page URL", {"page_url": "https://zz-test-alpha.invalid/my work"}, URLS),
    ("an image URL of 2,049 characters", {"image_url": "https://zz-test-alpha.invalid/".ljust(2049, "a")}, URLS),
    ("an image as the title", {"title": IMAGE}, SIZES),
    ("an image as the description", {"description": IMAGE}, SIZES),
    ("an image among the tags", {"tags": ["armor", IMAGE]}, SIZES),
    ("a title of 201 characters", {"title": "t" * 201}, SIZES),
    ("a description of 2,001 characters", {"description": "d" * 2001}, SIZES),
    ("41 tags", {"tags": [f"t{n}" for n in range(41)]}, SIZES),
    ("a tag of 61 characters", {"tags": ["t" * 61]}, SIZES),
]


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


@pytest.mark.parametrize(("label", "fields", "constraint"), BAD_ARTISTS, ids=[bad[0] for bad in BAD_ARTISTS])
def test_artist_must_be_canonical(label: str, fields: dict[str, Any], constraint: str):
    response = db.data_api("POST", "artists", "secret", json=artist("bad", **fields))
    assert refused(response, constraint), response.text[:300]
    assert db.sql("select count(*)::int as n from public.artists where slug like 'zz-test-bad%'") == [{"n": 0}]


def test_longest_clean_values_are_accepted(seeded):
    # The limits in the two tests around this one are exact: one character less is fine.
    longest = artist("long", da_username="z" * 40, site_url="https://zz-test-long.invalid/".ljust(2048, "a"))
    longest["links"] = [f"https://zz-test-long.invalid/{n}" for n in range(50)]
    longest |= {"name": "n" * 120, "profile": "p" * 2000, "specialties": ["s" * 60] * 20, "mediums": ["m" * 60] * 20}
    work = {**GOOD_WORK, "artist_id": seeded["zz-test-alpha"]}
    work["image_url"] = "https://zz-test-alpha.invalid/".ljust(2048, "a")
    work |= {"title": "t" * 200, "description": "d" * 2000, "tags": ["t" * 60] * 40}
    try:
        assert db.upsert("artists", [longest], "slug", "slug") == [{"slug": "zz-test-long"}]
        assert len(db.upsert("works", [work], "artist_id,image_url", "id")) == 1
    finally:
        db.sql("""
            delete from public.artists where slug = 'zz-test-long';
            delete from public.works where page_url = 'https://zz-test-alpha.invalid/work/99';
        """)


@pytest.mark.parametrize(("label", "fields", "constraint"), BAD_WORKS, ids=[bad[0] for bad in BAD_WORKS])
def test_work_must_be_clean_and_short(seeded, label: str, fields: dict[str, Any], constraint: str):
    # A data: URL, data hidden in a fragment, or an image pasted in as text would be image bytes in a
    # text column (hard rule 1).
    alpha = seeded["zz-test-alpha"]
    response = db.data_api("POST", "works", "secret", json={**GOOD_WORK, "artist_id": alpha, **fields})
    assert refused(response, constraint), response.text[:300]
    assert db.sql(f"select count(*)::int as n from public.works where artist_id = {alpha}") == [
        {"n": len(STYLES["alpha"])}
    ]


def test_removal_tables_hold_no_long_text():
    def post(table: str, row: dict[str, Any]) -> httpx.Response:
        return db.data_api("POST", table, "secret", json=row)

    assert refused(post("removal_requests", {"artist_slug": "zz-test-alpha", "note": IMAGE}), "removal_requests_sizes")
    assert refused(post("removal_requests", {"artist_slug": "zz-test-".ljust(81, "a")}), "removal_requests_sizes")
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

    secrets = [value for value in load_env().values() if len(value) >= 8]
    leaked = any(secret in str(error.value) for error in (refusal, failed) for secret in secrets)
    assert not leaked


# Full scale, on request: RUN_SCALE=1. Loads and deletes 3,000 artists and 30,040 works, then compacts
# the two tables (a brief lock), so it is not part of the normal run.

BULK = """
insert into public.artists
  (slug, name, da_username, site_url, profile, specialties, mediums, open_for_work, profile_embedding)
select 'zz-test-bulk-' || g, 'Bulk ' || g, 'zz-test-bulk-' || g, 'https://zz-test-bulk-' || g || '.invalid',
       'Character artist. Painterly fantasy armor and creature designs, mostly digital. ' || md5(g::text),
       array['character design', 'creature design'], array['digital'], g % 2 = 0,
       (select array_agg(random() - 0.5) from generate_series(1, 1024) where g = g)::extensions.halfvec(1024)
from generate_series(1, 3000) g;

-- Ten works each, and fifty for the seed: a big portfolio must not slow the scan down.
insert into public.works (artist_id, page_url, image_url, title, description, tags, style_embedding, position)
select a.id, a.site_url || '/portfolio/work-' || n, a.site_url || '/images/thumbnails/work-' || n || '.jpg',
       'Work title ' || n,
       'A knight in ornate plate armor stands before a ruined cathedral, painted with loose, textured '
         || 'brushwork and a warm, muted palette. ' || md5(random()::text),
       array['digital painting', 'fantasy', 'armor', 'character', 'painterly', 'knight'],
       (select array_agg(random() - 0.5) from generate_series(1, 768) where a.id = a.id and n = n
       )::extensions.halfvec(768),
       n
from public.artists a cross join generate_series(0, 49) n
where a.slug like 'zz-test-bulk-%' and (n < 10 or a.slug = 'zz-test-bulk-1');
"""


# What db.upsert sends for every work of the synthetic artists: an insert that updates on conflict.
# {title} is the title to send, so the same statement can resend the rows unchanged or changed.
REUPSERT = """
with written as (
  insert into public.works (artist_id, page_url, image_url, title, description, tags, style_embedding, position)
  select w.artist_id, w.page_url, w.image_url, {title}, w.description, w.tags, w.style_embedding, w.position
  from public.works w join public.artists a on a.id = w.artist_id
  where a.slug like 'zz-test-bulk-%'
  on conflict (artist_id, image_url) do update set
    page_url = excluded.page_url, title = excluded.title, description = excluded.description,
    tags = excluded.tags, style_embedding = excluded.style_embedding, position = excluded.position
  returning 1
)
select count(*)::int as rewritten from written
"""
SCALE = """
select (select count(*) from public.artists where slug like 'zz-test-bulk-%')::int as artists,
       (select count(*) from public.works w join public.artists a on a.id = w.artist_id
        where a.slug like 'zz-test-bulk-%')::int as works,
       round(pg_database_size(current_database()) / 1048576.0)::int as megabytes
"""


def server_ms(query: str) -> list[float]:
    """Three runs in a row: the first is the cold one."""
    plans = [db.sql("explain (analyze, costs off, buffers off) " + query) for _ in range(3)]
    return [float(plan[-1]["QUERY PLAN"].split()[2]) for plan in plans]  # "Execution Time: 123.456 ms"


def public_seconds(name: str, args: dict[str, Any], rows: int = 24) -> float:
    """One call through the publishable key. It raises if the public role's 3 s limit cuts the query off."""
    started = time.perf_counter()
    assert len(db.rpc(name, args, "publishable")) == rows
    return time.perf_counter() - started


@pytest.mark.skipif(os.environ.get("RUN_SCALE") != "1", reason="set RUN_SCALE=1 to load 3,000 artists and 30,000 works")
def test_full_scale_fits_the_budget_and_the_public_timeout():
    similar = ("similar_artists", {"p_slug": "zz-test-bulk-1"})  # the seed with fifty works
    with_embedding = (
        "select * from public.search_artists('painterly fantasy character', "
        "(select profile_embedding from public.artists where slug = 'zz-test-bulk-2'))"
    )
    limit = PUBLIC_TIMEOUT_MS / 1000

    def report(label: str, seconds: list[float]) -> None:
        print(f"{label}: " + ", ".join(f"{s:.2f} s" for s in seconds), flush=True)

    try:
        # A load, as the push stage does it: rows, then what similar_artists reads, then vacuum.
        db.sql(BULK)
        started = time.perf_counter()
        assert db.refresh_style_centroids() >= 3000
        report("\nrefresh_style_centroids", [time.perf_counter() - started])
        db.vacuum_analyze()
        [loaded] = db.sql(SCALE)
        print(f"loaded: {loaded}", flush=True)
        assert (loaded["artists"], loaded["works"]) == (3000, 30_040)
        assert loaded["megabytes"] < 400
        after_load = [public_seconds(*similar) for _ in range(3)]
        report("similar_artists as the public, after the load", after_load)

        # The push stage sends every work again. Rows that have not changed are not rewritten at all,
        # and the public notices nothing: six calls in a row, straight away.
        assert db.sql(REUPSERT.format(title="w.title")) == [{"rewritten": 0}]
        after_resend = [public_seconds(*similar) for _ in range(6)]
        report("similar_artists as the public, right after every work was sent again unchanged", after_resend)
        # Then the worst case, every work changed: each is rewritten, and the old versions stay until vacuum
        # runs. similar_artists must still answer at once, without waiting for it: six more public calls.
        assert db.sql(REUPSERT.format(title="w.title || ' (again)'")) == [{"rewritten": 30_040}]
        after_rewrite = [public_seconds(*similar) for _ in range(6)]
        report("similar_artists as the public, right after every work was rewritten", after_rewrite)
        assert max(*after_load, *after_resend, *after_rewrite) < limit
        [rewritten] = db.sql(SCALE)
        print(f"after every work was rewritten: {rewritten}", flush=True)
        assert rewritten["megabytes"] < 400

        # A search with a hundred thousand filter values is answered (its round trip is mostly the megabyte
        # of upload, so an answer is the proof), and so is an ordinary one, in time.
        huge_filter = public_seconds("search_artists", {"q": "", "p_mediums": ["digital", *["zz-none"] * 100_000]})
        search = public_seconds("search_artists", {"q": "painterly fantasy character artist"})
        report("search_artists as the public, ordinary and with 100,001 filter values", [search, huge_filter])
        assert search < limit

        timings = {"search_artists, keywords and embedding, before vacuum": server_ms(with_embedding)}
        db.vacuum_analyze()
        timings |= {
            "similar_artists, 50-work seed": server_ms("select * from public.similar_artists('zz-test-bulk-1')"),
            "similar_artists, 10-work seed": server_ms("select * from public.similar_artists('zz-test-bulk-2')"),
            "search_artists, keywords": server_ms("select * from public.search_artists('painterly fantasy character')"),
            "search_artists, keywords and embedding": server_ms(with_embedding),
            "get_artist": server_ms("select * from public.get_artist('zz-test-bulk-1')"),
        }
        for name, runs in timings.items():
            print(f"{name}, on the server: first {runs[0]:.0f} ms, best {min(runs):.0f} ms", flush=True)
        assert max(max(runs) for runs in timings.values()) < PUBLIC_TIMEOUT_MS
    finally:
        db.sql("delete from public.artists where slug like 'zz-test-bulk-%'")
        db.sql("vacuum full public.works")
        db.sql("vacuum full public.artists")
        db.sql("vacuum full public.artist_styles")
