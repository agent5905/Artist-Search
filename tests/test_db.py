"""Live tests for the schema, the search functions and the removal flow.

    uv run pytest tests/test_db.py --basetemp=.pytest_tmp_db
    RUN_SCALE=1 uv run pytest tests/test_db.py --basetemp=.pytest_tmp_db -s -k full_scale

These run against the real Supabase project, which holds real artists. Every artist they write has
a slug starting with zz-test-, the module fixture deletes those before and after, pass or fail, and no
statement here writes to, or deletes, any other artist's rows. The full-scale test refuses to run
while the project holds real artists.

Sections 5, 6 and 8 (what the site may call, the removal requests, the limits and the verifier's queue:
sql/002_functions.sql and 003_limits.sql) were written without a database to run them on: the owner runs
them, with SITE_TOKEN in .env. They count zz-test- visitors and take back what they added to the day's total
of reads, they close removal requests (which then keep no name, so they are found again by the time they
were made, and deleted), and for a moment they set the day's total to its cap: run them before the site is
public or when it is quiet.
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

from pipeline import config, crawl, db, discover, library, removals, store
from pipeline.config import ROOT, load_env

NEEDED = (
    "SUPABASE_PROJECT_REF",
    "SUPABASE_ACCESS_TOKEN",
    "VITE_SUPABASE_URL",
    "VITE_SUPABASE_PUBLISHABLE_KEY",
    "SUPABASE_SECRET_KEY",
    "SITE_TOKEN",
)
if not (ROOT / ".env").exists() or not all(load_env().get(name) for name in NEEDED):
    pytest.skip(".env lacks the Supabase keys or the site's token", allow_module_level=True)

TABLES = (
    "artists",
    "works",
    "artist_neighbors",
    "search_words",
    "removal_requests",
    "removal_secret",
    "removed",
    "client_counts",
    "site_secret",
)
# What the site's server functions call, as the public role with the publishable key and the site's token
# (sql/003_limits.sql). The public role may run these and nothing else.
PUBLIC_FUNCTIONS = [
    "client_wait",
    "site_artist",
    "site_removal_code",
    "site_request_removal",
    "site_search",
    "site_similar",
]
# What those run once the token is checked, and the rest of the service role's.
SITE_FUNCTIONS = ["get_artist", "request_removal", "search_artists", "similar_artists", "start_removal_proof"]
SERVICE_FUNCTIONS = [
    *SITE_FUNCTIONS,
    "artist_ids",
    "close_request",
    "confirm_removal",
    "delete_removed",
    "grant_request",
    "purge_removed",
    "refresh_search_words",
    "reject_removal",
    "removal_code",
    "removal_tally",
    "remove_artists",
    "replace_neighbors",
]
READS_PER_VISITOR, READS_PER_NETWORK, READS_A_DAY = 50, 100, 1000  # client_wait in sql/003_limits.sql, a day
REMOVALS_PER_VISITOR, REMOVALS_PER_NETWORK = 10, 20  # a day
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
    "removed_after_insert",
    "short_list",
    "site_check",
    "url_host",
    "works_before_write",
]
TOKEN = load_env()["SITE_TOKEN"]
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
    artist(  # the best match for every search, and it must never show
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
    # Loaded but not analyzed yet: no profile vector, no neighbours. They are found by their own words.
    artist("dana", name=f"{WORD2} Dana", tagline=f"{WORD2} painter", mediums=[MIXED]),
    artist("drew", search_text=f"studies {WORD2} sketches", mediums=[MIXED]),
    # For the misspelled query: one artist it is really about, one who only matches the word spelled right.
    artist("cora", tagline="character concept design", mediums=[TYPO]),
    artist("arti", tagline="artist artist artist", mediums=[TYPO]),
]
BY_SLUG = {row["slug"]: row for row in ARTISTS}

# How many works each has. Alpha has more than a card shows.
WORKS_EACH = {"alpha": 11, "beta": 2, "gamma": 1, "delta": 2, "epsilon": 2, "hidden": 2, "dana": 2}
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
SEEDED = [{"artists": len(ARTISTS), "works": sum(WORKS_EACH.values())}]


def thumbnail(name: str, position: int, token: str = "A") -> str:
    """A DeviantArt thumbnail URL in shape and length: a long signed token in the query (about 1,000 characters)."""
    signed = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9." + token * 760 + "." + "s" * 43
    return (
        "https://images-wixmp-ed30a86b8c4ca887773594c2.wixmp.com/f/0a1b2c3d-4e5f-6071-8293-a4b5c6d7e8f9/"
        f"dzz{position}-0a1b2c3d.jpg/v1/fill/w_300,h_300,q_70,strp/zz_test_{position}_by_zz_test_{name}.jpg?token={signed}"
    )


# What a few works say of themselves, for the search that puts matching works first on a card. The
# word is in no artist's own text and near no real word, so it changes nobody's place in a ranking.
MARK = "zzqxvorn"
ABOUT: dict[tuple[str, int], dict[str, Any]] = {
    ("alpha", 8): {"tags": [MARK, "ink"]},  # in the tags, which count most
    ("alpha", 6): {"description": f"a study of a {MARK}"},  # in the description, which counts least
    ("alpha", 3): {"description": MARK},  # the same match as the one above: they keep the artist's order
    ("beta", 1): {"title": f"{MARK} beta"},
}
UNMEASURED = {"dana"}  # whose works come without a size
WORK_FIELDS = {"image_url", "page_url", "title", "source", "width", "height"}  # what a card says of a work


def works_of(name: str, count: int, **owner: Any) -> list[dict[str, Any]]:
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
            "description": None,
            "tags": [],
            "position": position,
            "width": None if name in UNMEASURED else 300 + position,
            "height": None if name in UNMEASURED else 200,
            **ABOUT.get((name, position), {}),
        }
        for position in reversed(range(count))
    ]


WORKS = [row for name, count in WORKS_EACH.items() for row in works_of(name, count, artist_slug=f"zz-test-{name}")]


def neighbor(of: str, name: str, rank: int, score: float) -> dict[str, Any]:
    return {"artist_slug": f"zz-test-{of}", "neighbor_slug": f"zz-test-{name}", "rank": rank, "score": score}


# Style neighbours as the owner's machine ranks them, listed out of order. Alpha's are in neither id
# nor name order; one is hidden, and one is nobody the database knows.
NEIGHBORS = [
    neighbor("alpha", "beta", 5, 0.5),
    neighbor("alpha", "gamma", 1, 0.875),
    neighbor("alpha", "hidden", 2, 0.75),
    neighbor("alpha", "delta", 3, 0.625),
    neighbor("alpha", "nobody", 4, 0.5625),
    neighbor("beta", "alpha", 1, 0.5),
    neighbor("hidden", "alpha", 1, 0.75),
]
ALPHA_SIMILAR = ["zz-test-gamma", "zz-test-delta", "zz-test-beta"]  # what a visitor is shown for alpha


def seed() -> dict[str, int]:
    # The push stage's way in. It says how many neighbour rows it skipped: the one that names nobody.
    assert db.load(ARTISTS, WORKS, NEIGHBORS) == 1
    return db.rpc("artist_ids", {"p_slugs": list(BY_SLUG)}, "secret")


def clean(since: str = "infinity") -> None:
    """Delete the synthetic rows, and the removal requests closed since the module began: a closed request keeps
    no name to know it by."""
    db.sql(f"""
        delete from public.artists where slug like 'zz-test-%';
        delete from public.removal_requests
        where artist_slug like 'zz-test-%' or (outcome is not null and first_at >= '{since}');
        {FORGET};
        select public.refresh_search_words();  -- so the synthetic artists' words go with them
    """)


@pytest.fixture(scope="module", autouse=True)
def seeded():
    db.migrate()
    clean()  # in case an earlier run was killed
    began = db.sql("select now()::text as now")[0]["now"]
    try:
        yield seed()
    finally:
        clean(began)


def slugs(rows: list[dict[str, Any]]) -> list[str]:
    return [row["slug"] for row in rows]


def as_the_site(name: str, **args: Any) -> Any:
    """One of the site's functions, called as its server functions call it: the publishable key and the token."""
    return db.rpc(name, {"p_token": TOKEN, **args}, "publishable")


def search(**args: Any) -> list[str]:
    found = slugs(db.rpc("search_artists", {"q": "", **args}, "secret"))
    assert "zz-test-hidden" not in found
    return found


def similar(name: str, **args: Any) -> list[str]:
    return slugs(db.rpc("similar_artists", {"p_slug": f"zz-test-{name}", **args}, "secret"))


def visible(row: dict[str, Any]) -> bool:
    return slugs(db.rpc("get_artist", {"p_slug": row["slug"]}, "secret")) == [row["slug"]]


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
                            (select count(*) from public.removal_secret),
                            (select count(*) from public.artist_neighbors))
) as snapshot
"""


def test_migrate_twice_changes_nothing():
    before = db.sql(SNAPSHOT)
    names = {function[0] for function in before[0]["snapshot"]["functions"]}
    assert names >= {*PUBLIC_FUNCTIONS, *SERVICE_FUNCTIONS, *HELPER_FUNCTIONS}  # the snapshot really sees the schema
    code = db.rpc("start_removal_proof", {"p_slug": "zz-test-nobody"}, "secret")  # nobody's: it files no request
    db.migrate()
    db.migrate()
    # Same objects (same oids, so nothing was dropped and made again) and same rows.
    assert db.sql(SNAPSHOT) == before
    # And the same secret: a code an artist was told before a migration is still their code after it.
    assert db.rpc("start_removal_proof", {"p_slug": "zz-test-nobody"}, "secret") == code


# 2. Load the same artists and works twice: no duplicates.

VERSIONS = """
select md5(string_agg(w.id || ':' || w.xmin::text || ':' || w.ctid::text, ',' order by w.id)) as of_every_work,
       (select md5(string_agg(id || ':' || xmin::text || ':' || ctid::text, ',' order by id))
        from public.artists where slug like 'zz-test-%') as of_every_artist,
       (select md5(string_agg(n.artist_id || ':' || n.rank || ':' || n.xmin::text || ':' || n.ctid::text, ','
                              order by n.artist_id, n.rank))
        from public.artist_neighbors n join public.artists o on o.id = n.artist_id
        where o.slug like 'zz-test-%') as of_every_neighbour_row
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
    [retitled, *_] = works_of("delta", WORKS_EACH["delta"], artist_id=seeded["zz-test-delta"])
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
    names = [person["slug"].removeprefix("zz-test-") for person in CROWD]
    crowd_works = [row for name in names for row in works_of(name, 1, artist_slug=f"zz-test-{name}")]
    # Everyone's nearest is the next in line: for the last artist of a batch, somebody a later batch
    # writes. The first has everyone else after that, last first: more than a visitor is ever shown.
    ring = [neighbor(name, names[(n + 1) % len(names)], 1, 0.5) for n, name in enumerate(names)]
    rest = [neighbor(names[0], name, rank, 0.25) for rank, name in enumerate(reversed(names[2:]), start=2)]
    try:
        assert db.load(CROWD, crowd_works, ring + rest) == 0
        # Thirty artists in batches of seven: five batches, a plain vacuum after the second and the fourth.
        # The same again for their thirty lists of neighbours, and one vacuum with statistics to finish.
        assert vacuums == [False, False, False, False, True]
        [loaded] = db.sql("""
            select (select count(*) from public.artists where slug like 'zz-test-crowd-%')::int as artists,
                   (select count(*) from public.works w join public.artists c on c.id = w.artist_id
                    where c.slug like 'zz-test-crowd-%')::int as works,
                   (select count(*) from public.artist_neighbors n join public.artists c on c.id = n.artist_id
                    where c.slug like 'zz-test-crowd-%')::int as neighbours
        """)
        assert loaded == {"artists": 30, "works": 30, "neighbours": 58}
        # In the stored order, and never more than 24 of the 29, whatever is asked for.
        stored = [f"zz-test-{name}" for name in (names[1], *reversed(names[2:]))]
        assert similar("crowd-00", p_limit=100) == stored[:SIMILAR_CAP]
        assert similar("crowd-29") == ["zz-test-crowd-00"]
    finally:
        db.sql("delete from public.artists where slug like 'zz-test-crowd-%'")


def test_a_column_a_load_does_not_send_is_left_as_it_was():
    # A load may send fewer columns than the table has. An artist needs a slug, a name and a handle; a work
    # its artist, its source and its two URLs. On a row already here, a column that is not sent keeps its
    # value: an artist the owner hid stays hidden through a load that does not mention "hidden".
    whole = artist("partial", tagline="kept", hidden=True)
    again = {"slug": whole["slug"], "name": "Partial again", "da_username": whole["da_username"]}
    new = {"slug": "zz-test-minimal", "name": "Minimal", "da_username": "zz-test-minimal"}
    work = {
        "artist_slug": new["slug"],
        "source": "deviantart",
        "page_url": "https://www.deviantart.com/zz-test-minimal/art/Zz-Test-0-90000000",
        "image_url": thumbnail("minimal", 0),
    }
    try:
        db.upsert("artists", [whole], "slug", "slug")
        assert db.load([again, new], [work], []) == 0
        rows = db.sql("""
            select a.name, a.tagline, a.hidden, a.top_tags,
                   (select jsonb_agg(jsonb_build_array(w.position, w.tags, w.title, w.width, w.height))
                    from public.works w where w.artist_id = a.id) as works
            from public.artists a where a.slug in ('zz-test-partial', 'zz-test-minimal') order by a.slug
        """)
        assert rows == [
            {"name": "Minimal", "tagline": None, "hidden": False, "top_tags": [], "works": [[0, [], None, None, None]]},
            {"name": "Partial again", "tagline": "kept", "hidden": True, "top_tags": [], "works": None},
        ]
    finally:
        db.sql("delete from public.artists where slug in ('zz-test-partial', 'zz-test-minimal')")


def test_deviantart_work_is_the_same_work_under_a_new_thumbnail(seeded):
    # A DeviantArt thumbnail URL is long and carries a signed token that changes from one load to the
    # next. The deviation page is what identifies the work, so the row is updated, not doubled.
    dana = seeded["zz-test-dana"]
    [work] = [row for row in works_of("dana", WORKS_EACH["dana"], artist_id=dana) if row["position"] == 0]
    longer = (thumbnail("dana", 0, token="B") + "&pad=").ljust(2040, "x")  # near the 2,048 limit, and accepted
    assert db.upsert("works", [{**work, "image_url": longer}], "artist_id,key", "image_url") == [{"image_url": longer}]
    assert db.sql(f"select count(*)::int as n from public.works where artist_id = {dana}") == [{"n": 2}]
    [profile] = db.rpc("get_artist", {"p_slug": "zz-test-dana"}, "secret")
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
    assert search(q="zzqxwvkj") == []


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
        wrong = db.data_api("POST", "rpc/search_artists", "secret", json={"q": "", "q_embedding": [0.5] * size})
        assert wrong.status_code == 400 and "1,024 numbers" in wrong.text, size


def test_search_reads_only_the_start_of_a_long_query():
    # A 20,000-word query used to end in HTTP 500. Now only its first 200 characters are parsed.
    filler = "qqxzvwkjh "  # ten characters, and near no word anybody uses
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
    [card] = db.rpc("search_artists", {"q": WORD, "p_open_only": True}, "secret")
    assert set(card) == {"slug", "name", "tagline", "profile", "specialties", "mediums", "open_for_work", "works"}
    assert (card["slug"], card["tagline"]) == ("zz-test-alpha", "Armor and creatures")
    assert [work["title"] for work in card["works"]] == ["alpha 0", "alpha 1", "alpha 2", "alpha 3"]
    assert card["works"][0] == {
        "image_url": "https://zz-test-alpha.invalid/thumb/0.jpg",
        "page_url": "https://zz-test-alpha.invalid/work/0",
        "title": "alpha 0",
        "source": "site",  # each work says where it is shown, so a DeviantArt thumbnail can be credited
        "width": 300,  # and how large the image is, so the site can lay it out without cropping
        "height": 200,
    }
    [similar, *_] = db.rpc("similar_artists", {"p_slug": "zz-test-alpha"}, "secret")
    assert set(similar) == set(card)  # the same card from every function
    assert {work["source"] for work in similar["works"]} == {"deviantart"}
    assert all(set(work) == WORK_FIELDS for shown in (card, similar) for work in shown["works"])

    [profile] = db.rpc("get_artist", {"p_slug": "zz-test-alpha"}, "secret")
    assert set(profile) == set(card) | {"top_tags", "site_url", "deviantart_url"}
    assert profile["top_tags"] == ["zz-armor", "zz-knight"]
    assert profile["site_url"] == "https://www.zz-test-alpha.invalid/"
    assert profile["deviantart_url"] == "https://www.deviantart.com/zz-test-alpha"
    assert [work["title"] for work in profile["works"]] == [f"alpha {n}" for n in range(11)]
    assert all(set(work) == WORK_FIELDS for work in profile["works"])
    assert [(work["width"], work["height"]) for work in profile["works"]] == [(300 + n, 200) for n in range(11)]
    [unmeasured] = db.rpc("get_artist", {"p_slug": "zz-test-dana"}, "secret")
    assert {(work["width"], work["height"]) for work in unmeasured["works"]} == {(None, None)}  # not known: null
    assert db.rpc("get_artist", {"p_slug": "zz-test-hidden"}, "secret") == []
    assert db.rpc("get_artist", {"p_slug": "zz-test-nobody"}, "secret") == []


def test_a_card_leads_with_the_works_that_match_the_words():
    def cards(**args: Any) -> dict[str, list[str]]:
        found = db.rpc("search_artists", {"q": "", "p_mediums": [MEDIUM], **args}, "secret")
        return {card["slug"]: [work["title"] for work in card["works"]] for card in found}

    in_order = ["alpha 0", "alpha 1", "alpha 2", "alpha 3"]
    matching = ["alpha 8", "alpha 3", "alpha 6", "alpha 0"]
    # The artists are found by one word. The other is said by three of alpha's works and one of beta's:
    # alpha's card leads with the work tagged with it, then the two that describe it in the artist's own
    # order, then the first of the rest. Still four works.
    found = cards(q=f"{WORD} {MARK}")
    assert found["zz-test-alpha"] == matching
    assert found["zz-test-beta"] == [f"{MARK} beta", "beta 0"]  # a title counts too
    # An artist found by meaning alone still shows the works that match the words.
    by_meaning = cards(q=MARK, q_embedding=QUERY)
    assert list(by_meaning) == ["zz-test-beta", "zz-test-gamma", "zz-test-alpha"]
    assert by_meaning["zz-test-alpha"] == matching and by_meaning["zz-test-gamma"] == ["gamma 0"]
    # Words no work says, no words, a stop word, an embedding without words: the artist's own order.
    for args in ({"q": WORD}, {}, {"q": "the"}, {"q_embedding": QUERY}, {"q": WORD, "q_embedding": QUERY}):
        assert cards(**args)["zz-test-alpha"] == in_order, args
    # Similar artists and the profile page are not searches: the artist's own order, always.
    [as_neighbour] = db.rpc("similar_artists", {"p_slug": "zz-test-beta"}, "secret")
    assert [work["title"] for work in as_neighbour["works"]] == in_order


# 4. similar_artists: the stored neighbours, in the stored order.

ALPHA_ROWS = """
select n.rank::int as rank, b.slug, n.score::float as score
from public.artist_neighbors n
join public.artists a on a.id = n.artist_id
join public.artists b on b.id = n.neighbor_id
where a.slug = 'zz-test-alpha'
order by n.rank
"""


def stored(name: str) -> int:
    """How many neighbour rows an artist has."""
    count = "select count(*)::int as n from public.artist_neighbors n join public.artists a on a.id = n.artist_id"
    return db.sql(f"{count} where a.slug = 'zz-test-{name}'")[0]["n"]


def test_similar_artists_reads_the_stored_order():
    # What the load stored for alpha: the row that named nobody was skipped, the hidden artist's was kept.
    assert db.sql(ALPHA_ROWS) == [
        {"rank": 1, "slug": "zz-test-gamma", "score": 0.875},
        {"rank": 2, "slug": "zz-test-hidden", "score": 0.75},
        {"rank": 3, "slug": "zz-test-delta", "score": 0.625},
        {"rank": 5, "slug": "zz-test-beta", "score": 0.5},
    ]
    # What a visitor is shown: those, by rank, without the hidden one. That is neither the order they
    # were sent in, nor by id, nor by name.
    assert similar("alpha") == ALPHA_SIMILAR
    # A hidden neighbour takes up no place: the two asked for are the two best that may be shown.
    assert similar("alpha", p_limit=2) == ALPHA_SIMILAR[:2]
    assert similar("alpha", p_limit=-5) == ALPHA_SIMILAR[:1]  # out of range values are clamped
    assert similar("alpha", p_limit=None) == ALPHA_SIMILAR
    assert similar("beta") == ["zz-test-alpha"]
    assert similar("hidden") == []  # a hidden seed gives nothing away, though a neighbour is stored for it
    assert similar("nobody") == []
    assert similar("dana") == []  # no neighbours stored: an empty answer


def test_a_load_that_changes_neighbours_leaves_no_stale_row(seeded):
    try:
        # Alpha is ranked anew: two neighbours where there were four, at other ranks, with other scores.
        changed = [neighbor("alpha", "delta", 1, 0.75), neighbor("alpha", "beta", 2, 0.25)]
        assert db.load([BY_SLUG["zz-test-alpha"]], [], changed) == 0
        assert db.sql(ALPHA_ROWS) == [
            {"rank": 1, "slug": "zz-test-delta", "score": 0.75},
            {"rank": 2, "slug": "zz-test-beta", "score": 0.25},
        ]
        assert similar("alpha") == ["zz-test-delta", "zz-test-beta"]
        assert similar("beta") == ["zz-test-alpha"]  # an artist the load did not list keeps theirs
        # To leave an artist with no neighbours, the push stage sends one row for them that names none.
        # It is counted with the skipped rows, and among other artists' rows it clears only its own artist.
        none = {"artist_slug": "zz-test-beta", "neighbor_slug": None, "rank": None, "score": None}
        assert db.load([], [], [none, *changed]) == 1
        assert stored("beta") == 0 and similar("beta") == []
        assert similar("alpha") == ["zz-test-delta", "zz-test-beta"]

        def replace(*rows: dict[str, Any]) -> int:
            return db.rpc("replace_neighbors", {"p_rows": list(rows)}, "secret")

        # Sent again, or for an artist nobody knows, it is no error.
        assert replace(none, {**none, "artist_slug": "zz-test-nobody"}) == 2
        # Next to real rows for the same artist, those are written and it changes nothing.
        assert replace(none, neighbor("beta", "alpha", 1, 0.5)) == 1
        assert stored("beta") == 1 and similar("beta") == ["zz-test-alpha"]
        # A list of which nothing can be written replaces what was there just the same.
        assert replace(neighbor("beta", "nobody", 1, 0.5), neighbor("nobody", "beta", 1, 0.5)) == 2
        assert stored("beta") == 0 and similar("beta") == []
    finally:
        seed()
    assert similar("alpha") == ALPHA_SIMILAR and similar("beta") == ["zz-test-alpha"]


def test_a_similar_artist_leads_with_the_works_picked_for_the_seed(seeded):
    def shown(of: str) -> dict[str, list[str]]:
        cards = db.rpc("similar_artists", {"p_slug": f"zz-test-{of}"}, "secret")
        return {card["slug"]: [work["title"] for work in card["works"]] for card in cards}

    def replace(*rows: dict[str, Any]) -> httpx.Response:
        return db.data_api("POST", "rpc/replace_neighbors", "secret", json={"p_rows": list(rows)})

    stored = """select to_jsonb(n.picks) as picks, n.xmin::text || ':' || n.ctid::text as version
                from public.artist_neighbors n join public.artists a on a.id = n.artist_id
                where a.slug = 'zz-test-beta'"""
    unpicked = shown("beta"), shown("alpha")
    assert unpicked[0] == {"zz-test-alpha": ["alpha 0", "alpha 1", "alpha 2", "alpha 3"]}  # none picked: the first four
    assert db.sql(stored)[0]["picks"] is None
    row = neighbor("beta", "alpha", 1, 0.5)
    try:
        # Alpha's works at these positions are the closest to beta's, best first. The load stores them, and
        # beside beta alpha is shown with those, in that order. Still four works.
        assert db.load([], [], [{**row, "picks": [7, 0, 10, 3]}]) == 0
        [first] = db.sql(stored)
        assert first["picks"] == [7, 0, 10, 3]
        assert shown("beta") == {"zz-test-alpha": ["alpha 7", "alpha 0", "alpha 10", "alpha 3"]}
        assert shown("alpha") == unpicked[1]  # a row's picks are its own: nobody else's card changes
        assert replace({**row, "picks": [7, 0, 10, 3]}).json() == 0 and db.sql(stored) == [first]  # not rewritten
        # Fewer than a card shows, and one that names no work: the rest follow in the artist's own order.
        assert replace({**row, "picks": [9, 99]}).json() == 0
        assert shown("beta") == {"zz-test-alpha": ["alpha 9", "alpha 0", "alpha 1", "alpha 2"]}
        # More than a card shows is refused, and changes nothing.
        assert refused(replace({**row, "picks": [0, 1, 2, 3, 4]}), "artist_neighbors_few_picks")
        assert db.sql(stored)[0]["picks"] == [9, 99]
        # A row sent without picks has none: the first four again.
        assert replace(row).json() == 0
        assert db.sql(stored)[0]["picks"] is None and (shown("beta"), shown("alpha")) == unpicked
    finally:
        seed()
    assert (shown("beta"), shown("alpha")) == unpicked


def test_an_artist_is_never_their_own_neighbour(seeded):
    alpha = seeded["zz-test-alpha"]
    own = {"artist_id": alpha, "neighbor_id": alpha, "rank": 9, "score": 1}
    assert refused(db.data_api("POST", "artist_neighbors", "secret", json=own), "artist_neighbors_not_itself")
    listed = {"p_rows": [neighbor("beta", "gamma", 1, 0.5), neighbor("beta", "beta", 2, 0.5)]}
    assert refused(db.data_api("POST", "rpc/replace_neighbors", "secret", json=listed), "artist_neighbors_not_itself")
    assert similar("beta") == ["zz-test-alpha"]  # a list that is refused changes nothing


# 5. The publishable key.


def test_publishable_key_cannot_touch_tables(seeded):
    for table in TABLES:
        assert denied(db.data_api("GET", table, "publishable")), table
    # The same read with the secret key finds the row, so the check above is not empty.
    params = {"slug": "eq.zz-test-alpha", "select": "slug"}
    assert db.data_api("GET", "artists", "secret", params=params).json() == [{"slug": "zz-test-alpha"}]
    # The secrets the proof codes and the site's token are checked with are not even the service role's to read.
    assert denied(db.data_api("GET", "removal_secret", "secret"))
    assert denied(db.data_api("GET", "site_secret", "secret"))

    assert denied(db.data_api("POST", "artists", "publishable", json=artist("intruder")))
    hidden = {"slug": "eq.zz-test-hidden"}
    assert denied(db.data_api("PATCH", "artists", "publishable", params=hidden, json={"hidden": False}))
    assert denied(db.data_api("DELETE", "artists", "publishable", params={"slug": "like.zz-test-*"}))
    assert db.sql(COUNTS)[0]["artists"] >= len(ARTISTS)
    assert db.sql("select hidden from public.artists where slug = 'zz-test-hidden'") == [{"hidden": True}]


def test_publishable_key_runs_the_sites_functions_only_and_only_with_the_token():
    # With the token: what the site's server functions ask for.
    assert slugs(as_the_site("site_similar", p_slug="zz-test-alpha"))[0] == "zz-test-gamma"
    assert slugs(as_the_site("site_artist", p_slug="zz-test-alpha")) == ["zz-test-alpha"]
    assert slugs(as_the_site("site_search", q=WORD)) == ["zz-test-alpha", "zz-test-beta"]
    assert as_the_site("site_request_removal", p_slug="zz-test-nobody", p_note=None) is True
    assert CODE.fullmatch(as_the_site("site_removal_code", p_slug="zz-test-nobody"))
    # A page of search results is 24 cards, whatever is asked.
    assert (
        db.data_api(
            "POST", "rpc/site_search", "publishable", json={"p_token": TOKEN, "q": "", "p_limit": 100}
        ).status_code
        == 404
    )

    # Without it, or with another, each of the site's functions refuses: the publishable key alone opens nothing.
    site_calls = (
        ("site_similar", {"p_slug": "zz-test-alpha"}),
        ("site_artist", {"p_slug": "zz-test-alpha"}),
        ("site_search", {"q": WORD}),
        ("site_request_removal", {"p_slug": "zz-test-alpha", "p_note": None}),
        ("site_removal_code", {"p_slug": "zz-test-alpha"}),
        ("client_wait", {"p_kind": "search", "p_client": "zz-test-intruder", "p_network": "zz-test-intruder"}),
    )
    for name, args in site_calls:
        unknown = db.data_api("POST", f"rpc/{name}", "publishable", json=args)  # no token: no such function
        assert unknown.status_code == 404 and unknown.json()["code"] == "PGRST202", name
        for token in (None, "", TOKEN + "x", TOKEN.upper(), TOKEN[:-1]):
            assert denied(db.data_api("POST", f"rpc/{name}", "publishable", json={**args, "p_token": token})), name

    # Each of these exists and works for the service role or the owner; the public is refused, token or not.
    for name, args in (
        ("search_artists", {"q": WORD}),
        ("get_artist", {"p_slug": "zz-test-alpha"}),
        ("similar_artists", {"p_slug": "zz-test-alpha"}),
        ("request_removal", {"p_slug": "zz-test-alpha"}),
        ("start_removal_proof", {"p_slug": "zz-test-alpha"}),
        ("confirm_removal", {"p_slug": "zz-test-alpha", "p_code": "x"}),
        ("purge_removed", {"p_slugs": ["zz-test-alpha"]}),
        ("reject_removal", {"p_slug": "zz-test-alpha"}),
        ("close_request", {"p_slug": "zz-test-alpha", "p_outcome": "rejected"}),
        ("grant_request", {"p_slug": "zz-test-alpha", "p_outcome": "granted"}),
        ("removal_tally", {}),
        ("remove_artists", {"p_slugs": ["zz-test-alpha"]}),
        ("delete_removed", {}),
        ("removal_code", {"p_slug": "zz-test-alpha"}),
        ("artist_ids", {}),
        ("replace_neighbors", {"p_rows": []}),
        ("refresh_search_words", {}),
        ("artist_card", {"p_artist_id": 1}),
        ("artist_keys", {"p_da_username": "zz-test-alpha", "p_site_domain": None}),
        ("site_check", {"p_token": TOKEN}),
    ):
        assert denied(db.data_api("POST", f"rpc/{name}", "publishable", json=args)), name
    # Nor can the service role read the visitors' counts: only the function that keeps them can.
    assert denied(db.data_api("GET", "client_counts", "secret"))
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
        # The site's six, what they run, and the two that read the proof secret (neither is the public's).
        "definer_functions": sorted([*PUBLIC_FUNCTIONS, *SITE_FUNCTIONS, "confirm_removal", "removal_code"]),
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


# 6. Removal. Nothing a visitor can reach changes what is shown or undoes what an artist did;
#    proof of control removes at once. Visitors reach request_removal and start_removal_proof through the
#    site's server function, which calls them with a secret key: so do these tests.

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
    assert db.rpc("request_removal", {"p_slug": slug, "p_note": note}, "secret") is True


def proof_code(row_or_slug: dict[str, Any] | str) -> str:
    slug = row_or_slug if isinstance(row_or_slug, str) else row_or_slug["slug"]
    code = db.rpc("start_removal_proof", {"p_slug": slug}, "secret")
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
    # The log holds one row for each artist asked about, with how often (the first code asked for filed it), and
    # none for the unknown slug.
    assert db.sql(REQUESTS) == [
        {"slug": slug, "note": None, "requests": 1 + asked.count(slug), "in_order": True}
        for slug in sorted(targets[:-1])
    ]


def test_proof_of_control_removes_at_once_and_for_good(seeded):
    fresh(GONE, STAY)
    [gone] = db.sql(f"select id from public.artists where slug = '{GONE['slug']}'")
    db.upsert("works", works_of("gone", 2, artist_id=gone["id"]), "artist_id,key", "id")
    db.rpc("refresh_search_words", {}, "secret")
    pair = [neighbor("stay", "gone", 1, 0.5), neighbor("gone", "stay", 1, 0.5)]  # each is the other's neighbour
    assert db.rpc("replace_neighbors", {"p_rows": pair}, "secret") == 0
    assert similar("stay") == [GONE["slug"]]
    ask(GONE, "it is me")
    there = f"""
        select (select count(*) from public.artists where slug = '{GONE["slug"]}')::int as gone,
               (select count(*) from public.works where artist_id = {gone["id"]})::int as gone_works,
               (select count(*) from public.artists where slug = '{STAY["slug"]}')::int as stay,
               (select count(*) from public.search_words where word = 'zzqxgoneword')::int as gone_words,
               (select count(*) from public.artist_neighbors
                where {gone["id"]} in (artist_id, neighbor_id))::int as gone_neighbours
    """
    untouched = [{"gone": 1, "gone_works": 2, "stay": 1, "gone_words": 1, "gone_neighbours": 2}]
    nothing_left = [{"gone": 0, "gone_works": 0, "stay": 1, "gone_words": 0, "gone_neighbours": 0}]
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
    # pending for them, the words only they used, and every neighbour row they were in. Only the hashed
    # keys remain.
    assert confirm(GONE, code) is True
    assert db.sql(there) == nothing_left
    assert similar("stay") == []  # the artist who had them as a neighbour is still served, without them
    assert [row["key_hash"] for row in db.sql(remembered)] == GONE_HASHES
    assert slugs(db.sql(REQUESTS)) == [STAY["slug"]]  # GONE's is closed; STAY's code was asked for above
    assert visible(STAY)
    # The code opens nothing a second time, and the artist cannot be loaded again.
    assert confirm(GONE, code) is False
    assert db.upsert("artists", [GONE], "slug", "slug") == []
    # Nor by a load, which skips their works and every neighbour row that names them.
    assert db.load([GONE], works_of("gone", 1, artist_slug=GONE["slug"]), pair) == 2
    assert db.sql(there) == nothing_left


def test_public_functions_answer_alike_for_unknown_hidden_and_visible(seeded):
    fresh(STAY)
    counts = """
        select (select count(*) from public.removal_requests)::int as requests,
               (select count(*) from public.removed)::int as removed,
               (select count(*) from public.artists)::int as artists
    """
    [before] = db.sql(counts)
    codes = [proof_code(slug) for slug in (STAY["slug"], "zz-test-hidden", "zz-test-nobody")]
    assert len(set(codes)) == 3  # three codes of one shape (proof_code checks the shape)
    # Asking for a code files the request of an artist, hidden or not, and nothing else: the answer says nothing
    # of which it was.
    assert db.sql(counts) == [before | {"requests": before["requests"] + 2}]
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

    # Turned down: the request is closed, and nothing else changes.
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


OPEN = """
select artist_slug as slug, da_username, site_domain, da_key, site_key
from public.removal_requests where artist_slug like 'zz-test-%' and outcome is null order by artist_slug
"""
# Closed requests that still hold anything of an artist: there must be none.
NAMED_WHEN_CLOSED = """
select count(*)::int as n from public.removal_requests
where outcome is not null and (artist_slug not like 'closed:%' or note is not null or da_username is not null
                               or site_url is not null or site_domain is not null or due_at is not null)
"""


def tally() -> dict[str, int]:
    return db.rpc("removal_tally", {}, "secret")


def test_a_request_outlives_its_artists_row_and_only_an_answer_closes_it(seeded):
    """The critic's case: a push holds the artist back, so their row goes and every load ends with the sweep. Their
    request stays, with the handle and site it was made for: the code still removes them, and the owner can still
    answer. Closed, it keeps nothing of them."""
    fresh(GONE, STAY)
    before = tally()
    code = proof_code(GONE)
    ask(STAY, "it is me")
    db.sql("delete from public.artists where slug in ('zz-test-gone', 'zz-test-stay')")  # held back by a push
    db.after_load()  # what every load ends with: the sweep, the word list, a vacuum
    made_for = [
        {"slug": row["slug"], "da_username": row["da_username"], "site_domain": row["site_domain"]}
        | dict(zip(("da_key", "site_key"), key_hashes(row), strict=True))
        for row in (GONE, STAY)
    ]
    assert db.sql(OPEN) == made_for

    # Not listed, and removed all the same: for good, under the handle and site the request was made for.
    assert confirm(GONE, code) is True
    hashes = ", ".join(f"'{key_hash}'" for key_hash in GONE_HASHES)
    assert db.sql(f"select count(*)::int as n from public.removed where key_hash in ({hashes})") == [{"n": 2}]
    assert db.upsert("artists", [GONE], "slug", "slug") == []  # the next push cannot list them again
    # The owner grants the other while it is not listed either: the same.
    assert db.rpc("purge_removed", {"p_slugs": [STAY["slug"]]}, "secret") == 1
    assert db.upsert("artists", [STAY], "slug", "slug") == []
    assert db.sql(OPEN) == [] and db.sql(NAMED_WHEN_CLOSED) == [{"n": 0}]
    after = tally()
    assert (after["requests_proven"], after["requests_closed"]) == (
        before["requests_proven"] + 1,
        before["requests_closed"] + 1,
    )


def test_an_artist_removed_another_way_has_their_open_requests_closed(seeded):
    fresh(GONE)
    before = tally()
    ask(GONE, "it is me")
    proof_code(GONE)
    assert tally()["requests_open_with_code"] == before["requests_open_with_code"] + 1
    bar_gone()  # their keys on the list by hand, as a removal under another handle of theirs puts them
    assert db.sql(OPEN) == [] and db.sql(NAMED_WHEN_CLOSED) == [{"n": 0}]
    assert tally() == before | {"requests_closed": before["requests_closed"] + 1}


def test_a_rejected_artist_can_ask_again(seeded):
    fresh(STAY)
    ask(STAY, "first")
    assert db.rpc("reject_removal", {"p_slug": STAY["slug"]}, "secret") is True
    ask(STAY, "second")
    assert db.sql(REQUESTS) == [{"slug": STAY["slug"], "note": "second", "requests": 1, "in_order": True}]


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
    db.upsert("works", works_of("keep", 1, artist_id=keep["id"]), "artist_id,key", "id")
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
    # Nothing of that artist stays: not the row, not the works, not the words only they used, and their request
    # is closed, with nothing of them in it.
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
    fresh(HELD, STAY)
    assert db.rpc("replace_neighbors", {"p_rows": [neighbor("stay", "held", 1, 0.5)]}, "secret") == 0
    assert similar("stay") == [HELD["slug"]]
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
        assert HELD["slug"] not in slugs(db.rpc("search_artists", {"q": "held"}, "secret"))
        assert similar("stay") == []  # nor as anybody's neighbour, though that row is still there too
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
PIXELS = "works_dimensions"
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
    ("a width with no height", {"width": 300}, PIXELS),
    ("a height with no width", {"height": 200}, PIXELS),
    ("a width of zero", {"width": 0, "height": 200}, PIXELS),
    ("a negative height", {"width": 300, "height": -1}, PIXELS),
    ("a width of 100,001", {"width": 100_001, "height": 200}, PIXELS),
    ("a width that is not a whole number", {"width": 300.5, "height": 200}, "integer"),
    ("a style vector", {"style_embedding": "[0.5,0.5]"}, "style_embedding"),  # the column is gone: vectors stay home
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
    work |= {"width": 100_000, "height": 1}
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
        {"n": WORKS_EACH["alpha"]}
    ]


def test_a_work_cannot_be_on_the_site_of_an_artist_who_has_none(seeded):
    dana = {**GOOD_WORK, "artist_id": seeded["zz-test-dana"]}
    assert refused(db.data_api("POST", "works", "secret", json=dana), OWN_SITE)
    assert db.sql(f"select count(*)::int as n from public.works where artist_id = {seeded['zz-test-dana']}") == [
        {"n": WORKS_EACH["dana"]}
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


# 8. The limits and the verifier's queue (sql/003_limits.sql), for the owner to run: see the module's docstring.


def visitor() -> str:
    return f"zz-test-{secrets.token_hex(4)}"


def counted_as(budget: str, level: str, who: str) -> str:
    """SQL for the key client_wait counts a visitor or a network under: an HMAC, so the table holds no address."""
    secret = "(select secret from public.removal_secret)"
    return f"encode(extensions.hmac('{budget} {level} {who}', {secret}, 'sha256'), 'hex')"


def wait(kind: str, client: str, network: str | None = None) -> int:
    return as_the_site("client_wait", p_kind=kind, p_client=client, p_network=network or client)


def forget(*who: str, pages_let_through: float = 0) -> None:
    """Delete test visitors' and networks' counts, and take what they added off the day's total of reads."""
    keys = ", ".join(
        counted_as(budget, level, name)
        for name in who
        for budget in ("read", "removal")
        for level in ("visitor", "network")
    )
    db.sql(f"""
        delete from public.client_counts where client_key in ({keys});
        update public.client_counts set busy_until = busy_until - {pages_let_through * 86400 / READS_A_DAY}
        where client_key = 'read everyone';
    """)


def ahead(key: str) -> float:
    """How far ahead of now a count is, in seconds: a day when it is full."""
    now = "extract(epoch from clock_timestamp())"
    [row] = db.sql(f"select busy_until - {now} as ahead from public.client_counts where client_key = {key}")
    return row["ahead"]


def counts_kept(key: str) -> int:
    return db.sql(f"select count(*)::int as n from public.client_counts where client_key = {key}")[0]["n"]


def test_600_reads_at_once_from_one_visitor_let_their_share_through_and_the_rest_cost_nothing(seeded):
    client = visitor()
    try:
        with ThreadPoolExecutor(30) as pool:
            waits = list(pool.map(lambda _: wait("search", client), range(600)))
        assert waits.count(0) == READS_PER_VISITOR
        # The next is let in once a 50th of a day has passed, and the 550 refused moved that on by nothing.
        assert all(0 < seconds <= 86400 / READS_PER_VISITOR + 1 for seconds in waits if seconds)
        assert 86400 - 60 < ahead(counted_as("read", "visitor", client)) <= 86400
        assert 0 < wait("search", client) <= 86400 / READS_PER_VISITOR + 1
        # The table holds a hash for the visitor, never what they were called.
        [row] = db.sql(
            f"select client_key from public.client_counts where client_key = {counted_as('read', 'visitor', client)}"
        )
        assert re.fullmatch(r"[0-9a-f]{64}", row["client_key"])
        assert db.sql("select count(*)::int as n from public.client_counts where client_key like '%zz-test%'") == [
            {"n": 0}
        ]
    finally:
        forget(client, pages_let_through=READS_PER_VISITOR)


def test_a_profile_counts_a_tenth_of_a_page(seeded):
    client = visitor()
    try:
        for _ in range(10):
            assert wait("profile", client) == 0
        assert abs(ahead(counted_as("read", "visitor", client)) - 86400 / READS_PER_VISITOR) < 30  # one page's worth
    finally:
        forget(client, pages_let_through=1)


def test_one_network_shares_its_share_whatever_addresses_it_asks_from(seeded):
    network, clients = visitor(), [visitor() for _ in range(4)]
    try:
        with ThreadPoolExecutor(30) as pool:
            waits = list(pool.map(lambda n: wait("search", clients[n % 4], network), range(4 * READS_PER_VISITOR)))
        assert waits.count(0) == READS_PER_NETWORK  # four visitors' worth asked, two let in
    finally:
        forget(network, *clients, pages_let_through=READS_PER_NETWORK)


def test_removal_requests_are_counted_by_the_day_and_apart_from_reads(seeded):
    network, clients = visitor(), [visitor() for _ in range(3)]
    try:
        with ThreadPoolExecutor(20) as pool:
            waits = list(pool.map(lambda _: wait("removal", clients[0], network), range(50)))
        assert waits.count(0) == REMOVALS_PER_VISITOR
        assert all(0 < seconds <= 86400 for seconds in waits if seconds)
        more = [wait("removal", client, network) for client in clients[1:] for _ in range(REMOVALS_PER_VISITOR)]
        assert more.count(0) == REMOVALS_PER_NETWORK - REMOVALS_PER_VISITOR
        assert wait("search", clients[0], network) == 0  # told to wait for removals, and free to search
        unknown = db.data_api(
            "POST", "rpc/client_wait", "publishable",
            json={"p_token": TOKEN, "p_kind": "everything", "p_client": clients[0], "p_network": network},
        )  # fmt: skip
        assert unknown.status_code == 400 and "cannot count this request" in unknown.text
    finally:
        forget(network, *clients, pages_let_through=1)


def test_the_days_total_holds_reads_back_for_everyone_a_little_at_a_time(seeded):
    client = visitor()
    kept = db.sql("select busy_until from public.client_counts where client_key = 'read everyone'")
    try:
        db.sql("""
            insert into public.client_counts (client_key, busy_until)
            values ('read everyone', extract(epoch from clock_timestamp()) + 86400)
            on conflict (client_key) do update set busy_until = excluded.busy_until
        """)
        # Full: the next page is let in a 1,000th of a day on, a profile a tenth of that. Not until midnight.
        assert abs(wait("search", client) - 86400 / READS_A_DAY) <= 2
        assert abs(wait("profile", client) - 86400 / READS_A_DAY / 10) <= 2
        assert wait("removal", client) == 0  # asking for removal has no day's total
        # Nothing refused was counted.
        assert (
            counts_kept(counted_as("read", "visitor", client)) == 1
            and ahead(counted_as("read", "visitor", client)) <= 0
        )
    finally:
        restore = (
            f"update public.client_counts set busy_until = {kept[0]['busy_until']}"
            if kept
            else "delete from public.client_counts"
        )
        db.sql(f"{restore} where client_key = 'read everyone'")
        forget(client)


def test_the_count_fails_closed_without_what_it_needs(seeded):
    """Each case is one call, which the database runs as one transaction and undoes at its end (the 1 / 0). The site's
    token is a stand-in's within it, so the real one never travels in a query."""
    stand_in = "update public.site_secret set token_hash = public.key_hash('zz-test-token');"
    count = "select public.client_wait('zz-test-token', 'search', 'zz-test-x', 'zz-test-x');"
    with pytest.raises(RuntimeError, match="division by zero"):  # with everything there, it counts: the cases are real
        db.sql(f"{stand_in} {count} select 1 / 0;")
    for missing, error in (("site_secret", "not the site"), ("removal_secret", "cannot count this request")):
        with pytest.raises(RuntimeError, match=error):
            db.sql(f"{stand_in} delete from public.{missing}; {count} select 1 / 0;")
    secrets_kept = (
        "select (select count(*) from public.site_secret) + (select count(*) from public.removal_secret) as n"
    )
    assert db.sql(secrets_kept) == [{"n": 2}]
    assert counts_kept(counted_as("read", "visitor", "zz-test-x")) == 0


def test_many_calls_at_once_in_any_order_neither_wait_on_each_other_for_ever_nor_undo_a_count(seeded):
    """40 visitors in 5 networks, 160 reads from each network at once, 30 at a time: none fails (a deadlock would
    be a failure), and each network lets exactly its share through."""
    networks, clients = [visitor() for _ in range(5)], [visitor() for _ in range(40)]
    try:
        with ThreadPoolExecutor(30) as pool:
            waits = list(pool.map(lambda n: (n % 5, wait("search", clients[n % 40], networks[n % 5])), range(800)))
        assert [sum(seconds == 0 for at, seconds in waits if at == n) for n in range(5)] == [READS_PER_NETWORK] * 5
    finally:
        forget(*networks, *clients, pages_let_through=5 * READS_PER_NETWORK)


QUEUE = """
select artist_slug as slug, code_at is not null as coded, due_at = code_at as due_at_once, code_at, due_at
from public.removal_requests where artist_slug like 'zz-test-%' order by artist_slug
"""


def test_a_code_files_the_request_and_asking_for_it_again_starts_its_checks_over(seeded):
    fresh(GONE, STAY)
    ask(GONE, "it is me")  # without a code: for the owner alone, and never looked at by the verifier
    [bare] = db.sql(QUEUE)
    assert (bare["coded"], bare["due_at"]) == (False, None)

    for row_or_slug in (GONE, STAY, "zz-test-nobody"):
        proof_code(row_or_slug)
    first = db.sql(QUEUE)
    assert [(row["slug"], row["coded"], row["due_at_once"]) for row in first] == [
        (GONE["slug"], True, True),
        (STAY["slug"], True, True),
    ]
    # The verifier moves a request on with the service key; the publishable key cannot touch the table.
    moved = {"due_at": "2099-01-01T00:00:00+00:00"}
    mine = {"artist_slug": f"eq.{GONE['slug']}"}
    assert db.data_api("PATCH", "removal_requests", "secret", params=mine, json=moved).is_success
    assert denied(db.data_api("PATCH", "removal_requests", "publishable", params=mine, json=moved))
    assert due_later_than(GONE, "70 years")

    time.sleep(1)
    ask(GONE)
    ask(STAY, "again")
    assert db.sql(QUEUE)[1] == first[1]  # asking without a code moves nothing
    proof_code(GONE)
    proof_code(STAY)
    again = db.sql(QUEUE)
    # Asked for the code again: its checks start over, and it is due at once, unless it was due already, where it
    # keeps its place.
    assert again[0]["code_at"] > first[0]["code_at"] and again[0]["due_at_once"]
    assert again[1]["code_at"] > first[1]["code_at"] and again[1]["due_at"] == first[1]["due_at"]


def due_later_than(row: dict[str, Any], interval: str) -> bool:
    later = f"select due_at > now() + interval '{interval}' as later from public.removal_requests"
    return db.sql(f"{later} where artist_slug = '{row['slug']}'") == [{"later": True}]


def test_the_verifier_removes_through_the_real_queue_and_the_real_confirm_removal(seeded, tmp_path, monkeypatch):
    """pipeline.removals itself against this database: the due requests, removal_code, confirm_removal. Only
    DeviantArt is a stand-in, which answers for the artists' own accounts. The run reads zz-test- requests only."""
    monkeypatch.setattr(config, "CACHE_DIR", tmp_path / "cache")  # never the real cache
    monkeypatch.setattr(config, "LOCAL_DB", tmp_path / "cache" / "pipeline.sqlite")
    monkeypatch.setattr(time, "sleep", lambda seconds: None)
    monkeypatch.setattr(discover, "now", lambda: time.time() + 60)  # a minute ahead of the database, in case
    rows = removals._rows
    monkeypatch.setattr(
        removals,
        "_rows",
        lambda table, **params: rows(
            table, **params, **({"artist_slug": "like.zz-test-*"} if table == "removal_requests" else {})
        ),
    )
    fresh(GONE, STAY, KEEP)
    for row in (GONE, STAY):  # the library keeps their accounts
        handle = row["da_username"]
        assert store.upsert_artist(handle, f"https://www.deviantart.com/{handle}", [], 1)
        library.store_artist(
            store.artist_key(handle), user_id=f"FAKE-USER-{handle}", tagline=None, specialty=None, level=None,
            website=None, works=[],
        )  # fmt: skip
    code = proof_code(GONE)
    proof_code(STAY)  # asked for a code, and has not put it up
    ask(KEEP)  # no code: never looked at
    db.sql(f"delete from public.artists where slug = '{GONE['slug']}'")  # held back by a push: looked for all the same
    bios = {GONE["da_username"]: f"Please remove me: {code.upper()}", STAY["da_username"]: "Concept artist."}

    def deviantart(request: httpx.Request) -> httpx.Response:
        assert request.url.host == "www.deviantart.com"
        if request.url.path == "/oauth2/token":
            return httpx.Response(200, json={"access_token": "FAKE-TOKEN"})
        handle = request.url.path.rsplit("/", 1)[1]
        user = {"userid": f"FAKE-USER-{handle}", "username": handle}
        return httpx.Response(200, json={"user": user, "bio": bios[handle], "website": None})

    counts = dict.fromkeys(removals.COUNTS, 0)
    nowhere = crawl.PoliteFetcher(transport=httpx.MockTransport(lambda request: pytest.fail("a site was asked")))
    removals.run(counts, False, "FAKE-ID", "FAKE-SECRET", transport=httpx.MockTransport(deviantart), fetcher=nowhere)

    assert all(counts[name] >= 0 for name in removals.TALLY)  # every request in the database, whoever's
    assert {name: count for name, count in counts.items() if name not in removals.TALLY} == {
        name: 0 for name in removals.COUNTS if name not in removals.TALLY
    } | {"requests_due": 2, "proven_on_deviantart": 1, "artists_removed": 1, "left_for_owner_no_code_found": 1}
    hashes = ", ".join(f"'{key_hash}'" for key_hash in GONE_HASHES)
    assert db.sql(f"select count(*)::int as n from public.removed where key_hash in ({hashes})") == [{"n": 2}]
    assert db.upsert("artists", [GONE], "slug", "slug") == []  # and not listed again
    assert visible(STAY) and visible(KEEP)
    assert store.is_removed(GONE["da_username"])  # here too, at once
    # STAY is looked at again in an hour; KEEP's request, without a code, was never touched.
    queue = {row["slug"]: row for row in db.sql(QUEUE)}
    assert sorted(queue) == [KEEP["slug"], STAY["slug"]]
    assert due_later_than(STAY, "50 minutes")
    assert (queue[KEEP["slug"]]["coded"], queue[KEEP["slug"]]["due_at"]) == (False, None)


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


# Full scale, on request: RUN_SCALE=1. Loads 3,000 analyzed artists, 30,000 works and 24 neighbours
# each, loads them all again with new vectors and new neighbours while a visitor keeps calling, deletes
# them and compacts the tables (a brief lock). Too long for the normal run.

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
  (artist_id, source, page_url, image_url, title, description, tags, position)
select a.id, 'deviantart',
       'https://www.deviantart.com/' || a.slug || '/art/Work-' || n,
       'https://images-wixmp-ed30a86b8c4ca887773594c2.wixmp.com/f/0a1b2c3d-4e5f-6071-8293-a4b5c6d7e8f9/d' || a.id
         || n || '-pre.jpg/v1/fill/w_300,h_300,q_70,strp/work_' || n || '.jpg?token='
         || (select string_agg(md5(random()::text), '') from generate_series(1, 25) where a.id = a.id and n = n),
       'Work title ' || n,
       'A knight in ornate plate armor stands before a ruined cathedral, painted with loose, textured '
         || 'brushwork and a warm, muted palette. ' || md5(random()::text),
       array['digital painting', 'fantasy', 'armor', 'character', 'painterly', 'knight'],
       n
from public.artists a cross join generate_series(0, 9) n
where a.slug like 'zz-test-bulk-%';

-- Twenty-four neighbours each: for artist g, artists g + 37, g + 74 and so on, counting round. Each with
-- four of the neighbour's works picked, so a card is built the way it is after a push.
insert into public.artist_neighbors (artist_id, neighbor_id, rank, score, picks)
select a.id, b.id, k, 1 - k / 100.0, array[k % 10, (k + 3) % 10, (k + 5) % 10, (k + 8) % 10]
from public.artists a
cross join generate_series(1, 24) k
join public.artists b
  on b.slug = 'zz-test-bulk-' || ((substring(a.slug from '[0-9]+$')::int - 1 + k * 37) % 3000 + 1)
where a.slug like 'zz-test-bulk-%';
"""
SCALE = """
select (select count(*) from public.artists where slug like 'zz-test-bulk-%')::int as artists,
       (select count(*) from public.works w join public.artists a on a.id = w.artist_id
        where a.slug like 'zz-test-bulk-%')::int as works,
       (select count(*) from public.artist_neighbors n join public.artists a on a.id = n.artist_id
        where a.slug like 'zz-test-bulk-%')::int as neighbours,
       round(pg_database_size(current_database()) / 1048576.0)::int as megabytes
"""


def real_artists() -> int:
    """How many artists the project holds that are not these tests' own."""
    return db.sql("select count(*)::int as n from public.artists where slug not like 'zz-test-%'")[0]["n"]


def random_vector(numbers: random.Random, size: int) -> str:
    return "[" + ",".join(f"{numbers.uniform(-0.5, 0.5):.4f}" for _ in range(size)) + "]"


def reload_rows(numbers: random.Random) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    """Every bulk artist and work again, as the push stage would send them after analyzing everything anew:
    the same artists and deviation pages, with new vectors, new thumbnail tokens and new neighbours."""
    artists, works, neighbors = [], [], []
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
                    "position": n,
                }
            )
        for k in range(1, 25):  # nobody keeps a neighbour: g + 41, g + 82 and so on, counting round
            nearest = f"zz-test-bulk-{(g - 1 + k * 41) % 3000 + 1}"
            picks = [(k + n) % 10 for n in (1, 2, 4, 9)]
            neighbors.append(
                {"artist_slug": slug, "neighbor_slug": nearest, "rank": k, "score": 1 - k / 100, "picks": picks}
            )
    return artists, works, neighbors


def server_ms(query: str) -> list[float]:
    """Three runs in a row: the first is the cold one."""
    plans = [db.sql("explain (analyze, costs off, buffers off) " + query) for _ in range(3)]
    return [float(plan[-1]["QUERY PLAN"].split()[2]) for plan in plans]  # "Execution Time: 123.456 ms"


@pytest.mark.skipif(os.environ.get("RUN_SCALE") != "1", reason="set RUN_SCALE=1 to load 3,000 artists and 30,000 works")
def test_full_scale_fits_the_budget_and_the_public_timeout():
    if real_artists():
        pytest.skip(
            "this project holds real artists: the full-scale test puts 3,000 synthetic ones in front of "
            "visitors and locks the tables to compact them, so it runs only against a project with none"
        )
    limit = PUBLIC_TIMEOUT_MS / 1000
    numbers = random.Random(5)
    embedding = [round(numbers.uniform(-0.5, 0.5), 4) for _ in range(1024)]
    calls: dict[str, tuple[str, dict[str, Any]]] = {
        "search with an embedding": ("site_search", {"q": "painterly fantasy character", "q_embedding": embedding}),
        "similar": ("site_similar", {"p_slug": "zz-test-bulk-1"}),
        "search": ("site_search", {"q": "painterly fantasy character artist"}),
        "misspelled search": ("site_search", {"q": "concpet artist charcter desing"}),
        "browse": ("site_search", {"q": "", "p_mediums": ["digital"], "p_open_only": True}),
        "profile": ("site_artist", {"p_slug": "zz-test-bulk-1"}),
        "removal request": ("site_request_removal", {"p_slug": "zz-test-bulk-3", "p_note": "scale test"}),
        "proof code": ("site_removal_code", {"p_slug": "zz-test-bulk-3"}),
    }

    def as_the_public(label: str) -> float:
        """One call as the site makes it (the publishable key, the token), timed. One the 3 s limit cuts off raises."""
        name, args = calls[label]
        started = time.perf_counter()
        answer = as_the_site(name, **args)
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
        assert (loaded["artists"], loaded["works"], loaded["neighbours"]) == (3000, 30_000, 72_000)
        assert loaded["megabytes"] < 400
        after_load = {label: as_the_public(label) for label in calls}
        print(
            "as the public, after the load: " + ", ".join(f"{k} {v:.2f} s" for k, v in after_load.items()), flush=True
        )
        assert max(after_load.values()) < limit

        # Everything again, analyzed anew, through db.load, while a visitor keeps calling the public
        # functions and the database's size is watched. No call may fail or outlast the public's limit.
        artists, works, neighbors = reload_rows(numbers)
        seen: list[tuple[str, float, float]] = []  # what was called, how long it took, when it began
        failed: list[str] = []
        sizes = [loaded["megabytes"]]
        loading = threading.Event()

        def visitor() -> None:
            while not loading.is_set():
                for label in ("similar", "search with an embedding", "search", "profile"):
                    began = time.perf_counter() - started
                    try:
                        seen.append((label, as_the_public(label), began))
                    except Exception as error:  # every failure is recorded, and the test fails on them below
                        failed.append(f"{label} at {began:.0f} s: {error}")
                sizes.append(megabytes())

        # How long each of the load's own statements takes (to see how near the Data API's 8 s they come)
        # and when, so that a slow answer to the visitor can be set beside what the load was doing.
        statements: dict[str, list[tuple[float, float]]] = {}
        real_upsert, real_rpc, real_vacuum = db.upsert, db.rpc, db.vacuum_analyze

        def timed(kind: str, call: Any, *args: Any) -> Any:
            began = time.perf_counter()
            try:
                return call(*args)
            finally:
                statements.setdefault(kind, []).append((time.perf_counter() - began, began - started))

        db.upsert = lambda table, *args: timed(f"upsert {table}", real_upsert, table, *args)  # type: ignore[assignment]
        db.rpc = lambda name, *args: timed(name, real_rpc, name, *args)  # type: ignore[assignment]
        db.vacuum_analyze = lambda analyze=True: timed("vacuum", real_vacuum, analyze)  # type: ignore[assignment]
        started = time.perf_counter()
        watcher = threading.Thread(target=visitor)
        watcher.start()
        try:
            assert db.load(artists, works, neighbors) == 0
        finally:
            loading.set()
            watcher.join()
            db.upsert, db.rpc, db.vacuum_analyze = real_upsert, real_rpc, real_vacuum  # type: ignore[assignment]
        took = time.perf_counter() - started
        of_the_load = ("upsert artists", "artist_ids", "upsert works", "replace_neighbors", "vacuum")
        for kind in of_the_load:
            took_each = sorted(s for s, _ in statements[kind])
            middle, longest = took_each[len(took_each) // 2], took_each[-1]
            print(
                f"the load's {len(took_each)} '{kind}' statements: median {middle:.2f} s, slowest {longest:.2f} s",
                flush=True,
            )
        slowest = {label: max(s for name, s, _ in seen if name == label) for label in {name for name, _, _ in seen}}
        round_trips = sorted(s for _, s, _ in seen)
        median, most = round_trips[len(round_trips) // 2], round_trips[int(len(round_trips) * 0.99)]
        print(
            f"reload through db.load: {took:.0f} s; a visitor made {len(seen) + len(failed)} calls meanwhile",
            flush=True,
        )
        print(f"of those, {len(failed)} failed: {failed[:3]}", flush=True)
        print(f"round trips during the reload: median {median:.2f} s, 99 in 100 under {most:.2f} s", flush=True)
        print("slowest of each: " + ", ".join(f"{k} {v:.2f} s" for k, v in slowest.items()), flush=True)
        for label, s, began in seen:
            if s > 1:  # every answer that took over a second, with what the load was doing meanwhile
                meanwhile = sorted(
                    {kind for kind in of_the_load for t, at in statements[kind] if at < began + s and began < at + t}
                )
                print(
                    f"  {label} took {s:.2f} s at {began:.0f} s, during the load's: {meanwhile or 'rest'}", flush=True
                )
        print(f"database size during the reload: peak {max(sizes)} MB over {len(sizes)} readings", flush=True)
        # Every call was answered: one that the public's 3 s limit cut off would be among the failures.
        # (A round trip adds the network to the query, so it is held to the limit for 99 calls in 100.)
        assert failed == []
        assert len(seen) >= 40 and most < limit
        assert max(sizes) < 400
        [reloaded] = db.sql(SCALE)
        print(f"after the reload: {reloaded}", flush=True)
        assert (reloaded["artists"], reloaded["works"], reloaded["neighbours"]) == (3000, 30_000, 72_000)
        assert reloaded["megabytes"] < 400
        changed = "select count(*)::int as n from public.works where description like '%described again%'"
        assert db.sql(changed) == [{"n": 30_000}]  # the reload really rewrote every work

        # And the limits that keep single calls cheap, at this size.
        after_reload = {label: as_the_public(label) for label in calls}
        print(
            "as the public, after the reload: " + ", ".join(f"{k} {v:.2f} s" for k, v in after_reload.items()),
            flush=True,
        )
        assert max(after_reload.values()) < limit
        # The reload's neighbours, all of them new, in rank order.
        assert similar("bulk-1", p_limit=100) == [f"zz-test-bulk-{41 * k + 1}" for k in range(1, 25)]
        deep = {"q": "", "p_limit": 100, "p_offset": OFFSET_CAP}
        assert len(db.rpc("search_artists", deep, "secret")) == 100  # the deepest page, built for its own rows only
        assert db.rpc("search_artists", {**deep, "p_offset": OFFSET_CAP + 1}, "secret") == []  # and past it, nothing
        huge = {"q": "", "p_mediums": ["digital", *["zz-none"] * 100_000]}
        assert len(db.rpc("search_artists", huge, "secret")) == 24

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
        for table in ("works", "artists", "artist_neighbors", "search_words"):
            db.sql(f"vacuum full public.{table}")
