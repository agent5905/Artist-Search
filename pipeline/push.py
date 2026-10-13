"""Push: turn what the local stages know into the rows the public database serves, and load them.

    uv run python -m pipeline.push --dry-run     everything but the writes to the database
    uv run python -m pipeline.push

In this order, and the order is the point:
  1. Removal (hard rule 7). The key hashes in the database's `removed` table go to store.import_removed, so a
     removal made through the site reaches this machine, and everything kept here about an artist the store no
     longer lists is purged. When any of that fails the run stops: nothing is built and nothing is sent.
  2. The rows are built (see build) from what is left, and only from artists the store still lists.
  3. The database is brought to exactly those rows: an artist or a work it has that is not among them is
     deleted, then db.load writes the rest. So an artist who stops qualifying leaves on the next run.

Hard rule 1: a work is sent as two addresses, its page on DeviantArt and DeviantArt's own thumbnail, and words.
No image, no file name and no deviation id is ever put in a row.
Hard rule 6: an artist's own words (a title, their tags, a tagline) are sent only when the tagging stage read
them and found no private contact. A work's page address is made from its title, so a work whose words hold a
contact, or were not read, is not sent at all.
Stdout is one JSON object of counts. This stage keeps no table of its own, and writes to the local stores only
through store.import_removed and library.purge.
"""

import argparse
import contextlib
import json
import math
import pathlib
import re
import sqlite3
import sys
from collections import Counter
from typing import Any
from urllib.parse import urlsplit

from pipeline import config, crawl, db, library, profiles, store, style, tagging

MIN_WORKS = 4  # publishable works: an artist with fewer is not sent
ADULT_PERCENT = 30  # of an artist's rated works: at this share rated suggestive or explicit, the artist is left out
SAFE_RATING = "safe"  # the only rating a work is sent with
ADULT_RATINGS = ("suggestive", "explicit")
NEIGHBOURS = 24  # similar artists sent for each artist: as many as similar_artists ever returns
STYLE_MODEL, STYLE_RULE = "csd", "local"  # round 7's blind test chose them
TOP_TAGS = 12  # of the artist's own tags, the most used
MAX_SEARCH_TEXT = 4000  # sql/001_schema.sql: artists_own_words_sizes

ARTIST_REASONS = ("not_profiled", "profile_excluded", "site_removed", "adult_portfolio", "too_few_works", "slug_taken")
WORK_REASONS = (
    *("not_tagged", "not_artwork", "explicit", "declared_ai", "contact_in_words", "words_not_read"),
    *("not_rated", "suggestive", "bad_address", "artist_held_back"),
)

REMOVED_SQL = "select key_hash from public.removed"
# Every artist the database has, and every work: what a run compares its rows with.
# ponytail: one answer with a row a work, about 5 MB at 30,000 works. Page it by artist id if that is refused.
LISTING_SQL = (
    "select a.id as artist_id, a.slug, w.id as work_id, w.page_url "
    "from public.artists a left join public.works w on w.artist_id = a.id"
)
# sql/001_schema.sql: clean_url. A row the database would refuse fails the whole batch it is in, so an address
# that would not pass is held back here.
_CLEAN_URL = re.compile(
    r"https?://(?=[^/?:]{4,253}(?:[/?:]|$))"
    r"(?:[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\.)+(?:[a-z]{2,63}|xn--[a-z0-9-]{1,59})"
    r"(?::[0-9]{1,5})?(?:[/?][A-Za-z0-9._~:/?@!$&'()*+,;=%-]*)?"
)

Rows = list[dict[str, Any]]


def _clean(url: str) -> bool:
    return len(url) <= 2048 and "data:" not in url.lower() and bool(_CLEAN_URL.fullmatch(url))


def _addresses(page: object, thumb: object, username: str) -> tuple[str, str] | None:
    """A work's page, under the artist's own handle on www.deviantart.com, and its thumbnail on DeviantArt's
    image host. None for anything else.

    The library's own checks first (https, no "data:", a deviation page, a signed image address), then the two
    hosts the database accepts: the library also keeps thumbnails on deviantart.net, and the database does not.
    """
    try:
        page_url, image_url = library.page_address(page), library.image_address(thumb)
    except ValueError:
        return None
    where = urlsplit(page_url)
    theirs = where.hostname == "www.deviantart.com" and where.path.lower().startswith(f"/{username.lower()}/art/")
    if not theirs or not (urlsplit(image_url).hostname or "").endswith(".wixmp.com"):
        return None
    return (page_url, image_url) if _clean(page_url) and _clean(image_url) else None


def _own_site(website: str | None) -> tuple[str | None, str | None]:
    """(site_url, site_domain) when the website is the front page of a domain of the artist's own, else two Nones.

    crawl.is_own_site is the rule, so a profile on a platform is not a site. The domain is the registrable
    domain as the store's removal keys spell it, which is what the database hashes into site_key.
    """
    if not website or not crawl.is_own_site(website) or not _clean(website):
        return None, None
    try:
        return website, store.site_key(website).removeprefix("site:")
    except ValueError:
        return None, None


def _slug(username: str) -> str:
    """A handle as the database spells a slug: runs of lowercase letters and digits, joined by single hyphens."""
    return "-".join(re.findall(r"[a-z0-9]+", username.lower()))


def _held(result: dict[str, Any] | None) -> str | None:
    """Why a work is held back, as one of WORK_REASONS, or None when it may be sent.

    `result` is the work's entry in tagging.results() for the verified set: None when it has none. A reading
    or a rating that has not been made yet holds the work back, the same as one that came out wrong.
    """
    if result is None:
        return "not_tagged"
    rating, declared, contact = result.get("rating"), result.get("declared_ai"), result.get("contact_in_text")
    if result["not_artwork"]:
        return "not_artwork"
    if result["explicit"] or rating == "explicit":
        return "explicit"
    if declared == "yes":
        return "declared_ai"
    if contact == "yes":
        return "contact_in_words"
    if declared is None or contact != "no":
        return "words_not_read"
    if rating != SAFE_RATING:
        return "suggestive" if rating in ADULT_RATINGS else "not_rated"
    return None


def _collected(
    local: pathlib.Path | str | None,
) -> tuple[dict[str, list[tuple[Any, ...]]], dict[str, tuple[str | None, str | None]]]:
    """What the library holds, read through a connection that cannot write.

    Each artist's works, the most recent first, as (deviation id, page, title, the artist's tags as JSON,
    thumbnail); and each artist's (tagline, website).
    ponytail: the library has no reader for these columns, so they are selected here. Move it there when it has.
    """
    path = pathlib.Path(local or config.LOCAL_DB).resolve()
    works: dict[str, list[tuple[Any, ...]]] = {}
    with contextlib.closing(sqlite3.connect(f"{path.as_uri()}?mode=ro", uri=True, timeout=60)) as conn:
        for key, *work in conn.execute(
            "SELECT artist_key, deviation_id, page_url, title, tags, thumb_url FROM works "
            "ORDER BY artist_key, published_at IS NULL, published_at DESC, deviation_id"
        ):
            works.setdefault(key, []).append(tuple(work))
        rows = conn.execute("SELECT artist_key, tagline, website FROM profiles").fetchall()
    return works, {key: (tagline, website) for key, tagline, website in rows}


def build(local: pathlib.Path | str | None = None) -> tuple[Rows, Rows, Rows, Counter[str]]:
    """The rows for db.load (artists, works, neighbours) and the count of what was held back, by reason.

    An artist goes when the store lists them, pipeline/profiles.py gave them a profile, fewer than
    ADULT_PERCENT of their rated works are rated suggestive or explicit, and MIN_WORKS of their works may be
    sent (see _held). Everything in an artist's row that comes from their works comes from the works that are
    sent, and a neighbour is always an artist who is sent too. Reads the local stores and changes nothing.
    """
    listed = {key: username for key, username, _ in store.all_artists(local)}
    written = {row["artist_key"]: row for row in profiles.profiles(local)}
    excluded = profiles.excluded(local)
    # The description is the verified set's own, checked against the image by the verifier.
    # ponytail: not the two-model agreed one (results(second=...)), which no critic has judged yet.
    results = tagging.results(profiles.TAGGER, db=local, verifier=profiles.TAGGER)
    contacts = tagging.contacts(profiles.TAGGER, local)
    works_of, fields = _collected(local)
    counts: Counter[str] = Counter()
    artists: Rows = []
    works: Rows = []
    slugs: dict[str, str] = {}  # artist key -> slug, for every artist who goes
    for key in sorted(listed.keys() & (works_of.keys() | fields.keys())):
        ready: list[tuple[str, str, str | None, list[str], dict[str, Any]]] = []  # page, thumbnail, title, tags, result
        ratings: list[str] = []
        for work_id, page, title, own_tags, thumb in works_of.get(key, []):
            result = results.get(work_id)
            if result and result.get("rating") in tagging.RATINGS:
                ratings.append(result["rating"])
            reason, address = _held(result), _addresses(page, thumb, listed[key])
            # A page is one work in the database: a second work with the same page is not sent.
            if result is None or reason or address is None or address[0] in {page_url for page_url, *_ in ready}:
                counts[f"works_held_{reason or 'bad_address'}"] += 1
            else:
                ready.append((*address, title, [tag for tag in json.loads(own_tags) if isinstance(tag, str)], result))
        tagline, website = fields.get(key, (None, None))
        site_url, site_domain = _own_site(website)
        slug = _slug(listed[key])
        why = None
        if key not in written:
            why = "profile_excluded" if key in excluded else "not_profiled"
        elif site_url and store.is_removed(site_url, local):  # a removed site the store's own links did not show
            why = "site_removed"
        elif 100 * sum(rating in ADULT_RATINGS for rating in ratings) >= ADULT_PERCENT * max(len(ratings), 1):
            why = "adult_portfolio"
        elif len(ready) < MIN_WORKS:
            why = "too_few_works"
        elif not slug or slug in slugs.values():  # two handles that differ only in what a slug cannot hold
            why = "slug_taken"
        if why:
            counts[f"artists_held_{why}"] += 1
            counts["works_held_artist_held_back"] += len(ready)
            continue
        slugs[key] = slug
        profile = written[key]
        used = Counter(tag for _, _, _, own, _ in ready for tag in set(own))
        most_used = [tag for tag, _ in sorted(used.items(), key=lambda item: (-item[1], item[0]))]
        facts = [value for field in profiles.STATED for value in profile[field]]
        # ponytail: the specialty the artist picked on DeviantArt is not in it: nothing has read that field
        # for a private contact. Add it when the tagging stage does.
        search_text = " ".join([*facts, *(title for _, _, title, _, _ in ready if title), *most_used])
        if len(search_text) > MAX_SEARCH_TEXT:
            search_text = search_text[:MAX_SEARCH_TEXT].rsplit(" ", 1)[0]
        artists.append(
            {
                "slug": slug,
                "name": listed[key],
                "da_username": listed[key],
                "site_url": site_url,
                "site_domain": site_domain,
                "tagline": tagline if contacts.get(key) == "no" else None,
                "top_tags": most_used[:TOP_TAGS],
                "search_text": search_text or None,
                "profile": profile["written"],
                "specialties": profile["specialties"],
                "mediums": profile["mediums"],
                "open_for_work": profile["open_for_work"],
                "profile_embedding": profile["embedding"],
            }
        )
        for position, (page_url, image_url, title, _, result) in enumerate(ready):
            verified = [result["medium"], *result["subjects"], *result["genres"], *result["style_words"]]
            verified += result.get("formats") or []
            works.append(
                {
                    "artist_slug": slug,
                    "source": "deviantart",
                    "page_url": page_url,
                    "image_url": image_url,
                    "title": title,
                    "description": result["description"],
                    "tags": [tag for tag in dict.fromkeys(verified) if tag != "unclear"],
                    "position": position,
                }
            )
    # One site is one artist's in the database (a unique index). A domain two of them give is neither's.
    domains = Counter(row["site_domain"] for row in artists if row["site_domain"])
    for row in artists:
        if domains[row["site_domain"]] > 1:
            row["site_url"] = row["site_domain"] = None
    neighbours: Rows = []
    for key, slug in slugs.items():
        # Everybody is asked for, then those who do not go are taken out: asking for 24 would leave fewer.
        # ponytail: style.neighbors scores every pair of artists on every call (0.2 s at 4,400 works, about
        # 10 s at 30,000), so this loop is that times the artists sent. It needs a call in pipeline/style.py
        # that gives the whole table at once.
        near = style.neighbors(key, STYLE_MODEL, len(listed), STYLE_RULE, local)
        going = [(slugs[other], score) for other, score in near if other in slugs and math.isfinite(score)]
        neighbours += [
            {"artist_slug": slug, "neighbor_slug": other, "rank": rank, "score": round(score, 4)}
            for rank, (other, score) in enumerate(going[:NEIGHBOURS], start=1)
        ]
    return artists, works, neighbours, counts


def _delete_sql(table: str, ids: set[int]) -> str:
    return f"delete from public.{table} where id in ({', '.join(map(str, sorted(ids)))})"


def run(dry_run: bool, local: pathlib.Path | str | None = None) -> dict[str, int]:
    """One push, or with `dry_run` everything but the writes to the database. Returns counts only.

    artists_sent and works_sent, each with what was held back by reason (ARTIST_REASONS, WORK_REASONS),
    neighbours_sent, artists_removed_from_database and works_removed_from_database (rows the database had
    that are no longer sent), removals_applied_here (artists the imported removal list deleted from the store)
    and artists_purged_here. RuntimeError when the database fails, ValueError or OSError when the removal
    cannot be done here: whichever comes, nothing after it is done.
    """
    applied = store.import_removed([row["key_hash"] for row in db.sql(REMOVED_SQL)], local)
    listed = {key for key, _, _ in store.all_artists(local)}
    purged = 0
    for key in library.artist_keys(local):
        if key not in listed:
            # As the collect stage does: the account goes on the list first, so that the same account under
            # a new handle is still known once the purge has deleted the profile that names it.
            if account := library.account_hash(key, local):
                store.import_removed([account], local)
            library.purge(key, local)
            purged += 1
    artists, works, neighbours, held = build(local)
    slugs = {row["slug"] for row in artists}
    sent = {(row["artist_slug"], row["page_url"]) for row in works}
    there = db.sql(LISTING_SQL)
    gone_artists = {int(row["artist_id"]) for row in there if row["slug"] not in slugs}
    gone_works = {
        int(row["work_id"])
        for row in there
        if row["work_id"] is not None and row["slug"] in slugs and (row["slug"], row["page_url"]) not in sent
    }
    if not dry_run:
        # Before the load: db.load ends by rebuilding the search words, and a handle or a site that has moved
        # to another artist must be free when their row is written.
        stale = [_delete_sql(table, ids) for table, ids in (("works", gone_works), ("artists", gone_artists)) if ids]
        if stale:
            db.sql("; ".join(stale))  # one call: both or neither
        db.load(artists, works, neighbours)
    return (
        {"artists_sent": len(artists)}
        | {f"artists_held_{reason}": held[f"artists_held_{reason}"] for reason in ARTIST_REASONS}
        | {"works_sent": len(works)}
        | {f"works_held_{reason}": held[f"works_held_{reason}"] for reason in WORK_REASONS}
        | {
            "neighbours_sent": len(neighbours),
            "artists_removed_from_database": len(gone_artists),
            "works_removed_from_database": len(gone_works),
            "removals_applied_here": applied,
            "artists_purged_here": purged,
        }
    )


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    parser.add_argument("--dry-run", action="store_true", help="do everything but write to the database")
    args = parser.parse_args(argv)
    try:
        counts = run(args.dry_run)
    except Exception as error:
        # Whatever fails, the run ends there with a status that is not zero and no counts. On stderr: the kind
        # of failure and the start of its message, no more, because a database error can quote the row it refused.
        sys.exit(f"stopped: {type(error).__name__}: {str(error)[:120]}")
    print(json.dumps(counts))  # counts only, nothing about any artist or work


if __name__ == "__main__":
    main()
