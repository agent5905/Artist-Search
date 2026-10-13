"""Push: turn what the local stages know into the rows the public database serves, and load them.

    uv run python -m pipeline.push --dry-run     everything but the writes to the database
    uv run python -m pipeline.push
    uv run python -m pipeline.push --allow-delete N     let this one run delete up to N artists, and N works

In this order, and the order is the point:
  1. Removal (hard rule 7), in both directions. The key hashes in the database's `removed` table go to
     store.import_removed, so a removal made through the site reaches this machine, and everything kept here
     about an artist the store no longer lists is purged. Then every hash on the store's list that the
     database's list lacks is added to it, so a removal made here outlives this machine, and the artists the
     list bars are deleted from the database. When any of that fails the run stops: nothing is built.
  2. The rows are built (see build) from what is left, and only from artists the store still lists.
  3. The database is brought to exactly those rows: an artist or a work it has that is not among them is
     deleted, then db.load writes the rest. So an artist who stops qualifying leaves on the next run.

Three things stop a run before step 3 writes anything, each with a message and a status that is not zero:
  - the database holds a slug that begins "zz-". Those are another builder's test rows, and this stage would
    delete them. It is the first thing looked at, before anything is done here or there;
  - the local library has no artists. An empty or a new file is no reason to empty the database;
  - more would be deleted than a run is believed about: more than DELETE_PERCENT of what the database holds,
    and more than DELETE_FLOOR. A changed prompt version or a half-finished analysis looks just like that.
    `--allow-delete N` lets one run go ahead. Artists on the removal list are not counted: step 1 is never
    held up by this.

The collect stage deletes nothing on DeviantArt's say-so: it leaves a mark, or lets a work's last sighting grow
old. What that means for the public is decided here (see build): nothing marked is sent, and neither is a work
that was not in its artist's latest answer.

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
MAX_WORKS = 10  # works sent for an artist: the most recently published of those that may go
# Of an artist's rated works, the share rated suggestive or explicit; and of the pictures on the sheet the profile
# stage looked at, the share it counted as sexualised or gory. At this share of either, the artist is left out.
ADULT_PERCENT = 30
SAFE_RATING = "safe"  # the only rating a work is sent with
ADULT_RATINGS = ("suggestive", "explicit")
NEIGHBOURS = 24  # similar artists sent for each artist: as many as similar_artists ever returns
DELETE_PERCENT = 10  # of the artists, or of the works, the database holds: more than this going at once is not believed
DELETE_FLOOR = 5  # deletions of each kind that a run may always make, however little the database holds
TEST_SLUG = "zz-"  # how the other builders' test rows begin. Never sent, and a database that has one is left alone
STYLE_MODEL, STYLE_RULE = "csd", "local"  # round 7's blind test chose them
TOP_TAGS = 12  # of the artist's own tags, the most used
MAX_SEARCH_TEXT = 4000  # sql/001_schema.sql: artists_own_words_sizes

# Besides these, one reason for each mark the collect stage has left: "marked_<its reason>" (see _marked).
ARTIST_REASONS = (
    *("not_profiled", "profile_excluded", "site_removed", "adult_portfolio", "too_few_works", "slug_unusable"),
)
WORK_REASONS = (
    *("not_seen_lately", "not_tagged", "not_artwork", "explicit", "declared_ai", "contact_in_words"),
    *("words_not_read", "not_rated", "suggestive", "bad_address", "artist_held_back", "over_limit"),
)

REMOVED_SQL = "select key_hash from public.removed"
# Every artist the database has, with the two hashes its removal list knows them by, and every work: what a run
# compares its rows with.
# ponytail: one answer with a row a work, about 8 MB at 30,000 works. Page it by artist id if that is refused.
LISTING_SQL = (
    "select a.id as artist_id, a.slug, a.da_key, a.site_key, w.id as work_id, w.page_url "
    "from public.artists a left join public.works w on w.artist_id = a.id"
)
_SHA256 = re.compile(r"[0-9a-f]{64}")
# sql/001_schema.sql: clean_url. A row the database would refuse fails the whole batch it is in, so an address
# that would not pass is held back here.
_CLEAN_URL = re.compile(
    r"https?://(?=[^/?:]{4,253}(?:[/?:]|$))"
    r"(?:[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\.)+(?:[a-z]{2,63}|xn--[a-z0-9-]{1,59})"
    r"(?::[0-9]{1,5})?(?:[/?][A-Za-z0-9._~:/?@!$&'()*+,;=%-]*)?"
)

Rows = list[dict[str, Any]]


class Stop(Exception):
    """A run that was stopped on purpose. The message is for the operator; `counts` is what the run had counted,
    when it got that far."""

    def __init__(self, why: str, counts: dict[str, int] | None = None) -> None:
        super().__init__(why)
        self.counts = counts


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


def _marked(reason: str) -> str:
    """The reason a marked work or artist is held back: "marked_" and the collect stage's own words for it, as
    one word. Whatever it marks, and whatever it comes to call it, is held back and counted under that name."""
    return "marked_" + "_".join(re.findall(r"[a-z0-9]+", reason.lower()))


def _size(size: object) -> tuple[int | None, int | None]:
    """A thumbnail's (width, height) as the database takes them: two whole numbers of pixels, each from 1 to
    100,000 (sql/001_schema.sql). Anything else is no size: two Nones, never one number without the other."""
    if isinstance(size, list | tuple) and len(size) == 2:
        if all(type(side) is int and 0 < side <= 100_000 for side in size):
            return size[0], size[1]
    return None, None


def _too_many(part: int, whole: int) -> bool:
    """True when `part` is ADULT_PERCENT or more of a `whole` that is not nothing."""
    return whole > 0 and 100 * part >= ADULT_PERCENT * whole


def _reader(local: pathlib.Path | str | None) -> contextlib.closing[sqlite3.Connection]:
    """A connection to the local file that cannot write."""
    path = pathlib.Path(local or config.LOCAL_DB).resolve()
    return contextlib.closing(sqlite3.connect(f"{path.as_uri()}?mode=ro", uri=True, timeout=60))


def _removed_here(local: pathlib.Path | str | None) -> set[str]:
    """Every key hash on the store's removal list: artists, their sites, and the accounts the collect stage adds.
    ponytail: the store has no reader for its list, so it is selected here. Move it there when it has."""
    with _reader(local) as conn:
        return {key_hash for (key_hash,) in conn.execute("SELECT key_hash FROM removed")}


def _collected(
    local: pathlib.Path | str | None,
) -> tuple[dict[str, list[tuple[Any, ...]]], dict[str, tuple[str | None, str | None, str]]]:
    """What the library holds, read through a connection that cannot write.

    Each artist's works, the most recently published first, as (deviation id, page, title, the artist's tags as
    JSON, thumbnail, when it was last seen and still qualified); and each artist's (tagline, website, when their
    profile was last stored).
    ponytail: the library has no reader for these columns, so they are selected here. Move it there when it has.
    """
    works: dict[str, list[tuple[Any, ...]]] = {}
    with _reader(local) as conn:
        for key, *work in conn.execute(
            "SELECT artist_key, deviation_id, page_url, title, tags, thumb_url, fetched_at FROM works "
            "ORDER BY artist_key, published_at IS NULL, published_at DESC, deviation_id"
        ):
            works.setdefault(key, []).append(tuple(work))
        rows = conn.execute("SELECT artist_key, tagline, website, fetched_at FROM profiles").fetchall()
    return works, {key: (tagline, website, stored) for key, tagline, website, stored in rows}


def build(local: pathlib.Path | str | None = None) -> tuple[Rows, Rows, Rows, Counter[str]]:
    """The rows for db.load (artists, works, neighbours) and the count of what was held back, by reason.

    An artist goes when the store lists them, the collect stage has left no mark on them, pipeline/profiles.py
    gave them a profile, their portfolio is not an adult one (ADULT_PERCENT, of their rated works or of the
    pictures on their sheet, whichever says so), and MIN_WORKS of their works may be sent.

    A work may be sent when the collect stage has left no mark on it, it was in the artist's latest answer from
    DeviantArt, and it passes _held. The collect stage deletes nothing any more: a work it finds mature,
    somebody else's, no longer public or titled with contact details stays here with a mark, and a work the
    artist has taken down stays with the time it was last seen, which is before their profile was last stored.
    Of the works that may be sent, the MAX_WORKS most recently published are, each with its thumbnail's size
    when the collect stage has it.

    Everything in an artist's row that comes from their works comes from the works that are sent, and a
    neighbour is always an artist who is sent too. An artist who goes and has no neighbour gets one row that
    names no neighbour (see the end of this function). Reads the local stores and changes nothing.
    """
    listed = {key: username for key, username, _ in store.all_artists(local)}
    marked_artists, marked_works = library.artist_marks(local), library.work_marks(local)
    sizes = library.thumb_sizes(local)  # of the thumbnails the works' rows name; a work stored before sizes has none
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
        # page, thumbnail, title, the artist's tags, verified result, the thumbnail's (width, height)
        ready: list[tuple[str, str, str | None, list[str], dict[str, Any], tuple[int | None, int | None]]] = []
        ratings: list[str] = []
        tagline, website, stored = fields.get(key, (None, None, ""))
        for work_id, page, title, own_tags, thumb, seen in works_of.get(key, []):
            result = results.get(work_id)
            if result and result.get("rating") in tagging.RATINGS:
                ratings.append(result["rating"])
            reason, address = _held(result), _addresses(page, thumb, listed[key])
            if work_id in marked_works:
                reason = _marked(marked_works[work_id][0])
            elif seen < stored:  # not in the answer their profile was last stored with: taken down, as far as is known
                reason = "not_seen_lately"
            # A page is one work in the database: a second work with the same page is not sent.
            if result is None or reason or address is None or address[0] in {page_url for page_url, *_ in ready}:
                counts[f"works_held_{reason or 'bad_address'}"] += 1
            else:
                own = [tag for tag in json.loads(own_tags) if isinstance(tag, str)]
                ready.append((*address, title, own, result, _size(sizes.get(work_id))))
        site_url, site_domain = _own_site(website)
        slug = _slug(listed[key])
        why = None
        if key in marked_artists:
            why = _marked(marked_artists[key][0])
        elif key not in written:
            why = "profile_excluded" if key in excluded else "not_profiled"
        elif site_url and store.is_removed(site_url, local):  # a removed site the store's own links did not show
            why = "site_removed"
        # By either measure: the rating each work was given, or the profile stage's count over the artist's sheet
        # (a profile made before it counted has neither number, and is judged on the ratings alone).
        elif _too_many(sum(rating in ADULT_RATINGS for rating in ratings), len(ratings)) or _too_many(
            written[key].get("unsuitable_pictures") or 0, written[key].get("pictures") or 0
        ):
            why = "adult_portfolio"
        elif len(ready) < MIN_WORKS:
            why = "too_few_works"
        # No slug at all, the slug of a test row (see TEST_SLUG), or one that another handle has already made.
        elif not slug or slug.startswith(TEST_SLUG) or slug in slugs.values():
            why = "slug_unusable"
        if why:
            counts[f"artists_held_{why}"] += 1
            counts["works_held_artist_held_back"] += len(ready)
            continue
        counts["works_held_over_limit"] += len(ready[MAX_WORKS:])
        ready = ready[:MAX_WORKS]  # the most recently published: that is the order they were read in
        slugs[key] = slug
        profile = written[key]
        used = Counter(tag for _, _, _, own, *_ in ready for tag in set(own))
        most_used = [tag for tag, _ in sorted(used.items(), key=lambda item: (-item[1], item[0]))]
        facts = [value for field in profiles.STATED for value in profile.get(field, [])]  # an older row may lack a kind
        # ponytail: the specialty the artist picked on DeviantArt is not in it: nothing has read that field
        # for a private contact. Add it when the tagging stage does.
        search_text = " ".join([*facts, *(title for _, _, title, *_ in ready if title), *most_used])
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
        for position, (page_url, image_url, title, _, result, (width, height)) in enumerate(ready):
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
                    # The thumbnail's size, when the collect stage has it. Every work carries both keys either
                    # way, because the database wants one shape a load: two numbers, or two Nones.
                    "width": width,
                    "height": height,
                }
            )
    # One site is one artist's in the database (a unique index). A domain two of them give is neither's.
    domains = Counter(row["site_domain"] for row in artists if row["site_domain"])
    for row in artists:
        if domains[row["site_domain"]] > 1:
            row["site_url"] = row["site_domain"] = None
    # One pass, with the artists who go as the whole pool. What comes back is still held to that pool here: this
    # is the last place an artist who was left out could get into a row.
    near = style.all_neighbors(STYLE_MODEL, NEIGHBOURS, STYLE_RULE, set(slugs), local)
    neighbours: Rows = []
    for key, slug in slugs.items():
        going = [
            (slugs[other], score)
            for other, score in near.get(key, [])
            if other in slugs and other != key and math.isfinite(score)
        ]
        # replace_neighbors (sql/002_functions.sql) leaves an artist it gets no row for with the neighbours they
        # had. It has no way to be told "none", but a row that names the artist and no neighbour does it: the row
        # is skipped, and every rank it did not write for that artist is deleted. db.load counts it as skipped.
        neighbours += [
            {"artist_slug": slug, "neighbor_slug": other, "rank": rank, "score": round(score, 4)}
            for rank, (other, score) in enumerate(going[:NEIGHBOURS], start=1)
        ] or [{"artist_slug": slug, "neighbor_slug": None, "rank": None, "score": None}]
    return artists, works, neighbours, counts


def _delete_sql(table: str, ids: set[int]) -> str:
    return f"delete from public.{table} where id in ({', '.join(map(str, sorted(ids)))})"


def _removed_sql(key_hashes: set[str]) -> str:
    """The statement that puts key hashes on the database's removal list. ValueError unless every one is a sha256
    hex digest, because they go into the statement as they are."""
    if not all(_SHA256.fullmatch(key_hash) for key_hash in key_hashes):
        raise ValueError("the local removal list holds something that is not a sha256 hex digest")
    values = ", ".join(f"('{key_hash}')" for key_hash in sorted(key_hashes))
    return f"insert into public.removed (key_hash) values {values} on conflict (key_hash) do nothing"


def run(dry_run: bool, allow_delete: int | None = None, local: pathlib.Path | str | None = None) -> dict[str, int]:
    """One push, or with `dry_run` everything but the writes to the database. Returns counts only.

    artists_sent and works_sent, each with what was held back by reason (ARTIST_REASONS, WORK_REASONS, and a
    line for each mark of the collect stage that held something back: "..._held_marked_<its reason>");
    neighbours_sent, artists_sent_without_neighbours, and neighbours_skipped_by_database (what db.load said
    the database would not take: a row that names an artist it does not have); artists_deleted_from_database
    and works_deleted_from_database (rows it had that are no longer sent); removed_artists_deleted_from_database
    and removals_added_to_database (artists the removal list bars, and key hashes the database's list lacked);
    removals_applied_here (artists the imported list deleted from the store) and artists_purged_here.

    Stop, and nothing more is done, in the three cases the module's docstring names; when too much would be
    deleted it carries the counts. `allow_delete` is how many artists, and how many works, this one run may
    delete instead. RuntimeError when the database fails, ValueError or OSError when the removal cannot be done
    here: whichever comes, nothing after it is done.
    """
    there = db.sql(LISTING_SQL)
    if any(str(row["slug"]).startswith(TEST_SLUG) for row in there):
        raise Stop(
            f'the database holds test rows (slugs that begin "{TEST_SLUG}"): another builder\'s tests are using '
            "it, and a push would delete them. Nothing was done. Run it again when they have finished."
        )
    remote = {row["key_hash"] for row in db.sql(REMOVED_SQL)}
    applied = store.import_removed(remote, local)
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
    # The other direction. The store's list now holds the database's and whatever was removed on this machine:
    # what the database's list lacks is added, so that another machine, or this file made again, hears of it.
    # The artists the list bars go now, whatever happens further down: the database deletes them itself when
    # their hash arrives, and one it had to leave (a load held the row) is deleted here by id.
    here = _removed_here(local)
    unlisted = here - remote
    barred = {int(row["artist_id"]) for row in there if here & {row["da_key"], row["site_key"]}}
    if not dry_run and (unlisted or barred):
        removal = ([_removed_sql(unlisted)] if unlisted else []) + ([_delete_sql("artists", barred)] if barred else [])
        db.sql("; ".join(removal))
    if not library.artist_keys(local):
        raise Stop(
            "the local library has no artists, so there is nothing to push, and a push would delete every artist "
            "the database has. Nothing was deleted or loaded."
        )
    artists, works, neighbours, held = build(local)
    slugs = {row["slug"] for row in artists}
    sent = {(row["artist_slug"], row["page_url"]) for row in works}
    have_artists = {int(row["artist_id"]) for row in there} - barred
    gone_artists = {int(row["artist_id"]) for row in there if row["slug"] not in slugs} - barred
    have_works = [row for row in there if row["work_id"] is not None and row["slug"] in slugs]  # of artists who stay
    gone_works = {int(row["work_id"]) for row in have_works if (row["slug"], row["page_url"]) not in sent}
    empty = sum(row["neighbor_slug"] is None for row in neighbours)

    def held_back(kind: str, reasons: tuple[str, ...]) -> dict[str, int]:
        """A line for every reason there always is, then one for each mark that held something back this run."""
        marks = sorted(name for name in held if name.startswith(f"{kind}_held_marked_"))
        return {name: held[name] for name in (*(f"{kind}_held_{reason}" for reason in reasons), *marks)}

    counts = (
        {"artists_sent": len(artists)}
        | held_back("artists", ARTIST_REASONS)
        | {"works_sent": len(works)}
        | held_back("works", WORK_REASONS)
        | {
            "neighbours_sent": len(neighbours) - empty,
            "artists_sent_without_neighbours": empty,
            "neighbours_skipped_by_database": 0,
            "artists_deleted_from_database": len(gone_artists),
            "works_deleted_from_database": len(gone_works),
            "removed_artists_deleted_from_database": len(barred),
            "removals_added_to_database": len(unlisted),
            "removals_applied_here": applied,
            "artists_purged_here": purged,
        }
    )

    def allowed(have: int) -> int:
        return max(DELETE_FLOOR, have * DELETE_PERCENT // 100) if allow_delete is None else allow_delete

    if len(gone_artists) > allowed(len(have_artists)) or len(gone_works) > allowed(len(have_works)):
        raise Stop(
            f"this run would delete {len(gone_artists)} of the {len(have_artists)} artists the database has (it may "
            f"delete {allowed(len(have_artists))}) and {len(gone_works)} of the {len(have_works)} works of the artists "
            f"who stay (it may delete {allowed(len(have_works))}). Nothing was deleted or loaded. If that is what "
            f"should happen, run it again with --allow-delete {max(len(gone_artists), len(gone_works))}.",
            counts,
        )
    if not dry_run:
        # Before the load: db.load ends by rebuilding the search words, and a handle or a site that has moved
        # to another artist must be free when their row is written.
        stale = [_delete_sql(table, ids) for table, ids in (("works", gone_works), ("artists", gone_artists)) if ids]
        if stale:
            db.sql("; ".join(stale))  # one call: both or neither
        # Each row that names no neighbour is one the database counts as skipped: those are not news.
        counts["neighbours_skipped_by_database"] = max((db.load(artists, works, neighbours) or 0) - empty, 0)
    return counts


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    parser.add_argument("--dry-run", action="store_true", help="do everything but write to the database")
    parser.add_argument(
        "--allow-delete",
        type=int,
        metavar="N",
        help="let this one run delete up to N artists who no longer qualify, and up to N works of artists who stay "
        f"(without it: {DELETE_PERCENT} percent of what the database has, and never fewer than {DELETE_FLOOR})",
    )
    args = parser.parse_args(argv)
    try:
        counts = run(args.dry_run, args.allow_delete)
    except Stop as stop:
        if stop.counts is not None:
            print(json.dumps(stop.counts))  # what the run would have done
        sys.exit(f"stopped: {stop}")
    except Exception as error:
        # Whatever fails, the run ends there with a status that is not zero and no counts. On stderr: the kind
        # of failure and the start of its message, no more, because a database error can quote the row it refused.
        sys.exit(f"stopped: {type(error).__name__}: {str(error)[:120]}")
    print(json.dumps(counts))  # counts only, nothing about any artist or work


if __name__ == "__main__":
    main()
