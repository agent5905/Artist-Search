"""Push: turn what the local stages know into the rows the public database serves, and load them.

    uv run python -m pipeline.push --dry-run     what a run would do, and nothing changed anywhere
    uv run python -m pipeline.push
    uv run python -m pipeline.push --allow-delete N     let this one run delete up to N of each kind

In this order, and the order is the point:
  1. Removal (hard rule 7), in both directions. It is worked out first, with nothing changed (see _removal),
     from the two removal lists and from nothing else: the database's `removed` table and the store's. Whoever
     they bar is barred under every handle their account is kept under here, and the account goes on the list
     too. Then it is written down, here first (store.import_removed) and then in the database: the hashes its
     list lacks are added, so a removal made here outlives this machine, and the artists the list bars are
     deleted there. Only after that is what is kept here about them purged. A file that cannot be deleted is
     counted, and the next run tries again: the removal stands meanwhile, here and there, and the run goes on
     (it ends with a status that is not zero, and says why). When writing it down fails the run stops: nothing
     is purged and nothing is built.
  2. The rows are built (see build) from what is left, and only from artists the store still lists.
  3. The database is brought to exactly those rows: an artist or a work it has that is not among them is
     deleted, then db.load writes the rest. So an artist who stops qualifying leaves on the next run. An artist
     who loses a work has the words of their works taken out of their own row in the same call that deletes
     the work, so that a load that fails leaves none of them behind.

Four things stop a run, each with a message and a status that is not zero:
  - the database holds a slug that begins "zz-". Those are another builder's test rows, and this stage would
    delete them. It is the first thing looked at, before anything is done here or there;
  - more than DELETE_FLOOR key hashes would be added to the database's removal list, where they stay for good.
    Removals come one at a time, so many at once is a broken local store and nobody's wish. Nothing is done,
    here or there, and `--allow-delete N` lets N of them through;
  - the local library has no artists. An empty or a new file is no reason to empty the database;
  - more would be deleted than a run is believed about: more than DELETE_PERCENT of the artists the database
    holds, of their works or of their neighbour rows, and more than DELETE_FLOOR. A changed prompt version, a
    half-finished analysis or a lost table looks just like that. `--allow-delete N` lets one run go ahead.
    Artists on the removal list are not counted: step 1 is never held up by this.
The last two stop a run after step 1 and before step 3 writes anything.

`--dry-run` changes nothing: not the database, not the local store, not a file. It reads both, works out what a
run would do and prints the same counts, and stops where a run would stop.

The collect stage deletes nothing on DeviantArt's say-so: it leaves a mark, or lets a work's last sighting grow
old. What that means for the public is decided here (see build): nothing marked is sent, and neither is a work
that was not in its artist's latest answer.

Hard rule 1: a work is sent as two addresses, its page on DeviantArt and DeviantArt's own thumbnail, and words.
No image, no file name and no deviation id is ever put in a row.
Hard rule 6: an artist's own words (a title, their tags, a tagline) are sent only when the tagging stage read
them and found no private contact. A work's page address is made from its title, so a work whose words hold a
contact, or were not read, is not sent at all. And every text that is sent, whoever wrote it, is held to the
collect stage's plain rule as it goes (library.holds_contact): a description is a model's words about an image,
and an image can show an address. A text that fails is left out, and a work whose title or description fails is
not sent. The handle is the one text that is not asked: it is the name the artist is listed under.
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
from collections.abc import Collection
from typing import Any
from urllib.parse import urlsplit

from pipeline import config, crawl, db, library, profiles, store, style, tagging

MIN_WORKS = 4  # publishable works: an artist with fewer is not sent
MAX_WORKS = 10  # works sent for an artist: the most recently published of those that may go
# Of an artist's works that are rated or have an image to rate, the share rated suggestive or explicit, or not
# rated yet; and of the pictures on the sheet the profile stage looked at, the share it counted as sexualised or
# gory. At this share of either, the artist is left out.
ADULT_PERCENT = 30
SAFE_RATING = "safe"  # the only rating a work is sent with
ADULT_RATINGS = ("suggestive", "explicit")
NEIGHBOURS = 24  # similar artists sent for each artist: as many as similar_artists ever returns
# Of a similar artist's works, those picked to show beside the artist: as many as a card shows, and as the
# database takes (sql/001_schema.sql: artist_neighbors_few_picks).
PICKS = 4
# Of the artists the database holds, of their works or of their neighbour rows: more than this going at once is
# not believed.
DELETE_PERCENT = 10
# Deletions of each kind that a run may always make, however little the database holds. And the key hashes a run
# may always add to the database's removal list: one removal is two or three (each handle, the account, a site).
DELETE_FLOOR = 5
TEST_SLUG = "zz-"  # how the other builders' test rows begin. Never sent, and a database that has one is left alone
STYLE_MODEL, STYLE_RULE = "csd", "local"  # round 7's blind test chose them
TOP_TAGS = 12  # of the artist's own tags, the most used
MAX_SEARCH_TEXT = 4000  # sql/001_schema.sql: artists_own_words_sizes

# Besides these, one reason for each mark the collect stage has left: "marked_<its reason>" (see _marked).
ARTIST_REASONS = (
    *("not_profiled", "profile_excluded", "site_removed", "adult_portfolio", "not_all_rated", "too_few_works"),
    "slug_unusable",
)
WORK_REASONS = (
    *("not_seen_lately", "not_tagged", "not_artwork", "explicit", "declared_ai", "contact_in_words"),
    *("words_not_read", "not_rated", "suggestive", "contact_by_rule", "bad_address", "artist_held_back"),
    "over_limit",
)
# The local library has nothing to push. Said for a dry run that finds no file too, without making one.
NO_ARTISTS = (
    "the local library has no artists, so there is nothing to push, and a push would delete every artist the "
    "database has. Nothing was deleted or loaded."
)

REMOVED_SQL = "select key_hash from public.removed"
# Every artist the database has, with the two hashes its removal list knows them by and how many neighbour rows
# they have, and every work: what a run compares its rows with.
# ponytail: one answer with a row a work, about 8 MB at 30,000 works. Page it by artist id if that is refused.
LISTING_SQL = (
    "select a.id as artist_id, a.slug, a.da_key, a.site_key, w.id as work_id, w.page_url, "
    "(select count(*) from public.artist_neighbors n where n.artist_id = a.id) as neighbours "
    "from public.artists a left join public.works w on w.artist_id = a.id"
)
# What an artist's row holds of their works' own words, emptied: the load that follows writes it again.
BLANK_SQL = "update public.artists set search_text = '', top_tags = '{{}}' where id in ({})"
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
    JSON, thumbnail, when it was last seen and still qualified, whether it has a cached image, its rating from
    tagging.RATINGS or None); and each artist's (tagline, website, when their profile was last stored).
    ponytail: the library has no reader for these columns, and the tagging stage has none for the rating of a
    work that has no tags, so they are selected here. Move it there when they have.
    """
    works: dict[str, list[tuple[Any, ...]]] = {}
    with _reader(local) as conn:
        for key, *work, sorted_as in conn.execute(
            "SELECT w.artist_key, w.deviation_id, w.page_url, w.title, w.tags, w.thumb_url, w.fetched_at, "
            "w.image_file IS NOT NULL, f.result FROM works w LEFT JOIN work_format f "
            "ON f.deviation_id = w.deviation_id AND f.model = ? AND f.prompt_version = ? "
            "ORDER BY w.artist_key, w.published_at IS NULL, w.published_at DESC, w.deviation_id",
            (profiles.TAGGER, tagging.FORMAT_VERSION),
        ):
            rating = json.loads(sorted_as).get("rating") if sorted_as else None
            works.setdefault(key, []).append((*work, rating if rating in tagging.RATINGS else None))
        rows = conn.execute("SELECT artist_key, tagline, website, fetched_at FROM profiles").fetchall()
    return works, {key: (tagline, website, stored) for key, tagline, website, stored in rows}


def build(
    local: pathlib.Path | str | None = None, removed: Collection[str] = ()
) -> tuple[Rows, Rows, Rows, Counter[str]]:
    """The rows for db.load (artists, works, neighbours) and the count of what was held back, by reason.

    An artist goes when the store lists them, the collect stage has left no mark on them, pipeline/profiles.py
    gave them a profile, their portfolio is not an adult one (ADULT_PERCENT, by their works' ratings or by the
    pictures on their sheet, whichever says so), and MIN_WORKS of their works may be sent. `removed` is key
    hashes the store's removal list does not hold yet and will (see _removal): whoever they bar is not built.

    Every rating an artist's works have counts towards that share, whether the work has verified tags or not:
    the tagging stage rates every image. A work with an image and no rating counts as one that is not safe until
    it is rated. An artist those tip over the share, and the ratings alone would not, is held back as
    not_all_rated.

    A work may be sent when the collect stage has left no mark on it, it was in the artist's latest answer from
    DeviantArt, it passes _held, and the plain rule finds no contact in its title or its description. Every
    other text is held to that rule too, and one that fails is left out and counted (see the module's
    docstring). The collect stage deletes nothing any more: a work it finds mature,
    somebody else's, no longer public or titled with contact details stays here with a mark, and a work the
    artist has taken down stays with the time it was last seen, which is before their profile was last stored.
    Of the works that may be sent, the MAX_WORKS most recently published are, each with its thumbnail's size
    when the collect stage has it.

    Everything in an artist's row that comes from their works comes from the works that are sent, and a
    neighbour is always an artist who is sent too. A neighbour's row says which of their works to show beside
    the artist: the PICKS closest in style to the artist's (style.all_closest_works), best first, counting only
    works that are sent on either side. An artist who goes and has no neighbour gets one row that names no
    neighbour (see the end of this function). Reads the local stores and changes nothing.
    """
    listed = {key: username for key, username, _ in store.all_artists(local) if not store.covers(removed, key)}
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

    def plain(texts: list[str]) -> list[str]:
        """Of these texts, the ones the plain rule finds no contact in. The others are left out, and counted."""
        kept = [text for text in texts if not library.holds_contact(text)]
        counts["texts_left_out_for_contact"] += len(texts) - len(kept)
        return kept

    artists: Rows = []
    works: Rows = []
    slugs: dict[str, str] = {}  # artist key -> slug, for every artist who goes
    sent: dict[str, list[str]] = {}  # artist key -> the deviation ids of their works that go, by position
    for key in sorted(listed.keys() & (works_of.keys() | fields.keys())):
        # page, thumbnail, title, the artist's tags, verified result, the thumbnail's (width, height), deviation id
        ready: list[tuple[str, str, str | None, list[str], dict[str, Any], tuple[int | None, int | None], str]] = []
        ratings: list[str | None] = []  # of every work that is rated, or has an image to rate: None is "not yet"
        tagline, website, stored = fields.get(key, (None, None, ""))
        for work_id, page, title, own_tags, thumb, seen, cached, rating in works_of.get(key, []):
            result = results.get(work_id)
            if rating or cached:
                ratings.append(rating)
            reason, address = _held(result), _addresses(page, thumb, listed[key])
            if work_id in marked_works:
                reason = _marked(marked_works[work_id][0])
            elif seen < stored:  # not in the answer their profile was last stored with: taken down, as far as is known
                reason = "not_seen_lately"
            elif result and not reason and any(map(library.holds_contact, (title, result["description"]))):
                reason = "contact_by_rule"
            # A page is one work in the database: a second work with the same page is not sent.
            if result is None or reason or address is None or address[0] in {page_url for page_url, *_ in ready}:
                counts[f"works_held_{reason or 'bad_address'}"] += 1
            else:
                own = [tag for tag in json.loads(own_tags) if isinstance(tag, str)]
                ready.append((*address, title, own, result, _size(sizes.get(work_id)), work_id))
        site_url, site_domain = _own_site(website)
        slug = _slug(listed[key])
        adult = sum(rating in ADULT_RATINGS for rating in ratings)
        why = None
        if key in marked_artists:
            why = _marked(marked_artists[key][0])
        # A sentence the plain contact rule refuses is no profile to show: the artist waits for another.
        elif key not in written or library.holds_contact(written[key]["written"]):
            why = "profile_excluded" if key in excluded else "not_profiled"
        # A removed site the store's own links did not show.
        elif site_url and (store.is_removed(site_url, local) or store.covers(removed, site_url)):
            why = "site_removed"
        # By either measure: the rating each work was given, or the profile stage's count over the artist's sheet
        # (a profile made before it counted has neither number, and is judged on the ratings alone).
        elif _too_many(adult, len(ratings)) or _too_many(
            written[key].get("unsuitable_pictures") or 0, written[key].get("pictures") or 0
        ):
            why = "adult_portfolio"
        elif _too_many(adult + ratings.count(None), len(ratings)):  # an image nobody has rated is not safe yet
            why = "not_all_rated"
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
        sent[key] = [work_id for *_, work_id in ready]
        profile = written[key]
        used = Counter(tag for _, _, _, own, *_ in ready for tag in set(own))
        most_used = plain([tag for tag, _ in sorted(used.items(), key=lambda item: (-item[1], item[0]))])
        # An older row may lack a kind.
        facts = plain([value for field in profiles.STATED for value in profile.get(field, [])])
        # ponytail: the specialty the artist picked on DeviantArt is not in it: nothing has read that field
        # for a private contact. Add it when the tagging stage does.
        # ponytail: every part of it has passed the plain rule as a text of its own, and the text they make is
        # not read again as one. Digits only add up there: on 2026-10-13, 72 of 271 real artists failed that
        # way, 70 on nine digits from ten titles and 2 on seven beside "pastel". Ceiling: a number spread over
        # several titles on purpose, which is as public on the works' own rows. Read it as one when the rule
        # can tell a year from a number to call.
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
                "tagline": next(iter(plain([tagline])), None) if tagline and contacts.get(key) == "no" else None,
                "top_tags": most_used[:TOP_TAGS],
                "search_text": search_text or None,
                "profile": next(iter(plain([profile["written"]])), None),
                "specialties": plain(profile["specialties"]),
                "mediums": plain(profile["mediums"]),
                "open_for_work": profile["open_for_work"],
                "profile_embedding": profile["embedding"],
            }
        )
        for position, (page_url, image_url, title, _, result, (width, height), _) in enumerate(ready):
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
                    "tags": plain([tag for tag in dict.fromkeys(verified) if tag != "unclear"]),
                    "position": position,
                    # The thumbnail's size, when the collect stage has it. Every work carries both keys either
                    # way, because the database wants one shape a load: two numbers, or two Nones.
                    "width": width,
                    "height": height,
                }
            )
    # One site is one artist's in the database (a unique index), and a removal of that artist bars the site. So
    # a domain two artists give is neither's, whether or not both of them go: it is enough that both are kept here.
    domains = Counter(domain for _, website, _ in fields.values() if (domain := _own_site(website)[1]))
    for row in artists:
        if domains[row["site_domain"]] > 1:
            row["site_url"] = row["site_domain"] = None
    # One pass, with the artists who go as the whole pool. What comes back is still held to that pool here: this
    # is the last place an artist who was left out could get into a row.
    near = style.all_neighbors(STYLE_MODEL, NEIGHBOURS, STYLE_RULE, set(slugs), local)
    going = {
        key: [
            (other, score)
            for other, score in near.get(key, [])
            if other in slugs and other != key and math.isfinite(score)
        ][:NEIGHBOURS]
        for key in slugs
    }
    # What a neighbour is shown with beside the artist: of the neighbour's works that go, the closest in style to
    # the artist's that go. A work is named by its position among its artist's, never by its deviation id.
    pairs = [(key, other) for key, others in going.items() for other, _ in others]
    closest = style.all_closest_works(pairs, sent, STYLE_MODEL, PICKS, local)
    neighbours: Rows = []
    for key, slug in slugs.items():
        # replace_neighbors (sql/002_functions.sql) leaves an artist it gets no row for with the neighbours they
        # had. It has no way to be told "none", but a row that names the artist and no neighbour does it: the row
        # is skipped, and every rank it did not write for that artist is deleted. db.load counts it as skipped.
        neighbours += [
            {
                "artist_slug": slug,
                "neighbor_slug": slugs[other],
                "rank": rank,
                "score": round(score, 4),
                # None when either has no work that goes with a style vector: the neighbour's newest are shown.
                "picks": [sent[other].index(work) for work in closest[key, other]] or None,
            }
            for rank, (other, score) in enumerate(going[key], start=1)
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


def _removal(
    remote: set[str], there: Rows, kept: set[str], local: pathlib.Path | str | None
) -> tuple[set[str], set[str], int]:
    """What hard rule 7 asks of this run, worked out with nothing changed: every key hash the two removal lists
    must hold when the run is over, the keys of the artists those bar that are stored or kept here, and how many
    of them the store will stop listing.

    Only a removal list says who is removed: `remote`, the database's, and the store's own. An artist the
    store's `artists` table merely lacks is nobody's removal. That table was emptied once, and the run that
    followed purged the whole library and barred every account.

    A hash bars the stored artists it names, by their handle or by the site of their stored link
    (store.named_by), and the artists kept here (`kept`, the library's keys) under a handle it names. A removal
    covers the account, so it reaches every other handle the same account is kept under, as the collect stage
    linked them (library.same_account), and theirs in turn: no further, and never another artist. Each of those
    handles goes on the list, and each account's hash with it, so that the account is known for what it is
    under a handle it takes later, when the purge has deleted the profile that names it.

    An artist the database (`there`, its listing) shows with a removed site is removed under their handle as
    well, which is what the store does for the artists it lists: they stay out when their profile names
    another site.

    ValueError when the database's list holds something that is not a sha256 hex digest.
    """
    if not all(_SHA256.fullmatch(key_hash) for key_hash in remote):
        raise ValueError("the database's removal list holds something that is not a sha256 hex digest")
    hashes = _removed_here(local) | remote
    hashes |= {row["da_key"] for row in there if row["site_key"] in hashes}
    barred: set[str] = set()
    while True:  # until a pass finds nobody new: each pass but the last bars somebody
        stored = store.named_by(hashes, local)
        more = {*stored, *(key for key in kept if store.covers(hashes, key))}
        more |= {other for key in more for other in library.same_account(key, local)}
        if more <= barred:
            return hashes, barred, len(stored)
        barred |= more
        hashes |= {store._hash(key) for key in more}
        hashes |= {account for key in more if (account := library.account_hash(key, local))}


def run(dry_run: bool, allow_delete: int | None = None, local: pathlib.Path | str | None = None) -> dict[str, int]:
    """One push, or with `dry_run` what one would do, with nothing changed here or there. Returns counts only.

    artists_sent and works_sent, each with what was held back by reason (ARTIST_REASONS, WORK_REASONS, and a
    line for each mark of the collect stage that held something back: "..._held_marked_<its reason>");
    texts_left_out_for_contact (tags, taglines and other texts of rows that are sent, left out by the plain
    rule); neighbours_sent, artists_sent_without_neighbours, and neighbours_skipped_by_database (what db.load
    said the database would not take: a row that names an artist it does not have);
    artists_deleted_from_database, works_deleted_from_database and neighbours_deleted_from_database (rows it had
    that are no longer sent; of neighbour rows, those an artist who stays loses and gets no other for);
    removed_artists_deleted_from_database and removals_added_to_database (artists the removal list bars, and key
    hashes the database's list lacked); removals_applied_here (artists the list deleted from the store),
    artists_purged_here, and artists_left_to_purge_here (removed, with a file that could not be deleted: the
    next run tries again).

    Stop, and nothing more is done, in the four cases the module's docstring names; when too much would be
    deleted it carries the counts. `allow_delete` is how many artists, how many works and how many neighbour
    rows this one run may delete instead, and how many key hashes it may add to the database's removal list
    when that is more than DELETE_FLOOR. RuntimeError when the database fails, ValueError when a removal list
    cannot be read: whichever comes, nothing after it is done.
    """
    there = db.sql(LISTING_SQL)
    if any(str(row["slug"]).startswith(TEST_SLUG) for row in there):
        raise Stop(
            f'the database holds test rows (slugs that begin "{TEST_SLUG}"): another builder\'s tests are using '
            "it, and a push would delete them. Nothing was done. Run it again when they have finished."
        )
    remote = {row["key_hash"] for row in db.sql(REMOVED_SQL)}
    if dry_run and not pathlib.Path(local or config.LOCAL_DB).is_file():
        raise Stop(NO_ARTISTS)  # a run would make the file and find nothing in it: a dry run makes no file
    kept = set(library.artist_keys(local))
    hashes, removed, applied = _removal(remote, there, kept, local)
    purge = sorted(removed & kept)
    # What the database's list lacks is added, so that another machine, or this file made again, hears of it.
    # The artists the list bars go with it, whatever happens further down: the database deletes them itself when
    # their hash arrives, and one it had to leave (a load held the row) is deleted here by id.
    unlisted = hashes - remote
    barred = {int(row["artist_id"]) for row in there if hashes & {row["da_key"], row["site_key"]}}
    removal = ([_removed_sql(unlisted)] if unlisted else []) + ([_delete_sql("artists", barred)] if barred else [])
    may_add = max(DELETE_FLOOR, allow_delete or 0)  # a handful always goes through, whatever a run is told
    if len(unlisted) > may_add:
        raise Stop(
            f"this run would add {len(unlisted)} key hashes to the database's removal list, where they stay for "
            f"good (it may add {may_add}), delete the {len(barred)} artists there that the list bars, and purge "
            f"{len(purge)} artists here. Removals come one at a time, so this many at once looks like a broken "
            "local store. Nothing was done, here or there. If they are real removals, run it again with "
            f"--allow-delete {len(unlisted)}."
        )
    stuck = 0
    if not dry_run:
        store.import_removed(hashes, local)  # here first, then there, and only then is a file deleted
        if removal:
            db.sql("; ".join(removal))
        for key in purge:
            try:
                library.purge(key, local)
            except OSError:  # a file somebody holds open: the rows that name it stay, and the next run finds them
                stuck += 1
    if not kept - removed:
        raise Stop(NO_ARTISTS)
    artists, works, neighbours, held = build(local, hashes)
    slugs = {row["slug"] for row in artists}
    sent = {(row["artist_slug"], row["page_url"]) for row in works}
    have_artists = {int(row["artist_id"]) for row in there} - barred
    gone_artists = {int(row["artist_id"]) for row in there if row["slug"] not in slugs} - barred
    have_works = [row for row in there if row["work_id"] is not None and row["slug"] in slugs]  # of artists who stay
    gone_works = {int(row["work_id"]) for row in have_works if (row["slug"], row["page_url"]) not in sent}
    empty = sum(row["neighbor_slug"] is None for row in neighbours)
    # Neighbour rows of the artists who stay that would go and not be replaced: replace_neighbors leaves each
    # artist with what they are sent. Nobody has more neighbours than there are other artists, so a row that
    # names an artist who leaves is not counted as lost again.
    have_near = {row["slug"]: int(row["neighbours"]) for row in there if row["slug"] in slugs}
    near = Counter(row["artist_slug"] for row in neighbours if row["neighbor_slug"] is not None)
    gone_near = sum(max(0, min(have, len(slugs) - 1) - near[slug]) for slug, have in have_near.items())

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
            "texts_left_out_for_contact": held["texts_left_out_for_contact"],
            "neighbours_sent": len(neighbours) - empty,
            "artists_sent_without_neighbours": empty,
            "neighbours_skipped_by_database": 0,
            "artists_deleted_from_database": len(gone_artists),
            "works_deleted_from_database": len(gone_works),
            "neighbours_deleted_from_database": gone_near,
            "removed_artists_deleted_from_database": len(barred),
            "removals_added_to_database": len(unlisted),
            "removals_applied_here": applied,
            "artists_purged_here": len(purge) - stuck,
            "artists_left_to_purge_here": stuck,
        }
    )

    def allowed(have: int) -> int:
        return max(DELETE_FLOOR, have * DELETE_PERCENT // 100) if allow_delete is None else allow_delete

    there_are = [len(have_artists), len(have_works), sum(have_near.values())]
    going = [len(gone_artists), len(gone_works), gone_near]
    may = [allowed(count) for count in there_are]
    if any(gone > limit for gone, limit in zip(going, may, strict=True)):
        raise Stop(
            f"this run would delete {going[0]} of the {there_are[0]} artists the database has (it may delete "
            f"{may[0]}), {going[1]} of the {there_are[1]} works of the artists who stay (it may delete {may[1]}) and "
            f"{going[2]} of their {there_are[2]} neighbour rows (it may delete {may[2]}). Nothing was deleted or "
            f"loaded. If that is what should happen, run it again with --allow-delete {max(going)}.",
            counts,
        )
    if not dry_run:
        # Before the load: db.load ends by rebuilding the search words, and a handle or a site that has moved
        # to another artist must be free when their row is written.
        stale = [_delete_sql(table, ids) for table, ids in (("works", gone_works), ("artists", gone_artists)) if ids]
        # An artist who stays and loses a work loses its words in the same call. The title and the tags are in the
        # artist's own row too until the load rewrites it, and a load that fails first would leave them there.
        losing = {int(row["artist_id"]) for row in have_works if int(row["work_id"]) in gone_works}
        if losing:
            stale.append(BLANK_SQL.format(", ".join(map(str, sorted(losing)))))
        if stale:
            db.sql("; ".join(stale))  # one call: all of it or none
        # Each row that names no neighbour is one the database counts as skipped: those are not news.
        counts["neighbours_skipped_by_database"] = max((db.load(artists, works, neighbours) or 0) - empty, 0)
    return counts


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    parser.add_argument("--dry-run", action="store_true", help="say what a run would do, and change nothing anywhere")
    parser.add_argument(
        "--allow-delete",
        type=int,
        metavar="N",
        help="let this one run delete up to N artists who no longer qualify, and up to N works and N neighbour rows "
        f"of artists who stay (without it: {DELETE_PERCENT} percent of what the database has, and never fewer than "
        f"{DELETE_FLOOR}); and add up to N key hashes to the database's removal list (without it: {DELETE_FLOOR})",
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
    if counts["artists_left_to_purge_here"]:
        sys.exit(
            f"not finished: {counts['artists_left_to_purge_here']} removed artists still have a cached file here "
            "that could not be deleted. Their removal stands, here and in the database, and the push was done. "
            "Run it again when nothing holds those files open."
        )


if __name__ == "__main__":
    main()
