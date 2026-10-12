"""Collect each discovered artist's public profile and about ten recent works through DeviantArt's official API.

    uv run python -m pipeline.collect --max-artists 200

For every artist the discovery stage stored, and nobody on the removal list: the public professional fields of
their profile, and up to MAX_WORKS of their most recent works that are images, explicitly not mature and not
tagged as AI-made. Each work is kept with its own tags and one preview-size image in the local cache, for the
analysis stage (owner decision 7 in PROGRESS.md). pipeline/library.py does the keeping. Output is counts only.

Endpoints (deviantart.readme.io, read 2026-10-12). Each is documented under the security scheme
"oauth2AuthorizationCodeOrClientCredentials" with scope "browse", so a client_credentials token is accepted:
    GET /api/v1/oauth2/user/profile/{username}
    GET /api/v1/oauth2/gallery/all           username, limit (1 to 24)
    GET /api/v1/oauth2/deviation/metadata    deviationids
An image is fetched from the preview address its gallery entry carries, without the token: it is a public file.
"""

import argparse
import pathlib
import re
import sys
import time
import traceback
from typing import Any

import httpx

from pipeline import config, discover, library, store

MAX_WORKS = 10  # works kept per artist
# ponytail: one gallery page, the documented maximum of 24. An artist with fewer than MAX_WORKS qualifying works
# among their 24 newest gets fewer. Follow next_offset if too many artists come up short.
GALLERY_LIMIT = 24
IMAGE_INTERVAL = 0.5  # seconds waited before every image download: two a second at most
# A work is AI-made when one of its own tags, lowercased and with everything but letters and digits taken out,
# starts with one of these. So "AI_Generated", "ai-art" and "MidjourneyV6" all count, and "noai" does not.
# ponytail: tags are the only signal. DeviantArt's own "created using AI tools" label is not in the API, and
# the bare tag "ai" is left out because it is also a name.
AI_TAGS = (
    "aiart",
    "aigenerat",
    "aiimage",
    "aiillustrat",
    "generativeai",
    "midjourney",
    "nijijourney",
    "stablediffusion",
    "dalle",
    "dreamup",
    "novelai",
)
NOT_IMAGE_KEYS = frozenset(("videos", "flash", "text_content", "excerpt"))  # what a film or a text carries

PROCESSED = "artists processed"
PROFILES = "profiles stored"
WORKS = "works stored"
DOWNLOADED = "images downloaded"
CACHED = "images already cached"
PURGED = "purged artists (removed since the last run)"
REMOVED = "skipped artists (removed)"
UNAVAILABLE = "skipped artists (profile or gallery unavailable)"
MOSTLY_AI = "skipped artists (most recent works tagged AI-made)"
MALFORMED = "skipped works (malformed entry)"
NOT_OWN = "skipped works (another author's)"
NOT_IMAGE = "skipped works (not a public image)"
MATURE = "skipped works (mature or unrated)"
NO_METADATA = "skipped works (no tag data)"
AI_TAGGED = "skipped works (tagged AI-made)"
IMAGE_FAILED = "skipped works (image download failed)"
IMAGE_WRONG_TYPE = "skipped works (image not an accepted type)"
IMAGE_TOO_LARGE = "skipped works (image over the size cap)"
_COUNTS = (
    PROCESSED,
    PROFILES,
    WORKS,
    DOWNLOADED,
    CACHED,
    PURGED,
    REMOVED,
    UNAVAILABLE,
    MOSTLY_AI,
    MALFORMED,
    NOT_OWN,
    NOT_IMAGE,
    MATURE,
    NO_METADATA,
    AI_TAGGED,
    IMAGE_FAILED,
    IMAGE_WRONG_TYPE,
    IMAGE_TOO_LARGE,
)


def _is_ai(tags: list[object]) -> bool:
    return any(isinstance(tag, str) and re.sub(r"[^a-z0-9]", "", tag.lower()).startswith(AI_TAGS) for tag in tags)


def _src(part: object) -> object:
    return part.get("src") if isinstance(part, dict) else None


def _why_not(entry: dict[str, Any], username: str) -> str | None:
    """Why a gallery entry is not kept. None for the artist's own public image that is explicitly not mature."""
    author = entry.get("author")
    name = author.get("username") if isinstance(author, dict) else None
    if not isinstance(name, str) or name.lower() != username.lower():
        return NOT_OWN
    if (
        entry.get("is_deleted") is not False
        or entry.get("tier_access") in ("locked", "locked-subscribed")
        or not NOT_IMAGE_KEYS.isdisjoint(entry)
        or not isinstance(_src(entry.get("content")), str)
        or not isinstance(_src(entry.get("preview")), str)
    ):
        return NOT_IMAGE
    if entry.get("is_mature") is not False:  # only an explicit "not mature" counts
        return MATURE
    return None


def _body(response: httpx.Response, what: str) -> dict[str, Any] | None:
    """The JSON object of a 200. None for a 4XX, which the docs give for a suspended, inactive or missing user."""
    status = response.status_code
    if 400 <= status < 500 and status not in (401, 429):  # 401 means our token, and 429 never gets here
        return None
    if status != 200:
        raise discover.Stop(f"{what} failed with HTTP {status}")
    body = response.json()
    return body if isinstance(body, dict) else {}


def _fetch_image(http: httpx.Client, url: str) -> tuple[str, bytes] | str:
    """One image as (content type, bytes), or the count it falls under when there is none to keep.

    Sent without the API token. The type is checked before the body is read, and the body is read only as far
    as the size cap. A redirect is not followed, so it counts as a failure.
    """
    time.sleep(IMAGE_INTERVAL)  # not injectable, on purpose: the tests replace time.sleep
    try:
        with http.stream("GET", url) as response:
            if response.status_code == 429:
                # ponytail: no backoff for the image host. Being told to slow down ends the run; the next one resumes.
                raise discover.Stop("rate limited by the image host")
            if response.status_code != 200:
                return IMAGE_FAILED
            content_type = response.headers.get("content-type", "").partition(";")[0].strip().lower()
            if content_type not in library.IMAGE_TYPES:
                return IMAGE_WRONG_TYPE
            data = bytearray()
            for chunk in response.iter_bytes():
                data += chunk
                if len(data) > library.MAX_IMAGE_BYTES:
                    return IMAGE_TOO_LARGE
    except httpx.HTTPError:
        return IMAGE_FAILED
    return content_type, bytes(data)


def _collect_artist(
    api: discover._Api, http: httpx.Client, username: str, counts: dict[str, int], db: pathlib.Path | str | None
) -> None:
    key = store.artist_key(username)
    profile = _body(api.get(f"/user/profile/{username}"), "profile lookup")
    if profile is None:
        counts[UNAVAILABLE] += 1
        return
    # Hard rule 7, now that the profile is read: an artist whose website is a removed site is removed with it.
    if store.record_listing(username, store.hosts_in(profile.get("website")), db):
        library.purge(key, db)
        counts[REMOVED] += 1
        return
    page = _body(api.get("/gallery/all", username=username, limit=GALLERY_LIMIT), "gallery lookup")
    if page is None:
        counts[UNAVAILABLE] += 1
        return

    candidates: dict[str, dict[str, Any]] = {}  # deviation id -> gallery entry
    for entry in page.get("results") or []:
        try:
            work_id = library.deviation_id(entry.get("deviationid") if isinstance(entry, dict) else None)
        except ValueError:
            counts[MALFORMED] += 1
            continue
        reason = _why_not(entry, username)
        if reason:
            counts[reason] += 1
        else:
            candidates[work_id] = entry

    metadata: dict[str, dict[str, Any]] = {}  # deviation id -> its metadata, which is where the tags are
    if candidates:
        response = api.get("/deviation/metadata", **{f"deviationids[{n}]": id_ for n, id_ in enumerate(candidates)})
        if response.status_code != 200:  # the only documented error is asking for too many, which is ours to fix
            raise discover.Stop(f"metadata lookup failed with HTTP {response.status_code}")
        body = response.json()
        for item in (body.get("metadata") if isinstance(body, dict) else None) or []:
            if isinstance(item, dict) and isinstance(item.get("deviationid"), str):
                metadata[item["deviationid"].upper()] = item

    clean: list[tuple[str, dict[str, Any], list[object]]] = []  # works that are fit to keep
    ai_made = 0
    for work_id, entry in candidates.items():
        item = metadata.get(work_id)
        if item is None or not isinstance(item.get("tags"), list):
            counts[NO_METADATA] += 1
            continue
        if item.get("is_mature") is not False:
            counts[MATURE] += 1
            continue
        tags = [tag.get("tag_name") for tag in item["tags"] if isinstance(tag, dict)]
        if _is_ai(tags):
            counts[AI_TAGGED] += 1
            ai_made += 1
        else:
            clean.append((work_id, entry, tags))
    if ai_made > len(clean):  # most of the works that could be judged: this is not an artist to list
        library.purge(key, db)
        counts[MOSTLY_AI] += 1
        return

    library.upsert_profile(
        key,
        tagline=profile.get("tagline"),
        specialty=profile.get("artist_specialty"),
        level=profile.get("artist_level"),
        bio=profile.get("bio"),
        website=profile.get("website"),
        db=db,
    )
    counts[PROFILES] += 1
    clean.sort(key=lambda work: library.published_at(work[1].get("published_time")) or "", reverse=True)
    kept: list[str] = []
    for work_id, entry, tags in clean:  # newest first
        if len(kept) == MAX_WORKS:
            break
        thumbs = entry.get("thumbs")
        try:
            preview = library.checked_url(_src(entry["preview"]), library.IMAGE_DOMAINS)  # never the full original
            library.upsert_work(  # the row first, so no image is ever in the cache without one
                key,
                work_id,
                page_url=entry.get("url"),
                title=entry.get("title"),
                tags=tags,
                thumb_url=_src(thumbs[-1]) if isinstance(thumbs, list) and thumbs else preview,
                published=entry.get("published_time"),
                db=db,
            )
        except ValueError:
            counts[MALFORMED] += 1
            continue
        if library.cached_image(work_id, db):
            counts[CACHED] += 1
        else:
            fetched = _fetch_image(http, preview)
            if isinstance(fetched, str):
                counts[fetched] += 1
                continue
            try:
                library.save_image(work_id, *fetched, db=db)
            except ValueError:  # the bytes do not start the way the declared type does
                counts[IMAGE_WRONG_TYPE] += 1
                continue
            counts[DOWNLOADED] += 1
        kept.append(work_id)
    # Whatever else is on file for this artist goes: a work with no image, one that left the gallery, one that
    # no longer qualifies.
    library.drop_other_works(key, kept, db)
    counts[WORKS] += len(kept)


def _run(
    api: discover._Api, http: httpx.Client, counts: dict[str, int], max_artists: int, db: pathlib.Path | str | None
) -> None:
    for key in library.artist_keys(db):  # hard rule 7: whoever was removed since the last run goes, before anything
        if store.is_removed(key, db):
            library.purge(key, db)
            counts[PURGED] += 1
    collected = set(library.artist_keys(db))
    # Artists with nothing collected come first, so a run that was cut short picks up where it stopped.
    # ponytail: an artist skipped as unavailable or AI-made has no profile row, so every run asks about them again.
    artists = sorted(store.all_artists(db), key=lambda artist: artist[0] in collected)
    for _, username, _ in artists[: max(max_artists, 0)]:
        if store.is_removed(store.artist_key(username), db):  # asked again right before each artist is fetched
            counts[REMOVED] += 1
            continue
        counts[PROCESSED] += 1
        _collect_artist(api, http, username, counts, db)


def collect(
    client_id: str,
    client_secret: str,
    *,
    max_artists: int,
    db: pathlib.Path | str | None = None,
    transport: httpx.BaseTransport | None = None,
) -> int:
    """Run one collection pass and print its counts. Returns the exit code."""
    counts = dict.fromkeys(_COUNTS, 0)
    status = 0
    try:
        # The same client settings as discovery: the project user agent on every request, images included, and no
        # redirect ever followed. discover._Api adds the token, the 1 second between API requests, the backoff
        # on 429 and the one token refresh on 401.
        with (
            discover._http_logs_off(),
            httpx.Client(
                headers={"User-Agent": config.USER_AGENT}, transport=transport, timeout=30, follow_redirects=False
            ) as http,
        ):
            _run(discover._Api(http, client_id, client_secret), http, counts, max_artists, db)
    except discover.Stop as stop:
        print(f"stopped: {stop}", file=sys.stderr)
        status = 1
    except Exception as exc:
        # Deliberately broad, and no traceback: an exception message can quote response data or a request URL.
        lines = [frame.lineno for frame in traceback.extract_tb(exc.__traceback__) if frame.filename == __file__]
        print(f"stopped: {type(exc).__name__} at collect.py line {lines[-1] if lines else '?'}", file=sys.stderr)
        status = 1
    for name, count in counts.items():
        print(f"{name}: {count}")
    return status


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Collect artists' public profiles and recent works from DeviantArt.")
    parser.add_argument("--max-artists", type=int, default=200, help="stop after this many artists")
    args = parser.parse_args(argv)
    try:
        env = config.load_env()
    except FileNotFoundError:
        env = {}
    missing = [name for name in discover.ENV_NAMES if not env.get(name)]
    if missing:
        print(f"Missing in .env: {', '.join(missing)} (see .env.example)", file=sys.stderr)
        return 1
    client_id, client_secret = (env[name] for name in discover.ENV_NAMES)
    return collect(client_id, client_secret, max_artists=args.max_artists)


if __name__ == "__main__":
    sys.exit(main())
