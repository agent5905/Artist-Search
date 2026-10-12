"""Collect each discovered artist's public profile and about ten recent works through DeviantArt's official API.

    uv run python -m pipeline.collect --max-artists 200

For every artist the discovery stage stored, and nobody on the removal list: four public professional fields of
their profile, and up to MAX_WORKS of their most recent works that are images, explicitly not mature and not
tagged as AI-made. Each work is kept with its own tags and one preview-size image in the local cache, for the
analysis stage (owner decision 7 in PROGRESS.md). pipeline/library.py does the keeping. Output is counts only.

A run starts and ends by deleting what must not be there: everything kept about an artist who has been removed
since, and every cached file no row accounts for. Running it again over every artist also brings what an
earlier version stored up to the rules of this one: each artist's profile and works are replaced whole.

Endpoints (deviantart.readme.io, read 2026-10-12). Each is documented under the security scheme
"oauth2AuthorizationCodeOrClientCredentials" with scope "browse", so a client_credentials token is accepted:
    GET /api/v1/oauth2/user/profile/{username}   no parameters: "expand" would add age, sex and location
    GET /api/v1/oauth2/gallery/all               username, limit (1 to 24)
    GET /api/v1/oauth2/deviation/metadata        deviationids
An image is fetched from the preview address its gallery entry carries, without the token: it is a public file.
"""

import argparse
import pathlib
import re
import sqlite3
import sys
import time
import traceback
import unicodedata
from typing import Any

import httpx

from pipeline import config, discover, library, store

MAX_WORKS = 10  # works kept per artist
# ponytail: one gallery page, the documented maximum of 24. An artist with fewer than MAX_WORKS qualifying works
# among their 24 newest gets fewer. Follow next_offset if too many artists come up short.
GALLERY_LIMIT = 24
IMAGE_INTERVAL = 0.5  # seconds waited before every image download: two a second at most
MAX_FAILURES = 3  # artists failing unexpectedly one after another before the run gives up
NOT_IMAGE_KEYS = frozenset(("videos", "flash", "text_content", "excerpt"))  # what a film or a text carries

# Which tags say a work is AI-made. A tag is read in lower case with everything but letters and digits taken
# out, so "AI_Generated" is "aigenerated". It marks its work when it
#   - is "ai", or has "ai" as a word of its own ("anime_ai"),
#   - begins with "ai" and is not one of the ordinary words and names in AI_LOOKALIKES ("aiadoptable", "aitools"),
#   - ends in one of AI_ENDS ("createdwithai"), begins with one of AI_STARTS, is one of AI_EXACT, or has one of
#     AI_NAMES anywhere in it ("madewithmidjourney"),
# unless it says the opposite ("noai", "not_ai_art", "aifree"). When in doubt a tag counts.
# ponytail: tags are the only signal, because DeviantArt's own "created using AI tools" label is not in the
# API. An AI phrase that begins like an ordinary word is missed ("airender" reads as air...), and so is a tag
# in another script. Add to the lists as real tags turn up; a model reading the image is the real upgrade.
AI_NAMES = (
    *("midjourney", "nijijourney", "diffusion", "novelai", "comfyui", "leonardoai", "nightcafe", "civitai"),
    *("tensorart", "txt2im", "img2im", "text2im", "texttoim", "sdxl", "artificialintelligence", "generativeai"),
    *("inteligenciaartificial", "intelligenceartificielle", "dreambooth", "controlnet", "craiyon", "automatic1111"),
    *("invokeai", "bingimagecreator", "adobefirefly", "playgroundai", "chatgpt", "gpt4", "gpt5", "artbreeder"),
    *("deepdream", "runwayml"),
)
AI_STARTS = ("dalle", "dreamup", "genai", "pixai", "seaartai", "nijiv", "niji4", "niji5", "niji6", "niji7")
# Exact, because "nijisanji", "seaartist", "openair" and "sparklingair" are something else.
AI_EXACT = frozenset(
    ("niji", "seaart", "openai", "bingai", "deepai", "starryai", "klingai", "lumaai", "grokai", "fluxai")
)
AI_ENDS = ("withai", "byai", "usingai")
AI_DENIALS = ("not", "no", "non", "anti", "never", "without", "zero", "stop", "ban", "against")
_AI_PROTEST = re.compile(r"isnot|isnt|notart|theft|steal|stole|sucks|free")  # inside a tag that begins with "ai"
# Ordinary English and Japanese words and names that begin with "ai". A tag that is one of them is not about AI;
# nor is one that begins with one of the longer ones, where no AI phrase could begin the same way.
AI_LOOKALIKES = frozenset(
    "aid aids aide aides aided aiding ail ails ailed ailing ailment ailments aim aims aimed aiming aimless aimbot "
    "aint ais aiko aika aiki aimi aimee aimer aina aino ainu ainz aion aipom aiba aichi aizu aiai aiur aila aili "
    "aida aite aisu aijin aimai aibou aidoru aiueo".split()
)
AI_LOOKALIKE_STARTS = (
    *("air", "aisle", "aioli", "aiolos", "aiolia", "aileron", "aiguill", "aigrette", "aikido", "aikatsu", "aikuchi"),
    *("aizen", "aizawa", "aizome", "aikawa", "aihara", "aisaka", "aigis", "aisha", "aiden", "aidan", "aileen"),
    *("ailee", "aisling", "aincrad", "aishiteru", "aisatsu", "aihoshino", "aihaibara", "aihayasaka", "aienma"),
    *("aiohto", "ainominako"),
)

PROCESSED = "artists processed"
PROFILES = "profiles stored"
WORKS = "works stored"
DOWNLOADED = "images downloaded"
CACHED = "images already cached"
PURGED = "purged artists (removed)"
GONE = "purged artists (account suspended, inactive or gone)"
ORPHANS = "deleted cached files (no row for them)"
UNDELETED = "deletions that failed"
NOT_STORED = "skipped artists (the database refused the profile)"
ROW_REFUSED = "skipped works (the database refused the row)"
FAILED = "skipped artists (unexpected error)"
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
    GONE,
    ORPHANS,
    UNDELETED,
    NOT_STORED,
    ROW_REFUSED,
    FAILED,
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


def _ai_word(word: str) -> bool:
    """True for a squashed tag that names an AI tool or says AI made the work. Denials are _says_ai's business."""
    if word.startswith("ai"):
        return not word.startswith(AI_LOOKALIKE_STARTS)
    return word in AI_EXACT or word.startswith(AI_STARTS) or word.endswith(AI_ENDS) or any(n in word for n in AI_NAMES)


def _says_ai(tag: str) -> bool:
    """True when one of an artist's own tags marks the work as AI-made. See the comment above AI_NAMES."""
    words = re.findall(r"[a-z0-9]+", unicodedata.normalize("NFKC", tag).lower())
    word = "".join(words)
    # The opposite claim is not a claim: "noai", "no_ai_art", "notmadewithai", "aifree", "aiisnotart".
    if any(word.startswith(no) and _ai_word(word[len(no) :]) for no in AI_DENIALS):
        return False
    if word.startswith("ai") and (_AI_PROTEST.search(word) or words[0] in AI_LOOKALIKES):
        return False
    # "ai" as a word of its own counts wherever it stands, unless the word before it denies it ("art_not_ai").
    alone = any(w == "ai" and (at == 0 or words[at - 1] not in AI_DENIALS) for at, w in enumerate(words))
    return alone or _ai_word(word)


def _is_ai(tags: list[str]) -> bool:
    return any(_says_ai(tag) for tag in tags)


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


def _gone(response: httpx.Response) -> bool:
    """True when a 4XX carries one of the profile endpoint's documented codes: 0 suspended, 1 inactive, 2 not found.

    That is the API saying the account is no longer public, which an outage or a refusal never says.
    """
    try:
        body = response.json()
    except ValueError:
        return False
    return isinstance(body, dict) and type(body.get("error_code")) is int and body["error_code"] in (0, 1, 2)


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


def _purge(key: str, counts: dict[str, int], db: pathlib.Path | str | None) -> bool:
    """library.purge() that does not end the run: a file that cannot be deleted is counted, and the run goes on to
    the next artist, whose data must not stay on disk because of it. False when something could not be deleted."""
    try:
        library.purge(key, db)
    except OSError:
        counts[UNDELETED] += 1
        return False
    return True


def _sweep(counts: dict[str, int], db: pathlib.Path | str | None) -> None:
    """Hard rule 7, at both ends of every run: everything kept about an artist who is now on the removal list
    goes, and so does every cached file no row accounts for."""
    for key in library.artist_keys(db):
        if store.is_removed(key, db) and _purge(key, counts, db):
            counts[PURGED] += 1
    deleted, failed = library.drop_orphans(db)
    counts[ORPHANS] += deleted
    counts[UNDELETED] += failed


def _collect_artist(
    api: discover._Api, http: httpx.Client, username: str, counts: dict[str, int], db: pathlib.Path | str | None
) -> None:
    key = store.artist_key(username)
    response = api.get(f"/user/profile/{username}")
    profile = _body(response, "profile lookup")
    if profile is None:
        gone = _gone(response)
        if gone:
            _purge(key, counts, db)
        counts[GONE if gone else UNAVAILABLE] += 1
        return
    # Hard rule 7, now that the profile is read: an artist whose website is a removed site is removed with it.
    if store.record_listing(username, store.hosts_in(profile.get("website")), db):
        _purge(key, counts, db)
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

    clean: list[tuple[str, dict[str, Any], list[str]]] = []  # works that are fit to keep
    ai_made = 0
    for work_id, entry in candidates.items():
        item = metadata.get(work_id, {})
        listed = item.get("tags")
        names = (
            [tag.get("tag_name") if isinstance(tag, dict) else None for tag in listed]
            if isinstance(listed, list)
            else []
        )
        tags = [name for name in names if isinstance(name, str)]
        # No tag list, or an item in it that is not a named tag: what the work is cannot be told, so it is not kept.
        if not isinstance(listed, list) or len(tags) != len(names):
            counts[NO_METADATA] += 1
        elif item.get("is_mature") is not False:
            counts[MATURE] += 1
        elif _is_ai(tags):
            counts[AI_TAGGED] += 1
            ai_made += 1
        else:
            clean.append((work_id, entry, tags))
    if ai_made > len(clean):  # most of the works that could be judged: this is not an artist to list
        _purge(key, counts, db)
        counts[MOSTLY_AI] += 1
        return

    clean.sort(key=lambda work: library.published_at(work[1].get("published_time")) or "", reverse=True)
    kept: list[dict[str, Any]] = []
    for work_id, entry, tags in clean:  # newest first
        if len(kept) == MAX_WORKS:
            break
        thumbs = entry.get("thumbs")
        work = {
            "work_id": work_id,
            "page_url": entry.get("url"),
            "title": entry.get("title"),
            "tags": tags,
            "thumb_url": _src(thumbs[-1] if isinstance(thumbs, list) and thumbs else entry["preview"]),
            "published": entry.get("published_time"),
        }
        try:
            preview = library.image_address(_src(entry["preview"]))  # never the full original
            library.checked_work(**work)
        except ValueError:
            counts[MALFORMED] += 1
            continue
        if library.cached_image(work_id, db):
            counts[CACHED] += 1
        else:
            if store.is_removed(key, db):  # hard rule 7: asked again before every download
                break
            fetched = _fetch_image(http, preview)
            if isinstance(fetched, str):
                counts[fetched] += 1
                continue
            try:
                library.save_image(work_id, *fetched)
            except ValueError:  # the bytes do not begin the way the declared type does
                counts[IMAGE_WRONG_TYPE] += 1
                continue
            counts[DOWNLOADED] += 1
        kept.append(work)
    if store.is_removed(key, db):  # and once more before anything is written. What was downloaded is swept up.
        _purge(key, counts, db)
        counts[REMOVED] += 1
        return
    user = profile.get("user")
    try:
        # One transaction for the whole artist: the profile, these works, and nothing else of theirs left behind.
        refused, stuck = library.store_artist(
            key,
            user_id=user.get("userid") if isinstance(user, dict) else None,
            tagline=profile.get("tagline"),
            specialty=profile.get("artist_specialty"),
            level=profile.get("artist_level"),
            website=profile.get("website"),
            works=kept,
            db=db,
        )
    except sqlite3.IntegrityError:
        # A constraint refused the profile row. Nothing of this artist was written and what was downloaded is
        # swept up at the end. One artist the database will not take must not end the run for the rest.
        counts[NOT_STORED] += 1
        return
    counts[ROW_REFUSED] += refused
    counts[UNDELETED] += stuck
    counts[PROFILES] += 1
    counts[WORKS] += len(kept) - refused


def _where(exc: Exception) -> str:
    """An exception as its type and the line of this file it came through. Never its message and never a
    traceback: either can quote response data or a request URL."""
    lines = [frame.lineno for frame in traceback.extract_tb(exc.__traceback__) if frame.filename == __file__]
    return f"{type(exc).__name__} at collect.py line {lines[-1] if lines else '?'}"


def _run(
    api: discover._Api, http: httpx.Client, counts: dict[str, int], max_artists: int, db: pathlib.Path | str | None
) -> None:
    collected = set(library.artist_keys(db))
    # Artists with nothing collected come first, so a run that was cut short picks up where it stopped.
    # ponytail: an artist skipped as unavailable or AI-made has no profile row, so every run asks about them again.
    artists = sorted(store.all_artists(db), key=lambda artist: artist[0] in collected)
    failed_in_a_row = 0
    for _, username, _ in artists[: max(max_artists, 0)]:
        try:
            if store.is_removed(store.artist_key(username), db):  # asked again right before each artist is fetched
                counts[REMOVED] += 1
                continue
            counts[PROCESSED] += 1
            _collect_artist(api, http, username, counts, db)
            failed_in_a_row = 0
        except (discover.Stop, httpx.HTTPError):
            raise  # the API or the network has failed, not this artist: the next one would fail the same way
        except Exception as exc:
            # No single artist ends the run for the rest: whatever went wrong with this one is counted, and the
            # run goes on. What they had downloaded is swept up at the end.
            counts[FAILED] += 1
            failed_in_a_row += 1
            print(f"skipped an artist: {_where(exc)}", file=sys.stderr)
            if failed_in_a_row == MAX_FAILURES:  # by now it is not the artists
                raise discover.Stop(f"{MAX_FAILURES} artists in a row failed") from None


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
            _sweep(counts, db)
            try:
                _run(discover._Api(http, client_id, client_secret), http, counts, max_artists, db)
            finally:
                _sweep(counts, db)  # however the run ended: a removal that landed during it is honoured before exit
    except discover.Stop as stop:
        print(f"stopped: {stop}", file=sys.stderr)
        status = 1
    except Exception as exc:  # deliberately broad: nothing gets out of here as a traceback
        print(f"stopped: {_where(exc)}", file=sys.stderr)
        status = 1
    if counts[UNDELETED]:
        print("incomplete: something that had to be deleted could not be. Run again.", file=sys.stderr)
        status = 1
    if counts[NOT_STORED] or counts[ROW_REFUSED]:
        print("incomplete: the database refused rows it was given. See the counts.", file=sys.stderr)
        status = 1
    if counts[FAILED]:
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
