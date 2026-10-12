"""What the collect stage keeps: each artist's public profile fields, their recent works, one cached image a work.

Three tables in the local SQLite file, beside the store's (pipeline/store.py), and image files in image_dir().
Every value is checked here, on the way in, whoever calls. Nothing is cut out of a value to make it fit: a value
that might hold what must not be kept is dropped whole.
  - hard rule 1: the tables are STRICT and hold text only: short texts, addresses and a file name. A text or an
    address with "data:" in it in any spelling, or a text with a long encoded-looking run, is not kept.
  - hard rule 2: image bytes are written by save_image() and nowhere else, as files under config.CACHE_DIR.
  - hard rule 6: there is no free-text bio. A text with an "@", anything shaped like a domain, nine digits, or
    seven beside a word for calling is not kept. A work whose title holds one of those is not kept at all,
    because its page address is made from its title. These are the plain cases and no more: the analysis stage
    reads every title and tagline with a language model before anything is published. A website is kept only
    when it is the artist's own site or the front page of a profile on one of PROFILE_DOMAINS. The API's user
    id is kept only as a hash.
  - hard rule 7: purge() deletes everything kept about one artist in every table of the file, whichever stage
    made it, and the files; drop_orphans() deletes every cached file that no row accounts for.
Every function that reads or writes the tables takes an optional `db` path; the default is config.LOCAL_DB. A
work whose image_file is NULL has no cached image: the analysis stage must leave it alone.
"""

import base64
import contextlib
import hashlib
import html
import json
import pathlib
import re
import sqlite3
import unicodedata
from collections.abc import Iterable, Iterator, Mapping
from datetime import UTC, datetime
from typing import Any
from urllib.parse import unquote

import httpx

from pipeline import config, crawl, store

_TIMESTAMP = "[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]T*"  # for GLOB: what a timestamp begins with
# The CHECKs are there so that a row of the wrong shape is refused, never stored. The first version had none and
# wrote its rows by position. When this table changed under a collector of that version that was still
# running, it stored seven artists' websites as fetched_at and stopped on the eighth, who had no website.
_PROFILES = f"""(
    artist_key TEXT PRIMARY KEY,     -- artists.key: 'da:<lowercased handle>'
    tagline TEXT,
    specialty TEXT,
    level TEXT,
    website TEXT CHECK (website LIKE 'http%://%'),                    -- scheme://host/path, nothing else
    fetched_at TEXT NOT NULL CHECK (fetched_at GLOB '{_TIMESTAMP}'),  -- ISO-8601 UTC
    -- sha256 hex of 'uid:<the API's user id>': the same account under another handle
    user_hash TEXT CHECK (length(user_hash) = 64 AND user_hash NOT GLOB '*[^0-9a-f]*')
) STRICT"""
_SCHEMA = f"""
CREATE TABLE IF NOT EXISTS profiles {_PROFILES};
CREATE TABLE IF NOT EXISTS works (
    deviation_id TEXT PRIMARY KEY,   -- an uppercase UUID
    artist_key TEXT NOT NULL,
    page_url TEXT NOT NULL,          -- the work's own page on DeviantArt: the link back
    title TEXT,
    tags TEXT NOT NULL,              -- JSON list of the artist's own tags
    thumb_url TEXT NOT NULL,         -- DeviantArt's thumbnail, shown by address and never copied (hard rule 1)
    published_at TEXT,               -- ISO-8601 UTC
    image_file TEXT,                 -- a file name in image_dir(); NULL while there is no cached image
    fetched_at TEXT NOT NULL         -- ISO-8601 UTC
) STRICT;
CREATE INDEX IF NOT EXISTS works_by_artist ON works (artist_key);
-- Artists the collector passed over the last time their turn came, so that the next run takes them last and
-- not first. A new table, made when there is none: nothing here changes a table that exists (see _open).
CREATE TABLE IF NOT EXISTS skipped (
    artist_key TEXT PRIMARY KEY,
    skipped_at TEXT NOT NULL CHECK (skipped_at GLOB '{_TIMESTAMP}'),  -- ISO-8601 UTC
    unavailable INTEGER NOT NULL     -- runs that showed their account or all their works gone, since last stored
) STRICT;
"""
# A profiles table from an earlier version of this module (see _upgrade). It is rebuilt with the artists it
# lists and when each was fetched, and nothing else: what it held was kept under looser rules, some of it in the
# wrong column, and the next run fills in what still qualifies.
_UPGRADE = f"""
BEGIN;
CREATE TABLE profiles_upgraded {_PROFILES};
INSERT INTO profiles_upgraded (artist_key, fetched_at)
    SELECT artist_key, CASE WHEN fetched_at GLOB '{_TIMESTAMP}' THEN fetched_at ELSE '1970-01-01T00:00:00+00:00' END
    FROM profiles;
DROP TABLE profiles;
ALTER TABLE profiles_upgraded RENAME TO profiles;
UPDATE works SET title = NULL WHERE title LIKE '%@%' OR title LIKE '%data:%';
COMMIT;
"""

MAX_IMAGE_BYTES = 10 * 1024 * 1024  # a preview is a few hundred KB; anything near this is not one
# The image types kept: content type -> (file extension, how such a file begins).
# ponytail: the beginning is all that is checked. Decoding the whole file needs an image library, which the
# analysis stage has: it should delete any cached file it cannot open.
IMAGE_TYPES = {
    "image/jpeg": (".jpg", re.compile(rb"\xff\xd8\xff")),
    "image/png": (".png", re.compile(rb"\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR")),
    "image/gif": (".gif", re.compile(rb"GIF8[79]a")),
    "image/webp": (".webp", re.compile(rb"RIFF.{4}WEBP", re.DOTALL)),  # RIFF alone is also sound and film
}
PAGE_DOMAINS = ("deviantart.com",)  # where a work's page lives
IMAGE_DOMAINS = ("wixmp.com", "deviantart.net", "deviantart.com")  # where DeviantArt serves its images from
# The only places an artist's website may be, besides a site of their own: portfolio and social profiles.
# A list of what is kept, because a list of what is refused (messaging services) can never be finished.
PROFILE_DOMAINS = (
    *("artstation.com", "behance.net", "instagram.com", "x.com", "twitter.com", "tumblr.com", "patreon.com"),
    *("ko-fi.com", "linktr.ee", "carrd.co", "youtube.com", "twitch.tv", "bsky.app"),
)

_UUID = re.compile(r"[0-9A-F]{8}(?:-[0-9A-F]{4}){3}-[0-9A-F]{12}")
_TAG_NAME = re.compile(r"[\w-]{1,60}")
_PAGE_PATH = re.compile(r"/[A-Za-z0-9_-]{1,40}/art/[A-Za-z0-9_.%-]{1,200}")  # /<handle>/art/<title>-<number>
# DeviantArt's signature on an image address: token=<header>.<claims>.<signature>, the header a few dozen characters.
_IMAGE_TOKEN = re.compile(r"token=(?P<header>[A-Za-z0-9_-]{1,100})\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+")
MAX_TOKEN = 1000  # characters; the longest real one seen is 639
MAX_IMAGE_PATH = 600  # characters; the longest real one seen is 252
_MARKUP = re.compile(r"<[^<>]*>")
# Characters that show nothing and are not in Unicode's "other" categories, which _settle removes wholesale:
# the grapheme joiner, the Hangul and Khmer fillers, the Mongolian and the general variation selectors, blank braille.
_INVISIBLE = re.compile(
    "[\u034f\u115f\u1160\u17b4\u17b5\u180b-\u180f\u2800\u3164\ufe00-\ufe0f\uffa0\U000e0100-\U000e01ef]"
)
_ENCODED = re.compile(r"[A-Za-z0-9+/=_-]{100,}")  # no word is this long; a base64 file is
# What makes seven or eight digits a number to call and not a date or a serial number.
_CALLING = re.compile(r"call|tel|phone|whatsapp|\+", re.IGNORECASE)


class ContactInTitle(ValueError):
    """A work's title holds contact details. The work is not kept: its page address is made from its title."""


def image_dir() -> pathlib.Path:
    """Where cached images live. Read from config on every call, never passed in: there is no other place."""
    return config.CACHE_DIR / "images"


def _settle(text: str, percent: bool = False) -> str | None:
    """A text with every layer of disguise undone, or None when six passes do not settle it.

    Each pass: compatibility forms made plain (a fullwidth "@" is an "@"), HTML entities decoded, percent-encoding
    decoded when asked, every invisible character removed, markup removed. It repeats until a pass changes
    nothing, so "&amp;#64;" and "%2540" come out as "@" too.
    """
    for _ in range(6):
        before = text
        text = unicodedata.normalize("NFKC", html.unescape(text))
        text = unquote(text) if percent else text
        text = "".join(char for char in text if char.isspace() or unicodedata.category(char)[0] != "C")
        text = _MARKUP.sub(" ", _INVISIBLE.sub("", text))
        if text == before:
            return " ".join(text.split())
    return None


def _digits(text: str) -> int:
    return sum(map(str.isdigit, text))


def _embedded(text: str) -> bool:
    """True when a settled text could hold a file: "data:" in any spacing or case, or a long encoded-looking run."""
    return "data:" in "".join(text.split()).lower() or bool(_ENCODED.search(text))


def _contact(text: str) -> bool:
    """True when a settled text holds a plain way to reach a person: an "@", anything shaped like a domain (an
    address, a link, a messaging link), nine digits, or seven beside a word for calling.

    ponytail: the plain cases and no more, by the lead's decision. Addresses spelled out in words, service names
    and other scripts are left to the analysis stage, which reads every title and tagline with a language model.
    """
    digits = _digits(text)
    return "@" in text or digits >= 9 or (digits >= 7 and bool(_CALLING.search(text))) or bool(store.hosts_in(text))


def _text(value: object, limit: int) -> str | None:
    """A short public text cut to `limit`, or None for anything else: not a text, or one that would not settle or
    could hold contact details or a file, as it is written or once its percent-encoding is undone."""
    if not isinstance(value, str) or len(value) > 10_000:
        return None
    shown, probe = _settle(value), _settle(value, percent=True)
    if not shown or probe is None or any(_contact(text) or _embedded(text) for text in (shown, probe)):
        return None
    return shown[:limit].strip() or None


def _title(value: object) -> str | None:
    """A work's title cut to 200 characters. None for no title, or one that could hold a file.

    ContactInTitle when it holds what _contact() looks for, as it is written or once its percent-encoding is
    undone; and when it will not settle, which is somebody hiding something.
    """
    if not isinstance(value, str) or len(value) > 10_000:
        return None
    shown, probe = _settle(value), _settle(value, percent=True)
    if shown is None or probe is None or _contact(shown) or _contact(probe):
        raise ContactInTitle("contact details in a title")  # never echoes the title
    return None if _embedded(shown) or _embedded(probe) else shown[:200].strip() or None


def _choice(value: object) -> str | None:
    """A specialty or a level. The API spells "not given" as the word None."""
    text = _text(value, 100)
    return None if text == "None" else text


def _tags(value: object) -> list[str]:
    """The artist's own tags: plain tag names of 60 characters at most, and 60 of them at most."""
    tags = value if isinstance(value, list) else []
    # A tag is a name, so one that is long as it is written is not read: reading 100 of them took seconds.
    names = (_text(tag, 61) for tag in tags if isinstance(tag, str) and len(tag) <= 300)
    return [name for name in names if name and _TAG_NAME.fullmatch(name)][:60]


def _on(host: str, domains: tuple[str, ...]) -> bool:
    return any(host == domain or host.endswith(f".{domain}") for domain in domains)


def _website(value: object) -> str | None:
    """A profile's website field as scheme://host/path, or None.

    Kept only when it is the front page of a site of the artist's own (crawl.is_own_site is the rule), or the
    front page of a profile on one of PROFILE_DOMAINS: host/<name>, or <name>.host/, and nothing after it (what
    comes after is how one writes to them: /ask, /contact, /message). Never with a query or a fragment, and
    never with an "@", seven digits, "data:" or an encoded-looking run in it, however it is encoded.
    """
    if not isinstance(value, str) or len(value) > 300:
        return None
    shown, probe = _settle(value), _settle(value, percent=True)
    if not shown or " " in shown or probe is None or "@" in probe or _digits(probe) >= 7 or _embedded(probe):
        return None
    try:
        url = httpx.URL(shown if "://" in shown else f"https://{shown}")
        host = store.canonical_host(url.raw_host.decode("ascii"))
    except (httpx.InvalidURL, UnicodeError):
        return None
    if url.scheme not in ("http", "https") or host is None:
        return None
    path = url.raw_path.decode("ascii").partition("?")[0]
    address = f"{url.scheme}://{host}{path}"
    if not _on(host, PROFILE_DOMAINS):
        return address if crawl.is_own_site(address) else None
    parts = [part for part in path.split("/") if part]
    users_own_subdomain = host.removeprefix("www.") not in PROFILE_DOMAINS  # zzfake.tumblr.com, not tumblr.com
    return address if len(parts) == (0 if users_own_subdomain else 1) else None


def _address(value: object, domains: tuple[str, ...]) -> httpx.URL:
    """An https address on one of these domains, or ValueError. Never http, a port, a fragment, another host, or
    an address with "data:" anywhere in it, however that is encoded."""
    if not isinstance(value, str) or len(value) > 2000 or not (value.isascii() and value.isprintable()):
        raise ValueError("not an address")  # never echoes the input
    probe = _settle(value, percent=True)
    if probe is None or "data:" in "".join(probe.split()).lower() or set(value) & set(' \\"<>'):
        raise ValueError("not an address")
    try:
        url = httpx.URL(value)
    except httpx.InvalidURL:
        raise ValueError("not an address") from None
    host = url.raw_host.decode("ascii")
    if url.scheme != "https" or url.userinfo or url.port is not None or url.fragment or not _on(host, domains):
        raise ValueError("not an https address on DeviantArt")
    return url


def page_address(value: object) -> str:
    """The address of a work's own page: https://www.deviantart.com/<handle>/art/<title>, and nothing after it.
    The title part is 200 characters at most, of the letters, digits and four marks DeviantArt makes it from."""
    url = _address(value, PAGE_DOMAINS)
    if not _PAGE_PATH.fullmatch(url.raw_path.decode("ascii")):  # raw_path is the path and the query: so, no query
        raise ValueError("not a work's page")
    return str(url)


def image_address(value: object) -> str:
    """The address of an image on one of DeviantArt's image hosts: a path of MAX_IMAGE_PATH characters at most,
    and no query but DeviantArt's own signature, which is a token of MAX_TOKEN characters at most whose first
    part is a small JSON header that names an algorithm. So the whole address is short: too short for a file."""
    url = _address(value, IMAGE_DOMAINS)
    path, _, query = url.raw_path.decode("ascii").partition("?")
    if len(path) > MAX_IMAGE_PATH or (query and not _signature(query)):
        raise ValueError("not an image address")
    return str(url)


def _signature(query: str) -> bool:
    """True when a query is DeviantArt's signature and nothing else (see image_address)."""
    token = _IMAGE_TOKEN.fullmatch(query)
    if token is None or len(query) > len("token=") + MAX_TOKEN:
        return False
    try:
        header = json.loads(base64.urlsafe_b64decode(token["header"] + "=" * (-len(token["header"]) % 4)))
    except ValueError:  # not base64, not UTF-8 or not JSON
        return False
    return isinstance(header, dict) and isinstance(header.get("alg"), str)


def deviation_id(value: object) -> str:
    """A deviation id in its one spelling, an uppercase UUID. ValueError otherwise: it becomes a file name."""
    if not isinstance(value, str) or not _UUID.fullmatch(value.upper()):
        raise ValueError("not a deviation id")
    return value.upper()


def published_at(value: object) -> str | None:
    """The API's published_time (Unix seconds, as a string) as ISO-8601 UTC. None when it is not a time."""
    try:
        return datetime.fromtimestamp(int(str(value)), UTC).isoformat(timespec="seconds")
    except (ValueError, OverflowError, OSError):
        return None


def checked_work(
    work_id: object, *, page_url: object, title: object, tags: object, thumb_url: object, published: object
) -> tuple[str, str, str | None, str, str, str | None]:
    """One work's values as they are stored: id, page, title, tags as JSON, thumbnail, time published.

    ValueError for an id that is not a UUID or an address that is not acceptable, and its subclass ContactInTitle
    for a title that holds contact details. A tag that is not acceptable is left out, and the work is still good.
    """
    title_kept, tags_kept = _title(title), json.dumps(_tags(tags))
    return (
        deviation_id(work_id),
        page_address(page_url),
        title_kept,
        tags_kept,
        image_address(thumb_url),
        published_at(published),
    )


def _key(artist_key: object) -> str:
    """The artist key, only as the store spells it, so that a removal always finds what was kept under it."""
    if not isinstance(artist_key, str) or store.artist_key(artist_key.removeprefix("da:")) != artist_key:
        raise ValueError("not an artist key")
    return artist_key


def account(user_id: object, spelling: str = "uid") -> str | None:
    """The hash an account is known by: sha256 of 'uid:<the API's user id>'. None when there is no id.

    ponytail: profiles written before this spelling hashed 'da-user:<id>'. Both are recognised where an account
    is looked for; drop the old one when no row is left that a run has not rewritten.
    """
    if not isinstance(user_id, str) or not user_id:
        return None
    return hashlib.sha256(f"{spelling}:{user_id}".encode()).hexdigest()


@contextlib.contextmanager
def _open(db: pathlib.Path | str | None) -> Iterator[sqlite3.Connection]:
    """One transaction on the store's own file, opened the store's way (secure_delete on), with these tables.

    The tables are made when there are none, and a table that exists is never changed here. Other stages open
    the file through this for tables of their own, and one of them doing so is how an earlier version came to
    alter the profiles table under a collector that was still writing to it. Only store_artist() upgrades.
    """
    with store._open(db) as conn:
        conn.executescript(_SCHEMA)
        yield conn


def _upgrade(conn: sqlite3.Connection) -> None:
    """Rebuild a profiles table that an earlier version of this module made, in one transaction of its own.

    Called by store_artist() alone, so only by a collector that is about to write, and never by a stage that
    merely reads. An earlier table is known by having no CHECK: with a bio column, or without one and with
    rows whose values sit one column to the right of where they belong.
    """
    (made_by,) = conn.execute("SELECT sql FROM sqlite_master WHERE name = 'profiles'").fetchone()
    if "CHECK" not in made_by:
        conn.executescript(_UPGRADE)


def _cached_name(work_id: str) -> str | None:
    """The name of the work's file in the cache, when there is one."""
    names = (work_id + extension for extension, _ in IMAGE_TYPES.values())
    return next((name for name in names if (image_dir() / name).is_file()), None)


# ponytail: the store has no call that takes a handle out of its artists table without also putting the artist
# and their site on the removal list, so the row is deleted here. Move this to the store when it has one.
_UNLIST = "DELETE FROM artists WHERE key = ?"


def _delete(conn: sqlite3.Connection, column: str, value: str, but: str = "") -> None:
    """Delete the rows with this value in this column from every table in the file that has such a column.

    Hard rule 7 with no list of tables to keep up to date: the analysis stages keep tables of their own here,
    by deviation_id and by artist_key, and a purge that knew only this module's tables left their rows behind.
    """
    having = "SELECT m.name FROM sqlite_master AS m JOIN pragma_table_info(m.name) AS c WHERE m.type = 'table' "
    for (table,) in conn.execute(having + "AND lower(c.name) = ? AND m.name != ?", (column, but)).fetchall():
        quoted = table.replace('"', '""')
        conn.execute(f'DELETE FROM "{quoted}" WHERE {column} = ?', (value,))


def _drop_works(conn: sqlite3.Connection, key: str, keep: set[str]) -> int:
    """Delete the artist's works that are not in `keep`: every cached file of theirs, and their rows in every table.

    Returns how many works are stuck: a file of theirs could not be deleted, so their rows stay to say so, and
    the rest are deleted all the same.
    """
    stuck = 0
    for (work_id,) in conn.execute("SELECT deviation_id FROM works WHERE artist_key = ?", (key,)).fetchall():
        if work_id in keep:
            continue
        try:
            if _UUID.fullmatch(work_id):  # checked again: a file to delete is never named by a row taken on trust
                for path in image_dir().glob(f"{work_id}.*"):
                    path.unlink()
        except OSError:
            stuck += 1
            continue
        _delete(conn, "deviation_id", work_id)
    return stuck


def _forget(conn: sqlite3.Connection, key: str) -> int:
    """Delete an artist's works and cached files, and every other row under their key: the profile, the note
    that they were passed over, and whatever another stage keeps by artist_key. Returns how many works are
    stuck (see _drop_works)."""
    stuck = _drop_works(conn, key, set())
    _delete(conn, "artist_key", key, but="works")
    return stuck


def store_artist(
    artist_key: str,
    *,
    user_id: object,
    tagline: object,
    specialty: object,
    level: object,
    website: object,
    works: Iterable[Mapping[str, Any]],
    db: pathlib.Path | str | None = None,
) -> tuple[int, int]:
    """Replace everything kept about one artist, in one transaction.

    The profile is these four public fields and nothing else. `works` are keyword arguments for checked_work();
    each row names the work's cached image when there is one. The artist's other works go, with their files.
    So does whatever is kept under another key for the same `user_id`: that is this account under the handle
    it had before. A removal by the new handle must not leave the old one behind anywhere (hard rule 7), so the
    old handle also leaves the store's artists table, and the collector stops asking about it.
    The artist is no longer one that was passed over (see note_skip).

    Returns two counts of what went wrong without stopping the rest: works whose row the database refused (a
    constraint; the work is then not kept), and works that had to go but have a file that could not be deleted
    (their row stays, so the next purge finds it).

    ValueError, and nothing stored, when a work is not acceptable. sqlite3.IntegrityError, and nothing changed,
    when the database refuses the profile row itself.
    """
    key = _key(artist_key)
    user_hash = account(user_id)
    profile = (key, _text(tagline, 200), _choice(specialty), _choice(level), _website(website), store._now(), user_hash)
    rows = [checked_work(**work) for work in works]
    with _open(db) as conn:
        _upgrade(conn)
        # Every column is named, here and below: a row written by position lands in the wrong columns the day
        # the table changes.
        conn.execute(
            "INSERT OR REPLACE INTO profiles (artist_key, tagline, specialty, level, website, fetched_at, user_hash) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            profile,
        )
        conn.execute("DELETE FROM skipped WHERE artist_key = ?", (key,))
        stored = set()
        for work_id, *rest in rows:
            try:
                conn.execute(
                    "INSERT OR REPLACE INTO works (deviation_id, artist_key, page_url, title, tags, thumb_url, "
                    "published_at, image_file, fetched_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (work_id, key, *rest, _cached_name(work_id), store._now()),
                )
                stored.add(work_id)
            except sqlite3.IntegrityError:  # SQLite undoes that one statement; the transaction and the artist go on
                pass
        stuck = _drop_works(conn, key, stored)
        same_account = "SELECT artist_key FROM profiles WHERE user_hash IN (?, ?) AND artist_key != ?"
        for (other,) in conn.execute(same_account, (user_hash, account(user_id, "da-user"), key)).fetchall():
            stuck += _forget(conn, other)
            conn.execute(_UNLIST, (other,))
    return len({row[0] for row in rows} - stored), stuck


def has_works(artist_key: str, db: pathlib.Path | str | None = None) -> bool:
    """True when at least one work is kept for the artist."""
    with _open(db) as conn:
        return bool(conn.execute("SELECT 1 FROM works WHERE artist_key = ? LIMIT 1", (_key(artist_key),)).fetchone())


def cached_image(work_id: str, db: pathlib.Path | str | None = None) -> bool:
    """True when the work's row names a cached image and that file is there."""
    work_id = deviation_id(work_id)
    with _open(db) as conn:
        row = conn.execute("SELECT image_file FROM works WHERE deviation_id = ?", (work_id,)).fetchone()
    return bool(row and row[0] and (image_dir() / row[0]).is_file())


def save_image(work_id: str, content_type: str, data: bytes) -> str:
    """Write a work's image into the cache. Returns the file name. The only place image bytes are written.

    ValueError, and nothing written, unless the type is one of IMAGE_TYPES, the bytes begin the way that type
    does, and there are no more than MAX_IMAGE_BYTES of them. The row that names the file is written by
    store_artist(); a file that never gets one is deleted by drop_orphans().
    """
    kind = IMAGE_TYPES.get(content_type)
    if kind is None or not kind[1].match(data) or len(data) > MAX_IMAGE_BYTES:
        raise ValueError("not an image of an accepted type and size")
    name = deviation_id(work_id) + kind[0]
    image_dir().mkdir(parents=True, exist_ok=True)
    part = image_dir() / f"{name}.part"
    part.write_bytes(data)
    part.replace(image_dir() / name)  # whole or not at all: a rerun never takes half a file for a cached image
    return name


def purge(artist_key: str, db: pathlib.Path | str | None = None) -> None:
    """Hard rule 7: delete everything kept about the artist. Safe to repeat.

    That is their profile, their works with every cached image, the note that they were passed over, and every
    row another stage keeps about them or their works: whatever table of this file has an artist_key or a
    deviation_id column (see _delete). There is no list of tables to forget to add one to.

    One file that cannot be deleted does not stop the rest. OSError once everything else is gone: that work's
    rows stay, so artist_keys() still lists the artist and the purge can be run again.
    """
    key = _key(artist_key)
    with _open(db) as conn:
        stuck = _forget(conn, key)
    if stuck:
        raise OSError("a cached file could not be deleted")


def unlist(artist_key: str, db: pathlib.Path | str | None = None) -> None:
    """purge(), and then the handle leaves the collector's queue: its row in the store's artists table goes.

    For a handle whose account, or whose every work, the API has shown gone on two runs. It may be the old
    handle of an account that was then removed under its new one, and that must not stay anywhere (hard rule
    7). Unlike a removal this bars nobody: discovery stores the handle again if it finds it again. OSError, and
    the handle stays queued so the next run tries again, when a file could not be deleted.
    """
    purge(artist_key, db)
    with _open(db) as conn:
        conn.execute(_UNLIST, (artist_key,))


def note_skip(artist_key: str, gone: bool = False, db: pathlib.Path | str | None = None) -> int:
    """Note, with the time, that the collector passed this artist over, so that the next run takes them last.

    `gone` says the API called their account suspended, inactive or missing, or showed none of the works kept
    for them. Returns in how many runs it has been noted so since the artist was last stored. Nothing else
    moves that number: an answer that says nothing about the account, as every answer does while the API is
    down, must not count against anybody.
    """
    key = _key(artist_key)
    with _open(db) as conn:
        conn.execute(
            "INSERT INTO skipped (artist_key, skipped_at, unavailable) VALUES (?, ?, ?) ON CONFLICT(artist_key) "
            "DO UPDATE SET skipped_at = excluded.skipped_at, unavailable = unavailable + excluded.unavailable",
            (key, store._now(), int(gone)),
        )
        return int(conn.execute("SELECT unavailable FROM skipped WHERE artist_key = ?", (key,)).fetchone()[0])


def drop_stale_skips(db: pathlib.Path | str | None = None) -> None:
    """Delete the note about every handle that is no longer in the store's artists table.

    Nobody will ask about such a handle again, so the note serves nothing, and it names somebody: an artist who
    was removed, or the handle an account had before it was renamed and then removed (hard rule 7).
    """
    with _open(db) as conn:
        conn.execute("DELETE FROM skipped WHERE artist_key NOT IN (SELECT key FROM artists)")


def last_attempts(db: pathlib.Path | str | None = None) -> dict[str, str]:
    """For every artist the collector has stored or passed over: when it last did, as an ISO-8601 time."""
    query = "SELECT artist_key, max(at) FROM (SELECT artist_key, fetched_at AS at FROM profiles UNION ALL "
    with _open(db) as conn:
        return dict(conn.execute(query + "SELECT artist_key, skipped_at FROM skipped) GROUP BY artist_key"))


def account_hash(artist_key: str, db: pathlib.Path | str | None = None) -> str | None:
    """The hash the artist's account is known by, when their profile is stored. It is what goes on the store's
    removal list when they are removed, so that the same account under a new handle is known for what it is."""
    key = _key(artist_key)
    with _open(db) as conn:
        try:
            row = conn.execute("SELECT user_hash FROM profiles WHERE artist_key = ?", (key,)).fetchone()
        except sqlite3.OperationalError:  # a table from the first version, which kept no hash
            return None
    # Only a hash is handed on: a table an earlier version left may hold something else in this column.
    return row[0] if row and row[0] and re.fullmatch(r"[0-9a-f]{64}", row[0]) else None


def account_removed(user_id: object, db: pathlib.Path | str | None = None) -> bool:
    """True when the account with this API user id is on the store's removal list, under whatever handle it was
    removed (hard rule 7). Read through the store's own check, so there is one removal list and one way to ask it.

    A known gap, for whoever joins this to the database of record: the account hashes exist only in this file.
    An artist removed before they were ever collected has no hash on the list, so a new handle of theirs is
    not recognised; and if this file is lost, so are the hashes. Both need the hashes kept with the list of record.
    """
    if account(user_id) is None:
        return False
    with _open(db) as conn:
        return store._removed(conn, [f"uid:{user_id}", f"da-user:{user_id}"])


def drop_orphans(db: pathlib.Path | str | None = None) -> tuple[int, int]:
    """Delete every file under the image cache that no row names: (how many were deleted, how many could not be).

    Such a file is left by a run cut short between a download and its row, or by a database that was lost
    while the images stayed. Without a row no purge would ever find it (hard rule 7).

    The image cache belongs to the default database. Given any other database this does nothing: measured
    against a stranger's rows, every image in the cache would look like an orphan.
    """
    if not image_dir().is_dir() or pathlib.Path(db or config.LOCAL_DB).resolve() != config.LOCAL_DB.resolve():
        return 0, 0
    with _open(db) as conn:
        named = {name for (name,) in conn.execute("SELECT image_file FROM works WHERE image_file IS NOT NULL")}
    deleted = failed = 0
    for path in image_dir().rglob("*"):
        if path.is_file() and path.relative_to(image_dir()).as_posix() not in named:
            try:
                path.unlink()
                deleted += 1
            except OSError:
                failed += 1
    return deleted, failed


def artist_keys(db: pathlib.Path | str | None = None) -> list[str]:
    """The key of every artist with a profile or a work here, in key order."""
    query = "SELECT artist_key FROM profiles UNION SELECT artist_key FROM works ORDER BY 1"
    with _open(db) as conn:
        return [key for (key,) in conn.execute(query)]
