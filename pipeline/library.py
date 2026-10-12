"""What the collect stage keeps: each artist's public profile fields, their recent works, one cached image a work.

Two tables in the local SQLite file, beside the store's (pipeline/store.py), and image files in image_dir().
Every value is checked or cleaned here, on the way in, whoever calls:
  - hard rule 1: the tables are STRICT and hold text only: words, addresses and a file name. A text loses its
    markup, embedded files, email addresses and phone numbers and is cut to a limit. An address must be https
    on DeviantArt.
  - hard rule 2: image bytes are written by save_image() and nowhere else, as files under config.CACHE_DIR.
  - hard rule 7: purge() deletes everything kept about one artist, files included.
Every function takes an optional `db` path; the default is config.LOCAL_DB. A work whose image_file is NULL has
no cached image (its download failed, or a run was cut short): the analysis stage must leave it alone.
"""

import contextlib
import html
import json
import pathlib
import re
import sqlite3
import unicodedata
from collections.abc import Iterable, Iterator
from datetime import UTC, datetime

import httpx

from pipeline import config, store

_SCHEMA = """
CREATE TABLE IF NOT EXISTS profiles (
    artist_key TEXT PRIMARY KEY,     -- artists.key: 'da:<lowercased handle>'
    tagline TEXT,
    specialty TEXT,
    level TEXT,
    bio TEXT,
    website TEXT,                    -- scheme://host/path, nothing else
    fetched_at TEXT NOT NULL         -- ISO-8601 UTC
) STRICT;
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
"""

MAX_IMAGE_BYTES = 10 * 1024 * 1024  # a preview is a few hundred KB; anything near this is not one
# The image types kept: content type -> (file extension, the bytes such a file starts with).
IMAGE_TYPES = {
    "image/jpeg": (".jpg", b"\xff\xd8\xff"),
    "image/png": (".png", b"\x89PNG\r\n\x1a\n"),
    "image/gif": (".gif", b"GIF8"),
    "image/webp": (".webp", b"RIFF"),
}
PAGE_DOMAINS = ("deviantart.com",)  # where a work's page lives
IMAGE_DOMAINS = ("wixmp.com", "deviantart.net", "deviantart.com")  # where DeviantArt serves its images from

_UUID = re.compile(r"[0-9A-F]{8}(?:-[0-9A-F]{4}){3}-[0-9A-F]{12}")
_TAG_NAME = re.compile(r"[\w-]{1,64}")
_MARKUP = re.compile(r"<[^<>]*>")
_ENCODED_FILE = re.compile(r"[\w+/=%-]{200,}", re.ASCII)  # no word is this long; a base64 or percent-encoded file is
_DATA_URI = re.compile(r"data:[^\s,]{0,100},\S*", re.IGNORECASE)  # the short ones the line above lets through
# ponytail: plain spellings only. "name at host dot com" and a number written in words get through; nothing
# short of reading the text finds those.
_EMAIL = re.compile(r"[A-Za-z0-9._%+-]{1,64}@[A-Za-z0-9-]+(?:\.[A-Za-z0-9-]+)+")
_PHONE = re.compile(r"\+?\d(?:[\s().-]{0,2}\d){8,}")  # nine digits or more, however they are grouped


def image_dir() -> pathlib.Path:
    """Where cached images live. Read from config on every call, never passed in: there is no other place."""
    return config.CACHE_DIR / "images"


def _text(value: object, limit: int) -> str | None:
    """A text field as plain words: no markup, embedded file, email address or phone number, cut to `limit`.

    The whole value is cleaned before it is cut, so a tag or a file that straddles the limit leaves no half behind.
    Each pattern reads a character a bounded number of times, so the cost grows in step with the length.
    """
    if not isinstance(value, str):
        return None
    text = unicodedata.normalize("NFKC", html.unescape(value))  # fullwidth "@" and digits become plain
    for pattern in (_MARKUP, _ENCODED_FILE, _DATA_URI, _EMAIL, _PHONE):
        text = pattern.sub(" ", text)
    text = "".join(character if character.isprintable() else " " for character in text)
    return " ".join(text.split())[:limit].strip() or None


def _website(value: object) -> str | None:
    """A profile's website field as scheme://host/path, or None. No query, no fragment, and never an '@'."""
    if not isinstance(value, str) or len(value) > 300:
        return None
    text = unicodedata.normalize("NFKC", value).strip()
    if "@" in text or "%40" in text:  # an email address, or credentials (hard rule 6)
        return None
    try:
        url = httpx.URL(text if "://" in text else f"https://{text}")
        host = store.canonical_host(url.raw_host.decode("ascii"))
    except (httpx.InvalidURL, UnicodeError):
        return None
    if url.scheme not in ("http", "https") or host is None:
        return None
    return f"{url.scheme}://{host}{url.raw_path.decode('ascii').partition('?')[0]}"


def checked_url(value: object, domains: tuple[str, ...]) -> str:
    """An https address on one of these domains, or ValueError. So never http, data:, a port or another host."""
    if not isinstance(value, str) or len(value) > 2000 or not (value.isascii() and value.isprintable()):
        raise ValueError("not an address")  # never echoes the input
    try:
        url = httpx.URL(value)
    except httpx.InvalidURL:
        raise ValueError("not an address") from None
    host = url.raw_host.decode("ascii")
    on_domain = any(host == domain or host.endswith(f".{domain}") for domain in domains)
    if url.scheme != "https" or url.userinfo or url.port is not None or not on_domain or set(value) & {" ", "\\"}:
        raise ValueError("not an https address on DeviantArt")
    return str(url)


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


def _key(artist_key: object) -> str:
    """The artist key, only as the store spells it, so that a removal always finds what was kept under it."""
    if not isinstance(artist_key, str) or store.artist_key(artist_key.removeprefix("da:")) != artist_key:
        raise ValueError("not an artist key")
    return artist_key


@contextlib.contextmanager
def _open(db: pathlib.Path | str | None) -> Iterator[sqlite3.Connection]:
    """One transaction on the store's own file, opened the store's way (secure_delete on), with these tables."""
    with store._open(db) as conn:
        conn.executescript(_SCHEMA)
        yield conn


def upsert_profile(
    artist_key: str,
    *,
    tagline: object,
    specialty: object,
    level: object,
    bio: object,
    website: object,
    db: pathlib.Path | str | None = None,
) -> None:
    """Store an artist's public professional fields, replacing what was there. These five are all there is room for."""
    fields = (_text(tagline, 200), _text(specialty, 100), _text(level, 100), _text(bio, 2000), _website(website))
    row = (_key(artist_key), *fields, store._now())
    with _open(db) as conn:
        conn.execute("INSERT OR REPLACE INTO profiles VALUES (?, ?, ?, ?, ?, ?, ?)", row)


def upsert_work(
    artist_key: str,
    work_id: str,
    *,
    page_url: object,
    title: object,
    tags: object,
    thumb_url: object,
    published: object,
    db: pathlib.Path | str | None = None,
) -> None:
    """Insert one work, or refresh the row already there (one row per deviation id). Its cached image is untouched.

    ValueError, and nothing stored, for an id that is not a UUID or an address that is not https on DeviantArt.
    """
    names = [name for tag in (tags if isinstance(tags, list) else []) if (name := _text(tag, 100))]
    row = (
        deviation_id(work_id),
        _key(artist_key),
        checked_url(page_url, PAGE_DOMAINS),
        _text(title, 200),
        json.dumps([name for name in names if _TAG_NAME.fullmatch(name)][:60]),
        checked_url(thumb_url, IMAGE_DOMAINS),
        published_at(published),
        store._now(),
    )
    with _open(db) as conn:
        conn.execute(
            "INSERT INTO works (deviation_id, artist_key, page_url, title, tags, thumb_url, published_at, fetched_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT(deviation_id) DO UPDATE SET "
            "artist_key = excluded.artist_key, page_url = excluded.page_url, title = excluded.title, "
            "tags = excluded.tags, thumb_url = excluded.thumb_url, published_at = excluded.published_at, "
            "fetched_at = excluded.fetched_at",
            row,
        )


def cached_image(work_id: str, db: pathlib.Path | str | None = None) -> bool:
    """True when the work's image is already in the cache. Its row is then made to name that file."""
    work_id = deviation_id(work_id)
    for extension, _ in IMAGE_TYPES.values():
        if (image_dir() / f"{work_id}{extension}").is_file():
            with _open(db) as conn:
                conn.execute("UPDATE works SET image_file = ? WHERE deviation_id = ?", (work_id + extension, work_id))
            return True
    return False


def save_image(work_id: str, content_type: str, data: bytes, db: pathlib.Path | str | None = None) -> str:
    """Write a work's image into the cache and name the file on its row. Returns the file name.

    The only place image bytes are written (hard rule 2). ValueError, and nothing written, unless the work has
    a row (so purge() can always find the file), the type is one of IMAGE_TYPES, the bytes start the way that
    type does, and there are no more than MAX_IMAGE_BYTES of them.
    """
    work_id = deviation_id(work_id)
    extension, start = IMAGE_TYPES.get(content_type, ("", b""))
    if not extension or not data.startswith(start) or len(data) > MAX_IMAGE_BYTES:
        raise ValueError("not an image of an accepted type and size")
    name = work_id + extension
    with _open(db) as conn:
        if not conn.execute("SELECT 1 FROM works WHERE deviation_id = ?", (work_id,)).fetchone():
            raise ValueError("no such work")
        image_dir().mkdir(parents=True, exist_ok=True)
        part = image_dir() / f"{name}.part"
        part.write_bytes(data)
        part.replace(image_dir() / name)  # whole or not at all: a rerun never takes half a file for a cached image
        conn.execute("UPDATE works SET image_file = ? WHERE deviation_id = ?", (name, work_id))
    return name


def drop_other_works(artist_key: str, keep: Iterable[str], db: pathlib.Path | str | None = None) -> int:
    """Delete the artist's works that are not in `keep`, and every cached file of theirs. Returns how many works."""
    kept = set(keep)
    with _open(db) as conn:
        rows = conn.execute("SELECT deviation_id FROM works WHERE artist_key = ?", (_key(artist_key),)).fetchall()
        dropped = [work_id for (work_id,) in rows if work_id not in kept]
        for work_id in dropped:
            # Files first: if one cannot be deleted, the error undoes the transaction and the row stays to say so.
            # The id is checked again, because a file to delete is never named by a row taken on trust.
            if _UUID.fullmatch(work_id):
                for path in image_dir().glob(f"{work_id}.*"):
                    path.unlink()
            conn.execute("DELETE FROM works WHERE deviation_id = ?", (work_id,))
    return len(dropped)


def purge(artist_key: str, db: pathlib.Path | str | None = None) -> None:
    """Hard rule 7: delete the artist's profile, their works and every cached image of those works. Safe to repeat."""
    drop_other_works(artist_key, (), db)
    with _open(db) as conn:
        conn.execute("DELETE FROM profiles WHERE artist_key = ?", (artist_key,))


def artist_keys(db: pathlib.Path | str | None = None) -> list[str]:
    """The key of every artist with a profile or a work here, in key order."""
    query = "SELECT artist_key FROM profiles UNION SELECT artist_key FROM works ORDER BY 1"
    with _open(db) as conn:
        return [key for (key,) in conn.execute(query)]
