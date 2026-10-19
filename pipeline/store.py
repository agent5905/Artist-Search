"""The local SQLite store: discovered artists, and the removal list that keeps removed artists out (hard rule 7).

A removal identifier is 'da:<lowercased handle>' for an artist or 'site:<registrable domain>' for a site, and
the list holds only the sha256 of each. normalize() turns what an operator might type into one, or raises.
The removal list of record lives in the remote database; this file is a cache, and import_removed() refills it.
Once discovery has run, it also holds how far its browsing has got (browsing_pass): tags, offsets and hashes.
Every function takes an optional `db` path; the default is config.LOCAL_DB.
"""

import contextlib
import functools
import hashlib
import html
import json
import pathlib
import re
import sqlite3
import unicodedata
from collections.abc import Collection, Iterable, Iterator
from datetime import UTC, datetime
from urllib.parse import unquote, urlsplit

from pipeline import config, crawl

_SCHEMA = """
CREATE TABLE IF NOT EXISTS artists (
    key TEXT PRIMARY KEY,            -- 'da:<lowercased username>'
    da_username TEXT NOT NULL,
    da_profile_url TEXT NOT NULL,
    links TEXT NOT NULL,             -- JSON list of public URLs
    niche_hits INTEGER NOT NULL,
    discovered_at TEXT NOT NULL      -- ISO-8601 UTC, set once
);
CREATE TABLE IF NOT EXISTS removed (
    key_hash TEXT PRIMARY KEY,       -- sha256 hex of the normalized identifier, so the list names nobody
    removed_at TEXT NOT NULL         -- ISO-8601 UTC, set once
);
-- What each profile that was read lists, in a link of any shape, kept or not. Hashes only: it names no artist
-- and no site, and a link that was dropped never reaches the disk. Only own_site_candidates() reads it.
CREATE TABLE IF NOT EXISTS listed (
    domain_hash TEXT NOT NULL,       -- sha256 hex of 'site:<registrable domain>'
    artist_hash TEXT NOT NULL,       -- sha256 hex of 'da:<lowercased username>'
    PRIMARY KEY (domain_hash, artist_hash)
);
"""
# Discovery's own two tables, which only its functions make, on first use (see _open_pass): no other stage's run
# adds a table to the file, and nothing here changes a table that exists.
_PASS = """
-- Where discovery's browsing pass has got to, so that a run that is stopped carries on from there.
CREATE TABLE IF NOT EXISTS browsed (
    tag TEXT PRIMARY KEY,            -- one of discovery's own niche tags
    next_offset INTEGER,             -- where the tag's next page is; NULL when the API says there is no more
    pages INTEGER NOT NULL,          -- pages of the tag read in this pass
    began_at TEXT NOT NULL           -- ISO-8601 UTC, set once: a pass is as old as its first page
);
-- The pass's tally: one row for each work seen, so that a work under two tags counts once. Hashes only: no
-- title, no id, no handle. A handle is written down when its author has works enough, and then in `artists`.
CREATE TABLE IF NOT EXISTS counted (
    work_hash TEXT PRIMARY KEY,      -- sha256 hex of 'work:<deviation id>'
    author_hash TEXT                 -- sha256 hex of 'da:<lowercased username>'; NULL for a work counted for nobody
);
CREATE INDEX IF NOT EXISTS counted_by_author ON counted (author_hash);
"""

HANDLE = re.compile(r"[A-Za-z0-9_-]{1,40}")  # a DeviantArt handle. Also what makes one safe to put in a URL path.
_PROFILE_PATH = re.compile(rf"/({HANDLE.pattern})/?")  # deviantart.com/<handle> and nothing after it
# A public DNS name: letter-digit-hyphen labels and an alphabetic (or punycode) top level. No IP address, no
# localhost, no percent sign, backslash or other stray character.
_HOSTNAME = re.compile(r"(?=.{4,253}$)(?:[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\.)+(?:[a-z]{2,63}|xn--[a-z0-9-]{1,59})")
_DOTS = "." + "".join(map(chr, (0x3002, 0xFF0E, 0xFF61)))  # the full stop, and the three characters IDNA reads as one
# For finding hostnames in running text: a run of letters, digits, hyphens and dots, and a run of two or more
# dots inside one (an ellipsis). Each character is read once, so a long run costs time in proportion to its length.
_HOST_RUN = re.compile(rf"(?:[^\W_]|[-.{chr(0x3002)}])+")
_ELLIPSIS = re.compile(rf"[.{chr(0x3002)}]{{2,}}")
# A dot written as "[.]", "(dot)" or " dot ". It is swapped for a mark first, so that only the single spaces
# around that mark are closed up. Nothing here can match the same character twice, so it stays linear too.
_MARK = chr(0xE000)  # a private-use character: it means nothing anywhere else
_DOT_WORDS = re.compile(r"[\[({] ?(?:\.|dot) ?[\])}]|(?<=\S) dot (?=\S)", re.IGNORECASE)
_SPACED_DOT = re.compile(f" ?{_MARK} ?")
# ponytail: the standard library's codec is IDNA 2003. It respells these five characters where browsers keep
# them (sharp s becomes "ss"), so the result could name somebody else's site. A host that has one is dropped.
# The idna package (IDNA 2008) is the upgrade if a German or Greek artist's site is lost this way.
_IDNA_2003_ONLY = "".join(map(chr, (0xDF, 0x1E9E, 0x3C2, 0x200C, 0x200D)))  # sharp s twice, final sigma, two joiners
_SHA256 = re.compile(r"[0-9a-f]{64}")
_OTHERS = "SELECT 1 FROM listed WHERE domain_hash = ? AND artist_hash != ?"  # is a domain listed by anybody else


def canonical_host(host: str) -> str | None:
    """The one spelling of a hostname: IDNA-encoded, lowercase, no trailing dot. None if it is not a public DNS name.

    Every host comparison in discovery and in the removal list goes through here.
    pipeline/crawl.py keeps its hostname check private, so this one is ours.
    """
    if any(character in host for character in _IDNA_2003_ONLY):
        return None
    try:
        host = host.strip().rstrip(_DOTS).encode("idna").decode("ascii").lower()
    except UnicodeError:  # an empty or over-long label, or a character IDNA forbids
        return None
    return host if _HOSTNAME.fullmatch(host) else None


def hosts_in(text: object) -> set[str]:
    """Every public hostname written anywhere in a piece of text, each in its one spelling.

    This is not a URL parser, on purpose. It serves hard rule 7, where a site must be recognised in a profile's
    website field however the field is written: inside brackets, behind a backslash, followed by a remark,
    percent-encoded, in fullwidth letters, with invisible characters in it, or with its dot spelled out.

    ponytail: it errs towards finding a name. "polka dot art" reads as polka.art. That costs nothing unless
    polka.art is on the removal list or is somebody's site; tighten _DOT_WORDS if it ever does.
    """
    found: set[str] = set()
    if not isinstance(text, str):
        return found
    versions = [text[:10_000], html.unescape(text[:10_000])]  # as written, and with &#46; read as a dot
    for _ in range(3):  # percent-encoding undone, as deep as %25252E
        versions.append(unquote(versions[-1]))
    # A dot spelled out to fool a reader of addresses: name[.]art, name (dot) art, name dot art.
    versions += [_SPACED_DOT.sub(".", _DOT_WORDS.sub(_MARK, version)) for version in versions]
    for version in dict.fromkeys(versions):
        version = unicodedata.normalize("NFKC", version)  # fullwidth letters and dots become plain ones
        version = "".join(character for character in version if unicodedata.category(character) != "Cf")
        tokens = (token for run in _HOST_RUN.findall(version) for token in _ELLIPSIS.split(run))
        found.update(host for token in tokens if (host := canonical_host(token.strip(_DOTS))))
    return found


@functools.lru_cache(maxsize=10_000)
def registrable_domain(host: str) -> str | None:
    """The registrable domain of a canonical host, when that domain could be somebody's own site.

    This is pipeline/crawl.py's notion (the bundled Public Suffix List), reached through its public is_own_site
    because it exposes no helper of its own: is_own_site is True for the front page of a registrable domain and
    for nothing shorter, so the shortest suffix of the host that passes is the registrable domain.
    None for a platform, a suffix some host hands out, or a name with no public suffix.
    """
    labels = host.split(".")
    for start in range(len(labels) - 2, -1, -1):
        domain = ".".join(labels[start:])
        if crawl.is_own_site(f"https://{domain}/"):
            return domain
    return None


def _host(url: str) -> str | None:
    """The canonical host of a URL or of a bare hostname. None when it has none, or holds an @ (an email address)."""
    try:
        parts = urlsplit(url if "://" in url else f"//{url}")
    except ValueError:
        return None
    return None if "@" in parts.netloc else canonical_host(parts.hostname or "")


def _named(text: str) -> tuple[str, str] | None:
    """A handle, a DeviantArt profile URL, a hostname or a site URL as ('da', handle) or ('site', canonical host)."""
    if HANDLE.fullmatch(text):
        return "da", text.lower()
    host = _host(text)
    if host is None:
        return None
    if host.removeprefix("www.") != "deviantart.com":
        return "site", host
    # Only deviantart.com/<handle> is a profile. Anything longer could be one of the site's own routes
    # (/notes/..., /tag/...), and guessing a handle out of it would bar an artist who does not exist.
    profile = _PROFILE_PATH.fullmatch(urlsplit(text if "://" in text else f"//{text}").path)
    return ("da", profile[1].lower()) if profile else None


def _parse(identifier: str) -> tuple[str, str]:
    """What somebody typed as ('da', handle) or ('site', canonical host). ValueError for anything else."""
    text = identifier.strip()
    kind, colon, rest = text.partition(":")
    kind = kind.strip().lower()
    if colon and kind in ("da", "site"):
        text = rest.strip()
    else:
        kind = ""
    named = _named(text.removeprefix("@"))
    if named is None or kind not in ("", named[0]):
        raise ValueError("not an artist handle, a DeviantArt profile URL or a site address")  # never echoes the input
    return named


def normalize(identifier: str) -> str:
    """Any plain spelling of an artist or a site as 'da:<handle>' or 'site:<registrable domain>'.

    Takes a handle, a DeviantArt profile URL (deviantart.com/<handle>, nothing longer), a hostname or a site URL,
    with or without a 'da:' or 'site:' prefix. Raises ValueError for anything else, so a removal can never
    silently do nothing (hard rule 7). That includes a host discovery can never store: one on a platform
    (artstation.com, someone.tumblr.com) or with no registrable domain.
    """
    kind, value = _parse(identifier)
    if kind == "da":
        return f"da:{value}"
    domain = registrable_domain(value)
    if domain is None:
        raise ValueError("not a site of anybody's own: a platform, or a name with no registrable domain")
    return f"site:{domain}"


def artist_key(username: str) -> str:
    """'da:<lowercased handle>'. ValueError unless the username is a plain handle."""
    if not HANDLE.fullmatch(username):
        raise ValueError("not an artist handle")
    return f"da:{username.lower()}"


def site_key(url: str) -> str:
    """'site:<registrable domain>' for the site a URL or hostname is part of. ValueError when it is nobody's own."""
    return normalize(f"site:{url}")


def _host_keys(host: str) -> list[str]:
    """The removal keys that cover a canonical host: for the host and each parent domain, its own and its www. form.

    A removal made here is always a registrable domain, which is one of these. A hash imported from the list of
    record may have been made from a host: 'site:www.janedoe.art' covers janedoe.art and every host under it,
    because that is one of these too. A hash made from another subdomain ('site:shop.janedoe.art') covers that
    host and the hosts under it, and no more: a hash cannot be turned back into the site it belongs to.
    """
    labels = host.split(".")
    parents = [".".join(labels[start:]) for start in range(len(labels) - 1)]
    return [f"site:{prefix}{parent}" for parent in parents for prefix in ("", "www.")]


def _link_keys(links: list[str]) -> set[str]:
    """Every removal key that covers one of an artist's links. A link with no public host covers nothing."""
    return {key for link in links if (host := _host(link)) for key in _host_keys(host)}


def _link_domains(links: list[str]) -> set[str]:
    """The registrable domain of each link that has one. A link to a platform has none."""
    return {domain for link in links if (host := _host(link)) and (domain := registrable_domain(host))}


def _hash(key: str) -> str:
    return hashlib.sha256(key.encode()).hexdigest()


def _now() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds")


class Snapshot(str):
    """The store's path, standing for one copy of the whole file made at one moment (see snapshot). Given it for
    `db`, every function here, in pipeline/library.py and in the stages that open the file through them reads
    that copy."""

    conn: sqlite3.Connection  # the copy
    _file: sqlite3.Connection | None  # the file, asked only whether it has changed since
    _version: int

    def changed(self) -> bool:
        """True when something has been committed to the file since the copy was made."""
        return self._file is not None and self._file.execute("PRAGMA data_version").fetchone()[0] != self._version


@contextlib.contextmanager
def snapshot(db: pathlib.Path | str | None = None) -> Iterator[Snapshot]:
    """The store as it is at this moment, for reading: a copy of the whole file, made in one read transaction
    that ends when the copy is made, so that a stage that writes waits on it no longer than that. What is read
    through it all comes from that one moment, and nothing done to it reaches the file: a table the file has not
    got is made in the copy, empty, by whichever reader opens it. No file is made when there is none.

    ponytail: the whole file in memory, 61 MB in 0.24 s on 2026-10-13. Past a few hundred MB, hold one read
    transaction on the file instead, in WAL mode so that the stages that write do not wait on it.
    """
    path = pathlib.Path(db or config.LOCAL_DB)
    held = Snapshot(path)
    held.conn, held._file = sqlite3.connect(":memory:"), None
    try:
        if path.is_file():
            held._file = sqlite3.connect(f"{path.resolve().as_uri()}?mode=ro", uri=True, timeout=60)
            held._file.execute("BEGIN")
            held._file.execute("SELECT 1 FROM sqlite_master").fetchone()  # the moment: from here the file holds still
            held._version = held._file.execute("PRAGMA data_version").fetchone()[0]
            held._file.backup(held.conn)
            held._file.commit()
        yield held
    finally:
        held.conn.close()
        if held._file is not None:
            held._file.close()


@contextlib.contextmanager
def _open(db: pathlib.Path | str | None) -> Iterator[sqlite3.Connection]:
    """One transaction on the store: committed on success, rolled back on error, always closed. Given a
    Snapshot, one on its copy, which stays open for the next reader."""
    if isinstance(db, Snapshot):
        conn = db.conn
    else:
        path = pathlib.Path(db or config.LOCAL_DB)
        path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(path, timeout=60)  # several stages write to this file; wait for a lock, do not fail
    try:
        conn.execute("PRAGMA secure_delete = ON")  # a removed artist's row is zeroed in the file, not just unlinked
        conn.executescript(_SCHEMA)
        with conn:
            yield conn
    finally:
        if not isinstance(db, Snapshot):
            conn.close()


@contextlib.contextmanager
def _open_pass(db: pathlib.Path | str | None) -> Iterator[sqlite3.Connection]:
    """_open(), with discovery's two tables made when there are none. Only the functions that read or write a
    browsing pass come in this way, so it is a discovery run that adds them to a store, and no other stage's."""
    with _open(db) as conn:
        conn.executescript(_PASS)
        yield conn


def _removed(conn: sqlite3.Connection, keys: Iterable[str]) -> bool:
    """True when any of these normalized keys is on the removal list."""
    query = "SELECT 1 FROM removed WHERE key_hash = ?"
    return any(conn.execute(query, (_hash(key),)).fetchone() for key in keys)


def _named_by(conn: sqlite3.Connection, named: set[str]) -> list[str]:
    """The keys of the stored artists these key hashes name by their handle. A site's hash names nobody: a removal
    bars only what it is bound to, and a site can be in anybody's row, so barring one never removes an artist.
    Whoever links to a removed site stays out while they do (all_artists), and the collect and push stages hold
    back whoever's profile names it."""
    return sorted(key for (key,) in conn.execute("SELECT key FROM artists") if _hash(key) in named)


def _remove(conn: sqlite3.Connection, key_hashes: Iterable[str]) -> int:
    """Put key hashes on the removal list and delete the stored artists whose handle they are. Returns how many.

    A removal goes no further: a handle bars no site, and a site bars no handle (see _named_by), so it never
    reaches another artist. It works on hashes, so it also works for a site known only as a hash.
    """
    named = set(key_hashes)
    gone = _named_by(conn, named)
    conn.executemany("DELETE FROM artists WHERE key = ?", [(key,) for key in gone])
    conn.executemany("INSERT OR IGNORE INTO removed VALUES (?, ?)", [(key_hash, _now()) for key_hash in sorted(named)])
    return len(gone)


def is_removed(identifier: str, db: pathlib.Path | str | None = None) -> bool:
    """True when the artist, the site or a parent domain of the site is on the removal list.

    ValueError when the identifier names neither an artist nor a host. A host on a platform is simply not removed.
    """
    kind, value = _parse(identifier)
    with _open(db) as conn:
        return _removed(conn, [f"da:{value}"] if kind == "da" else _host_keys(value))


def covers(key_hashes: Collection[str], identifier: str) -> bool:
    """is_removed(), asked of key hashes that need not be in the file: True when one of them is the artist's, the
    site's or a parent domain's. For a caller that must know what a removal list will bar before it is written.
    ValueError as is_removed()."""
    kind, value = _parse(identifier)
    return any(_hash(key) in key_hashes for key in ([f"da:{value}"] if kind == "da" else _host_keys(value)))


def named_by(key_hashes: Iterable[str], db: pathlib.Path | str | None = None) -> list[str]:
    """The keys of the stored artists import_removed() would delete for these key hashes. It changes nothing."""
    with _open(db) as conn:
        return _named_by(conn, set(key_hashes))


def mark_removed(identifier: str, db: pathlib.Path | str | None = None) -> int:
    """Put an artist or a site on the removal list. Safe to repeat.

    A handle bars that handle and deletes that artist's row, and nothing else. A site bars the site and deletes
    nobody: whoever links to it stays out while they do (see _named_by). Returns how many stored artists it
    deleted, so a removal that matched nobody can be seen. ValueError when the identifier names nothing discovery
    could store (see normalize): a removal never silently does nothing.
    """
    key = normalize(identifier)
    with _open(db) as conn:
        return _remove(conn, [_hash(key)])


def import_removed(key_hashes: Iterable[str], db: pathlib.Path | str | None = None) -> int:
    """Take removals from the list of record: sha256 hex digests of 'da:<handle>' and 'site:<host>' keys.

    Stores them, then removes every stored artist whose handle one of them is, just as mark_removed() would; one
    whose link is on a removed site, parent domain or www. form stays out while it is. Returns how many artists it
    deleted. This file can be deleted and refilled; a removal cannot be lost that way.
    """
    hashes = [key_hash.strip().lower() for key_hash in key_hashes]
    if not all(_SHA256.fullmatch(key_hash) for key_hash in hashes):
        raise ValueError("not a sha256 hex digest")
    with _open(db) as conn:
        return _remove(conn, hashes)


def record_listing(username: str, hosts: Iterable[str], db: pathlib.Path | str | None = None) -> bool:
    """Call for every profile read, with every host its website field names, in a link of any shape.

    Records, as hashes only, that this artist lists the registrable domain of each host: own_site_candidates()
    counts those. Returns True when one of the hosts is on a removed site (the host, a parent domain or a www.
    form). That is one answer's word and no proof of anything: nothing is deleted and nobody is barred for it
    here. The caller leaves the artist as they are and holds them back.
    """
    key, hosts = artist_key(username), list(hosts)
    listing = [(_hash(f"site:{domain}"), _hash(key)) for host in hosts if (domain := registrable_domain(host))]
    with _open(db) as conn:
        conn.executemany("INSERT OR IGNORE INTO listed VALUES (?, ?)", listing)
        return _removed(conn, [covering for host in hosts for covering in _host_keys(host)])


def upsert_artist(
    username: str, profile_url: str, links: list[str] | None, niche_hits: int, db: pathlib.Path | str | None = None
) -> bool:
    """Insert one artist, or refresh the row already there. Safe to repeat.

    `links` given as None is discovery's "not known here": it reads no profile. A new row then has no link, and a
    row that is there keeps the one the collect stage wrote into it (see keep_own_site).

    Returns False and changes nothing when the artist is on the removal list, or a link is on a removed site:
    a row they have stays as it was, and nobody is barred for it. ValueError for a username that is not a plain
    handle, or a link that has no public hostname.
    """
    key = artist_key(username)
    if None in map(_host, links or []):
        raise ValueError("a link has no public hostname")
    with _open(db) as conn:
        if _removed(conn, [key, *_link_keys(links or [])]):
            return False
        conn.execute(
            "INSERT INTO artists VALUES (?, ?, ?, ?, ?, ?) "
            "ON CONFLICT(key) DO UPDATE SET da_username = excluded.da_username, "
            "da_profile_url = excluded.da_profile_url, niche_hits = excluded.niche_hits"
            + ("" if links is None else ", links = excluded.links"),
            (key, username, profile_url, json.dumps(links or []), niche_hits, _now()),
        )
    return True


def keep_own_site(username: str, website: str | None, db: pathlib.Path | str | None = None) -> bool:
    """Write the front page of an artist's own site into their row, as their one link. True when a row changed.

    For the collect stage, the one stage that reads a profile: discovery stores an artist with no link.
    One place then says whose site is whose, and a removal of the site keeps the artist out (see all_artists). Nothing
    is changed unless the website is an own site (crawl.is_own_site is the rule), the artist has a row, and
    neither they nor the site are on the removal list.
    """
    key = artist_key(username)
    if not website or not crawl.is_own_site(website):
        return False
    with _open(db) as conn:
        if _removed(conn, [key, *_link_keys([website])]):
            return False
        return conn.execute("UPDATE artists SET links = ? WHERE key = ?", (json.dumps([website]), key)).rowcount == 1


def browsing_pass(since: datetime, db: pathlib.Path | str | None = None) -> dict[str, tuple[int | None, int]]:
    """Where discovery's saved browsing pass has got to: tag -> (the offset of the tag's next page, or None when
    the API said it has no more; the pages of it that are read).

    A pass that began at or before `since` is forgotten first, tally and all, and the answer is empty: the run
    starts browsing afresh. Nothing else forgets a pass.
    """
    at = since.astimezone(UTC).isoformat(timespec="seconds")
    with _open_pass(db) as conn:
        if conn.execute("SELECT 1 FROM browsed WHERE began_at <= ?", (at,)).fetchone():
            conn.execute("DELETE FROM browsed")
            conn.execute("DELETE FROM counted")
        rows = conn.execute("SELECT tag, next_offset, pages FROM browsed").fetchall()
    return {tag: (next_offset, pages) for tag, next_offset, pages in rows}


def count_works(works: Iterable[tuple[str, str | None]], db: pathlib.Path | str | None = None) -> dict[str, int]:
    """Add one browse page to the pass's tally: each work as (deviation id, its author's handle), with None for
    the handle of a work that is to count for nobody. A work that is in the tally already is not counted again.

    Hashes are all that is written, of the id and of the handle. Returns how many works each of these authors has
    in the tally now, by the handle as it was given. ValueError for a handle that is not a plain one.
    """
    works = list(works)
    authors = {username: _hash(artist_key(username)) for _, username in works if username is not None}
    with _open_pass(db) as conn:
        conn.executemany(
            "INSERT OR IGNORE INTO counted VALUES (?, ?)",
            [(_hash(f"work:{work_id}"), username and authors[username]) for work_id, username in works],
        )
        count = "SELECT count(*) FROM counted WHERE author_hash = ?"
        return {username: conn.execute(count, (author,)).fetchone()[0] for username, author in authors.items()}


def note_browsed(tag: str, next_offset: int | None, db: pathlib.Path | str | None = None) -> None:
    """Note that one more page of a tag is read, and where the tag's next page is: None when it has no more."""
    with _open_pass(db) as conn:
        conn.execute(
            "INSERT INTO browsed VALUES (?, ?, 1, ?) "
            "ON CONFLICT(tag) DO UPDATE SET next_offset = excluded.next_offset, pages = pages + 1",
            (tag, next_offset, _now()),
        )


def tally(at_least: int, db: pathlib.Path | str | None = None) -> tuple[int, int, int, int, int, int, int, int]:
    """The saved browsing pass in eight numbers, in the order discovery prints them: pages read, works counted,
    mature works passed over, authors seen, and of the authors with `at_least` works: how many there are, how
    many are on the removal list, how many are not stored yet, and how many are stored."""
    with _open_pass(db) as conn:
        pages, works, mature, authors = conn.execute(
            "SELECT (SELECT coalesce(sum(pages), 0) FROM browsed), count(author_hash), "
            "count(*) - count(author_hash), count(DISTINCT author_hash) FROM counted"
        ).fetchone()
        enough = "SELECT author_hash FROM counted WHERE author_hash IS NOT NULL GROUP BY 1 HAVING count(*) >= ?"
        candidates = {author for (author,) in conn.execute(enough, (at_least,))}
        removed = candidates & {key_hash for (key_hash,) in conn.execute("SELECT key_hash FROM removed")}
        stored = (candidates - removed) & {_hash(key) for (key,) in conn.execute("SELECT key FROM artists")}
    waiting = len(candidates) - len(removed) - len(stored)
    return pages, works, mature, authors, len(candidates), len(removed), waiting, len(stored)


def all_artists(db: pathlib.Path | str | None = None) -> list[tuple[str, str, str]]:
    """Every stored artist who is not on the removal list, as (key, da_username, da_profile_url), in key order.

    For the collect stage. The removal list is asked again here, about the artist and about the site they link
    to, so a row that somehow outlived its removal is not handed on.
    """
    with _open(db) as conn:
        rows = conn.execute("SELECT key, da_username, da_profile_url, links FROM artists ORDER BY key").fetchall()
        return [
            (key, username, profile_url)
            for key, username, profile_url, links in rows
            if not _removed(conn, [key, *_link_keys(json.loads(links))])
        ]


def own_site_candidates(db: pathlib.Path | str | None = None) -> list[tuple[str, str]]:
    """The artists the crawl stage may visit, as (artist key, the front page of their own domain), in key order.

    An artist qualifies with exactly one link, and only when crawl.is_own_site accepts it. A registrable domain
    that any other artist's profile was seen to list, in a link of any shape, is a platform whatever any list
    says, so nobody who lists it qualifies. Nobody on the removal list qualifies either.

    ponytail: a profile is only seen when the collect stage reads it. An artist who has not had their turn yet
    has listed nothing, so a second listing can be missed until every stored artist has had one.
    """
    found = []
    with _open(db) as conn:
        rows = [(key, json.loads(links)) for key, links in conn.execute("SELECT key, links FROM artists ORDER BY key")]
        for key, links in rows:  # a row from before listings were kept still lists the domains of its links
            listing = [(_hash(f"site:{domain}"), _hash(key)) for domain in _link_domains(links)]
            conn.executemany("INSERT OR IGNORE INTO listed VALUES (?, ?)", listing)
        for key, links in rows:
            host = _host(links[0]) if len(links) == 1 and crawl.is_own_site(links[0]) else None
            domain = registrable_domain(host) if host else None
            if domain is None or _removed(conn, [key, *_host_keys(host or "")]):
                continue
            if not conn.execute(_OTHERS, (_hash(f"site:{domain}"), _hash(key))).fetchone():
                found.append((key, links[0]))
    return found
