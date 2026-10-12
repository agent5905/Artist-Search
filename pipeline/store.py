"""The local SQLite store: discovered artists, and the removal list that keeps removed artists out (hard rule 7).

A removal identifier is 'da:<lowercased handle>' for an artist or 'site:<registrable domain>' for a site, and
the list holds only the sha256 of each. normalize() turns what an operator might type into one, or raises.
The removal list of record lives in the remote database; this file is a cache, and import_removed() refills it.
Every function takes an optional `db` path; the default is config.LOCAL_DB.
"""

import contextlib
import hashlib
import json
import pathlib
import re
import sqlite3
from collections import Counter
from collections.abc import Iterable, Iterator
from datetime import UTC, datetime
from urllib.parse import urlsplit

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
"""

HANDLE = re.compile(r"[A-Za-z0-9_-]{1,40}")  # a DeviantArt handle. Also what makes one safe to put in a URL path.
# ponytail: DeviantArt's own top-level routes, written from its URL scheme. deviantart.com/tag/x is not the profile
# of an artist called "tag". Ceiling: a route missing here is read as a handle; mark_removed() returns how many
# artists it deleted, so a removal that matched nobody is visible.
_DA_ROUTES = frozenset(
    """
    about account api art browse chat checkout comments core core-membership daily-deviations deviation deviations
    developers download dreamup forum gallery groups help home join journal journals legal messages morelikethis
    newest notifications oauth2 popular posts print prints rss search settings shop stash status studio submit
    subscriptions tag tags team timeline topic topics undiscovered users view watch
    """.split()
)
# A public DNS name: letter-digit-hyphen labels and an alphabetic (or punycode) top level. No IP address, no
# localhost, no percent sign, backslash or other stray character.
_HOSTNAME = re.compile(r"(?=.{4,253}$)(?:[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\.)+(?:[a-z]{2,63}|xn--[a-z0-9-]{1,59})")
_DOTS = "." + "".join(map(chr, (0x3002, 0xFF0E, 0xFF61)))  # the full stop, and the three characters IDNA reads as one
# ponytail: the standard library's codec is IDNA 2003. It respells these five characters where browsers keep
# them (sharp s becomes "ss"), so the result could name somebody else's site. A host that has one is dropped.
# The idna package (IDNA 2008) is the upgrade if a German or Greek artist's site is lost this way.
_IDNA_2003_ONLY = "".join(map(chr, (0xDF, 0x1E9E, 0x3C2, 0x200C, 0x200D)))  # sharp s twice, final sigma, two joiners
_SHA256 = re.compile(r"[0-9a-f]{64}")


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
    handle = urlsplit(text if "://" in text else f"//{text}").path.strip("/").split("/")[0]
    return ("da", handle.lower()) if HANDLE.fullmatch(handle) else None


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
    if named is None or kind not in ("", named[0]) or (named[0] == "da" and named[1] in _DA_ROUTES):
        raise ValueError("not an artist handle, a DeviantArt profile URL or a site address")  # never echoes the input
    return named


def normalize(identifier: str) -> str:
    """Any plain spelling of an artist or a site as 'da:<handle>' or 'site:<registrable domain>'.

    Takes a handle, a DeviantArt profile URL, a hostname or a site URL, with or without a 'da:' or 'site:' prefix.
    Raises ValueError for anything else, so a removal can never silently do nothing (hard rule 7). That includes
    what discovery can never store: a host on a platform (artstation.com, someone.tumblr.com), and a DeviantArt
    URL that is a site route (deviantart.com/tag/x) and not a profile.
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
    """The removal keys that cover a canonical host: its own and each parent domain's.

    A removal made here is always the registrable domain, which is one of these. A hash imported from the
    database may have been made from a longer host (www.janedoe.art), and that is one of these too.
    """
    labels = host.split(".")
    return [f"site:{'.'.join(labels[start:])}" for start in range(len(labels) - 1)]


def _link_keys(links: list[str]) -> set[str]:
    """Every removal key that covers one of an artist's links. A link with no public host covers nothing."""
    return {key for link in links if (host := _host(link)) for key in _host_keys(host)}


def _hash(key: str) -> str:
    return hashlib.sha256(key.encode()).hexdigest()


def _now() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds")


@contextlib.contextmanager
def _open(db: pathlib.Path | str | None) -> Iterator[sqlite3.Connection]:
    """One transaction on the store: committed on success, rolled back on error, always closed."""
    path = pathlib.Path(db or config.LOCAL_DB)
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    try:
        conn.execute("PRAGMA secure_delete = ON")  # a removed artist's row is zeroed in the file, not just unlinked
        conn.executescript(_SCHEMA)
        with conn:
            yield conn
    finally:
        conn.close()


def _removed(conn: sqlite3.Connection, keys: Iterable[str]) -> bool:
    """True when any of these normalized keys is on the removal list."""
    query = "SELECT 1 FROM removed WHERE key_hash = ?"
    return any(conn.execute(query, (_hash(key),)).fetchone() for key in keys)


def _remove(conn: sqlite3.Connection, key: str) -> int:
    """Put a key on the removal list with everything stored that it reaches. Returns how many artists that deleted.

    Removing an artist also removes the registrable domain of each link in their row, and removing a site also
    removes every artist who links to it. So a renamed or second account that lists the same site stays out,
    and so does the first account under a new site.
    """
    pending, done, deleted = [key], set(), 0
    while pending:
        key = pending.pop()
        if key in done:
            continue
        done.add(key)
        row = conn.execute("SELECT links FROM artists WHERE key = ?", (key,)).fetchone()
        conn.execute("INSERT OR IGNORE INTO removed VALUES (?, ?)", (_hash(key), _now()))
        conn.execute("DELETE FROM artists WHERE key = ?", (key,))
        if row:
            deleted += 1
            domains = (registrable_domain(host) for link in json.loads(row[0]) if (host := _host(link)))
            pending += [f"site:{domain}" for domain in domains if domain]  # never a platform: that has no domain
        if key.startswith("site:"):
            # ponytail: scans every artist per removed site. Fine at a few thousand rows; index link hosts if it grows.
            rows = conn.execute("SELECT key, links FROM artists").fetchall()
            pending += [artist for artist, links in rows if key in _link_keys(json.loads(links))]
    return deleted


def is_removed(identifier: str, db: pathlib.Path | str | None = None) -> bool:
    """True when the artist, the site or a parent domain of the site is on the removal list.

    ValueError when the identifier names neither an artist nor a host. A host on a platform is simply not removed.
    """
    kind, value = _parse(identifier)
    with _open(db) as conn:
        return _removed(conn, [f"da:{value}"] if kind == "da" else _host_keys(value))


def mark_removed(identifier: str, db: pathlib.Path | str | None = None) -> int:
    """Put an artist or a site on the removal list, with everything stored that it reaches. Safe to repeat.

    Returns how many stored artists it deleted, so a removal that matched nobody can be seen. ValueError when
    the identifier names nothing discovery could store (see normalize): a removal never silently does nothing.
    """
    key = normalize(identifier)
    with _open(db) as conn:
        return _remove(conn, key)


def import_removed(key_hashes: Iterable[str], db: pathlib.Path | str | None = None) -> int:
    """Take removals from the list of record: sha256 hex digests of 'da:<handle>' and 'site:<host>' keys.

    Stores them, then removes every stored artist whose own key, site or parent domain matches one, just as
    mark_removed() would. Returns how many artists it deleted. This file can be deleted and refilled; a
    removal cannot be lost that way.
    """
    hashes = [key_hash.strip().lower() for key_hash in key_hashes]
    if not all(_SHA256.fullmatch(key_hash) for key_hash in hashes):
        raise ValueError("not a sha256 hex digest")
    deleted = 0
    with _open(db) as conn:
        conn.executemany("INSERT OR IGNORE INTO removed VALUES (?, ?)", [(key_hash, _now()) for key_hash in hashes])
        for artist, links in conn.execute("SELECT key, links FROM artists").fetchall():
            for key in (artist, *sorted(_link_keys(json.loads(links)))):
                if _removed(conn, [key]):
                    deleted += _remove(conn, key)
    return deleted


def bar_if_site_removed(username: str, host: str, db: pathlib.Path | str | None = None) -> bool:
    """For an artist whose profile points at `host`, in a link of any shape: are they to be left out?

    True when the host or a parent domain is on the removal list. The artist then goes on the list too (and any
    row they had is deleted), so they stay out whatever site they list next.
    """
    key = artist_key(username)
    with _open(db) as conn:
        if not _removed(conn, _host_keys(host)):
            return False
        _remove(conn, key)
    return True


def upsert_artist(
    username: str, profile_url: str, links: list[str], niche_hits: int, db: pathlib.Path | str | None = None
) -> bool:
    """Insert one artist, or refresh the row already there. Safe to repeat.

    Returns False and stores nothing when the artist is on the removal list. An artist who links to a removed
    site is put on the list too (and any row they had is deleted). ValueError for a username that is not a
    plain handle, or a link that has no public hostname.
    """
    key = artist_key(username)
    if any(_host(link) is None for link in links):
        raise ValueError("a link has no public hostname")
    with _open(db) as conn:
        if _removed(conn, _link_keys(links)):
            _remove(conn, key)
        if _removed(conn, [key]):
            return False
        conn.execute(
            "INSERT INTO artists VALUES (?, ?, ?, ?, ?, ?) "
            "ON CONFLICT(key) DO UPDATE SET da_username = excluded.da_username, "
            "da_profile_url = excluded.da_profile_url, links = excluded.links, niche_hits = excluded.niche_hits",
            (key, username, profile_url, json.dumps(links), niche_hits, _now()),
        )
    return True


def own_site_candidates(db: pathlib.Path | str | None = None) -> list[tuple[str, str]]:
    """The artists the crawl stage may visit, as (artist key, the front page of their own domain), in key order.

    An artist qualifies with exactly one link, and only when crawl.is_own_site accepts it. A registrable domain
    listed by more than one artist is a platform, whatever any list says, so nobody who lists it qualifies.
    Nobody on the removal list qualifies either, even if a row of theirs were still here.
    """
    with _open(db) as conn:
        rows = [(key, json.loads(links)) for key, links in conn.execute("SELECT key, links FROM artists ORDER BY key")]
        rows = [(key, links) for key, links in rows if not _removed(conn, [key, *_link_keys(links)])]
    hosts = {key: [host for link in links if (host := _host(link))] for key, links in rows}
    domains = {key: {domain for host in hosts[key] if (domain := registrable_domain(host))} for key, _ in rows}
    listed = Counter(domain for found in domains.values() for domain in found)  # each artist counts once per domain
    return [
        (key, links[0])
        for key, links in rows
        if len(links) == 1 and crawl.is_own_site(links[0]) and all(listed[domain] == 1 for domain in domains[key])
    ]
