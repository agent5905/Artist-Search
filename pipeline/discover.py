"""Find niche artists through DeviantArt's official API: every author with enough work under the niche tags.

    uv run python -m pipeline.discover --max-artists 200

An author with MIN_HITS or more non-mature works under the niche tags is stored, straight from the tally of the
browse pages (owner decision 7 in PROGRESS.md: DeviantArt is the primary source, an own site is optional). No
profile is read here. The collect stage reads each artist's profile on their turn, and every rule about what a
profile says is enforced there: the site of their own, a removed site, a domain several profiles list. Nothing
from a response is printed, logged or stored except the handle of an author who has works enough, the profile
URL built from it, and a hit count. Output is counts only.

Browsing survives a stop. After every page the store has where the tag's next page is, and the tally so far as
hashes that name nobody, so a run that is stopped, by the pacing rule or by a person, is carried on by the next
one, and an artist reaches the store on the page that gives them works enough. A saved pass is carried on from
for PASS_VALID after its first page. A run that finds an older one forgets it and browses afresh, and nothing
else does: a run that finds a finished pass younger than that has nothing to browse and makes no request.

Every request waits its turn in one place, Pace, which slows down when the API asks: for the rest of the run,
and for the run after it, which begins at the pace this one ended with. One run talks to the API at a time.
While a run is on, tell() keeps a file in the cache folder that says where it is.

A tag the API refuses with a 4XX is noted on standard error and passed over: the run goes on to the next tag,
exits non-zero, and the next run asks for that tag again. A 401 that outlasts a new token still stops the run.

Endpoints (deviantart.readme.io, read 2026-10-11). Each is documented as accepting a client_credentials token:
    POST /oauth2/token                          grant_type=client_credentials
    GET  /api/v1/oauth2/browse/tags             scope "browse"
"""

import argparse
import contextlib
import logging
import math
import os
import pathlib
import sys
import time
import traceback
from collections.abc import Callable, Iterator
from datetime import UTC, datetime, timedelta
from typing import Any

import httpx

from pipeline import config, store

if sys.platform == "win32":  # the lock a run holds on a host (see Pace) is the operating system's own
    import msvcrt
else:
    import fcntl

ENV_NAMES = ("DEVIANT_ART_ID", "DEVIANT_ART_SECRET")  # the client id and secret, as .env.example names them
TOKEN_URL = "https://www.deviantart.com/oauth2/token"
API_URL = "https://www.deviantart.com/api/v1/oauth2"
PROFILE_URL = "https://www.deviantart.com/{}"
# Tags that are specific to concept and character art for games. No broad one (digitalart, fantasy): it would
# let in anybody. The owner needs 2,000 public artists, and about one collected artist in three is held back
# later, so a run has to find some 3,500; the first six tags at 40 pages found about 400.
NICHE_TAGS = (
    *("conceptart", "characterdesign", "characterconcept", "creaturedesign", "gameart", "environmentconcept"),
    *("creatureconcept", "environmentdesign", "propdesign", "weapondesign", "mechdesign", "vehicledesign"),
    *("characterart", "monsterdesign", "armordesign", "costumedesign", "visualdevelopment", "gameconceptart"),
)
MIN_HITS = 3  # deviations under the niche tags before an author counts as a niche artist
PAGE_SIZE = 50  # the documented maximum for browse/tags
MAX_OFFSET = 50000  # the documented maximum for browse/tags
# ponytail: fixed browse depth, 18 tags x 120 pages x 50 = 108,000 deviations a pass at most: 2,160 requests,
# which is 72 minutes at the base pace. Raise it, and a saved pass is carried on deeper.
PAGES_PER_TAG = 120
PASS_VALID = timedelta(days=7)  # how long after its first page a saved browsing pass is carried on from
# The pacing rule's numbers (see Pace). The base is half the rate the API limited us at after most of a day.
BASE_INTERVAL = 2.0  # seconds between two requests to the API while nothing has asked for more
MAX_INTERVAL = 300.0  # seconds: the ceiling that refusals raise the interval to
EASE = 0.9  # what an answered request multiplies a raised interval by: down gradually, never below the base
IDLE_HALVING = 3600.0  # seconds with no request to a host that halve the interval the next run begins with
SERVER_RETRIES = 3  # times a 5XX answer is asked for again. The API docs suggest three for a 500.
MAX_AT_CEILING = 12  # waits at the ceiling, each of them for a 429, before the run stops
PATIENCE = MAX_AT_CEILING * MAX_INTERVAL  # the same in seconds, an hour: also the longest wait a host is given
_KEPT = "Everything stored is kept: run again later"


class Stop(Exception):
    """The run cannot go on. The message is safe to print: it never carries response data or credentials."""


def now() -> float:
    """The time a pace file is written with and read against, in seconds. The one clock for that, so that a
    test can replace it as it replaces time.sleep."""
    return time.time()


class Pace:
    """The interval between requests to one host: for the life of the client, and for the run after it,
    whichever process that is. The one place for the pacing rule.

    It starts at `base`. A 429 or a 5XX doubles it, up to MAX_INTERVAL, for every request that follows and not
    only for the one that is asked again: the API docs call a 429 "a request to slow down", and say that a
    client which keeps to a fixed rate after one stays limited for longer. Any other answer takes a tenth off
    it, and never below the base. A Retry-After that asks for longer than the interval is waited, once. How
    often an answer is asked for again is the caller's rule, not this one's.

    The stop is about time spent refused. Two kinds of wait are counted: the one for a 429 once the interval is
    at the ceiling, and whatever a Retry-After asks for. PATIENCE seconds of them stop the run: an hour of
    asking as slowly as we go, or as slowly as we were told to, has not helped by then. One thing starts the
    count again: the interval coming all the way back down to the base, which takes some seven answers to
    every refusal. An answer or two among the refusals does not, and neither does a 401. A Retry-After that
    asks for more than is left of that hour stops the run there and then.

    As a context manager it holds the host for this process and remembers the pace for the next run, in two
    files in the cache folder that hold numbers and nothing else:
      <host>.lock  held through the operating system for as long as the run lasts. A second run that finds it
                   held stops before it does anything. A process that died holds nothing, whatever it left.
      <host>.pace  the interval, and the time before which the next request should not go: written before
                   every request and after every answer. The next run begins with that interval, halved for
                   every IDLE_HALVING seconds that have passed since that time. A run that begins before it
                   waits what is left, and no less than one whole interval. No file means no run before this
                   one. A file that is not those two numbers means start slow: at the ceiling, after a wait
                   of the ceiling.
    """

    def __init__(self, base: float, host: str) -> None:
        self.base = self.interval = self.longest = base
        self.host = host  # a name of ours for the summary, such as "the API"
        self.requests = self.limited = 0
        # Seconds: waited so far, asked for before the next request, and counted towards the hour that stops a run.
        self.waited = self._asked = self._refused = 0.0
        self._lock: int | None = None
        self.tell: Callable[[], None] = lambda: None  # called before every request: see tell()

    def _file(self, kind: str) -> pathlib.Path:
        return config.CACHE_DIR / f"{self.host.replace(' ', '-')}.{kind}"

    def __enter__(self) -> "Pace":
        config.CACHE_DIR.mkdir(parents=True, exist_ok=True)
        lock = os.open(self._file("lock"), os.O_RDWR | os.O_CREAT)
        try:
            if sys.platform == "win32":
                msvcrt.locking(lock, msvcrt.LK_NBLCK, 1)
            else:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            os.close(lock)
            raise Stop(f"another run is talking to {self.host}, and one at a time is the rule") from None
        self._lock = lock
        try:
            interval, ready = map(float, self._file("pace").read_text(encoding="ascii").split())
            late = now() - ready
            if not (0 <= interval <= MAX_INTERVAL and -PATIENCE <= late < math.inf):
                raise ValueError("not numbers a run could have left")
        except FileNotFoundError:
            return self
        except (OSError, ValueError):  # unreadable, half written, or anything else: slow is the safe reading
            interval, late = MAX_INTERVAL, -MAX_INTERVAL
        self.interval = self.longest = max(self.base, interval * 0.5 ** (max(late, 0.0) / IDLE_HALVING))
        self._asked = max(self.interval, -late) if late < 0 else 0.0
        return self

    def __exit__(self, *exc: object) -> None:
        if self._lock is not None:
            os.close(self._lock)  # which lets the lock go, as the end of the process would
            self._lock = None

    def _save(self) -> None:
        """Write the pace down for the next run. It only remembers: a file that cannot be written ends no run."""
        with contextlib.suppress(OSError):
            ready = now() + max(self.interval, self._asked)
            self._file("pace").write_text(f"{self.interval!r} {ready!r}\n", encoding="ascii")

    def wait(self) -> None:
        """Call before every request to the host. It waits the interval; before the first request of a run,
        only what the run before it left to wait."""
        self.tell()
        seconds = max(self.interval, self._asked) if self.requests else self._asked
        if seconds:
            # Not injectable, on purpose: the tests replace time.sleep, so this is the line they prove.
            time.sleep(seconds)
            self.waited += seconds
        self.requests += 1
        self._asked = 0.0
        self._save()

    def ease(self) -> None:
        """Call after an answer that is neither a 429 nor a 5XX."""
        self.interval = max(self.base, self.interval * EASE)
        if self.interval == self.base:
            self._refused = 0.0
        self._save()

    def slow(self, response: httpx.Response | None = None) -> None:
        """Call after an answer that is a 429 or a 5XX, and with nothing after no answer at all."""
        limited = response is not None and response.status_code == 429
        self.limited += limited
        if self._refused >= PATIENCE:
            said = f"{PATIENCE / 60:.0f} minutes of waits, each {MAX_INTERVAL / 60:.0f} minutes or as long as it asked"
            raise Stop(f"rate limited: {self.host} kept refusing through {said}. {_KEPT}")
        # ponytail: Retry-After as a number of seconds only. Given as a date it is not read, and the doubled
        # interval is what is waited. Read the date too if a host is ever seen to send one.
        asked = response.headers.get("retry-after", "") if response is not None else ""
        # Up to nine ASCII digits are a number of seconds. Anything else says nothing: a sign, a word, 400 digits.
        self._asked = float(asked) if asked.isascii() and asked.isdigit() and len(asked) < 10 else 0.0
        self.interval = min(self.interval * 2, MAX_INTERVAL)
        self.longest = max(self.longest, self.interval)
        too_long = self._asked > PATIENCE - self._refused
        self._asked = min(self._asked, PATIENCE)  # what the next run is left to wait, when this one will not
        # What counts towards the hour: what a Retry-After asks for, and the wait for a 429 once the interval is
        # at the ceiling. A 5XX with no Retry-After counts for nothing: it is asked for again a few times at the
        # slower pace, and then it is the caller's to judge.
        self._refused += max(self._asked, MAX_INTERVAL * (limited and self.interval == MAX_INTERVAL))
        self._save()
        if too_long:
            raise Stop(f"rate limited: {self.host} asks for a wait of {asked} seconds, more than a run waits. {_KEPT}")

    def counts(self) -> dict[str, int]:
        """What the pace has done so far, for the summary and for tell(): counts, and whole seconds rounded up."""
        return {
            f"requests to {self.host}": self.requests,
            f"rate limit answers from {self.host}": self.limited,
            f"longest interval between requests to {self.host}, in seconds": math.ceil(self.longest),
            f"interval between requests to {self.host} now, in seconds": math.ceil(max(self.interval, self._asked)),
            f"seconds spent waiting between requests to {self.host}": round(self.waited),
        }


NOT_REACHED = "not reached (--max-artists)"
# The first eight are the browsing pass as it is saved, whichever runs made it, in the order store.tally() gives
# them. They add up: candidates = skipped (removed) + not reached + artists stored. The rest are this run's own.
_TALLY = (
    "pages fetched",
    "deviations counted",
    "mature skipped",
    "authors seen",
    f"candidates (>= {MIN_HITS} hits)",
    "skipped (removed)",
    NOT_REACHED,
    "artists stored",
)
_PACING = tuple(Pace(BASE_INTERVAL, "the API").counts())  # the labels of what the API's pace did
_COUNTS = (*_TALLY, *_PACING)


def tell(stage: str, counts: dict[str, int] | None = None) -> None:
    """Say where a running stage is, in a file in the cache folder that anybody may read while it runs, and that
    `scripts/gpu.py status` shows: the time, then the counts as the summary will print them. Counts only, never
    a handle, a title or an address. Called with no counts when the run is over, and the file goes: one that
    is there is a run in progress, or one that was killed, and the time in it says which.

    It only informs. A file that cannot be written or deleted never ends a run.
    """
    path = config.CACHE_DIR / f"{stage}.status"
    with contextlib.suppress(OSError):
        if counts is None:
            path.unlink(missing_ok=True)
        else:
            lines = [datetime.now(UTC).isoformat(timespec="seconds"), *(f"{name}: {n}" for name, n in counts.items())]
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text("\n".join(lines) + "\n", encoding="utf-8")


class _Api:
    """Sequential access to the API. Every request, the token request included, goes through _send."""

    def __init__(self, http: httpx.Client, client_id: str, client_secret: str) -> None:
        self._http = http
        self._credentials = {"grant_type": "client_credentials", "client_id": client_id, "client_secret": client_secret}
        self._token = ""
        self.pace = Pace(BASE_INTERVAL, "the API")

    def _send(self, method: str, url: str, **kwargs: Any) -> httpx.Response:
        """One request at the pace of the run, asked for again while the answer is one to back off from: a 429
        until the pace gives up, a 5XX SERVER_RETRIES times. So a 429 never comes back from here, and a 5XX
        that does has outlasted its retries. What comes back is the caller's to judge."""
        failed = 0
        while True:
            self.pace.wait()
            response = self._http.request(method, url, **kwargs)
            if response.status_code != 429 and response.status_code < 500:
                self.pace.ease()
                return response
            self.pace.slow(response)
            failed += response.status_code != 429
            if failed > SERVER_RETRIES:
                return response

    def _new_token(self) -> None:
        response = self._send("POST", TOKEN_URL, data=self._credentials)  # in the body, never in the URL
        token = response.json().get("access_token") if response.status_code == 200 else None
        if not isinstance(token, str) or not token:
            raise Stop(f"token request failed with HTTP {response.status_code}")
        self._token = token

    def _authorized_get(self, path: str, params: dict[str, str | int]) -> httpx.Response:
        return self._send("GET", API_URL + path, params=params, headers={"Authorization": f"Bearer {self._token}"})

    def get(self, path: str, **params: str | int) -> httpx.Response:
        if not self._token:
            self._new_token()
        response = self._authorized_get(path, params)
        if response.status_code == 401:  # tokens last an hour: get a new one, once
            self._new_token()
            response = self._authorized_get(path, params)
        return response


@contextlib.contextmanager
def _http_logs_off() -> Iterator[None]:
    """httpx logs every request URL at INFO, and a profile lookup's URL holds a handle. Not during a run."""
    loggers = [logging.getLogger(name) for name in ("httpx", "httpcore")]
    levels = [logger.level for logger in loggers]
    for logger in loggers:
        logger.setLevel(logging.CRITICAL + 1)
    try:
        yield
    finally:
        for logger, level in zip(loggers, levels, strict=True):
            logger.setLevel(level)


def _run(api: _Api, counts: dict[str, int], max_artists: int, db: pathlib.Path | str | None) -> int:
    """Browse, from where the saved pass has got to. Returns how many tags the API refused and the run passed over."""
    progress = store.browsing_pass(datetime.now(UTC) - PASS_VALID, db)
    # The pass as it is saved, first: a run that stops at its first request still says where the pass is.
    counts.update(zip(_TALLY, store.tally(MIN_HITS, db), strict=True))
    known = {key for key, _, _ in store.all_artists(db)}
    added = 0  # artists this run has stored who were not in the store before it
    refused = 0
    for tag in NICHE_TAGS:
        offset, pages = progress.get(tag, (0, 0))
        while offset is not None and pages < PAGES_PER_TAG:
            response = api.get("/browse/tags", tag=tag, offset=offset, limit=PAGE_SIZE)
            if 400 <= response.status_code < 500 and response.status_code != 401:  # a 429 never gets here
                # The API will not serve this tag, and that says nothing of the next one. The tag is ours, so it
                # can be named. The page is not noted as read: the next run asks for it again.
                print(f"skipped the tag {tag}: browse answered HTTP {response.status_code}", file=sys.stderr)
                refused += 1
                break
            if response.status_code != 200:
                raise Stop(f"browse failed with HTTP {response.status_code}")
            page = response.json()
            works: list[tuple[str, str | None]] = []  # deviation id, and its author's handle when the work counts
            for deviation in page.get("results") or []:
                if not isinstance(deviation, dict):  # one malformed entry is skipped; it does not end the run
                    continue
                deviation_id, author = deviation.get("deviationid"), deviation.get("author")
                username = author.get("username") if isinstance(author, dict) else None
                if not isinstance(deviation_id, str) or not isinstance(username, str):
                    continue
                if store.HANDLE.fullmatch(username):
                    # Only an explicit "not mature" counts. Any other work is in the tally for nobody.
                    works.append((deviation_id, username if deviation.get("is_mature") is False else None))
            # The tally takes the page, as hashes, and says how many works each of its authors has now. Whoever
            # has enough is stored there and then, under the handle this page spells: none is written down sooner.
            capped = False
            for username, hits in store.count_works(works, db).items():
                key = store.artist_key(username)
                if hits < MIN_HITS:
                    continue
                if key not in known and added >= max_artists:
                    capped = True  # the page is not noted as read: the next run reads it again and stores the rest
                    break
                # The store has the last word (hard rule 7): somebody on the removal list is not stored again.
                if store.upsert_artist(username, PROFILE_URL.format(username), None, hits, db) and key not in known:
                    known.add(key)
                    added += 1
            if not capped:
                # The next page is where the response says it is, never our own arithmetic, and only forwards
                # inside the documented range.
                next_offset = page.get("next_offset")
                more = page.get("has_more") is True and type(next_offset) is int and offset < next_offset <= MAX_OFFSET
                offset, pages = next_offset if more else None, pages + 1
                store.note_browsed(tag, offset, db)
            counts.update(zip(_TALLY, store.tally(MIN_HITS, db), strict=True))
            if capped:
                return refused
    return refused


def discover(
    client_id: str,
    client_secret: str,
    *,
    max_artists: int,
    db: pathlib.Path | str | None = None,
    transport: httpx.BaseTransport | None = None,
) -> int:
    """Run one discovery pass, or carry on the one that is saved, and print its counts. Returns the exit code."""
    counts = dict.fromkeys(_COUNTS, 0)
    status = 0
    try:
        # A redirect is never followed: it would carry the credentials or the token to wherever it points.
        # httpx adds Accept-Encoding to every request beside this user agent: the API docs ask for both.
        with (
            _http_logs_off(),
            httpx.Client(
                headers={"User-Agent": config.USER_AGENT}, transport=transport, timeout=30, follow_redirects=False
            ) as http,
        ):
            api = _Api(http, client_id, client_secret)
            api.pace.tell = lambda: tell("discover", counts | api.pace.counts())
            # Another run that has the API is found out here, before anything of its is touched: not the saved
            # pass, and not the file that says where it is.
            with api.pace:
                try:
                    status = int(_run(api, counts, max_artists, db) > 0)
                finally:
                    tell("discover")  # first: whatever follows, no file is left that says a run is on
                    counts.update(api.pace.counts())
    except Stop as stop:
        print(f"stopped: {stop}", file=sys.stderr)
        status = 1
    except Exception as exc:
        # Deliberately broad, and no traceback: an exception message can quote response data or a request URL.
        lines = [frame.lineno for frame in traceback.extract_tb(exc.__traceback__) if frame.filename == __file__]
        print(f"stopped: {type(exc).__name__} at discover.py line {lines[-1] if lines else '?'}", file=sys.stderr)
        status = 1
    for name, count in counts.items():
        print(f"{name}: {count}")
    return status


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Find niche artists through the DeviantArt API (handles only).")
    parser.add_argument("--max-artists", type=int, default=200, help="stop after storing this many new artists")
    args = parser.parse_args(argv)
    try:
        env = config.load_env()
    except FileNotFoundError:
        env = {}
    missing = [name for name in ENV_NAMES if not env.get(name)]
    if missing:
        print(f"Missing in .env: {', '.join(missing)} (see .env.example)", file=sys.stderr)
        return 1
    client_id, client_secret = (env[name] for name in ENV_NAMES)
    return discover(client_id, client_secret, max_artists=args.max_artists)


if __name__ == "__main__":
    sys.exit(main())
