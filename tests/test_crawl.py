"""Proves the polite-crawling rules against a fake web: no network, no real sleeping.

Spec section 2 (hard rules 4 and 5) and owner decision 2 (honor AI opt-outs everywhere).
"""

import gzip
import inspect
import itertools
import random
import re
import socket
import sys
import threading
import time
import zlib
from collections import defaultdict
from pathlib import Path

import httpcore
import httpx
import pytest
import tldextract

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from pipeline import config, crawl  # noqa: E402
from pipeline.crawl import (  # noqa: E402
    AI_CRAWLER_TOKENS,
    GZIP_MEMBERS_MAX,
    IMAGE_MAX_BYTES,
    MAX_CRAWL_DELAY,
    MAX_REDIRECTS,
    MAX_SECONDS_PER_RESPONSE,
    META_TAG_MAX,
    PAGE_MAX_BYTES,
    PLATFORM_DOMAINS,
    PLATFORM_NAMES,
    SITE_REQUEST_BUDGET,
    VERIFY_REQUEST_BUDGET,
    FetchResult,
    PoliteFetcher,
    Site,
    is_own_site,
)

SITE = "https://janedoe.art"
ROBOTS = f"{SITE}/robots.txt"
TDM = f"{SITE}/.well-known/tdmrep.json"
MIN = config.MIN_SECONDS_PER_HOST
NOAI = '<meta name="robots" content="noai">'
PROFILE = "https://www.deviantart.com/JaneDoe"  # the account that claims janedoe.art
LINK_BACK = f'<a href="{PROFILE}">my DeviantArt</a>'  # what a front page must hold to prove that claim
FRONT = f"{SITE}/"
PROOF = [ROBOTS, TDM, FRONT]  # all that proving a site may cost
PSL = tldextract.TLDExtract(suffix_list_urls=(), cache_dir=None, include_psl_private_domains=True)


@pytest.fixture(autouse=True)
def fresh_site_state():
    """The per-site clocks and budgets are shared by every fetcher in the process: each test starts them afresh."""
    crawl._LAST.clear()
    crawl._DELAY.clear()
    crawl._SPENT.clear()
    crawl._OWNERS.clear()


class Clock:
    """A fake clock. Unless told otherwise, sleeping moves the hands and nothing else does."""

    def __init__(self, tick=0.0, sleeping=True):
        self.now = 1000.0
        self.sleeps = []
        self.tick = tick  # how far each reading moves the hands, the way a real clock moves during a busy wait
        self.sleeping = sleeping

    def __call__(self):
        self.now += self.tick
        return self.now

    def sleep(self, seconds):
        assert seconds > 0
        self.sleeps.append(seconds)
        if self.sleeping:
            self.now += seconds


class Web:
    """A fake web. `routes` maps a URL to a function of the request, or to an exception to raise.

    A URL with no route answers 404, so a host with no routes has no robots.txt and no tdmrep.json.
    Every name resolves to a public address unless `dns` says otherwise.
    """

    def __init__(self, routes=None, clock=None):
        self.routes = dict(routes or {})
        self.clock = clock or Clock()
        self.dns = {}  # host -> its addresses, or an exception to raise
        self.lookups = []
        self.requests = []
        self.urls = []  # what each request was for: its URL with the name back in place of the pinned address
        self.pins = []  # (name, address connected to, TLS server name) of each request
        self.times = []  # the fake time at which each request reached the server
        self.fetcher = self.new_fetcher()

    def new_fetcher(self):
        transport = httpx.MockTransport(self._serve)
        return PoliteFetcher(transport=transport, clock=self.clock, sleep=self.clock.sleep, resolver=self._resolve)

    def _resolve(self, host):
        self.lookups.append(host)
        answer = self.dns.get(host, ["93.184.216.34"])
        if isinstance(answer, Exception):
            raise answer
        return answer

    def _serve(self, request):
        name = request.headers["host"]
        url = str(request.url.copy_with(host=name))
        self.requests.append(request)
        self.urls.append(url)
        self.pins.append((name, request.url.host, request.extensions.get("sni_hostname")))
        self.times.append(self.clock.now)
        route = self.routes.get(url, reply(404))
        if isinstance(route, Exception):
            raise route
        return route(request)

    @property
    def gaps(self):
        return [later - earlier for earlier, later in itertools.pairwise(self.times)]

    def open(self, root=FRONT, profile=PROFILE, fetcher=None):
        """The one public way in: ask for the site to be opened on the strength of its front page."""
        return (fetcher or self.fetcher).open_site(root, profile)

    def site(self, root=SITE, fetcher=None):
        """A handle on a site whose front page has already proved the claim.

        Most tests here are about what happens after that proof, so they start from it. The proof itself is
        tested through open_site alone, in section 5.
        """
        host = httpx.URL(root).raw_host.decode("ascii")
        crawl._OWNERS.setdefault(PSL.extract_str(host).top_domain_under_public_suffix, "janedoe")
        return Site(fetcher or self.fetcher, root, PROFILE)

    def fetch(self, url, fetcher=None, **kwargs):
        """Fetch a URL of a proven site. A URL whose site could never be opened gets the refusal open_site gives."""
        try:
            host = httpx.URL(url).raw_host.decode("ascii")
            root = f"https://{PSL.extract_str(host).top_domain_under_public_suffix or host}/"
        except Exception:
            root = url
        if not is_own_site(root):
            return self.open(root, fetcher=fetcher)
        return self.site(root, fetcher).fetch(url, **kwargs)

    def get(self, path, **kwargs):
        return self.fetch(SITE + path, **kwargs)


def short(value):
    """A test id that fits in an environment variable and on any console."""
    return ascii(value)[:60]


def reply(status=200, body=b"", headers=None):
    return lambda request: httpx.Response(status, headers=headers, content=body)


def raw_reply(body, headers=None):
    """A body sent exactly as given, in one piece and with no Content-Length: httpx does not decode it for us."""
    return lambda request: httpx.Response(200, headers=headers, content=iter([body]))


def html(body=f"<html><head><title>Jane Doe</title></head><body>art {LINK_BACK}</body></html>", headers=None):
    return reply(200, body, {"content-type": "text/html; charset=utf-8", **(headers or {})})


def redirect(location, status=301):
    return reply(status, headers={"location": location})


def stream(total, produced, chunk=65_536, headers=None):
    """A body of `total` bytes with no Content-Length. `produced` collects the size of each chunk actually read."""

    def body():
        for start in range(0, total, chunk):
            produced.append(min(chunk, total - start))
            yield b"\0" * produced[-1]

    return lambda request: httpx.Response(200, headers=headers, content=body())


def files(origin):
    """The two policy files asked of an origin before its first page."""
    return [f"{origin}/robots.txt", f"{origin}/.well-known/tdmrep.json"]


# --- 1. Identification ---------------------------------------------------------------------------------------------


def test_every_request_sends_our_user_agent():
    web = Web({f"{SITE}/a": redirect("/b"), f"{SITE}/b": html(), f"{SITE}/pic.png": reply(200, b"png")})
    assert web.get("/a").ok
    assert web.get("/pic.png", image=True).ok
    assert web.urls == [ROBOTS, TDM, f"{SITE}/a", f"{SITE}/b", f"{SITE}/pic.png"]
    assert all(request.headers["user-agent"] == config.USER_AGENT for request in web.requests)
    assert config.ROBOTS_TOKEN in config.USER_AGENT and config.CONTACT_URL in config.USER_AGENT


# --- 2. robots.txt -------------------------------------------------------------------------------------------------

ROBOTS_CASES = [
    # Rules for `*` and for our own token are obeyed.
    ("User-agent: *\nDisallow: /private", "/private/a", "robots"),
    ("User-agent: *\nDisallow: /private", "/public", None),
    ("User-agent: *\nDisallow: /", "/", "robots"),
    ("User-agent: ArtistSearchBot\nDisallow: /private", "/private/a", "robots"),
    ("User-agent: ArtistSearchBot\nDisallow: /private", "/public", None),
    ("User-agent: artistsearchbot\nDisallow: /x", "/x", "robots"),
    ("User-agent: ArtistSearchBot/0.1\nDisallow: /", "/a", "robots"),
    ("User-agent: ArtistSearchBot 0.1\nDisallow: /", "/a", "robots"),
    ("User-agent: ArtistSearchBot*\nDisallow: /", "/a", "robots"),
    ("USER-AGENT: *\nDISALLOW: /x", "/x", "robots"),
    ("User-agent: *\nDisallow: /Art", "/art", None),  # paths keep their case
    ("User-agent: a\nUser-agent: ArtistSearchBot\nUser-agent: b\nDisallow: /x", "/x", "robots"),
    ("User-agent: *\nDisallow: /x\n\nUser-agent: *\nDisallow: /y", "/x", "robots"),
    ("User-agent: *\nDisallow: /x\n\nUser-agent: *\nDisallow: /y", "/y", "robots"),
    ("\ufeffUser-agent: *\r\nDisallow: /x\r\n", "/x", "robots"),
    ("\ufeffUser-agent: Googlebot\r\nDisallow: /\r\n", "/a", None),  # a byte-order mark does not hide the first line
    ("User-agent: *\n# Disallow: /\nDisallow: /x # not /y", "/x", "robots"),
    ("User-agent: *\n# Disallow: /\nDisallow: /x # not /y", "/y", None),
    ("User-agent: *\nDisallow:", "/anything", None),
    ("User-agent: Googlebot\nDisallow: /", "/a", None),
    ("User-agent: *\nDisallow: /a\n\nUser-agent: Googlebot\nDisallow: /", "/b", None),  # a new group starts clean
    ("<html><body>No such page</body></html>", "/a", None),
    # A robots.txt in UTF-16 or UTF-32 is still read, with or without a byte-order mark.
    ("User-agent: *\nDisallow: /x".encode("utf-16"), "/x", "robots"),
    ("User-agent: *\nDisallow: /x".encode("utf-16-le"), "/x", "robots"),
    ("User-agent: *\nDisallow: /x".encode("utf-16-be"), "/x", "robots"),
    ("User-agent: *\nDisallow: /x".encode("utf-32"), "/x", "robots"),
    ("User-agent: GPTBot\nDisallow: /".encode("utf-16"), "/a", "ai_opt_out"),
    ("User-agent: *\nDisallow: /x".encode("utf-16"), "/y", None),
    # A rule with no User-agent line above it is read as meant for everyone.
    ("Disallow: /", "/a", "robots"),
    ("Disallow: /private\nUser-agent: Googlebot\nDisallow: /", "/a", None),
    # An empty group written for us is an invitation.
    ("User-agent: ArtistSearchBot\nDisallow:\n\nUser-agent: *\nDisallow: /", "/a", None),
    # A group written for us replaces the `*` group (RFC 9309), in both directions.
    ("User-agent: *\nDisallow: /\nUser-agent: ArtistSearchBot\nAllow: /", "/a", None),
    ("User-agent: *\nAllow: /\nUser-agent: ArtistSearchBot\nDisallow: /", "/a", "robots"),
    # The longest match wins, not the first one: urllib.robotparser gets the first of these wrong.
    ("User-agent: *\nAllow: /\nDisallow: /private/", "/private/a", "robots"),
    ("User-agent: *\nDisallow: /art\nAllow: /art/public", "/art/public/1", None),
    ("User-agent: *\nDisallow: /art\nAllow: /art/public", "/art/secret", "robots"),
    # Allow wins a tie, whichever line comes first.
    ("User-agent: *\nDisallow: /art\nAllow: /art", "/art/1", None),
    ("User-agent: *\nAllow: /art\nDisallow: /art", "/art/1", None),
    # The longest match is counted in octets with everything outside ASCII percent-encoded, not in characters:
    # the Disallow below is 8 characters, 20 bytes of UTF-8 and 56 octets as the RFC counts them.
    ("User-agent: *\nAllow: /gallery/\nDisallow: /*プライベート", "/gallery/プライベート/1.png", "robots"),
    (
        "User-agent: *\nAllow: /gallery/\nDisallow: /*%E3%83%97%E3%83%A9%E3%82%A4%E3%83%99%E3%83%BC%E3%83%88",
        "/gallery/プライベート/1.png",
        "robots",
    ),
    (
        "User-agent: *\nAllow: /gallery/" + "*" * 20 + "\nDisallow: /*プライベート",
        "/gallery/プライベート/1.png",
        "robots",
    ),
    ("User-agent: *\nAllow: /gallery/プライベート\nDisallow: /*プライベート", "/gallery/プライベート/1.png", None),
    ("User-agent: GPTBot\nAllow: /gallery/\nDisallow: /*プライベート", "/gallery/プライベート/1.png", "ai_opt_out"),
    # Wildcards.
    ("User-agent: *\nDisallow: /*.jpg$", "/img/a.jpg", "robots"),
    ("User-agent: *\nDisallow: /*.jpg$", "/img/a.jpg.html", None),
    ("User-agent: *\nDisallow: /*?", "/gallery?page=2", "robots"),
    ("User-agent: *\nDisallow: /*?", "/gallery", None),
    ("User-agent: *\nDisallow: /a?b", "/a?b=1", "robots"),
    ("User-agent: *\nDisallow: /a?b", "/axb", None),
    # Percent-encoding and dot segments are not a way round a rule.
    ("User-agent: *\nDisallow: /private/", "/%70rivate/a", "robots"),
    ("User-agent: *\nDisallow: /caf%C3%A9", "/café", "robots"),
    ("User-agent: *\nDisallow: /private", "/public/../private/a", "robots"),
    # 4. An AI crawler shut out of the whole site closes the whole host to us.
    ("User-agent: gptbot\nDisallow: /", "/a", "ai_opt_out"),
    ("User-agent: GPTBot\nDisallow: /*", "/a", "ai_opt_out"),
    ("User-agent: GPTBot/1.2\nDisallow: /", "/a", "ai_opt_out"),
    ("User-agent: Googlebot\nUser-agent: CCBot\nDisallow: /", "/a", "ai_opt_out"),
    ("User-agent: GPTBot\nDisallow: /\nAllow: /press/", "/press/kit", "ai_opt_out"),
    ("User-agent: GPTBot\nDisallow: /\nUser-agent: ArtistSearchBot\nAllow: /", "/a", "ai_opt_out"),
    # Shut out of one path closes that path to us, and only that path.
    ("User-agent: ClaudeBot\nDisallow: /gallery/", "/gallery/1", "ai_opt_out"),
    ("User-agent: ClaudeBot\nDisallow: /gallery/", "/about", None),
    # Naming an AI crawler without shutting it out is not an opt-out.
    ("User-agent: GPTBot\nDisallow:", "/a", None),
    ("User-agent: GPTBot\nAllow: /", "/a", None),
    # A Content-Signal or Content-Usage line that reserves AI use closes the host, wherever it stands.
    ("User-agent: *\nContent-Signal: search=yes, ai-train=no\nAllow: /", "/a", "ai_opt_out"),
    ("User-agent: *\nContent-Signal: ai-input=no", "/a", "ai_opt_out"),
    ("User-agent: Googlebot\ncontent-signal: AI-TRAIN = NO", "/a", "ai_opt_out"),
    ("Content-Usage: train-ai=n", "/a", "ai_opt_out"),
    ("User-agent: *\nContent-Usage: /gallery/ train-ai=n", "/about", "ai_opt_out"),
    ("User-agent: *\nContent-Signal: search=yes, ai-train=yes, ai-input=yes\nAllow: /", "/a", None),
    ("User-agent: *\nContent-Usage: train-ai=y", "/a", None),
    ("User-agent: *\n# Content-Signal: ai-train=no\nAllow: /", "/a", None),
]


@pytest.mark.parametrize(("robots_txt", "path", "skip"), ROBOTS_CASES)
def test_robots_rules(robots_txt, path, skip):
    web = Web({ROBOTS: reply(200, robots_txt)})
    target = httpx.URL(SITE + path)
    web.routes[str(target)] = html()
    result = web.get(path)
    assert result.skip == skip
    assert web.urls == ([ROBOTS] if skip else [ROBOTS, TDM, str(target)])  # a refused page is never requested


def test_robots_and_tdmrep_are_fetched_once_per_origin_and_before_anything_else():
    web = Web({ROBOTS: reply(200, "User-agent: *\nDisallow: /private"), f"{SITE}/a": html(), f"{SITE}/b": html()})
    for path in ("/a", "/private", "/b", "/a", "/private/x"):
        web.get(path)
    assert web.urls == [ROBOTS, TDM, f"{SITE}/a", f"{SITE}/b", f"{SITE}/a"]


def test_robots_is_read_per_origin_so_the_https_rules_hold_after_an_http_fetch():
    web = Web({"http://janedoe.art/a": html(), ROBOTS: reply(200, "User-agent: *\nDisallow: /"), f"{SITE}/a": html()})
    assert web.fetch("http://janedoe.art/a").ok
    assert web.get("/a").skip == "robots"
    assert web.urls == [*files("http://janedoe.art"), "http://janedoe.art/a", ROBOTS]


def test_a_noindex_header_on_robots_itself_does_not_close_the_host():
    robots = reply(200, "User-agent: *\nDisallow: /private", {"x-robots-tag": "noindex", "tdm-reservation": "1"})
    web = Web({ROBOTS: robots, f"{SITE}/a": html()})
    assert web.get("/a").ok
    assert web.get("/private").skip == "robots"


def test_each_host_has_its_own_robots():
    web = Web({ROBOTS: reply(200, "User-agent: *\nDisallow: /"), "https://other-artist.com/a": html()})
    assert web.get("/a").skip == "robots"
    assert web.fetch("https://other-artist.com/a").ok
    assert web.urls == [ROBOTS, *files("https://other-artist.com"), "https://other-artist.com/a"]


@pytest.mark.parametrize("status", [404, 410])
def test_missing_robots_means_allowed(status):
    web = Web({ROBOTS: reply(status), f"{SITE}/a": html()})
    assert web.get("/a").ok


@pytest.mark.parametrize(
    "robots",
    [
        *(reply(status) for status in (400, 401, 402, 403, 405, 406, 408, 409, 418, 421, 422, 426, 429, 451)),
        *(reply(status) for status in (500, 501, 502, 503, 504, 507, 520)),
        *(reply(status) for status in (300, 301, 302, 303, 304, 307, 308)),  # a redirect that names no Location
        httpx.ConnectError("no route"),
        httpx.ReadTimeout("slow"),
        OSError("socket closed"),
        redirect("https://elsewhere.net/robots.txt"),
        redirect("/"),  # robots.txt may only move to another /robots.txt
        redirect("/private/rules.txt"),
        reply(200, b"User-agent: *\nAllow: /\n" + b"#" * 600_000),  # too big to trust a part of it
    ],
)
def test_unreachable_robots_closes_the_host(robots):
    web = Web({ROBOTS: robots, f"{SITE}/a": html(), f"{SITE}/b": html(), f"{SITE}/": html()})
    web.routes[f"{SITE}/private/rules.txt"] = reply(200, "User-agent: *\nAllow: /")
    assert web.get("/a").skip == "robots"
    assert web.get("/b").skip == "robots"
    assert web.urls == [ROBOTS]  # no page was requested, and the failure is remembered, not retried


def test_a_redirecting_robots_is_followed_to_another_robots_txt_on_the_site():
    web = Web(
        {
            "http://janedoe.art/robots.txt": redirect("https://www.janedoe.art/robots.txt"),
            "https://www.janedoe.art/robots.txt": reply(200, "User-agent: *\nDisallow: /private"),
            "http://janedoe.art/private": html(),
            "http://janedoe.art/public": html(),
        }
    )
    assert web.fetch("http://janedoe.art/private").skip == "robots"
    assert web.fetch("http://janedoe.art/public").ok
    assert web.urls == [
        "http://janedoe.art/robots.txt",
        "https://www.janedoe.art/robots.txt",
        "http://janedoe.art/.well-known/tdmrep.json",
        "http://janedoe.art/public",
    ]


def test_a_larger_crawl_delay_raises_that_hosts_delay():
    web = Web({ROBOTS: reply(200, "User-agent: *\nCrawl-delay: 10"), f"{SITE}/a": html()})
    web.routes["https://other-artist.com/a"] = html()
    for _ in range(3):
        web.get("/a")
    assert web.times == [1000.0, 1010.0, 1020.0, 1030.0, 1040.0]
    for _ in range(2):
        web.fetch("https://other-artist.com/a")
    assert web.times[5:] == [1040.0, 1040.0 + MIN, 1040.0 + 2 * MIN, 1040.0 + 3 * MIN]  # the default, elsewhere


@pytest.mark.parametrize(
    "robots_txt",
    [
        "User-agent: *\nCrawl-delay: 4.5",
        "User-agent: *\nCrawl-delay: 4,5",
        "User-agent: *\nCrawl-delay: 4.5s",
        "User-agent: *\nCrawl-delay: 4.5 seconds",
        "User-agent: ArtistSearchBot\nCrawl-delay: 4.5",
        "User-agent: ArtistSearchBot\nDisallow: /x\n\nUser-agent: *\nCrawl-delay: 4.5",  # the larger of ours and `*`
        "Crawl-delay: 4.5",
    ],
)
def test_crawl_delay_is_read_for_us_and_for_star(robots_txt):
    web = Web({ROBOTS: reply(200, robots_txt), f"{SITE}/a": html()})
    web.get("/a")
    web.get("/a")
    assert web.times == [1000.0, 1004.5, 1009.0, 1013.5]


@pytest.mark.parametrize("value", ["0", "1", "0.5", "2.999", "2s", "-30", "nan", "inf", "soon", "", "3", "1e12"])
def test_crawl_delay_never_lowers_the_delay(value):
    web = Web({ROBOTS: reply(200, f"User-agent: *\nCrawl-delay: {value}"), f"{SITE}/a": html()})
    web.get("/a")
    web.get("/a")
    assert web.gaps == [MIN, MIN, MIN]


def test_a_crawl_delay_on_one_host_holds_for_the_whole_site_and_is_not_lowered_by_another():
    www = "https://www.janedoe.art"
    web = Web({f"{www}/robots.txt": reply(200, "User-agent: *\nCrawl-delay: 8"), f"{www}/a": html()})
    web.routes.update({ROBOTS: reply(200, "User-agent: *\nCrawl-delay: 5"), f"{SITE}/a": html()})
    for url in (f"{www}/a", f"{www}/a", f"{SITE}/a", f"{www}/a"):
        web.fetch(url)
    assert web.urls == [*files(www), f"{www}/a", f"{www}/a", ROBOTS, TDM, f"{SITE}/a", f"{www}/a"]
    assert web.gaps == [8.0] * 7


def test_a_crawl_delay_for_another_bot_is_not_ours():
    web = Web({ROBOTS: reply(200, "User-agent: Googlebot\nCrawl-delay: 30"), f"{SITE}/a": html()})
    web.get("/a")
    assert web.gaps == [MIN, MIN]


@pytest.mark.parametrize("value", [str(MAX_CRAWL_DELAY + 1), "86400", "100000000000", "3600 seconds"])
def test_an_absurd_crawl_delay_closes_the_host_instead_of_stalling_the_run(value):
    web = Web({ROBOTS: reply(200, f"User-agent: *\nCrawl-delay: {value}"), f"{SITE}/a": html()})
    assert web.get("/a").skip == "robots"
    assert web.urls == [ROBOTS]
    assert web.clock.sleeps == []


# --- 3. Rate limit -------------------------------------------------------------------------------------------------


def test_requests_to_a_host_are_spaced_and_the_policy_files_count():
    web = Web({f"{SITE}/a": html(), f"{SITE}/b": html()})
    web.get("/a")
    web.get("/b")
    web.get("/a")
    assert web.urls == [ROBOTS, TDM, f"{SITE}/a", f"{SITE}/b", f"{SITE}/a"]
    assert web.times == [1000.0 + n * MIN for n in range(5)]
    assert web.clock.sleeps == [MIN] * 4


def test_only_the_time_still_owed_is_slept():
    web = Web({f"{SITE}/a": html()})
    web.get("/a")
    web.clock.now += 1.0
    web.get("/a")
    web.clock.now += MIN
    web.get("/a")
    web.clock.now += 50.0
    web.get("/a")
    assert web.clock.sleeps == [MIN, MIN, MIN - 1.0]
    assert min(web.gaps) == MIN


def test_other_sites_do_not_wait_but_subdomains_of_one_site_share_its_clock():
    web = Web({f"{SITE}/a": html(), "https://other-artist.com/a": html(), "https://www.janedoe.art/a": html()})
    web.get("/a")  # asked at 1000, 1003 and 1006
    web.fetch("https://other-artist.com/a")  # a different site: at once, then its own waits
    assert web.times == [1000.0, 1003.0, 1006.0, 1006.0, 1009.0, 1012.0]
    web.clock.now = 1007.0
    web.fetch("https://www.janedoe.art/a")  # the same site as janedoe.art, last asked at 1006
    assert web.times[6:] == [1009.0, 1012.0, 1015.0]


def test_failed_requests_and_redirect_hops_are_spaced_too():
    web = Web({f"{SITE}/down": httpx.ConnectError("refused"), f"{SITE}/a": redirect("/b"), f"{SITE}/b": html()})
    web.routes[f"{SITE}/gone"] = reply(500)
    for path in ("/down", "/gone", "/a", "/down"):
        web.get(path)
    assert web.urls == [ROBOTS, TDM, *(SITE + path for path in ("/down", "/gone", "/a", "/b", "/down"))]
    assert web.gaps == [MIN] * 6


def test_the_interval_holds_for_a_site_whose_robots_could_not_be_read():
    web = Web({ROBOTS: reply(500), "https://www.janedoe.art/a": html()})
    assert web.get("/a").skip == "robots"
    assert web.fetch("https://www.janedoe.art/a").ok  # another host of the same site
    assert web.gaps == [MIN] * 3


def test_the_rate_limit_holds_through_a_messy_crawl(monkeypatch):
    monkeypatch.setattr(crawl, "SITE_REQUEST_BUDGET", 10**6)  # this test is about spacing, not the budget
    apex, www, other = "https://janedoe.art", "https://www.janedoe.art", "https://other-artist.co.uk"
    web = Web(
        {
            f"{apex}/": redirect(f"{www}/"),
            f"{www}/": html(),
            f"{www}/gallery": html(),
            f"{www}/img/1.png": reply(200, b"png", {"content-type": "image/png"}),
            f"{www}/locked": reply(403),
            f"{www}/down": httpx.ReadTimeout("slow"),
            f"{www}/robots.txt": reply(200, "User-agent: *\nDisallow: /private"),
            f"{other}/robots.txt": reply(200, "User-agent: *\nCrawl-delay: 5"),
            f"{other}/": html(),
            f"{other}/old": redirect("/"),
        }
    )
    urls = [url for url in web.routes if "robots.txt" not in url] + [f"{www}/private", f"{other}/missing"]
    rng = random.Random(7)
    for _ in range(300):
        web.clock.now += rng.choice([0, 0, 0, 0.25, 1.0, 2.75, 3.0, 4.75, 9.5])  # exact in binary: no rounding noise
        fetcher = web.new_fetcher() if rng.random() < 0.1 else web.fetcher  # a new fetcher changes nothing
        web.fetch(rng.choice(urls), fetcher=fetcher, image=rng.random() < 0.3)
    by_site = defaultdict(list)
    for url, at in zip(web.urls, web.times, strict=True):
        by_site["other" if "other-artist" in url else "janedoe"].append(at)
    assert len(by_site["janedoe"]) > 100 and len(by_site["other"]) > 50
    assert min(b - a for a, b in itertools.pairwise(by_site["janedoe"])) == MIN
    assert min(b - a for a, b in itertools.pairwise(by_site["other"])) == 5.0  # raised by its Crawl-delay


def test_a_new_fetcher_for_every_url_cannot_outrun_the_limit():
    web = Web({ROBOTS: reply(200, "User-agent: *\nCrawl-delay: 7"), f"{SITE}/a": html()})
    for _ in range(3):
        assert web.fetch(f"{SITE}/a", fetcher=web.new_fetcher()).ok
    assert web.urls == [ROBOTS, TDM, f"{SITE}/a"] * 3
    assert web.gaps == [7.0] * 8  # the raised delay outlives the fetcher that learned it


def test_threads_cannot_fetch_one_site_at_the_same_time():
    web = Web()
    inside, most = [], []

    def slow(request):
        inside.append(request)
        most.append(len(inside))
        threading.Event().wait(0.02)  # long enough for another thread to barge in, if anything lets it
        inside.pop()
        return httpx.Response(200, content=b"art")

    web.routes[f"{SITE}/a"] = slow
    threads = [threading.Thread(target=web.get, args=("/a",)) for _ in range(4)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert web.urls == [ROBOTS, TDM] + [f"{SITE}/a"] * 4
    assert max(most) == 1
    assert web.gaps == [MIN] * 5


def test_a_sleep_that_does_not_sleep_cannot_shorten_the_wait():
    web = Web({f"{SITE}/a": html()}, clock=Clock(tick=0.125, sleeping=False))  # only reading the clock moves it
    for _ in range(3):
        assert web.get("/a").ok
    assert len(web.times) == 5
    assert min(web.gaps) >= MIN
    assert len(web.clock.sleeps) > 20  # it kept asking, and kept checking, until the time had really passed


def test_a_clock_that_stands_still_stops_the_fetch_instead_of_hanging_or_hurrying():
    web = Web({f"{SITE}/a": html()}, clock=Clock(sleeping=False))
    assert web.get("/a").skip == "error"
    assert web.get("/a").skip == "error"
    assert web.urls == [ROBOTS]  # the first request needs no wait; no second one can be shown to be due


def test_a_reading_left_by_another_clock_costs_one_interval_not_an_age():
    ahead = Web({f"{SITE}/a": html()})
    ahead.clock.now = 5_000_000.0
    ahead.get("/a")
    web = Web({f"{SITE}/a": html()})  # its clock reads 1000: the reading above is in its far future
    assert web.get("/a").ok
    assert web.clock.sleeps == [MIN, MIN, MIN]


def test_the_interval_runs_from_the_end_of_the_previous_response():
    web = Web()

    def slow(request):
        web.clock.now += 10.0  # the response takes ten seconds to arrive
        return httpx.Response(200, content=b"art")

    web.routes[f"{SITE}/a"] = slow
    web.get("/a")
    web.get("/a")
    assert web.gaps == [MIN, MIN, 10.0 + MIN]


def test_a_site_gets_a_fixed_number_of_requests_per_process():
    web = Web({f"{SITE}/a": redirect("/b"), f"{SITE}/b": html(), f"{SITE}/down": httpx.ConnectError("refused")})
    web.routes.update({"https://www.janedoe.art/b": html(), "https://other-artist.com/a": html()})
    assert SITE_REQUEST_BUDGET == 60
    paths = itertools.cycle(["/a", "/down", "/missing", "/b"])
    skips = [web.get(next(paths)).skip for _ in range(SITE_REQUEST_BUDGET)]
    assert len(web.requests) == SITE_REQUEST_BUDGET  # robots.txt, tdmrep.json, redirect hops and failures all count
    assert skips[:4] == [None, "error", "error", None] and skips[-8:] == ["budget"] * 8
    assert web.get("/b").skip == "budget"
    assert web.fetch("https://www.janedoe.art/b").skip == "budget"  # every host of the site
    assert web.fetch(f"{SITE}/b", fetcher=web.new_fetcher()).skip == "budget"  # every fetcher in the process
    assert len(web.requests) == SITE_REQUEST_BUDGET  # not one request more, policy files included
    assert web.fetch("https://other-artist.com/a").ok  # another site has a budget of its own


def test_the_budget_is_not_overrun_by_the_policy_files_of_another_host(monkeypatch):
    monkeypatch.setattr(crawl, "SITE_REQUEST_BUDGET", 4)
    web = Web({f"{SITE}/a": html(), "https://www.janedoe.art/a": html()})
    assert web.get("/a").ok  # three requests
    assert web.fetch("https://www.janedoe.art/a").skip == "budget"  # its robots.txt is the fourth, and the last
    assert web.urls == [ROBOTS, TDM, f"{SITE}/a", "https://www.janedoe.art/robots.txt"]


@pytest.mark.parametrize("answer", [(429, "120"), (503, "600"), (429, None), (503, None)])
@pytest.mark.parametrize("where", ["page", "image", "robots", "tdmrep"])
def test_a_429_or_503_closes_the_site_for_the_rest_of_the_run(answer, where):
    status, retry_after = answer
    web = Web({f"{SITE}/a": html(), f"{SITE}/b": html(), "https://www.janedoe.art/b": html()})
    web.routes["https://other-artist.com/a"] = html()
    target = {"page": f"{SITE}/a", "image": f"{SITE}/a", "robots": ROBOTS, "tdmrep": TDM}[where]
    web.routes[target] = reply(status, headers={"retry-after": retry_after} if retry_after else None)
    assert web.get("/a", image=where == "image").skip in ("error", "robots")
    asked = list(web.urls)
    assert asked[-1] == target
    web.clock.now += 3600.0  # however long we wait
    assert web.get("/b").skip in ("budget", "robots", "error")
    assert web.fetch("https://www.janedoe.art/b").skip == "budget"  # every host of the site
    assert web.fetch(f"{SITE}/b", fetcher=web.new_fetcher()).skip == "budget"  # every fetcher in the process
    assert web.urls == asked  # the site is not asked again
    assert web.fetch("https://other-artist.com/a").ok


def test_http_and_https_of_one_site_share_one_clock():
    web = Web({"http://janedoe.art/old": redirect(f"{SITE}/new"), f"{SITE}/new": html()})
    assert web.fetch("http://janedoe.art/old").ok
    assert web.urls == [*files("http://janedoe.art"), "http://janedoe.art/old", ROBOTS, TDM, f"{SITE}/new"]
    assert web.gaps == [MIN] * 5  # the https robots.txt waits its turn after the http redirect


def test_a_crawl_delay_learned_over_http_holds_for_https():
    web = Web({"http://janedoe.art/robots.txt": reply(200, "User-agent: *\nCrawl-delay: 9"), f"{SITE}/a": html()})
    web.routes["http://janedoe.art/a"] = html()
    web.get("/a")
    web.fetch("http://janedoe.art/a")
    web.get("/a")
    web.get("/a")
    assert (
        web.urls == [ROBOTS, TDM, f"{SITE}/a", *files("http://janedoe.art"), "http://janedoe.art/a"] + [f"{SITE}/a"] * 2
    )
    assert web.gaps == [MIN, MIN, MIN, 9.0, 9.0, 9.0, 9.0]


def test_no_caller_and_no_environment_variable_can_lower_the_delay():
    assert MIN == 3.0
    constructor = inspect.signature(PoliteFetcher).parameters
    assert list(constructor) == ["transport", "clock", "sleep", "resolver"]  # nothing here sets a delay
    assert all(parameter.kind is parameter.KEYWORD_ONLY for parameter in constructor.values())
    assert list(inspect.signature(PoliteFetcher.open_site).parameters) == ["self", "root_url", "profile_url"]
    assert list(inspect.signature(Site.fetch).parameters) == ["self", "url", "image"]
    assert list(inspect.signature(Site).parameters) == ["fetcher", "root", "profile", "front_page"]
    source = Path(crawl.__file__).read_text(encoding="utf-8")
    assert not re.search(r"\bos\b|\bgetenv\b|\benviron\b", source)  # nothing is read from the environment


@pytest.mark.parametrize("name", ["CRAWL_DELAY", "MIN_DELAY", "MIN_SECONDS_PER_HOST", "ARTIST_SEARCH_MIN_DELAY"])
def test_an_environment_variable_changes_nothing(monkeypatch, name):
    monkeypatch.setenv(name, "0")
    monkeypatch.setenv(f"POLITE_{name}", "0")
    web = Web({f"{SITE}/a": html()})
    web.get("/a")
    web.get("/a")
    assert web.gaps == [MIN, MIN, MIN]


# --- 4. AI opt-outs ------------------------------------------------------------------------------------------------


def test_the_ai_crawler_list_is_the_agreed_one():
    assert sorted(AI_CRAWLER_TOKENS) == sorted(
        "GPTBot ChatGPT-User OAI-SearchBot ClaudeBot anthropic-ai Claude-Web CCBot Google-Extended PerplexityBot "
        "Bytespider Applebot-Extended Meta-ExternalAgent Amazonbot cohere-ai Diffbot omgili ImagesiftBot "
        "FacebookBot Omgilibot Claude-SearchBot Claude-User Perplexity-User img2dataset YouBot Timpibot AI2Bot "
        "PetalBot DuckAssistBot meta-externalfetcher Google-CloudVertexBot cohere-training-data-crawler "
        "Webzio-Extended MistralAI-User Ai2Bot-Dolma GoogleOther TikTokSpider PanguBot".split()
        + ["Kangaroo Bot"]
    )


@pytest.mark.parametrize("token", AI_CRAWLER_TOKENS)
def test_a_site_wide_block_of_any_ai_crawler_skips_the_whole_host(token):
    web = Web({ROBOTS: reply(200, f"User-agent: *\nAllow: /\n\nUser-agent: {token}\nDisallow: /\n")})
    web.routes.update({f"{SITE}/": html(), f"{SITE}/gallery/1": html(), f"{SITE}/img/a.png": reply(200, b"png")})
    assert web.get("/").skip == "ai_opt_out"
    assert web.get("/gallery/1").skip == "ai_opt_out"
    assert web.get("/img/a.png", image=True).skip == "ai_opt_out"
    assert web.urls == [ROBOTS]


OPT_OUT_HEADERS = [
    [("x-robots-tag", "noai")],
    [("x-robots-tag", "noimageai")],
    [("x-robots-tag", "noindex")],
    [("x-robots-tag", "none")],
    [("X-Robots-Tag", "NoAI, NoImageAI")],
    [("x-robots-tag", "noindex, nofollow")],
    [("x-robots-tag", "all,noai")],
    [("x-robots-tag", "ArtistSearchBot: noindex")],
    [("x-robots-tag", "GPTBot: noindex")],
    [("x-robots-tag", "nofollow"), ("x-robots-tag", "noai")],  # two header lines: every one of them is read
    [("x-robots-tag", "noai"), ("x-robots-tag", "nofollow")],
    [("x-robots-tag", "index"), ("x-robots-tag", "noimageai"), ("x-robots-tag", "follow")],
    [("tdm-reservation", "1"), ("tdm-reservation", "0")],
    [("content-signal", "ai-train=no"), ("content-signal", "search=yes")],
    [("tdm-reservation", "1")],
    [("TDM-Reservation", " 1 ")],
    [("content-signal", "ai-train=no")],
    [("Content-Signal", "search=yes, ai-input=no")],
    [("content-usage", "train-ai=n")],
    [("Content-Usage", "train-genai=n, search=y")],
]
NOT_OPT_OUT_HEADERS = [
    [("x-robots-tag", "index, follow, max-image-preview:large, max-snippet:-1")],
    [("x-robots-tag", "nofollow, noarchive")],
    [("x-robots-tag", "nonexistent, noaim")],  # whole words only
    [("tdm-reservation", "0")],
    [("content-signal", "search=yes, ai-train=yes, ai-input=yes")],
    [("content-usage", "train-ai=y")],
    [("x-content-note", "noai noindex none ai-train=no")],  # not a header that speaks to crawlers
]


@pytest.mark.parametrize("image", [False, True])
@pytest.mark.parametrize("headers", OPT_OUT_HEADERS)
def test_an_opt_out_header_skips_the_page_or_image(headers, image):
    content_type = ("content-type", "image/png" if image else "text/html")
    web = Web({f"{SITE}/work": reply(200, b"the work", [content_type, *headers])})
    result = web.get("/work", image=image)
    assert result.skip == "ai_opt_out"
    assert result.content == b""  # nothing of an opted-out response is handed on


@pytest.mark.parametrize("image", [False, True])
@pytest.mark.parametrize("headers", NOT_OPT_OUT_HEADERS)
def test_other_headers_do_not_skip(headers, image):
    content_type = ("content-type", "image/png" if image else "text/html")
    web = Web({f"{SITE}/work": reply(200, b"the work", [content_type, *headers])})
    assert web.get("/work", image=image).content == b"the work"


OPT_OUT_PAGES = [
    f"<html><head>{NOAI}</head></html>",
    '<meta name="robots" content="noimageai">',
    '<meta name="robots" content="noindex">',
    '<meta name="robots" content="none">',
    '<META NAME="ROBOTS" CONTENT="NOINDEX, NOFOLLOW">',
    "<meta name=robots content=noai>",
    "<meta name=robots content=noai/>",
    "<meta content=noai name=robots/>",
    "<meta content='index, noimageai' name='robots' />",
    '<meta\n  name="robots"\n  content="noai,noimageai"\n>',
    '<meta name="robots" content="no&#97;i">',  # a browser reads this as noai
    '<meta name="ArtistSearchBot" content="noindex">',
    '<meta name="artistsearchbot" content="none">',
    '<meta name="GPTBot" content="noindex">',  # addressed to any AI crawler
    '<meta name="ccbot" content="noai">',
    '<meta name="tdm-reservation" content="1">',
    '<meta name="content-usage" content="train-ai=n">',
    '<meta http-equiv="X-Robots-Tag" content="noai">',
    f"<html><body><p>late</p>{NOAI}</body></html>",
    "<p>padding</p>" * 100_000 + NOAI,  # 1.4 MB in
    "<!--" * 400_000 + NOAI,  # malformed, and still read in good time
    '<meta data-note="a > b" name="robots" content="noai">',  # a ">" inside a quoted value does not end the tag
    "<meta data-note='a > b' name='robots' content='noai'>",
    '<meta data-pad="' + "x" * 5_000 + '" name="robots" content="noai">',  # a long tag is read to its end
    '<meta name="robots" name="description" content="noai">',  # of a repeated attribute, the first one counts
    '<meta name="robots" content="noai" content="index, follow">',
    '<p>cut off</p><meta name="robots" content="noai',  # a tag with no end counts as an opt-out
    '<meta data-pad="' + "x" * (META_TAG_MAX + 1) + '" name="description" content="art">',  # nor one too long to read
    NOAI.encode("utf-16"),
    NOAI.encode("utf-16-le"),  # no byte-order mark, and the Content-Type says utf-8
    NOAI.encode("utf-16-be"),
    NOAI.encode("utf-32"),
    NOAI.encode("utf-32-le"),
    NOAI.encode("utf-32-be"),
    b"\x89PNG\r\n\x1a\n\x00\x00" + NOAI.encode(),  # whatever else the body holds
]
NOT_OPT_OUT_PAGES = [
    '<meta name="robots" content="index, follow, max-image-preview:large">',
    '<meta name="robots" content="nofollow, noarchive">',
    '<meta name="robots" content="nonexistent">',
    '<meta name="googlebot" content="noindex">',  # addressed to a crawler that is not an AI crawler
    '<meta name="tdm-reservation" content="0">',
    '<meta name="description" content="noai, noindex and none are robots directives">',
    "<p>noai noindex none tdm-reservation: 1</p>",
    "<metadata name='robots' content='noai'></metadata>",
    '<meta data-pad="' + "x" * 5_000 + '" name="description" content="noai">',  # read to its end, and harmless
    '<meta name="description" name="robots" content="noai">',  # the first name counts, as in a browser
    '<meta name="robots" content="index, follow" content="noai">',
    '<meta name="description" content="a > b"><p>noai</p>',
]


@pytest.mark.parametrize("page", OPT_OUT_PAGES, ids=short)
def test_an_opt_out_meta_tag_skips_the_page(page):
    web = Web({f"{SITE}/work": html(page)})
    result = web.get("/work")
    assert result.skip == "ai_opt_out"
    assert result.content == b""


@pytest.mark.parametrize("page", NOT_OPT_OUT_PAGES, ids=short)
def test_other_meta_tags_do_not_skip(page):
    web = Web({f"{SITE}/work": html(page)})
    assert web.get("/work").content == page.encode()


@pytest.mark.parametrize("image", [False, True])
@pytest.mark.parametrize("content_type", ["text/html", "text/html; charset=utf-16", "image/png", None])
def test_an_opt_out_meta_tag_counts_whatever_the_content_type_claims(content_type, image):
    headers = {"content-type": content_type} if content_type else None
    web = Web({f"{SITE}/a.png": reply(200, NOAI, headers)})
    assert web.get("/a.png", image=image).skip == "ai_opt_out"


def test_real_image_bytes_are_not_mistaken_for_an_opt_out():
    xmp = b'<x:xmpmeta xmlns:x="adobe:ns:meta/"><metadata>noai none</metadata></x:xmpmeta>'
    png = b"\x89PNG\r\n\x1a\n" + xmp + random.Random(1).randbytes(200_000)
    web = Web({f"{SITE}/a.png": reply(200, png, {"content-type": "image/png"})})
    assert web.get("/a.png", image=True).content == png


COMPRESSED = [  # (Content-Encoding, how a body is sent under it)
    ("gzip", gzip.compress),
    ("x-gzip", gzip.compress),
    ("GZIP", gzip.compress),
    ("deflate", zlib.compress),
    ("deflate", lambda body: zlib.compress(body)[2:-4]),  # deflate without its zlib wrapper, as some servers send
    ("gzip, gzip", lambda body: gzip.compress(gzip.compress(body))),
    ("identity", bytes),
    ("gzip", lambda body: gzip.compress(b"<p>first member</p>") + gzip.compress(body)),
]


@pytest.mark.parametrize(("encoding", "pack"), COMPRESSED)
def test_an_opt_out_inside_a_compressed_body_is_seen(encoding, pack):
    web = Web({f"{SITE}/work": raw_reply(pack(NOAI.encode()), {"content-encoding": encoding})})
    assert web.get("/work").skip == "ai_opt_out"


@pytest.mark.parametrize(("encoding", "pack"), COMPRESSED)
def test_a_compressed_body_comes_back_decoded(encoding, pack):
    web = Web({f"{SITE}/work": raw_reply(pack(b"<p>art</p>"), {"content-encoding": encoding})})
    assert web.get("/work").content.removeprefix(b"<p>first member</p>") == b"<p>art</p>"


@pytest.mark.parametrize("encoding", ["br", "zstd", "compress", "gzip, br", "rot13"])
def test_a_body_we_cannot_decode_is_not_handed_on_unread(encoding):
    web = Web({f"{SITE}/work": raw_reply(NOAI.encode(), {"content-encoding": encoding})})
    assert (web.get("/work").skip, web.get("/work").content) == ("error", b"")


def test_the_only_encodings_we_ask_for_are_the_ones_we_can_decode():
    web = Web({f"{SITE}/a": html()})
    web.get("/a")
    assert {request.headers["accept-encoding"] for request in web.requests} == {"gzip, deflate"}


TDM_CASES = [
    ('[{"location": "/", "tdm-reservation": 1}]', "/gallery/1", "ai_opt_out"),
    ('[{"location": "/gallery/*", "tdm-reservation": 1}]', "/gallery/1", "ai_opt_out"),
    ('[{"location": "/gallery/", "tdm-reservation": 1}]', "/about", None),
    ('[{"location": "*.jpg", "tdm-reservation": "1"}]', "/img/a.jpg", "ai_opt_out"),
    ('[{"location": "/caf%C3%A9", "tdm-reservation": 1}]', "/café", "ai_opt_out"),
    (
        '[{"location": "/press/", "tdm-reservation": 0}, {"location": "/", "tdm-reservation": 1}]',
        "/press/kit",
        "ai_opt_out",
    ),
    ('[{"tdm-reservation": 1, "tdm-policy": "https://janedoe.art/policy.json"}]', "/a", "ai_opt_out"),
    ('[{"location": "everything", "tdm-reservation": 1}]', "/a", "ai_opt_out"),  # an odd location: everywhere
    ('{"location": "/", "tdm-reservation": 1}', "/a", "ai_opt_out"),  # a lone object
    ('[{"location": "/", "tdm-reservation": 1}]'.encode("utf-16"), "/a", "ai_opt_out"),
    ('[{"location": "/", "tdm-reservation": 0}]', "/a", None),
    ('[{"location": "/", "tdm-reservation": 2}]', "/a", None),
    ("[]", "/a", None),
    ("null", "/a", None),
    ('["tdm-reservation", 1]', "/a", None),
    ("<html><body>No such page</body></html>", "/a", None),  # a 200 that is not the file
    ("[" * 100_000, "/a", None),
    ("", "/a", None),
]


@pytest.mark.parametrize(("tdmrep", "path", "skip"), TDM_CASES, ids=short)
def test_a_reservation_in_tdmrep_json_is_honored(tdmrep, path, skip):
    web = Web({TDM: reply(200, tdmrep, {"content-type": "application/json"})})
    target = str(httpx.URL(SITE + path))
    web.routes[target] = html()
    assert web.get(path).skip == skip
    assert web.get(path, image=True).skip == skip
    assert web.urls == ([ROBOTS, TDM] if skip else [ROBOTS, TDM, target, target])  # read once, ahead of the page


@pytest.mark.parametrize(
    "tdmrep", [reply(404), reply(410), reply(400), reply(401), reply(403), redirect("/")], ids=short
)
def test_no_tdmrep_json_reserves_nothing(tdmrep):
    web = Web({TDM: tdmrep, f"{SITE}/a": html(), f"{SITE}/": html()})
    assert web.get("/a").ok
    assert web.urls == [ROBOTS, TDM, f"{SITE}/a"]  # and a redirect away from the file is not followed


@pytest.mark.parametrize(
    "tdmrep",
    [
        reply(500),
        reply(503),
        reply(408),
        reply(429),
        httpx.ConnectError("refused"),
        httpx.ReadTimeout("slow"),
        reply(200, b" " * 600_000),
    ],
    ids=short,
)
def test_a_tdmrep_json_we_could_not_read_closes_the_origin(tdmrep):
    web = Web({TDM: tdmrep, f"{SITE}/a": html()})
    assert web.get("/a").skip == "error"
    assert web.get("/a").skip in ("error", "budget")
    assert web.urls == [ROBOTS, TDM]


def test_tdmrep_json_is_not_asked_for_where_robots_keeps_us_out_of_it():
    web = Web({ROBOTS: reply(200, "User-agent: *\nDisallow: /.well-known/"), f"{SITE}/a": html()})
    web.routes[TDM] = reply(200, '[{"location": "/", "tdm-reservation": 1}]')
    assert web.get("/a").ok
    assert web.urls == [ROBOTS, f"{SITE}/a"]


# --- 5. Own sites only ---------------------------------------------------------------------------------------------

OWN_SITES = [
    "https://janedoe.art",
    "https://janedoe.art/",
    "http://www.janedoe.art/",
    "HTTPS://WWW.JANEDOE.ART/",
    "https://janedoe.art:443/",
    "https://janedoe.art/#",
    "https://janedoe.co.uk/",
    "https://www.janedoe.art.br",
    "https://janedoe.id.au/",
    "https://bücher.de/",
    "https://xn--bcher-kva.de/",
    # Names that only look like a platform's.
    "https://fox.com/",
    "https://myx.com/",
    "https://notartstation.com/",
    "https://deviantartist.com/",
    "https://informat.com/",
]
NOT_OWN_SITES = [
    # A platform nobody has listed still has this shape: a user path, a query, or a user subdomain.
    "https://some-unlisted-platform.com/janedoe",
    "https://some-unlisted-platform.com/users/janedoe/",
    "https://some-unlisted-platform.com/?user=janedoe",
    "https://janedoe.some-unlisted-platform.com/",
    "https://janedoe.some-unlisted-platform.co.uk/",
    "https://portfolio.janedoe.art/",
    "https://www.www.janedoe.art/",
    "https://www2.janedoe.art/",
    "https://janedoe.art/index.html",
    "https://janedoe.art/#work",
    "https://some-unlisted-platform.com/#/users/janedoe",
    "https://some-unlisted-platform.com/#!/janedoe",
    "https://www.some-unlisted-platform.com#janedoe",
    "https://janedoe.art//",
    # Subdomains handed out by a host: the Public Suffix List calls these suffixes private.
    "https://janedoe.github.io/",
    "https://janedoe.uk.com/",
    "https://janedoe.eu.org/",
    "https://janedoe.framer.website/",
    "https://janedoe.wixstudio.io/",
    "https://janedoe.blogspot.com/",
    # A suffix alone is nobody's site.
    "https://co.uk/",
    "https://github.io/",
    "https://com/",
    # Input that is not a URL.
    "https://janedoe.art/\ud83d",  # a lone surrogate
    "\ud83d",
    "janedoe.art",
    "",
]
FETCHABLE = [  # not front pages, so not "own sites" to discover, but pages and images the fetcher will get
    "http://www.jane-doe.com/portfolio?page=2",
    "https://art.janedoe.co.uk/",
    "https://cdn.janedoe.art/img/1.png",
    "https://janedoe.com/artstation",
    "https://janedoe.com/?from=instagram.com",
    "https://janedoe.art/a%20b/c.d/e..f/.g",
]


@pytest.mark.parametrize("url", OWN_SITES, ids=short)
def test_the_front_page_of_a_bare_custom_domain_is_an_own_site(url):
    assert is_own_site(url)
    parsed = httpx.URL(url)
    home = str(parsed.copy_with(raw_path=parsed.raw_path, fragment=None))  # as it is requested: "/" and no fragment
    web = Web({home: html()})
    site = web.open(url)
    assert isinstance(site, Site) and site.front_page.url == home
    assert web.urls == [f"{home}robots.txt", f"{home}.well-known/tdmrep.json", home]


@pytest.mark.parametrize("url", NOT_OWN_SITES + FETCHABLE, ids=short)
def test_anything_else_is_not_an_own_site(url):
    assert is_own_site(url) is False


@pytest.mark.parametrize("url", FETCHABLE, ids=short)
def test_pages_and_images_under_a_site_are_fetched(url):
    web = Web({str(httpx.URL(url)): html()})
    assert web.fetch(url).ok


NAMED_PLATFORMS = (
    "fiverr.com upwork.com freelancer.com deviantart.com artstation.com behance.net instagram.com facebook.com "
    "x.com twitter.com tiktok.com pinterest.com linkedin.com tumblr.com patreon.com ko-fi.com linktr.ee carrd.co "
    "wixsite.com squarespace.com weebly.com wordpress.com blogspot.com itch.io myportfolio.com cargo.site "
    "format.com sites.google.com youtube.com twitch.tv discord.com "
    # Found by the round 1 critic.
    "designcrowd.com contra.com workana.com artistsnclients.com twine.net coroflot.com telegram.me instagr.am "
    "lnkd.in bilibili.com inkbunny.net weasyl.com itaku.ee misskey.io fxtwitter.com artstn.co 500px.com fantia.jp "
    "weeblysite.com wixstudio.io framer.website ghost.io crevado.com journoportfolio.com smugmug.com home.blog "
    "livejournal.com bearblog.dev uk.com wixstatic.com shopify.com squarespace-cdn.com wp.com taplink.cc lit.link"
).split()
PLATFORM_URLS = [
    url
    for domain in NAMED_PLATFORMS
    for url in (
        f"https://{domain}/",
        f"https://{domain}/janedoe",
        f"http://www.{domain}/",
        f"https://janedoe.{domain}/gallery?page=2",
        f"HTTPS://{domain.upper()}/JaneDoe",
    )
] + [
    "https://janedoe.blogspot.co.uk/",
    "https://www.pinterest.de/janedoe",
    "https://www.freelancer.in/u/janedoe",
    "https://discord.gg/abc",
    "https://youtu.be/abc",
    "https://t.co/abc",
    "https://fav.me/abc",
    "https://images-wixmp-ed30a86b8c4ca887773594c2.wixmp.com/f/a.jpg",  # DeviantArt's image host
    "https://cdna.artstation.com/p/assets/images/1.jpg",
    "https://pbs.twimg.com/media/a.jpg",
    "https://i.pinimg.com/originals/a.jpg",
    # Image hosts of site builders that serve custom domains: not approved by the owner, so refused.
    "https://static.wixstatic.com/media/a.jpg",
    "https://cdn.shopify.com/s/files/a.jpg",
    "https://images.squarespace-cdn.com/content/a.jpg",
    "https://i0.wp.com/janedoe.art/a.jpg",
    # Subdomains handed out by a host (private entries of the Public Suffix List), listed here or not.
    "https://janedoe.github.io/",
    "https://janedoe.neocities.org/",
    "https://janedoe.eu.org/work",
    "https://janedoe-art.s3.amazonaws.com/a.png",
    "https://d111abcdef8.cloudfront.net/a.png",
    "https://janedoe.pages.dev/",
    "https://docs.google.com/document/d/abc",
    "https://www.peopleperhour.com/freelancer/janedoe",
    # Wildcard DNS services: the name says which address it resolves to.
    "http://127.0.0.1.nip.io/",
    "http://10.0.0.1.sslip.io/",
    "http://localtest.me/",
    "http://admin.lvh.me/",
]


@pytest.mark.parametrize("url", PLATFORM_URLS, ids=short)
def test_platforms_are_refused_without_any_request(url):
    assert not is_own_site(url)
    web = Web({url: html(), str(httpx.URL(url).copy_with(raw_path=b"/robots.txt")): reply(200, "")})
    assert web.fetch(url).skip == "platform"
    assert web.requests == [] and web.lookups == []


# A copy of the whole denylist. Dropping an entry from the code, or adding one without a line here, fails.
DENYLIST_NAMES = """
    fiverr upwork freelancer peopleperhour 99designs toptal designcrowd workana artistsnclients coroflot
    crowdspring designhill truelancer kwork legiit zeerk workingnotworking artistree commishes sketchmob artcorgi
    coconala crowdworks cgtrader turbosquid malt the-dots lancers skima therookies cubebrush domestika reedsy
    deviantart wixmp artstation behance dribbble pixiv fanbox fantia newgrounds furaffinity inkbunny weasyl
    sofurry itaku artfol artfight sketchfab cgsociety 500px unsplash giphy pximg skeb vgen unvale zcool vsco
    instagram cdninstagram ddinstagram facebook fbcdn twitter twimg fxtwitter vxtwitter fixupx fixvx nitter
    tiktok douyin pinterest pinimg linkedin tumblr reddit redditmedia redditstatic snapchat whatsapp telegram
    bsky mastodon mstdn pawoo baraag misskey pixelfed cohost pillowfort weibo bilibili xiaohongshu lofter naver
    kakao tistory nicovideo nijie plurk flickr staticflickr imgur vimeo dailymotion youtube ytimg twitch picarto
    piczel discord discordapp quora livejournal dreamwidth wattpad webtoons archiveofourown aminoapps myspace xing
    spotify soundcloud sndcdn bandcamp gamejolt roblox steamcommunity steampowered vk vkontakte ameblo fc2
    patreon patreonusercontent ko-fi buymeacoffee subscribestar boosty gumroad substack kickstarter indiegogo
    onlyfans fansly
    etsy redbubble society6 inprnt teepublic displate threadless fineartamerica saatchiart amazon amazonaws ebay
    bigcartel storenvy shopify myshopify shopifycdn
    linktr carrd allmylinks taplink tinyurl
    wix wixsite wixstudio wixstatic parastorage squarespace squarespace-cdn sqspcdn weebly weeblysite weeblycloud
    editmysite wordpress wpcomstaging blogspot typepad hatenablog hatena over-blog myportfolio adobe
    cargocollective webflow jimdo jimdofree jimdosite jimcdn strikingly mystrikingly strikinglycdn godaddysites
    webnode site123 yolasite ucraft tildacdn neocities tripod angelfire 000webhostapp carbonmade portfoliobox
    crevado journoportfolio dunked krop pixpa smugmug zenfolio pixieset photoshelter 22slides viewbook readymag
    framer bearblog wsimg website-files replit
    google googleusercontent ggpht gstatic github githubusercontent gitlab netlify vercel herokuapp firebaseapp
    onrender dropbox dropboxusercontent icloud sharepoint mediafire wetransfer photobucket imageshack postimg
    imgbb gyazo cloudinary imgix gravatar wikipedia wikimedia
""".split()
DENYLIST_DOMAINS = """
    x.com t.co fb.com fb.me fb.watch m.me messenger.com t.me telesco.pe telegra.ph wa.me youtu.be pin.it redd.it
    flic.kr sta.sh fav.me artstn.co be.net instagr.am lnkd.in threads.net threads.com cara.app ok.ru
    qq.com line.me kick.com ello.co medium.com note.com about.me blogger.com fandom.com
    guru.com contra.com twine.net
    format.com cargo.site persona.co fabrik.io ghost.io write.as home.blog art.blog photo.blog design.blog wp.com
    webs.com super.site itch.io itch.zone tapas.io booth.pm toyhou.se
    commiss.io ych.art pages.dev workers.dev web.app fly.dev surge.sh glitch.me repl.co
    tilda.ws notion.site notion.so square.site crd.co uk.com
    bio.link lnk.bio linkin.bio linkr.bio campsite.bio lit.link hoo.be msha.ke solo.to beacons.ai direct.me
    stan.store flow.page bento.me potofu.me profcard.info
    bit.ly goo.gl is.gd buff.ly ow.ly rebrand.ly cutt.ly rb.gy tiny.cc t.ly
    ibb.co prnt.sc 1drv.ms mega.nz mega.io we.tl box.com b-cdn.net
    nip.io sslip.io xip.io localtest.me lvh.me vcap.me
""".split()


def test_the_denylist_is_the_recorded_one():
    assert sorted(PLATFORM_NAMES) == sorted(DENYLIST_NAMES)
    assert sorted(PLATFORM_DOMAINS) == sorted(DENYLIST_DOMAINS)


@pytest.mark.parametrize("name", DENYLIST_NAMES)
def test_every_listed_name_is_refused_wherever_it_stands_in_a_host(name):
    for url in (
        f"https://www.{name}.com/janedoe",
        f"https://{name}.janedoe-studio.net/",
        f"https://cdn.{name}.co.uk/a.png",
    ):
        web = Web({url: html()})
        assert (web.fetch(url).skip, web.requests, is_own_site(url)) == ("platform", [], False)


@pytest.mark.parametrize("domain", DENYLIST_DOMAINS)
def test_every_listed_domain_is_refused_with_its_subdomains(domain):
    for url in (
        f"https://{domain}/",
        f"https://{domain}/janedoe",
        f"https://janedoe.{domain}/",
        f"http://a.b.{domain}/c.png",
    ):
        web = Web({url: html()})
        assert (web.fetch(url).skip, web.requests, is_own_site(url)) == ("platform", [], False)


# --- 6. Public pages only ------------------------------------------------------------------------------------------

BAD_URLS = [
    # Not http or https.
    "ftp://janedoe.art/a.png",
    "file:///C:/Users/Owner/secrets.txt",
    "javascript:alert(1)",
    "data:text/html,<p>hi</p>",
    "mailto:jane@janedoe.art",
    "//janedoe.art/a",
    "janedoe.art/a",
    "/a",
    "",
    # Credentials in the URL.
    "https://jane:hunter2@janedoe.art/",
    "https://jane@janedoe.art/",
    # Not a public site: local names, special-use names, every spelling of an IP address, other ports.
    "http://localhost/",
    "http://localhost:11434/api/tags",
    "http://intranet/",
    "http://app.localhost/",
    "http://printer.local/",
    "http://nas.lan/",
    "http://fritz.box/",
    "http://server.internal/",
    "http://host.docker.internal/",
    "http://router.home.arpa/",
    "http://janedoe.home/",
    "http://janedoe.corp/",
    "http://janedoe.test/",
    "http://janedoe.example/",
    "http://janedoe.invalid/",
    "http://expyuzz4wqqyqhjn.onion/",
    "http://127.0.0.1/",
    "http://127.1/",
    "http://2130706433/",
    "http://0x7f.0.0.1/",
    "http://192.168.1.1/admin",
    "http://169.254.169.254/latest/meta-data/",
    "http://[::1]/",
    "http://[::ffff:127.0.0.1]/",
    "https://janedoe.art:8443/",
    "http://janedoe.art:22/",
    "https://janedoe.art./",
    "https://co.uk/",
    # A path that a server would rewrite, so robots.txt cannot be held against it.
    "https://janedoe.art/public/%2e%2e/private/a",
    "https://janedoe.art/public/%2E%2E/private/a",
    "https://janedoe.art/%2e/private/a",
    "https://janedoe.art/public/.%2e/private/a",
    "https://janedoe.art/public%2f..%2fprivate/a",
    "https://janedoe.art/public/%252e%252e/private/a",
    "https://janedoe.art/public/..%5cprivate/a",
    "https://janedoe.art/public/..;/private/a",
    "https://janedoe.art/public/%2e%2e",
    "https://janedoe.art//private/a",  # an empty segment: a server that merges slashes serves /private/a
    "https://janedoe.art/public//a",
    "https://janedoe.art/%2Fprivate/a",  # an encoded slash
    "https://janedoe.art/public%2fa",
    "https://janedoe.art/%252Fprivate/a",
    "https://janedoe.art/%5Cprivate/a",  # a backslash, encoded or not
    "https://janedoe.art/%5cprivate/a",
    "https://janedoe.art/public\\a",
    # Not a URL at all.
    "https://jane doe.art/",
    "https://ｄｅｖｉａｎｔａｒｔ.com/",
    "http://",
    "https://[::1",
    "https://janedoe.art\n.evil.example/",
    "https://janedoe.art/\ud83d",
    "x" * 70_000,
]


@pytest.mark.parametrize("url", BAD_URLS, ids=short)
def test_only_plain_public_http_urls_are_requested(url):
    assert not is_own_site(url)
    web = Web({ROBOTS: reply(200, "User-agent: *\nDisallow: /private")})
    result = web.fetch(url)
    assert result.skip == "bad_url"
    assert web.requests == [] and web.lookups == []


def test_there_is_no_public_way_to_fetch_without_opening_a_site():
    # read_proof is the one other door, and it opens only two fixed addresses: the next test.
    assert [name for name in vars(PoliteFetcher) if not name.startswith("_")] == ["open_site", "read_proof"]
    assert [name for name in vars(Site) if not name.startswith("_")] == ["fetch"]


@pytest.mark.parametrize(
    ("root", "skip"),
    [
        ("https://janedoe.art/gallery", "not_own_site"),
        ("https://janedoe.some-unlisted-platform.com/", "not_own_site"),
        ("https://janedoe.github.io/", "platform"),
        ("http://localhost/", "bad_url"),
        ("https://janedoe.art/", None),
    ],
)
def test_a_removal_proof_is_read_from_two_addresses_of_an_own_site_and_nothing_else(root, skip):
    web = Web({"https://janedoe.art/": reply(200, '<html><head><meta name="x" content="yes"></head><body>')})
    found = web.fetcher.read_proof(root, "x", lambda said: said == "yes")
    if skip:
        assert found == skip and web.urls == []
    else:
        assert found is True
        assert web.urls == [f"{root}robots.txt", f"{root}.well-known/x.txt", root]


@pytest.mark.parametrize(
    ("root", "skip"),
    [
        ("https://some-unlisted-platform.com/janedoe", "not_own_site"),
        ("https://janedoe.some-unlisted-platform.com/", "not_own_site"),
        ("https://some-unlisted-platform.com/?user=janedoe", "not_own_site"),
        ("https://some-unlisted-platform.com/#/users/janedoe", "not_own_site"),
        ("https://some-unlisted-platform.com/#!/janedoe", "not_own_site"),
        ("https://www.artstation.com/", "platform"),
        ("https://www.malt.fr/", "platform"),
        ("https://janedoe.github.io/", "platform"),
        ("http://localhost/", "bad_url"),
        ("http://192.168.1.1/", "bad_url"),
        ("not a url", "bad_url"),
    ],
)
def test_open_site_says_why_a_url_is_not_an_own_site(root, skip):
    assert Web().open(root) == FetchResult(root, skip=skip)


@pytest.mark.parametrize("root", NOT_OWN_SITES + FETCHABLE + PLATFORM_URLS + BAD_URLS, ids=short)
def test_a_site_opens_only_from_the_front_page_of_an_own_site(root):
    web = Web({root: html()})
    refused = web.open(root)
    assert isinstance(refused, FetchResult) and refused.skip in ("not_own_site", "platform", "bad_url")
    forged = Site(web.fetcher, root, PROFILE)  # a handle made by hand opens nothing more
    assert forged.fetch(root).skip == refused.skip
    assert forged.fetch(f"{SITE}/").skip == refused.skip
    assert web.requests == [] and web.lookups == []


def front(*links):
    """A front page that holds this markup among the rest."""
    return html("<html><head><title>Jane Doe</title></head><body><h1>Jane Doe</h1>" + "".join(links) + "</body></html>")


LINKS_BACK = [
    '<a href="https://www.deviantart.com/janedoe">DeviantArt</a>',
    '<a href="https://deviantart.com/janedoe">DeviantArt</a>',
    '<a href="http://www.deviantart.com/janedoe">DeviantArt</a>',
    '<a href="https://www.deviantart.com/JaneDoe">DeviantArt</a>',
    '<a href="HTTPS://WWW.DEVIANTART.COM/JANEDOE">DeviantArt</a>',
    '<a href="https://www.deviantart.com/janedoe/">DeviantArt</a>',
    '<a href="https://www.deviantart.com/janedoe/gallery">my gallery</a>',
    '<a href="https://www.deviantart.com/janedoe/art/Dragon-Rider-912345678">a piece of mine</a>',
    '<a href="https://www.deviantart.com/janedoe?utm_source=site#about">DeviantArt</a>',
    '<a href="https://janedoe.deviantart.com">DeviantArt</a>',  # the old form
    '<a href="http://JaneDoe.DeviantArt.com/gallery/">DeviantArt</a>',
    '<a href="//www.deviantart.com/janedoe">DeviantArt</a>',
    '<a href="  https://www.deviantart.com/janedoe\n">DeviantArt</a>',
    '<a rel="nofollow noopener" target="_blank" href="https://www.deviantart.com/janedoe">me</a>',  # still a link
    "<a href='https://www.deviantart.com/janedoe'>DeviantArt</a>",
    "<A HREF=https://www.deviantart.com/janedoe>DeviantArt</A>",
    '<a class="social" href="https&#58;//www.deviantart.com/janedoe">DeviantArt</a>',
    '<link rel="me" href="https://www.deviantart.com/janedoe">',
    '<area shape="rect" coords="0,0,9,9" href="https://www.deviantart.com/janedoe">',
    '<!-- socials --><script>var x = "<a href=nothing>";</script><a href="https://www.deviantart.com/janedoe">me</a>',
    '<a href="https://www.deviantart.com/someone-else">a friend</a><a href="https://www.deviantart.com/janedoe">me</a>',
    "<p>filler</p>" * 100_000 + '<a href="https://www.deviantart.com/janedoe">DeviantArt</a>',
]
NEAR_MISSES = [
    "",  # no link at all
    "<p>Find me at https://www.deviantart.com/janedoe</p>",  # the handle only in text
    "<p>deviantart.com/janedoe</p>",
    '<!-- <a href="https://www.deviantart.com/janedoe">an old link</a> -->',  # only inside a comment
    '<!-- never closed <a href="https://www.deviantart.com/janedoe">x</a>',
    "<script>document.write('<a href=\"https://www.deviantart.com/janedoe\">x</a>')</script>",  # only inside a script
    '<script type="application/ld+json">{"sameAs": ["https://www.deviantart.com/janedoe"]}</script>',
    '<style>a[href="https://www.deviantart.com/janedoe"] { color: red }</style>',
    '<textarea><a href="https://www.deviantart.com/janedoe">x</a></textarea>',
    '<iframe><a href="https://www.deviantart.com/janedoe">x</a></iframe>',
    '<img alt=\'<a href="https://www.deviantart.com/janedoe">\' src="a.png">',  # only inside an attribute
    '<div data-href="https://www.deviantart.com/janedoe">x</div>',
    '<a name="https://www.deviantart.com/janedoe">x</a>',
    '<a title="https://www.deviantart.com/janedoe" href="/links">x</a>',
    '<img src="https://www.deviantart.com/janedoe">',
    '<div href="https://www.deviantart.com/janedoe">x</div>',  # an href on something that is not a link
    '<base href="https://www.deviantart.com/janedoe">',
    '<a href="https://www.deviantart.com/janedoe2">x</a>',  # another handle that shares a prefix
    '<a href="https://www.deviantart.com/janedoeart">x</a>',
    '<a href="https://www.deviantart.com/jane">x</a>',
    '<a href="https://www.deviantart.com/janedoe-fan">x</a>',
    '<a href="https://www.deviantart.com/someone-else/art/Portrait-of-janedoe-912345678">x</a>',  # another's deviation
    '<a href="https://www.deviantart.com/someone-else/janedoe">x</a>',
    '<a href="https://www.deviantart.com/tag/janedoe">x</a>',
    '<a href="https://www.deviantart.com/users/outgoing?https://www.deviantart.com/janedoe">x</a>',
    '<a href="https://www.deviantart.com/">x</a>',
    '<a href="https://www.deviantart.com//janedoe">x</a>',
    '<a href="https://janedoe2.deviantart.com/">x</a>',
    '<a href="https://www.janedoe.deviantart.com/">x</a>',
    '<a href="https://janedoe.deviantart.com.evil.example/">x</a>',
    '<a href="https://deviantart.com.evil.example/janedoe">x</a>',
    '<a href="https://notdeviantart.com/janedoe">x</a>',
    '<a href="https://www.deviantart.net/janedoe">x</a>',
    '<a href="https://sta.sh/janedoe">x</a>',
    '<a href="https://www.artstation.com/janedoe">x</a>',
    '<a href="ftp://www.deviantart.com/janedoe">x</a>',
    '<a href="https://www.deviantart.com:8443/janedoe">x</a>',
    '<a href="https://janedoe@www.deviantart.com/">x</a>',
    '<a href="/janedoe">x</a>',  # this site's /janedoe, not DeviantArt's
    '<a href="janedoe.deviantart.com">x</a>',
    '<a href="mailto:janedoe@deviantart.com">x</a>',
    "<a href=\"javascript:location='https://www.deviantart.com/janedoe'\">x</a>",
]


@pytest.mark.parametrize("link", LINKS_BACK, ids=short)
def test_a_front_page_that_links_back_to_the_profile_opens_the_site(link):
    web = Web({FRONT: front(link), f"{SITE}/gallery": html("<p>the gallery</p>")})
    site = web.open()
    assert isinstance(site, Site) and (site.root, site.profile) == (FRONT, PROFILE)
    assert site.front_page.ok and link.encode() in site.front_page.content  # the proof, kept so it need not be re-asked
    assert web.urls == PROOF
    assert site.fetch(f"{SITE}/gallery").content == b"<p>the gallery</p>"
    assert web.urls == [*PROOF, f"{SITE}/gallery"]


@pytest.mark.parametrize("link", NEAR_MISSES, ids=short)
def test_a_front_page_without_that_link_closes_the_site_for_the_run(link):
    web = Web({FRONT: front(link), f"{SITE}/gallery": html()})
    assert web.open() == FetchResult(FRONT, skip="not_own_site")
    assert web.urls == PROOF
    web.routes[FRONT] = front(LINK_BACK)  # whatever the page says later, the run is over for this site
    for fetcher in (web.fetcher, web.new_fetcher()):
        assert fetcher.open_site(FRONT, PROFILE).skip == "not_own_site"
        assert fetcher.open_site("http://www.janedoe.art", PROFILE).skip == "not_own_site"  # any front page of the site
        assert Site(fetcher, FRONT, PROFILE).fetch(f"{SITE}/gallery").skip == "not_own_site"
        assert Site(fetcher, FRONT, PROFILE).fetch(FRONT, image=True).skip == "not_own_site"
    assert web.urls == PROOF and len(web.requests) == VERIFY_REQUEST_BUDGET == 3


UNPROVEN = [  # (what the site answers, the skip, what was asked of it)
    ({ROBOTS: reply(200, "User-agent: *\nDisallow: /")}, "robots", [ROBOTS]),
    ({ROBOTS: reply(500)}, "robots", [ROBOTS]),
    ({ROBOTS: reply(200, "User-agent: GPTBot\nDisallow: /")}, "ai_opt_out", [ROBOTS]),
    ({TDM: reply(200, '[{"location": "/", "tdm-reservation": 1}]')}, "ai_opt_out", [ROBOTS, TDM]),
    ({FRONT: front(LINK_BACK, NOAI)}, "ai_opt_out", PROOF),
    ({FRONT: html(headers={"x-robots-tag": "noindex"})}, "ai_opt_out", PROOF),
    ({FRONT: reply(404)}, "error", PROOF),
    ({FRONT: reply(500)}, "error", PROOF),
    ({FRONT: reply(401)}, "auth", PROOF),
    ({FRONT: httpx.ConnectError("refused")}, "error", PROOF),
    ({FRONT: RuntimeError("a bug somewhere")}, "error", PROOF),
    ({FRONT: reply(200, LINK_BACK.encode() + bytes(PAGE_MAX_BYTES))}, "too_large", PROOF),
    # A front page that moves is not followed: proving a site is three requests, never a crawl.
    ({FRONT: redirect("https://www.janedoe.art/")}, "not_own_site", PROOF),
    ({FRONT: redirect("/home")}, "not_own_site", PROOF),
    ({FRONT: redirect("https://someone-else.com/")}, "not_own_site", PROOF),
    (
        {FRONT: redirect("/home"), ROBOTS: reply(200, "User-agent: *\nDisallow: /.well-known/")},
        "not_own_site",
        [ROBOTS, FRONT],
    ),
    # Nor is the allowance stretched by policy files that move.
    (
        {ROBOTS: redirect("https://www.janedoe.art/robots.txt"), "https://www.janedoe.art/robots.txt": reply(200, "")},
        "not_own_site",
        [ROBOTS, "https://www.janedoe.art/robots.txt", TDM],
    ),
    (
        {TDM: redirect("https://www.janedoe.art/.well-known/tdmrep.json")},
        "not_own_site",
        [ROBOTS, TDM, "https://www.janedoe.art/.well-known/tdmrep.json"],
    ),
]


@pytest.mark.parametrize(("answers", "skip", "asked"), UNPROVEN, ids=short)
def test_a_site_that_cannot_be_proven_is_closed_for_the_run(answers, skip, asked):
    web = Web({FRONT: front(LINK_BACK), "https://www.janedoe.art/": front(LINK_BACK), f"{SITE}/home": front(LINK_BACK)})
    web.routes.update(answers)
    refused = web.open()
    assert isinstance(refused, FetchResult) and refused.skip == skip and refused.content == b""
    assert web.urls == asked and len(asked) <= VERIFY_REQUEST_BUDGET
    web.routes.update({ROBOTS: reply(404), TDM: reply(404), FRONT: front(LINK_BACK)})  # too late
    for fetcher in (web.fetcher, web.new_fetcher()):
        assert fetcher.open_site(FRONT, PROFILE).skip == "not_own_site"
        assert fetcher.open_site("https://www.janedoe.art/", PROFILE).skip == "not_own_site"
        assert Site(fetcher, FRONT, PROFILE).fetch(f"{SITE}/home").skip == "not_own_site"
    assert web.urls == asked


def test_a_name_at_a_private_address_is_not_asked_for_proof():
    web = Web({FRONT: front(LINK_BACK)})
    web.dns["janedoe.art"] = ["192.168.1.1"]
    assert web.open().skip == "bad_url"
    assert web.requests == []
    web.dns.clear()
    assert web.open().skip == "not_own_site"  # closed all the same
    assert web.requests == []


BAD_PROFILES = [
    "",
    "janedoe",
    "https://www.deviantart.com/",
    "https://www.deviantart.com",
    "https://www.artstation.com/janedoe",
    "https://deviantart.com.evil.example/janedoe",
    "https://www.deviantart.com.evil.example/janedoe",
    "ftp://www.deviantart.com/janedoe",
    "https://www.deviantart.com:8443/janedoe",
    "https://janedoe:pw@www.deviantart.com/janedoe",
    "https://www.deviantart.com/jane doe",
    "https://www.deviantart.com/jane.doe",
    "https://www.deviantart.com/%2e%2e/janedoe",
    "https://www.deviantart.com/\ud83d",
    None,
    42,
]


@pytest.mark.parametrize("profile", BAD_PROFILES, ids=short)
def test_a_claim_needs_a_deviantart_profile(profile):
    web = Web({FRONT: front(LINK_BACK, '<a href="https://www.artstation.com/janedoe">ArtStation</a>')})
    assert web.open(profile=profile) == FetchResult(FRONT, skip="bad_url")
    assert Site(web.fetcher, FRONT, profile).fetch(FRONT).skip == "not_own_site"
    assert web.requests == [] and web.lookups == []
    assert isinstance(web.open(), Site)  # and a claim that was never looked into closes nothing


@pytest.mark.parametrize(
    ("root", "home"),
    [
        ("http://janedoe.art/", "http://janedoe.art/"),
        ("https://www.janedoe.art", "https://www.janedoe.art/"),
        ("HTTPS://JANEDOE.ART/#", "https://janedoe.art/"),
    ],
)
def test_a_site_is_proven_from_whichever_front_page_was_given(root, home):
    web = Web({home: front(LINK_BACK), f"{home}gallery": html()})
    site = web.open(root)
    assert isinstance(site, Site) and site.front_page.ok
    assert web.urls == [f"{home}robots.txt", f"{home}.well-known/tdmrep.json", home]
    assert site.fetch(f"{home}gallery").ok
    assert site.fetch(f"{SITE}/a").skip == "error"  # any host or scheme of the proven site, each under its own rules


def test_a_proven_site_is_proven_for_every_fetcher_and_is_not_asked_twice():
    web = Web({FRONT: front(LINK_BACK), f"{SITE}/a": html(), "https://www.janedoe.art/a": html()})
    first = web.open()
    again = web.open()
    elsewhere = web.open(
        "https://www.janedoe.art/", "https://deviantart.com/JANEDOE/gallery", fetcher=web.new_fetcher()
    )
    assert all(isinstance(site, Site) for site in (first, again, elsewhere))
    assert first.front_page.ok and again.front_page is None and elsewhere.front_page is None
    assert web.urls == PROOF
    assert again.fetch(f"{SITE}/a").ok and elsewhere.fetch("https://www.janedoe.art/a").ok
    assert len(crawl._SPENT) == 1 and crawl._SPENT["janedoe.art"] == len(web.requests)  # the proof counts in the budget


def test_a_second_account_claiming_a_proven_site_closes_it():
    web = Web(
        {FRONT: front(LINK_BACK, '<a href="https://www.deviantart.com/a-friend">a friend</a>'), f"{SITE}/a": html()}
    )
    site = web.open()
    assert site.fetch(f"{SITE}/a").ok
    asked = list(web.urls)
    assert web.open(profile="https://www.deviantart.com/someone-else") == FetchResult(FRONT, skip="not_own_site")
    assert site.fetch(f"{SITE}/a").skip == "not_own_site"  # one site, two claimants: nobody's own site
    assert web.open().skip == "not_own_site"
    assert web.open(profile="https://www.deviantart.com/a-friend").skip == "not_own_site"
    assert web.urls == asked


def test_only_the_account_that_was_proven_can_fetch():
    web = Web({FRONT: front(LINK_BACK), f"{SITE}/a": html()})
    assert isinstance(web.open(), Site)
    forged = Site(web.fetcher, FRONT, "https://www.deviantart.com/someone-else")
    assert forged.fetch(f"{SITE}/a").skip == "not_own_site"
    assert (
        Site(web.fetcher, "https://other-artist.com/", PROFILE).fetch("https://other-artist.com/").skip
        == "not_own_site"
    )
    assert web.urls == PROOF


def test_an_unproven_site_is_never_asked_a_fourth_request():
    web = Web({FRONT: front(), f"{SITE}/a": html()})
    assert web.open().skip == "not_own_site"
    crawl._OWNERS.pop("janedoe.art")  # even if the record of the failed claim were lost
    assert Site(web.fetcher, FRONT, PROFILE).fetch(f"{SITE}/a").skip == "not_own_site"
    assert web.open().skip == "not_own_site" and web.open().skip == "not_own_site"
    assert len(web.requests) == VERIFY_REQUEST_BUDGET


UNLISTED_PLATFORMS = """
    thumbtack.com bark.com taskrabbit.com freelancehunt.com fl.ru mihuashi.com truthsocial.com mas.to foriio.com
    xfolio.jp linktree.com rentry.co trello.com hubstaff.com flexjobs.com weworkremotely.com remoteok.com
    codementor.io gigster.com arc.dev polywork.com read.cv peerlist.io layers.to glass.photo vero.co minds.com
    gab.com gettr.com mewe.com odysee.com rumble.com bitchute.com taringa.net hive.blog steemit.com dev.to
    hashnode.com producthunt.com meetup.com eventbrite.com craigslist.org gumtree.com kijiji.ca yelp.com
    nextdoor.com houzz.com angi.com
""".split()


@pytest.mark.parametrize("www", ["", "www."])
@pytest.mark.parametrize("domain", UNLISTED_PLATFORMS)
def test_an_unlisted_platform_home_page_gets_three_requests_and_never_another(domain, www):
    home = f"https://{www}{domain}/"
    assert is_own_site(home)  # no list names it, and its shape is that of an artist's front page
    members = "".join(
        f'<a href="/pro/{n}">a member</a><a href="https://www.deviantart.com/member{n}">their DeviantArt</a>'
        for n in range(40)
    )
    web = Web({home: html(f"<html><body><a href='/login'>Log in</a>{members}</body></html>"), f"{home}pro/1": html()})
    assert web.open(home) == FetchResult(home, skip="not_own_site")
    assert web.urls == [f"{home}robots.txt", f"{home}.well-known/tdmrep.json", home]
    other_form = f"https://{'' if www else 'www.'}{domain}/"
    for fetcher in (web.fetcher, web.new_fetcher()):
        for root in (home, other_form, home.replace("https:", "http:")):
            assert fetcher.open_site(root, PROFILE).skip == "not_own_site"
            assert (
                fetcher.open_site(root, "https://www.deviantart.com/member1").skip == "not_own_site"
            )  # closed is closed
            assert Site(fetcher, root, PROFILE).fetch(f"{home}pro/1").skip == "not_own_site"
            assert Site(fetcher, root, "https://www.deviantart.com/member1").fetch(home).skip == "not_own_site"
    assert len(web.requests) == VERIFY_REQUEST_BUDGET


OFF_THE_SITE = PLATFORM_URLS + [
    "https://some-unlisted-platform.com/",
    "https://some-unlisted-platform.com/janedoe",
    "https://other-artist.com/",
    "https://janedoe.com/",
    "https://janedoe.art.br/",
    "https://notjanedoe.art/",
    "https://janedoe.art.evil.example.com/",
    # Asked for directly in round 2, and requested then.
    "https://www.malt.fr/profile/janedoe",
    "https://i.pximg.net/img-original/img/1.png",
    "https://telegra.ph/janedoe-01-01",
    "https://onlyfans.com/janedoe",
    "https://vsco.co/janedoe",
    "https://www.zcool.com.cn/u/1",
    "https://www.therookies.co/u/janedoe",
    "https://cubebrush.co/janedoe",
    "http://localhost/",
    "http://127.0.0.1/",
    "http://192.168.1.1/admin",
]


@pytest.mark.parametrize("url", OFF_THE_SITE, ids=short)
def test_a_site_handle_fetches_nothing_that_is_not_on_its_site(url):
    web = Web({url: html()})
    site = web.site()
    assert site.fetch(url) == FetchResult(url, skip="offsite")
    assert site.fetch(url, image=True).skip == "offsite"
    assert web.requests == [] and web.lookups == []


@pytest.mark.parametrize("url", BAD_URLS, ids=short)
def test_a_site_handle_fetches_no_bad_url(url):
    web = Web()
    assert web.site().fetch(url).skip in ("bad_url", "offsite")
    assert web.requests == [] and web.lookups == []


PRIVATE_ADDRESSES = [
    ["127.0.0.1"],
    ["127.8.8.8"],
    ["10.0.0.5"],
    ["172.16.0.1"],
    ["192.168.1.1"],
    ["169.254.169.254"],  # cloud metadata
    ["100.64.0.1"],  # carrier-grade NAT
    ["0.0.0.0"],
    ["224.0.0.1"],  # multicast
    ["::1"],
    ["fe80::1"],
    ["fe80::1%eth0"],
    ["fc00::1"],
    ["::ffff:127.0.0.1"],
    ["::ffff:10.0.0.5"],
    ["fec0::1"],  # site-local
    ["::127.0.0.1"],  # an IPv4 address in the old "compatible" form
    ["::8.8.8.8"],
    ["64:ff9b::7f00:1"],  # 127.0.0.1 behind a NAT64 translator
    ["64:ff9b::a00:5"],  # 10.0.0.5 likewise
    ["2002:7f00:1::"],  # 6to4
    ["93.184.216.34", "127.0.0.1"],  # one bad address among good ones is enough
    ["not an address"],
    [],
]


@pytest.mark.parametrize("scheme", ["https", "http"])
@pytest.mark.parametrize("addresses", PRIVATE_ADDRESSES, ids=short)
def test_a_name_that_resolves_to_a_private_address_is_never_connected_to(addresses, scheme):
    web = Web({f"{scheme}://janedoe.art/admin": html("the router's admin page")})
    web.dns["janedoe.art"] = addresses
    result = web.fetch(f"{scheme}://janedoe.art/admin")
    assert (result.skip, result.content) == ("bad_url", b"")
    assert web.requests == [] and web.lookups == ["janedoe.art"]


def test_a_name_that_does_not_resolve_is_skipped():
    web = Web({f"{SITE}/a": html()})
    web.dns["janedoe.art"] = socket.gaierror("no such host")
    assert web.get("/a").skip == "error"
    assert web.requests == []


def test_every_request_is_preceded_by_a_fresh_look_at_the_address():
    web = Web({f"{SITE}/a": html(), f"{SITE}/to-intranet": redirect("https://intranet.janedoe.art/")})
    web.routes["https://intranet.janedoe.art/"] = html("staff only")
    assert web.get("/a").ok
    assert web.lookups == ["janedoe.art"] * 3  # robots.txt, tdmrep.json, the page: one look each, no second one
    web.dns["intranet.janedoe.art"] = ["10.0.0.5"]
    assert web.get("/to-intranet").skip == "bad_url"  # a redirect hop is checked like any other request
    web.dns["janedoe.art"] = ["127.0.0.1"]  # the name now points somewhere else
    assert web.get("/a").skip == "bad_url"
    assert web.urls == [ROBOTS, TDM, f"{SITE}/a", f"{SITE}/to-intranet"]


PUBLIC_ADDRESSES = [  # (what the name resolves to, the one address the connection may go to)
    (["93.184.216.34"], "93.184.216.34"),
    (["2606:4700::6810:84e5"], "2606:4700::6810:84e5"),
    (["2606:4700::6810:84e5", "151.101.1.69", "93.184.216.34"], "151.101.1.69"),  # an IPv4 address if there is one
    (["64:ff9b::5db8:d822"], "64:ff9b::5db8:d822"),  # a public address behind a NAT64 translator
]


@pytest.mark.parametrize(("addresses", "pinned"), PUBLIC_ADDRESSES, ids=short)
def test_the_connection_goes_to_the_address_that_was_checked(addresses, pinned):
    web = Web({f"{SITE}/a": redirect("https://www.janedoe.art/b"), "https://www.janedoe.art/b": html()})
    web.dns["janedoe.art"] = addresses
    web.dns["www.janedoe.art"] = ["8.8.4.4"]
    assert web.get("/a").ok
    assert len(web.pins) == 6  # two policy files and a page on each host
    assert web.pins[:3] == [("janedoe.art", pinned, "janedoe.art")] * 3
    assert web.pins[3:] == [("www.janedoe.art", "8.8.4.4", "www.janedoe.art")] * 3
    assert web.lookups == ["janedoe.art"] * 3 + ["www.janedoe.art"] * 3  # one look per request, and no second one
    assert all(
        request.headers["connection"] == "close" for request in web.requests
    )  # so none is reused for another name


def test_each_request_connects_to_the_address_checked_for_it_and_looks_no_name_up_twice():
    web = Web({f"{SITE}/a": html()})
    answers = iter([["93.184.216.34"], ["151.101.1.69"], ["8.8.4.4"], ["192.168.1.1"]])  # the name keeps moving
    web.fetcher._resolve = lambda host: next(answers)
    assert web.get("/a").ok
    assert dict(zip(web.urls, (address for _, address, _ in web.pins), strict=True)) == {
        f"{SITE}/a": "93.184.216.34",  # checked first, requested last
        ROBOTS: "151.101.1.69",
        TDM: "8.8.4.4",
    }
    assert next(answers) == ["192.168.1.1"]  # nobody asked again, so nothing ever connected here


@pytest.mark.parametrize("policy", ["robots.txt", ".well-known/tdmrep.json"])
@pytest.mark.parametrize("address", ["10.0.0.5", "127.0.0.1", "fec0::1"])
def test_a_policy_file_is_never_fetched_from_a_private_address(policy, address):
    inside = f"https://intranet.janedoe.art/{policy}"
    web = Web({f"{SITE}/{policy}": redirect(inside), inside: reply(200, ""), f"{SITE}/a": html()})
    web.dns["intranet.janedoe.art"] = [address]
    assert web.get("/a").skip in ("robots", "error")
    assert inside not in web.urls and f"{SITE}/a" not in web.urls
    assert all(connected == "93.184.216.34" for _, connected, _ in web.pins)


@pytest.mark.parametrize("policy", ["robots.txt", ".well-known/tdmrep.json"])
def test_a_policy_file_of_a_name_that_turns_private_is_not_fetched(policy):
    web = Web({f"{SITE}/a": html()})
    later = {"robots.txt": [["10.0.0.5"]], ".well-known/tdmrep.json": [["93.184.216.34"], ["10.0.0.5"]]}[policy]
    answers = iter([["93.184.216.34"], *later])  # the page's own check passes; the policy file's does not
    web.fetcher._resolve = lambda host: next(answers)
    assert web.get("/a").skip in ("robots", "error")
    assert f"{SITE}/{policy}" not in web.urls and f"{SITE}/a" not in web.urls


def test_without_an_injected_resolver_the_systems_answer_is_checked(monkeypatch):
    asked = []

    def getaddrinfo(host, *args, **kwargs):
        asked.append(host)
        return [(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("127.0.0.1", 0))]

    monkeypatch.setattr(socket, "getaddrinfo", getaddrinfo)
    requests = []
    clock = Clock()
    transport = httpx.MockTransport(lambda request: requests.append(request) or httpx.Response(200))
    fetcher = PoliteFetcher(transport=transport, clock=clock, sleep=clock.sleep)
    crawl._OWNERS["janedoe.art"] = "janedoe"  # a site already proven
    assert Site(fetcher, SITE, PROFILE).fetch(f"{SITE}/admin").skip == "bad_url"
    assert asked == ["janedoe.art"] and requests == []


def test_the_environment_cannot_add_a_proxy_or_credentials(monkeypatch):
    for name in ("HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY"):
        monkeypatch.setenv(name, "http://127.0.0.1:9")
    client = PoliteFetcher()._client
    assert client.trust_env is False
    assert client._mounts == {}  # where httpx would have mounted a proxy for each of those variables


def test_only_get_is_ever_sent():
    web = Web({f"{SITE}/a": redirect("/b", 307), f"{SITE}/b": redirect("/c", 308), f"{SITE}/c": html()})
    assert web.get("/a").ok
    web.get("/img.png", image=True)
    assert len(web.requests) == 6
    assert {request.method for request in web.requests} == {"GET"}
    assert all(request.read() == b"" for request in web.requests)


def test_no_cookie_or_credential_is_ever_sent():
    cookie = {"set-cookie": "session=abc123; Path=/; Max-Age=3600"}
    web = Web(
        {
            ROBOTS: reply(200, "", cookie),
            TDM: reply(404, headers=cookie),
            f"{SITE}/a": reply(302, headers={**cookie, "location": "/b"}),
            f"{SITE}/b": html(headers=cookie),
            f"{SITE}/members": reply(401, headers={**cookie, "www-authenticate": 'Basic realm="members"'}),
        }
    )
    for path in ("/a", "/members", "/b", "/a", "/members"):
        web.get(path)
    assert len(web.requests) == 9
    for request in web.requests:
        assert "cookie" not in request.headers
        assert "authorization" not in request.headers
        assert "proxy-authorization" not in request.headers


@pytest.mark.parametrize("status", [401, 403])
def test_pages_behind_a_login_are_skipped(status):
    web = Web({f"{SITE}/members": reply(status, b"members only", {"content-type": "text/html"})})
    result = web.get("/members")
    assert (result.skip, result.status, result.content) == ("auth", status, b"")
    assert web.urls == [ROBOTS, TDM, f"{SITE}/members"]  # asked once, never retried with credentials


@pytest.mark.parametrize("status", [204, 304, 400, 404, 410, 429, 500, 503])
def test_other_failures_are_skipped_as_errors(status):
    web = Web({f"{SITE}/a": reply(status, b"" if status in (204, 304) else b"sorry")})
    result = web.get("/a")
    if status == 204:
        assert result.ok and result.content == b""
    else:
        assert (result.skip, result.status, result.content) == ("error", status, b"")


@pytest.mark.parametrize(
    ("start", "location", "final"),
    [
        (f"{SITE}/old", "/new", f"{SITE}/new"),
        (f"{SITE}/dir/old", "new?x=1", f"{SITE}/dir/new?x=1"),
        ("http://janedoe.art/", "https://janedoe.art/", "https://janedoe.art/"),
        (f"{SITE}/", "https://www.janedoe.art/", "https://www.janedoe.art/"),
        ("https://shop.janedoe.co.uk/", "//www.janedoe.co.uk/shop", "https://www.janedoe.co.uk/shop"),
        ("https://janedoe.art.br/", "https://www.janedoe.art.br/", "https://www.janedoe.art.br/"),
    ],
)
@pytest.mark.parametrize("status", [301, 302, 303, 307, 308])
def test_redirects_inside_the_site_are_followed(start, location, final, status):
    web = Web({start: redirect(location, status), final: html("<p>moved here</p>")})
    result = web.fetch(start)
    assert (result.url, result.content, result.skip) == (final, b"<p>moved here</p>", None)


@pytest.mark.parametrize(
    ("location", "skip"),
    [
        ("https://someone-else.com/janedoe", "offsite"),
        ("https://janedoe.art.someone-else.com/", "offsite"),
        ("https://notjanedoe.art/", "offsite"),
        ("https://janedoe.com/", "offsite"),
        ("https://www.artstation.com/janedoe", "platform"),
        ("https://janedoe.tumblr.com/", "platform"),
        ("//instagram.com/janedoe", "platform"),
        ("https://janedoe.github.io/", "platform"),
        ("https://jane:hunter2@janedoe.art/private", "bad_url"),
        ("https://janedoe.art:8443/", "bad_url"),
        ("http://127.0.0.1/admin", "bad_url"),
        ("http://localhost:11434/api/tags", "bad_url"),
        ("http://janedoe.localhost/", "bad_url"),
        ("/public/%2e%2e/private", "bad_url"),
        ("ftp://janedoe.art/a", "bad_url"),
        ("file:///etc/passwd", "bad_url"),
        ("https://[::1", "error"),
    ],
)
def test_a_redirect_that_leaves_the_site_is_not_followed(location, skip):
    web = Web({f"{SITE}/links": redirect(location)})
    result = web.get("/links")
    assert result.skip == skip
    assert result.content == b""
    assert web.urls == [ROBOTS, TDM, f"{SITE}/links"]  # nothing was asked of the redirect target, not even robots.txt


@pytest.mark.parametrize(
    ("start", "location"),
    [
        ("https://janedoe.co.uk/links", "https://stranger.co.uk/"),
        ("https://janedoe.art.br/links", "https://stranger.art.br/"),
        ("https://janedoe.id.au/links", "https://stranger.id.au/"),
        ("https://janedoe.me.uk/links", "https://stranger.me.uk/"),
        ("https://janedoe.ne.jp/links", "https://stranger.ne.jp/"),
    ],
)
def test_neighbours_under_one_public_suffix_are_different_sites(start, location):
    web = Web({start: redirect(location), location: html()})
    assert web.fetch(start).skip == "offsite"
    assert location not in web.urls
    assert web.fetch(location).ok
    assert web.times[3] == web.times[2]  # and the neighbour does not wait on the first site's clock


def test_every_hop_is_checked_against_robots_and_tdmrep():
    web = Web(
        {
            ROBOTS: reply(200, "User-agent: *\nDisallow: /private"),
            f"{SITE}/a": redirect("/private/b"),
            f"{SITE}/private/b": html(),
            f"{SITE}/www": redirect("https://www.janedoe.art/gallery"),
            "https://www.janedoe.art/robots.txt": reply(200, "User-agent: *\nDisallow: /gallery"),
            "https://www.janedoe.art/gallery": html(),
            f"{SITE}/ai": redirect("https://ai.janedoe.art/"),
            "https://ai.janedoe.art/robots.txt": reply(200, "User-agent: GPTBot\nDisallow: /"),
            "https://ai.janedoe.art/": html(),
            f"{SITE}/down": redirect("https://down.janedoe.art/"),
            "https://down.janedoe.art/robots.txt": reply(500),
            "https://down.janedoe.art/": html(),
            f"{SITE}/tdm": redirect("https://tdm.janedoe.art/"),
            "https://tdm.janedoe.art/.well-known/tdmrep.json": reply(200, '[{"location": "/", "tdm-reservation": 1}]'),
            "https://tdm.janedoe.art/": html(),
        }
    )
    assert web.get("/a").skip == "robots"
    assert web.get("/www").skip == "robots"
    assert web.get("/ai").skip == "ai_opt_out"
    assert web.get("/down").skip == "robots"
    assert web.get("/tdm").skip == "ai_opt_out"
    for never in (f"{SITE}/private/b", "https://www.janedoe.art/gallery", "https://ai.janedoe.art/"):
        assert never not in web.urls
    assert "https://down.janedoe.art/" not in web.urls and "https://tdm.janedoe.art/" not in web.urls
    assert min(web.gaps) == MIN


def test_the_last_hops_opt_out_is_honored():
    web = Web({f"{SITE}/a": redirect("/b"), f"{SITE}/b": html(headers={"x-robots-tag": "noai"})})
    assert web.get("/a").skip == "ai_opt_out"


def test_a_redirect_loop_ends():
    web = Web({f"{SITE}/a": redirect("/b"), f"{SITE}/b": redirect("/a")})
    assert web.get("/a").skip == "error"
    assert len(web.requests) == 2 + MAX_REDIRECTS + 1  # the policy files, the first request, then the hops allowed


def test_the_size_caps_are_the_agreed_ones():
    assert PAGE_MAX_BYTES <= 2 * 1024 * 1024
    assert IMAGE_MAX_BYTES <= 15 * 1024 * 1024


@pytest.mark.parametrize(
    ("image", "size", "skip"),
    [
        (False, PAGE_MAX_BYTES, None),
        (False, PAGE_MAX_BYTES + 1, "too_large"),
        (True, PAGE_MAX_BYTES + 1, None),
        (True, IMAGE_MAX_BYTES, None),
        (True, IMAGE_MAX_BYTES + 1, "too_large"),
    ],
)
@pytest.mark.parametrize("declared", [True, False])
def test_the_size_cap_is_exact(image, size, skip, declared):
    route = reply(200, bytes(size)) if declared else stream(size, [])
    web = Web({f"{SITE}/big": route})
    result = web.get("/big", image=image)
    assert result.skip == skip
    assert len(result.content) == (0 if skip else size)


def test_a_declared_oversize_body_is_not_read_at_all():
    produced = []
    web = Web({f"{SITE}/big": stream(10**9, produced, headers={"content-length": str(PAGE_MAX_BYTES + 1)})})
    assert web.get("/big").skip == "too_large"
    assert produced == []


@pytest.mark.parametrize("headers", [None, {"content-length": "10"}, {"content-length": "lots"}])
def test_an_undeclared_or_understated_body_stops_being_read_at_the_cap(headers):
    produced = []
    web = Web({f"{SITE}/big": stream(10**9, produced, headers=headers)})
    assert web.get("/big").skip == "too_large"
    assert PAGE_MAX_BYTES < sum(produced) <= PAGE_MAX_BYTES + 65_536  # one chunk past the cap, not a gigabyte


@pytest.mark.parametrize("encoding", ["gzip", "x-gzip", "deflate"])
@pytest.mark.parametrize("extra", [1, 50_000_000])
def test_a_compressed_body_is_capped_after_decoding(encoding, extra):
    pack = zlib.compress if encoding == "deflate" else gzip.compress
    bomb = pack(bytes(PAGE_MAX_BYTES + extra))
    assert len(bomb) < 100_000
    web = Web({f"{SITE}/bomb": raw_reply(bomb, {"content-encoding": encoding})})
    web.routes[f"{SITE}/fits"] = raw_reply(pack(bytes(PAGE_MAX_BYTES)), {"content-encoding": encoding})
    assert web.get("/bomb").skip == "too_large"
    assert len(web.get("/fits").content) == PAGE_MAX_BYTES


def test_a_body_of_endless_gzip_members_is_given_up_on():
    member = gzip.compress(b"x")
    web = Web({f"{SITE}/ok": raw_reply(member * GZIP_MEMBERS_MAX, {"content-encoding": "gzip"})})
    web.routes[f"{SITE}/endless"] = raw_reply(member * (GZIP_MEMBERS_MAX + 1), {"content-encoding": "gzip"})
    assert web.get("/ok").content == b"x" * GZIP_MEMBERS_MAX
    assert web.get("/endless").skip == "error"


def test_every_request_carries_timeouts():
    web = Web({f"{SITE}/a": redirect("/b"), f"{SITE}/b": html()})
    web.get("/a")
    web.get("/pic.png", image=True)
    assert len(web.requests) == 5
    for request in web.requests:
        timeout = request.extensions["timeout"]
        assert set(timeout) == {"connect", "read", "write", "pool"}
        assert all(0 < seconds <= 60 for seconds in timeout.values())


@pytest.mark.parametrize(
    ("headers", "first", "drop"),
    [
        (None, b"", b"."),  # a plain body, one byte at a time
        ({"content-encoding": "gzip"}, gzip.compress(b"art")[:10], b""),  # a gzip header, then nothing to decode
        ({"content-encoding": "gzip"}, b"", gzip.compress(b"")),  # gzip members that hold nothing
    ],
)
def test_a_slow_drip_is_cut_off(headers, first, drop):
    web = Web()
    drops = []

    def drip(request):
        def body():
            yield first
            for _ in range(10_000):
                web.clock.now += 20.0  # each chunk arrives inside the read timeout
                drops.append(1)
                yield drop

        return httpx.Response(200, headers=headers, content=body())

    web.routes[f"{SITE}/slow"] = drip
    result = web.get("/slow")
    assert (result.skip, result.content) == ("error", b"")
    assert len(drops) <= MAX_SECONDS_PER_RESPONSE / 20.0 + 1
    web.routes[f"{SITE}/a"] = html()
    web.get("/a")
    assert web.times[2] == 1000.0 + 2 * MIN
    assert web.times[3] - web.times[2] >= MAX_SECONDS_PER_RESPONSE + MIN  # the wait runs from the end of the drip


class Wire(httpcore.MockStream):
    """httpcore's in-memory connection, which MockTransport does not have: the answer is read a byte at a time, each
    read moving the clock `per_read` seconds (inside the read timeout), and a TLS handshake waits, in real time, for
    `handshake` to be set (one that never ends, as far as the fetcher can tell)."""

    def __init__(self, answer, clock, per_read, handshake):
        super().__init__([answer[i : i + 1] for i in range(len(answer))])
        self.clock, self.per_read, self.handshake = clock, per_read, handshake

    def read(self, max_bytes, timeout=None):
        self.clock.now += self.per_read
        return super().read(max_bytes, timeout)

    def start_tls(self, ssl_context, server_hostname=None, timeout=None):
        self.handshake.wait(5)
        return self


def over_the_wire(answer, clock, per_read=0.0, handshake=None):
    """A fetcher whose every request goes through httpx's own transport and httpcore to a Wire sending `answer`, and
    the list of the Wires it opened."""
    opened = []
    if handshake is None:
        handshake = threading.Event()
        handshake.set()

    class Network(httpcore.MockBackend):
        def connect_tcp(self, *args, **kwargs):
            opened.append(Wire(answer, clock, per_read, handshake))
            return opened[-1]

    transport = httpx.HTTPTransport()
    transport._pool = httpcore.ConnectionPool(network_backend=Network([]))
    fetcher = PoliteFetcher(
        transport=transport, clock=clock, sleep=clock.sleep, resolver=lambda host: ["93.184.216.34"]
    )
    return fetcher, opened


DRIPPED_HEAD = b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nX-Pad: " + b"a" * 100_000 + b"\r\n\r\n"


def test_a_head_that_drips_is_cut_off_at_the_deadline():
    """The critic's tarpit: the head a byte every 14 s, each inside the 15 s read timeout, and 100 kB of it."""
    clock = Clock()
    fetcher, opened = over_the_wire(DRIPPED_HEAD, clock, per_read=14.0)
    crawl._OWNERS["janedoe.art"] = "janedoe"
    began = clock.now
    assert Site(fetcher, SITE, PROFILE).fetch(f"{SITE}/a").skip == "robots"  # robots.txt took too long: closed
    assert len(opened) == 1 and clock.now - began <= MIN + MAX_SECONDS_PER_RESPONSE + 14.0


def test_a_site_that_drips_costs_a_proof_one_short_deadline_and_nothing_more_this_run():
    clock = Clock()
    fetcher, opened = over_the_wire(DRIPPED_HEAD, clock, per_read=14.0)
    began = clock.now
    assert fetcher.read_proof(f"{SITE}/", "artist-search-removal", lambda said: True) == "robots"
    assert len(opened) == 1 and clock.now - began <= MIN + crawl.PROOF_SECONDS_PER_RESPONSE + 14.0
    # Closed for the run: the next look, by any fetcher, asks the site nothing.
    again, _ = over_the_wire(DRIPPED_HEAD, clock, per_read=14.0)
    assert again.read_proof(f"{SITE}/", "artist-search-removal", lambda said: True) == "budget"
    assert len(opened) == 1


def test_a_tls_handshake_that_never_ends_is_left_to_the_site_at_the_deadline(monkeypatch):
    """No read of ours runs inside a handshake, so only the deadline itself, in real time, can end the wait."""
    monkeypatch.setattr(crawl, "PROOF_SECONDS_PER_RESPONSE", 0.2)
    handshake = threading.Event()
    fetcher, opened = over_the_wire(b"", Clock(), handshake=handshake)
    began = time.monotonic()
    try:
        assert fetcher.read_proof(f"{SITE}/", "artist-search-removal", lambda said: True) == "robots"
        assert time.monotonic() - began < 2 and len(opened) == 1
    finally:
        handshake.set()


# --- 7. A skip never raises ----------------------------------------------------------------------------------------


@pytest.mark.parametrize(
    "failure",
    [
        httpx.ConnectError("refused"),
        httpx.ConnectTimeout("slow"),
        httpx.ReadTimeout("slow"),
        httpx.RemoteProtocolError("garbage"),
        httpx.DecodingError("bad gzip"),
        httpx.TooManyRedirects("loop"),
        OSError("socket closed"),
        RuntimeError("a bug somewhere"),
    ],
    ids=short,
)
@pytest.mark.parametrize("where", ["page", "robots", "tdmrep"])
def test_network_failures_come_back_as_skips(failure, where):
    web = Web({f"{SITE}/a": html()})
    web.routes[{"page": f"{SITE}/a", "robots": ROBOTS, "tdmrep": TDM}[where]] = failure
    result = web.get("/a")
    assert isinstance(result, FetchResult)
    assert result.skip in ("error", "robots") and result.content == b""
    assert (f"{SITE}/a" in web.urls) == (where == "page")


class Unprintable:
    """Something whose str() and repr() raise."""

    def __str__(self):
        raise RuntimeError("no string for you")

    __repr__ = __str__


NONSENSE = [None, 42, b"https://janedoe.art/", ["https://janedoe.art/"], object(), Unprintable()]


@pytest.mark.parametrize("url", NONSENSE, ids=["none", "int", "bytes", "list", "object", "unprintable"])
def test_nonsense_input_comes_back_as_a_skip(url):
    web = Web()
    assert web.open(root=url).skip == "bad_url"
    assert web.open(profile=url).skip == "bad_url"
    assert web.site().fetch(url).skip == "bad_url"
    assert web.site().fetch(url, image=True).url == ""
    assert Site(web.fetcher, url, PROFILE).fetch(SITE).skip == "bad_url"
    assert Site(web.fetcher, SITE, url).fetch(SITE).skip == "not_own_site"
    assert web.requests == [] and web.lookups == []
    assert is_own_site(url) is False


@pytest.mark.parametrize("encoding", ["gzip", "x-gzip", "deflate"])
def test_a_broken_compressed_body_comes_back_as_a_skip(encoding):
    web = Web({f"{SITE}/a": raw_reply(b"\xff\xfenot compressed at all", {"content-encoding": encoding})})
    assert web.get("/a").skip == "error"


def test_the_result_carries_content_or_a_reason():
    png = b"\x89PNG\r\n\x1a\n" + bytes(100)
    web = Web({f"{SITE}/a.png": reply(200, png, {"content-type": "image/png"}), f"{SITE}/old": redirect("/a.png")})
    good = web.get("/old", image=True)
    assert good == FetchResult(f"{SITE}/a.png", png, "image/png", 200, None)
    assert good.ok
    bad = web.open("https://www.deviantart.com/")
    assert bad == FetchResult("https://www.deviantart.com/", b"", "", 0, "platform")
    away = web.site().fetch("https://someone-else.com/a.png", image=True)
    assert away == FetchResult("https://someone-else.com/a.png", b"", "", 0, "offsite")
    assert not bad.ok
    with pytest.raises(AttributeError):
        bad.skip = None
