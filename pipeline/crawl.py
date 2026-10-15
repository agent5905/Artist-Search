"""The polite fetcher for artists' own portfolio sites.

Hard rules 4 and 5 and owner decision 2 live here, in one place. Later stages get pages and images in two steps,
and there is no other way in:

    site = fetcher.open_site("https://janedoe.art/", "https://www.deviantart.com/janedoe")
    page = site.fetch("https://janedoe.art/gallery")   # a FetchResult: the content, or the reason it was skipped

open_site returns a Site only once the front page has been seen to link back to that DeviantArt profile, and a
FetchResult saying why not otherwise. Proving it costs at most VERIFY_REQUEST_BUDGET requests (robots.txt,
tdmrep.json, the front page). A site that fails is closed for the rest of the run, for every fetcher. Neither step
raises, nothing is requested that is not on a proven site, and no site is asked for more than SITE_REQUEST_BUDGET
requests in one process.

One narrower door, for hard rule 7 only: read_proof reads the two places where an artist puts a removal proof code
on their own site, and nothing else (see its docstring).
"""

import concurrent.futures
import fnmatch
import ipaddress
import json
import logging
import math
import re
import socket
import threading
import time
import zlib
from collections.abc import Callable, Iterable, Iterator
from dataclasses import dataclass
from html import unescape
from html.parser import HTMLParser
from http.cookiejar import DefaultCookiePolicy
from typing import Any, Literal
from urllib.parse import unquote

import httpcore
import httpx
import tldextract

from pipeline import config

log = logging.getLogger(__name__)

PAGE_MAX_BYTES = 2_000_000
IMAGE_MAX_BYTES = 15_000_000
POLICY_MAX_BYTES = 512_000  # robots.txt and tdmrep.json. RFC 9309 asks crawlers to read at least 500 KiB
TIMEOUT = httpx.Timeout(15.0)  # connect, read, write and pool, each
# One deadline a request, from connecting to the last byte: the timeouts above are per read, so a slow drip of the
# handshake, the head or the body needs a limit of its own (see _send). A site that reaches it is closed for the run.
MAX_SECONDS_PER_RESPONSE = 120.0
PROOF_SECONDS_PER_RESPONSE = 20.0  # the same for a removal proof: a small file and a page, and a run looks at 30 sites
MAX_CRAWL_DELAY = 60.0  # a host that asks for more patience than this is treated as closed to us
MAX_REDIRECTS = 5
SITE_REQUEST_BUDGET = 60  # requests of one site per process, everything counted. A mistaken site stays a small one
VERIFY_REQUEST_BUDGET = 3  # all a site is asked until its front page proves whose it is: robots.txt, tdmrep.json, it
# All a site is asked for a removal proof: robots.txt, the proof file and the front page, each perhaps once more on
# the other side of an http-to-https or www redirect, which brings a robots.txt of its own.
PROOF_REQUEST_BUDGET = 8
PROOF_REDIRECTS = 3
STALLED_CLOCK_SECONDS = 0.1  # real time. A clock that reads the same for this long has stopped, not just ticked late
META_TAG_MAX = 100_000  # characters. A <meta> tag that does not end within this is treated as an opt-out
GZIP_MEMBERS_MAX = 64  # no real page has more than one; each further one costs a copy of what is left
ROBOTS_PATH = "/robots.txt"
TDMREP_PATH = "/.well-known/tdmrep.json"

Skip = Literal[
    "bad_url",  # not a plain public http(s) URL
    "platform",  # a listed platform, or a subdomain handed out by a host
    "not_own_site",  # open_site only: not the front page of a bare custom domain
    "offsite",  # not on the site that was opened, or a redirect that leaves it
    "robots",
    "ai_opt_out",
    "auth",
    "too_large",
    "budget",  # the site's request budget is spent, or it answered 429 or 503
    "error",
]

# Owner decision 2: a site that shuts any of these out has opted out of AI use, so we stay out too.
AI_CRAWLER_TOKENS = (
    "GPTBot",
    "ChatGPT-User",
    "OAI-SearchBot",
    "ClaudeBot",
    "anthropic-ai",
    "Claude-Web",
    "Claude-SearchBot",
    "Claude-User",
    "CCBot",
    "Google-Extended",
    "PerplexityBot",
    "Perplexity-User",
    "Bytespider",
    "Applebot-Extended",
    "Meta-ExternalAgent",
    "meta-externalfetcher",
    "FacebookBot",
    "Amazonbot",
    "cohere-ai",
    "Diffbot",
    "omgili",
    "Omgilibot",
    "ImagesiftBot",
    "img2dataset",
    "YouBot",
    "Timpibot",
    "AI2Bot",
    "PetalBot",
    "DuckAssistBot",
    "Google-CloudVertexBot",
    "cohere-training-data-crawler",
    "Webzio-Extended",
    "MistralAI-User",
    "Ai2Bot-Dolma",
    "GoogleOther",
    "TikTokSpider",
    "PanguBot",
    "Kangaroo Bot",
)
OPT_OUT_DIRECTIVES = frozenset({"noai", "noimageai", "noindex", "none"})


def _agent(value: str) -> str:
    """The product token of a robots.txt User-agent value, in lower case: "ArtistSearchBot 0.1" is "artistsearchbot"."""
    token = re.match(r"[\w-]+|\*", value.strip().lower())
    return token.group() if token else ""


_US = _agent(config.ROBOTS_TOKEN)
_AI_AGENTS = tuple(_agent(token) for token in AI_CRAWLER_TOKENS)
# Headers and <meta> names whose directives bind us: the general ones, ours, and any AI crawler's.
_ROBOTS_NAMES = frozenset({"robots", "x-robots-tag", _US, *(token.lower() for token in AI_CRAWLER_TOKENS)})
# Content-Signal: ai-train=no / ai-input=no, and Content-Usage: train-ai=n, in robots.txt or in a header.
_AI_RESERVED = re.compile(r"\b(?:ai-train|ai-input|train-ai|train-genai)\s*=\s*no?\b")

# The denylist. It is the second layer, and no list can be complete: the first layer is that nothing is fetched
# except on a site opened from a front page that passed is_own_site, which needs no list, and that every host under
# a private Public Suffix List entry is refused. Listed here: gig platforms (hard rule 5), social networks,
# galleries, hosted site builders, link pages, and the image hosts of all of those, including the image hosts of
# site builders that serve custom domains (wixstatic, squarespace-cdn, shopify, wp.com): the owner has not approved
# fetching those. A host is refused when ANY of its labels is one of these names, which covers every subdomain and
# every country domain (someone.blogspot.co.uk, pinterest.de).
# ponytail: label matching also refuses an artist's own subdomain named after a platform (patreon.janedoe.com).
PLATFORM_NAMES = frozenset(
    """
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
)
# Platforms whose name is too short or too ordinary to match as a label: these exact domains and their subdomains.
# A brand that lives under several country suffixes (malt.fr, vk.ru) belongs in the names above, not here.
# The last line is wildcard DNS services, whose names resolve to whatever address is written in them.
PLATFORM_DOMAINS = frozenset(
    """
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
)

# The bundled Public Suffix List snapshot, read offline. It gives the registrable domain (janedoe.art.br, not
# art.br) and says which suffixes are private: hosts that hand out subdomains (github.io, blogspot.com, uk.com).
_PSL = tldextract.TLDExtract(suffix_list_urls=(), cache_dir=None, include_psl_private_domains=True)
# Top levels that are on the list but are not the public web. A name with no public suffix at all (.localhost,
# .local, .lan, .internal, .test) is turned away without needing a list.
_SPECIAL_USE = frozenset({"arpa", "box", "onion"})
_HOSTNAME = re.compile(r"(?:[a-z0-9-]+\.)+[a-z0-9-]+")
_DEFAULT_PORT = {"http": 80, "https": 443}
_NAT64 = ipaddress.ip_network("64:ff9b::/96")

_META_OPEN = re.compile(r"<meta(?=[\s/>])", re.IGNORECASE)
_TAG = re.compile(r"<!--|<([a-zA-Z][^\s/>]*)")  # a comment, or the opening of a tag with its name
# Elements whose content is text, not markup: nothing inside them is a link.
_RAW_TEXT = {
    name: re.compile(rf"</{name}(?=[\s/>])", re.IGNORECASE)
    for name in ("script", "style", "textarea", "title", "xmp", "iframe", "noembed", "noframes", "plaintext")
}
_HANDLE = re.compile(r"[a-z0-9-]+")
# One step inside a tag: its closing ">", or the next attribute with its quoted or bare value.
_ATTRIBUTE = re.compile(r"""[\s/]*(?:(>)|([^\s/>][^\s/>=]*)(?:\s*=\s*(?:"([^"]*)"|'([^']*)'|([^\s>]*)))?)""")

# ponytail: one crawl process at a time is assumed. Every fetcher in the process shares these clocks and takes this
# one lock for a whole fetch, so fetches run one at a time; per-site locks if throughput ever matters. A second
# process on the same site would double its request rate.
_LOCK = threading.RLock()  # re-entrant, so a fetch made from inside a callback cannot deadlock
_LAST: dict[str, float] = {}  # site -> the clock reading when its last request ended
_DELAY: dict[str, float] = {}  # site -> seconds between requests, once a Crawl-delay has raised it
_SPENT: dict[str, int] = {}  # site -> requests made of it so far, out of SITE_REQUEST_BUDGET
# site -> the DeviantArt handle its front page links back to, or None once a claim on it has failed: closed for the run
_OWNERS: dict[str, str | None] = {}


@dataclass(frozen=True)
class FetchResult:
    """What one fetch produced: the content, or the reason it was skipped."""

    url: str  # the final URL after redirects, or the URL at which the fetch stopped
    content: bytes = b""
    content_type: str = ""
    status: int = 0  # status of the last response; 0 when there was none
    skip: Skip | None = None

    @property
    def ok(self) -> bool:
        return self.skip is None


def _host(url: httpx.URL) -> str:
    return url.raw_host.decode("ascii")


def _site(url: httpx.URL) -> str:
    """The registrable domain: one site for the rate limit, the budget and redirects. Empty when there is none."""
    return _PSL.extract_str(_host(url)).top_domain_under_public_suffix


def _unsafe_path(url: httpx.URL) -> bool:
    """True when a server might serve this path as a different one, so that robots.txt cannot be held against it.

    That is a `.` or `..` segment that httpx did not resolve (encoded as %2e, or carrying a path parameter: ..;),
    an empty segment (//private), an encoded slash (%2F) or a backslash, however deep the encoding goes.
    """
    sent = url.raw_path.partition(b"?")[0].decode("ascii")
    path = sent
    while "%" in path and unquote(path) != path:
        path = unquote(path)
    return (
        "\\" in path
        or "//" in path
        or path.count("/") != sent.count("/")
        or any(segment.partition(";")[0] in (".", "..") for segment in path.split("/"))
    )


def _refusal(url: httpx.URL) -> Skip | None:
    """Why this URL must not be requested at all, decided before any network traffic."""
    host = _host(url)
    parts = _PSL.extract_str(host)
    if (
        url.scheme not in _DEFAULT_PORT
        or url.port not in (None, _DEFAULT_PORT[url.scheme])
        or url.userinfo  # credentials in a URL would be sent as an Authorization header
        or not _HOSTNAME.fullmatch(host)
        or _unsafe_path(url)
    ):
        return "bad_url"
    labels = host.split(".")
    if (
        parts.is_private  # a subdomain handed out by a host, not a domain someone registered
        or not PLATFORM_NAMES.isdisjoint(labels)
        or any(".".join(labels[i:]) in PLATFORM_DOMAINS for i in range(len(labels)))
    ):
        return "platform"
    # No registrable domain: an IP address, a local name (.localhost, .local, .lan, .internal) or a bare suffix.
    if not parts.top_domain_under_public_suffix or parts.suffix.rpartition(".")[2] in _SPECIAL_USE:
        return "bad_url"
    return None


def _own_site_refusal(url: str) -> Skip | None:
    """Why a URL is not the front page of a bare custom domain, or None when it is one."""
    try:
        parsed = httpx.URL(url)
        host, site = _host(parsed), _site(parsed)
        reason = _refusal(parsed)
    except Exception:  # nothing that fails to parse is anyone's site
        return "bad_url"
    if reason:
        return reason
    front_page = parsed.path in ("", "/") and not parsed.query and not parsed.fragment
    return None if front_page and host in (site, f"www.{site}") else "not_own_site"


def is_own_site(url: str) -> bool:
    """True only for the front page of a bare custom domain: http(s)://janedoe.art/ or http(s)://www.janedoe.art/.

    This is a test of shape, so a platform nobody has listed still fails it: a profile on a platform always has
    a user path, a query, a fragment route or a user subdomain. Hosts the fetcher refuses fail it too.
    """
    return _own_site_refusal(url) is None


def _resolve(host: str) -> list[str]:
    return [str(info[4][0]) for info in socket.getaddrinfo(host, None, type=socket.SOCK_STREAM)]


def _public(address: str) -> bool:
    """True for an address on the public internet: not loopback, private, link-local, reserved or multicast."""
    try:
        ip = ipaddress.ip_address(address)
    except ValueError:
        return False
    if isinstance(ip, ipaddress.IPv6Address):
        if ip in _NAT64:  # an IPv4 address behind a translator: that is the address that has to be public
            ip = ipaddress.IPv4Address(int(ip) & 0xFFFFFFFF)
        elif int(ip) >> 32 == 0 or ip.is_site_local:  # ::a.b.c.d and fec0::/10, both retired, both local in effect
            return False
    return ip.is_global and not ip.is_multicast


def _decodings(body: bytes, *words: str) -> list[str]:
    """The body as UTF-8, and as each UTF-16 or UTF-32 flavour in which one of `words` can be found.

    The charset a server declares is never trusted: every reading that could hold the words is searched.
    """
    lowered = body.lower()  # `words` are given in lower case
    wide = [
        codec
        for codec in ("utf-16-le", "utf-16-be", "utf-32-le", "utf-32-be")
        if any(word.encode(codec) in lowered for word in words)
    ]
    return [body.decode(codec, "replace") for codec in ("utf-8", *wide)]


def _inflate(data: bytes, limit: int) -> tuple[bytes, bytes]:
    """One compressed stream off the front of `data`: what it holds, up to `limit` bytes, and what follows it."""
    for wbits in (47, -15):  # gzip or zlib, told apart by their headers; then deflate sent without its wrapper
        decoder = zlib.decompressobj(wbits)
        try:
            return decoder.decompress(data, limit), decoder.unused_data
        except zlib.error:
            continue
    raise httpx.DecodingError("the body is not what its Content-Encoding says")


def _decoded(raw: bytes, encoding: str, cap: int) -> bytes | None:
    """Undo Content-Encoding by hand: gzip under any of its names, every member, and never past `cap` (then None).

    httpx would leave x-gzip compressed, and a compressed body hides its <meta> tags from us.
    """
    for name in reversed([part.strip().lower() for part in encoding.split(",") if part.strip()]):
        if name == "identity":
            continue
        if name not in ("gzip", "x-gzip", "deflate"):
            raise httpx.DecodingError(f"cannot undo Content-Encoding {name!r}")
        data, raw = raw, b""
        for _ in range(GZIP_MEMBERS_MAX):
            piece, data = _inflate(data, cap + 1 - len(raw))
            raw += piece
            if len(raw) > cap:
                return None
            if not data:
                break
        else:
            raise httpx.DecodingError("too many compressed members")
    return raw


def _matches(pattern: str, path: str) -> bool:
    """RFC 9309 path matching: a prefix match where `*` is any run of characters and a final `$` anchors the end."""
    glob = pattern.replace("[", "[[]").replace("?", "[?]")  # only `*` is special in robots.txt
    return fnmatch.fnmatchcase(path, glob[:-1] if glob.endswith("$") else glob + "*")


def _octets(pattern: str) -> int:
    """How long RFC 9309 says a rule is: its octets once every byte outside ASCII is written as %XX."""
    return sum(1 if byte < 128 else 3 for byte in pattern.encode())


def _allowed(rules: list[tuple[bool, str]], path: str) -> bool:
    """The matching rule with the most octets decides, Allow wins a tie, and no match means allowed."""
    return max(((_octets(pattern), allow) for allow, pattern in rules if _matches(pattern, path)), default=(0, True))[1]


class _Robots:
    """One origin's robots.txt, read the RFC 9309 way.

    Not urllib.robotparser: it takes the first matching rule instead of the longest (so `Allow: /` ahead of
    `Disallow: /private/` opens everything), ignores `*` and `$`, and drops a fractional Crawl-delay.
    """

    def __init__(self, text: str) -> None:
        self.groups: dict[str, list[tuple[bool, str]]] = {}  # user agent -> [(allow, path pattern)]
        self.crawl_delay = 0.0  # the largest delay asked of us or of `*`
        self.reserved = False  # a Content-Signal or Content-Usage line keeps the site from AI use
        self.tdm: list[str] | None = None  # paths reserved by tdmrep.json; None until that file has been read
        agents: list[str] = []
        in_rules = False  # the first rule line closes the run of User-agent lines that opened the group
        for line in text.replace("\ufeff", "").splitlines():
            key, _, value = line.split("#", 1)[0].partition(":")
            key, value = key.strip().lower(), value.strip()
            if key == "user-agent":
                if in_rules:
                    agents, in_rules = [], False
                agents.append(_agent(value))
                self.groups.setdefault(agents[-1], [])
            elif key in ("allow", "disallow", "crawl-delay"):
                in_rules = True
                agents = agents or ["*"]  # a rule ahead of any User-agent line is read as meant for everyone
                if key == "crawl-delay":
                    number = re.match(r"\d+(?:[.,]\d+)?", value)  # "10", "4.5", "4,5", "10s", "10 seconds"
                    if number and (_US in agents or "*" in agents):
                        self.crawl_delay = max(self.crawl_delay, float(number.group().replace(",", ".")))
                elif value:  # an empty Disallow forbids nothing
                    for agent in agents:
                        self.groups.setdefault(agent, []).append((key == "allow", unquote(value)))
            elif key in ("content-signal", "content-usage"):
                self.reserved = self.reserved or bool(_AI_RESERVED.search(value.lower()))

    def allows(self, path: str) -> bool:
        """Our own group if the site wrote one, otherwise the `*` group."""
        return _allowed(self.groups.get(_US, self.groups.get("*", [])), path)

    def ai_opt_out(self, path: str) -> bool:
        """True when the site reserves itself from AI use, or shuts an AI crawler out of the site or this path."""
        if self.reserved:
            return True
        for agent in _AI_AGENTS:
            rules = self.groups.get(agent, [])
            if any(not allow and _matches(pattern, "/") for allow, pattern in rules) or not _allowed(rules, path):
                return True
        return False


def _tdm_reserved(content: bytes) -> list[str]:
    """The path patterns that a /.well-known/tdmrep.json reserves. Anything that is not such a file reserves none."""
    try:
        rules = json.loads(content)
    except (ValueError, RecursionError):
        return []
    reserved = []
    for rule in rules if isinstance(rules, list) else [rules]:
        if isinstance(rule, dict) and rule.get("tdm-reservation") in (1, "1"):
            location = unquote(str(rule.get("location") or "/"))
            reserved.append(location if location.startswith(("/", "*")) else "/")  # an odd location: everywhere
    return reserved


def _reserves(name: str, value: str) -> bool:
    """True when one header or <meta> pair opts the content out of AI use or indexing."""
    name, value = name.strip().lower(), value.strip().lower()
    if name == "tdm-reservation":
        return value == "1"
    if name in ("content-signal", "content-usage"):
        return bool(_AI_RESERVED.search(value))
    # ponytail: any of the four words anywhere in the value counts, so a directive scoped to another crawler
    # ("googlebot: noindex") or "max-image-preview: none" also skips the page. Fails closed.
    return name in _ROBOTS_NAMES and not OPT_OUT_DIRECTIVES.isdisjoint(re.split(r"[^a-z]+", value))


def _attributes(text: str, position: int) -> tuple[dict[str, str] | None, int]:
    """The attributes of the tag whose name ends at `position`, and where the tag ends. None for a tag with no end.

    Tags are read here, attribute by attribute, and not by html.parser: on a malformed page it is quadratic
    (2 MB of "<!--" takes minutes).
    """
    attributes: dict[str, str] = {}
    limit = position + META_TAG_MAX
    while True:
        step = _ATTRIBUTE.match(text, position, limit)
        if not step:
            return None, position
        position = step.end()
        if step.group(1):
            return attributes, position
        value = step.group(3) or step.group(4) or (step.group(5) or "").rstrip("/")
        attributes.setdefault(step.group(2).lower(), unescape(value))  # the first of a repeated attribute wins


def _meta_reserves(text: str) -> bool:
    """True when a <meta> tag in the text opts out, or cannot be read to its end.

    Ceiling: a commented-out meta still counts, which fails closed.
    """
    for opening in _META_OPEN.finditer(text):
        attributes, _ = _attributes(text, opening.end())
        if attributes is None:
            return True  # no end in sight: a tag we cannot read counts as an opt-out
        if _reserves(attributes.get("name") or attributes.get("http-equiv") or "", attributes.get("content", "")):
            return True
    return False


class _HeadEnds(Exception):
    """Raised by _Head where a browser would end the page's head."""


class _Head(HTMLParser):
    """The <meta> elements of a page's head, read with the standard library's HTML parser as a browser builds the
    head: it ends at </head>, at <body>, at the first element that has no place in a head (a <p>, an <h1>), or at
    the first text that is not white space, whether or not the page writes <head> and </head> at all. A <meta> in a
    comment, in a script, style, title or other element whose content is text, or inside another element's
    attribute value, is not an element. Parsing stops where the head ends, so a malformed body costs nothing."""

    IN_HEAD = {"html", "head", "base", "basefont", "bgsound", "link", "meta", "template"}
    TEXT = {"script", "style", "title", "noscript", "noframes"}  # in a head too, and their content is text

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.metas: list[dict[str, str]] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag not in self.IN_HEAD | self.TEXT:
            raise _HeadEnds
        if tag == "meta":
            attributes: dict[str, str] = {}
            for key, value in attrs:
                attributes.setdefault(key, value or "")  # the first of a repeated attribute wins, as in a browser
            self.metas.append(attributes)
        elif tag in self.TEXT:
            self.set_cdata_mode(tag)  # the parser itself does this for script and style only

    def handle_endtag(self, tag: str) -> None:
        if tag in ("head", "body", "html", "br"):
            raise _HeadEnds

    def handle_data(self, data: str) -> None:
        if not self.cdata_elem and data.strip(" \t\n\f\r"):  # type: ignore[attr-defined]
            raise _HeadEnds


def _head_meta(text: str, name: str) -> list[str]:
    """The content of every <meta name="<name>"> element in the page's head (_Head). Nothing after the head counts,
    since a visitor's comment or guestbook entry lands in the body, never the head, and nothing in the head that is
    not a <meta> element counts either: a review the head carries as JSON-LD, a comment quoted in og:description."""
    head = _Head()
    try:
        head.feed(text.removeprefix(chr(0xFEFF)))  # a byte order mark is not text before the head
        head.close()
    except (_HeadEnds, AssertionError):  # the parser asserts on a "<![" it cannot read: the head is read up to it
        pass
    return [meta.get("content", "") for meta in head.metas if meta.get("name", "").strip().lower() == name]


def _hrefs(text: str) -> Iterator[str]:
    """The href of every <a>, <area> and <link> that is markup: none from a comment, a script or an attribute."""
    position = 0
    while found := _TAG.search(text, position):
        name = (found.group(1) or "").lower()
        if not name:  # a comment runs to its "-->", or to the end of the page
            position = text.find("-->", found.end() - 2) + 3
            if position < 3:
                return
            continue
        attributes, position = _attributes(text, found.end())
        if attributes is None:
            return  # a tag with no end: nothing after it can be trusted to be markup
        if name in _RAW_TEXT:
            closing = _RAW_TEXT[name].search(text, position)
            if not closing:
                return
            position = closing.end()
        elif name in ("a", "area", "link"):
            yield attributes.get("href", "")


def _deviantart_handle(url: httpx.URL) -> str | None:
    """The DeviantArt account a URL points at, in lower case, or None.

    Two forms, and nothing else: deviantart.com/<handle> (with or without www., a trailing slash or anything
    deeper under the handle) and the old <handle>.deviantart.com.
    """
    host = _host(url)
    if url.scheme not in _DEFAULT_PORT or url.userinfo or url.port not in (None, _DEFAULT_PORT[url.scheme]):
        return None
    if host in ("deviantart.com", "www.deviantart.com"):
        handle = (url.path.split("/") + [""])[1].lower()
    else:
        handle, _, parent = host.partition(".")
        if parent != "deviantart.com" or handle == "www":
            return None
    return handle if _HANDLE.fullmatch(handle) else None


def _links_back(page: FetchResult, handle: str) -> bool:
    """True when the page holds a link to that DeviantArt profile: the proof that the site is the account's own."""
    base = httpx.URL(page.url)
    for href in _hrefs(page.content.decode("utf-8", "replace")):  # the markup that matters here is ASCII
        try:
            if _deviantart_handle(base.join(href.strip())) == handle:
                return True
        except httpx.InvalidURL:
            continue
    return False


def _opted_out(headers: httpx.Headers, body: bytes) -> bool:
    """True when a response reserves itself, in its headers or in a <meta> tag.

    Every body is searched, whatever its Content-Type claims: 15 MB of image bytes takes about 0.2 s.
    """
    if any(_reserves(name, value) for name, value in headers.multi_items()):
        return True
    return any(_meta_reserves(text) for text in _decodings(body, "<meta"))


def _reads_until(deadline: float, clock: Callable[[], float]) -> Callable[[str, dict[str, Any]], None]:
    """httpcore's trace extension, which hands over each stream a request opens (the connection, then TLS over it).
    Each is read only until the deadline, by `clock`: so a head that drips is cut off as a body that drips is."""

    def opened(event: str, info: dict[str, Any]) -> None:
        stream = info.get("return_value")
        if event in ("connection.connect_tcp.complete", "connection.start_tls.complete") and stream is not None:
            read = stream.read

            def timed(max_bytes: int, timeout: float | None = None) -> bytes:
                if clock() > deadline:
                    raise httpcore.ReadTimeout("the response took too long")
                return read(max_bytes, timeout)

            stream.read = timed

    return opened


def _shown(url: object) -> str:
    """The URL as it was given, for a result. Anything that is not a string is shown as nothing: str() can raise."""
    return url if isinstance(url, str) else ""


def _request_url(url: str) -> httpx.URL:
    """The URL as it will be requested: no fragment, and "/" spelled out where the path was left empty."""
    parsed = httpx.URL(url)
    return parsed.copy_with(raw_path=parsed.raw_path, fragment=None)


def _profile_handle(profile_url: str) -> str | None:
    """The handle of a DeviantArt profile URL, or None when it is not one."""
    try:
        return _deviantart_handle(httpx.URL(profile_url))
    except Exception:
        return None


class PoliteFetcher:
    """GETs public pages and images from artists' own sites, and nothing else.

    Nothing is fetched except through a site opened with open_site, and a site opens only when its front page
    links back to the DeviantArt profile that claims it. Which sites are proven or closed, their clocks and their
    budgets are shared by every fetcher in the process. The robots.txt cache belongs to one fetcher, so use one
    for a whole run.
    """

    def __init__(
        self,
        *,
        transport: httpx.BaseTransport | None = None,
        clock: Callable[[], float] = time.perf_counter,
        sleep: Callable[[float], None] = time.sleep,
        resolver: Callable[[str], Iterable[str]] = _resolve,
    ) -> None:
        self._client = httpx.Client(
            transport=transport,
            headers={
                "User-Agent": config.USER_AGENT,
                "Accept-Encoding": "gzip, deflate",  # what _decoded can undo
                "Connection": "close",  # a connection is made for one name at one checked address, and not reused
            },
            timeout=TIMEOUT,
            follow_redirects=False,  # every hop is checked here before it is requested
            trust_env=False,  # no proxy or credentials picked up from the environment
        )
        self._client.cookies.jar.set_policy(DefaultCookiePolicy(allowed_domains=[]))  # never keep or send a cookie
        self._clock = clock
        self._sleep = sleep
        self._resolve = resolver
        # ponytail: robots.txt is cached for the life of the fetcher. RFC 9309 says 24 hours at most; a run is shorter.
        self._origins: dict[str, _Robots | Skip] = {}  # scheme://host -> its rules, or why it is closed to us

    def open_site(self, root_url: str, profile_url: str) -> "Site | FetchResult":
        """Prove that `root_url` is the own site of the DeviantArt account at `profile_url`, and hand back a Site.

        The proof is a link back to that profile on the site's front page. It is looked for once per site and
        process, and costs at most VERIFY_REQUEST_BUDGET requests: robots.txt, tdmrep.json and the front page,
        with no redirect followed from that page. Anything short of proof comes back as a skip and closes the
        site for the rest of the run, for every fetcher. So does a second account claiming a proven site.
        """
        with _LOCK:
            reason = _own_site_refusal(root_url)
            handle = _profile_handle(profile_url)
            if reason or not handle:
                return FetchResult(_shown(root_url), skip=reason or "bad_url")
            root = _request_url(root_url)
            site, front_page = _site(root), None
            if site not in _OWNERS:
                _OWNERS[site] = None  # closed, unless the front page says otherwise
                try:
                    front_page = self._get(root, PAGE_MAX_BYTES, redirects=0)
                    if front_page.ok and _links_back(front_page, handle):
                        _OWNERS[site] = handle
                except Exception:  # a skip never raises, so an unexpected failure is logged and skipped
                    log.exception("could not open %s", root)
                    return FetchResult(str(root), skip="error")
                if front_page.skip:
                    unproven = front_page.skip == "budget" or 300 <= front_page.status < 400
                    reason = "not_own_site" if unproven else front_page.skip
                    return FetchResult(front_page.url, status=front_page.status, skip=reason)
            if _OWNERS[site] != handle:
                _OWNERS[site] = None
                return FetchResult(str(root), skip="not_own_site")
            return Site(self, root_url, profile_url, front_page)

    def read_proof(self, root_url: str, name: str, holds: Callable[[str], bool]) -> bool | Skip:
        """Whether the site says, where only its owner can write, something that `holds` accepts: in the whole
        text of /.well-known/<name>.txt, or in the content of a <meta name="<name>"> in the front page's head
        (_head_meta). Nowhere else on any page. False when both were read and neither holds it; the reason when
        one of them could not be read and the other did not hold it. Never raises.

        Hard rule 7, as the owner's lead decided: reading this one file or tag, which the artist put there for us at
        their own request, is not AI use, so no AI opt-out stops it (noai, an AI crawler shut out, a TDM
        reservation). robots.txt for our own user agent still does: what it disallows us is not fetched. No link
        back to a profile is asked for, and a redirect is followed only within the site, PROOF_REDIRECTS at most.
        """
        with _LOCK:
            reason = _own_site_refusal(root_url)
            if reason:
                return reason
            root, skipped = _request_url(root_url), None
            try:
                for path in (f"/.well-known/{name}.txt", "/"):
                    page = self._get(root.copy_with(raw_path=path.encode()), PAGE_MAX_BYTES, "", PROOF_REDIRECTS, True)
                    if page.ok:
                        text = page.content.decode("utf-8", "replace")
                        if any(map(holds, _head_meta(text, name) if path == "/" else [text])):
                            return True
                    elif page.status not in (404, 410):  # no such file is an answer; anything else is not
                        skipped = skipped or page.skip
            except Exception:  # a skip never raises, so an unexpected failure is logged and skipped
                log.exception("could not read the proof on %s", root)
                return "error"
            return skipped or False

    def _fetch(self, root: str, profile: str, url: str, image: bool) -> FetchResult:
        """What Site.fetch runs. The claim is judged again here, so a handle made by hand opens nothing more."""
        with _LOCK:
            try:
                reason = _own_site_refusal(root)
                handle = _profile_handle(profile)
                parsed = _request_url(url)
                if not reason:
                    site = _site(httpx.URL(root))
                    if not handle or _OWNERS.get(site) != handle:
                        reason = "not_own_site"
                    elif _site(parsed) != site:
                        reason = "offsite"
            except Exception:
                reason = "bad_url"
            if reason:
                return FetchResult(_shown(url), skip=reason)
            try:
                return self._get(parsed, IMAGE_MAX_BYTES if image else PAGE_MAX_BYTES)
            except Exception:  # a skip never raises, so an unexpected failure is logged and skipped
                log.exception("could not fetch %s", parsed)
                return FetchResult(str(parsed), skip="error")

    def _get(
        self, url: httpx.URL, cap: int, policy_path: str = "", redirects: int = MAX_REDIRECTS, proof: bool = False
    ) -> FetchResult:
        """Follow one URL through at most `redirects` redirects.

        `policy_path` is set when the URL is robots.txt or tdmrep.json itself. No rule can forbid reading those,
        their own headers do not close the host, and they may only redirect to the same path on the same site.
        `proof` is set for read_proof: its own budget, and robots.txt for us is the only rule that applies.
        """
        site, status = _site(url), 0
        for _ in range(redirects + 1):
            try:
                reason, address = self._admit(url, site, policy_path, proof)
                unproven = PROOF_REQUEST_BUDGET if proof else VERIFY_REQUEST_BUDGET
                budget = SITE_REQUEST_BUDGET if _OWNERS.get(site) else unproven
                if not reason and _SPENT.get(site, 0) >= budget:
                    reason = "budget"
                if reason:
                    return FetchResult(str(url), skip=reason)
                seconds = PROOF_SECONDS_PER_RESPONSE if proof else MAX_SECONDS_PER_RESPONSE
                response, body = self._send(url, address, cap, seconds)
                status = response.status_code
                if response.has_redirect_location:
                    url = url.join(response.headers["location"])
                    continue
            except (httpx.HTTPError, httpx.InvalidURL, OSError) as error:
                if proof and isinstance(error, httpx.TimeoutException):  # a slow site costs a proof one deadline a run
                    _SPENT[site] = SITE_REQUEST_BUDGET
                return FetchResult(str(url), skip="error")
            if status in (401, 403):
                reason = "auth"
            elif not response.is_success:
                reason = "error"
            elif body is None:
                reason = "too_large"
            elif not policy_path and not proof and _opted_out(response.headers, body):
                reason = "ai_opt_out"
            else:
                return FetchResult(str(url), body, response.headers.get("content-type", ""), status)
            return FetchResult(str(url), status=status, skip=reason)
        return FetchResult(str(url), status=status, skip="error")  # one redirect too many: `url` is where it pointed

    def _admit(self, url: httpx.URL, site: str, policy_path: str, proof: bool = False) -> tuple[Skip | None, str]:
        """Every check a URL must pass before it is requested, run again for each redirect hop.

        Returns the reason to refuse it, or the one address the request may connect to.
        """
        reason = _refusal(url)
        if not reason and (_site(url) != site or (policy_path and url.path != policy_path)):
            reason = "offsite"
        if reason:
            return reason, ""
        addresses = list(self._resolve(_host(url)))
        if not addresses or not all(map(_public, addresses)):
            return "bad_url", ""
        # ponytail: one address is tried, an IPv4 one if there is one. No falling back to the others.
        address = next((address for address in addresses if ":" not in address), addresses[0])
        return (None if policy_path else self._origin_refusal(url, proof)), address

    def _send(self, url: httpx.URL, address: str, cap: int, seconds: float) -> tuple[httpx.Response, bytes | None]:
        """One rate-limited GET, over within `seconds` of being sent: connecting, a TLS handshake, the head and the
        body. The body is None when the response failed, redirected or is larger than `cap`.

        Past the deadline it is a timeout (for a removal proof, the site is then closed for the run: see _get).
        Every read of the connection checks the deadline by this fetcher's clock
        (_reads_until), and the GET runs in a thread of its own, which is waited for no longer than that: a TLS
        handshake blocks where no read of ours runs, so a dripped one is left to the site.
        ponytail: a thread a request, and one left behind for each site that holds a handshake open until the
        process ends. Cancel the socket itself if that ever adds up.
        """
        site = _site(url)
        delay = _DELAY.get(site, config.MIN_SECONDS_PER_HOST)
        now = self._clock()
        # A last reading later than now came from another clock: a full interval from now is then the safe wait.
        due = min(_LAST.get(site, -math.inf), now) + delay
        stalled = None  # real time at which our clock was first seen not to move
        while now < due:  # checked again after every sleep, so a sleep that returns early cannot shorten the wait
            self._sleep(due - now)
            before, now = now, self._clock()
            if now > before:
                stalled = None
            elif stalled is None:
                stalled = time.monotonic()
            elif time.monotonic() - stalled > STALLED_CLOCK_SECONDS:
                raise TimeoutError("the clock has stopped, so the wait cannot be shown to be over")
        deadline = now + seconds
        _SPENT[site] = _SPENT.get(site, 0) + 1
        answer: concurrent.futures.Future[tuple[httpx.Response, bytes | None]] = concurrent.futures.Future()

        def exchange() -> None:
            try:
                answer.set_result(self._exchange(url, address, cap, deadline))
            except Exception as error:  # handed over as it is
                answer.set_exception(error)

        threading.Thread(target=exchange, daemon=True).start()
        try:
            return answer.result(timeout=max(deadline - self._clock(), 0.0))
        except TimeoutError:
            raise httpx.ReadTimeout("the response took too long") from None
        finally:
            _LAST[site] = self._clock()  # the wait runs from the end of a request, and a failed one still counts

    def _exchange(self, url: httpx.URL, address: str, cap: int, deadline: float) -> tuple[httpx.Response, bytes | None]:
        """The GET itself, for _send."""
        site, host = _site(url), _host(url)
        # The connection goes to the address that was checked, not to whatever the name resolves to by now.
        # The name still travels in the Host header, and it is the name the TLS certificate is verified for.
        pinned = url.copy_with(host=address)
        extensions = {"sni_hostname": host, "trace": _reads_until(deadline, self._clock)}
        with self._client.stream("GET", pinned, headers={"Host": host}, extensions=extensions) as response:
            if self._clock() > deadline:  # however the head came (a transport may have no reads to check)
                raise httpx.ReadTimeout("the response took too long")
            if response.status_code in (429, 503):  # told to back off: the site is closed for the rest of the run
                _SPENT[site] = SITE_REQUEST_BUDGET
            declared = response.headers.get("content-length", "")
            if not response.is_success or (declared.isdigit() and int(declared) > cap):
                return response, None
            assert isinstance(response.stream, httpx.SyncByteStream)
            raw = bytearray()
            for chunk in response.stream:  # bytes as they arrive, so a stream that decodes to nothing still ends
                raw += chunk
                if len(raw) > cap:
                    return response, None
                if self._clock() > deadline:
                    raise httpx.ReadTimeout("the response took too long")
            return response, _decoded(bytes(raw), response.headers.get("content-encoding", ""), cap)

    def _origin_refusal(self, url: httpx.URL, proof: bool = False) -> Skip | None:
        """What the origin's robots.txt and tdmrep.json say about this URL. Each is fetched once per origin.
        For a removal proof (`proof`), only what robots.txt says to us: see read_proof."""
        origin = f"{url.scheme}://{_host(url)}"
        if origin not in self._origins:
            self._origins[origin] = self._load_robots(url, proof)
        robots = self._origins[origin]
        if isinstance(robots, str):
            return robots
        path = unquote(url.raw_path.decode("ascii"))  # decoded, so /%70rivate cannot slip past Disallow: /private
        if not proof and robots.ai_opt_out(path):
            return "ai_opt_out"
        if not robots.allows(path):
            return "robots"
        if proof:
            return None
        if robots.tdm is None:  # asked for only once robots.txt has let a URL through
            tdm = self._load_tdm(url, robots)
            if isinstance(tdm, str):
                self._origins[origin] = tdm
                return tdm
            robots.tdm = tdm
        return "ai_opt_out" if any(_matches(location, path) for location in robots.tdm) else None

    def _load_robots(self, url: httpx.URL, proof: bool = False) -> _Robots | Skip:
        """Fetch one origin's robots.txt. Only a 404 or 410 means "no rules"; any other failure closes the origin."""
        robots_url = url.copy_with(raw_path=ROBOTS_PATH.encode(), fragment=None)
        result = self._get(robots_url, POLICY_MAX_BYTES, ROBOTS_PATH, proof=proof)
        if result.skip == "budget":
            return "budget"
        if result.skip and result.status not in (404, 410):
            return "robots"
        robots = _Robots("\n".join(_decodings(result.content, "user-agent", "allow")))
        if robots.crawl_delay > MAX_CRAWL_DELAY:
            return "robots"
        site = _site(url)
        _DELAY[site] = max(_DELAY.get(site, config.MIN_SECONDS_PER_HOST), robots.crawl_delay)
        return robots

    def _load_tdm(self, url: httpx.URL, robots: _Robots) -> list[str] | Skip:
        """Fetch one origin's tdmrep.json: the paths it reserves, or why the origin is closed for want of an answer.

        A plain "no such file" reserves nothing. A timeout, a 408, a 429 or a 5xx is no answer at all.
        """
        if not robots.allows(TDMREP_PATH) or robots.ai_opt_out(TDMREP_PATH):
            return []  # robots.txt keeps us away from the file
        result = self._get(url.copy_with(raw_path=TDMREP_PATH.encode(), fragment=None), POLICY_MAX_BYTES, TDMREP_PATH)
        if result.ok:
            return _tdm_reserved(result.content)
        if result.skip == "budget":
            return "budget"
        no_file = 400 <= result.status < 500 and result.status not in (408, 429)
        return [] if no_file or result.skip == "offsite" else "error"


class Site:
    """One artist's own site, proven and opened by PoliteFetcher.open_site. It is the only way to fetch anything."""

    def __init__(self, fetcher: PoliteFetcher, root: str, profile: str, front_page: FetchResult | None = None) -> None:
        self._fetcher = fetcher
        self.root = root
        self.profile = profile
        self.front_page = front_page  # the page that proved the claim, when this handle is the one that fetched it

    def fetch(self, url: str, *, image: bool = False) -> FetchResult:
        """GET one public page of this site, or one image when `image` is set (only the size cap differs).

        Never raises. A URL that is not on this site comes back as `offsite`, unrequested.
        """
        return self._fetcher._fetch(self.root, self.profile, url, image)
