"""A profile for each artist: the facts about their work, one plain sentence, and a vector for search.

    uv run python -m pipeline.profiles --max-artists 500
    uv run python -m pipeline.profiles --sample 20 --seed 3     one JSON object a line, for the critic
    uv run python -m pipeline.profiles --parity                 the local embedding against Workers AI's

An artist is profiled once every collected work of theirs with a cached image has a verified tag result (the
result set "<model>+<model>" of pipeline/tagging.py, at its current prompt version) and a reading of its own
title and tags. Until then the artist waits: one who has no row gets none, and one who has a row keeps it, so
that an artist is not gone from the site each time collection brings a new work of theirs.

A reading counts only while it is of the image and the words as they are now (pipeline/tagging.py): one made
before an artist replaced the image or changed the title is as if it had never been made, and the artist waits
for it like any other. A row is tied to the readings it was made from: the end of its basis is one fingerprint
of the works it counted, each by its image and by what its words declare about AI, and its facts say what their
verified tags were. While either is not what those works are read as now (a new prompt version, too), the row is
not current (see current): it is kept, it is not published, and it is made again when the readings come out
otherwise. When they come out the same, it is current again as it stands.

The facts are counts over the artist's eligible works: analyzed, not flagged or rated explicit, not flagged
not_artwork, not declared AI-made. Only the verified tags are counted, never the descriptions, and which works
have each value is kept (`tagged`). They come from fixed lists and each was checked against its image, so they
are all a card is made of: a subject, a genre, a drawing manner and a medium, each worded by its share of the
works and never on fewer than MIN_SUPPORT of them. The subjects are counted together, by the works that show
any of them:
  - flat from CLEAR_PERCENT ("Fantasy character and creature art in traditional media");
  - "mostly" from more than half ("Mostly character art"; a manner is "mainly in an anime style");
  - behind a hedge from PART_PERCENT ("character art, among other things", "often in an anime style");
  - below that, nothing. A genre and a medium are said flat or not at all: "with fantasy themes" was filler,
    and wrong both ways. The text leaves out "digital painting", which nearly every artist has.
A style word is still counted, for search, and no longer said.

The artist's own tags are no part of the card (owner decision 11): the site shows them apart, as theirs, and
they are in the facts for search (`own_words`, see _own_tags). An artist of whom the card can name no subject,
genre or manner has no profile ("nothing_clear"), whatever their tags say: a card must rest on what was checked.

The eligible works still go on one contact sheet, held in memory (hard rule 2), for two counts that are never
stated and that the push stage weighs: the pictures that are sexualised or gory, and those of the one narrow
subject the artist repeats most. An artist whose pictures will not open has no sheet, and no profile.

Open for work is True only on the artist's own words (see open_evidence), and otherwise unknown. Never False.

One text is kept, built by code (template) and held to the print check's rules (unfit). The model's own
sentence, "written", was the same text for most artists and never beat it; the column is still filled, with
the same text, for the push stage.

One table, artist_profiles, in the local SQLite file. Hard rule 7: a row is written only for an artist the
store still lists, rows of artists it no longer lists go at the start of every run, and no read returns one.
Hard rule 9: the models are Ollama's on a loopback address. `--parity` alone talks to anything else: it sends
the eval queries, and nothing about any artist, to Workers AI.
Every function takes an optional `db` path; the default is config.LOCAL_DB.
"""

import argparse
import base64
import hashlib
import io
import json
import logging
import math
import pathlib
import re
import sqlite3
import statistics
import sys
import time
from array import array
from collections import Counter
from collections.abc import Callable, Collection, Iterable
from datetime import UTC, datetime, timedelta
from typing import Any

import httpx
from PIL import Image, ImageDraw, ImageFont

from pipeline import config, library, store, tagging

TAGGER = tagging.DEFAULT_MODEL  # whose tags are read: this model's, verified by itself. It counts on the sheet too
EMBEDDER = "bge-m3"
CLOUD_EMBEDDER = "@cf/baai/bge-m3"  # what the search box embeds a query with
DIMENSIONS = 1024  # sql/001_schema.sql: artists.profile_embedding is halfvec(1024)
QUERIES = config.ROOT / "eval" / "queries.jsonl"

MIN_WORKS = 5  # eligible works; with fewer an artist cannot be characterised
MIN_SUPPORT = 4  # works: nothing is stated on fewer, whatever share they are
PART_PERCENT = 40  # of the eligible works: a subject or a manner is said with a hedge
CLEAR_PERCENT = 70  # anything is stated flat; a genre, a medium or a verified colour word is a fact at all
NEARLY_ALL_PERCENT = 90  # any other style word is a fact
MAX_MODIFIERS = 3  # manner and genre words before the kind of art
AT_MOST = {"specialties": 3, "worlds": 2, "manners": 1, "own_words": 12, "style_words": 2}
SHARED = 3  # artists in the store who use a tag, the artist among them, before it is shown as the artist's
CELL, PER_ROW, LABEL = 300, 5, 34  # the contact sheet: pixels a picture, pictures a row, pixels for its number
RECENT_DAYS = 180  # a work's own tags say something about commissions for this long
TEMPLATE_VERSION = 11  # bump when template(), what facts_of() makes a fact, or embedded_text() changes
SHEET_VERSION = 7  # bump when the sheet or COUNT_PROMPT changes

# Every value a profile may state, and its spellings. The first is the template's. "photo" and "unclear" are not
# here: neither says what the art is made with.
MEDIUM_FORMS = {
    "digital painting": ("digital painting", "digital paintings"),
    "digital line art": ("digital line work", "digital line art", "digital linework"),  # after "character art"
    "3d render": ("3D", "3D renders", "3D render", "3D rendering"),
    "pixel art": ("pixel art",),
    "traditional": ("traditional media", "traditional art", "traditional"),
}
COMMON_MEDIUM = "digital painting"  # a fact and a search filter like the others, and never worth a word of the text
# The tag "prop or weapon" does not say which, so neither does a profile. A weapon is a prop; a prop need not
# be a weapon.
SUBJECT_FORMS = {
    "character": ("character", "characters"),
    "creature": ("creature", "creatures"),
    "environment": ("environment", "environments"),
    "prop or weapon": ("prop", "props"),
    "vehicle or mech": ("vehicle", "vehicles"),
    "interface or icons": ("interface", "interfaces"),
}
WORLD_FORMS = {
    "fantasy": ("fantasy",),
    "dark fantasy": ("dark fantasy",),
    "science fiction": ("science fiction", "sci-fi"),
    "cyberpunk": ("cyberpunk",),
    "horror": ("horror",),
    "historical": ("historical",),
    "modern": ("modern-day", "modern", "present-day"),
}
MANNER_FORMS = {
    "anime style": ("anime", "anime-style"),
    "stylized cartoon": ("cartoon", "stylized cartoon", "stylised cartoon"),  # not "a stylized cartoon style"
    "realistic": ("realistic",),
}
# The style words the tagging prompt itself names as checkable at a glance. Any other word a model came up with
# ("moody", "cinematic", "detailed") is counted and never stated.
STYLE_FORMS = {
    "painterly": ("painterly",),
    "cel-shaded": ("cel-shaded", "cel shaded", "cel shading"),
    "monochrome": ("monochrome",),
    "sketchy": ("sketchy",),
    "flat-colour": ("flat-colour", "flat colour", "flat-color", "flat color"),
    "lineart": ("linework", "line work", "line art", "lineart"),  # not "line art character art"
    "high-contrast": ("high-contrast", "high contrast"),
    "muted": ("muted",),
    "saturated": ("saturated",),
    "textured": ("textured",),
    "soft-shaded": ("soft-shaded", "soft shaded", "soft shading"),
    "hard-edged": ("hard-edged", "hard edged"),
    "chibi": ("chibi",),
    "isometric": ("isometric",),
    "low-poly": ("low-poly", "low poly"),
}
# The three the verifier checks, by picking a colour for the image itself: each word that says it -> the one counted.
COLOUR_WORDS = dict.fromkeys(("saturated", "vibrant", "vivid"), "saturated") | {
    "muted": "muted",
    "monochrome": "monochrome",
}
# A genre that is a kind of another: a work of the first is one of the second, and a card says one of the two.
_WIDER = {"dark fantasy": "fantasy", "cyberpunk": "science fiction"}
GENRE_FORMS = WORLD_FORMS | MANNER_FORMS
FORMS = MEDIUM_FORMS | SUBJECT_FORMS | GENRE_FORMS | STYLE_FORMS
# The fields of the facts that search reads, in the order the vector's text lists them. `own_words` are the
# artist's own tags that the site shows as theirs, as the reviewed list spells them. `formats` is a kind of output,
# which only a row made before round 7 has: the push stage reads it from such a row until the row is made again.
STATED = ("specialties", "own_words", "mediums", "genres", "formats", "style_words")

# The artist's own tags (see _own_tags). The catch-alls every artist uses, and words that tell a studio nothing
# of what the artist makes: a tag made of these, and of words a tag shown before it has, is not shown.
_SAYS_NOTHING = frozenset(
    "art arts artwork artworks artist artists artistic digital digitally 2d drawing drawings draw drawn painting "
    "paintings paint painted illustration illustrations illustrator illustrated design designs designer concept "
    "concepts conceptual game games gaming gamedev visual development visdev picture pictures image images work "
    "works style daily my deviation mood composition imagination inspiration idea aesthetic color colors colour "
    "human humans people man men woman women girl girls boy boys male female hot oc ocs".split()
)
# The words a tag may be split at for certain: the catch-alls, the fixed lists' own, those of the tags that
# discovery searches by (pipeline/discover.py), and three that the artists join to another most often.
_WORDS = _SAYS_NOTHING | {word for forms in FORMS.values() for form in forms for word in form.lower().split()}
_WORDS |= frozenset("weapon mech icons monster armor costume scifi original fan".split())
# Long names that artists also write short, and one word they spell two ways: a tag is compared as the second,
# so the site shows one of the two. ponytail: the pairs that several artists in the store use both of. Another
# is shown beside its other spelling, as two of the artist's tags.
_SHORT = {
    "dungeonsanddragons": "dnd",
    "sciencefiction": "scifi",
    "magicthegathering": "mtg",
    "lordoftherings": "lotr",
    "tabletoprpg": "ttrpg",
    "tradingcardgame": "tcg",
    "colour": "color",
}
_LONG = re.compile("|".join(_SHORT))
# The artist's own tags the site may show at all: a reviewed list of plain words for what the work depicts or how
# it is made (a subject, a genre, a setting, a medium, a technique, a kind of output, a game or a franchise), one a
# line as a tag is compared (_compared), sorted. A tag that is not on it is not shown, whatever else it passes, until
# a person has read it and added it. Never one about a body, dress, sex, a fetish, an age or a real person, nor
# selling or the artist's own promotion, nor one the reader was unsure of. Made on 2026-10-14 by reading the 2,200
# tags the rules below would then show most, out of 9,162: 90% of the places for a tag on the site.
APPROVED = frozenset(pathlib.Path(__file__).with_name("approved_tags.txt").read_text(encoding="utf-8").split())
# Two claims counted on every sheet and never stated: the push stage's to weigh.
UNSUITABLE = "sexualised or gory"
SAME = "one repeated subject"
_NEVER_STATED = {
    UNSUITABLE: "the framing or the dress of a figure is sexualised, whether or not anything is explicit: a pin-up "
    "or fan-service pose, the view on chest, hips, buttocks or crotch, lingerie, swimwear or a barely-there outfit, "
    "bare skin shown off for its own sake. Or one fetish subject is the point of the picture, such as feet, tongues "
    "and drool, a mouth seen from inside, bondage, inflation or being swallowed. Or strong gore. For this claim a "
    "picture you miss is what counts against us: in doubt, list it",
    SAME: "the one narrow thing that picture after picture here is a picture of, shown the same way each time: the "
    "same part of a body in close-up, the same pose, the same single object or act. Find the one that the most "
    "pictures show, say in closest what it is, and list those pictures. A broad kind of subject (characters, "
    "creatures, landscapes) is not one, and neither is a style or a medium",
}
COUNT_PROMPT = """You check one claim about one sheet of numbered pictures, all by one artist, for a search index \
of artists. A reviewer will hold the claim against each picture you list: a picture that does not plainly show \
the thing AS IT IS NAMED counts against us, and a picture you leave out costs nothing. So list a picture only \
when it leaves no doubt.

Something like the thing is not the thing. A horse-shaped fantasy creature is not a horse. A finished scene is \
not a design sheet. A small figure standing in a landscape does not make the picture about a character. A \
picture that merely looks rendered is not shown to be 3D.

Answer with one JSON object with these fields.
closest: a few plain words on what the pictures that come nearest to the claim really show.
like_it_only: the numbers of the pictures that show something like the claim, and not the thing as named.
shows: the numbers of the pictures that plainly show the thing as named. Never one that is in like_it_only. \
An empty list is a good answer."""

# An artist's own words on whether they take work. A tagline is read a part at a time ("NO AI | COMMISSIONS -
# OPEN" is two parts), each part as plain lowercase words; a tag is read with its "_" and "-" taken out.
_C = r"(?:comm?iss?ions?|comms)"  # also "comissions", "commisions": the commonest spellings on the site
_SAYS_OPEN = re.compile(
    rf"\b{_C}(?: (?:are|is|currently|now))* open\b|\bopen (?:for|to) {_C}\b|\b(?:taking|accepting|available for) {_C}\b"
)
_SAYS_HIRE = re.compile(r"\bavailable for (?:hire|work|freelance)\b|\bfor hire\b|\bopen for work\b")
# A part with one of these in it says nothing: closed, not yet, no longer. "t" is what is left of "aren't".
_UNSAYS = re.compile(
    r"\b(?:closed?|closing|not|no|never|full|hold|paused?|hiatus|soon|will|when|until|later|next|waitlist|t)\b"
)
_OPEN_TAG = re.compile(rf"(?:vgen)?{_C}(?:are)?open|open(?:for)?{_C}|(?:taking|accepting|availablefor){_C}")
_HIRE_TAG = re.compile(r"(?:artist|illustrator)forhire|availablefor(?:hire|work|freelance)|openforwork")
_CLOSED_TAG = re.compile(rf"{_C}.*clos|clos.*{_C}|no{_C}|{_C}(?:full|onhold|paused)")
OPEN_SENTENCE = {"commissions": "Open for commissions.", "hire": "Available for hire."}

REASONS = ("mostly_ai_declared", "mostly_explicit", "too_few_works", "nothing_clear")  # why there is no profile
# The works that no longer stand as the artist's public works: marked by the collect stage (another author's,
# mature, no longer public...), or missing from the artist's latest answer, which is how one taken down shows
# (pipeline/library.py). Nothing is counted over them, and a row counted over one is out of date (see current).
_FALLEN = (
    "SELECT deviation_id FROM work_marks UNION SELECT w.deviation_id FROM works w "
    "JOIN profiles p ON p.artist_key = w.artist_key WHERE w.fetched_at < p.fetched_at"
)

_SCHEMA = """
CREATE TABLE IF NOT EXISTS artist_profiles (
    artist_key TEXT PRIMARY KEY,     -- works.artist_key
    basis TEXT NOT NULL,             -- sha256 of all the row was made from, then tagging.read_as() of what it counted
    excluded TEXT,                   -- one of REASONS: why there is no profile. NULL when there is one
    facts TEXT NOT NULL,             -- JSON: the counts, and the values that passed the threshold
    open_evidence TEXT NOT NULL,     -- JSON list of the artist's own words; empty means unknown
    template TEXT,                   -- the profile built by code
    template_version INTEGER,
    written TEXT,                    -- the same text: what the push stage reads. No model writes one any more
    written_version INTEGER,
    written_by_model INTEGER,        -- 0
    embedding BLOB,                  -- 1,024 float32 numbers, unit length
    made_at TEXT NOT NULL            -- ISO-8601 UTC
) STRICT
"""


def _transact[T](db: pathlib.Path | str | None, work: Callable[[sqlite3.Connection], T]) -> T:
    """One transaction with this stage's table in it, waiting for the other stages the way tagging does."""

    def with_table(conn: sqlite3.Connection) -> T:
        conn.execute(_SCHEMA)
        return work(conn)

    return tagging._transact(db, with_table)


def _plain(tag: str) -> str:
    """An artist's tag as it is compared: lowercase, with its "_" and "-" taken out."""
    return re.sub(r"[_-]", "", tag.lower())


def _compared(tag: str) -> str:
    """A tag as one, and as the site prints it: _plain(), with a long name written short (_SHORT)."""
    return _LONG.sub(lambda name: _SHORT[name[0]], _plain(tag))


def approved(tag: str) -> bool:
    """True when a tag is on the reviewed list (APPROVED), however its case, "_" and "-" are written."""
    return _compared(tag) in APPROVED


def _leading(counts: Counter[str], need: int, order: Iterable[str], at_most: int) -> list[str]:
    """The `at_most` values of `order` that most works have, of those at least `need` works have."""
    return sorted((value for value in order if counts[value] >= need), key=lambda value: -counts[value])[:at_most]


def _ways(tag: str) -> list[list[str]]:
    """Every way a tag is words of _WORDS joined."""
    if not tag:
        return [[]]
    return [[word, *rest] for word in _WORDS if tag.startswith(word) for rest in _ways(tag[len(word) :])]


def _reads(written: str) -> str:
    """A tag as a reader expects it, where that is certain: with the spaces the artist put in it themselves
    ("character_design"), or split where it is words of the fixed lists joined, in the one way there is to
    split it ("creaturedesign"). Otherwise as the artist wrote it: no word is made up."""
    if re.search("[_-]", written):
        return " ".join(written.lower().replace("_", " ").split())
    ways = _ways(_plain(written))
    return " ".join(ways[0]) if len(ways) == 1 else _plain(written)


def _own_tags(works: list[dict[str, Any]], handle: str, shared: Collection[str] | None) -> dict[str, int]:
    """The artist's own tags that the site shows as theirs, as the reviewed list spells them -> how many of
    `works` carry each.

    The one rule for them: the push stage sends these, of those on a work it sends, and no other tag of the
    artist's as theirs. A tag is compared without its case, "_" and "-" (_plain), and in one of the two ways
    artists write some names (_SHORT), and it is printed so: never with the artist's own spacing, which once
    made the listed "titan" the words "tit an". The most used come first, and of two as often used the one the
    artist writes earlier among a work's tags; AT_MOST["own_words"] are kept. Only a tag on the reviewed list is
    shown (approved): every other is left out. Left out as well, because it says nothing, is not about the work,
    or says again what one before it says:
      - a tag made only of catch-all words (_SAYS_NOTHING) and of the words of those taken before it, a plural
        as its singular: "digitalart", and after "dark fantasy" "dark art";
      - the artist's `handle`, a part of it, or a tag it is part of, the handle compared as a tag is (_plain);
      - a tag that fewer than SHARED artists in the store use (`shared` has those that enough of them do; None
        is no store to ask): a name, the artist's or somebody else's, is seldom anyone else's tag;
      - one the plain contact rule refuses (library.holds_contact), alone or with those taken before it;
      - one that has a tag already taken in it, or is in one: "dragon", and then "dragonart"; "dnd", and then
        "dungeonsanddragons".
    ponytail: a word for what the artist draws is lost when their handle has it ("dragon" under a handle with
    "dragon" in it). The upgrade is the language model's word on each tag.
    """
    used: Counter[str] = Counter()
    late: Counter[str] = Counter()  # the places a tag has among its works' tags, added up
    spelled: dict[str, str] = {}  # as the artist wrote it, with the "_" or "-" they put in it when they did once
    for work in works:
        own: dict[str, str] = {}
        for written in work.get("tags", ()):
            own.setdefault(_compared(written), written)
        for place, (tag, written) in enumerate(own.items()):
            used[tag] += 1
            late[tag] += place
            if tag not in spelled or re.search("[_-]", written):
                spelled[tag] = written
    quoted: dict[str, int] = {}
    handle = _plain(handle)  # "yy-dark-raven" is the tag "yydarkraven"
    # The words that add nothing, each without its plural ending: "elf", then "elves".
    nothing = {_single(word) for word in _SAYS_NOTHING}
    for tag in sorted(used, key=lambda tag: (-used[tag], late[tag], tag)):
        if len(quoted) == AT_MOST["own_words"]:
            break
        reads = _reads(spelled[tag])
        parts = [_single(part) for part in reads.replace("-", " ").split()]
        if not (
            not approved(tag)
            or all(
                part in nothing or any(nothing.issuperset(map(_single, way)) for way in _ways(part)) for part in parts
            )
            or (handle and (tag in handle or handle in tag))
            or (shared is not None and tag not in shared)
            or any(tag in other or other in tag for other in quoted)
            or library.holds_contact(" ".join([*quoted, tag]))
        ):
            quoted[tag] = used[tag]
            nothing |= {_single(tag), *parts}  # nor is this one said again: "dark fantasy", then "dark art"
    return quoted


def facts_of(
    works: list[dict[str, Any]], handle: str = "", shared: Collection[str] | None = None
) -> tuple[dict[str, Any], str | None]:
    """The facts about one artist, and why they get no profile (one of REASONS) or None.

    `works` has one entry per collected work: its id, its verified `result` (None when it has none), what its
    own title and tags `declared` about AI, and the artist's own `tags` on it. A work counts when it has a
    result and is not explicit (flagged, or rated so), not not_artwork and not declared AI-made. Each counted
    work adds one to its medium and to each of its subjects, genres and style words. A work of a genre is one
    of the wider genre too (_WIDER), and a card names one of the two: the narrower when the works are clearly
    that. Its description is never read. `tagged` says which works have each value a card can state, by their
    place among the counted works, from 1, and the module's docstring says what share of them makes a fact.
    `mostly` lists the subjects (by the works that show any of them) and the manner that most works have and
    not CLEAR_PERCENT of them, and `hedged` those that hold for part of the works only: neither may be stated
    flat. `own_words` are the artist's own tags that the site shows as theirs, as the list spells them, with
    their counts in counts["own_words"]: see _own_tags, which `handle` and `shared` are for.
    """

    def explicit(result: dict[str, Any]) -> bool:
        return result["explicit"] or result.get("rating") == "explicit"

    analyzed = [work["result"] for work in works if work["result"]]
    counted = sorted(
        (
            work
            for work in works
            if work["result"]
            and work["declared"] != "yes"
            and not explicit(work["result"])
            and not work["result"]["not_artwork"]
        ),
        key=lambda work: work["id"],
    )
    fields = ("medium", "subjects", "genres", "style_words")
    counts: dict[str, Counter[str]] = {field: Counter() for field in fields}
    tagged: dict[str, list[int]] = {}
    for place, work in enumerate(counted, start=1):
        result = work["result"]
        has = {
            "medium": {result["medium"]},
            "subjects": set(result["subjects"]),
            "genres": {*result["genres"], *(_WIDER[genre] for genre in result["genres"] if genre in _WIDER)},
            "style_words": {COLOUR_WORDS.get(word, word) for word in result["style_words"]},
        }
        for field, values in has.items():
            counts[field].update(values)
        stated = has["medium"] & MEDIUM_FORMS.keys() | has["subjects"] & SUBJECT_FORMS.keys()
        for value in stated | has["genres"] & GENRE_FORMS.keys() | has["style_words"] & STYLE_FORMS.keys():
            tagged.setdefault(value, []).append(place)
    part, clear, nearly_all = (
        max(MIN_SUPPORT, -(-len(counted) * percent // 100))
        for percent in (PART_PERCENT, CLEAR_PERCENT, NEARLY_ALL_PERCENT)
    )
    most = max(MIN_SUPPORT, len(counted) // 2 + 1)
    have = Counter({term: len(places) for term, places in tagged.items()})  # all the first sentence is counted from
    subjects = _leading(have, part, SUBJECT_FORMS, AT_MOST["specialties"])
    worlds = _leading(have, clear, WORLD_FORMS, len(WORLD_FORMS))
    # One of a genre and its wider one: "dark fantasy" where the works are clearly that, and "fantasy" otherwise.
    worlds = [world for world in worlds if world not in {_WIDER.get(near) for near in worlds}][: AT_MOST["worlds"]]
    manners = _leading(have, part, MANNER_FORMS, AT_MOST["manners"])
    # The subjects are named together, as the kind of art, by the share of the works that show any of them:
    # "character and creature art" where seven works in ten are characters or creatures.
    together = set().union(*(tagged[term] for term in subjects))
    # What stands flat before the kind of art is one statement, "fantasy character art": most works must bear
    # all of it out at once. A genre or a manner that does not hold together with the subjects is not said.
    led = set(together) if len(together) >= clear else set(range(1, len(counted) + 1))
    flat = [term for term in (*worlds, *manners) if have[term] >= clear]
    for term in sorted(flat, key=lambda term: -len(led & set(tagged[term]))):
        if len(led & set(tagged[term])) >= most:
            led &= set(tagged[term])
        else:
            worlds, manners = ([kept for kept in terms if kept != term] for terms in (worlds, manners))
    mediums = _leading(have, clear, MEDIUM_FORMS, 1)
    shown = _own_tags(counted, handle, shared)
    share = {term: len(together) for term in subjects} | {term: have[term] for term in manners}
    styles = Counter(
        {
            word: count
            for word, count in counts["style_words"].items()
            if count >= (clear if word in COLOUR_WORDS.values() else nearly_all)
            and (word, *mediums) != ("lineart", "digital line art")  # said once, as the medium
        }
    )
    ai, explicits = sum(work["declared"] == "yes" for work in works), sum(map(explicit, analyzed))
    facts = {
        "works": len(works),
        "eligible": len(counted),
        "declared_ai": ai,
        "explicit": explicits,
        "not_artwork": sum(result["not_artwork"] for result in analyzed),
        "counted": sorted(work["id"] for work in counted),
        "counts": {
            field: dict(sorted(counter.items(), key=lambda item: (-item[1], item[0])))
            for field, counter in counts.items()
        }
        | {"own_words": shown},
        "tagged": dict(sorted(tagged.items())),
        "specialties": subjects,
        "own_words": list(shown),
        "mediums": mediums,
        "genres": worlds + manners,
        "style_words": _leading(styles, 1, STYLE_FORMS, AT_MOST["style_words"]),
        "mostly": [term for term, count in share.items() if most <= count < clear],
        "hedged": [term for term, count in share.items() if count < most],
    }
    if 2 * ai > len(works):
        return facts, "mostly_ai_declared"
    if 2 * explicits > len(analyzed):
        return facts, "mostly_explicit"
    if len(counted) < MIN_WORKS:
        return facts, "too_few_works"
    # A medium or a style word alone tells no one what the artist makes, and neither do tags nobody checked.
    return facts, None if subjects or worlds or manners else "nothing_clear"


def open_evidence(tagline: str | None, works: list[tuple[str, list[str], str | None]], now: datetime) -> list[dict]:
    """The artist's own words that say they take work now. Empty means unknown: it is never read as "closed".

    Read: the tagline, and the artist's own tags on works published in the last RECENT_DAYS; `works` is
    (id, tags, published_at). Evidence is the words themselves ("commissions open", "open for commissions",
    "taking commissions", "available for hire"; as a tag "commissionsopen", "opencommissions"), never a
    guess: not a tag "commission", not "freelance illustrator", not a price list. And when anything read says
    the opposite ("commissions closed", "not taking commissions", a tag "commissionsclosed"), there is no
    evidence at all, whatever else says open.

    ponytail: a short list of English phrases. "DM me for commissions" and a tag "commissioninfo" are left as
    unknown though a reader would take them as open. Add a phrase only when it can mean nothing else.
    """
    found: list[dict] = []
    for part, ending in re.findall(r"([\w\s:'-]+)([^\w\s:'-]*)", (tagline or "").lower()):
        words = " ".join(re.findall(r"[a-z]+", part))
        kind = "commissions" if _SAYS_OPEN.search(words) else "hire" if _SAYS_HIRE.search(words) else None
        if _UNSAYS.search(words) and (kind or re.search(rf"\b{_C}\b", words)):
            return []
        if kind and "?" not in ending:  # a question is not a statement
            found.append({"source": "tagline", "says": tagline, "kind": kind})
    for work_id, tags, published in sorted(works, key=lambda work: work[2] or "", reverse=True):
        if not published or now - datetime.fromisoformat(published) > timedelta(days=RECENT_DAYS):
            continue
        for tag in tags:
            word = _plain(tag)
            if _CLOSED_TAG.search(word):
                return []
            kind = "commissions" if _OPEN_TAG.fullmatch(word) else "hire" if _HIRE_TAG.fullmatch(word) else None
            if kind:
                found.append({"source": "tag", "says": tag, "kind": kind, "work": work_id, "published_at": published})
    return found[:5]


def _evidence(db: pathlib.Path | str | None, now: datetime) -> dict[str, list[dict]]:
    """open_evidence() for every collected artist, of their own words as they are now: the tagline their latest
    answer gives (one it gave empty is none, library.answers), when the tagging stage read it as it is and found no
    private contact in it, since it is quoted; and their own tags on the works that still stand (_FALLEN)."""
    quotable = {key for key, answer in tagging.contacts(TAGGER, db).items() if answer == "no"}
    quotable -= {key for key, (emptied, _, _) in library.answers(db).items() if emptied}
    query = f"SELECT artist_key, deviation_id, tags, published_at FROM works WHERE deviation_id NOT IN ({_FALLEN})"
    works, taglines = _transact(
        db,
        lambda conn: (
            conn.execute(query + " ORDER BY 1, 2").fetchall(),
            dict(conn.execute("SELECT artist_key, tagline FROM profiles").fetchall()),
        ),
    )
    own: dict[str, list[tuple[str, list[str], str | None]]] = {}
    for key, work_id, tags, published in works:
        own.setdefault(key, []).append((work_id, json.loads(tags), published))
    return {key: open_evidence(taglines.get(key) if key in quotable else None, mine, now) for key, mine in own.items()}


def _open_sentence(evidence: list[dict]) -> str:
    """What a profile says of the evidence: nothing without any, and "commissions" only when those are named."""
    if not evidence:
        return ""
    return " " + OPEN_SENTENCE["commissions" if any(item["kind"] == "commissions" for item in evidence) else "hire"]


def _sheet(names: Iterable[str]) -> tuple[str | None, int]:
    """One contact sheet of these cached images, each under its number, and how many are on it.

    A JPEG in memory, as base64: it is never written anywhere (hard rule 2). The grid is the one of
    eval/profile_pairs.py, with a number over each picture so that a model can say which ones it means.
    """
    thumbs = []
    for name in names:
        try:
            if pathlib.PurePath(name).name != name:
                continue  # a row that names anything but a file of the cache
            with Image.open(library.image_dir() / name) as image:
                thumb = image.convert("RGB")
            thumb.thumbnail((CELL - 8, CELL - 8))
            thumbs.append(thumb)
        except Exception:  # Pillow raises many kinds for a bad file; a picture that will not open is left off
            continue
    if not thumbs:
        return None, 0
    canvas = Image.new("RGB", (PER_ROW * CELL, -(-len(thumbs) // PER_ROW) * (CELL + LABEL)), "white")
    draw, font = ImageDraw.Draw(canvas), ImageFont.load_default(LABEL - 6)
    for index, thumb in enumerate(thumbs):
        left, top = index % PER_ROW * CELL, index // PER_ROW * (CELL + LABEL)
        draw.text((left + 6, top + 1), str(index + 1), fill="black", font=font)
        canvas.paste(thumb, (left + 4, top + LABEL))
    held = io.BytesIO()
    canvas.save(held, "JPEG", quality=85)
    return base64.b64encode(held.getvalue()).decode("ascii"), len(thumbs)


def _counted(client: httpx.Client, sheet: str, pictures: int, claims: dict[str, str]) -> dict[str, int]:
    """In how many pictures of the sheet each claim plainly shows, as it is named: claim -> count.

    Only for the two claims no profile states (_NEVER_STATED): one is about the pictures side by side, and the
    push stage weighs both as they were counted when its limits were set. Nothing a card states is counted
    here: round 5's unsupported claims had passed such a count, which gave "anime" 9 pictures of 10 where the
    verified tags had 5.
    One call a claim, so that no count can follow another's. The model says what the nearest pictures really
    show, lists those that only show something like the claim, and lists those that show it as named; the count
    is of the last list, each number once, less any it also put in the other. A number that is not on the
    sheet makes the answer no answer. A claim with no whole answer in MAX_ATTEMPTS is not counted.
    """
    numbers = {"type": "array", "items": {"type": "integer", "minimum": 1, "maximum": pictures}, "maxItems": pictures}
    shape = {
        "type": "object",
        "properties": {"closest": {"type": "string"}, "like_it_only": numbers, "shows": numbers},
        "required": ["closest", "like_it_only", "shows"],
    }
    counts = {}
    for claim, meaning in claims.items():
        question = f"The sheet has {pictures} pictures, numbered 1 to {pictures}.\nClaim: {claim}"
        question += f"\nIt means: {meaning}" if meaning else ""
        for attempt in range(tagging.MAX_ATTEMPTS):
            answer = tagging._chat(client, TAGGER, COUNT_PROMPT, question, sheet, shape, attempt)
            lists = [answer.get(name) for name in ("shows", "like_it_only")] if isinstance(answer, dict) else [None]
            if all(
                isinstance(found, list) and all(type(n) is int and 1 <= n <= pictures for n in found) for found in lists
            ):
                counts[claim] = len(set(lists[0] or ()) - set(lists[1] or ()))
                break
    return counts


def _and(items: list[str]) -> str:
    return " and ".join(filter(None, [", ".join(items[:-1]), *items[-1:]]))


def template(facts: dict[str, Any]) -> str:
    """The card, one sentence the same way for every artist, of what the verified tags count.

    It is built on the kind of art: the subjects, named together ("character and creature art"). Before it stand
    the manner and the genres that are flat; after it what it is made in ("in 3D"; a medium that is itself a kind
    of art takes the place of that word: "character pixel art"). The subjects are said by the share of the works
    that show any of them: flat, "mostly character art", or "character art, among other things". Where a manner
    or a genre is flat and the subjects are not, that leads and they follow: "fantasy art, mostly characters", or
    "including characters". Last the manner that is not flat: "mainly", so that no card says "mostly" twice, or
    "often". With no subject and nothing flat, that manner is the one named: "cartoon art, among other things".
    """
    after = (*facts["hedged"], *facts["mostly"])
    manners, worlds = (
        [FORMS[term][0] for term in facts["genres"] if term in forms and term not in after]
        for forms in (MANNER_FORMS, WORLD_FORMS)
    )
    lead = list(filter(None, [*manners, _and(worlds)]))
    styles = [term for term in facts["genres"] if term in MANNER_FORMS and term in after]
    named = facts["specialties"] or ([] if lead else styles)
    share = next((share for share in ("mostly", "hedged") if set(named) & set(facts[share])), "")
    words = "" if share and lead else _and([FORMS[term][0] for term in named])
    media = [FORMS[medium][0] for medium in facts["mediums"] if medium != COMMON_MEDIUM]
    noun = f"{words} {media.pop()}" if media and media[0].endswith(" art") else f"{words} art"
    text = " ".join([*lead, noun.strip()]) + (f" in {media[0]}" if media else "")
    if words and share == "mostly":
        text = "mostly " + text
    elif words and share:
        text += ", among other things"
    elif share:
        text += f", {'mostly' if share == 'mostly' else 'including'} {_and([FORMS[term][1] for term in named])}"
    for word, kind in (("mainly", "mostly"), ("often", "hedged")):
        for manner in (FORMS[term][0] for term in styles if term in facts[kind] and term not in named):
            text += f", {word} in {'an' if manner[0] in 'aeiou' else 'a'} {manner} style"
    return text[0].upper() + text[1:] + "."


# The print check: what a card may not read like, true or not.
# Words that make a phrase the name of a kind of art, and not a caption of what is drawn.
_ART = frozenset("art artwork".split())
# The manner and genre words, the longest first: what stacks up before the kind of art.
_MODIFIERS = "|".join(sorted((re.escape(forms[0].lower()) for forms in GENRE_FORMS.values()), key=len, reverse=True))
# The small words: a card may say one as often as it needs, and may not end on one.
_SMALL = frozenset("and with in a an including partly mostly mainly some".split())
# Words a card may not end on: one that needs another after it, a subject in the singular.
_LOOSE = _SMALL | {forms[0] for forms in SUBJECT_FORMS.values()}


def _single(word: str) -> str:
    """A word without its plural ending, so that "characters" is "character" again.
    ponytail: five endings and no dictionary. "knives" and "knife" still pass as two words."""
    for ending, to in (("ies", "y"), ("ches", "ch"), ("shes", "sh"), ("ves", "f"), ("s", "")):
        if word.endswith(ending) and len(word) > len(ending) + 1 and not word.endswith("ss"):
            return word[: -len(ending)] + to
    return word


def unfit(text: str) -> str | None:
    """Why a card may not be printed as it reads. None when it may. The rules are of what the card says in its
    own voice, the first sentence:
      - no word twice, a plural counted as its singular: not "Fantasy character art, mostly fantasy characters";
      - it names a kind of art, not only what is drawn, and does not open with what the art is made in;
      - MAX_MODIFIERS manner and genre words at most are stacked before the kind of art;
      - it is a whole phrase, and does not break off on a small word or on a subject in the singular.
    """
    whole = text.split(". ")[0].rstrip(".").lower()
    words = re.findall(r"[a-z0-9-]+", whole)
    if twice := [word for word, times in Counter(map(_single, words)).items() if times > 1 and word not in _SMALL]:
        return f'"{twice[0]}" is said twice: say each thing once'
    if not _ART & set(words):
        return "it names what is drawn and no kind of art"
    if whole.startswith(tuple(forms[0].lower() for forms in MEDIUM_FORMS.values())):
        return "what it is made in comes after the kind of art"
    stack = re.match(rf"(?:(?:{_MODIFIERS})(?: and)? )*", whole)
    if stack and len(re.findall(_MODIFIERS, stack.group())) > MAX_MODIFIERS:
        return f"more than {MAX_MODIFIERS} words are stacked before the kind of art"
    if words[-1] in _LOOSE or whole.endswith(","):
        return f'it breaks off at "{words[-1]}"'
    return None


def _unit(vector: object) -> list[float]:
    """A vector of DIMENSIONS numbers at unit length. ValueError for anything else."""
    if not isinstance(vector, list) or len(vector) != DIMENSIONS or not all(isinstance(x, int | float) for x in vector):
        raise ValueError(f"an embedding must be {DIMENSIONS} numbers")
    length = math.sqrt(sum(x * x for x in vector))
    if not (math.isfinite(length) and length > 0):
        raise ValueError("an embedding must have a length")
    return [x / length for x in vector]


def _embed(client: httpx.Client, texts: list[str]) -> list[list[float]]:
    """One unit vector a text, from the local embedding model."""
    response = client.post("/api/embed", json={"model": EMBEDDER, "input": texts})
    response.raise_for_status()
    vectors = response.json().get("embeddings")
    if not isinstance(vectors, list) or len(vectors) != len(texts):
        raise ValueError("the embedding model must give one vector a text")
    return [_unit(vector) for vector in vectors]


def embedded_text(profile: str, facts: dict[str, Any]) -> str:
    """What an artist's vector is made of: the profile, then every fact by its name, as search filters on it.

    The second part does not depend on how the sentence happens to be worded, and has what it leaves out: the
    artist's own tags, the medium everybody has, and the style words. The own tags are facts_of's, so each is on
    the reviewed list and spelled as the list spells it: a row whose own_words are not is not current, and its
    vector is not sent.
    """
    names = {"specialties": "Specialties", "own_words": "In the artist's words", "mediums": "Mediums"}
    names |= {"genres": "Genres", "style_words": "Style"}
    return profile + "".join(f"\n{name}: {', '.join(facts[field])}." for field, name in names.items() if facts[field])


def _listed(db: pathlib.Path | str | None) -> set[str]:
    """Every artist the store still lists. A removed artist is not one of them (hard rule 7)."""
    return {key for key, _, _ in store.all_artists(db)}


def run(
    max_artists: int,
    *,
    db: pathlib.Path | str | None = None,
    ollama_url: str = tagging.OLLAMA_URL,
    transport: httpx.BaseTransport | None = None,
    now: datetime | None = None,
) -> dict[str, float]:
    """Give up to `max_artists` artists the row they lack, or the new one their works or own words now call for.

    Returns counts only: profiled, excluded_<reason> for each of REASONS, unchanged (their row is still what
    this run would write: nothing is asked or written again), waiting_for_tags (not every work of theirs is
    tagged yet; a row they had stays as it is until they are), left_for_next_run,
    removed (rows of artists the store no longer lists, deleted), open_for_work and with_own_tags (of the
    profiled: those with tags of their own to show), and seconds_per_artist. Each row is committed as it is made.
    ValueError before anything is sent if the address is not loopback; httpx.HTTPError when Ollama is in
    trouble, with what was stored kept.
    """
    now = now or datetime.now(UTC)
    counts = dict.fromkeys(("profiled", *(f"excluded_{reason}" for reason in REASONS)), 0)
    counts |= dict.fromkeys(("unchanged", "waiting_for_tags", "left_for_next_run", "removed"), 0)
    counts |= {"open_for_work": 0, "with_own_tags": 0}
    versions = [TEMPLATE_VERSION, SHEET_VERSION, TAGGER, EMBEDDER]
    result_set = tagging.result_set(TAGGER, TAGGER)

    def read(conn: sqlite3.Connection) -> tuple[list[Any], set[str], dict[str, str]]:
        return (
            conn.execute(
                "SELECT artist_key, deviation_id, image_file, tags, published_at, "
                f"deviation_id IN ({_FALLEN}) FROM works ORDER BY 1, 2"
            ).fetchall(),
            {
                work_id
                for (work_id,) in conn.execute(  # tagged, and from the image the work shows now
                    "SELECT t.deviation_id FROM work_tags t JOIN works w USING (deviation_id) "
                    f"WHERE t.model = ? AND t.prompt_version = ? AND t.fingerprint = {library.IMAGE}",
                    (result_set, tagging.PROMPT_VERSION),
                )
            },
            dict(conn.execute("SELECT artist_key, basis FROM artist_profiles").fetchall()),
        )

    def forget(keys: Iterable[str]) -> None:
        gone = [(key,) for key in keys]
        _transact(db, lambda conn: conn.executemany("DELETE FROM artist_profiles WHERE artist_key = ?", gone))

    def store_row(key: str, *row: object) -> bool:
        # Not for an artist removed while a model was at work: the store deletes their row in `artists` at once.
        query = (
            "INSERT OR REPLACE INTO artist_profiles (artist_key, basis, excluded, facts, open_evidence, template, "
            "template_version, written, written_version, written_by_model, embedding, made_at) "
            "SELECT ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ? WHERE EXISTS (SELECT 1 FROM works WHERE artist_key = ?) "
            "AND EXISTS (SELECT 1 FROM artists WHERE key = ?)"
        )
        return _transact(db, lambda conn: conn.execute(query, (key, *row, store._now(), key, key)).rowcount) == 1

    with tagging._client(ollama_url, transport) as client:
        listed = _listed(db)
        results, declared = tagging.results(TAGGER, db=db, verifier=TAGGER), tagging.declared_ai(TAGGER, db)
        images = library.image_prints(db)
        said = _evidence(db, now)
        rows, tagged, stored = _transact(db, read)
        collected: dict[str, list[Any]] = {}
        users: dict[str, set[str]] = {}  # an own tag, as it is compared -> the artists who use it
        for key, *work, fell in rows:
            collected.setdefault(key, []).extend([] if fell else [work])  # what is counted, of works that stand
            for tag in json.loads(work[2]):
                users.setdefault(_plain(tag), set()).add(key)
        shared = {tag for tag, who in users.items() if len(who) >= SHARED}
        stale = stored.keys() - (collected.keys() & listed)
        counts["removed"] = len(stale)
        todo = []
        for key in sorted(collected.keys() & listed):
            works = collected[key]
            if not all(work_id in tagged and work_id in declared for work_id, image, _, _ in works if image):
                counts["waiting_for_tags"] += 1
                continue
            own = [(work_id, json.loads(tags), at) for work_id, _, tags, at in works]
            facts, reason = facts_of(
                [
                    {"id": work_id, "result": results.get(work_id), "declared": declared.get(work_id), "tags": tags}
                    for work_id, tags, _ in own
                ],
                key.partition(":")[2],  # the handle, which the store keeps as "da:<handle>"
                shared,
            )
            evidence = said.get(key, [])
            basis = hashlib.sha256(json.dumps([versions, facts, evidence], sort_keys=True).encode()).hexdigest()
            # And what the row rests on (see current): the same tags read from another image make another row,
            # because the sheet is of the images.
            basis += tagging.read_as(facts["counted"], images, declared)
            if stored.get(key) == basis:
                counts["unchanged"] += 1
            else:
                todo.append((key, basis, facts, reason, evidence))
        forget(stale)
        todo, later = todo[: max(max_artists, 0)], todo[max(max_artists, 0) :]
        counts["left_for_next_run"] = len(later)

        seconds = 0.0
        for key, basis, facts, reason, evidence in todo:
            if reason:
                counts[f"excluded_{reason}"] += store_row(key, basis, reason, json.dumps(facts), "[]", *[None] * 6)
                continue
            started = time.monotonic()
            # The sheet is of the works the facts were counted over, and of no other.
            files = {work_id: name for work_id, name, _, _ in collected[key]}
            sheet, pictures = _sheet(files[work_id] for work_id in facts["counted"])
            text = template(facts) + _open_sentence(evidence)
            # No sheet: the pictures will not open, so nothing was weighed for the push stage. And a card the
            # print check refuses is not printed.
            if sheet is None or pictures < MIN_WORKS or unfit(text):
                counts["excluded_nothing_clear"] += store_row(
                    key, basis, "nothing_clear", json.dumps(facts), "[]", *[None] * 6
                )
                continue
            # The sheet is of the images the row's basis names. One replaced since this run read the store waits
            # for the next run, which starts from the new image's readings.
            shown = [tagging._image(files[work_id]) for work_id in facts["counted"]]
            if any(image and image[1] != images.get(work) for work, image in zip(facts["counted"], shown, strict=True)):
                counts["waiting_for_tags"] += 1
                continue
            seen = _counted(client, sheet, pictures, _NEVER_STATED)
            weighed = {"pictures": pictures, "unsuitable": seen.get(UNSUITABLE, 0), "same_subject": seen.get(SAME, 0)}
            facts |= {"sheet": weighed}
            blob = array("f", _embed(client, [embedded_text(text, facts)])[0]).tobytes()
            seconds += time.monotonic() - started
            # One text, in both columns: the push stage reads "written".
            if store_row(
                key, basis, None, json.dumps(facts), json.dumps(evidence), *(text, TEMPLATE_VERSION) * 2, 0, blob
            ):
                counts["profiled"] += 1
                counts["open_for_work"] += bool(evidence)
                counts["with_own_tags"] += bool(facts["own_words"])
    return {**counts, "seconds_per_artist": round(seconds / counts["profiled"], 2) if counts["profiled"] else 0.0}


def _rows(db: pathlib.Path | str | None, where: str) -> list[Any]:
    """This stage's rows of artists who are still collected and still listed by the store, in key order."""
    listed = _listed(db)
    query = (
        "SELECT artist_key, excluded, facts, open_evidence, template, template_version, written, written_version, "
        f"written_by_model, embedding FROM artist_profiles p WHERE {where} "
        "AND EXISTS (SELECT 1 FROM works w WHERE w.artist_key = p.artist_key) ORDER BY artist_key"
    )
    return [row for row in _transact(db, lambda conn: conn.execute(query).fetchall()) if row[0] in listed]


def _readings(facts: dict[str, Any]) -> list[Any]:
    """What a row's facts say the verified tags of the works it counted were: which works count, which work has
    each value a card can state, and every count facts_of makes of them."""
    counts = facts.get("counts", {})
    fields = ("medium", "subjects", "genres", "style_words")
    return [facts.get("counted", []), facts.get("tagged", {}), *(counts.get(field, {}) for field in fields)]


def current(db: pathlib.Path | str | None = None, now: datetime | None = None) -> set[str]:
    """The artists whose row still rests on what it was made from: each work it counted still stands (_FALLEN)
    and is read as it was then, from the same image and with the same word on AI (tagging.read_as, kept at the
    end of the row's basis), and its verified tags as they stand now, at this prompt version, are the ones the
    row counted (facts_of, counted again over the same works). A work that has fallen counts as one with no
    image, and a work with no verified tags now as one that is not counted.

    A profile rests as well on the version of this stage that made it, and on what the rules and the artist say
    now: it was made at TEMPLATE_VERSION; each of its own_words is on the reviewed list as it is now, spelled as
    the list spells it, which is what its vector was made from (embedded_text); and each word of its
    open_evidence is still what the artist's own words say at `now` (_evidence): a card that says "Open for
    commissions." beside a tagline that now says closed, or on a tag that has grown older than RECENT_DAYS, does
    not. A card that claims nothing claims nothing false: when the artist comes to say they are open, it waits
    for this stage to make it again, and is sent meanwhile.

    The others are waiting: for the tagging stage to read again a work whose image or words have changed, or
    every work after a new prompt version, and then, when that reading comes out otherwise, for this stage to
    make the row again; or for this stage to make it again without the works that fell, or as it makes rows
    now. Their row stays, so that a reading that comes out the same costs nothing; but it says what nobody has
    read of the work as it is, and the push stage holds the artist back meanwhile (see profiles).
    """
    images, declared = library.image_prints(db), tagging.declared_ai(TAGGER, db)
    results = tagging.results(TAGGER, db=db, verifier=TAGGER)
    fallen = {work_id for (work_id,) in _transact(db, lambda conn: conn.execute(_FALLEN).fetchall())}
    images = {work_id: image for work_id, image in images.items() if work_id not in fallen}
    said = _evidence(db, now or datetime.now(UTC))
    query = "SELECT artist_key, basis, facts, excluded, template_version, open_evidence FROM artist_profiles"
    rows = _transact(db, lambda conn: conn.execute(query).fetchall())

    def rests(
        key: str, basis: str, facts: dict[str, Any], excluded: str | None, version: int | None, evidence: str
    ) -> bool:
        counted = facts.get("counted", [])
        works = [{"id": work, "result": results.get(work), "declared": declared.get(work)} for work in counted]
        if not basis.endswith(tagging.read_as(counted, images, declared)) or (
            _readings(facts) != _readings(facts_of(works)[0])
        ):
            return False
        own = facts.get("own_words", [])
        return excluded is not None or (
            version == TEMPLATE_VERSION
            and all(approved(tag) and tag == _compared(tag) for tag in own)
            and all(item in said.get(key, []) for item in json.loads(evidence))
        )

    return {key for key, basis, facts, *row in rows if rests(key, basis, json.loads(facts), *row)}


def profiles(db: pathlib.Path | str | None = None) -> list[dict[str, Any]]:
    """Every artist's profile, in key order, for the push stage. Never an excluded or a removed artist's.

    Each has artist_key, the text as `template` and again as `written`, with its version (written_by_model is
    False), the search filters mediums and specialties, the genres, style_words and own_words (the artist's own
    tags that the site shows as theirs), hedged (which of those hold for part of the works only) and mostly
    (which most works have, and not clearly enough to be said flat), open_for_work (True, or None for unknown) with its
    open_evidence, the facts with their counts, and the embedding as 1,024 numbers. And `current`: False for a
    profile made from a reading that has gone stale or been made again since (see current), which is not one to
    publish.
    """
    found, fresh = [], current(db)
    for key, _, facts_json, evidence, built, built_version, composed, composed_version, by_model, blob in _rows(
        db, "excluded IS NULL"
    ):
        facts, vector = json.loads(facts_json), array("f")
        vector.frombytes(blob)
        found.append(
            {
                "artist_key": key,
                "current": key in fresh,
                "template": built,
                "template_version": built_version,
                "written": composed,
                "written_version": composed_version,
                "written_by_model": bool(by_model),
                **{field: facts.get(field, []) for field in (*STATED, "hedged", "mostly")},
                "open_for_work": True if json.loads(evidence) else None,
                "open_evidence": json.loads(evidence),
                "facts": facts,
                # for the push stage to weigh: of how many pictures on the sheet, how many are sexualised or gory,
                # and how many show the one narrow subject the artist repeats most
                "pictures": (facts.get("sheet") or {}).get("pictures", 0),
                "unsuitable_pictures": (facts.get("sheet") or {}).get("unsuitable", 0),
                "same_subject_pictures": (facts.get("sheet") or {}).get("same_subject", 0),
                "embedding": vector.tolist(),
            }
        )
    return found


def excluded(db: pathlib.Path | str | None = None) -> dict[str, str]:
    """Why each artist without a profile has none: artist key -> one of REASONS."""
    return {row[0]: row[1] for row in _rows(db, "excluded IS NOT NULL")}


def sample(count: int, seed: int, db: pathlib.Path | str | None = None) -> list[dict[str, Any]]:
    """A random sample of the profiles for the critic, each with the paths of the artist's cached images.

    `images` are the works the facts were counted over; `images_left_out` are the artist's other cached works
    (flagged or declared AI-made). `own_tags` are the artist's own tags that the site shows as theirs, as the
    reviewed list spells them, each with the number of those works that carry it.
    The order is a hash of the seed and the artist, as in tagging.sample(). Only profiles that are current, and
    made as this version makes them: the critic holds each against the images as they are now.
    """
    fresh = [row for row in profiles(db) if row["current"] and row["template_version"] == TEMPLATE_VERSION]
    chosen = sorted(fresh, key=lambda row: hashlib.sha256(f"{seed}:{row['artist_key']}".encode()).digest())
    query = "SELECT artist_key, deviation_id, image_file FROM works ORDER BY 2"
    collected = _transact(db, lambda conn: conn.execute(query).fetchall())
    found = []
    for row in chosen[: max(count, 0)]:
        counted = set(row["facts"]["counted"])
        images = [(work_id, name) for key, work_id, name in collected if key == row["artist_key"] and name]
        found.append(
            {
                "artist": row["artist_key"],
                "template": row["template"],
                "written": row["written"],
                "written_by_model": row["written_by_model"],
                "facts": row["facts"],
                "open_for_work": row["open_for_work"],
                "open_evidence": row["open_evidence"],
                "own_tags": row["facts"]["counts"]["own_words"],
                "images": [str(library.image_dir() / name) for work_id, name in images if work_id in counted],
                "images_left_out": [
                    str(library.image_dir() / name) for work_id, name in images if work_id not in counted
                ],
            }
        )
    return found


def parity(*, ollama_url: str = tagging.OLLAMA_URL, transport: httpx.BaseTransport | None = None) -> dict[str, Any]:
    """Embed the eval queries here and on Workers AI, and say how far the two agree.

    Only the query texts are sent to Cloudflare. The token goes in a header. The account id is part of the
    address, as the API has it, so nothing here ever prints or logs an address or an httpx error's message.
    RuntimeError, with the kind of failure and no more, when Workers AI does not answer with 1,024 numbers a query.
    """
    queries = [json.loads(line)["query"] for line in QUERIES.read_text(encoding="utf-8").splitlines() if line.strip()]
    with tagging._client(ollama_url, transport) as client:
        here = _embed(client, queries)
    env = config.load_env()
    if not (env.get("CLOUDFLARE_ACCOUNT_ID") and env.get("CLOUDFLARE_API_TOKEN")):
        raise RuntimeError("CLOUDFLARE_ACCOUNT_ID and CLOUDFLARE_API_TOKEN must be set in .env")
    address = f"https://api.cloudflare.com/client/v4/accounts/{env['CLOUDFLARE_ACCOUNT_ID']}/ai/run/{CLOUD_EMBEDDER}"
    headers = {"Authorization": f"Bearer {env['CLOUDFLARE_API_TOKEN']}"}
    log = logging.getLogger("httpx")  # it logs the address of every request
    was_disabled, log.disabled = log.disabled, True
    try:
        with httpx.Client(transport=transport, timeout=60, trust_env=False) as cloud:
            response = cloud.post(address, headers=headers, json={"text": queries})
        response.raise_for_status()
        there = [_unit(vector) for vector in response.json()["result"]["data"]]
        if len(there) != len(queries):
            raise ValueError
    except (httpx.HTTPError, ValueError, KeyError, TypeError) as error:
        status = getattr(getattr(error, "response", None), "status_code", "no status")
        raise RuntimeError(f"Workers AI did not answer as expected ({type(error).__name__}, {status})") from None
    finally:
        log.disabled = was_disabled

    def orders(vectors: list[list[float]]) -> list[list[int]]:
        """For each query, the other queries from the nearest to the farthest."""
        near = [[sum(map(float.__mul__, a, b)) for b in vectors] for a in vectors]
        others = [[j for j in range(len(vectors)) if j != i] for i in range(len(vectors))]
        return [sorted(rest, key=row.__getitem__, reverse=True) for rest, row in zip(others, near, strict=True)]

    cosines = [sum(map(float.__mul__, a, b)) for a, b in zip(here, there, strict=True)]
    local, cloud_order = orders(here), orders(there)
    pairs = list(zip(local, cloud_order, strict=True))
    same = sum(a == b for a, b in pairs)
    return {
        "queries": len(queries),
        "cosine_min": round(min(cosines), 6),
        "cosine_median": round(statistics.median(cosines), 6),
        "cosine_max": round(max(cosines), 6),
        "neighbour_order_same": same == len(queries),
        "queries_with_same_order": same,
        "queries_with_same_nearest": sum(a[0] == b[0] for a, b in pairs),
        # How many places apart the same neighbour is in the two orders, at most. 1 is two neighbours swapped.
        "largest_rank_shift": max(abs(a.index(j) - b.index(j)) for a, b in pairs for j in a),
    }


def main(argv: list[str] | None = None, transport: httpx.BaseTransport | None = None) -> None:
    parser = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--max-artists", type=int, help="profile at most this many artists that lack a current row")
    mode.add_argument("--sample", type=int, help="print this many stored profiles, one JSON object a line")
    mode.add_argument("--parity", action="store_true", help="compare the local embedding with Workers AI's")
    parser.add_argument("--seed", type=int, default=0, help="which sample")
    parser.add_argument("--ollama-url", default=tagging.OLLAMA_URL, help="a loopback address, nothing else is accepted")
    args = parser.parse_args(argv)
    if args.sample is not None:
        for row in sample(args.sample, args.seed):
            print(json.dumps(row))
        return
    try:
        if args.parity:
            print(json.dumps(parity(ollama_url=args.ollama_url, transport=transport)))
        else:
            print(json.dumps(run(args.max_artists, ollama_url=args.ollama_url, transport=transport)))  # counts only
    except (ValueError, RuntimeError) as error:
        sys.exit(f"stopped: {error}")
    except httpx.HTTPError as error:  # the name of the error only; what was stored before it is kept
        sys.exit(f"stopped: Ollama did not answer ({type(error).__name__}). Rerun to carry on.")


if __name__ == "__main__":
    main()
