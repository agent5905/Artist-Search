"""A profile for each artist: the facts about their work, one or two plain sentences, and a vector for search.

    uv run python -m pipeline.profiles --max-artists 500
    uv run python -m pipeline.profiles --sample 20 --seed 3     one JSON object a line, for the critic
    uv run python -m pipeline.profiles --parity                 the local embedding against Workers AI's

An artist is profiled once every collected work of theirs with a cached image has a verified tag result (the
result set "<model>+<model>" of pipeline/tagging.py, at its current prompt version) and a reading of its own
title and tags. Until then the artist waits, and has no row here.

The facts are counts over the artist's eligible works: analyzed, not flagged or rated explicit, not flagged
not_artwork, not declared AI-made. Only the verified tags are counted, never the descriptions. What a profile
may state is a share of those works, and never rests on fewer than MIN_SUPPORT of them:
  - a subject at PART_PERCENT, so that both halves of a split portfolio are named;
  - a genre at PART_PERCENT, hedged ("with fantasy themes") until it reaches CLEAR_PERCENT;
  - a drawing manner at PART_PERCENT, hedged ("partly in an anime style") until most works have it;
  - a format, once the tagging stage has sorted every eligible work by format, on the manner's terms. One that
    names a subject ("environment keyframe or scene") counts on a work only where the verified subject agrees;
  - a medium at CLEAR_PERCENT, and the text leaves out "digital painting", which nearly every artist has;
  - a colour word the verifier checked at CLEAR_PERCENT, any other style word at NEARLY_ALL_PERCENT;
  - a word of the artist's own (OWN_WORDS) when most works carry it as a tag and their verified subject agrees.
"Most" is more than half.

Open for work is True only on the artist's own words (see open_evidence), and otherwise unknown. Never False.

Two texts are kept: "written", composed by the local language model from the stated facts alone and then held
to them by unsupported(), which round 1's blind critic preferred 16 to 2; and "template", built by code, which
stands in when the model's text fails MAX_WRITES times, and which the vector is made from.

One table, artist_profiles, in the local SQLite file. Hard rule 7: a row is written only for an artist the
store still lists, rows of artists it no longer lists go at the start of every run, and no read returns one.
Hard rule 9: the models are Ollama's on a loopback address. `--parity` alone talks to anything else: it sends
the eval queries, and nothing about any artist, to Workers AI.
Every function takes an optional `db` path; the default is config.LOCAL_DB.
"""

import argparse
import hashlib
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
from collections.abc import Callable, Iterable
from datetime import UTC, datetime, timedelta
from typing import Any

import httpx

from pipeline import config, library, store, tagging

TAGGER = tagging.DEFAULT_MODEL  # whose tags are read: this model's, verified by itself
WRITER = "qwen3.8:latest"  # the same model the tagging run keeps loaded, asked with the same settings
EMBEDDER = "bge-m3"
CLOUD_EMBEDDER = "@cf/baai/bge-m3"  # what the search box embeds a query with
DIMENSIONS = 1024  # sql/001_schema.sql: artists.profile_embedding is halfvec(1024)
QUERIES = config.ROOT / "eval" / "queries.jsonl"

MIN_WORKS = 5  # eligible works; with fewer an artist cannot be characterised
MIN_SUPPORT = 3  # works: nothing is stated on fewer, whatever share they are
PART_PERCENT = 30  # of the eligible works: a subject is named; a genre, manner or format is stated with a hedge
CLEAR_PERCENT = 70  # a genre is stated flat; a medium or a verified colour word is a fact
NEARLY_ALL_PERCENT = 90  # any other style word is a fact
AT_MOST = {"specialties": 3, "worlds": 2, "manners": 1, "own_words": 2, "formats": 2, "style_words": 2}
RECENT_DAYS = 180  # a work's own tags say something about commissions for this long
MAX_PROFILE = 340  # characters of a profile's one sentence; the longest the template can write is 321
MAX_WRITES = 3  # times the model is asked for one artist's sentence
TEMPLATE_VERSION = 2  # bump when template() would say something else
WRITTEN_VERSION = 3  # bump when WRITE_PROMPT, GLUE or unsupported() changes

# Every value a profile may state, and the spellings it may state it in. The first is the template's.
# "photo" and "unclear" are not here: neither says what the art is made with. Nor is "digital" on its own: with
# the style word "line art" beside it, it would spell a medium the facts do not have.
MEDIUM_FORMS = {
    "digital painting": ("digital painting", "digital paintings"),
    "digital line art": ("digital line art", "digital line work", "digital linework"),
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
    "prop or weapon": ("prop or weapon", "props or weapons", "prop", "props"),
    "vehicle or mech": ("vehicle or mech", "vehicles or mechs"),
    "interface or icons": ("interface or icon", "interface or icons", "interfaces or icons"),
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
    "stylized cartoon": ("stylized cartoon", "stylised cartoon", "cartoon"),
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
    "lineart": ("line art", "lineart", "line work", "linework"),
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
# How a format of the tagging stage is said. A format it adds later is said as it spells it (see _spellings).
FORMAT_FORMS = {
    "character sheet or turnaround": ("character sheets", "character sheet", "character sheets or turnarounds"),
    "creature sheet": ("creature sheets", "creature sheet"),
    "expression or pose sheet": ("expression or pose sheets", "expression or pose sheet"),
    "environment keyframe or scene": ("environment scenes", "environment scene", "environment keyframes or scenes"),
    "concept sketches or thumbnails": ("concept sketches", "concept sketches or thumbnails"),
    "card or splash illustration": ("card or splash illustrations", "card or splash illustration"),
    "portrait or bust": ("portraits", "portrait", "portraits or busts"),
    "adoptable or design-for-sale sheet": ("designs for sale or adoption", "designs for adoption or sale"),
    "comic page or panel sequence": ("comic pages or panel sequences", "comic page or panel sequence"),
    "icons or interface elements": ("icons or interface elements",),
    "sprite sheet": ("sprite sheets", "sprite sheet"),
    "colour keys or mood studies": ("colour keys or mood studies", "color keys or mood studies"),
}
# A format that names a subject counts on a work only when the verifier gave the work that subject, as a word of
# the artist's own does: the format is one model's pick, and a figure in a room is not an "environment scene".
FORMAT_OF = {
    "character sheet or turnaround": "character",
    "expression or pose sheet": "character",
    "portrait or bust": "character",
    "creature sheet": "creature",
    "environment keyframe or scene": "environment",
    "icons or interface elements": "interface or icons",
}
SAID_BY_THE_ARTIST = {"adoptable or design-for-sale sheet": "adoptable designs"}  # one of OWN_WORDS: see facts_of
_FORMAT = re.compile(r"[a-z]+(?:[ -][a-z]+){0,5}")  # what a format this module has never heard of must look like


def _is_format(value: object, subjects: list[str]) -> bool:
    """True for a format this module knows how to say, or a plainly spelled one of the tagging stage's own list,
    on a work whose verified subjects bear it out."""
    if not isinstance(value, str) or not (value in FORMAT_FORMS or value in getattr(tagging, "FORMATS", ())):
        return False
    return bool(_FORMAT.fullmatch(value)) and FORMAT_OF.get(value) in (None, *subjects)


# Words of the artist's own. Each names a kind of a verified subject: (the subjects, one, many, and the other
# spellings of the tag; "character design" is the tag "characterdesign" or "characterdesigns" without being told).
# A tag counts on a work only when the verifier gave that work one of those subjects, and the word is stated only
# when that is so for most of the artist's works: their "dragon" under a verified "creature".
# Nothing gets in that is not here, so no tag an artist puts on everything can: "art", "digitalart", "oc",
# "commission", "fantasy", "conceptart". Nor a tag that only contains a word: "dragonage", "catgirl".
# ponytail: a hand list, and what is not on it is never said, however plainly an artist says it. The upgrade is to
# ask the language model whether a tag most works carry names a kind of the verified subject, and to hold its
# answer to the tags themselves.
_ANY = tuple(SUBJECT_FORMS)
_OWN = (
    (("creature",), "dragon", "dragons", ""),
    (("creature",), "monster", "monsters", ""),
    (("creature",), "beast", "beasts", ""),
    (("creature",), "dinosaur", "dinosaurs", ""),
    (("creature",), "animal", "animals", ""),
    (("creature",), "wolf", "wolves", ""),
    (("creature",), "horse", "horses", ""),
    (("creature",), "bird", "birds", ""),
    (("creature",), "cat", "cats", ""),
    (("creature",), "dog", "dogs", ""),
    (("creature",), "creature design", "creature designs", "creatureconcept creatureconcepts creatureconceptart"),
    (("character",), "warrior", "warriors", ""),
    (("character",), "knight", "knights", ""),
    (("character",), "elf", "elves", ""),
    (("character",), "mage", "mages", ""),
    (("character",), "wizard", "wizards", ""),
    (("character",), "witch", "witches", ""),
    (("character",), "angel", "angels", ""),
    (("character",), "assassin", "assassins", ""),
    (("character",), "soldier", "soldiers", ""),
    (("character",), "samurai", "samurai", ""),
    (("character",), "pirate", "pirates", ""),
    (("character",), "vampire", "vampires", ""),
    (("character",), "portrait", "portraits", ""),
    (("character",), "character design", "character designs", "characterconcept characterconcepts characterconceptart"),
    (
        ("character",),
        "character sheet",
        "character sheets",
        "referencesheet refsheet characterreference characterturnaround",
    ),
    (("character", "creature"), "demon", "demons", ""),
    (("character", "creature"), "alien", "aliens", ""),
    (("character", "creature"), "anthro character", "anthro characters", "anthro furry"),
    (("character", "creature"), "adoptable design", "adoptable designs", "adoptable adoptables adopt adopts"),
    (("character", "creature", "vehicle or mech"), "robot", "robots", ""),
    (("environment",), "landscape", "landscapes", ""),
    (("environment",), "cityscape", "cityscapes", ""),
    (("environment",), "castle", "castles", ""),
    (("environment",), "interior", "interiors", ""),
    (("environment",), "architecture", "architecture", ""),
    (("environment",), "environment design", "environment designs", "environmentconcept environmentconceptart"),
    (("environment",), "matte painting", "matte paintings", ""),
    (("prop or weapon",), "weapon", "weapons", ""),
    (("prop or weapon",), "sword", "swords", ""),
    (("prop or weapon",), "gun", "guns", ""),
    (("prop or weapon",), "weapon design", "weapon designs", ""),
    (("prop or weapon",), "prop design", "prop designs", ""),
    (("character", "prop or weapon"), "armour", "armour", "armor armors armours"),
    (("vehicle or mech",), "mech", "mechs", "mecha mechas"),
    (("vehicle or mech",), "spaceship", "spaceships", "starship starships"),
    (("vehicle or mech",), "tank", "tanks", ""),
    (("vehicle or mech",), "ship", "ships", ""),
    (("vehicle or mech",), "vehicle design", "vehicle designs", ""),
    (("vehicle or mech",), "mech design", "mech designs", "mechadesign"),
    (_ANY, "fan art", "fan art", "fanarts"),
)
# many -> (the verified subjects it is a kind of, every spelling of the tag with "_" and "-" taken out)
OWN_WORDS = {
    many: (frozenset(under), frozenset({one.replace(" ", ""), many.replace(" ", ""), *more.split()}))
    for under, one, many, more in _OWN
}
OWN_FORMS = {many: (many, one) for _, one, many, _ in _OWN}

GENRE_FORMS = WORLD_FORMS | MANNER_FORMS
FORMS = MEDIUM_FORMS | SUBJECT_FORMS | GENRE_FORMS | STYLE_FORMS | FORMAT_FORMS | OWN_FORMS
# The fields of the facts whose values a profile may state, in the order the vector's text lists them.
STATED = ("specialties", "own_words", "mediums", "genres", "formats", "style_words")
_PAIRED = ("prop or weapon", "vehicle or mech", "interface or icons")  # each said on its own: "prop or weapon art"
# A fact that holds for part of the works may only be said after one of these, in the same clause: the first is
# the template's. "With" is a hedge for a genre ("with fantasy themes") and none for a manner or a format.
_HEDGES = {
    "worlds": ("with", "partly", "some", "including", "also"),
    "manners": ("partly", "some", "including", "also"),
    "formats": ("including", "partly", "some", "also"),
}

# The only words a written profile may use besides the facts. None of them says what the art is, what it is
# made with or how good it is, and the three that say how much of it ("partly", "some", "including") can only
# say less: no "paints", "concept", "illustration", "mostly".
GLUE = frozenset(
    "a an the and or with in of for on as to by across plus alongside both also that which who whose their its is "
    "are art artist artwork artworks work works working piece pieces image images style styles styled look looks "
    "theme themes themed subject subjects medium media focus focused focusing focuses mix mixes mixing combining "
    "combines done made rendered rendering featuring features colour color colours colors tone tones palette "
    "palettes shading partly some including".split()
)

WRITE_PROMPT = f"""You write one sentence for a search index of artists. It says what kind of art an artist makes, \
from a short list of facts. A reviewer will hold every word of it against the artist's images, so the sentence \
may say only what the facts say.

Rules
- Use the facts as they are given, word for word. You need not use every one. Add no fact of your own.
- One sentence in plain language that ends with a full stop, {MAX_PROFILE} characters at most, and much shorter \
when the facts are few. Do not name or address anyone. Write it the way a directory of artists would, short and \
natural, with the style, manner and theme words before the kind of art.
- The kinds of art are listed as they are to be said: "character" is character art, and "character designs" is \
that, not "character art".
- A fact under "most of the works" is said as "mostly dragons", or put in the place of the kind of art: \
"dragon art".
- A fact under "only part of the works" is true of some works and not of the rest. Say it only as it is listed, \
after its "with", "partly" or "including", and never before the kind of art.
- From "kinds of art: environment / medium: 3D / themes: science fiction / style: saturated" a good sentence \
is "Saturated science fiction environments in 3D." From "kinds of art: creature designs, character / most of \
the works: dragons / manner: realistic / only part of the works: with horror themes; including creature sheets" \
a good one is "Realistic creature designs and character art, mostly dragons, with horror themes, including \
creature sheets."
- Besides the facts, use only these plain words: {" ".join(sorted(GLUE))}.
- So: no numbers. No names of people, studios, games, films or software. No praise or judgement (stunning, \
talented, skilled, detailed, unique). Nothing about experience, career, clients, tools, prices or availability. \
No word of your own for how much or how often (mainly, often, sometimes, only, varied). No "concept", \
"illustration", "design", "painting" or "drawing" unless the facts have that word.

Answer with one JSON object with one field, profile."""
_WRITTEN = {"type": "object", "properties": {"profile": {"type": "string"}}, "required": ["profile"]}

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

_SCHEMA = """
CREATE TABLE IF NOT EXISTS artist_profiles (
    artist_key TEXT PRIMARY KEY,     -- works.artist_key
    basis TEXT NOT NULL,             -- sha256 of all the row was made from: a run that finds it unchanged does nothing
    excluded TEXT,                   -- one of REASONS: why there is no profile. NULL when there is one
    facts TEXT NOT NULL,             -- JSON: the counts, and the values that passed the threshold
    open_evidence TEXT NOT NULL,     -- JSON list of the artist's own words; empty means unknown
    template TEXT,                   -- the profile built by code
    template_version INTEGER,
    written TEXT,                    -- the profile composed by the language model, or the template again
    written_version INTEGER,
    written_by_model INTEGER,        -- 0 when the model's text failed the check and the template stands in
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


def _leading(counts: Counter[str], need: int, order: Iterable[str], at_most: int) -> list[str]:
    """The `at_most` values of `order` that most works have, of those at least `need` works have."""
    return sorted((value for value in order if counts[value] >= need), key=lambda value: -counts[value])[:at_most]


def facts_of(works: list[dict[str, Any]]) -> tuple[dict[str, Any], str | None]:
    """The facts about one artist, and why they get no profile (one of REASONS) or None.

    `works` has one entry per collected work: its id, its verified `result` (None when it has none), what its
    own title and tags `declared` about AI, and the artist's own `tags` on it. A work counts when it has a
    result and is not explicit (flagged, or rated so), not not_artwork and not declared AI-made. Each counted
    work adds one to its medium and to each of its subjects, genres, style words and formats, and to each of
    OWN_WORDS that one of its tags spells and one of its verified subjects bears out. Its description is never
    read. The module's docstring says which of those counts make a fact; `hedged` lists the facts that hold
    for part of the works only, and may not be stated flat.
    """

    def explicit(result: dict[str, Any]) -> bool:
        return result["explicit"] or result.get("rating") == "explicit"

    analyzed = [work["result"] for work in works if work["result"]]
    counted = [
        work
        for work in works
        if work["result"]
        and work["declared"] != "yes"
        and not explicit(work["result"])
        and not work["result"]["not_artwork"]
    ]
    fields = ("medium", "subjects", "genres", "style_words", "formats", "own_words")
    counts: dict[str, Counter[str]] = {field: Counter() for field in fields}
    for work in counted:
        result = work["result"]
        tags = {_plain(tag) for tag in work.get("tags", ())}
        counts["medium"][result["medium"]] += 1
        counts["subjects"].update(set(result["subjects"]))
        counts["genres"].update(set(result["genres"]))
        counts["style_words"].update({COLOUR_WORDS.get(word, word) for word in result["style_words"]})
        counts["formats"].update(
            {value for value in result.get("formats") or () if _is_format(value, result["subjects"])}
        )
        counts["own_words"].update(
            word for word, (under, spelled) in OWN_WORDS.items() if tags & spelled and under & set(result["subjects"])
        )
    part, clear, nearly_all = (
        max(MIN_SUPPORT, -(-len(counted) * percent // 100))
        for percent in (PART_PERCENT, CLEAR_PERCENT, NEARLY_ALL_PERCENT)
    )
    most = max(MIN_SUPPORT, len(counted) // 2 + 1)
    worlds = _leading(counts["genres"], part, WORLD_FORMS, AT_MOST["worlds"])
    manners = _leading(counts["genres"], part, MANNER_FORMS, AT_MOST["manners"])
    # A format is a fact only once every counted work has been sorted by format: until then the counts are short.
    formats = []
    if all(work["result"].get("formats") is not None for work in counted):
        formats = _leading(counts["formats"], part, sorted(counts["formats"]), AT_MOST["formats"])
    # One thing is said once. "Portraits" the format is the tag "portrait": the format stays. "Adoptable designs" is
    # the artist's word for what its format can only call designs for sale or adoption: the artist's word stays.
    said_already = {_spellings(value)[0] for value in formats}
    own = [word for word in OWN_WORDS if word not in said_already]
    own = _leading(counts["own_words"], most, own, AT_MOST["own_words"])
    formats = [value for value in formats if SAID_BY_THE_ARTIST.get(value) not in own]
    mediums = _leading(counts["medium"], clear, MEDIUM_FORMS, 1)
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
        },
        "specialties": _leading(counts["subjects"], part, SUBJECT_FORMS, AT_MOST["specialties"]),
        "own_words": own,
        "mediums": mediums,
        "genres": worlds + manners,
        "formats": formats,
        "style_words": _leading(styles, 1, STYLE_FORMS, AT_MOST["style_words"]),
        "hedged": [world for world in worlds if counts["genres"][world] < clear]
        + [manner for manner in manners if counts["genres"][manner] < most]
        + [value for value in formats if counts["formats"][value] < most],
    }
    if 2 * ai > len(works):
        return facts, "mostly_ai_declared"
    if 2 * explicits > len(analyzed):
        return facts, "mostly_explicit"
    if len(counted) < MIN_WORKS:
        return facts, "too_few_works"
    return facts, None if _stated(facts) else "nothing_clear"


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


def _open_sentence(evidence: list[dict]) -> str:
    """What a profile says of the evidence: nothing without any, and "commissions" only when those are named."""
    if not evidence:
        return ""
    return " " + OPEN_SENTENCE["commissions" if any(item["kind"] == "commissions" for item in evidence) else "hire"]


def _spellings(term: str) -> tuple[str, ...]:
    """How a fact may be spelled, the template's way first. A format this module does not know is said as it is."""
    return FORMS.get(term) or (f"{term} work", term)


def _kind(term: str) -> str:
    return "worlds" if term in WORLD_FORMS else "manners" if term in MANNER_FORMS else "formats"


def _stated(facts: dict[str, Any]) -> dict[str, tuple[str, ...]]:
    """Every fact a profile of these facts may state -> the words one of which must hedge it, none for a flat one."""
    terms = (term for field in STATED for term in facts[field] if term != COMMON_MEDIUM)
    return {term: _HEDGES[_kind(term)] if term in facts["hedged"] else () for term in terms}


def _and(items: list[str]) -> str:
    return " and ".join(filter(None, [", ".join(items[:-1]), *items[-1:]]))


def _kinds(facts: dict[str, Any]) -> tuple[list[str], list[str]]:
    """The kinds of art a profile names, and what most of the works are, more closely than that.

    A kind is a subject, as the tagging stage spells it, or the artist's own word for it: "character designs"
    takes the place of "character", so that nothing reads "character art, mostly character designs". Most of
    the works are what the artist's other words say, and a format that most works have.
    """
    mostly = facts["own_words"] + [_spellings(value)[0] for value in facts["formats"] if value not in facts["hedged"]]
    designs = {subject: f"{_spellings(subject)[0]} designs" for subject in facts["specialties"]}
    kinds = [designs[subject] if designs[subject] in mostly else subject for subject in facts["specialties"]]
    return kinds, [word for word in mostly if word not in kinds]


def _hedged(facts: dict[str, Any]) -> list[str]:
    """The clauses that say what holds for part of the works, each begun by the hedge of its kind."""
    said = {kind: [_spellings(term)[0] for term in facts["hedged"] if _kind(term) == kind] for kind in _HEDGES}
    clauses = [f"with {_and(said['worlds'])} themes"] if said["worlds"] else []
    clauses += [f"partly in {'an' if manner[0] in 'aeiou' else 'a'} {manner} style" for manner in said["manners"]]
    return clauses + ([f"including {_and(said['formats'])}"] if said["formats"] else [])


def template(facts: dict[str, Any]) -> str:
    """One sentence that states every fact these facts allow and nothing else, the same way for every artist:
    what leads it is what holds for the body of work, and what holds for part of it comes after, hedged."""
    said = {term: _spellings(term)[0] for term in _stated(facts)}
    flat = [term for term in facts["genres"] if term not in facts["hedged"]]
    kinds, mostly = _kinds(facts)
    plain = [said[kind] for kind in kinds if kind in SUBJECT_FORMS and kind not in _PAIRED]
    named = [kind for kind in kinds if kind not in SUBJECT_FORMS] + ([f"{_and(plain)} art"] if plain else [])
    named += [f"{said[kind]} art" for kind in kinds if kind in _PAIRED]
    lead = [", ".join(said[word] for word in facts["style_words"])]
    lead += [said[manner] for manner in flat if manner in MANNER_FORMS]
    lead += [_and([said[world] for world in flat if world in WORLD_FORMS]), _and(named) or "artwork"]
    text = " ".join(filter(None, lead))
    if media := [said[medium] for medium in facts["mediums"] if medium in said]:
        text += f" in {_and(media)}"
    if mostly:
        text += f", mostly {_and(mostly)}"
    text = ", ".join([text, *_hedged(facts)])
    return text[0].upper() + text[1:] + "."


def unsupported(text: object, facts: dict[str, Any]) -> str | None:
    """Why this text may not be the profile of these facts. None when every word of it is supported.

    It must be one sentence of MAX_PROFILE characters at most. Once every fact it may state is taken out of
    it, in any spelling of FORMS, each word left must be one of GLUE. So anything else is refused, whatever
    it is: a medium, subject, genre, format or style word the facts do not have, a number, a name, praise, a
    claim about experience, clients, software or availability. A fact that holds for part of the works must
    come after a hedge in its own clause ("with", "partly", "including"). And two facts at least are stated.

    ponytail: each fact is checked on its own. "Painterly fantasy character art" passes when each of the three
    holds for its share of the works, whether or not the same works have all three, and nothing checks which
    fact a sentence leads with. Counting pairs of facts over the works is the upgrade, if a critic finds a
    profile that joins what the works keep apart.
    """
    if not isinstance(text, str) or len(text) > MAX_PROFILE:
        return f"it must be a text of {MAX_PROFILE} characters at most"
    if not re.fullmatch(r"[^.!?\n]*\w[^.!?\n]*\.", text.strip()):
        return "it must be one sentence that ends with a full stop"
    whole, stated, used = text.strip().lower()[:-1], _stated(facts), set()
    spellings = [(form.lower(), term) for term in stated for form in _spellings(term)]
    # "mostly" is no plain word: it may stand only before what most of the works are. A format that part of them
    # are is refused below all the same, for want of a hedge.
    most = {*facts["own_words"], *facts["formats"]}
    spellings += [(f"mostly {form}", term) for form, term in spellings if term in most]
    rest = whole
    for form, term in sorted(spellings, key=lambda pair: -len(pair[0])):  # the longest first: "dark fantasy"
        said = re.compile(rf"(?<![\w-]){re.escape(form)}(?![\w-])")
        for found in said.finditer(rest):
            clause = whole[
                max(whole.rfind(",", 0, found.start()), whole.rfind(";", 0, found.start())) + 1 : found.start()
            ]
            if stated[term] and not re.search(rf"\b(?:{'|'.join(stated[term])})\b", clause):
                return f'"{form}" is true of only part of the works: say it after "{stated[term][0]}", never flat'
            used.add(term)
        rest = said.sub(lambda found: " " * len(found.group()), rest)  # the same length: places stay where they were
    for word in re.findall(r"[^\s,;]+", rest):
        if word not in GLUE:
            return f'"{word}" is not one of the facts and not one of the plain words allowed'
    # "character designs" states two facts: the artist's word, and the subject it is a kind of
    used |= {subject for subject in facts["specialties"] if f"{_spellings(subject)[0]} designs" in used}
    return None if len(used) >= min(2, len(stated)) else "it must state at least two of the facts"


def _question(facts: dict[str, Any]) -> str:
    """All the model is told about an artist: the facts a profile may state, as the template would spell them."""
    stated = _stated(facts)
    kinds, mostly = _kinds(facts)

    def flat(terms: Iterable[str]) -> str:
        return ", ".join(_spellings(term)[0] for term in terms if term in stated and not stated[term])

    said = {
        "kinds of art": flat(kinds),
        "most of the works": ", ".join(mostly),
        "medium": flat(facts["mediums"]),
        "manner": flat(term for term in facts["genres"] if term in MANNER_FORMS),
        "themes": flat(term for term in facts["genres"] if term in WORLD_FORMS),
        "style": flat(facts["style_words"]),
        "only part of the works": "; ".join(_hedged(facts)),
    }
    return "Facts" + "".join(f"\n{name}: {terms}" for name, terms in said.items() if terms)


def _written(client: httpx.Client, facts: dict[str, Any]) -> str | None:
    """The model's sentence for these facts, or None when none of MAX_WRITES passes unsupported().

    Asked through tagging's own call, so with its settings: a second context size for the same model would
    make Ollama reload it between this stage's requests and the tagging run's.
    """
    question, refused = _question(facts), None
    for attempt in range(MAX_WRITES):
        asked = (
            question if refused is None else f"{question}\n\nYour last sentence was refused: {refused}. Write it again."
        )
        answer = tagging._chat(client, WRITER, WRITE_PROMPT, asked, None, _WRITTEN, attempt)
        text = answer.get("profile") if isinstance(answer, dict) else None
        text = " ".join(text.split()) if isinstance(text, str) else None
        refused = unsupported(text, facts)
        if refused is None:
            return text
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

    The second part does not depend on how a sentence happens to be worded, and has what the sentence leaves
    out: the medium everybody has, and each format as the tagging stage spells it.
    """
    names = {"specialties": "Specialties", "own_words": "In the artist's words", "mediums": "Mediums"}
    names |= {"genres": "Genres", "formats": "Formats", "style_words": "Style"}
    return profile + "".join(f"\n{names[field]}: {', '.join(facts[field])}." for field in STATED if facts[field])


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
    tagged yet; a row they had is deleted, because it is about works that have changed), left_for_next_run,
    removed (rows of artists the store no longer lists, deleted), open_for_work and written_fallback (of the
    profiled), and seconds_per_artist. Each row is committed as it is made. ValueError before anything is sent
    if the address is not loopback; httpx.HTTPError when Ollama is in trouble, with what was stored kept.
    """
    now = now or datetime.now(UTC)
    counts = dict.fromkeys(("profiled", *(f"excluded_{reason}" for reason in REASONS)), 0)
    counts |= dict.fromkeys(("unchanged", "waiting_for_tags", "left_for_next_run", "removed"), 0)
    counts |= {"open_for_work": 0, "written_fallback": 0}
    versions = [TEMPLATE_VERSION, WRITTEN_VERSION, WRITER, EMBEDDER]
    result_set = tagging.result_set(TAGGER, TAGGER)

    def read(conn: sqlite3.Connection) -> tuple[list[Any], dict[str, str | None], set[str], dict[str, str]]:
        return (
            conn.execute(
                "SELECT artist_key, deviation_id, image_file, tags, published_at FROM works ORDER BY 1, 2"
            ).fetchall(),
            dict(conn.execute("SELECT artist_key, tagline FROM profiles").fetchall()),
            {
                work_id
                for (work_id,) in conn.execute(
                    "SELECT deviation_id FROM work_tags WHERE model = ? AND prompt_version = ?",
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
        # A tagline is quoted as evidence, so only one that was read and found to hold no private contact is used.
        quotable = {key for key, answer in tagging.contacts(TAGGER, db).items() if answer == "no"}
        rows, taglines, tagged, stored = _transact(db, read)
        collected: dict[str, list[Any]] = {}
        for key, *work in rows:
            collected.setdefault(key, []).append(work)
        stale = stored.keys() - (collected.keys() & listed)
        counts["removed"] = len(stale)
        todo = []
        for key in sorted(collected.keys() & listed):
            works = collected[key]
            if not all(work_id in tagged and work_id in declared for work_id, image, _, _ in works if image):
                counts["waiting_for_tags"] += 1
                stale |= {key} & stored.keys()
                continue
            own = [(work_id, json.loads(tags), at) for work_id, _, tags, at in works]
            facts, reason = facts_of(
                [
                    {"id": work_id, "result": results.get(work_id), "declared": declared.get(work_id), "tags": tags}
                    for work_id, tags, _ in own
                ]
            )
            evidence = open_evidence(taglines.get(key) if key in quotable else None, own, now)
            basis = hashlib.sha256(json.dumps([versions, facts, evidence], sort_keys=True).encode()).hexdigest()
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
            built, composed, is_open = template(facts), _written(client, facts), _open_sentence(evidence)
            # ponytail: the vector is of the template whichever text the critic prefers, so every artist is
            # embedded in the same words. Embed the written text instead if search turns out to need its variety.
            blob = array("f", _embed(client, [embedded_text(built + is_open, facts)])[0]).tobytes()
            seconds += time.monotonic() - started
            texts = (built + is_open, TEMPLATE_VERSION, (composed or built) + is_open, WRITTEN_VERSION)
            if store_row(
                key, basis, None, json.dumps(facts), json.dumps(evidence), *texts, int(composed is not None), blob
            ):
                counts["profiled"] += 1
                counts["open_for_work"] += bool(evidence)
                counts["written_fallback"] += composed is None
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


def profiles(db: pathlib.Path | str | None = None) -> list[dict[str, Any]]:
    """Every artist's profile, in key order, for the push stage. Never an excluded or a removed artist's.

    Each has artist_key, the two texts with their versions (and written_by_model: False when the written text
    is the template again), the search filters mediums and specialties, the leading genres, formats,
    style_words and own_words (the artist's own), hedged (which of those hold for part of the works only),
    open_for_work (True, or None for unknown) with its open_evidence, the facts with their counts, and the
    embedding as 1,024 numbers.
    """
    found = []
    for key, _, facts_json, evidence, built, built_version, composed, composed_version, by_model, blob in _rows(
        db, "excluded IS NULL"
    ):
        facts, vector = json.loads(facts_json), array("f")
        vector.frombytes(blob)
        found.append(
            {
                "artist_key": key,
                "template": built,
                "template_version": built_version,
                "written": composed,
                "written_version": composed_version,
                "written_by_model": bool(by_model),
                **{field: facts[field] for field in (*STATED, "hedged")},
                "open_for_work": True if json.loads(evidence) else None,
                "open_evidence": json.loads(evidence),
                "facts": facts,
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
    (flagged or declared AI-made). `own_tags` are, for each word of the artist's own that the profile uses, the
    tags that say it, as the artist wrote them on those works: the support a claim has that no image shows.
    The order is a hash of the seed and the artist, as in tagging.sample().
    """
    chosen = sorted(profiles(db), key=lambda row: hashlib.sha256(f"{seed}:{row['artist_key']}".encode()).digest())
    query = "SELECT artist_key, deviation_id, image_file, tags FROM works ORDER BY 2"
    collected = _transact(db, lambda conn: conn.execute(query).fetchall())
    found = []
    for row in chosen[: max(count, 0)]:
        counted = set(row["facts"]["counted"])
        works = [
            (work_id, name, json.loads(tags)) for key, work_id, name, tags in collected if key == row["artist_key"]
        ]
        images = [(work_id, name) for work_id, name, _ in works if name]
        written = {tag for work_id, _, tags in works if work_id in counted for tag in tags}
        found.append(
            {
                "artist": row["artist_key"],
                "template": row["template"],
                "written": row["written"],
                "written_by_model": row["written_by_model"],
                "facts": row["facts"],
                "open_for_work": row["open_for_work"],
                "open_evidence": row["open_evidence"],
                "own_tags": {
                    word: sorted(tag for tag in written if _plain(tag) in OWN_WORDS[word][1])
                    for word in row["own_words"]
                },
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
