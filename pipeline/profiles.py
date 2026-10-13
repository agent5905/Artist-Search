"""A profile for each artist: the facts about their work, one or two plain sentences, and a vector for search.

    uv run python -m pipeline.profiles --max-artists 500
    uv run python -m pipeline.profiles --sample 20 --seed 3     one JSON object a line, for the critic
    uv run python -m pipeline.profiles --parity                 the local embedding against Workers AI's

An artist is profiled once every collected work of theirs with a cached image has a verified tag result (the
result set "<model>+<model>" of pipeline/tagging.py, at its current prompt version) and a reading of its own
title and tags. Until then the artist waits: one who has no row gets none, and one who has a row keeps it, so
that an artist is not gone from the site each time collection brings a new work of theirs.

The facts are counts over the artist's eligible works: analyzed, not flagged or rated explicit, not flagged
not_artwork, not declared AI-made. Only the verified tags are counted, never the descriptions. What a profile
may state is a share of those works, and never rests on fewer than MIN_SUPPORT of them:
  - a subject at PART_PERCENT, so that both halves of a split portfolio are named;
  - a genre at PART_PERCENT, hedged ("with fantasy themes") until most works have it ("mostly fantasy"), and
    flat from CLEAR_PERCENT;
  - a drawing manner at PART_PERCENT, hedged ("partly in an anime style") until most works have it ("mainly in
    an anime style"), and flat from CLEAR_PERCENT;
  - a medium at CLEAR_PERCENT, and the text leaves out "digital painting", which nearly every artist has;
  - a colour word the verifier checked at CLEAR_PERCENT, any other style word at NEARLY_ALL_PERCENT;
  - a word of the artist's own (OWN_WORDS) when most works carry it as a tag and their verified subject agrees.
"Most" is more than half. Which works have each of these is kept (`tagged`), so that what a card says in one
breath can be held to the works that have all of it.

Then the looks, which are what makes one artist's profile unlike the next. The eligible works go on one contact
sheet, held in memory (hard rule 2), and the vision model proposes up to three concrete things that recur
across it. Nothing on a card is counted there: both of round 5's unsupported claims had passed a count made on
ten thumbnails, which gave "anime" 9 pictures where the verified tags had 5. Each work is looked at once, at
full size (_looked): the model picks the kind of output it is from KINDS (a design, an illustration, a
portrait, a sketch, a comic page, a map), and says of each proposed thing and each word of the artist's own,
word by word, whether the picture plainly shows it. A thing counts on a work only when every word of it does.
The shares decide the wording (see specifics):
  - the kind of output that most of the works are is the noun the card is built on, after the subject the
    verified tags give those works ("Character designs", and "design sheets" from CLEAR_PERCENT); a kind of
    fewer works is "including designs" from PART_PERCENT;
  - a thing is "mostly dragons" from most of the works and "including dragons" from PART_PERCENT;
  - what stands flat before the noun is true together: "Fantasy character designs" needs most works to be
    fantasy and a character design at once, or the genre goes behind it ("mostly fantasy").
An artist with no sheet, or of whom nothing is kept, has no profile ("nothing_clear"). Two claims are still
counted on the sheet and never stated, for the push stage to weigh: the pictures that are sexualised or gory,
and those of the one narrow subject the artist repeats most.

Open for work is True only on the artist's own words (see open_evidence), and otherwise unknown. Never False.

Two texts are kept: "written", composed by the local language model from the stated facts alone and then held
to them by unsupported(), which round 1's blind critic preferred 16 to 2; and "template", built by code, which
stands in when the model's text fails MAX_WRITES times, and which the vector is made from. Both pass the print
check (_printed): the rules at the end of unsupported(), then the model's yes or no to the line as it stands.

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
from collections.abc import Callable, Iterable
from datetime import UTC, datetime, timedelta
from typing import Any

import httpx
from PIL import Image, ImageDraw, ImageFont

from pipeline import config, library, store, tagging

TAGGER = tagging.DEFAULT_MODEL  # whose tags are read: this model's, verified by itself
WRITER = "qwen3.8:latest"  # the same model the tagging run keeps loaded, asked with the same settings
EMBEDDER = "bge-m3"
CLOUD_EMBEDDER = "@cf/baai/bge-m3"  # what the search box embeds a query with
DIMENSIONS = 1024  # sql/001_schema.sql: artists.profile_embedding is halfvec(1024)
QUERIES = config.ROOT / "eval" / "queries.jsonl"

MIN_WORKS = 5  # eligible works; with fewer an artist cannot be characterised
MIN_SUPPORT = 4  # works: nothing is stated on fewer, whatever share they are
PART_PERCENT = 40  # of the eligible works: a subject is named; a genre, manner, kind or thing is said with a hedge
CLEAR_PERCENT = 70  # a genre or a kind of output is stated flat; a medium or a verified colour word is a fact
NEARLY_ALL_PERCENT = 90  # any other style word is a fact
MAX_MODIFIERS = 3  # style, manner and theme words before the kind of art; more than that go behind it
AT_MOST = {"specialties": 3, "worlds": 2, "manners": 1, "own_words": 2, "style_words": 2}
AT_MOST |= {"recurring": 3, "mostly": 2, "hedged": 2}  # things the sheet proposes; things said in one clause
CELL, PER_ROW, LABEL = 300, 5, 34  # the contact sheet: pixels a picture, pictures a row, pixels for its number
RECENT_DAYS = 180  # a work's own tags say something about commissions for this long
MAX_PROFILE = 420  # characters of a profile's one sentence; the longest the template can write is under it
MAX_WRITES = 3  # times the model is asked for one artist's sentence
TEMPLATE_VERSION = 8  # bump when template() would say something else
WRITTEN_VERSION = 9  # bump when WRITE_PROMPT, PRINT_PROMPT, GLUE or unsupported() changes
SHEET_VERSION = 7  # bump when the sheet, PROPOSE_PROMPT, COUNT_PROMPT, LOOK_PROMPT, KINDS or specifics() changes

# Every value a profile may state, and the spellings it may state it in. The first is the template's.
# "photo" and "unclear" are not here: neither says what the art is made with. Nor is "digital" on its own: with
# the style word "line art" beside it, it would spell a medium the facts do not have.
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
# The kind of output a work is, which is what a studio hires for: value -> what the model that looks at the work
# is told it means (see _looked). A closed list, picked from as the best fit and never ticked yes or no.
_NO_KIND = "none of these"
KINDS = {
    "design": "drawn to show a design, on a plain or simple ground with no scene: one whole figure, creature or "
    "object, or several views, poses, details or variants of it, with or without notes",
    "illustration": "one finished picture with a scene or a worked background: a figure or a creature in a setting, "
    "a moment, a landscape, a city, an interior",
    "portrait": "one head or bust, from the chest or shoulders up, the face the subject",
    "sketch": "a rough or unfinished drawing, or a page of them: loose or construction lines, little or no colour "
    "and shading",
    "comic page": "panels that tell a sequence, with or without speech balloons",
    "map": "a drawn map of a world, a region, a city or a dungeon, seen from above",
    _NO_KIND: "something else, or you cannot tell",
}
# How a kind is said: after the subject the verified tags give the same works, "character designs". A design
# sheet is a design that the look found drawn several times over on its page.
KIND_FORMS = {
    "design": ("designs", "design"),
    "design sheet": ("design sheets", "design sheet"),
    "illustration": ("illustrations", "illustration"),
    "portrait": ("portraits", "portrait"),
    "sketch": ("sketches", "sketch"),
    "comic page": ("comic pages", "comic page"),
    "map": ("maps", "map"),
}
_OWN_NOUN = ("comic page", "map")  # said without a subject, and counted over all the works
# A genre that is a kind of another: a work of the first is one of the second, and a card says one of the two.
_WIDER = {"dark fantasy": "fantasy", "cyberpunk": "science fiction"}


# Words of the artist's own. Each names a kind of a verified subject: (the subjects, one, many, and the other
# spellings of the tag; "dragon" is the tag "dragon" or "dragons" without being told). None names a kind of
# output: what a work is ("character design", "portrait") is for the look at it to say, not for a tag.
# A tag counts on a work only when the verifier gave that work one of those subjects, and the word is stated only
# when that is so for most of the artist's works: their "dragon" under a verified "creature".
# Nothing gets in that is not here, so no tag an artist puts on everything can: "art", "digitalart", "oc",
# "commission", "fantasy", "conceptart". Nor a tag that only contains a word: "dragonage", "catgirl".
# ponytail: a hand list, and what is not on it is never said, however plainly an artist says it. The upgrade is to
# ask the language model whether a tag most works carry names a kind of the verified subject, and to hold its
# answer to the tags themselves.
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
    (("character", "creature"), "demon", "demons", ""),
    (("character", "creature"), "alien", "aliens", ""),
    (("character", "creature"), "anthro", "anthros", "furry"),
    (("character", "creature"), "adoptable", "adoptables", "adopt adopts"),
    (("character", "creature", "vehicle or mech"), "robot", "robots", ""),
    (("environment",), "landscape", "landscapes", ""),
    (("environment",), "cityscape", "cityscapes", ""),
    (("environment",), "castle", "castles", ""),
    (("environment",), "interior", "interiors", ""),
    (("environment",), "architecture", "architecture", ""),
    (("prop or weapon",), "weapon", "weapons", ""),
    (("prop or weapon",), "sword", "swords", ""),
    (("prop or weapon",), "gun", "guns", ""),
    (("character", "prop or weapon"), "armour", "armour", "armor armors armours"),
    (("vehicle or mech",), "mech", "mechs", "mecha mechas"),
    (("vehicle or mech",), "spaceship", "spaceships", "starship starships"),
    (("vehicle or mech",), "tank", "tanks", ""),
    (("vehicle or mech",), "ship", "ships", ""),
)
# many -> (the verified subjects it is a kind of, every spelling of the tag with "_" and "-" taken out)
OWN_WORDS = {
    many: (frozenset(under), frozenset({one.replace(" ", ""), many.replace(" ", ""), *more.split()}))
    for under, one, many, more in _OWN
}
OWN_FORMS = {many: (many, one) for _, one, many, _ in _OWN}
# What a word of the artist's own means, where the word alone could be read loosely: told to the model that looks.
OWN_MEANS = {
    "adoptables": "the picture itself carries words that offer the design for sale or adoption: adoptable, adopt, "
    "auction, open, sold or a price"
}

GENRE_FORMS = WORLD_FORMS | MANNER_FORMS
FORMS = MEDIUM_FORMS | SUBJECT_FORMS | GENRE_FORMS | STYLE_FORMS | KIND_FORMS | OWN_FORMS
# The fields of the facts whose values a profile may state, in the order the vector's text lists them.
STATED = ("recurring", "specialties", "own_words", "mediums", "genres", "formats", "style_words")
# A fact that holds for part of the works may only be said after one of these, in the same clause: the first is
# the template's. "With" is a hedge for a genre ("with fantasy themes") and none for a manner, a kind or a thing.
# A fact that most works have, and not clearly enough to stand flat, may only be said after "mostly", or a
# manner after "mainly": neither is a plain word.
_HEDGES = {
    "worlds": ("with", "partly", "some", "including", "also"),
    "manners": ("partly", "some", "including", "also"),
    "formats": ("including", "partly", "some", "also"),
    "beside": ("some", "partly", "also"),  # a thing of another kind than the profile leads with: see _within
}

# The only words a written profile may use besides the facts. None of them says what the art is, what it is
# made with or how good it is, and the three that say how much of it ("partly", "some", "including") can only
# say less: no "paints", "concept", "illustration", and "mostly" only where the facts have it (see unsupported).
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
- The kind of art is given as it is to be said, word for word: "character designs" is that, not "character art".
- A fact under "most of the works" or "only part of the works" is not true of all the works. Say it only as it \
is listed, after its "mostly", "mainly", "with", "partly", "including" or "and some", and never before the kind \
of art.
- It is printed on the artist's card, so it must read as a person would write it. No word twice. At most \
{MAX_MODIFIERS} style, manner and theme words before the kind of art: say any more after it, "in a painterly \
style". And it names a kind of art, not only what is drawn: "environment art, mostly ruined castles", never just \
"ruined castles". What it is made in comes after the kind of art, never first: "vehicle art in digital line work".
- From "kind of art: environment illustrations / medium: 3D / themes: science fiction / style: saturated" a \
good sentence is "Saturated science fiction environment illustrations in 3D." From "kind of art: creature \
designs / manner: realistic / most of the works: mostly dragons / only part of the works: with horror themes; \
including wolves" a good one is "Realistic creature designs with horror themes, mostly dragons, including wolves."
- Besides the facts, use only these plain words: {" ".join(sorted(GLUE))}.
- So: no numbers. No names of people, studios, games, films or software. No praise or judgement (stunning, \
talented, skilled, detailed, unique). Nothing about experience, career, clients, tools, prices or availability. \
No word of your own for how much or how often (mainly, mostly, often, sometimes, only, varied). No "concept", \
"illustration", "design", "painting" or "drawing" unless the facts have that word.

Answer with one JSON object with one field, profile."""
_WRITTEN = {"type": "object", "properties": {"profile": {"type": "string"}}, "required": ["profile"]}
# The last question of the print check, for what no rule of unsupported() catches. Its examples are invented.
PRINT_PROMPT = """You proofread one line for the card of an artist in a directory that game studios use. The line \
says what kind of art the artist makes, in the directory's clipped style. Whether it is true is not your question, \
and neither is whether it is short, plain or vague: only whether it is a natural, grammatical line that an editor \
would print as it stands. Most lines are.

The directory's style, every one of these fit to print: a kind of art with its style and genre words before it \
and what it is made in after it ("Painterly fantasy character art in traditional media."); what most of it is \
("Environment art, mostly ruined castles."); what part of it is ("Fantasy character art, including armoured \
warriors.", "Creature art, including dragons and designs.", "Cartoon character art in digital line work, \
partly in an anime style.", "Character art and some vehicles.", "Artwork, including environments."); its themes \
("Character art with science fiction themes."). A kind of work is a kind of art: "Anime character designs, \
mostly adoptables.", "Prop and creature design sheets.", "Environment illustrations in 3D.", "Comic pages." And \
a line may say which kind of work most or part of the art is, which says nothing twice: "Character art, mostly \
designs.", "Creature art, including illustrations.", "Character art, mostly designs, mostly monsters.", \
"Environment art, including sketches and castles."

Not fit to print, for one of these four reasons and no other:
- no noun for the art: "Saturated fantasy open window, including doors."
- nouns piled on nouns for one thing: "Sci-fi concept art character art."
- one thing said twice, in the same word: "Fantasy character art, including fantasy characters."
- a detail that is odd, crude or about a body, or that no studio hires an artist for, put as if it were the \
artist's field: "Character art, including striped flags.", "Fantasy art, including snarling heads.", "Character \
art, mostly long capes.", "Environment art, including calm water and burning fires." A kind of creature, \
character, place, vehicle or object is no such detail: "including robots", "mostly ruined castles" are fit.

Answer with one JSON object with these fields.
problem: which of the four reasons, in a few words, or "" when it is none of them.
fit: yes or no."""
_PRINTED = {
    "type": "object",
    "properties": {"problem": {"type": "string"}, "fit": {"type": "string", "enum": ["yes", "no"]}},
    "required": ["problem", "fit"],
}

# What recurs across an artist's works is proposed by the vision model from one contact sheet of them, and then
# held to each work in a look of its own (LOOK_PROMPT). Nothing is said that the looks do not bear out.
_PHRASE = re.compile(r"[a-z]+(?:[ -][a-z]+){0,2}")  # a recurring thing: one to three plain words
# Words no recurring thing may have: the fixed vocabularies' own (a genre, a manner, a style or a medium is said
# from the verified tags, and a kind of output from the look), and words too general to tell one artist from
# another. So a card cannot say a word twice by way of a phrase: no "fantasy" beside "dark fantasy castles".
_NOT_A_SUBJECT = frozenset(
    word
    for forms in (MEDIUM_FORMS | GENRE_FORMS | STYLE_FORMS | SUBJECT_FORMS | KIND_FORMS).values()
    for form in forms
    for word in form.lower().split()
) | frozenset(
    "figure figures person people creatures environments scene scenes object objects thing things artwork picture "
    "pictures image images drawing drawings painting paintings work works style concept concepts art fan subject "
    "subjects human humans man men woman women girl girls boy boys male female face faces head heads eye eyes hand "
    "hands hair body bodies and or of in with at the a an".split()
)
# Dress or body. A card never describes either, however many pictures show it: a phrase with one of these is refused.
# ponytail: a short hand list. What it misses is still counted as UNSUITABLE and still put to the print check.
_BODY = re.compile(
    r"\b(?:revealing|skimpy|scantily|lingerie|bikinis?|swimsuits?|swimwear|underwear|nudes?|naked|topless|busty|"
    r"breasts?|cleavage|curvy|voluptuous|thighs?|feet|foot|butts?|buttocks|sexy|seductive|sensual|erotic|pin-?ups?|"
    r"fetish|bondage|latex|mouths?|jaws?|maws?|tongues?|drool|saliva|bell(?:y|ies)|outfits?|dress(?:es)?|skirts?|"
    r"capes?|cloaks?|stockings|corsets?|heels|uniforms?|tails?)\b"
)
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
PROPOSE_PROMPT = f"""You look at one sheet of numbered pictures, all by one artist, for a search index of artists \
that game studios use. Each thing you name will be held against every picture at full size, word by word: a \
picture counts only when every word of the thing is plainly true of it.

Name up to {AT_MOST["recurring"]} concrete things that recur across the pictures, the one that the most pictures \
show first. Each is a thing a studio might look for an artist by: a kind of creature, character, place, vehicle \
or object, such as dinosaurs, horses, dragons, armoured knights, robots, ruined castles, city streets or \
spaceships. One to three plain lowercase words, as few as say it: "castles" where not every castle is ruined. \
Each plain to see in several pictures. Not a detail of a picture: no clothing, weather, water, fire, light, sky, \
plant, flag or background. An empty list is a good answer.

Rules
- Only what can be seen, named as what it is. A horse-shaped creature that is not a horse is not "horses".
- Never a name, and nothing that takes knowing one to recognise: no franchise, game, film, series, character, \
person or brand.
- No praise and no mood. No word for a genre, a style or a kind of work: no fantasy, sci-fi, dark, anime, \
cartoon, design, sketch, portrait or concept. No word as general as character, figure, person, creature, scene \
or art.
- Never how a figure is dressed or built.

Answer with one JSON object with one field.
recurring: a list, each entry with
  many: the thing in the plural, one to three plain lowercase words: "armoured knights"
  one: the same in the singular: "armoured knight"
  kind: which it is, one of: {", ".join(SUBJECT_FORMS)}"""
_PROPOSED = {
    "type": "object",
    "properties": {
        "recurring": {
            "type": "array",
            "maxItems": AT_MOST["recurring"],
            "items": {
                "type": "object",
                "properties": {
                    "many": {"type": "string"},
                    "one": {"type": "string"},
                    "kind": {"type": "string", "enum": list(SUBJECT_FORMS)},
                },
                "required": ["many", "one", "kind"],
            },
        },
    },
    "required": ["recurring"],
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

# The look: one call a work, at full size. The model sorts the picture by how it is presented before it is asked
# about anything, as the tagging stage's check does, and then answers for each thing word by word.
LOOK_PROMPT = f"""You look at one picture for a search index of artists that game studios use, and answer with \
one JSON object. A reviewer will hold every answer against the picture: an answer the picture does not plainly \
bear out counts against us.

First sort the picture by how it is presented. This is your own judgement of it.

about: in eight words at most, what it is a picture of and how it is laid out: "one armoured figure on a blank \
ground", "three views of a dragon design", "a city street at night", "a page of rough head sketches".
drawings: one, when the picture is a single drawing; several, when the page holds several separate drawings, \
views, poses or items.
finish: rough, for loose or unfinished lines with little or no colour; finished, for clean and complete work, \
whether line art, flat colour or fully painted.
kind: the one that fits best
{tagging._listed(KINDS)}

Then the things you are asked about, if any. Each is a few words, with one field for each of its words: true \
only if the picture plainly shows the thing and that word is plainly true of it, false when that is doubtful, \
partly so or merely possible. Something like the thing is not the thing: a horse-shaped creature is no horse, a \
lit street at dusk is not "dark", one tower is no "city"."""
_PICKS = {"drawings": ("one", "several"), "finish": ("rough", "finished"), "kind": tuple(KINDS)}
_LOOKED = {"about": {"type": "string"}} | {
    name: {"type": "string", "enum": list(choices)} for name, choices in _PICKS.items()
}

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
    work adds one to its medium and to each of its subjects, genres and style words, and to each of OWN_WORDS
    that one of its tags spells and one of its verified subjects bears out. A work of a genre is one of the
    wider genre too (_WIDER), and a card names one of the two: the narrower when the works are clearly that.
    Its description is never read. The module's docstring says which of those counts make a fact. `hedged`
    lists the facts that hold for part of the works only, and `mostly` those that most works have and not
    CLEAR_PERCENT of them: neither may be stated flat. `tagged` says which works have each value a card can
    state, by their place among the counted works, from 1.
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
    fields = ("medium", "subjects", "genres", "style_words", "own_words")
    counts: dict[str, Counter[str]] = {field: Counter() for field in fields}
    tagged: dict[str, list[int]] = {}
    for place, work in enumerate(counted, start=1):
        result = work["result"]
        tags = {_plain(tag) for tag in work.get("tags", ())}
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
        counts["own_words"].update(
            word for word, (under, spelled) in OWN_WORDS.items() if tags & spelled and under & set(result["subjects"])
        )
    part, clear, nearly_all = (
        max(MIN_SUPPORT, -(-len(counted) * percent // 100))
        for percent in (PART_PERCENT, CLEAR_PERCENT, NEARLY_ALL_PERCENT)
    )
    most = max(MIN_SUPPORT, len(counted) // 2 + 1)
    worlds = _leading(counts["genres"], part, WORLD_FORMS, len(WORLD_FORMS))
    # One of a genre and its wider one: "dark fantasy" where the works are clearly that, and "fantasy" otherwise.
    close = {world for world in worlds if world in _WIDER and counts["genres"][world] >= clear}
    worlds = [w for w in worlds if (w in close if w in _WIDER else w not in {_WIDER[near] for near in close})]
    worlds = worlds[: AT_MOST["worlds"]]
    manners = _leading(counts["genres"], part, MANNER_FORMS, AT_MOST["manners"])
    own = _leading(counts["own_words"], most, OWN_WORDS, AT_MOST["own_words"])
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
        "tagged": dict(sorted(tagged.items())),
        "recurring": [],  # the things the sheet proposes, and
        "formats": [],  # the kinds of output, as far as the looks at the works bear them out: see specifics()
        "specialties": _leading(counts["subjects"], part, SUBJECT_FORMS, AT_MOST["specialties"]),
        "own_words": own,
        "mediums": mediums,
        "genres": worlds + manners,
        "style_words": _leading(styles, 1, STYLE_FORMS, AT_MOST["style_words"]),
        "mostly": [term for term in (*worlds, *manners) if most <= counts["genres"][term] < clear],
        "hedged": [term for term in (*worlds, *manners) if counts["genres"][term] < most],
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


def _need(works: int, percent: int) -> int:
    """How many of so many works a share is, rounded up, and never fewer than MIN_SUPPORT."""
    return max(MIN_SUPPORT, -(-works * percent // 100))


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


def _proposed(client: httpx.Client, sheet: str, pictures: int) -> list[dict[str, str]]:
    """What the model says the sheet shows again and again: not yet believed, each to be held to every work.

    Up to three recurring things, each as many, one and kind. A thing is kept only as plain words that are none
    of the fixed vocabularies' own and not too general to tell artists apart, with no word for how a body is
    dressed or built, and its `one` must be `many` in the singular. Nothing an answer says of a name can be
    checked by code: the prompt forbids one, and the looks that follow are at what can be seen, as it is named.
    """
    for attempt in range(tagging.MAX_ATTEMPTS):
        question = f"The sheet has {pictures} pictures, numbered 1 to {pictures}."
        answer = tagging._chat(client, WRITER, PROPOSE_PROMPT, question, sheet, _PROPOSED, attempt)
        items = answer.get("recurring") if isinstance(answer, dict) else None
        if not isinstance(items, list):
            continue
        kept: dict[str, dict[str, str]] = {}
        for item in items:
            many, one, kind = (item.get(name) if isinstance(item, dict) else None for name in ("many", "one", "kind"))
            if not all(isinstance(value, str) and _PHRASE.fullmatch(value) for value in (many, one)):
                continue
            words, single = str(many).split(), str(one).split()
            alike = len(words) == len(single) and words[:-1] == single[:-1] and words[-1][:3] == single[-1][:3]
            barred = bool(_NOT_A_SUBJECT & {*words, *single}) or bool(_BODY.search(str(many)))
            if alike and kind in SUBJECT_FORMS and not barred:
                kept.setdefault(str(many), {"many": str(many), "one": str(one), "kind": str(kind)})
        return list(kept.values())[: AT_MOST["recurring"]]
    return []


def _counted(client: httpx.Client, sheet: str, pictures: int, claims: dict[str, str]) -> dict[str, int]:
    """In how many pictures of the sheet each claim plainly shows, as it is named: claim -> count.

    Only for the two claims no profile states (_NEVER_STATED): one is about the pictures side by side, and the
    push stage weighs both as they were counted when its limits were set. What a card states is counted work
    by work (_looked).
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
            answer = tagging._chat(client, WRITER, COUNT_PROMPT, question, sheet, shape, attempt)
            lists = [answer.get(name) for name in ("shows", "like_it_only")] if isinstance(answer, dict) else [None]
            if all(
                isinstance(found, list) and all(type(n) is int and 1 <= n <= pictures for n in found) for found in lists
            ):
                counts[claim] = len(set(lists[0] or ()) - set(lists[1] or ()))
                break
    return counts


def _looked(client: httpx.Client, image: str, things: dict[str, str]) -> list[str] | None:
    """What one work bears out, looked at on its own and at full size: its kind of output (of KIND_FORMS), and
    each of `things` (as a card says it -> the same in the singular) of which every word is plainly true of it:
    "dark ruined castles" needs dark, ruined and a castle. None when there is no whole answer in MAX_ATTEMPTS.

    The kind is the model's pick of KINDS, held to what it said of the page before it picked: a sketch must be
    rough, and a design is a design sheet as well when the page holds several drawings.
    ponytail: a comic page and a map rest on the pick alone. Ask for the panels, or for the place names, if a
    critic finds one that is not.
    """
    asked = {many: one.split() for many, one in things.items() if many not in _LOOKED}
    shape = {
        "type": "object",
        "properties": _LOOKED
        | {
            many: {"type": "object", "properties": {word: {"type": "boolean"} for word in words}, "required": words}
            for many, words in asked.items()
        },
        "required": [*_LOOKED, *asked],
    }
    lines = [
        f'- {many}: each word of "{" ".join(words)}"' + (f". It means: {OWN_MEANS[many]}" if many in OWN_MEANS else "")
        for many, words in asked.items()
    ]
    question = "\n".join(["Things:", *lines]) if lines else "No things to ask about."
    for attempt in range(tagging.MAX_ATTEMPTS):
        answer = tagging._chat(client, WRITER, LOOK_PROMPT, question, image, shape, attempt)
        if not isinstance(answer, dict) or any(answer.get(name) not in choices for name, choices in _PICKS.items()):
            continue
        said: dict[str, Any] = {many: answer.get(many) for many in asked}
        if not all(
            isinstance(said[many], dict) and all(isinstance(said[many].get(word), bool) for word in words)
            for many, words in asked.items()
        ):
            continue
        kind = answer["kind"]
        kinds = [] if kind == _NO_KIND or (kind == "sketch" and answer["finish"] != "rough") else [kind]
        kinds += ["design sheet"] if kind == "design" and answer["drawings"] == "several" else []
        return kinds + [many for many, words in asked.items() if all(said[many][word] for word in words)]
    return None


def specifics(
    facts: dict[str, Any],
    proposed: list[dict[str, str]],
    looks: list[list[str] | None],
    pictures: int,
    counts: dict[str, int] | None = None,
) -> dict[str, Any]:
    """The facts as far as the looks at the works bear them out. `looks` has what each counted work bears out
    (see _looked), in the order of `counted`; `counts` is what the sheet was counted for (_NEVER_STATED).

    A work counts for a thing or a kind of output when its look says so, and for a subject, a genre, a manner
    or a style word when its verified tags do (`tagged`). The shares are of `pictures`:
      - the kind of output that most of the works are is what the card is built on: "character designs", with
        the subjects the tags give those works, or "design sheets" where CLEAR_PERCENT of the works are those.
        Two kinds that are each PART_PERCENT of the works lead together. A kind of fewer works is "including"
        that from PART_PERCENT. A kind that goes with a subject counts only on the works of the subjects the
        card names, when it names any; comic pages and maps count on any;
      - each thing the sheet proposed and each word of the artist's own is "mostly" that from most of the works
        and "including" it from PART_PERCENT, and not said below that. Two things at most are said in a clause;
      - one subject alone, of half the works or fewer, is not what the artist makes: round 4's critic refused a
        flat "Environment art." on 4 pictures in 10. It is said as a part: "artwork, including environments";
      - what the tags make flat stays flat only together: a style word, a manner and a genre stand before the
        kind of art when most works have them all and are of that kind as well. One that does not hold with the
        rest goes behind it (a genre "mostly fantasy", a manner "mainly in an anime style") or, a style word,
        is not said. The genre and the manner are held first: a style word gives way to them;
      - a card says "mostly" once: where things are what most works show, a genre that most works have goes
        back behind "with ... themes". It does so beside a genre of part of the works as well: "with science
        fiction themes, mostly fantasy" reads as two minds;
      - a thing of another kind than the card leads with is never "mostly": cathedrals behind the figures of
        character art are "and some cathedrals", however many works show them. A creature is no other kind
        than a character for this: most of an artist's characters may be monsters.
    What was taken away is kept in `sheet`, with the looks and every count.
    """
    part, clear = (_need(pictures, percent) for percent in (PART_PERCENT, CLEAR_PERCENT))
    most = max(MIN_SUPPORT, pictures // 2 + 1)
    shown = {term: set(places) for term, places in facts["tagged"].items()}
    for place, look in enumerate(looks, start=1):
        for term in look or ():
            shown.setdefault(term, set()).add(place)

    def of(term: str) -> set[int]:
        return shown.get(term, set())

    subjects = facts["specialties"]
    alone = [term for term in subjects[: len(subjects) == 1] if 2 * len(of(term)) <= pictures]
    named = set().union(*(of(term) for term in subjects if term not in alone)) or set(range(1, pictures + 1))
    works = {kind: of(kind) if kind in _OWN_NOUN else of(kind) & named for kind in KIND_FORMS}
    size = {kind: len(places) for kind, places in works.items()}
    ranked = sorted((kind for kind in KINDS if size.get(kind, 0) >= part), key=lambda kind: -size[kind])
    head = ranked[:2] if len(ranked) > 1 else [kind for kind in ranked if size[kind] >= most]
    if head == ["design"] and size["design sheet"] >= clear:
        head = ["design sheet"]
    led = set().union(*(works[kind] for kind in head)) if head else set(named)
    # What stands flat before the kind of art is one statement: most works must bear out all of it at once.
    after = (*facts["hedged"], *facts["mostly"])
    flat = [term for term in (*facts["style_words"], *facts["genres"]) if term not in after]
    apart = []
    for term in sorted(flat, key=lambda term: (term in STYLE_FORMS, -len(led & of(term)))):
        if len(led & of(term)) >= most:
            led &= of(term)
        else:
            apart.append(term)
    things = {item["many"]: item for item in proposed}
    count = {term: len(of(term)) for term in (*things, *facts["own_words"])}
    count |= {kind: size[kind] for kind in ranked if not head}
    said = sorted((term for term in count if count[term] >= part), key=lambda term: -count[term])
    kin = [term for term in ("character", "creature") if {"character", "creature"} & {*subjects} - {*alone}]
    view = {"specialties": [*subjects, *kin], "hedged": alone, "sheet": {"recurring": things}}  # all _within reads
    more = [term for term in said if count[term] >= most and _within(view, term)][: AT_MOST["mostly"]]
    some = [term for term in said if count[term] < most or not _within(view, term)][: AT_MOST["hedged"] - len(alone)]
    recurring = {many: item | {"count": count[many]} for many, item in things.items() if many in (*more, *some)}
    mostly = [*facts["mostly"], *(term for term in apart if term not in STYLE_FORMS)]
    themes = [term for term in mostly if term in WORLD_FORMS and (more or WORLD_FORMS.keys() & {*facts["hedged"]})]
    removed = {
        "recurring": [many for many in things if count[many] < part],
        "own_words": [word for word in facts["own_words"] if count[word] < part],
        "flat": apart,
    }
    sheet = {"pictures": pictures, "looks": looks, "counts": count | {kind: size[kind] for kind in head}}
    sheet |= {"recurring": recurring, "removed": removed}
    sheet |= {"unsuitable": (counts or {}).get(UNSUITABLE, 0), "same_subject": (counts or {}).get(SAME, 0)}
    return facts | {
        "formats": [*head, *(term for term in (*more, *some) if term in KIND_FORMS)],
        "recurring": list(recurring),
        "own_words": [word for word in facts["own_words"] if word in (*more, *some) and word not in things],
        "style_words": [word for word in facts["style_words"] if word not in apart],
        "mostly": [*(term for term in mostly if term not in themes), *more],
        "hedged": [*alone, *themes, *facts["hedged"], *some],
        "sheet": sheet,
    }


def _spellings(term: str) -> tuple[str, ...]:
    """How a fact may be spelled, the template's way first. A value this module does not know is said as it is."""
    return FORMS.get(term) or (f"{term} work", term)


def _forms(facts: dict[str, Any], term: str) -> tuple[str, ...]:
    """_spellings(), and for a recurring thing of these facts what the sheet gave: many, and one."""
    recurring = (facts.get("sheet") or {}).get("recurring", {})
    return (term, recurring[term]["one"]) if term in recurring else _spellings(term)


def _kind(term: str) -> str:
    return "worlds" if term in WORLD_FORMS else "manners" if term in MANNER_FORMS else "formats"


def _within(facts: dict[str, Any], term: str) -> bool:
    """True when a thing is a kind of what the profile leads with, and so may follow "including". Castles are no
    kind of character art: those are said beside it, "and some castles"."""
    recurring = (facts.get("sheet") or {}).get("recurring", {})
    kinds = {recurring[term]["kind"]} if term in recurring else OWN_WORDS.get(term, (frozenset(),))[0]
    led = {subject for subject in facts["specialties"] if subject not in facts["hedged"]}
    # a prop or a vehicle may be a detail of any picture ("including swords"): only these three are told apart
    return not led or not kinds & {"character", "creature", "environment"} or bool(kinds & led)


def _stated(facts: dict[str, Any]) -> dict[str, tuple[str, ...]]:
    """Every fact a profile of these facts may state -> the words one of which must stand before it in its
    clause: a hedge when it holds for part of the works, "mostly" when most have it ("mainly" for a manner, so
    that a card says neither word twice), none when it is flat."""
    terms = (term for field in STATED for term in facts[field] if term != COMMON_MEDIUM)
    return {
        term: _HEDGES[_kind(term) if _within(facts, term) else "beside"]
        if term in facts["hedged"]
        else ("mainly" if term in MANNER_FORMS else "mostly",) * (term in facts["mostly"])
        for term in terms
    }


# Words that make a phrase the name of a kind of art, and not a caption of what is drawn: "character designs"
# stands alone on a card, and "massive stone towers" does not.
_ART = frozenset(
    "art artwork artworks design designs sheet sheets portrait portraits illustration illustrations sketch "
    "sketches render renders painting paintings drawing drawings study studies scenes page pages icons comic "
    "comics concepts tutorial tutorials mascots sprites map maps".split()
) | {forms[1] for forms in SUBJECT_FORMS.values()}


def _and(items: list[str]) -> str:
    return " and ".join(filter(None, [", ".join(items[:-1]), *items[-1:]]))


def _noun(facts: dict[str, Any]) -> tuple[str, list[str]]:
    """The kind of art a card is built on, and what it is made in, to be said after it.

    The kinds of output that lead, after the subjects they are of ("prop and creature designs"); or those
    subjects ("character art"); or "artwork". Comic pages and maps stand without a subject. A medium that is
    itself a kind of art takes the place of that word: "character pixel art", not "character art in pixel art".
    """
    after = (*facts["hedged"], *facts["mostly"])
    subjects = _and([_spellings(term)[0] for term in facts["specialties"] if term not in after])
    kinds = [term for term in facts["formats"] if term not in after]
    media = [_spellings(medium)[0] for medium in facts["mediums"] if medium != COMMON_MEDIUM]
    if kinds:
        said = _and([_spellings(kind)[0] for kind in kinds])
        return said if not subjects or all(kind in _OWN_NOUN for kind in kinds) else f"{subjects} {said}", media
    if subjects and media and media[0].endswith(" art"):
        return f"{subjects} {media[0]}", media[1:]
    return f"{subjects} art" if subjects else "artwork", media


def _themes(facts: dict[str, Any], flat: Iterable[str] = ()) -> str:
    """What is said of the genres that hold for part of the works: "with fantasy themes", after no comma.
    `flat`: genres said there for want of room."""
    worlds = [*flat, *(_spellings(term)[0] for term in facts["hedged"] if term in WORLD_FORMS)]
    return f"with {_and(worlds)} themes" if worlds else ""


def _a(words: str) -> str:
    return f"{'an' if words[0] in 'aeiou' else 'a'} {words}"


def _hedged(facts: dict[str, Any], said: str) -> list[str]:
    """The clauses that say what does not hold for all the works, each begun by the word for its share: what
    most of them are after "mostly"; then, for part of them, a narrower thing than the profile leads with after
    "including", and a thing of another kind after "and some"."""

    def words(term: str) -> str:
        return term if term in facts["recurring"] else _spellings(term)[term in SUBJECT_FORMS]  # "environments"

    most = [words(term) for term in facts["mostly"] if term not in MANNER_FORMS]
    clauses = [f"mostly {_and(most)}"] if most else []
    # "partly anime" where the word "style" is taken: "in a painterly style, partly in an anime style" says it twice
    for share, terms in (("mainly", facts["mostly"]), ("partly", facts["hedged"])):
        for manner in (_spellings(term)[0] for term in terms if term in MANNER_FORMS):
            clauses.append(f"{share} {manner}" if " style" in said else f"{share} in {_a(manner)} style")
    things = {term: words(term) for term in facts["hedged"] if _kind(term) == "formats"}
    for hedge, within in (("including", True), ("and some", False)):
        if shown := [said_as for term, said_as in things.items() if _within(facts, term) is within]:
            clauses.append(f"{hedge} {_and(shown)}")
    return clauses


def template(facts: dict[str, Any]) -> str:
    """One sentence that states every fact these facts allow and nothing else, the same way for every artist.

    It is built on the kind of art (_noun): the kind of output where the works agree on one, the subject where
    they do not. Before it stand the style, manner and genre that hold together with it; after it what it is
    made in, then what most of the works are, then what part of them are, each behind its own word for the
    share. There is one way with commas: between stacked style words, before "mostly", "partly" and
    "including", and none before "with ... themes".
    """
    said = {term: _forms(facts, term)[0] for term in _stated(facts)}
    after = (*facts["hedged"], *facts["mostly"])
    flat = [term for term in (*facts["style_words"], *facts["genres"]) if term not in after]
    styles = [said[word] for word in flat if word in STYLE_FORMS]
    manners = [said[manner] for manner in flat if manner in MANNER_FORMS]
    worlds = [said[world] for world in flat if world in WORLD_FORMS]
    head, media = _noun(facts)
    # MAX_MODIFIERS words before the kind of art. Past that the style words go behind it, then the manner
    # ("in a painterly anime style"), then the genres ("with fantasy themes").
    room, behind = MAX_MODIFIERS, ""
    if len(styles) + len(manners) + len(worlds) > room:
        behind, styles = ", ".join(styles), []
    if len(manners) + len(worlds) > room:
        behind, manners = " ".join(filter(None, [behind, *manners])), []
    themes, worlds = (worlds, []) if len(worlds) > room else ([], worlds)
    text = " ".join(filter(None, [", ".join(styles), *manners, _and(worlds), head]))
    if media := media + ([f"{_a(behind)} style"] if behind else []):
        text += f" in {_and(media)}"
    text = " ".join(filter(None, [text, _themes(facts, themes)]))
    text = ", ".join([text, *_hedged(facts, text)])
    # after the kind of art itself, no comma: not after "with fantasy themes", which would take it for a theme
    text = re.sub(r"^((?:(?! with )[^,])*), and some", r"\1 and some", text)
    return text[0].upper() + text[1:] + "."


_STYLES = "|".join(
    sorted((re.escape(form.lower()) for forms in STYLE_FORMS.values() for form in forms), key=len, reverse=True)
)
# The style, manner and genre words in every spelling, the longest first: what stacks up before the kind of art.
_MODIFIERS = "|".join(
    sorted(
        (re.escape(form.lower()) for forms in (STYLE_FORMS | GENRE_FORMS).values() for form in forms),
        key=len,
        reverse=True,
    )
)
# The small words: a profile may say one as often as it needs, and may not end on one.
_SMALL = frozenset(
    "and or with in of for on as to by a an the including partly mostly also some plus that which who whose their "
    "its is are both alongside across".split()
)
# Words a profile may not end on: one that needs another after it, a style word, a subject in the singular.
_LOOSE = _SMALL | {form.lower() for forms in STYLE_FORMS.values() for form in forms if " " not in form}
_LOOSE |= {forms[0].lower() for forms in SUBJECT_FORMS.values()}
# A word of the fixed lists that is spelled another way -> its first spelling: "sci-fi" is "science fiction".
_ALIAS = {
    form.lower(): forms[0].lower() for forms in FORMS.values() for form in forms[1:] if not re.search(r"\s", form)
}


def _single(word: str) -> str:
    """A word without its plural ending, so that "designs" is "design" again.
    ponytail: five endings and no dictionary. "knives" and "knife" still pass as two words."""
    for ending, to in (("ies", "y"), ("ches", "ch"), ("shes", "sh"), ("ves", "f"), ("s", "")):
        if word.endswith(ending) and len(word) > len(ending) + 2 and not word.endswith("ss"):
            return word[: -len(ending)] + to
    return word


def _tidy(text: str) -> str:
    """A written sentence with the template's commas: none before "with", one between stacked style words."""
    text = re.sub(r"\s*,\s*(with\b)", r" \1", text, flags=re.IGNORECASE)
    return re.sub(rf"\b({_STYLES}) (?=(?:{_STYLES})\b)", r"\1, ", text, flags=re.IGNORECASE)


def unsupported(text: object, facts: dict[str, Any]) -> str | None:
    """Why this text may not be the profile of these facts. None when every word of it is supported.

    It must be one sentence of MAX_PROFILE characters at most. Once every fact it may state is taken out of
    it, in any spelling of FORMS, each word left must be one of GLUE. So anything else is refused, whatever
    it is: a medium, subject, genre, kind or style word the facts do not have, a recurring thing the looks at
    the works did not keep, a number, a name, praise, a claim about experience, clients, software or
    availability. A fact that holds for part of the works must come after a hedge in its own clause ("with",
    "partly", "including"), and one that most works have after "mostly" ("mainly" for a manner), which may
    stand nowhere else. And two facts at least are stated.

    Then what a card may not read like, true or not. No word twice: every word the text says is counted, a fact
    of the fixed lists in its first spelling ("sci-fi" is "science fiction") and a plural as its singular, so
    "fantasy" beside "dark fantasy" is refused, and "designs" beside "adoptable designs". Round 5 counted each
    fact of the lists as one thing under its own name, which let all four of its critic's repeats through.

    ponytail: which facts hold together on the same works is settled before this (specifics), not here: a
    written text may still move a flat word next to another one. It cannot make a hedged fact flat.
    """
    if not isinstance(text, str) or len(text) > MAX_PROFILE:
        return f"it must be a text of {MAX_PROFILE} characters at most"
    if not re.fullmatch(r"[^.!?\n]*\w[^.!?\n]*\.", text.strip()):
        return "it must be one sentence that ends with a full stop"
    whole, stated, used = text.strip().lower()[:-1], _stated(facts), set()
    spellings = [(form.lower(), term) for term in stated for form in _forms(facts, term)]
    rest, words = whole, []  # words: every word the text says, a fact of the fixed lists in its first spelling
    opens = ""  # the fact the text begins with
    for form, term in sorted(spellings, key=lambda pair: -len(pair[0])):  # the longest first: "dark fantasy"
        said = re.compile(rf"(?<![\w-]){re.escape(form)}(?![\w-])")
        for found in said.finditer(rest):
            start = max(whole.rfind(",", 0, found.start()), whole.rfind(";", 0, found.start())) + 1
            hedge = re.search(rf"\b(?:{'|'.join(stated[term])})\b", whole[start : found.start()])
            if stated[term] and not hedge:
                share = "most of the works, not all" if term in facts["mostly"] else "only part of the works"
                return f'"{form}" is true of {share}: say it after "{stated[term][0]}", never flat'
            if hedge and term in facts["mostly"]:  # no plain word: it may stand only where the facts have it
                at = start + hedge.start()
                rest = rest[:at] + " " * len(hedge.group()) + rest[at + len(hedge.group()) :]
            used.add(term)
            # a thing from the sheet word by word, each in the first spelling of a fact that it spells another way
            spelled = " ".join(_ALIAS.get(word, word) for word in form.split()) if term in facts["recurring"] else ""
            words += (spelled or _forms(facts, term)[0].lower()).split()
            opens = term if found.start() == 0 else opens
        rest = said.sub(lambda found: " " * len(found.group()), rest)  # the same length: places stay where they were
    for word in re.findall(r"[^\s,;]+", rest):
        if word not in GLUE:
            return f'"{word}" is not one of the facts and not one of the plain words allowed'
        words += [word] if word not in _SMALL else []
    used |= {subject for subject in facts["specialties"] if used & {*_OWN_NOUN}}  # "comic pages" has no other word
    if len(used) < min(2, len(stated)):
        return "it must state at least two of the facts"
    last = re.findall(r"[a-z0-9-]+", whole)[-1]
    if whole.rstrip().endswith((",", ";")) or last in _LOOSE:
        return f'it must be a whole phrase, and it breaks off at "{last}": end on what the art is'
    # The print check, the part of it that is mechanical: what a card may not read like, true or not.
    said_once = Counter(map(_single, words))
    if twice := [word for word, times in said_once.items() if times > 1 and word not in _SMALL]:
        return f'"{twice[0]}" is said twice: say each thing once'
    if opens in MEDIUM_FORMS:
        return 'what it is made in comes after the kind of art, as in "vehicle art in 3D"'
    if not _ART & set(re.findall(r"[a-z]+", whole)):
        return 'it names what is drawn and no kind of art: say the kind of art first, as in "creature art, mostly ..."'
    stack = re.match(rf"(?:(?:{_MODIFIERS})(?:,| and)? )*", whole)
    if stack and len(re.findall(_MODIFIERS, stack.group())) > MAX_MODIFIERS:
        return (
            f"more than {MAX_MODIFIERS} words are stacked before the kind of art: say the style words after it, "
            'as in "character art in a painterly style"'
        )
    return None


def _question(facts: dict[str, Any]) -> str:
    """All the model is told about an artist: the facts a profile may state, as the template would spell them."""
    stated = _stated(facts)
    head, media = _noun(facts)
    clauses = _hedged(facts, head)

    def flat(terms: Iterable[str]) -> str:
        return ", ".join(_spellings(term)[0] for term in terms if term in stated and not stated[term])

    said = {
        "kind of art": head,
        "medium": ", ".join(media),
        "manner": flat(term for term in facts["genres"] if term in MANNER_FORMS),
        "themes": flat(term for term in facts["genres"] if term in WORLD_FORMS),
        "style": flat(facts["style_words"]),
        "most of the works": "; ".join(clause for clause in clauses if clause.startswith(("mostly", "mainly"))),
        "only part of the works": "; ".join(
            filter(
                None, [_themes(facts), *(clause for clause in clauses if clause.startswith(("partly", "inc", "and")))]
            )
        ),
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
        text = _tidy(text) if text else text
        refused = unsupported(text, facts)
        if refused is None:
            return text
    return None


def _less(facts: dict[str, Any]) -> dict[str, Any] | None:
    """The facts without the specific thing the fewest works show: what the print check takes away, recorded
    in `sheet`. None when there is none to take. A subject or a genre never goes: those are words of the fixed
    lists, which the template is known to say well, and what reads oddly is a thing from the sheet, a word of
    the artist's or a kind of output beside them (a trial dropped "science fiction" and kept "including flags")."""
    sheet = facts["sheet"]
    claims = [(field, term) for field in ("recurring", "own_words", "formats") for term in facts[field]]
    if not claims:
        return None
    field, gone = min(claims, key=lambda claim: sheet["counts"].get(claim[1], 0))
    removed = sheet["removed"] | {"print_check": [*sheet["removed"].get("print_check", []), gone]}
    kept = {name: [term for term in facts[name] if term != gone] for name in (field, "hedged", "mostly")}
    return facts | kept | {"sheet": sheet | {"removed": removed}}


def _printable(facts: dict[str, Any]) -> dict[str, Any]:
    """The facts, less whatever keeps the template from being a line fit to print by the rules of unsupported()."""
    while unsupported(template(facts), facts) and (fewer := _less(facts)):
        facts = fewer
    return facts


def _fits(client: httpx.Client, text: str) -> bool:
    """The model's answer to PRINT_PROMPT: is this a natural, grammatical line to print on an artist's card.
    No whole answer in MAX_ATTEMPTS is no objection: the rules of unsupported() have passed it already."""
    for attempt in range(tagging.MAX_ATTEMPTS):
        answer = tagging._chat(client, WRITER, PRINT_PROMPT, text, None, _PRINTED, attempt)
        if isinstance(answer, dict) and answer.get("fit") in ("yes", "no"):
            return answer["fit"] == "yes"
    return True


def _printed(client: httpx.Client, facts: dict[str, Any]) -> tuple[dict[str, Any], str | None, bool]:
    """The print check: the facts as they can be printed, the model's sentence for them (None when the template
    stands in), and whether the model's sentence was refused and the template is what is printed.

    The rules first (_printable). Then the question, of the text that would be shown. On "no" to the model's
    sentence the template is asked about, and stands when only the wording was at fault. Otherwise the specific
    thing the fewest pictures show goes, the text is made again and asked about once more. On a "no" to that as
    well the template stands, and of the facts without the next such thing: a line refused twice is not printed
    as it was, and nothing is asked a fourth time.
    """
    facts = _printable(facts)
    composed = _written(client, facts)
    if _fits(client, composed or template(facts)):
        return facts, composed, False
    if composed and _fits(client, template(facts)):
        return facts, None, True
    if fewer := _less(facts):
        facts = _printable(fewer)
        composed = _written(client, facts)
        if _fits(client, composed or template(facts)):
            return facts, composed, False
    return _printable(_less(facts) or facts), None, True


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
    out: the medium everybody has, and a subject that a kind of output is said without ("comic pages").
    """
    names = {"recurring": "Recurring subjects", "specialties": "Specialties", "mediums": "Mediums"}
    names |= {"own_words": "In the artist's words"}
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
    tagged yet; a row they had stays as it is until they are), left_for_next_run,
    removed (rows of artists the store no longer lists, deleted), open_for_work and written_fallback (of the
    profiled), and seconds_per_artist. Each row is committed as it is made. ValueError before anything is sent
    if the address is not loopback; httpx.HTTPError when Ollama is in trouble, with what was stored kept.
    """
    now = now or datetime.now(UTC)
    counts = dict.fromkeys(("profiled", *(f"excluded_{reason}" for reason in REASONS)), 0)
    counts |= dict.fromkeys(("unchanged", "waiting_for_tags", "left_for_next_run", "removed"), 0)
    counts |= {"open_for_work": 0, "written_fallback": 0}
    counts |= dict.fromkeys(
        (
            "with_output",  # the card is built on a kind of output
            "with_recurring",
            "phrases_removed",  # what the sheet proposed and the looks at the works did not bear out
            "own_words_removed",
            "flat_removed",  # a style word, manner or genre that did not hold together with the rest
            "print_changed",  # the print check took a claim away, by a rule or on the model's "no"
            "print_rejected",  # the model said "no" twice: the template is what is printed
        ),
        0,
    )
    versions = [TEMPLATE_VERSION, WRITTEN_VERSION, SHEET_VERSION, WRITER, EMBEDDER]
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
            # The sheet is of the works the facts were counted over, and of no other.
            files = {work_id: name for work_id, name, _, _ in collected[key]}
            sheet, pictures = _sheet(files[work_id] for work_id in facts["counted"])
            if sheet is not None and pictures >= MIN_WORKS:
                proposed = _proposed(client, sheet, pictures)
                things = {item["many"]: item["one"] for item in proposed} | {
                    word: OWN_FORMS[word][1] for word in facts["own_words"]
                }
                images = [tagging._image(files[work_id]) for work_id in facts["counted"]]
                looks = [_looked(client, image, things) if image else None for image in images]
                facts = specifics(facts, proposed, looks, pictures, _counted(client, sheet, pictures, _NEVER_STATED))
            # No sheet, or nothing kept: a style word or a medium of the tags alone tells no one what the artist makes.
            if "sheet" not in facts or not any(
                facts[field] for field in ("specialties", "recurring", "own_words", "formats", "genres")
            ):
                counts["excluded_nothing_clear"] += store_row(
                    key, basis, "nothing_clear", json.dumps(facts), "[]", *[None] * 6
                )
                continue
            facts, composed, rejected = _printed(client, facts)
            built, is_open = template(facts), _open_sentence(evidence)
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
                after = (*facts["hedged"], *facts["mostly"])
                counts["with_output"] += any(kind not in after for kind in facts["formats"])
                counts["with_recurring"] += bool(facts["recurring"])
                counts["print_changed"] += "print_check" in facts["sheet"]["removed"]
                counts["print_rejected"] += rejected
                for field, name in (("recurring", "phrases"), ("own_words", "own_words"), ("flat", "flat")):
                    counts[f"{name}_removed"] += len(facts["sheet"]["removed"][field])
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
    is the template again), the search filters mediums and specialties, the leading genres, formats (the kinds
    of output, of KIND_FORMS), style_words and own_words (the artist's own), hedged (which of those hold for part
    of the works only) and mostly (which most works have, and not clearly enough to be said flat),
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
                    if word in OWN_WORDS  # a row made before a word left the list still has it, until it is remade
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
