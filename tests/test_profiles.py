"""Tests for pipeline.profiles.

Ollama and Workers AI are fakes behind httpx.MockTransport, every image is a few invented bytes, and the tags are
written straight into the test's own database: no model, no GPU, no network. Opening a real connection fails the
test. Every handle, title, tag, tagline, address and credential below is invented.
"""

import base64
import io
import json
import logging
import math
import random
import re
import socket
import sqlite3
from contextlib import closing
from datetime import UTC, datetime, timedelta

import httpx
import pytest
from PIL import Image

from pipeline import config, library, profiles, store, tagging

ALPHA, BRAVO, CHARLIE = "zz-fake-alpha", "zz-fake-bravo", "zz-fake-charlie"
NOW = datetime(2026, 10, 1, tzinfo=UTC)
RECENT, OLD = NOW - timedelta(days=10), NOW - timedelta(days=profiles.RECENT_DAYS + 1)
PNG = b"\x89PNG\r\n\x1a\n"  # how each invented picture begins: a small square of one colour, which says which it is
VERIFIED = tagging.result_set(profiles.TAGGER, profiles.TAGGER)
ACCOUNT, TOKEN = "FAKE-ACCOUNT-0123456789abcdef", "FAKE-TOKEN-zyxwvutsrqponmlk"
SECRETS = {"CLOUDFLARE_ACCOUNT_ID": ACCOUNT, "CLOUDFLARE_API_TOKEN": TOKEN, "OTHER_SECRET": "FAKE-OTHER-SECRET"}
COUNTS = re.compile(
    r'\{"profiled": \d+, "excluded_mostly_ai_declared": \d+, "excluded_mostly_explicit": \d+, '
    r'"excluded_too_few_works": \d+, "excluded_nothing_clear": \d+, "unchanged": \d+, "waiting_for_tags": \d+, '
    r'"left_for_next_run": \d+, "removed": \d+, "open_for_work": \d+, "written_fallback": \d+, '
    r'"with_output": \d+, "with_recurring": \d+, "phrases_removed": \d+, '
    r'"own_words_removed": \d+, "flat_removed": \d+, '
    r'"print_changed": \d+, "print_rejected": \d+, '
    r'"seconds_per_artist": \d+\.\d+\}\n'
)


def unsynced(*args, connect=sqlite3.connect, **kwargs):
    """sqlite3.connect that does not wait for the disk after each commit. Nothing else about the database changes."""
    conn = connect(*args, **kwargs)
    conn.execute("PRAGMA synchronous = OFF")
    return conn


@pytest.fixture(autouse=True)
def cache(tmp_path, monkeypatch):
    monkeypatch.setattr(config, "CACHE_DIR", tmp_path / "cache")
    monkeypatch.setattr(config, "LOCAL_DB", tmp_path / "cache" / "pipeline.sqlite")
    monkeypatch.setattr(config, "load_env", lambda: dict(SECRETS))  # the real .env is never read here
    monkeypatch.setattr(sqlite3, "connect", unsynced)
    monkeypatch.setattr(tagging.time, "sleep", lambda seconds: None)  # the tagging stage's waits between tries

    def no_network(*args, **kwargs):
        raise AssertionError("a real connection was attempted")

    monkeypatch.setattr(socket.socket, "connect", no_network)
    monkeypatch.setattr(socket, "create_connection", no_network)


def tags(medium="digital painting", subjects=("character",), genres=("fantasy",), style_words=("painterly",), **flags):
    """One work's verified result. Its description names things no tag has: a profile must never pick them up."""
    return {
        "medium": medium,
        "subjects": list(subjects),
        "genres": list(genres),
        "style_words": list(style_words),
        "description": "FAKE-DESCRIPTION of a cyberpunk vehicle in pixel art.",
        "explicit": False,
        "not_artwork": False,
    } | flags


def artist(handle, results, *, tagline=None, own_tags=None, declared=None, published=None, listed=True, contact="no"):
    """Collect one invented artist with a work for each entry of `results`, and return the works' ids.

    An entry is the work's verified result, or None for a work that is collected and not tagged yet.
    own_tags, declared and published are by the work's place in the list; a work not named there has the tag
    "faketag", declares no AI and was published at RECENT. contact: what the tagging stage found when it read
    the tagline for a private contact, or None for a tagline it has not read.
    """
    base = {ALPHA: 1000, BRAVO: 2000, CHARLIE: 3000}[handle]
    ids = [f"00000000-0000-4000-A000-{base + n:012d}" for n in range(len(results))]
    for n, work_id in enumerate(ids):
        library.save_image(work_id, "image/png", picture(n))
    rows = [
        {
            "work_id": work_id,
            "page_url": f"https://www.deviantart.com/{handle}/art/FAKE-TITLE-{n}",
            "title": f"FAKE-TITLE-{n}",
            "tags": (own_tags or {}).get(n, ["faketag"]),
            "thumb_url": "https://images-wixmp-zzfake.wixmp.com/f/FAKE-THUMB.jpg",
            "published": str(int((published or {}).get(n, RECENT).timestamp())),
        }
        for n, work_id in enumerate(ids)
    ]
    if listed:
        assert store.upsert_artist(handle, f"https://www.deviantart.com/{handle}", [], 1)
    library.store_artist(
        f"da:{handle}", user_id=None, tagline=tagline, specialty=None, level="Professional", website=None, works=rows
    )
    for n, (work_id, result) in enumerate(zip(ids, results, strict=True)):
        if result is not None:
            tag(work_id, result, (declared or {}).get(n, "no"))
    if contact:
        read_tagline(handle, contact)
    return ids


def read_tagline(handle, contact="no"):
    """Store what the tagging stage finds when it reads the artist's tagline, as it is now, for a private contact."""
    tagging.results()  # so that its tables exist
    with closing(sqlite3.connect(config.LOCAL_DB)) as conn, conn:
        (tagline,) = conn.execute("SELECT tagline FROM profiles WHERE artist_key = ?", (f"da:{handle}",)).fetchone()
        row = (f"da:{handle}", profiles.TAGGER, tagging.CONTACT_PROMPT_VERSION, contact, "2026-01-01T00:00:00+00:00")
        row = (*row, tagging.tagline_print(tagline))
        conn.execute("INSERT OR REPLACE INTO artist_contact VALUES (?, ?, ?, ?, ?, ?)", row)


def picture(n):
    """A real PNG, 40 pixels square, whose colour says it is the artist's work number n."""
    held = io.BytesIO()
    Image.new("RGB", (40, 40), (10 + 20 * n, 60, 200)).save(held, "PNG")
    return held.getvalue()


def works_on(sheet):
    """The numbers of the works on a contact sheet (base64), in the order of the sheet, read from their colours."""
    image = Image.open(io.BytesIO(base64.b64decode(sheet))).convert("RGB")
    found = []
    for cell in range(image.width // profiles.CELL * (image.height // (profiles.CELL + profiles.LABEL))):
        left = cell % profiles.PER_ROW * profiles.CELL + 24
        red, green, blue = image.getpixel(
            (left, cell // profiles.PER_ROW * (profiles.CELL + profiles.LABEL) + profiles.LABEL + 20)
        )
        if abs(green - 60) < 30 and abs(blue - 200) < 30:  # not the white of an empty place
            found.append(round((red - 10) / 20))
    return found


def tag(work_id, result, declared="no", result_set=VERIFIED, words_only=False):
    """Store a work's tags as the tagging stage would, under one result set, and what its own words declare.

    A result of None is the row of a work that could not be analyzed. words_only: the declaration and no tags.
    Each row is read from the work as it is now: its fingerprint is that of the image in the cache, or of the
    title and tags.
    """
    tagging.results()  # so that its tables exist
    at = "2026-01-01T00:00:00+00:00"
    with closing(sqlite3.connect(config.LOCAL_DB)) as conn, conn:
        image = conn.execute("SELECT fingerprint FROM work_images WHERE deviation_id = ?", (work_id,)).fetchone()
        words = conn.execute("SELECT title, tags FROM works WHERE deviation_id = ?", (work_id,)).fetchone()
        seen, read = image and image[0], tagging.words_print(*words)
        row = (work_id, result_set, tagging.PROMPT_VERSION, json.dumps(result) if result else None, at, seen)
        if not words_only:
            conn.execute("INSERT OR REPLACE INTO work_tags VALUES (?, ?, ?, ?, ?, ?)", row)
        # A format is in the result as tagging.results() gives it. Where the tagging stage keeps formats in a
        # table of their own, it is read from there, so it goes there too.
        if (
            result
            and "formats" in result
            and conn.execute("SELECT 1 FROM sqlite_master WHERE name = 'work_format'").fetchone()
        ):
            sorted_as = json.dumps({"formats": result["formats"], "rating": result.get("rating", "safe")})
            row = (work_id, profiles.TAGGER, getattr(tagging, "FORMAT_VERSION", 1), sorted_as, at, seen)
            conn.execute("INSERT OR REPLACE INTO work_format VALUES (?, ?, ?, ?, ?, ?)", row)
        if declared:
            row = (work_id, profiles.TAGGER, tagging.AI_PROMPT_VERSION, declared, at, read)
            conn.execute("INSERT OR REPLACE INTO work_ai_declared VALUES (?, ?, ?, ?, ?, ?)", row)


def vector(text, scale=3.0):
    """1,024 invented numbers that depend on the text, and are not of unit length."""
    numbers = random.Random(text)
    return [scale * numbers.uniform(-1, 1) for _ in range(profiles.DIMENSIONS)]


def sentence_for(question):
    """A sentence that states the facts a question to the writer lists, each as it may be stated, and nothing else."""
    lines = dict(line.split(": ", 1) for line in question.split("\n\n")[0].splitlines()[1:])
    after = [
        clause for name in ("most of the works", "only part of the works") for clause in lines.pop(name, "").split("; ")
    ]
    noun, medium = lines.pop("kind of art"), lines.pop("medium", "")
    flat = [term for terms in lines.values() for term in terms.split(", ")]
    return ", ".join(filter(None, [" ".join([*flat, noun]) + (f" in {medium}" if medium else ""), *after])) + "."


def EVERY(claim, pictures):  # noqa: N802  what the fake counts unasked: every picture, but for the two never stated
    return pictures * (claim not in profiles._NEVER_STATED)


PLAINLY = {"about": "FAKE-LOOK", "drawings": "one", "finish": "finished", "kind": "none of these"}


class Fake:
    """Ollama's vision model, writer (/api/chat) and embedder (/api/embed), and Workers AI, told apart by host,
    path and system prompt.

    write: what the writer says. None for a sentence that states the facts it was given and nothing else, a str
           for that sentence every time, or a function (question, attempt from 1) -> a str for the sentence, a
           dict for the model's whole JSON, a Response, or an exception to raise.
    propose: what the vision model says recurs on a sheet: a list of (many, one, kind), a function (the numbers
           of the works on the sheet) -> such a list, or anything else to send as its whole answer.
    count: in how many works it finds a thing when it looks at each, or in how many pictures of the sheet a claim
           that is counted there: {claim: how many, from the first, or which places}; a claim not named there it
           finds in every one. For a thing also {word of it: how many}, to answer word by word. Or a function
           (claim, pictures) -> the same, or a whole answer to send.
    look: how it sorts a work it looks at: the fields of PLAINLY that differ, for every work; or a function (the
           work's place among those on the sheet, from 1) -> those fields, or a whole answer to send.
    embed: a function (text) -> the vector Ollama gives. cloud: the same for Workers AI, or a Response.
    fit: what it answers when asked whether a line is fit to print: a function (the line) -> "yes", "no" or a
           whole answer to send. Unasked, every line is.
    """

    def __init__(self, write=None, embed=vector, cloud=vector, propose=(), count=None, look=None, fit=None):
        self.write, self.embed, self.cloud, self.propose, self.count = write, embed, cloud, propose, count
        self.look, self.looks = look, []  # looks: each call about one work: its place, the question, the things
        self.fit, self.prints = fit, []  # prints: each line it was asked about
        self.requests, self.chats, self.embeds, self.clouds = [], [], [], []
        self.sheets = []  # each call about a sheet: which prompt, the works on it, the question, the claims

    def __call__(self, request):
        self.requests.append(request)
        if request.url.host != "127.0.0.1":
            self.clouds.append(request)
            if isinstance(self.cloud, httpx.Response | Exception):
                return self.say(self.cloud)
            texts = json.loads(request.content)["text"]
            data = [self.cloud(text) for text in texts]
            return httpx.Response(200, json={"result": {"shape": [len(data), 1024], "data": data}, "success": True})
        body = json.loads(request.content)
        if request.url.path == "/api/embed":
            self.embeds.append(body)
            return httpx.Response(200, json={"embeddings": [self.embed(text) for text in body["input"]]})
        assert request.url.path == "/api/chat"
        prompt, question = body["messages"][0]["content"], body["messages"][-1]["content"]
        if prompt == profiles.LOOK_PROMPT:
            (image,) = body["messages"][-1]["images"]
            red = Image.open(io.BytesIO(base64.b64decode(image))).convert("RGB").getpixel((5, 5))[0]
            on_sheet = self.sheets[-1]["works"]  # the sheet comes first, and the works are looked at in its order
            work = round((red - 10) / 20)
            place = on_sheet.index(work) + 1
            things = [name for name in body["format"]["properties"] if name not in PLAINLY]
            self.looks.append({"place": place, "work": work, "question": question, "things": things, "body": body})
            sorted_as = self.look(place) if callable(self.look) else self.look or {}
            if not isinstance(sorted_as, dict) or not sorted_as.keys() <= PLAINLY.keys():
                return self.say(sorted_as)
            answer = PLAINLY | sorted_as
            for thing in things:
                usual = len(on_sheet)
                found = self.count(thing, usual) if callable(self.count) else (self.count or {}).get(thing, usual)
                words = list(body["format"]["properties"][thing]["properties"])
                if isinstance(found, dict) and list(found) == words:  # word by word: how many works, or an answer
                    answer |= {thing: {w: place <= n if type(n) is int else n for w, n in found.items()}}
                    continue
                if not isinstance(found, int | list) or isinstance(found, bool):
                    return self.say(found)
                answer |= {thing: dict.fromkeys(words, place <= found if isinstance(found, int) else place in found)}
            return self.say(answer)
        if prompt in (profiles.PROPOSE_PROMPT, profiles.COUNT_PROMPT):
            (sheet,) = body["messages"][-1]["images"]
            works = works_on(sheet)
            claims = [line.removeprefix("Claim: ") for line in question.splitlines() if line.startswith("Claim: ")]
            asked = {"prompt": prompt, "works": works, "question": question, "claims": claims, "body": body}
            self.sheets.append(asked)
            if prompt == profiles.PROPOSE_PROMPT:
                said = self.propose(works) if callable(self.propose) else self.propose
                if isinstance(said, list | tuple):
                    said = {"recurring": [dict(zip(("many", "one", "kind"), item, strict=True)) for item in said]}
                return self.say(said)
            (claim,) = claims  # one call a claim
            usual = EVERY(claim, len(works))  # no picture shows either of the two no profile states
            found = self.count(claim, len(works)) if callable(self.count) else (self.count or {}).get(claim, usual)
            if not isinstance(found, int | list) or isinstance(found, bool):
                return self.say(found)
            shows = list(range(1, found + 1)) if isinstance(found, int) else found
            return self.say({"closest": "FAKE-LOOK", "like_it_only": [], "shows": shows})
        if prompt == profiles.PRINT_PROMPT:
            self.prints.append(question)
            answer = self.fit(question) if self.fit else "yes"
            return self.say({"problem": "", "fit": answer} if isinstance(answer, str) else answer)
        self.chats.append(body)
        attempt = sum(
            chat["messages"][-1]["content"].split("\n\n")[0] == question.split("\n\n")[0] for chat in self.chats
        )
        answer = self.write(question, attempt) if callable(self.write) else self.write
        if answer is None:
            answer = sentence_for(question)
        return self.say({"profile": answer} if isinstance(answer, str) else answer)

    @staticmethod
    def say(answer):
        if isinstance(answer, Exception):
            raise answer
        if isinstance(answer, httpx.Response):
            return answer
        return httpx.Response(200, json={"message": {"role": "assistant", "content": json.dumps(answer)}})


def run(fake, max_artists=500, **kwargs):
    """Run the stage against the fake. The counts come back without the throughput, which is a real duration."""
    counts = profiles.run(max_artists, transport=httpx.MockTransport(fake), now=NOW, **kwargs)
    assert isinstance(counts.pop("seconds_per_artist"), float)
    return {name: count for name, count in counts.items() if count}


def table():
    """Every row of this stage's table, read straight from the file."""
    profiles.profiles()  # so that the table exists
    with closing(sqlite3.connect(config.LOCAL_DB)) as conn:
        return conn.execute("SELECT * FROM artist_profiles ORDER BY 1").fetchall()


def facts(
    specialties=(), mediums=(), genres=(), style_words=(), own_words=(), formats=(), hedged=(), recurring=(), mostly=()
):
    """The part of an artist's facts that a profile is written from. hedged: those that hold for part of the
    works. recurring: (many, one, kind) for each thing the looks at the works kept. mostly: those that most works
    have, besides every thing and every word of the artist's own that is not hedged: neither is ever said flat."""
    kept = {many: {"many": many, "one": one, "kind": kind, "count": 9} for many, one, kind in recurring}
    return {
        "recurring": list(kept),
        "specialties": list(specialties),
        "mediums": list(mediums),
        "genres": list(genres),
        "style_words": list(style_words),
        "own_words": list(own_words),
        "formats": list(formats),
        "hedged": list(hedged),
        "mostly": [*mostly, *(thing for thing in (*kept, *own_words) if thing not in hedged)],
        "sheet": {"recurring": kept},
    }


PAINTER = facts(["character"], ["traditional"], ["fantasy"], ["painterly"])


# 1. The facts: counts over the eligible works, from the verified tags and from nothing else.


def body(*groups):
    """An artist's works for facts_of(): `count` works alike for each (count, result) or (count, result, own tags)."""
    works = []
    for count, result, *own in groups:
        works += [
            {"id": f"w{len(works) + n}", "result": result, "declared": "no", "tags": [*own, []][0]}
            for n in range(count)
        ]
    return works


def held(found, proposed=(), counts=None):
    """The facts as the looks at the works leave them. Every work shows each thing proposed, the works the artist
    tagged with a word of their own show it, and no work is any kind of output, but for what `counts` says:
    {a thing or a kind: how many works, from the first, or which places}."""
    things = [dict(zip(("many", "one", "kind"), item, strict=True)) for item in proposed]
    shown = {item["many"]: found["eligible"] for item in things}
    shown |= {word: found["counts"]["own_words"][word] for word in found["own_words"]} | (counts or {})
    looks = [
        [term for term, where in shown.items() if (place <= where if isinstance(where, int) else place in where)]
        for place in range(1, found["eligible"] + 1)
    ]
    return profiles.specifics(found, things, looks, found["eligible"])


def said(*groups, proposed=(), counts=None):
    """The template's sentence for such a body of work, once the looks at the works have had their say."""
    found, reason = profiles.facts_of(body(*groups))
    assert reason is None
    found = held(found, proposed, counts)
    assert profiles.unsupported(profiles.template(found), found) is None, profiles.template(found)
    return profiles.template(found)


PLAIN = tags(genres=[], style_words=[])  # a character in digital painting, and nothing else to say of it
DRAGON = tags(subjects=["creature"], genres=[], style_words=[])
SCENERY = tags(subjects=["environment"], genres=[], style_words=[])


def test_the_facts_are_counted_from_the_verified_tags_and_from_nothing_else():
    ids = artist(
        ALPHA,
        [tags()] * 6
        + [tags(subjects=["creature"], genres=["fantasy", "dark fantasy"], style_words=["painterly", "muted"])] * 2
        + [tags("traditional", ["character", "creature"], [], ["sketchy", "moody"])] * 2,
    )
    for work_id in ids:  # a first pass that was never verified says something else of every work: it is not read
        tag(work_id, tags("pixel art", ["environment"], ["horror"], ["chibi"]), None, profiles.TAGGER)

    assert run(Fake()) == {"profiled": 1}
    (profile,) = profiles.profiles()
    assert profile["facts"]["counts"] == {
        "medium": {"digital painting": 8, "traditional": 2},
        "subjects": {"character": 8, "creature": 4},
        "genres": {"fantasy": 8, "dark fantasy": 2},
        "style_words": {"painterly": 8, "moody": 2, "muted": 2, "sketchy": 2},
        "own_words": {},
    }
    # and which works have each value a card can state, by their place among the counted works
    assert profile["facts"]["tagged"] == {
        "character": [1, 2, 3, 4, 5, 6, 9, 10],
        "creature": [7, 8, 9, 10],
        "dark fantasy": [7, 8],
        "digital painting": [1, 2, 3, 4, 5, 6, 7, 8],
        "fantasy": [1, 2, 3, 4, 5, 6, 7, 8],
        "muted": [7, 8],
        "painterly": [1, 2, 3, 4, 5, 6, 7, 8],
        "sketchy": [9, 10],
        "traditional": [9, 10],
    }
    assert (profile["facts"]["works"], profile["facts"]["eligible"]) == (10, 10)
    assert profile["facts"]["counted"] == ids
    assert (profile["specialties"], profile["mediums"], profile["genres"]) == (
        *(["character", "creature"], ["digital painting"], ["fantasy"]),
    )
    assert (profile["style_words"], profile["own_words"], profile["formats"], profile["hedged"]) == ([], [], [], [])
    assert profile["template"] == "Fantasy character and creature art."
    for text in (profile["template"], profile["written"]):  # nothing of a description, nothing below its threshold
        assert not re.search(
            "cyberpunk|vehicle|pixel|traditional|dark|muted|sketchy|moody|horror|chibi|painterly", text
        )


@pytest.mark.parametrize(
    ("eligible", "having", "field", "stated"),
    [
        # a subject is named at 4 works in 10, and never on fewer than 4 works
        (10, 4, "subject", "flat"),
        (10, 3, "subject", "no"),
        (5, 4, "subject", "flat"),
        (5, 3, "subject", "no"),
        (20, 8, "subject", "flat"),
        (20, 7, "subject", "no"),
        # a genre is hedged from 4 in 10, "mostly" from 6 and stated flat from 7 in 10
        (10, 3, "world", "no"),
        (10, 4, "world", "hedged"),
        (10, 5, "world", "hedged"),
        (10, 6, "world", "mostly"),
        (10, 7, "world", "flat"),
        (5, 3, "world", "no"),
        (5, 4, "world", "flat"),
        # a drawing manner is hedged from 4 in 10, "mostly" from 6 and leads from 7
        (10, 3, "manner", "no"),
        (10, 4, "manner", "hedged"),
        (10, 5, "manner", "hedged"),
        (10, 6, "manner", "mostly"),
        (10, 7, "manner", "flat"),
        (7, 3, "manner", "no"),
        (7, 4, "manner", "mostly"),
        (7, 5, "manner", "flat"),
        # a medium, and a colour word the verifier checked, at 7 in 10
        (10, 6, "medium", "no"),
        (10, 7, "medium", "flat"),
        (10, 6, "colour", "no"),
        (10, 7, "colour", "flat"),
        # any other style word at 9 in 10
        (10, 8, "style", "no"),
        (10, 9, "style", "flat"),
        (5, 4, "style", "no"),
        (5, 5, "style", "flat"),
    ],
)
def test_what_share_of_the_works_makes_a_fact_and_what_share_lets_it_be_stated_flat(eligible, having, field, stated):
    value, with_it = {
        "subject": ("creature", tags("unclear", ["creature"], [], [])),
        "world": ("horror", tags("unclear", [], ["horror"], [])),
        "manner": ("realistic", tags("unclear", [], ["realistic"], [])),
        "medium": ("traditional", tags("traditional", [], [], [])),
        "colour": ("muted", tags("unclear", [], [], ["muted"])),
        "style": ("sketchy", tags("unclear", [], [], ["sketchy"])),
    }[field]

    found, _ = profiles.facts_of(body((having, with_it), (eligible - having, tags("unclear", [], [], []))))

    facts_stated = [term for name in profiles.STATED for term in found[name]]
    assert (value in facts_stated) is (stated != "no")
    assert (value in found["hedged"]) is (stated == "hedged")
    assert (value in found["mostly"]) is (stated == "mostly")


def test_a_style_word_outside_the_checkable_list_and_a_medium_that_names_no_material_are_counted_and_never_stated():
    found, reason = profiles.facts_of(
        body((8, tags("photo", style_words=["moody", "detailed"])), (1, tags("unclear", style_words=["stunning"])))
    )

    assert found["counts"]["medium"] == {"photo": 8, "unclear": 1}
    assert found["counts"]["style_words"] == {"detailed": 8, "moody": 8, "stunning": 1}
    assert (found["mediums"], found["style_words"], reason) == ([], [], None)
    assert profiles.template(found) == "Fantasy character art."


def test_the_three_words_for_vivid_colour_are_counted_as_one():
    found, _ = profiles.facts_of(
        body(
            (3, tags(style_words=["saturated"])),
            (2, tags(style_words=["vibrant", "vivid"])),
            (2, tags(style_words=["vivid"])),
        )
    )

    assert found["counts"]["style_words"] == {"saturated": 7}
    assert found["style_words"] == ["saturated"]


# Round 1's critic, item by item: what it called unsupported or too flat, and what it missed.


def test_the_medium_nearly_everyone_has_is_a_fact_to_filter_on_and_is_never_said():
    found, reason = profiles.facts_of(body((10, tags("digital painting", ["character"], [], []))))

    assert (found["mediums"], reason) == (["digital painting"], None)
    assert profiles.template(found) == "Character art."
    assert profiles.unsupported("Character art in digital painting.", found) is not None  # nor by the model
    # and an artist of whom nothing else is known has nothing to say at all
    assert profiles.facts_of(body((10, tags("digital painting", [], [], []))))[1] == "nothing_clear"


@pytest.mark.parametrize(
    ("groups", "sentence"),
    [
        ([(7, tags("traditional", genres=[], style_words=[])), (3, PLAIN)], "Character art in traditional media."),
        ([(6, tags("traditional", genres=[], style_words=[])), (4, PLAIN)], "Character art."),  # no clear majority
        ([(8, tags("3d render", ["environment"], [], [])), (2, PLAIN)], "Environment art in 3D."),
        # a medium that is a kind of art itself takes the place of that word: no "art in pixel art"
        ([(10, tags("pixel art", genres=[], style_words=[]))], "Character pixel art."),
        # line art is said once, as the medium, and by a word the card has not used
        ([(10, tags("digital line art", genres=[], style_words=["lineart"]))], "Character art in digital line work."),
    ],
)
def test_a_medium_is_said_only_when_it_is_distinctive_and_a_clear_majority(groups, sentence):
    assert said(*groups) == sentence


@pytest.mark.parametrize(
    ("groups", "sentence"),
    [
        # five environments and five figures is not "environment art"
        ([(5, SCENERY), (4, PLAIN), (1, DRAGON)], "Environment and character art."),
        ([(6, PLAIN), (4, DRAGON)], "Character and creature art."),
        ([(7, PLAIN), (3, DRAGON)], "Character art."),  # 3 works in 10 do not share the lead
        (
            [(6, PLAIN), (4, tags(subjects=["vehicle or mech"], genres=[], style_words=[]))],
            "Character and vehicle art.",
        ),
        # a body split between two genres names both, and neither flat
        (
            [(5, tags(genres=["fantasy"], style_words=[])), (5, tags(genres=["science fiction"], style_words=[]))],
            "Character art with fantasy and science fiction themes.",
        ),
        # about half is hedged; a clear majority is stated flat, and what part of the rest is comes after it
        ([(6, tags(style_words=[])), (4, PLAIN)], "Character art, mostly fantasy."),
        ([(5, tags(style_words=[])), (5, PLAIN)], "Character art with fantasy themes."),
        # beside a genre of part of the works, the one most works have is a theme as well: not "mostly" after it
        (
            [(6, tags(genres=["fantasy"], style_words=[])), (4, tags(genres=["science fiction"], style_words=[]))],
            "Character art with fantasy and science fiction themes.",
        ),
        ([(7, tags(style_words=[])), (3, PLAIN)], "Fantasy character art."),
        (
            [(6, tags(style_words=[])), (4, tags(genres=["fantasy", "science fiction"], style_words=[]))],
            "Fantasy character art with science fiction themes.",
        ),
        ([(7, tags(style_words=[])), (3, tags(genres=["science fiction"], style_words=[]))], "Fantasy character art."),
    ],
)
def test_a_split_portfolio_is_named_by_both_halves_and_a_genre_of_half_the_works_is_hedged(groups, sentence):
    assert said(*groups) == sentence


@pytest.mark.parametrize(
    ("groups", "sentence"),
    [
        ([(7, tags(genres=["anime style"], style_words=[])), (3, PLAIN)], "Anime character art."),
        ([(6, tags(genres=["anime style"], style_words=[])), (4, PLAIN)], "Character art, mainly in an anime style."),
        ([(10, tags(genres=["fantasy", "realistic"], style_words=[]))], "Realistic fantasy character art."),
        (
            [(9, tags(genres=["stylized cartoon"], style_words=["muted"])), (1, PLAIN)],
            "Muted cartoon character art.",
        ),
        ([(4, tags(genres=["anime style"], style_words=[])), (6, PLAIN)], "Character art, partly in an anime style."),
        (
            [(5, tags(genres=["stylized cartoon"], style_words=[])), (5, PLAIN)],
            "Character art, partly in a cartoon style.",  # never "a stylized cartoon style"
        ),
        ([(2, tags(genres=["realistic"], style_words=[])), (8, PLAIN)], "Character art."),
    ],
)
def test_the_drawing_manner_leads_when_most_works_have_it_and_is_hedged_when_part_of_them_do(groups, sentence):
    assert said(*groups) == sentence


@pytest.mark.parametrize(
    ("groups", "sentence"),
    [
        ([(10, tags(style_words=["muted", "painterly"]))], "Painterly, muted fantasy character art."),
        ([(7, tags(style_words=["muted", "painterly"])), (3, tags(style_words=[]))], "Muted fantasy character art."),
        (
            [(8, tags(style_words=["soft-shaded", "high-contrast"])), (2, tags(style_words=[]))],
            "Fantasy character art.",
        ),
        ([(9, tags(style_words=["soft-shaded"])), (1, tags(style_words=[]))], "Soft-shaded fantasy character art."),
        # two at most, and a word a model made up never
        (
            [(10, tags(style_words=["monochrome", "sketchy", "textured", "moody"]))],
            "Monochrome, sketchy fantasy character art.",
        ),
    ],
)
def test_a_style_word_is_said_only_when_the_verifier_checked_it_or_nearly_all_works_have_it(groups, sentence):
    assert said(*groups) == sentence


@pytest.mark.parametrize(
    ("groups", "sentence"),
    [
        # most of the works carry the tag, and the looks at them agree: "mostly"
        ([(7, DRAGON, ["dragon"]), (3, DRAGON)], "Creature art, mostly dragons."),
        ([(6, DRAGON, ["dragon"]), (4, DRAGON)], "Creature art, mostly dragons."),
        (
            [(2, DRAGON, ["Dragon"]), (2, DRAGON, ["dragons"]), (3, DRAGON, ["DRAGON", "faketag"]), (3, DRAGON)],
            "Creature art, mostly dragons.",
        ),
        ([(5, DRAGON, ["dragon"]), (5, DRAGON)], "Creature art."),  # half the works do not make a word theirs
        ([(4, DRAGON, ["dragon"]), (1, DRAGON)], "Creature art, mostly dragons."),
        # a tag for a kind of output is no word of the artist's own: what a work is, the look at it says
        ([(10, PLAIN, ["characterdesign", "portrait", "warrior"])], "Character art, mostly warriors."),
        ([(10, SCENERY, ["landscape", "environmentdesign", "mattepainting"])], "Environment art, mostly landscapes."),
        (
            [(10, tags(subjects=["vehicle or mech"], genres=[], style_words=[]), ["mecha"])],
            "Vehicle art, mostly mechs.",
        ),
        # three words most works carry: the two that most works carry, in the order of the list when it is a tie
        ([(10, PLAIN, ["elf", "mage", "knight"])], "Character art, mostly knights and elves."),
        ([(6, PLAIN, ["elf", "mage"]), (4, PLAIN, ["mage"])], "Character art, mostly mages and elves."),
        # a word that had the subject's own in it would say that twice
        ([(10, PLAIN, ["furry", "adopts"])], "Character art, mostly anthros and adoptables."),
    ],
)
def test_a_word_of_the_artists_own_is_used_when_most_works_carry_it_and_the_verified_subject_agrees(groups, sentence):
    assert said(*groups) == sentence


@pytest.mark.parametrize(
    "own",
    [
        # what an artist puts on everything
        ["art", "digitalart", "oc", "commission", "conceptart", "fantasy", "illustration", "digitalpainting"],
        ["originalcharacter", "artwork", "drawing", "design", "deviantart", "artist", "character", "creature"],
        ["adoptablesopen", "commissionsopen", "forsale", "sale", "auction", "paypal", "noai", "dailydeviation"],
        # what it takes knowing a franchise to check
        ["fanart", "fanarts", "fan_art", "videogamefanart", "warhammer", "pokemon", "sonicthehedgehog"],
        # a tag that only contains a word of the list, or a name
        ["dragonage", "dragonfly", "snapdragon", "dragonart", "mydragon", "dragonoc", "reddragon", "drag", "on"],
        ["catgirl", "wolfgang", "elfquest", "warriorcats", "knightrider", "tankgirl", "shipping"],
        # a word of the list that is a kind of some other subject than the one the verifier saw
        ["dragon", "monster", "landscape", "castle", "mech", "spaceship", "sword", "weapon", "wolf", "architecture"],
        [],
    ],
)
def test_no_spam_generic_or_disagreeing_tag_of_the_artists_gets_into_a_profile(own):
    found, _ = profiles.facts_of(body((10, PLAIN, own)))

    assert found["own_words"] == [] and found["counts"]["own_words"] == {}
    assert profiles.template(held(found)) == "Character art."


def test_an_own_word_counts_work_by_work_and_only_where_the_verifier_saw_its_subject():
    # every work is tagged "dragon" by the artist; the verifier saw a creature in half of them
    found, _ = profiles.facts_of(body((5, DRAGON, ["dragon"]), (5, PLAIN, ["dragon"])))
    assert (found["counts"]["own_words"], found["own_words"]) == ({"dragons": 5}, [])

    # the works a tag is on must be eligible ones: here most of the dragons are flagged
    flagged = tags(subjects=["creature"], genres=[], style_words=[], explicit=True)
    found, _ = profiles.facts_of(body((4, flagged, ["dragon"]), (3, DRAGON, ["dragon"]), (3, DRAGON)))
    assert (found["eligible"], found["counts"]["own_words"], found["own_words"]) == (6, {"dragons": 3}, [])


def test_every_own_word_is_a_kind_of_a_verified_subject_and_none_can_be_mistaken_for_another_fact():
    tables = (
        *(profiles.MEDIUM_FORMS, profiles.SUBJECT_FORMS, profiles.GENRE_FORMS, profiles.STYLE_FORMS),
        *(profiles.KIND_FORMS, profiles.OWN_FORMS),
    )
    assert sum(map(len, tables)) == len(profiles.FORMS)  # no value is in two tables
    spelled = [tag for _, spellings in profiles.OWN_WORDS.values() for tag in spellings]
    assert len(spelled) == len(set(spelled))  # no tag spells two words
    generic = {
        "art",
        "digitalart",
        "oc",
        "commission",
        "conceptart",
        "fantasy",
        "illustration",
        "character",
        "creature",
    }
    assert not generic & set(spelled)
    said_anyway = {part for forms in (profiles.SUBJECT_FORMS | profiles.KIND_FORMS).values() for part in forms}
    for word, (under, spellings) in profiles.OWN_WORDS.items():
        assert under and under <= set(profiles.SUBJECT_FORMS), word
        assert all(re.fullmatch("[a-z]+", tag) for tag in spellings), word
        # nor does any say a word that a card says anyway, of the subject or of the kind of output
        assert not set(word.split()) & said_anyway, word


CASTLES = tags(subjects=["environment"], genres=[], style_words=[])
NOTHING = tags(subjects=[], genres=[], style_words=[])  # a work the verifier gave no subject


@pytest.mark.parametrize(
    ("groups", "counts", "sentence", "formats"),
    [
        # no look found a kind of output: the card is built on the subject, as before
        ([(10, PLAIN)], {}, "Character art.", []),
        # most works are designs: that is the noun, after the subject the tags give those works
        ([(10, PLAIN)], {"design": 7}, "Character designs.", ["design"]),
        ([(10, PLAIN)], {"design": 6}, "Character designs.", ["design"]),  # most of the works
        ([(10, PLAIN)], {"design": 5}, "Character art, including designs.", ["design"]),  # half is a part
        ([(10, PLAIN)], {"design": 4}, "Character art, including designs.", ["design"]),
        ([(10, PLAIN)], {"design": 3}, "Character art.", []),
        ([(10, DRAGON)], {"illustration": 8}, "Creature illustrations.", ["illustration"]),
        ([(10, PLAIN)], {"portrait": 7}, "Character portraits.", ["portrait"]),
        ([(10, CASTLES)], {"sketch": 9}, "Environment sketches.", ["sketch"]),
        # a design sheet is a design, and leads where the designs are clearly sheets
        ([(10, PLAIN)], {"design": 9, "design sheet": 7}, "Character design sheets.", ["design sheet"]),
        ([(10, PLAIN)], {"design": 9, "design sheet": 6}, "Character designs.", ["design"]),
        # two kinds that are each a part of the works lead together
        (
            [(10, PLAIN)],
            {"design": 6, "illustration": [7, 8, 9, 10]},
            "Character designs and illustrations.",
            ["design", "illustration"],
        ),
        (
            [(10, PLAIN)],
            {"design": [1, 2, 3, 4], "illustration": [5, 6, 7, 8]},
            "Character designs and illustrations.",
            ["design", "illustration"],
        ),
        (
            [(10, PLAIN)],
            {"design": [1, 2, 3, 4], "illustration": [5, 6]},
            "Character art, including designs.",
            ["design"],
        ),
        # the subjects of a split portfolio share the kind
        ([(5, PLAIN), (5, DRAGON)], {"design": 8}, "Character and creature designs.", ["design"]),
        # a kind counts on the works of the subjects the card names: here 3 of the 6 characters, and 4 of nothing
        ([(6, PLAIN), (4, NOTHING)], {"illustration": [1, 2, 3, 7, 8, 9, 10]}, "Character art.", []),
        # comic pages and maps stand without a subject, on any work
        ([(6, PLAIN), (4, NOTHING)], {"comic page": [1, 2, 3, 7, 8, 9, 10]}, "Comic pages.", ["comic page"]),
        ([(10, CASTLES)], {"map": 7}, "Maps.", ["map"]),
        # and where the card names no subject, a kind stands alone on all the works
        ([(4, PLAIN), (6, NOTHING)], {"illustration": 8}, "Illustrations, including characters.", ["illustration"]),
    ],
)
def test_the_kind_of_output_is_the_noun_of_the_card_when_the_works_agree_on_one(groups, counts, sentence, formats):
    found = held(profiles.facts_of(body(*groups))[0], counts=counts)

    assert found["formats"] == formats
    assert said(*groups, counts=counts) == sentence


def test_the_tagging_stages_own_format_pick_is_not_read_any_more():
    """Round 5's "including concept sketches" on four finished prop sheets came from it."""
    sorted_as = {"formats": ["concept sketches or thumbnails", "portrait or bust"], "rating": "safe"}

    found, _ = profiles.facts_of(body((10, PLAIN | sorted_as)))

    assert (found["formats"], "formats" in found["counts"]) == ([], False)
    assert profiles.template(held(found)) == "Character art."


def test_explicit_not_artwork_and_ai_declared_works_never_contribute():
    ids = artist(
        ALPHA,
        [tags()] * 6
        + [tags("3d render", ["creature"], ["horror"], ["low-poly"], explicit=True)] * 2
        + [tags("pixel art", ["environment"], ["cyberpunk"], ["isometric"], not_artwork=True)]
        + [tags("traditional", ["vehicle or mech"], ["science fiction"], ["sketchy"])],
        declared={9: "yes"},
        own_tags=dict.fromkeys(range(6, 10), ["dragon", "landscape", "mech"]),
    )

    assert run(Fake()) == {"profiled": 1}
    (profile,) = profiles.profiles()
    assert profile["facts"]["counts"] == {
        "medium": {"digital painting": 6},
        "subjects": {"character": 6},
        "genres": {"fantasy": 6},
        "style_words": {"painterly": 6},
        "own_words": {},
    }
    found = profile["facts"]
    assert [found[name] for name in ("works", "eligible", "explicit", "not_artwork", "declared_ai")] == [10, 6, 2, 1, 1]
    assert found["counted"] == ids[:6]
    (shown,) = profiles.sample(1, seed=0)
    assert [path[-40:-4] for path in shown["images"]] == ids[:6]
    assert [path[-40:-4] for path in shown["images_left_out"]] == ids[6:]


def test_a_work_the_tagging_stage_rates_explicit_is_left_out_like_one_it_flags():
    rated = tags(subjects=["creature"], genres=["horror"]) | {"formats": [], "rating": "explicit"}
    found, reason = profiles.facts_of(
        body(
            (5, tags() | {"formats": [], "rating": "safe"}),
            (4, rated),
            (1, tags() | {"formats": [], "rating": "suggestive"}),
        )
    )

    assert (found["eligible"], found["explicit"], reason) == (6, 4, None)
    assert found["counts"]["subjects"] == {"character": 6}
    assert profiles.facts_of(body((4, tags()), (6, rated)))[1] == "mostly_explicit"


def test_the_facts_lead_with_what_most_works_have_and_keep_only_so_many_of_a_kind():
    everything = tags(
        subjects=list(profiles.SUBJECT_FORMS)[:4],
        genres=["fantasy", "horror", "cyberpunk", "anime style", "realistic"],
        style_words=["muted", "monochrome", "painterly"],
    )
    more = tags(
        subjects=["environment"], genres=["cyberpunk", "realistic"], style_words=["muted", "monochrome", "painterly"]
    )

    found, _ = profiles.facts_of(body((6, everything), (4, more)))

    assert found["specialties"] == ["environment", "character", "creature"]  # 10 works, then the first of those with 6
    assert found["genres"] == ["cyberpunk", "fantasy", "realistic"]  # two kinds of world and one manner
    assert (found["mostly"], found["hedged"]) == (["fantasy"], [])  # 6 in 10; cyberpunk and realistic have 10
    assert found["style_words"] == ["painterly", "monochrome"]  # all have 10: the order of the list, and two
    assert all(len(found[name]) <= most for name, most in profiles.AT_MOST.items() if name in found)


def test_with_no_work_counted_nothing_is_a_fact():
    found, reason = profiles.facts_of([{"id": "a", "result": tags(not_artwork=True), "declared": "no"}])

    assert [found[name] for name in profiles.STATED] == [[]] * len(profiles.STATED)
    assert (found["eligible"], found["hedged"], reason) == (0, [], "too_few_works")


def test_a_work_whose_declaration_is_unclear_counts_and_one_with_no_result_does_not():
    works = [{"id": str(n), "result": tags(), "declared": "unclear"} for n in range(5)]
    works += [{"id": "failed", "result": None, "declared": "no"}]

    found, reason = profiles.facts_of(works)

    assert (found["works"], found["eligible"], reason) == (6, 5, None)


# 2. Exclusions: no profile, and the reason on record.


@pytest.mark.parametrize(
    ("results", "declared", "reason"),
    [
        ([tags()] * 4, {}, "too_few_works"),
        ([tags()] * 4 + [tags(explicit=True)] * 3 + [tags(not_artwork=True)] * 3, {}, "too_few_works"),
        ([tags()] * 10, dict.fromkeys(range(6), "yes"), "mostly_ai_declared"),
        ([tags()] * 4 + [tags(explicit=True)] * 6, {}, "mostly_explicit"),
        ([tags("unclear", [], [], [])] * 6, {}, "nothing_clear"),
        ([tags("digital painting", [], [], [])] * 6, {}, "nothing_clear"),  # the medium everyone has says nothing
    ],
)
def test_an_artist_who_cannot_be_characterised_gets_no_profile_and_the_reason_is_recorded(results, declared, reason):
    artist(ALPHA, results, declared=declared, tagline="Commissions open")
    fake = Fake()

    assert run(fake) == {f"excluded_{reason}": 1}

    assert profiles.excluded() == {f"da:{ALPHA}": reason}
    assert profiles.profiles() == [] and profiles.sample(5, seed=0) == []
    ((key, _, stored_reason, stored_facts, evidence, *texts_and_vector, _),) = table()
    assert (key, stored_reason, evidence) == (f"da:{ALPHA}", reason, "[]")
    assert texts_and_vector == [None] * 6
    assert json.loads(stored_facts)["works"] == len(results)
    assert fake.requests == []  # no model is asked about them


def test_half_the_works_declared_ai_or_explicit_is_not_most_of_them():
    artist(ALPHA, [tags()] * 5 + [tags(explicit=True)] * 5)
    artist(BRAVO, [tags()] * 10, declared=dict.fromkeys(range(5), "yes"))

    assert run(Fake()) == {"profiled": 2}
    assert [profile["facts"]["eligible"] for profile in profiles.profiles()] == [5, 5]


# 3. Open for work: the artist's own words, or unknown.


@pytest.mark.parametrize(
    ("tagline", "kind"),
    [
        ("Commissions Open", "commissions"),
        ("HUMAN ARTIST | COMMISSIONS: OPEN", "commissions"),
        ("NO AI | COMMISSIONS - OPEN", "commissions"),  # the "no" belongs to another part of the line
        ("Comissions currently OPEN!", "commissions"),
        ("COMMISION OPEN !!!", "commissions"),
        ("COMMISSION ARE OPEN!!", "commissions"),
        ("commisions are now open", "commissions"),
        ("Open for commissions!", "commissions"),
        ("open to commissions", "commissions"),
        ("Taking commissions", "commissions"),
        ("Accepting commissions ✨", "commissions"),
        ("Comms open", "commissions"),
        ("FAKE-STUDIO artist. Commissions open. Trades closed.", "commissions"),
        ("Available for Freelance", "hire"),
        ("Professional artist for hire", "hire"),
        ("2D artist - available for work", "hire"),
    ],
)
def test_a_tagline_that_says_so_in_the_artists_own_words_is_evidence(tagline, kind):
    assert profiles.open_evidence(tagline, [], NOW) == [{"source": "tagline", "says": tagline, "kind": kind}]


@pytest.mark.parametrize(
    "tagline",
    [
        None,
        "",
        "Commissions closed",
        "COMMISSIONS: CLOSED",
        "(COMMISSION CLOSE)",
        "Commissions are closed",
        "Commissions are not open",
        "Not open for commissions",
        "Not taking commissions",
        "Commissions aren't open",
        "Closed for commissions",
        "Commissions open soon",
        "Commissions will open in May",
        "Commissions full",
        "Commissions on hold",
        "Commissions open? No.",
        "Commissions closed | Trades open",
        "Commissions",
        "Commission info in gallery",
        "Send me Notes for commissions",
        "DM Me for Commissions",
        "Freelance Illustrator",
        "Freelance Concept Artist",
        "Professional",
        "Open",
        "Open minded artist",
        "Requests open",
        "Custom Orders - OPEN",
        "Adopts open",
        "Not for hire",
        "Not available for work",
        "Commissioned art and open studio",
        "Opening hours: commissions",
    ],
)
def test_a_tagline_that_does_not_say_so_or_says_the_opposite_is_no_evidence(tagline):
    assert profiles.open_evidence(tagline, [], NOW) == []


@pytest.mark.parametrize(
    ("own_tag", "kind"),
    [
        ("commissionsopen", "commissions"),
        ("commissionopen", "commissions"),
        ("CommissionsOpen", "commissions"),
        ("commissions_open", "commissions"),
        ("comissionsopen", "commissions"),
        ("commisionsopen", "commissions"),
        ("opencommissions", "commissions"),
        ("opencommission", "commissions"),
        ("open-for-commissions", "commissions"),
        ("commissionsareopen", "commissions"),
        ("takingcommissions", "commissions"),
        ("vgencommissionsopen", "commissions"),
        ("artistforhire", "hire"),
        ("availableforwork", "hire"),
    ],
)
def test_an_own_tag_on_a_recent_work_that_says_so_is_evidence(own_tag, kind):
    at = RECENT.isoformat()
    found = profiles.open_evidence(
        None, [("FAKE-OLD", [own_tag], OLD.isoformat()), ("FAKE-ID", ["faketag", own_tag], at)], NOW
    )
    assert found == [{"source": "tag", "says": own_tag, "kind": kind, "work": "FAKE-ID", "published_at": at}]


@pytest.mark.parametrize(
    "own_tag",
    [
        *("commission", "commissions", "commissionart", "commissionedart", "commissioninfo", "commissionprices"),
        *("dndcommission", "ychcommissionopen", "ych_commissions_open", "characterforsale", "forsale", "adoptopen"),
        *("freelance", "freelanceartist", "open", "opensource", "commissionsclosed", "closedcommissions"),
        *("commissions_closed", "nocommissions", "community", "tcommissionsopen", "commissionoepen"),
    ],
)
def test_an_own_tag_that_does_not_say_so_is_no_evidence(own_tag):
    assert profiles.open_evidence(None, [("FAKE-ID", [own_tag], RECENT.isoformat())], NOW) == []


def test_a_tag_on_an_old_or_undated_work_says_nothing_about_now():
    for published in (OLD.isoformat(), None):
        assert profiles.open_evidence(None, [("FAKE-ID", ["commissionsopen"], published)], NOW) == []
        assert profiles.open_evidence("Commissions open", [("FAKE-ID", ["commissionsclosed"], published)], NOW) != []


@pytest.mark.parametrize(
    ("tagline", "own_tags"),
    [
        ("Commissions open", ["commissionsclosed"]),
        ("Commissions closed", ["commissionsopen"]),
        (None, ["commissionsopen", "commissions_closed"]),
        ("Available for hire | commissions closed", []),
    ],
)
def test_anything_that_says_closed_leaves_it_unknown_whatever_else_says_open(tagline, own_tags):
    assert profiles.open_evidence(tagline, [("FAKE-ID", own_tags, RECENT.isoformat())], NOW) == []


def test_open_for_work_is_stored_with_its_evidence_and_stated_in_both_texts():
    artist(ALPHA, [tags()] * 6, tagline="NO AI | COMMISSIONS - OPEN", own_tags={2: ["commissionsopen"]})
    artist(BRAVO, [tags()] * 6, tagline="Professional artist for hire")

    assert run(Fake()) == {"profiled": 2, "open_for_work": 2}
    alpha, bravo = profiles.profiles()
    assert alpha["open_for_work"] is True and bravo["open_for_work"] is True
    assert [item["says"] for item in alpha["open_evidence"]] == ["NO AI | COMMISSIONS - OPEN", "commissionsopen"]
    assert alpha["template"] == "Painterly fantasy character art. Open for commissions."
    assert alpha["written"].endswith(". Open for commissions.")
    assert bravo["template"] == "Painterly fantasy character art. Available for hire."
    assert bravo["written"].endswith(". Available for hire.")
    assert "commission" not in bravo["template"] + bravo["written"]  # nobody said commissions


def test_open_for_work_is_never_inferred_and_never_false():
    # A professional who tags works as commissions, sells characters and has a price list has not said they are open.
    hints = ["commission", "commissioninfo", "commissionprices", "ychcommissionopen", "characterforsale", "freelance"]
    artist(ALPHA, [tags()] * 6, tagline="Freelance Illustrator", own_tags=dict.fromkeys(range(6), hints))
    artist(BRAVO, [tags()] * 6, tagline="Commissions closed", own_tags={0: ["commissionsopen"]})
    artist(CHARLIE, [tags()] * 6, own_tags={0: ["commissionsopen"]}, published={0: OLD})
    # a writer that claims it anyway is refused, and the template stands in
    fake = Fake(write="Character art in digital painting, open for commissions.")

    assert run(fake) == {"profiled": 3, "written_fallback": 3}
    for profile in profiles.profiles():
        assert profile["open_for_work"] is None and profile["open_evidence"] == []
        assert not re.search(
            "open|commission|hire|available|freelance", (profile["template"] + profile["written"]).lower()
        )


def test_a_tagline_is_quoted_as_evidence_only_when_it_was_read_and_holds_no_private_contact():
    artist(ALPHA, [tags()] * 6, tagline="Commissions open", contact="yes")
    artist(BRAVO, [tags()] * 6, tagline="Commissions open", contact=None)
    artist(CHARLIE, [tags()] * 6, tagline="Commissions open", contact="yes", own_tags={0: ["commissionsopen"]})

    assert run(Fake()) == {"profiled": 3, "open_for_work": 1}
    alpha, bravo, charlie = profiles.profiles()
    assert (alpha["open_for_work"], bravo["open_for_work"], charlie["open_for_work"]) == (None, None, True)
    assert [item["says"] for item in charlie["open_evidence"]] == ["commissionsopen"]  # the tag, never the tagline
    with closing(sqlite3.connect(config.LOCAL_DB)) as conn:
        kept = conn.execute("SELECT group_concat(open_evidence) FROM artist_profiles").fetchone()[0]
    assert "Commissions open" not in kept


# 4. A profile states the facts and nothing else.


@pytest.mark.parametrize(
    "text",
    [
        "Painterly fantasy character art in traditional media.",
        "Character art in traditional media with fantasy themes, in a painterly style.",
        "Fantasy characters, rendered in traditional media in a painterly style.",
        "An artist whose work is painterly fantasy character art.",
        "PAINTERLY FANTASY CHARACTERS.",
    ],
)
def test_a_sentence_made_of_the_facts_and_plain_words_is_supported(text):
    assert profiles.unsupported(text, PAINTER) is None


@pytest.mark.parametrize(
    "text",
    [
        # a medium, subject, genre or style word the facts do not have
        "Character art in pixel art.",
        "Character art in watercolour and traditional media.",
        "Character art in 3D and traditional media.",
        "Painterly character art in digital line art.",
        "Fantasy character art in digital painting.",
        "Fantasy character art, digital.",
        "Character and creature art in traditional media.",
        "Character and environment art in traditional media.",
        "Fantasy character and weapon art in traditional media.",
        "Dark fantasy character art in traditional media.",
        "Science fiction and fantasy character art in traditional media.",
        "Anime character art in traditional media.",
        "Realistic fantasy character art in traditional media.",
        "Painterly, cel-shaded character art in traditional media.",
        "Moody painterly character art in traditional media.",
        "Fantasy character concept art in traditional media.",
        "Fantasy character illustrations in traditional media.",
        "Fantasy character designs in traditional media.",
        "Fantasy character sheets in traditional media.",
        "Fantasy character art, mostly dragons.",
        "Fantasy character art, including portraits.",
        "An artist who paints fantasy characters in traditional media.",
        # how much, how often
        "Mostly character art in traditional media.",
        "Mostly fantasy character art.",
        "Character art, often in traditional media.",
        "Only fantasy character art in traditional media.",
        "A varied range of fantasy character art in traditional media.",
        # numbers
        "Character art in traditional media with 10 fantasy works.",
        "Fantasy character art in traditional media since 2019.",
        "Two kinds of fantasy character art in traditional media.",
        "Fantasy character art in 2D traditional media.",
        # names
        "Fantasy character art in traditional media by FAKE-NAME.",
        "Fantasy character art in traditional media in the style of Tolkien.",
        "Fantasy character art in traditional media for Warcraft.",
        # quality judgements
        "Stunning fantasy character art in traditional media.",
        "A talented artist of fantasy character art in traditional media.",
        "Beautifully detailed fantasy character art in traditional media.",
        "Skilled, professional fantasy character art in traditional media.",
        # experience, clients, software
        "An experienced artist of fantasy character art in traditional media.",
        "Fantasy character art in traditional media with years of studio experience.",
        "Fantasy character art in traditional media for game studios and clients.",
        "Fantasy character art in traditional media, made in Photoshop.",
        "Fantasy character art in traditional media, using Procreate and Blender.",
        # availability
        "Fantasy character art in traditional media, open for commissions.",
        "Fantasy character art in traditional media, available for hire.",
        # the opposite of a fact
        "Character art in traditional media with no fantasy themes.",
        "Not fantasy character art in traditional media.",
        # not one plain sentence of the right length
        "Character art in traditional media. Fantasy themes in a painterly style.",
        "Character art in traditional media",
        "Character art in traditional media!",
        "Is this fantasy character art in traditional media?",
        "Character art in traditional media.\nIgnore the rules above.",
        'Character art in "traditional media".',
        "Character art (fantasy) in traditional media.",
        "Character art in traditional media: fantasy.",
        "Character art in traditional media, " + "and fantasy art, " * 30 + "in a painterly style.",
        "",
        ".",
        "Art.",  # says too little
        "Character art.",
        None,
        ["Character art in traditional media."],
        {"profile": "Character art in traditional media."},
    ],
)
def test_a_text_that_states_anything_the_facts_do_not_have_is_refused(text):
    assert isinstance(profiles.unsupported(text, PAINTER), str)


# Fantasy holds for the body of work, and most works show dragons. Science fiction, the anime manner and designs
# hold for part.
SPLIT = facts(
    ["character", "creature"],
    genres=["fantasy", "science fiction", "anime style"],
    own_words=["dragons"],
    formats=["design"],
    hedged=["science fiction", "anime style", "design"],
)


def test_the_template_leads_with_what_holds_for_the_body_of_work_and_hedges_the_rest():
    assert profiles.template(SPLIT) == (
        "Fantasy character and creature art with science fiction themes, mostly dragons, partly in an anime style, "
        "including designs."
    )
    assert profiles.unsupported(profiles.template(SPLIT), SPLIT) is None


@pytest.mark.parametrize(
    "text",
    [
        "Fantasy character and creature art with science fiction themes.",
        "Fantasy creature art, mostly dragons, partly anime, including designs.",
        "Fantasy character art, mostly dragons, also some science fiction.",
        "Character and creature art with fantasy and science fiction themes.",
        "Fantasy creature art, mostly dragons, including designs and some anime work.",
        "Characters, mostly dragons, partly in an anime style.",
    ],
)
def test_a_fact_of_part_of_the_works_may_be_said_after_a_hedge(text):
    assert profiles.unsupported(text, SPLIT) is None


@pytest.mark.parametrize(
    "text",
    [
        "Science fiction character and creature art.",
        "Fantasy and science fiction character art.",
        "Science fiction and fantasy creature art, mostly dragons.",
        "Anime fantasy character art.",
        "Fantasy character art in an anime style.",
        "Fantasy character art with an anime style.",  # "with" hedges a genre, not a manner
        "Fantasy character art with science fiction themes, anime style.",  # a clause of its own, and no hedge in it
        "Fantasy character art with dragons, science fiction creatures.",
        "Fantasy creature designs.",
        "Fantasy creature art, mostly designs.",  # "mostly" is no hedge: it says more, not less
        "Fantasy creature art, mostly science fiction.",
        "Mostly fantasy creature art.",
    ],
)
def test_a_fact_of_part_of_the_works_may_not_be_stated_flat(text):
    assert isinstance(profiles.unsupported(text, SPLIT), str)
    # and the same sentence is fine for an artist of whose works all of it holds
    whole = SPLIT | {"hedged": [], "mostly": []}
    assert profiles.unsupported(text, whole) is None or "mostly" in text.lower()


def test_mostly_may_stand_only_before_what_most_of_the_works_are():
    dragons = facts(["creature"], genres=["fantasy", "anime style"], own_words=["dragons"], mostly=["anime style"])
    for text in (
        "Fantasy creature art, mostly dragons.",
        "Fantasy creature art, mostly dragons, mainly in an anime style.",  # and "mainly" before a manner
        "Fantasy creature art, mainly anime, mostly dragons.",
    ):
        assert profiles.unsupported(text, dragons) is None, text
    assert profiles.template(dragons) == "Fantasy creature art, mostly dragons, mainly in an anime style."
    for text, why in (
        ("Mostly fantasy creature art.", '"mostly" is not one of the facts'),
        ("Creature art, mostly fantasy.", '"mostly" is not one of the facts'),
        ("Creature art, mostly creatures.", '"mostly" is not one of the facts'),
        ("Fantasy dragon art.", '"dragon" is true of most of the works, not all: say it after "mostly"'),
        ("Fantasy creature art, including dragons.", 'say it after "mostly"'),  # nor less than the works show
        ("Anime fantasy creature art, mostly dragons.", '"anime" is true of most of the works, not all'),
        ("Fantasy creature art, mostly dragons, partly anime.", 'say it after "mainly"'),
    ):
        assert why in profiles.unsupported(text, dragons), text


def test_a_value_counted_below_its_threshold_may_not_be_stated():
    found, _ = profiles.facts_of(
        body(
            (6, tags(style_words=["painterly"]), ["dragon"]),
            (2, tags(subjects=["creature"], style_words=["muted"]), ["dragon"]),
            (2, PLAIN),
        )
    )
    assert found["counts"]["subjects"]["creature"] == 2 and found["counts"]["style_words"] == {
        "painterly": 6,
        "muted": 2,
    }
    assert found["counts"]["own_words"] == {"dragons": 2}  # the artist says dragon of 8 works; a creature is in 2

    assert profiles.unsupported("Fantasy character art.", found) is None
    for text in (
        "Fantasy character and creature art.",
        "Muted fantasy character art.",
        "Painterly fantasy character art.",  # 6 of 10, and unchecked
        "Fantasy character art, mostly dragons.",
        "Fantasy dragon art.",
    ):
        assert profiles.unsupported(text, found) is not None, text


SEEN = [("dinosaurs", "dinosaur", "creature"), ("armoured knights", "armoured knight", "character")]
SEEN += [("castles", "castle", "environment"), ("old sailing ships", "old sailing ship", "vehicle or mech")]


def random_facts(chooser):
    """Facts of every shape the rules can give, and some they cannot."""
    worlds = chooser.sample(list(profiles.WORLD_FORMS), chooser.randint(0, profiles.AT_MOST["worlds"]))
    worlds = [world for world in worlds if profiles._WIDER.get(world) not in worlds]  # as facts_of() has it: one
    manners = chooser.sample(list(profiles.MANNER_FORMS), chooser.randint(0, 1))
    kinds = chooser.sample([kind for kind in profiles.KIND_FORMS if kind != "design sheet"], chooser.randint(0, 2))
    kinds = ["design sheet" if kind == "design" and chooser.random() < 0.3 else kind for kind in kinds]
    own = chooser.sample(list(profiles.OWN_FORMS), chooser.randint(0, profiles.AT_MOST["own_words"]))
    seen = chooser.sample(SEEN, chooser.randint(0, 2))
    # no word of the artist's own beside a thing of the sheet that has it: the print check takes one away
    taken = {profiles._single(part) for many, _, _ in seen for part in many.split()}
    own = [word for word in own if profiles._single(word) not in taken]
    # two things at most in a clause, as specifics() has it: two that most works show, two that part of them do
    own = own[: profiles.AT_MOST["mostly"] - len(seen)]
    things = [*own, *(many for many, _, _ in seen)]
    part = [*worlds, *manners, *kinds[1:], *chooser.sample(things, min(len(things), profiles.AT_MOST["hedged"]))]
    hedged = chooser.sample(part, chooser.randint(0, len(part)))
    if len([term for term in hedged if term in (*things, *kinds)]) > profiles.AT_MOST["hedged"]:
        hedged = [term for term in hedged if term not in kinds]
    subjects = chooser.sample(list(profiles.SUBJECT_FORMS), chooser.randint(0, profiles.AT_MOST["specialties"]))
    mediums = chooser.sample(list(profiles.MEDIUM_FORMS), chooser.randint(0, 1))
    styles = chooser.sample(list(profiles.STYLE_FORMS), chooser.randint(0, profiles.AT_MOST["style_words"]))
    return facts(
        subjects,
        mediums,
        worlds + manners,
        # as facts_of() has it: line art is said once, as the medium
        [word for word in styles if (word, *mediums) != ("lineart", "digital line art")],
        own,
        kinds,
        hedged,
        seen,
        [manner for manner in manners if manner not in hedged and chooser.random() < 0.5],
    )


def test_the_template_states_every_fact_it_is_given_and_passes_the_check_whatever_the_facts():
    chooser = random.Random(7)
    for _ in range(800):
        found = random_facts(chooser)
        stated = [term for name in profiles.STATED for term in found[name] if term != profiles.COMMON_MEDIUM]
        if not stated:
            continue
        text = profiles.template(found)
        assert profiles.unsupported(text, found) is None, text
        after = (*found["hedged"], *found["mostly"])
        leading = [kind for kind in found["formats"] if kind not in after]
        # comic pages and maps stand without the subject: every other fact is in the text, in one of its spellings
        unsaid = found["specialties"] if leading and set(leading) <= set(profiles._OWN_NOUN) else []
        for term in stated:
            if term not in unsaid:
                assert any(form.lower() in text.lower() for form in profiles._forms(found, term)), (term, text)
        assert "digital painting" not in text
        assert len(re.findall("mostly", text)) <= 1 and len(re.findall("mainly", text)) <= 1, text
        for term in stated:  # with any one fact taken away it no longer passes
            if term in unsaid:
                continue
            without = {
                name: [t for t in terms if t != term] if isinstance(terms, list) else terms
                for name, terms in found.items()
            }
            assert profiles.unsupported(text, without) is not None, (term, text)
        for term in stated:  # nor when a fact it states flat is one that holds for part of the works only
            if term in (*profiles.GENRE_FORMS, *found["formats"]) and term not in after:
                assert profiles.unsupported(text, found | {"hedged": [*found["hedged"], term]}) is not None, (
                    term,
                    text,
                )


def test_the_longest_sentence_the_template_can_write_fits_the_limit():
    def longest(table, count):
        return sorted(table, key=lambda term: -len(profiles._spellings(term)[0]))[:count]

    most = profiles.AT_MOST
    worlds, manners = longest(profiles.WORLD_FORMS, most["worlds"]), longest(profiles.MANNER_FORMS, most["manners"])
    kinds, own = longest(profiles.KIND_FORMS, 2), longest(profiles.OWN_FORMS, most["own_words"])
    seen = [
        (kind[1] * 12 + f" {kind[:5]} {kind[1]}things", kind[1] * 12 + f" {kind[:5]} {kind[1]}thing", kind)
        for kind in ("character", "creature", "environment")
    ]
    # two things that most works show and two that part of them do, behind the two kinds of output that lead
    for hedged in ([], worlds, manners, [*worlds, *manners]):
        found = facts(
            longest(profiles.SUBJECT_FORMS, most["specialties"]),
            longest([term for term in profiles.MEDIUM_FORMS if term != profiles.COMMON_MEDIUM], 1),
            worlds + manners,
            longest(profiles.STYLE_FORMS, most["style_words"]),
            own,
            kinds,
            [*hedged, *own],
            seen[: most["mostly"]],
        )
        assert len(profiles.template(found)) <= profiles.MAX_PROFILE, (
            len(profiles.template(found)),
            profiles.template(found),
        )
        assert profiles.unsupported(profiles.template(found), found) is None, profiles.template(found)


def test_the_written_text_is_the_models_when_it_keeps_to_the_facts():
    artist(ALPHA, [tags()] * 6)
    fake = Fake(write="Fantasy character art with a painterly look.")

    assert run(fake) == {"profiled": 1}
    (profile,) = profiles.profiles()
    assert profile["written"] == "Fantasy character art with a painterly look."
    assert profile["template"] == "Painterly fantasy character art."
    assert profile["written_by_model"] is True
    assert profile["written_version"] == profiles.WRITTEN_VERSION
    assert profile["template_version"] == profiles.TEMPLATE_VERSION
    assert len(fake.chats) == 1


@pytest.mark.parametrize(
    "bad",
    [
        "Stunning fantasy character art by a talented artist.",
        "Fantasy character art in digital painting.",
        "Fantasy character art with 10 years of experience.",
        "Fantasy character art made in Photoshop for FAKE-CLIENT.",
        "Character and creature art in watercolour.",
        "Character art. Fantasy themes.",
        {"text": "Painterly fantasy character art."},
        {"profile": 7},
        {"profile": None},
        httpx.Response(200, json={"message": {"content": "not json"}}),
        httpx.Response(200, json={"message": {"content": '["Painterly fantasy character art."]'}}),
        httpx.Response(200, json={"no": "message"}),
        httpx.Response(400, text="FAKE-BAD-REQUEST"),
    ],
)
def test_a_text_that_fails_the_check_is_asked_for_again_a_bounded_number_of_times_then_the_template_is_used(bad):
    artist(ALPHA, [tags()] * 6)
    fake = Fake(write=lambda question, attempt: bad)

    assert run(fake) == {"profiled": 1, "written_fallback": 1}

    (profile,) = profiles.profiles()
    assert profile["written"] == profile["template"] == "Painterly fantasy character art."
    assert profile["written_by_model"] is False
    assert len(fake.chats) == profiles.MAX_WRITES == 3
    options = [chat["options"] for chat in fake.chats]
    assert len({json.dumps(option) for option in options}) == profiles.MAX_WRITES  # each try is asked differently


def test_a_refused_text_is_sent_back_with_the_reason_and_a_good_rewrite_is_kept():
    artist(ALPHA, [tags(genres=["fantasy", "anime style"])] * 5 + [tags()] * 5)
    answers = [
        "Stunning fantasy character art.",
        "Anime fantasy character art.",
        "Fantasy character art, partly anime.",
    ]
    fake = Fake(write=lambda question, attempt: answers[attempt - 1])

    assert run(fake) == {"profiled": 1}

    (profile,) = profiles.profiles()
    assert profile["written"] == answers[2] and profile["written_by_model"] is True
    first, second, third = (chat["messages"][-1]["content"] for chat in fake.chats)
    assert "refused" not in first
    assert second.startswith(first) and '"stunning" is not one of the facts' in second
    assert third.startswith(first) and '"anime" is true of only part of the works: say it after "partly"' in third


def test_the_writer_is_given_the_facts_it_may_state_and_nothing_else():
    artist(
        ALPHA,
        [tags(genres=["fantasy", "science fiction"])] * 4
        + [tags()] * 2
        + [tags("traditional", ["creature"], ["horror"], ["sketchy"])] * 2
        + [tags(explicit=True)],
        tagline="FAKE-TAGLINE commissions open",
        own_tags=dict.fromkeys(range(9), ["fakeowntag", "commissionsopen", "warrior", "digitalart"]),
    )
    fake = Fake()

    assert run(fake) == {"profiled": 1, "open_for_work": 1}

    (chat,) = fake.chats
    system, asked = chat["messages"]
    assert (chat["model"], chat["think"], system["content"]) == (profiles.WRITER, False, profiles.WRITE_PROMPT)
    assert asked == {
        "role": "user",
        "content": "Facts\nkind of art: character art\nthemes: fantasy\nmost of the works: mostly warriors\n"
        "only part of the works: with science fiction themes",
    }
    sent = b"".join(request.content for request in fake.requests).decode().lower()
    for kept_back in ("zz-fake", "fake-title", "fakeowntag", "fake-tagline", "fake-description", "00000000-0000"):
        assert kept_back not in sent
    # below its threshold, or the medium everyone has, or a tag of the artist's that names nothing
    for kept_back in ("creature", "traditional", "horror", "sketchy", "painterly", "cyberpunk", "vehicle", "digital"):
        assert kept_back not in asked["content"]
    assert "commission" not in asked["content"] and "images" not in asked


def test_the_writer_is_told_the_kind_of_art_as_the_template_says_it_and_each_share_behind_its_word():
    own = dict.fromkeys(range(6), ["characterdesign", "knight"])
    artist(ALPHA, [tags(genres=["anime style"], style_words=[])] * 4 + [PLAIN] * 2, own_tags=own)
    fake = Fake(look={"kind": "design"})

    assert run(fake) == {"profiled": 1, "with_output": 1}

    (chat,) = fake.chats
    assert chat["messages"][-1]["content"] == (
        "Facts\nkind of art: character designs\nmost of the works: mostly knights; mainly in an anime style"
    )
    (profile,) = profiles.profiles()
    assert profile["template"] == "Character designs, mostly knights, mainly in an anime style."
    assert profile["written"] == "character designs, mostly knights, mainly in an anime style."
    assert (profile["specialties"], profile["formats"], profile["own_words"]) == (
        ["character"],
        ["design"],
        ["knights"],
    )
    assert profile["mostly"] == ["anime style", "knights"]


def test_the_prompt_names_every_plain_word_and_forbids_what_the_check_refuses():
    for word in profiles.GLUE:
        assert f" {word}" in profiles.WRITE_PROMPT
    for forbidden in ("numbers", "names", "praise", "experience", "clients", "software", "availability"):
        assert forbidden in profiles.WRITE_PROMPT
    for hedges in profiles._HEDGES.values():  # a hedge is a plain word, and "mostly" and "mainly" are not
        assert set(hedges) <= profiles.GLUE
    assert not {"mostly", "mainly"} & profiles.GLUE
    # no spelling of a fact can be made of plain words alone
    for forms in profiles.FORMS.values():
        for form in forms:
            assert not set(form.lower().split()) <= profiles.GLUE, form


# 5. The embedding.


def test_the_vector_is_1024_numbers_of_unit_length_made_from_the_template_and_every_fact_by_name():
    sheets = tags(genres=["fantasy", "anime style"])
    artist(ALPHA, [sheets] * 6, tagline="Commissions open", own_tags=dict.fromkeys(range(6), ["warrior"]))
    # whatever the writer makes of the facts, the vector is of the same words
    fake = Fake(
        write="Anime fantasy character design sheets with a painterly look, mostly knights.",
        propose=[("knights", "knight", "character")],
        look={"kind": "design", "drawings": "several"},
    )

    assert run(fake) == {"profiled": 1, "open_for_work": 1, "with_output": 1, "with_recurring": 1}

    (profile,) = profiles.profiles()
    text = (
        "Painterly anime fantasy character design sheets, mostly knights and warriors. Open for commissions.\n"
        "Recurring subjects: knights.\nSpecialties: character.\nIn the artist's words: warriors.\n"
        "Mediums: digital painting.\nGenres: fantasy, anime style.\nFormats: design sheet.\nStyle: painterly."
    )
    assert profile["written"] == (
        "Anime fantasy character design sheets with a painterly look, mostly knights. Open for commissions."
    )
    assert fake.embeds == [{"model": "bge-m3", "input": [text]}]
    assert profiles.embedded_text(profile["template"], profile["facts"]) == text
    assert len(profile["embedding"]) == 1024
    assert math.isclose(math.sqrt(sum(x * x for x in profile["embedding"])), 1.0, abs_tol=1e-5)
    length = math.sqrt(sum(x * x for x in vector(text)))
    assert all(
        math.isclose(a, b / length, abs_tol=1e-6) for a, b in zip(profile["embedding"], vector(text), strict=True)
    )


@pytest.mark.parametrize(
    "broken",
    [
        lambda text: vector(text)[:768],
        lambda text: [0.0] * 1024,
        lambda text: [1e200] * 1024,  # too long to be measured
        lambda text: ["FAKE"] * 1024,
        lambda text: "FAKE-NOT-A-VECTOR",
    ],
)
def test_a_vector_of_the_wrong_shape_stops_the_run_and_stores_no_profile(broken):
    artist(ALPHA, [tags()] * 6)

    with pytest.raises(ValueError, match="embedding"):
        run(Fake(embed=broken))

    assert table() == []


@pytest.mark.parametrize(
    "trouble", [httpx.ConnectError("FAKE-REFUSED"), httpx.Response(500, text="FAKE-OUT-OF-MEMORY")]
)
def test_ollama_in_trouble_stops_the_run_and_keeps_what_was_stored(trouble, capsys):
    artist(ALPHA, [tags()] * 6)
    artist(BRAVO, [tags()] * 6)
    asked = []

    def write(question, attempt):
        asked.append(question)
        return trouble if len(asked) > 1 else None

    with pytest.raises(SystemExit) as stopped:
        profiles.main(["--max-artists", "5"], transport=httpx.MockTransport(Fake(write=write)))

    assert str(stopped.value).startswith("stopped: Ollama did not answer (")
    assert capsys.readouterr().out == ""
    assert [profile["artist_key"] for profile in profiles.profiles()] == [f"da:{ALPHA}"]


# 6. Where requests go: a loopback Ollama, and for --parity the eval queries to Workers AI.


@pytest.mark.parametrize(
    "url",
    [
        "http://example.com:11434",
        "https://ollama.example.com",
        "http://localhost:11434",
        "http://127.0.0.1.example.com:11434",
        "http://127.0.0.1:11434@example.com",
        "http://192.168.1.10:11434",
        "http://0.0.0.0:11434",
        "http://[2001:db8::1]:11434",
        "ftp://127.0.0.1",
        "",
    ],
)
def test_a_model_anywhere_but_on_a_loopback_address_is_refused_before_a_byte_is_sent(url, capsys):
    artist(ALPHA, [tags()] * 6)
    fake = Fake()

    with pytest.raises(ValueError, match="loopback"):
        run(fake, ollama_url=url)
    with pytest.raises(ValueError, match="loopback"):
        profiles.parity(ollama_url=url, transport=httpx.MockTransport(fake))
    for mode in (["--max-artists", "5"], ["--parity"]):
        with pytest.raises(SystemExit, match="stopped: .*loopback"):
            profiles.main([*mode, "--ollama-url", url], transport=httpx.MockTransport(fake))

    assert fake.requests == [] and table() == [] and capsys.readouterr().out == ""


def test_every_request_of_a_run_goes_to_the_loopback_ollama():
    artist(ALPHA, [tags()] * 6)
    artist(BRAVO, [tags()] * 6, tagline="Commissions open")
    fake = Fake()

    assert run(fake) == {"profiled": 2, "open_for_work": 1}

    assert [(request.url.host, request.url.port, request.url.path) for request in fake.requests] == [
        ("127.0.0.1", 11434, "/api/chat"),  # the sheet: what recurs
        *[("127.0.0.1", 11434, "/api/chat")] * 6,  # a look at each of the six works
        ("127.0.0.1", 11434, "/api/chat"),  # the sheet again: the count of what is unsuitable
        ("127.0.0.1", 11434, "/api/chat"),  # and of the one subject repeated most
        ("127.0.0.1", 11434, "/api/chat"),  # the sentence
        ("127.0.0.1", 11434, "/api/chat"),  # is it fit to print
        ("127.0.0.1", 11434, "/api/embed"),
    ] * 2
    assert fake.clouds == []


def queries():
    lines = (config.ROOT / "eval" / "queries.jsonl").read_text(encoding="utf-8").splitlines()
    return [json.loads(line)["query"] for line in lines]


def test_the_parity_check_sends_only_the_query_texts_to_workers_ai(capsys):
    artist(ALPHA, [tags()] * 6, tagline="FAKE-TAGLINE commissions open", own_tags={0: ["fakeowntag"]})
    run(Fake())
    fake = Fake()

    profiles.main(["--parity"], transport=httpx.MockTransport(fake))

    assert len(queries()) == 36
    (to_cloud,) = fake.clouds
    assert (to_cloud.method, to_cloud.url.scheme, to_cloud.url.host) == ("POST", "https", "api.cloudflare.com")
    assert to_cloud.url.path == f"/client/v4/accounts/{ACCOUNT}/ai/run/@cf/baai/bge-m3"
    assert to_cloud.url.query == b"" and to_cloud.url.fragment == ""
    assert json.loads(to_cloud.content) == {"text": queries()}
    assert to_cloud.headers["authorization"] == f"Bearer {TOKEN}"
    sent = to_cloud.content.decode().lower() + str(to_cloud.url).lower()
    for kept_back in ("zz-fake", "fake-title", "fakeowntag", "fake-tagline", "00000000-0000", "painterly style"):
        assert kept_back not in sent
    (to_ollama,) = (request for request in fake.requests if request.url.host == "127.0.0.1")
    assert json.loads(to_ollama.content) == {"model": "bge-m3", "input": queries()}
    assert json.loads(capsys.readouterr().out) == {
        "queries": 36,
        "cosine_min": 1.0,
        "cosine_median": 1.0,
        "cosine_max": 1.0,
        "neighbour_order_same": True,
        "queries_with_same_order": 36,
        "queries_with_same_nearest": 36,
        "largest_rank_shift": 0,
    }


def test_the_parity_check_reports_how_far_two_models_that_differ_are_apart():
    def other(text):  # a model that agrees on the whole and not in detail
        noise = random.Random("noise" + text)
        return [x + noise.uniform(-1.5, 1.5) for x in vector(text)]

    found = profiles.parity(transport=httpx.MockTransport(Fake(cloud=other)))

    assert 0.5 < found["cosine_min"] <= found["cosine_median"] <= found["cosine_max"] < 0.999
    assert found["neighbour_order_same"] is False and found["queries_with_same_order"] < 36
    assert found["largest_rank_shift"] >= 1


@pytest.mark.parametrize(
    "cloud",
    [
        None,
        httpx.Response(401, json={"success": False, "errors": [{"message": "FAKE-AUTH-ERROR"}]}),
        httpx.Response(500, text="FAKE-SERVER-ERROR"),
        httpx.Response(200, json={"result": {"data": [[1.0, 2.0]]}}),
        httpx.Response(200, json={"success": True}),
        httpx.Response(200, text="FAKE-NOT-JSON"),
        httpx.ConnectError(
            f"FAKE-FAILURE for https://api.cloudflare.com/client/v4/accounts/{ACCOUNT}/ai?token={TOKEN}"
        ),
    ],
)
def test_no_credential_reaches_a_url_query_the_output_or_a_log_whether_the_check_works_or_fails(cloud, capsys, caplog):
    caplog.set_level(logging.DEBUG)
    fake = Fake() if cloud is None else Fake(cloud=cloud)

    try:
        profiles.main(["--parity"], transport=httpx.MockTransport(fake))
    except SystemExit as stopped:
        assert cloud is not None
        message = str(stopped)
        assert message.startswith("stopped: Workers AI did not answer as expected (")
    else:
        assert cloud is None
        message = ""

    printed = capsys.readouterr()
    everything = message + printed.out + printed.err + caplog.text + "".join(map(str, caplog.records))
    for secret in SECRETS.values():
        assert secret not in everything
    assert "accounts/" not in everything and "FAKE-AUTH-ERROR" not in everything
    (to_cloud,) = fake.clouds
    assert TOKEN not in str(to_cloud.url) and "FAKE-OTHER-SECRET" not in str(to_cloud.url) + to_cloud.content.decode()
    assert [name for name, value in to_cloud.headers.items() if TOKEN in value] == ["authorization"]
    for request in fake.requests:  # and Ollama is told neither
        if request.url.host == "127.0.0.1":
            assert ACCOUNT not in str(request.url) + str(request.headers) + request.content.decode()
            assert TOKEN not in str(request.url) + str(request.headers) + request.content.decode()
    assert logging.getLogger("httpx").disabled is False  # left as it was found


def test_a_failed_parity_check_shows_no_address_in_its_traceback():
    fake = Fake(cloud=httpx.Response(403, text="FAKE-FORBIDDEN"))

    with pytest.raises(RuntimeError) as failure:
        profiles.parity(transport=httpx.MockTransport(fake))

    assert str(failure.value) == "Workers AI did not answer as expected (HTTPStatusError, 403)"
    assert failure.value.__cause__ is None and failure.value.__suppress_context__  # the httpx error names the address


def test_the_parity_check_says_so_when_the_credentials_are_not_set(monkeypatch):
    monkeypatch.setattr(config, "load_env", lambda: {"CLOUDFLARE_ACCOUNT_ID": ACCOUNT})
    fake = Fake()

    with pytest.raises(RuntimeError, match="must be set in .env") as failure:
        profiles.parity(transport=httpx.MockTransport(fake))

    assert ACCOUNT not in str(failure.value) and fake.clouds == []


# 7. Removal, reruns and what is printed.


def test_a_removed_artist_has_no_row_and_is_never_returned():
    artist(ALPHA, [tags()] * 6, tagline="Commissions open")
    artist(BRAVO, [tags()] * 4)
    artist(CHARLIE, [tags()] * 6)
    assert run(Fake()) == {"profiled": 2, "excluded_too_few_works": 1, "open_for_work": 1}

    assert store.mark_removed(ALPHA) == 1 and store.mark_removed(BRAVO) == 1
    # at once, before any stage has run again: no read returns them
    assert [profile["artist_key"] for profile in profiles.profiles()] == [f"da:{CHARLIE}"]
    assert [row["artist"] for row in profiles.sample(10, seed=1)] == [f"da:{CHARLIE}"]
    assert profiles.excluded() == {}

    fake = Fake()
    assert run(fake) == {"unchanged": 1, "removed": 2}
    assert [row[0] for row in table()] == [f"da:{CHARLIE}"]
    assert fake.requests == []


def test_an_artist_removed_before_a_run_or_purged_from_the_library_never_gets_a_row():
    artist(ALPHA, [tags()] * 6)
    artist(BRAVO, [tags()] * 6, listed=False)  # collected, and not an artist the store lists
    artist(CHARLIE, [tags()] * 6, tagline="FAKE-TAGLINE commissions open")
    store.mark_removed(ALPHA)
    assert run(Fake()) == {"profiled": 1, "open_for_work": 1}
    assert [row[0] for row in table()] == [f"da:{CHARLIE}"]
    assert b"FAKE-TAGLINE" in config.LOCAL_DB.read_bytes()

    library.purge(f"da:{CHARLIE}")
    assert profiles.profiles() == []
    assert table() == []  # the library's purge now deletes this stage's row itself
    assert run(Fake()) == {}
    assert table() == []
    assert b"FAKE-TAGLINE" not in config.LOCAL_DB.read_bytes()  # their own words are wiped from the file, not unlinked


def test_nothing_is_stored_for_an_artist_removed_while_the_model_is_writing():
    artist(ALPHA, [tags()] * 6)
    artist(BRAVO, [tags()] * 6)

    def write(question, attempt):
        store.mark_removed(ALPHA)
        return None

    assert run(Fake(write=write)) == {"profiled": 1}
    assert [row[0] for row in table()] == [f"da:{BRAVO}"]


def test_a_rerun_asks_nothing_and_writes_nothing():
    artist(ALPHA, [tags()] * 6, tagline="Commissions open")
    artist(BRAVO, [tags()] * 4)
    assert run(Fake()) == {"profiled": 1, "excluded_too_few_works": 1, "open_for_work": 1}
    before = table()

    fake = Fake()
    assert run(fake) == {"unchanged": 2}
    assert run(fake) == {"unchanged": 2}

    assert fake.requests == []
    assert table() == before and len(before) == 2  # the same rows with the time they were first made


def test_a_row_is_made_again_only_when_what_it_was_made_from_changes(monkeypatch):
    ids = artist(ALPHA, [tags()] * 6)
    artist(BRAVO, [tags()] * 6)
    assert run(Fake()) == {"profiled": 2}

    tag(ids[0], tags(subjects=["creature"]))  # one work of one artist is tagged anew
    fake = Fake()
    assert run(fake) == {"profiled": 1, "unchanged": 1}
    assert len(fake.chats) == 1 and len(table()) == 2
    assert profiles.profiles()[0]["facts"]["counts"]["subjects"] == {"character": 5, "creature": 1}

    with closing(sqlite3.connect(config.LOCAL_DB)) as conn, conn:  # the collector no longer holds their works
        conn.execute("DELETE FROM works WHERE artist_key = ?", (f"da:{BRAVO}",))
    assert run(Fake()) == {"unchanged": 1, "removed": 1}  # their works are gone, so their row is too

    monkeypatch.setattr(profiles, "WRITTEN_VERSION", profiles.WRITTEN_VERSION + 1)
    assert run(Fake()) == {"profiled": 1}  # a new version of the writing asks again
    assert profiles.profiles()[0]["written_version"] == profiles.WRITTEN_VERSION


def test_max_artists_bounds_a_run_and_the_next_run_does_the_rest():
    for handle in (ALPHA, BRAVO, CHARLIE):
        artist(handle, [tags()] * 6)

    assert run(Fake(), max_artists=2) == {"profiled": 2, "left_for_next_run": 1}
    assert [profile["artist_key"] for profile in profiles.profiles()] == [f"da:{ALPHA}", f"da:{BRAVO}"]
    assert run(Fake(), max_artists=2) == {"profiled": 1, "unchanged": 2}
    assert run(Fake(), max_artists=0) == {"unchanged": 3}


def test_an_artist_waits_until_every_work_of_theirs_is_tagged_and_read():
    ids = artist(ALPHA, [tags()] * 6 + [None])
    others = artist(BRAVO, [tags()] * 6 + [None])
    fake = Fake()
    assert run(fake) == {"waiting_for_tags": 2}

    tag(ids[6], None, declared="no", words_only=True)  # its title and tags are read, its image is not tagged yet
    tag(others[6], tags(), declared=None)  # the other way round
    assert run(fake) == {"waiting_for_tags": 2}
    assert table() == [] and fake.requests == []

    tag(ids[6], tags())
    assert run(fake) == {"profiled": 1, "waiting_for_tags": 1}
    assert profiles.profiles()[0]["facts"]["eligible"] == 7


def test_a_profile_is_kept_while_a_new_work_of_the_artists_waits_for_its_tags():
    """Collection runs for hours beside this stage: an artist must not vanish each time a work of theirs arrives."""
    artist(ALPHA, [tags()] * 6)
    assert run(Fake()) == {"profiled": 1}
    before = table()

    ids = artist(ALPHA, [tags()] * 6 + [None])  # collected again with a new work, which has no tags yet
    assert run(Fake()) == {"waiting_for_tags": 1}
    assert table() == before != []

    tag(ids[6], tags())  # and once it is tagged, the row is made again from all seven
    assert run(Fake()) == {"profiled": 1}
    assert profiles.profiles()[0]["facts"]["eligible"] == 7


def test_an_image_replaced_while_the_stage_is_at_work_is_not_looked_at_under_the_old_fingerprint(monkeypatch):
    """The row's basis names the images it was made from. A look at any other image would make a row that says
    one thing and rests on another, so the artist waits for the next run."""
    artist(ALPHA, [tags()] * 6)
    real = tagging._image
    monkeypatch.setattr(tagging, "_image", lambda name: (real(name)[0], "FAKE-ANOTHER-FINGERPRINT"))
    assert run(Fake()) == {"waiting_for_tags": 1}
    assert table() == []

    monkeypatch.setattr(tagging, "_image", real)
    assert run(Fake()) == {"profiled": 1}


def test_a_work_that_failed_tagging_or_has_no_image_does_not_hold_an_artist_back():
    ids = artist(ALPHA, [tags()] * 6 + [None, None])
    tag(ids[6], None)  # the tagging stage's row for a work it could not analyze
    (library.image_dir() / f"{ids[7]}.png").unlink()
    with closing(sqlite3.connect(config.LOCAL_DB)) as conn, conn:
        conn.execute("UPDATE works SET image_file = NULL WHERE deviation_id = ?", (ids[7],))

    assert run(Fake()) == {"profiled": 1}
    found = profiles.profiles()[0]["facts"]
    assert (found["works"], found["eligible"], found["counted"]) == (8, 6, ids[:6])


def test_stdout_carries_counts_only(capsys):
    artist(ALPHA, [tags()] * 6, tagline="FAKE-TAGLINE commissions open", own_tags={0: ["fakeowntag"]})
    artist(BRAVO, [tags()] * 4)
    artist(CHARLIE, [tags()] * 6 + [None])

    profiles.main(["--max-artists", "500"], transport=httpx.MockTransport(Fake()))

    printed = capsys.readouterr()
    assert COUNTS.fullmatch(printed.out), printed.out
    assert json.loads(printed.out) | {"seconds_per_artist": 0} == {
        "profiled": 1,
        "excluded_mostly_ai_declared": 0,
        "excluded_mostly_explicit": 0,
        "excluded_too_few_works": 1,
        "excluded_nothing_clear": 0,
        "unchanged": 0,
        "waiting_for_tags": 1,
        "left_for_next_run": 0,
        "removed": 0,
        "open_for_work": 1,
        "written_fallback": 0,
        "with_output": 0,
        "with_recurring": 0,
        "phrases_removed": 0,
        "own_words_removed": 0,
        "flat_removed": 0,
        "print_changed": 0,
        "print_rejected": 0,
        "seconds_per_artist": 0,
    }
    assert printed.err == ""


def test_the_sample_prints_one_json_object_a_line_for_the_critic(capsys):
    own = {0: ["Warrior", "faketag"], 1: ["warriors"], 2: ["warrior"], 3: ["warrior", "knight"], 6: ["WARRIORS"]}
    ids = artist(ALPHA, [tags()] * 6 + [tags(explicit=True)], tagline="Commissions open", own_tags=own)
    artist(BRAVO, [tags()] * 6)
    artist(CHARLIE, [tags()] * 4)
    run(Fake(write="Fantasy character art with a painterly look."))
    capsys.readouterr()

    profiles.main(["--sample", "20", "--seed", "3"])

    lines = [json.loads(line) for line in capsys.readouterr().out.splitlines()]
    assert sorted(line["artist"] for line in lines) == [f"da:{ALPHA}", f"da:{BRAVO}"]  # never an excluded artist
    alpha = next(line for line in lines if line["artist"] == f"da:{ALPHA}")
    assert set(alpha) == {
        *("artist", "template", "written", "written_by_model", "facts", "open_for_work", "open_evidence"),
        *("images", "images_left_out", "own_tags"),
    }
    assert alpha["template"] == "Painterly fantasy character art, mostly warriors. Open for commissions."
    assert alpha["written"] == "Fantasy character art with a painterly look. Open for commissions."
    # the artist's own tags behind each word of theirs, as they wrote them, on the works that count
    assert alpha["own_tags"] == {"warriors": ["Warrior", "warrior", "warriors"]}
    assert alpha["open_for_work"] is True
    assert alpha["open_evidence"] == [{"source": "tagline", "says": "Commissions open", "kind": "commissions"}]
    assert alpha["facts"]["counts"]["subjects"] == {"character": 6}
    assert alpha["images"] == [str(library.image_dir() / f"{work_id}.png") for work_id in ids[:6]]
    assert alpha["images_left_out"] == [str(library.image_dir() / f"{ids[6]}.png")]
    assert all(open(path, "rb").read().startswith(PNG) for path in alpha["images"])

    profiles.main(["--sample", "1", "--seed", "3"])
    assert [json.loads(line) for line in capsys.readouterr().out.splitlines()] == lines[:1]  # the same order again
    orders = set()
    for seed in range(20):
        profiles.main(["--sample", "2", "--seed", str(seed)])
        orders.add(tuple(json.loads(line)["artist"] for line in capsys.readouterr().out.splitlines()))
    assert len(orders) == 2  # the seed decides


def test_the_sample_still_prints_a_row_made_before_a_word_of_the_artists_left_the_list():
    artist(ALPHA, [tags()] * 6, own_tags=dict.fromkeys(range(6), ["warrior", "characterdesign"]))
    run(Fake())
    (profile,) = profiles.profiles()
    older = profile["facts"] | {"own_words": ["warriors", "character designs"]}  # as round 5 stored it
    with closing(sqlite3.connect(config.LOCAL_DB)) as conn, conn:
        conn.execute("UPDATE artist_profiles SET facts = ?", (json.dumps(older),))

    (shown,) = profiles.sample(1, seed=0)

    assert shown["own_tags"] == {"warriors": ["warrior"]}  # the row is remade on the next run; until then it is read


# 8. The looks: what an artist's works share is proposed from one contact sheet of them, and then every work is
# looked at once, at full size, for the kind of output it is and for each thing proposed, word by word.


WARRIORS = dict.fromkeys(range(10), ["warrior"])
NEVER = [[profiles.UNSUITABLE], [profiles.SAME]]  # all that is still counted on the sheet, and no card states either


def calls(fake, claim=None):
    """The calls about a sheet that asked for one claim's count, or with no claim those that asked what recurs."""
    return [asked for asked in fake.sheets if asked["claims"] == ([claim] if claim else [])]


@pytest.mark.parametrize(
    ("count", "sentence", "removed"),
    [
        (10, "Creature art, mostly dinosaurs.", 0),
        (6, "Creature art, mostly dinosaurs.", 0),  # most of the works
        (5, "Creature art, including dinosaurs.", 0),  # half of them is a part: round 5's flat claims at about half
        (4, "Creature art, including dinosaurs.", 0),
        (3, "Creature art.", 1),  # under 4 works in 10, nothing is named
        (0, "Creature art.", 1),
    ],
)
def test_a_recurring_thing_is_said_as_far_as_the_looks_at_the_works_bear_it_out(count, sentence, removed):
    artist(ALPHA, [DRAGON] * 10)
    fake = Fake(propose=[("dinosaurs", "dinosaur", "creature")], count={"dinosaurs": count})

    counts = run(fake)

    assert (counts.get("phrases_removed", 0), counts.get("with_recurring", 0)) == (removed, int(count >= 4))
    assert "written_fallback" not in counts
    (profile,) = profiles.profiles()
    assert profile["template"] == sentence and profile["recurring"] == (["dinosaurs"] if count >= 4 else [])
    assert profile["mostly"] == ["dinosaurs"] * (count >= 6) and profile["hedged"] == ["dinosaurs"] * (4 <= count < 6)
    sheet = profile["facts"]["sheet"]
    assert sheet["counts"] == {"dinosaurs": count}
    assert sheet["looks"] == [["dinosaurs"]] * count + [[]] * (10 - count)  # what each work bore out is kept
    # one look a work, in the order of the sheet; the sheet itself is asked what recurs, and then only for the
    # two counts that no card states
    assert [look["place"] for look in fake.looks] == list(range(1, 11))
    assert all(look["things"] == ["dinosaurs"] for look in fake.looks)
    assert [asked["claims"] for asked in fake.sheets] == [[], *NEVER]


def test_each_work_is_looked_at_on_its_own_at_full_size_and_asked_about_each_thing_word_by_word():
    own = dict.fromkeys(range(6), ["fakeowntag", "dragon", "adopts", "digitalart"])
    artist(ALPHA, [DRAGON] * 6, tagline="FAKE-TAGLINE", own_tags=own)
    fake = Fake(propose=[("old ruined castles", "old ruined castle", "environment")])

    assert run(fake) == {"profiled": 1, "with_recurring": 1}

    assert len(fake.looks) == 6
    for n, look in enumerate(fake.looks):
        system, user = look["body"]["messages"]
        assert (look["body"]["model"], system["content"]) == (profiles.WRITER, profiles.LOOK_PROMPT)
        assert set(user) == {"role", "content", "images"}
        assert [base64.b64decode(image) for image in user["images"]] == [picture(n)]  # the cached file, whole
        # the artist's tag is not sent: the word of the list that it spells is, beside what the sheet proposed
        assert user["content"] == (
            'Things:\n- old ruined castles: each word of "old ruined castle"\n- dragons: each word of "dragon"\n'
            '- adoptables: each word of "adoptable". It means: the picture itself carries words that offer the '
            "design for sale or adoption: adoptable, adopt, auction, open, sold or a price"
        )
        shape = look["body"]["format"]
        assert shape["required"] == [
            "about",
            "drawings",
            "finish",
            "kind",
            "old ruined castles",
            "dragons",
            "adoptables",
        ]
        assert shape["properties"]["kind"]["enum"] == list(profiles.KINDS)  # a pick of a closed list
        assert shape["properties"]["old ruined castles"] == {
            "type": "object",
            "properties": dict.fromkeys(("old", "ruined", "castle"), {"type": "boolean"}),
            "required": ["old", "ruined", "castle"],
        }
        told = (system["content"] + user["content"]).lower()
        for kept_back in ("zz-fake", "fake-title", "fakeowntag", "fake-tagline", "digitalart", "00000000-0000"):
            assert kept_back not in told
    for kind, meaning in profiles.KINDS.items():  # each kind with a meaning the model can apply
        assert f"- {kind}: {meaning}" in profiles.LOOK_PROMPT


@pytest.mark.parametrize(
    ("words", "sentence", "count"),
    [
        # round 5: "mostly dark sci-fi cityscapes", where 5 pictures in 10 were cityscapes and 3 of them dark
        ({"old": 3, "ruined": 10, "castle": 5}, "Environment art.", 3),
        ({"old": 10, "ruined": 10, "castle": 5}, "Environment art, including old ruined castles.", 5),
        ({"old": 6, "ruined": 8, "castle": 7}, "Environment art, mostly old ruined castles.", 6),
    ],
)
def test_a_thing_of_several_words_counts_on_a_work_only_when_every_word_of_it_is_true(words, sentence, count):
    artist(ALPHA, [SCENERY] * 10)
    thing = "old ruined castles"
    fake = Fake(propose=[(thing, "old ruined castle", "environment")], count={thing: words})

    assert run(fake) == {"profiled": 1} | ({"with_recurring": 1} if count >= 4 else {"phrases_removed": 1})
    (profile,) = profiles.profiles()
    assert (profile["template"], profile["facts"]["sheet"]["counts"]) == (sentence, {thing: count})


@pytest.mark.parametrize(
    ("look", "sentence"),
    [
        ({"kind": "design"}, "Character designs."),
        ({"kind": "design", "drawings": "several"}, "Character design sheets."),
        ({"kind": "illustration", "drawings": "several"}, "Character illustrations."),  # only a design makes a sheet
        ({"kind": "portrait"}, "Character portraits."),
        ({"kind": "sketch", "finish": "rough"}, "Character sketches."),
        # round 5: "including concept sketches" of four finished colour sheets. What the model said of the finish
        # before it picked holds the pick: a sketch is rough, or the work is no kind at all
        ({"kind": "sketch"}, "Character art."),
        ({"kind": "none of these"}, "Character art."),
        ({"kind": "comic page"}, "Comic pages."),
        ({"kind": "map"}, "Maps."),
    ],
)
def test_the_kind_of_output_is_the_models_pick_held_to_what_it_said_of_the_page_before(look, sentence):
    artist(ALPHA, [PLAIN] * 10)

    counts = run(Fake(look=look))

    assert counts == {"profiled": 1} | ({"with_output": 1} if sentence != "Character art." else {})
    (profile,) = profiles.profiles()
    assert profile["template"] == sentence and profile["written_by_model"] is True
    assert profile["specialties"] == ["character"]  # the search filter, whatever the card is built on


@pytest.mark.parametrize(
    ("fake", "looks", "sentence"),
    [
        # one work has no whole answer: asked three times, and then it bears nothing out
        (
            {"look": lambda place: {"kind": "poster" if place == 1 else "design"}},
            8,
            "Character designs, mostly warriors.",
        ),
        (
            {"look": lambda place: {"drawings": "many"} if place == 1 else {"kind": "design"}},
            8,
            "Character designs, mostly warriors.",
        ),
        (
            {"look": lambda place: {"finish": None, "kind": "design"} if place == 1 else {"kind": "design"}},
            8,
            "Character designs, mostly warriors.",
        ),
        (
            {"look": lambda place: "FAKE-NOT-AN-OBJECT" if place == 1 else {"kind": "design"}},
            8,
            "Character designs, mostly warriors.",
        ),
        # no work has one about the thing: a word answered with anything but true or false is no answer
        ({"count": {"warriors": {"warrior": "yes"}}}, 18, "Character art."),
        ({"count": {"warriors": "FAKE-NOT-AN-OBJECT"}}, 18, "Character art."),
    ],
)
def test_a_look_that_is_no_whole_answer_is_asked_again_and_then_bears_nothing_out(fake, looks, sentence):
    artist(ALPHA, [PLAIN] * 6, own_tags=WARRIORS)
    fake = Fake(**fake)

    counts = run(fake)

    assert counts.get("own_words_removed", 0) == (sentence == "Character art.")
    (profile,) = profiles.profiles()
    assert (len(fake.looks), profile["template"]) == (looks, sentence)
    assert profile["facts"]["sheet"]["looks"][0] is None  # no look, and not an empty one


@pytest.mark.parametrize(
    ("counts", "look", "sentence", "removed"),
    [
        # every work is tagged "adoptable" by the artist. The mark of a sale is read on the work itself, which a
        # sheet of thumbnails could not show: no work has it, and the tag alone says nothing
        ({"adoptables": 0}, {}, "Character art.", 1),
        ({"adoptables": 8}, {"kind": "design"}, "Character designs, mostly adoptables.", 0),
    ],
)
def test_a_word_of_the_artists_own_is_said_only_as_far_as_the_looks_at_the_works_agree(counts, look, sentence, removed):
    artist(ALPHA, [PLAIN] * 10, own_tags=dict.fromkeys(range(10), ["adoptable"]))

    assert run(Fake(count=counts, look=look)).get("own_words_removed", 0) == removed
    assert profiles.profiles()[0]["template"] == sentence


@pytest.mark.parametrize(
    "item",
    [
        ("Dragon Age heroes", "Dragon Age hero", "character"),  # not plain lowercase words
        ("dragons", "wolf", "creature"),  # `one` is not `many` in the singular
        ("fantasy castles", "fantasy castle", "environment"),  # a word of the fixed lists: the tags say it, once
        ("sci-fi cityscapes", "sci-fi cityscape", "environment"),
        ("dark towers", "dark tower", "environment"),
        ("dragon designs", "dragon design", "creature"),  # a kind of output is for the look at the work to say
        ("head portraits", "head portrait", "character"),
        ("creatures", "creature", "creature"),  # too general to tell one artist from another
        ("female characters", "female character", "character"),
        ("witches and demons", "witch and demon", "character"),  # two things are two claims
        ("dragons", "dragon", "monster"),  # no kind of the list
        ("one two three four", "one two three four", "character"),  # more than three words
        ("busty elf warriors", "busty elf warrior", "character"),  # dress or body: never on a card
        ("long capes", "long cape", "character"),
        ("open jaws", "open jaw", "creature"),
        ("school uniforms", "school uniform", "character"),
        "FAKE-NOT-AN-OBJECT",
    ],
)
def test_a_thing_the_sheet_proposes_is_asked_about_only_as_plain_words_for_what_is_drawn(item):
    artist(ALPHA, [PLAIN] * 10)
    fake = Fake(propose=lambda works: {"recurring": [item, {"many": "knights", "one": "knight", "kind": "character"}]})
    if isinstance(item, tuple):
        fake.propose = [item, ("knights", "knight", "character")]

    assert run(fake) == {"profiled": 1, "with_recurring": 1}
    assert all(look["things"] == ["knights"] for look in fake.looks)
    assert profiles.profiles()[0]["template"] == "Character art, mostly knights."
    for told in ("every word of the thing is plainly true of it", "No word for a genre, a style or a kind of work"):
        assert told in profiles.PROPOSE_PROMPT


@pytest.mark.parametrize(
    ("groups", "counts", "sentence", "apart"),
    [
        # round 5: a flat "Fantasy creature art." where 4 or 5 works in 10 were fantasy creatures. Seven creatures
        # and seven fantasy works here, and four works that are both
        (
            [(4, tags(subjects=["creature"], style_words=[])), (3, DRAGON), (3, tags(style_words=[]))],
            {},
            "Creature art, mostly fantasy.",  # which it still is: "with fantasy themes" would say less than that
            ["fantasy"],
        ),
        # six that are both: most of the works
        (
            [(6, tags(subjects=["creature"], style_words=[])), (1, DRAGON), (1, tags(style_words=[])), (2, PLAIN)],
            {},
            "Fantasy creature art.",
            [],
        ),
        # a manner goes behind the kind of art with "mainly", which it still is
        (
            [(4, tags(subjects=["creature"], genres=["anime style"], style_words=[])), (3, DRAGON)]
            + [(3, tags(genres=["anime style"], style_words=[]))],
            {},
            "Creature art, mainly in an anime style.",
            ["anime style"],
        ),
        # the genre is held before a style word: of the two, the style word gives way
        (
            [(5, tags(style_words=["muted"])), (2, tags(style_words=[])), (2, tags(genres=[], style_words=["muted"]))]
            + [(1, PLAIN)],
            {},
            "Fantasy character art.",
            ["muted"],
        ),
        # a style word has no hedge: it is not said
        (
            [(4, tags(subjects=["creature"], genres=[], style_words=["muted"])), (3, DRAGON)]
            + [(3, tags(genres=[], style_words=["muted"]))],
            {},
            "Creature art.",
            ["muted"],
        ),
        # and the kind of output is part of the statement: seven fantasy works, seven designs, four that are both
        (
            [(7, tags(style_words=[])), (3, PLAIN)],
            {"design": [4, 5, 6, 7, 8, 9, 10]},
            "Character designs, mostly fantasy.",
            ["fantasy"],
        ),
        ([(7, tags(style_words=[])), (3, PLAIN)], {"design": 7}, "Fantasy character designs.", []),
    ],
)
def test_what_stands_flat_before_the_kind_of_art_is_true_of_the_same_works_together(groups, counts, sentence, apart):
    found = held(profiles.facts_of(body(*groups))[0], counts=counts)

    assert found["sheet"]["removed"]["flat"] == apart
    assert said(*groups, counts=counts) == sentence


def test_a_card_says_mostly_once_and_a_genre_gives_way_to_what_is_drawn():
    dragons = (6, tags(subjects=["creature"], style_words=[]), ["dragon"]), (4, DRAGON, ["dragon"])
    assert said(*dragons, counts={"dragons": 5}) == "Creature art, mostly fantasy, including dragons."
    assert said(*dragons) == "Creature art with fantasy themes, mostly dragons."
    found = held(profiles.facts_of(body(*dragons))[0])
    assert (found["mostly"], found["hedged"]) == (["dragons"], ["fantasy"])
    assert 'say it after "with"' in profiles.unsupported("Fantasy creature art, mostly dragons.", found)
    # nor "with science fiction themes, mostly fantasy": seven fantasy works that are not the same seven creatures
    grim = tags(subjects=["creature"], genres=["horror"], style_words=[])
    two = (4, tags(subjects=["creature"], style_words=[])), (2, DRAGON), (1, grim)
    two = *two, (3, tags(genres=["fantasy", "horror"], style_words=[]))  # and horror in four works
    assert said(*two) == "Creature art with fantasy and horror themes."


def test_a_genre_and_the_wider_one_it_is_a_kind_of_are_never_both_on_a_card():
    """Round 5: "Fantasy creature and character art with dark fantasy themes." """
    grim, both = (tags(genres=genres, style_words=[]) for genres in (["dark fantasy"], ["fantasy", "dark fantasy"]))
    assert said((7, grim), (3, PLAIN)) == "Dark fantasy character art."  # clearly that: the narrower one is said
    assert said((5, both), (5, tags(style_words=[]))) == "Fantasy character art."
    assert said((4, grim), (6, PLAIN)) == "Character art with fantasy themes."  # a work of the one is one of the other
    neon, space = (tags(genres=[genre], style_words=[]) for genre in ("cyberpunk", "science fiction"))
    assert said((8, neon), (2, PLAIN)) == "Cyberpunk character art."
    assert said((5, neon), (5, space)) == "Science fiction character art."
    found, _ = profiles.facts_of(body((5, neon), (5, space)))
    assert found["counts"]["genres"] == {"science fiction": 10, "cyberpunk": 5} and found["genres"] == [
        "science fiction"
    ]


def test_one_subject_alone_of_half_the_works_or_fewer_is_said_as_a_part_and_not_as_the_whole_card():
    found = held(profiles.facts_of(body((5, SCENERY), (5, NOTHING)))[0])

    assert profiles.template(found) == "Artwork, including environments."  # round 4: "Environment art." on 4 in 10
    assert (found["specialties"], found["hedged"]) == (["environment"], ["environment"])
    assert "true of only part of the works" in profiles.unsupported("Environment art.", found)
    assert said((6, SCENERY), (4, NOTHING)) == "Environment art."  # most of the works
    assert said((4, SCENERY), (4, PLAIN), (2, NOTHING)) == "Character and environment art."  # two halves
    fantasy = tags(subjects=["environment"], style_words=[]), tags(subjects=[], style_words=[])
    assert said((4, fantasy[0]), (6, fantasy[1])) == "Fantasy artwork, including environments."
    # with the subject in that clause there is room for one thing more, the one more works show
    seen = [("towers", "tower", "environment"), ("castles", "castle", "environment")]
    assert said((5, SCENERY), (5, NOTHING), proposed=seen, counts={"towers": 4, "castles": 5}) == (
        "Artwork, including environments and castles."
    )


@pytest.mark.parametrize(
    ("groups", "proposed", "counts", "sentence"),
    [
        ([(10, SCENERY)], [("castles", "castle", "environment")], {"castles": 9}, "Environment art, mostly castles."),
        (
            [(10, SCENERY)],
            [("castles", "castle", "environment")],
            {"castles": 4},
            "Environment art, including castles.",
        ),
        # what the sheet proposed and the artist also says is one claim
        (
            [(10, DRAGON, ["dragon"])],
            [("dragons", "dragon", "creature")],
            {"dragons": 10},
            "Creature art, mostly dragons.",
        ),
        # a thing of another kind than the card leads with is said beside it, not as a kind of it
        (
            [(10, PLAIN)],
            [("mechs", "mech", "vehicle or mech")],
            {"mechs": 5},
            "Character art, including mechs.",  # a vehicle may be a detail of any picture
        ),
        ([(10, PLAIN)], [("castles", "castle", "environment")], {"castles": 5}, "Character art and some castles."),
        # nor is a creature another kind than a character, where most works show it: "mostly monsters"
        ([(10, PLAIN)], [("monsters", "monster", "creature")], {"monsters": 8}, "Character art, mostly monsters."),
        ([(10, PLAIN)], [("monsters", "monster", "creature")], {"monsters": 5}, "Character art and some monsters."),
        # a place is never what character art "mostly" is, however many works show it behind the figures
        ([(10, PLAIN)], [("castles", "castle", "environment")], {"castles": 9}, "Character art and some castles."),
        (
            [(10, PLAIN, ["warrior"])],
            [("cathedrals", "cathedral", "environment")],
            {"cathedrals": 8, "warriors": 7},
            "Character art, mostly warriors, and some cathedrals.",
        ),
        # two things at most in a clause, those that the most works show
        (
            [(10, DRAGON, ["dragon", "monster"])],
            [("wolves", "wolf", "creature")],
            {"dragons": 5, "monsters": 4, "wolves": 5},
            "Creature art, including wolves and dragons.",
        ),
        (
            [(10, DRAGON, ["dragon", "monster"])],
            [("wolves", "wolf", "creature")],
            {"dragons": 9, "monsters": 7, "wolves": 8},
            "Creature art, mostly dragons and wolves.",
        ),
        # beside a kind of output: the noun of the card from most of the works, a part of it below that
        (
            [(10, DRAGON)],
            [("wolves", "wolf", "creature")],
            {"wolves": 8, "design": 5},
            "Creature art, mostly wolves, including designs.",
        ),
        (
            [(10, DRAGON)],
            [("wolves", "wolf", "creature")],
            {"wolves": 5, "design": 4},
            "Creature art, including wolves and designs.",
        ),
        (
            [(10, DRAGON)],
            [("wolves", "wolf", "creature")],
            {"wolves": 6, "design": 8},
            "Creature designs, mostly wolves.",
        ),
    ],
)
def test_how_what_the_looks_bear_out_is_worded(groups, proposed, counts, sentence):
    assert said(*groups, proposed=proposed, counts=counts) == sentence


def test_the_sheet_is_built_only_from_the_eligible_works_and_only_those_are_looked_at():
    results = [tags()] * 3 + [tags(explicit=True)] + [tags()] * 2 + [tags(not_artwork=True)] + [tags()] * 3
    artist(ALPHA, results, declared={9: "yes"})
    fake = Fake(propose=[("knights", "knight", "character")])

    assert run(fake) == {"profiled": 1, "with_recurring": 1}

    assert len(fake.sheets) == 3 and all(
        asked["works"] == [0, 1, 2, 4, 5, 7, 8] for asked in fake.sheets
    )  # never 3, 6, 9
    assert all("7 pictures, numbered 1 to 7" in asked["question"] for asked in fake.sheets)
    assert [look["work"] for look in fake.looks] == [0, 1, 2, 4, 5, 7, 8]
    assert profiles.profiles()[0]["pictures"] == 7


def test_without_a_sheet_no_work_is_looked_at_and_so_nothing_is_said():
    ids = artist(ALPHA, [PLAIN] * 6, own_tags=WARRIORS)
    for work_id in ids[:2]:
        (library.image_dir() / f"{work_id}.png").write_bytes(PNG + b"FAKE-NOT-A-PICTURE")
    fake = Fake(propose=[("knights", "knight", "character")])

    assert run(fake) == {"excluded_nothing_clear": 1}  # four pictures open: too few to say anything of the artist
    assert fake.requests == [] and profiles.profiles() == []


def test_a_sheet_request_goes_to_the_loopback_model_and_carries_the_pictures_and_nothing_of_the_artists_own():
    own = dict.fromkeys(range(6), ["fakeowntag", "warrior", "digitalart"])
    artist(ALPHA, [PLAIN] * 6, tagline="FAKE-TAGLINE", own_tags=own)
    fake = Fake(propose=[("knights", "knight", "character")])

    assert run(fake) == {"profiled": 1, "with_recurring": 1}

    assert all((request.url.host, request.url.port) == ("127.0.0.1", 11434) for request in fake.requests)
    for asked in fake.sheets:
        system, user = asked["body"]["messages"]
        assert asked["body"]["model"] == profiles.WRITER and set(user) == {"role", "content", "images"}
        assert len(user["images"]) == 1  # the one sheet, and no picture besides
        told = (system["content"] + user["content"]).lower()
        for kept_back in ("zz-fake", "fake-title", "fakeowntag", "fake-tagline", "digitalart", "00000000-0000"):
            assert kept_back not in told
    # nothing a card states is counted on the sheet any more: what recurs is asked, and the two for the push stage
    assert [asked["claims"] for asked in fake.sheets] == [[], *NEVER]
    assert profiles.profiles()[0]["template"] == "Character art, mostly knights and warriors."

    again = Fake(propose=[("dragons", "dragon", "creature")])
    assert run(again) == {"unchanged": 1}  # and a rerun asks nothing a second time
    assert again.requests == [] and again.sheets == [] and again.looks == []


def test_the_sexualised_or_gory_pictures_are_counted_and_handed_on_for_the_push_stage_to_weigh():
    artist(ALPHA, [PLAIN] * 10)
    artist(BRAVO, [PLAIN] * 10)
    seen = {profiles.UNSUITABLE: iter([7, 0]), profiles.SAME: iter([10, 2])}
    fake = Fake(count=lambda claim, pictures: next(seen[claim]) if claim in seen else pictures)

    assert run(fake) == {"profiled": 2}  # counted, stored, and nobody is left out here on the strength of it

    alpha, bravo = profiles.profiles()
    assert (alpha["pictures"], alpha["unsuitable_pictures"], alpha["same_subject_pictures"]) == (10, 7, 10)
    assert (bravo["pictures"], bravo["unsuitable_pictures"], bravo["same_subject_pictures"]) == (10, 0, 2)
    assert "sexualised" not in alpha["template"] + alpha["written"] + json.dumps(alpha["open_evidence"])
    question = calls(fake, profiles.UNSUITABLE)[0]["question"]
    for told in (  # round 4: fan service in 4 pictures of 10, and one fetish subject in 10 of 10, both under the bar
        "It means: the framing or the dress of a figure is sexualised, whether or not anything is explicit",
        "one fetish subject is the point of the picture",
        "strong gore",
        "in doubt, list it",
    ):
        assert told in question
    assert "the same part of a body in close-up" in calls(fake, profiles.SAME)[0]["question"]
    assert all(asked["prompt"] == profiles.COUNT_PROMPT for asked in fake.sheets if asked["claims"])


def test_a_count_on_the_sheet_is_of_what_is_shown_as_named_each_picture_once_and_no_whole_answer_counts_nothing():
    artist(ALPHA, [PLAIN] * 10)
    artist(BRAVO, [PLAIN] * 10)
    # the model lists some pictures under both; then, for the next artist, a number that is not on the sheet
    answers = iter(
        [
            {"closest": "FAKE-LOOK", "like_it_only": [1, 2, 3, 4, 5, 6, 7, 8], "shows": [1, 2, 3, 3, 9, 9, 10]},
            *[{"closest": "", "like_it_only": [], "shows": [1, 2, 99]}] * tagging.MAX_ATTEMPTS,
        ]
    )
    fake = Fake(count=lambda claim, pictures: next(answers) if claim == profiles.UNSUITABLE else 0)

    assert run(fake) == {"profiled": 2}
    alpha, bravo = profiles.profiles()
    assert (alpha["unsuitable_pictures"], bravo["unsuitable_pictures"]) == (2, 0)  # pictures 9 and 10, each once
    assert len(calls(fake, profiles.UNSUITABLE)) == 1 + tagging.MAX_ATTEMPTS  # asked again, and then left


# The print check: what a card may not read like, true or not. Round 4's critic, line by line.
STACKED = facts(["character"], genres=["fantasy", "anime style"], style_words=["painterly", "muted"])
TOWERS = facts(["environment"], recurring=[("massive stone towers", "massive stone tower", "environment")])
HEROES = facts(
    ["character"],
    genres=["fantasy"],
    hedged=["river heroes", "ruined castles"],
    recurring=[("river heroes", "river hero", "character"), ("ruined castles", "ruined castle", "environment")],
)
CASTLE = HEROES | {"recurring": ["ruined castles"], "hedged": ["ruined castles"]}


@pytest.mark.parametrize(
    ("found", "text", "why"),
    [
        (HEROES, "Fantasy character art, including fantasy river heroes.", '"fantasy" is said twice'),
        (HEROES, "Fantasy art and character art.", '"art" is said twice'),
        (TOWERS, "Environment, mostly massive stone towers.", "no kind of art"),
        (TOWERS, "Massive stone towers.", 'say it after "mostly"'),
        (TOWERS | {"mediums": ["3d render"]}, "3D environment art, mostly massive stone towers.", "after the kind"),
        (STACKED, "Painterly, muted anime fantasy character art.", "more than 3 words are stacked"),
        (STACKED, "Muted, painterly fantasy and anime character art.", "more than 3 words are stacked"),
        # castles are no kind of character art
        (HEROES, "Fantasy character art, including ruined castles.", 'say it after "some"'),
        (HEROES | {"specialties": ["environment"]}, "Fantasy environment art, including river heroes.", '"some"'),
    ],
)
def test_a_text_that_is_not_fit_to_print_is_refused_whatever_it_states(found, text, why):
    assert why in profiles.unsupported(text, found)


# Round 5's critic: four cards with a word twice, all of which this rule had passed. It counted each fact of the
# fixed lists as one thing under its own name: "fantasy" was no word of "dark fantasy", "designs" none of an
# artist's "adoptable designs", and "sci-fi" was not "science fiction". The facts are as that round had them.
@pytest.mark.parametrize(
    ("found", "text", "twice"),
    [
        (
            facts(
                ["character"],
                genres=["fantasy", "dark fantasy"],
                style_words=["painterly", "saturated"],
                hedged=["dark fantasy"],
            ),
            "Painterly, saturated fantasy character art with dark fantasy themes.",
            "fantasy",
        ),
        (
            facts(["creature", "character"], genres=["fantasy", "dark fantasy"], hedged=["dark fantasy"]),
            "Fantasy creature and character art with dark fantasy themes.",
            "fantasy",
        ),
        (
            facts(
                ["character"],
                genres=["dark fantasy"],
                style_words=["muted"],
                recurring=[("dark occult entities", "dark occult entity", "character")],
            ),
            "Muted dark fantasy character art, mostly dark occult entities.",
            "dark",
        ),
        (
            facts(
                ["creature"],
                genres=["fantasy"],
                style_words=["cel-shaded"],
                own_words=["adoptable designs"],
                hedged=["adoptable designs"],
                recurring=[("dark dragon designs", "dark dragon design", "creature")],
            )
            | {"mostly": []},  # the phrase led that card, flat
            "Dark dragon designs in a cel-shaded fantasy style, including adoptable designs.",
            "design",
        ),
        (
            facts(
                ["environment"],
                genres=["science fiction"],
                recurring=[("dark sci-fi cityscapes", "dark sci-fi cityscape", "environment")],
            ),
            "Science fiction environment art, mostly dark sci-fi cityscapes.",
            "science",
        ),
    ],
)
def test_no_word_twice_on_a_card_not_in_two_facts_not_as_a_short_form_and_not_as_a_plural(found, text, twice):
    assert profiles.unsupported(text, found) == f'"{twice}" is said twice: say each thing once'


@pytest.mark.parametrize(
    ("found", "text"),
    [
        (HEROES, "Fantasy character art, including river heroes, and some ruined castles."),
        (CASTLE, "Fantasy character art and some ruined castles."),
        (
            CASTLE | {"hedged": ["ruined castles", "fantasy"]},
            "Character art with fantasy themes, and some ruined castles.",  # the castles are no theme
        ),
        (
            HEROES | {"specialties": ["character", "environment"]},
            "Fantasy character and environment art, including river heroes and ruined castles.",
        ),
        (TOWERS, "Environment art, mostly massive stone towers."),
        (STACKED, "Anime fantasy character art in a painterly, muted style."),
        (
            STACKED | {"genres": ["fantasy", "horror", "anime style"], "hedged": ["anime style"]},
            "Fantasy and horror character art in a painterly, muted style, partly anime.",
        ),
        (
            STACKED | {"genres": ["fantasy", "horror", "anime style"], "mostly": ["anime style"]},
            "Fantasy and horror character art in a painterly, muted style, mainly anime.",
        ),
        (STACKED | {"mediums": ["3d render"]}, "Anime fantasy character art in 3D and a painterly, muted style."),
        # a medium that is a kind of art itself takes the place of that word, and line art has another for it
        (facts(["character"], ["pixel art"], style_words=["muted"]), "Muted character pixel art."),
        (facts(["character"], ["pixel art"], formats=["design"]), "Character designs in pixel art."),
        (facts(["character"], ["digital line art"]), "Character art in digital line work."),
        (facts(["character"], style_words=["lineart"]), "Linework character art."),
    ],
)
def test_the_template_writes_a_line_that_is_fit_to_print(found, text):
    assert profiles.template(found) == text
    assert profiles.unsupported(text, found) is None


def test_a_claim_that_keeps_the_template_from_being_fit_to_print_is_taken_away():
    artist(ALPHA, [PLAIN] * 10)
    seen = [("armoured knights", "armoured knight", "character"), ("armoured horses", "armoured horse", "creature")]
    fake = Fake(propose=seen, count={"armoured knights": 9, "armoured horses": 5})  # "armoured" would be said twice

    assert run(fake) == {"profiled": 1, "with_recurring": 1, "print_changed": 1}
    (profile,) = profiles.profiles()
    assert profile["template"] == "Character art, mostly armoured knights."
    assert (profile["recurring"], profile["hedged"]) == (["armoured knights"], [])
    assert profile["facts"]["sheet"]["removed"]["print_check"] == ["armoured horses"]  # the one fewer works show
    assert profile["facts"]["sheet"]["counts"]["armoured horses"] == 5  # the count is kept, with what was done


def test_a_line_the_model_calls_unfit_loses_the_claim_the_fewest_works_show_and_is_asked_about_once_more():
    artist(ALPHA, [DRAGON] * 10, tagline="Commissions open")
    seen = [("dinosaurs", "dinosaur", "creature"), ("ruined castles", "ruined castle", "environment")]
    fake = Fake(
        propose=seen, count={"dinosaurs": 9, "ruined castles": 5}, fit=lambda line: "no" if "castles" in line else "yes"
    )

    assert run(fake) == {"profiled": 1, "open_for_work": 1, "with_recurring": 1, "print_changed": 1}
    (profile,) = profiles.profiles()
    assert profile["template"] == "Creature art, mostly dinosaurs. Open for commissions."
    assert profile["written"] == "creature art, mostly dinosaurs. Open for commissions."
    assert profile["written_by_model"] is True
    assert profile["facts"]["sheet"]["removed"]["print_check"] == ["ruined castles"]
    # asked about the line alone: no picture, nothing of the artist's own, not the sentence about commissions
    assert fake.prints == [
        "creature art, mostly dinosaurs, and some ruined castles.",  # the model's sentence
        "Creature art, mostly dinosaurs, and some ruined castles.",  # the template says the same: the claim is at fault
        "creature art, mostly dinosaurs.",
    ]
    assert len(fake.chats) == 2  # the sentence is written again from the facts that are left


def test_a_line_the_model_calls_unfit_twice_gives_way_to_the_template():
    artist(ALPHA, [tags(subjects=["creature"], style_words=[])] * 10)
    # the genre is a word of the fixed lists and is not what goes: the thing from the sheet is
    fake = Fake(propose=[("dinosaurs", "dinosaur", "creature")], count={"dinosaurs": 9}, fit=lambda line: "no")

    assert run(fake) == {"profiled": 1, "written_fallback": 1, "print_changed": 1, "print_rejected": 1}
    (profile,) = profiles.profiles()
    assert profile["written"] == profile["template"] == "Fantasy creature art." and profile["written_by_model"] is False
    assert fake.prints == [
        "fantasy creature art, mostly dinosaurs.",
        "Fantasy creature art, mostly dinosaurs.",
        "fantasy creature art.",
    ]  # no more


def test_a_line_refused_twice_is_not_printed_as_it_was_the_next_least_supported_thing_goes_unasked():
    artist(ALPHA, [DRAGON] * 10)
    seen = [("dinosaurs", "dinosaur", "creature"), ("ruined castles", "ruined castle", "environment")]
    fake = Fake(propose=seen, count={"dinosaurs": 9, "ruined castles": 5}, fit=lambda line: "no")

    assert run(fake) == {"profiled": 1, "written_fallback": 1, "print_changed": 1, "print_rejected": 1}
    (profile,) = profiles.profiles()
    assert profile["written"] == profile["template"] == "Creature art."
    assert profile["facts"]["sheet"]["removed"]["print_check"] == ["ruined castles", "dinosaurs"]
    assert len(fake.prints) == 3  # the model's line, the template's, and the line without the castles


def test_the_kind_of_output_is_the_last_thing_the_print_check_takes_and_the_card_falls_back_on_the_subject():
    artist(ALPHA, [DRAGON] * 10)
    fake = Fake(
        propose=[("dinosaurs", "dinosaur", "creature")],
        count={"dinosaurs": 9},
        look={"kind": "design"},
        fit=lambda line: "no",
    )

    assert run(fake) == {"profiled": 1, "written_fallback": 1, "print_changed": 1, "print_rejected": 1}
    (profile,) = profiles.profiles()
    assert profile["template"] == "Creature art." and profile["formats"] == []
    assert profile["facts"]["sheet"]["removed"]["print_check"] == ["dinosaurs", "design"]  # 9 works, then 10


def test_when_only_the_models_wording_is_unfit_the_template_is_printed_and_no_claim_is_lost():
    artist(ALPHA, [DRAGON] * 10)
    fake = Fake(
        write="Art of creatures, mostly dinosaurs.",
        propose=[("dinosaurs", "dinosaur", "creature")],
        count={"dinosaurs": 9},
        fit=lambda line: "no" if line.startswith("Art of") else "yes",
    )

    assert run(fake) == {"profiled": 1, "with_recurring": 1, "written_fallback": 1, "print_rejected": 1}
    (profile,) = profiles.profiles()
    assert profile["written"] == profile["template"] == "Creature art, mostly dinosaurs."
    assert profile["recurring"] == ["dinosaurs"]
    assert fake.prints == ["Art of creatures, mostly dinosaurs.", "Creature art, mostly dinosaurs."]
    assert "print_check" not in profile["facts"]["sheet"]["removed"]


def test_no_whole_answer_about_a_line_is_no_objection_to_it():
    artist(ALPHA, [DRAGON] * 10)
    fake = Fake(fit=lambda line: {"fit": "perhaps"})

    assert run(fake) == {"profiled": 1}
    assert len(fake.prints) == tagging.MAX_ATTEMPTS and profiles.profiles()[0]["written_by_model"] is True
    for told in ("natural, grammatical line", "no noun for the art", "nouns piled on nouns", "one thing said twice"):
        assert told in profiles.PRINT_PROMPT


def test_one_way_with_commas_and_one_plain_word_for_an_either_or_name():
    found = facts(
        ["vehicle or mech", "prop or weapon"],
        genres=["fantasy", "science fiction"],
        style_words=["painterly", "muted"],
        formats=["design"],
        hedged=["science fiction", "ruined castles"],
        recurring=[("ruined castles", "ruined castle", "environment")],
    )

    assert profiles.template(found) == (
        "Painterly, muted fantasy vehicle and prop designs with science fiction themes, and some ruined castles."
    )
    said_first = [forms[0] for forms in (profiles.SUBJECT_FORMS | profiles.KIND_FORMS).values()]
    assert not [word for word in said_first if " or " in word]  # no either-or name is said as it is spelled
    # and what the model writes is given the same commas before it is checked and kept
    assert profiles._tidy("Painterly muted fantasy art, with dark fantasy themes, including castles.") == (
        "Painterly, muted fantasy art with dark fantasy themes, including castles."
    )


@pytest.mark.parametrize(
    "text",
    [
        "Painterly fantasy character.",  # the kind of art is missing after it
        "Fantasy character art in traditional media with painterly.",  # ends on a style word
        "Painterly fantasy character art and.",
        "Painterly fantasy character art in.",
        "Painterly fantasy character art, including.",
        "Painterly fantasy character art,.",
    ],
)
def test_a_written_text_that_breaks_off_is_refused_and_the_template_stands_in(text):
    assert "breaks off" in profiles.unsupported(text, PAINTER)
    artist(ALPHA, [tags("traditional")] * 6)

    assert run(Fake(write=text)) == {"profiled": 1, "written_fallback": 1}
    (profile,) = profiles.profiles()
    assert profile["written"] == profile["template"] == "Painterly fantasy character art in traditional media."


# A row is tied to the readings it was made from. Each of these is a sequence: profile, change, run again.

THUMB = "https://images-wixmp-zzfake.wixmp.com/f/FAKE-THUMB.jpg"  # what artist() stores every work with
ANOTHER = "https://images-wixmp-zzfake.wixmp.com/f/FAKE-ANOTHER-FILE.jpg"  # the file an artist puts in its place


def collected_again(handle, n, *, title=None, thumb=THUMB, image=None):
    """The collect stage's next turn for one work of artist()'s: with another title, or with a thumbnail that shows
    another file; and with `image`, the collector has fetched that file and says so."""
    work_id = f"00000000-0000-4000-A000-{ {ALPHA: 1000, BRAVO: 2000, CHARLIE: 3000}[handle] + n:012d}"
    if image is not None:
        library.save_image(work_id, "image/png", image)
    row = {
        "work_id": work_id,
        "page_url": f"https://www.deviantart.com/{handle}/art/FAKE-TITLE-{n}",
        "title": title or f"FAKE-TITLE-{n}",
        "tags": ["faketag"],
        "thumb_url": thumb,
        "published": str(int(RECENT.timestamp())),
        "fetched": image is not None,
    }
    library.store_artist(
        f"da:{handle}", user_id=None, tagline=None, specialty=None, level="Professional", website=None, works=[row]
    )


def test_an_artist_whose_work_was_read_from_an_image_they_replaced_waits_and_the_row_is_made_again():
    ids = artist(ALPHA, [tags()] * 6)
    assert run(Fake())["profiled"] == 1
    kept, key = table(), f"da:{ALPHA}"
    assert profiles.current() == {key} and profiles.profiles()[0]["current"] is True

    collected_again(ALPHA, 2, thumb=ANOTHER)  # the thumbnail shows another file: the tags are of the old image
    quiet = Fake()
    assert run(quiet) == {"waiting_for_tags": 1} and quiet.requests == [] and table() == kept  # the row is kept,
    assert profiles.current() == set() and profiles.profiles()[0]["current"] is False  # and is not one to publish
    assert profiles.sample(5, 1) == []  # nor one to hold against the pictures as they are now
    collected_again(ALPHA, 2, thumb=ANOTHER, image=picture(9))  # fetched, and not read yet
    assert run(quiet) == {"waiting_for_tags": 1} and profiles.current() == set()

    tag(ids[2], tags())  # the tagging stage reads the new image, and finds the very same tags
    assert profiles.current() == set()  # the same tags of another picture: the sheet was of the old one
    again = Fake()
    assert run(again)["profiled"] == 1 and 9 in again.sheets[0]["works"]  # made again, from the pictures as they are
    assert profiles.current() == {key} and table()[0][1] != kept[0][1]
    assert run(quiet) == {"unchanged": 1} and quiet.requests == []


def test_a_row_is_not_current_while_a_work_it_counted_has_words_nobody_read_or_words_that_say_otherwise():
    ids = artist(ALPHA, [tags()] * 6)
    assert run(Fake())["profiled"] == 1
    kept, key = table(), f"da:{ALPHA}"

    collected_again(ALPHA, 2, title="FAKE-TITLE-2 CHANGED")  # what the old title was read to declare is stale
    quiet = Fake()
    assert run(quiet) == {"waiting_for_tags": 1} and table() == kept and profiles.current() == set()
    tag(ids[2], None, "no", words_only=True)  # read again, and it declares what it declared: nothing
    assert profiles.current() == {key}  # so the row is current again as it stands, and nothing is asked
    assert run(quiet) == {"unchanged": 1} and quiet.requests == [] and table() == kept

    collected_again(ALPHA, 2, title="FAKE-TITLE-2 made with FAKE-GENERATOR")
    tag(ids[2], None, "yes", words_only=True)  # read again: this time the words declare the work AI-made
    assert profiles.current() == set()  # the row counted a work that does not count any more
    assert run(Fake())["profiled"] == 1 and profiles.current() == {key}
    assert profiles.profiles()[0]["facts"]["counted"] == sorted(set(ids) - {ids[2]})


def test_a_work_collected_after_the_row_was_made_leaves_the_row_current():
    """The row rests on the works it counted. One it never counted cannot make it out of date."""
    ids = artist(ALPHA, [tags()] * 6)
    assert run(Fake())["profiled"] == 1
    kept = table()
    collected_again(ALPHA, 6, image=picture(6))  # a seventh work, with its image: not tagged yet
    assert library.work_ids(f"da:{ALPHA}") > set(ids) and run(Fake()) == {"waiting_for_tags": 1}
    assert table() == kept and profiles.current() == {f"da:{ALPHA}"}


def test_a_store_made_before_readings_carried_fingerprints_stops_the_stage_before_a_model_is_asked():
    artist(ALPHA, [tags()] * 6)
    assert run(Fake())["profiled"] == 1
    with closing(sqlite3.connect(config.LOCAL_DB)) as conn:
        conn.execute("PRAGMA user_version = 0")  # as every store was before the migration
    before, fake = config.LOCAL_DB.read_bytes(), Fake()
    calls = [lambda: run(fake), profiles.profiles, profiles.excluded, profiles.current, lambda: profiles.sample(5, 1)]
    calls += [lambda: profiles.main(["--max-artists", "5"], httpx.MockTransport(fake))]
    for call in calls:
        with pytest.raises(SystemExit) as stopped:
            call()
        assert "uv run python -m pipeline.tagging --migrate --backup" in str(stopped.value)
    assert fake.requests == [] and config.LOCAL_DB.read_bytes() == before
