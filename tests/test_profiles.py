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
    r'"with_recurring": \d+, "with_defining": \d+, "subjects_removed": \d+, "phrases_removed": \d+, '
    r'"own_words_removed": \d+, "formats_removed": \d+, '
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
        tagging.results()  # so that its tables exist
        row = (f"da:{handle}", profiles.TAGGER, tagging.CONTACT_PROMPT_VERSION, contact, "2026-01-01T00:00:00+00:00")
        with closing(sqlite3.connect(config.LOCAL_DB)) as conn, conn:
            conn.execute("INSERT OR REPLACE INTO artist_contact VALUES (?, ?, ?, ?, ?)", row)
    return ids


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
    """
    tagging.results()  # so that its tables exist
    at = "2026-01-01T00:00:00+00:00"
    with closing(sqlite3.connect(config.LOCAL_DB)) as conn, conn:
        row = (work_id, result_set, tagging.PROMPT_VERSION, json.dumps(result) if result else None, at)
        if not words_only:
            conn.execute("INSERT OR REPLACE INTO work_tags VALUES (?, ?, ?, ?, ?)", row)
        # A format is in the result as tagging.results() gives it. Where the tagging stage keeps formats in a
        # table of their own, it is read from there, so it goes there too.
        if (
            result
            and "formats" in result
            and conn.execute("SELECT 1 FROM sqlite_master WHERE name = 'work_format'").fetchone()
        ):
            sorted_as = json.dumps({"formats": result["formats"], "rating": result.get("rating", "safe")})
            row = (work_id, profiles.TAGGER, getattr(tagging, "FORMAT_VERSION", 1), sorted_as, at)
            conn.execute("INSERT OR REPLACE INTO work_format VALUES (?, ?, ?, ?, ?)", row)
        if declared:
            row = (work_id, profiles.TAGGER, tagging.AI_PROMPT_VERSION, declared, at)
            conn.execute("INSERT OR REPLACE INTO work_ai_declared VALUES (?, ?, ?, ?, ?)", row)


def vector(text, scale=3.0):
    """1,024 invented numbers that depend on the text, and are not of unit length."""
    numbers = random.Random(text)
    return [scale * numbers.uniform(-1, 1) for _ in range(profiles.DIMENSIONS)]


def sentence_for(question):
    """A sentence that states the facts a question to the writer lists, each as it may be stated, and nothing else."""
    lines = dict(line.split(": ", 1) for line in question.split("\n\n")[0].splitlines()[1:])
    part = lines.pop("only part of the works", "")
    flat = [term for terms in lines.values() for term in terms.split(", ")]
    return ", ".join([f"{' and '.join(flat)} art".strip(), *filter(None, part.split("; "))]) + "."


class Fake:
    """Ollama's vision model, writer (/api/chat) and embedder (/api/embed), and Workers AI, told apart by host,
    path and system prompt.

    write: what the writer says. None for a sentence that states the facts it was given and nothing else, a str
           for that sentence every time, or a function (question, attempt from 1) -> a str for the sentence, a
           dict for the model's whole JSON, a Response, or an exception to raise.
    propose: what the vision model says recurs on a sheet: a list of (many, one, kind), a function (the numbers
           of the works on the sheet) -> such a list, or anything else to send as its whole answer.
    count: in how many pictures it finds a claim: {claim: how many, or which numbers}; a claim not named there
           it finds in every picture. Or a function (claim, pictures) -> the same, or a whole answer to send.
    embed: a function (text) -> the vector Ollama gives. cloud: the same for Workers AI, or a Response.
    """

    def __init__(self, write=None, embed=vector, cloud=vector, propose=(), count=None, define=None):
        self.write, self.embed, self.cloud, self.propose, self.count = write, embed, cloud, propose, count
        self.define = define  # the defining thing it names: (phrase, subject or look, kind), or None for none
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
        if prompt in (profiles.PROPOSE_PROMPT, profiles.COUNT_PROMPT):
            (sheet,) = body["messages"][-1]["images"]
            works = works_on(sheet)
            claims = [line.removeprefix("Claim: ") for line in question.splitlines() if line.startswith("Claim: ")]
            asked = {"prompt": prompt, "works": works, "question": question, "claims": claims, "body": body}
            self.sheets.append(asked)
            if prompt == profiles.PROPOSE_PROMPT:
                said = self.propose(works) if callable(self.propose) else self.propose
                if isinstance(said, list | tuple):
                    first = dict(zip(("phrase", "is", "kind"), self.define or ("", "none", "character"), strict=True))
                    recurring = [dict(zip(("many", "one", "kind"), item, strict=True)) for item in said]
                    said = {"defining": first, "recurring": recurring}
                return self.say(said)
            (claim,) = claims  # one call a claim
            usual = 0 if claim == profiles.UNSUITABLE else len(works)  # every picture shows every claim, but that one
            found = self.count(claim, len(works)) if callable(self.count) else (self.count or {}).get(claim, usual)
            if not isinstance(found, int | list) or isinstance(found, bool):
                return self.say(found)
            shows = list(range(1, found + 1)) if isinstance(found, int) else found
            return self.say({"closest": "FAKE-LOOK", "like_it_only": [], "shows": shows})
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


def facts(specialties=(), mediums=(), genres=(), style_words=(), own_words=(), formats=(), hedged=(), recurring=()):
    """The part of an artist's facts that a profile is written from. hedged: those that hold for part of the
    works. recurring: (many, one, kind) for each subject the sheet pass kept."""
    kept = {
        many: {"many": many, "one": one, "kind": kind, "is": "subject", "count": 9} for many, one, kind in recurring
    }
    return {
        "recurring": list(kept),
        "specialties": list(specialties),
        "mediums": list(mediums),
        "genres": list(genres),
        "style_words": list(style_words),
        "own_words": list(own_words),
        "formats": list(formats),
        "hedged": list(hedged),
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


def held(found, proposed=(), counts=None, defining=None):
    """The facts as the sheet pass leaves them. The sheet shows every counted work, and what the tags and the
    artist's own words say of a work is plain to see on it, but for the claims `counts` gives another count.
    defining: (phrase, subject or look, kind), when the model names a defining thing."""
    claims = profiles._claims(found, [])
    tagged = found["counts"]["subjects"] | found["counts"]["own_words"] | found["counts"]["formats"]
    tagged |= found["counts"]["genres"]
    agreed = {profiles._spellings(term)[0]: tagged[term] for term in tagged if profiles._spellings(term)[0] in claims}
    recurring = [dict(zip(("many", "one", "kind"), item, strict=True)) | {"is": "subject"} for item in proposed]
    if defining:
        phrase, what, kind = defining
        recurring.insert(0, {"many": phrase, "one": phrase, "kind": kind, "is": what, "defining": "yes"})
    return profiles.specifics(found, recurring, agreed | (counts or {}), found["eligible"])


def said(*groups, proposed=(), counts=None, defining=None):
    """The template's sentence for such a body of work, once the sheet pass has held it to its counts."""
    found, reason = profiles.facts_of(body(*groups))
    assert reason is None
    found = held(found, proposed, counts, defining)
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
        "formats": {},
        "own_words": {},
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
        # a genre is hedged from 4 in 10 and stated flat from 7 in 10
        (10, 3, "world", "no"),
        (10, 4, "world", "hedged"),
        (10, 6, "world", "hedged"),
        (10, 7, "world", "flat"),
        (5, 3, "world", "no"),
        (5, 4, "world", "flat"),
        # a drawing manner is hedged from 4 in 10 and leads when most works have it
        (10, 3, "manner", "no"),
        (10, 4, "manner", "hedged"),
        (10, 5, "manner", "hedged"),
        (10, 6, "manner", "flat"),
        (7, 3, "manner", "no"),
        (7, 4, "manner", "flat"),
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
        ([(10, tags("pixel art", genres=[], style_words=[]))], "Character art in pixel art."),
        # line art is said once, as the medium
        ([(10, tags("digital line art", genres=[], style_words=["lineart"]))], "Character art in digital line art."),
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
        ([(6, tags(style_words=[])), (4, PLAIN)], "Character art with fantasy themes."),
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
        ([(6, tags(genres=["anime style"], style_words=[])), (4, PLAIN)], "Anime character art."),
        ([(10, tags(genres=["fantasy", "realistic"], style_words=[]))], "Realistic fantasy character art."),
        (
            [(9, tags(genres=["stylized cartoon"], style_words=["muted"])), (1, PLAIN)],
            "Muted stylized cartoon character art.",
        ),
        ([(4, tags(genres=["anime style"], style_words=[])), (6, PLAIN)], "Character art, partly in an anime style."),
        (
            [(5, tags(genres=["stylized cartoon"], style_words=[])), (5, PLAIN)],
            "Character art, partly in a stylized cartoon style.",
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
        # most of the works, by the tags and then by the count of the sheet: "mostly" at 7 in 10, "including" below
        ([(7, DRAGON, ["dragon"]), (3, DRAGON)], "Creature art, mostly dragons."),
        ([(6, DRAGON, ["dragon"]), (4, DRAGON)], "Creature art, including dragons."),
        (
            [(2, DRAGON, ["Dragon"]), (2, DRAGON, ["dragons"]), (3, DRAGON, ["DRAGON", "faketag"]), (3, DRAGON)],
            "Creature art, mostly dragons.",
        ),
        ([(5, DRAGON, ["dragon"]), (5, DRAGON)], "Creature art."),  # half the works do not make a word theirs
        ([(4, DRAGON, ["dragon"]), (1, DRAGON)], "Creature art, mostly dragons."),
        # the artist's word for the kind of art itself takes its place, and is not said twice
        ([(10, PLAIN, ["characterdesign", "warrior"])], "Character designs, mostly warriors."),
        ([(10, PLAIN, ["character_design"])], "Character designs."),
        ([(6, PLAIN, ["characterconcept"]), (4, PLAIN, ["knight"])], "Character art, including designs."),
        ([(6, PLAIN, ["characterdesign"]), (4, SCENERY)], "Character and environment art, including designs."),
        ([(10, SCENERY, ["landscape", "environmentdesign"])], "Environment designs, mostly landscapes."),
        (
            [(10, tags(subjects=["vehicle or mech"], genres=[], style_words=[]), ["mecha"])],
            "Vehicle art, mostly mechs.",
        ),
        # three words most works carry: the two that most works carry, in the order of the list when it is a tie
        ([(10, PLAIN, ["elf", "mage", "portrait"])], "Character art, mostly elves and mages."),
        (
            [(6, PLAIN, ["elf", "mage", "portrait"]), (4, PLAIN, ["portrait"])],
            "Character art, mostly portraits, including elves.",
        ),
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
        *(profiles.FORMAT_FORMS, profiles.OWN_FORMS),
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
    for word, (under, spellings) in profiles.OWN_WORDS.items():
        assert under and under <= set(profiles.SUBJECT_FORMS), word
        assert all(re.fullmatch("[a-z]+", tag) for tag in spellings), word


SHEET, BUST = "character sheet or turnaround", "portrait or bust"
FOR_SALE, KEYFRAME = "adoptable or design-for-sale sheet", "environment keyframe or scene"


@pytest.mark.parametrize(
    ("groups", "sentence", "formats", "hedged"),
    [
        # the tagging stage has not sorted the works by format: there is no such fact
        ([(10, PLAIN)], "Character art.", [], []),
        ([(10, PLAIN | {"formats": None})], "Character art.", [], []),
        ([(10, PLAIN | {"formats": []})], "Character art.", [], []),
        # 7 in 10: what the artist mostly makes. 3 in 10: hedged. Fewer: nothing. And "character" is said once
        (
            [(7, PLAIN | {"formats": [SHEET]}), (3, PLAIN | {"formats": []})],
            "Character art, mostly design sheets.",
            [SHEET],
            [],
        ),
        (
            [(6, PLAIN | {"formats": [SHEET]}), (4, PLAIN | {"formats": []})],
            "Character art, including design sheets.",
            [SHEET],
            [SHEET],
        ),
        (
            [(4, PLAIN | {"formats": [SHEET]}), (6, PLAIN | {"formats": []})],
            "Character art, including design sheets.",
            [SHEET],
            [SHEET],
        ),
        ([(2, PLAIN | {"formats": [SHEET]}), (8, PLAIN | {"formats": []})], "Character art.", [], []),
        (
            [(7, PLAIN | {"formats": [SHEET, BUST]}), (3, PLAIN | {"formats": [BUST]})],
            "Character art, mostly portraits and design sheets.",
            [BUST, SHEET],
            [],
        ),
        (
            [(6, PLAIN | {"formats": [SHEET]}), (4, PLAIN | {"formats": ["sprite sheet"]})],
            "Character art, including design sheets and sprite sheets.",
            [SHEET, "sprite sheet"],
            [SHEET, "sprite sheet"],
        ),
        # half the works are not sorted yet: the counts would be short, so nothing is said of formats
        ([(5, PLAIN | {"formats": [SHEET]}), (5, PLAIN | {"formats": None})], "Character art.", [], []),
        # a format the tagging stage adds to its list is said as it spells it, if that is plain words
        (
            [(4, PLAIN | {"formats": ["lore page"]}), (6, PLAIN | {"formats": []})],
            "Character art, including lore page work.",
            ["lore page"],
            ["lore page"],
        ),
        (
            [(10, PLAIN | {"formats": ["x/y; DROP", "Stunning!", "", "a b c d e f g", 7, None]})],
            "Character art.",
            [],
            [],
        ),
        # and a value that is on nobody's list is not a format, however plainly it is spelled
        ([(10, PLAIN | {"formats": ["stunning art", "character", "fantasy"]})], "Character art.", [], []),
        # the artist's "portrait" and the format say the same thing: it is said once
        ([(10, PLAIN | {"formats": [BUST]}, ["portrait"])], "Character art, mostly portraits.", [BUST], []),
        # and where the artist has a word of their own for a format, that is the one said
        (
            [(10, PLAIN | {"formats": [FOR_SALE]})],
            "Character art, mostly designs for sale.",
            [FOR_SALE],
            [],
        ),
        ([(10, PLAIN | {"formats": [FOR_SALE]}, ["adoptable"])], "Character art, mostly adoptable designs.", [], []),
        (
            [(10, PLAIN | {"formats": [FOR_SALE, SHEET]}, ["adopts"])],
            "Character art, mostly adoptable designs and design sheets.",
            [SHEET],
            [],
        ),
        # a format that names a subject counts only on the works where the verifier saw that subject
        ([(10, PLAIN | {"formats": [KEYFRAME, "creature sheet"]})], "Character art.", [], []),
        (
            [(4, SCENERY | {"formats": [KEYFRAME]}), (6, PLAIN | {"formats": [KEYFRAME]})],
            "Character and environment art, including finished scenes.",
            [KEYFRAME],
            [KEYFRAME],
        ),
        # no clause says again what the one before it said
        (
            [(10, SCENERY | {"formats": [KEYFRAME]}, ["environmentdesign"])],
            "Environment designs, mostly finished scenes.",
            [KEYFRAME],
            [],
        ),
        (
            [(10, DRAGON | {"formats": ["creature sheet"]}, ["creaturedesign"])],
            "Creature designs, mostly design sheets.",
            ["creature sheet"],
            [],
        ),
    ],
)
def test_a_format_is_read_when_the_tagging_stage_gives_one_and_nothing_breaks_when_it_does_not(
    groups, sentence, formats, hedged, monkeypatch
):
    listed = ["lore page", "x/y; DROP", "Stunning!", "", "a b c d e f g"]
    monkeypatch.setattr(tagging, "FORMATS", dict.fromkeys(listed, "FAKE-MEANING"), raising=False)
    found = held(profiles.facts_of(body(*groups))[0])

    assert (found["formats"], [value for value in found["hedged"] if value in formats]) == (formats, hedged)
    assert said(*groups) == sentence


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
        "formats": {},
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
    assert found["hedged"] == ["fantasy"]  # 6 in 10; cyberpunk has 10, and most works are realistic
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


# Fantasy, and dragons, hold for the body of work. Science fiction, the anime manner and creature sheets hold for part.
SPLIT = facts(
    ["character", "creature"],
    genres=["fantasy", "science fiction", "anime style"],
    own_words=["dragons"],
    formats=["creature sheet"],
    hedged=["science fiction", "anime style", "creature sheet"],
)


def test_the_template_leads_with_what_holds_for_the_body_of_work_and_hedges_the_rest():
    assert profiles.template(SPLIT) == (
        "Fantasy character and creature art with science fiction themes, mostly dragons, partly in an anime style, "
        "including design sheets."
    )
    assert profiles.unsupported(profiles.template(SPLIT), SPLIT) is None


@pytest.mark.parametrize(
    "text",
    [
        "Fantasy character and creature art with science fiction themes.",
        "Fantasy dragon art, partly anime, including creature sheets.",
        "Fantasy character art, mostly dragons, also some science fiction.",
        "Character and creature art with fantasy and science fiction themes.",
        "Fantasy creature art, mostly dragons, including creature sheets and some anime work.",
        "Dragons and characters, partly in an anime style.",
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
        "Fantasy creature sheets.",
        "Fantasy creature art, mostly creature sheets.",
        "Fantasy creature art, mostly science fiction.",
        "Mostly fantasy creature art.",
    ],
)
def test_a_fact_of_part_of_the_works_may_not_be_stated_flat(text):
    assert isinstance(profiles.unsupported(text, SPLIT), str)
    # and the same sentence is fine for an artist of whose works all of it holds
    whole = SPLIT | {"hedged": []}
    assert profiles.unsupported(text, whole) is None or "mostly" in text.lower()


def test_mostly_may_stand_only_before_what_most_of_the_works_are():
    sheets = facts(["creature"], genres=["fantasy"], own_words=["dragons"], formats=["creature sheet"])
    for text in ("Fantasy creature art, mostly dragons.", "Fantasy creature art, mostly creature sheets and dragons."):
        assert profiles.unsupported(text, sheets) is None
    assert profiles.unsupported("Fantasy creature sheets, mostly dragons.", sheets) is None
    for text in ("Mostly fantasy creature art.", "Creature art, mostly fantasy.", "Creature art, mostly creatures."):
        assert profiles.unsupported(text, sheets) is not None


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
    manners = chooser.sample(list(profiles.MANNER_FORMS), chooser.randint(0, 1))
    formats = chooser.sample([*profiles.FORMAT_FORMS, "lore page"], chooser.randint(0, profiles.AT_MOST["formats"]))
    own = chooser.sample(list(profiles.OWN_FORMS), chooser.randint(0, profiles.AT_MOST["own_words"]))
    # as facts_of() has it: what a format says already is not said again in the artist's word for it; and here
    # no two things that are shortened to the same word ("designs") are said of one artist
    own = [word for word in own if word not in {profiles._spellings(value)[0] for value in formats}]
    short = [profiles.SHORT.get(profiles._spellings(term)[0]) for term in (*own, *formats)]
    if len(set(filter(None, short))) < len(list(filter(None, short))):
        own = []
    seen = chooser.sample(SEEN, chooser.randint(0, 2))
    specific = [*formats, *own, *(many for many, _, _ in seen)]
    part = [*worlds, *manners, *chooser.sample(specific, min(len(specific), profiles.AT_MOST["hedged"]))]
    return facts(
        chooser.sample(list(profiles.SUBJECT_FORMS), chooser.randint(0, profiles.AT_MOST["specialties"])),
        chooser.sample(list(profiles.MEDIUM_FORMS), chooser.randint(0, 1)),
        worlds + manners,
        chooser.sample(list(profiles.STYLE_FORMS), chooser.randint(0, profiles.AT_MOST["style_words"])),
        own,
        formats,
        chooser.sample(part, chooser.randint(0, len(part))),
        seen,
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
        head, *later = re.split(r", (?:mostly|with|partly|including) ", text.lower())
        for clause in later:  # no clause begins with a subject word that the head said
            assert clause.split()[0] not in profiles.SUBJECT_FORMS or clause.split()[0] not in head.split(), text
        # a recurring character or creature that most pictures show stands in the place of that word
        for many in found["recurring"]:
            kind = found["sheet"]["recurring"][many]["kind"]
            if many not in found["hedged"] and kind in ("character", "creature"):
                assert kind not in re.findall(r"[a-z-]+", head), text
        said_as_designs = {
            subject
            for subject in found["specialties"]
            if f"{profiles._spellings(subject)[0]} designs" in found["own_words"]
        }
        stood_for = {
            found["sheet"]["recurring"][many]["kind"] for many in found["recurring"] if many not in found["hedged"]
        }
        for term in stated:  # each fact is in it, in one of its spellings, unless a recurring subject stands for it
            if term not in stood_for & {"character", "creature"}:
                assert any(form.lower() in text.lower() for form in profiles._forms(found, term)), (term, text)
        assert "digital painting" not in text
        for term in stated:  # with any one fact taken away it no longer passes
            if term in said_as_designs or term in stood_for & {"character", "creature"}:
                continue  # the sentence says the artist's word, or the recurring subject, in the place of this one
            without = {
                name: [t for t in terms if t != term] if isinstance(terms, list) else terms
                for name, terms in found.items()
            }
            assert profiles.unsupported(text, without) is not None, (term, text)
        for term in stated:  # nor when a fact it states flat is one that holds for part of the works only
            if term in (*profiles.GENRE_FORMS, *found["formats"]) and term not in found["hedged"]:
                assert profiles.unsupported(text, found | {"hedged": [*found["hedged"], term]}) is not None, (
                    term,
                    text,
                )


def test_the_longest_sentence_the_template_can_write_fits_the_limit():
    def longest(table, count):
        return sorted(table, key=lambda term: -len(profiles._spellings(term)[0]))[:count]

    most = profiles.AT_MOST
    worlds, manners = longest(profiles.WORLD_FORMS, most["worlds"]), longest(profiles.MANNER_FORMS, most["manners"])
    formats, own = longest(profiles.FORMAT_FORMS, most["formats"]), longest(profiles.OWN_FORMS, most["own_words"])
    seen = [
        ("a" * 12 + f" {kind[:5]} things", "a" * 12 + f" {kind[:5]} thing", kind)
        for kind in ("character", "creature", "environment")
    ]
    for hedged in ([], worlds, manners, [*formats, *own][: most["hedged"]], [*worlds, *manners, *formats]):
        found = facts(
            longest(profiles.SUBJECT_FORMS, most["specialties"]),
            longest([term for term in profiles.MEDIUM_FORMS if term != profiles.COMMON_MEDIUM], 1),
            worlds + manners,
            longest(profiles.STYLE_FORMS, most["style_words"]),
            own,
            formats,
            hedged,
            seen,
        )
        assert len(profiles.template(found)) <= profiles.MAX_PROFILE, (
            len(profiles.template(found)),
            profiles.template(found),
        )
        assert profiles.unsupported(profiles.template(found), found) is None


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
    fake = Fake(write=lambda question, attempt: answers[attempt - 1], count={"anime": 5})  # the sheet agrees: part

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
        "content": "Facts\nkinds of art: character\nmost of the works: warriors\nthemes: fantasy\n"
        "only part of the works: with science fiction themes",
    }
    sent = b"".join(request.content for request in fake.requests).decode().lower()
    for kept_back in ("zz-fake", "fake-title", "fakeowntag", "fake-tagline", "fake-description", "00000000-0000"):
        assert kept_back not in sent
    # below its threshold, or the medium everyone has, or a tag of the artist's that names nothing
    for kept_back in ("creature", "traditional", "horror", "sketchy", "painterly", "cyberpunk", "vehicle", "digital"):
        assert kept_back not in asked["content"]
    assert "commission" not in asked["content"] and "images" not in asked


def test_the_writer_is_told_the_artists_word_for_a_kind_of_art_in_the_place_of_the_subject():
    own = dict.fromkeys(range(6), ["characterdesign", "knight"])
    artist(ALPHA, [tags(genres=[], style_words=[])] * 6, own_tags=own)
    fake = Fake()

    assert run(fake) == {"profiled": 1}

    (chat,) = fake.chats
    assert chat["messages"][-1]["content"] == "Facts\nkinds of art: character designs\nmost of the works: knights"
    (profile,) = profiles.profiles()
    assert profile["template"] == "Character designs, mostly knights."  # not "character art, mostly character designs"
    assert profile["written"] == "character designs and knights art."
    assert (profile["specialties"], profile["own_words"]) == (["character"], ["knights", "character designs"])


def test_the_prompt_names_every_plain_word_and_forbids_what_the_check_refuses():
    for word in profiles.GLUE:
        assert f" {word}" in profiles.WRITE_PROMPT
    for forbidden in ("numbers", "names", "praise", "experience", "clients", "software", "availability"):
        assert forbidden in profiles.WRITE_PROMPT
    for hedges in profiles._HEDGES.values():  # a hedge is a plain word, and "mostly" is not
        assert set(hedges) <= profiles.GLUE
    assert "mostly" not in profiles.GLUE
    # no spelling of a fact can be made of plain words alone
    for forms in profiles.FORMS.values():
        for form in forms:
            assert not set(form.lower().split()) <= profiles.GLUE, form


# 5. The embedding.


def test_the_vector_is_1024_numbers_of_unit_length_made_from_the_template_and_every_fact_by_name():
    sheets = tags(genres=["fantasy", "anime style"]) | {"formats": ["character sheet or turnaround"], "rating": "safe"}
    artist(ALPHA, [sheets] * 6, tagline="Commissions open", own_tags=dict.fromkeys(range(6), ["warrior"]))
    # whatever the writer makes of the facts, the vector is of the same words
    fake = Fake(write="Anime fantasy knights with a painterly look.", propose=[("knights", "knight", "character")])

    assert run(fake) == {"profiled": 1, "open_for_work": 1, "with_recurring": 1}

    (profile,) = profiles.profiles()
    text = (
        "Painterly anime fantasy knight art, mostly character sheets. Open for commissions.\n"
        "Recurring subjects: knights.\nSpecialties: character.\nMediums: digital painting.\n"
        "Genres: fantasy, anime style.\nFormats: character sheet or turnaround.\nStyle: painterly."
    )
    assert profile["written"] == "Anime fantasy knights with a painterly look. Open for commissions."
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
        ("127.0.0.1", 11434, "/api/chat"),  # the count of the subject
        ("127.0.0.1", 11434, "/api/chat"),  # the count of what is unsuitable
        ("127.0.0.1", 11434, "/api/chat"),  # the sentence
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


def test_a_profile_does_not_outlive_the_works_it_was_made_from():
    artist(ALPHA, [tags()] * 6)
    assert run(Fake()) == {"profiled": 1}

    artist(ALPHA, [tags()] * 6 + [None])  # collected again with a new work, which has no tags yet
    assert run(Fake()) == {"waiting_for_tags": 1}
    assert table() == []


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
        "with_recurring": 0,
        "with_defining": 0,
        "subjects_removed": 0,
        "phrases_removed": 0,
        "own_words_removed": 0,
        "formats_removed": 0,
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


# 8. The sheet pass: what an artist's works share, proposed from one contact sheet of them, and every claim counted.


WARRIORS = dict.fromkeys(range(10), ["warrior"])
EVERY = lambda claim, pictures: pictures * (claim != profiles.UNSUITABLE)  # noqa: E731  what the fake counts unasked


def calls(fake, claim=None):
    """The calls about a sheet that asked for one claim's count, or with no claim those that asked what recurs."""
    return [asked for asked in fake.sheets if asked["claims"] == ([claim] if claim else [])]


@pytest.mark.parametrize(
    ("count", "sentence", "removed"),
    [
        (10, "Dinosaur art.", 0),  # most pictures: it leads, in the place of "creature"
        (7, "Dinosaur art.", 0),
        (6, "Creature art, including dinosaurs.", 0),  # part of them: hedged
        (4, "Creature art, including dinosaurs.", 0),
        (3, "Creature art.", 1),  # under 4 pictures in 10, nothing is named
        (0, "Creature art.", 1),
    ],
)
def test_a_recurring_subject_is_said_as_far_as_its_count_on_the_sheet_bears_it_out(count, sentence, removed):
    artist(ALPHA, [DRAGON] * 10)
    fake = Fake(propose=[("dinosaurs", "dinosaur", "creature")], count={"dinosaurs": count})

    counts = run(fake)

    assert (counts.get("phrases_removed", 0), counts.get("with_recurring", 0)) == (removed, int(count >= 4))
    assert "written_fallback" not in counts
    (profile,) = profiles.profiles()
    assert profile["template"] == sentence and profile["recurring"] == (["dinosaurs"] if count >= 4 else [])
    assert profile["facts"]["sheet"]["counts"] == {"creature": 10, "dinosaurs": count, profiles.UNSUITABLE: 0}
    # one call a claim, each about that claim and no other
    assert [asked["claims"] for asked in fake.sheets] == [[], ["creature"], ["dinosaurs"], [profiles.UNSUITABLE]]
    assert "10 pictures, numbered 1 to 10.\nClaim: dinosaurs" in calls(fake, "dinosaurs")[0]["question"]
    assert all(asked["prompt"] == profiles.COUNT_PROMPT for asked in fake.sheets[1:])


@pytest.mark.parametrize(
    ("seen", "sentence", "removed"),
    [
        ({}, "Environment and character art.", 0),
        ({"character": 4}, "Environment and character art.", 0),
        ({"character": 3}, "Environment art.", 1),
        ({"character": 1}, "Environment art.", 1),  # a figure merely appears in the works the tags gave it
        # the count keeps neither: the first subject of the tags stays, so that an artist is not left with none
        ({"character": 1, "environment": 2}, "Environment art.", 1),
    ],
)
def test_a_subject_the_tags_gave_is_named_only_when_the_pictures_are_counted_as_about_it(seen, sentence, removed):
    figures = tags(subjects=["environment", "character"], genres=[], style_words=[])
    artist(ALPHA, [SCENERY] * 5 + [figures] * 5)
    fake = Fake(count=seen)

    assert run(fake).get("subjects_removed", 0) == removed
    (profile,) = profiles.profiles()
    assert profile["template"] == sentence and profile["specialties"] == sentence[:-5].lower().split(" and ")
    assert "Claim: character\nIt means: the picture is about this. " in calls(fake, "character")[0]["question"]


@pytest.mark.parametrize(
    ("counts", "sentence", "removed"),
    [
        ({}, "Creature art, mostly dragons and design sheets.", {}),  # every picture shows both
        ({"dragons": 7, "creature sheets": 7}, "Creature art, mostly dragons and design sheets.", {}),
        ({"dragons": 6, "creature sheets": 4}, "Creature art, including dragons and design sheets.", {}),
        ({"dragons": 3}, "Creature art, mostly design sheets.", {"own_words_removed": 1}),
        ({"creature sheets": 3}, "Creature art, mostly dragons.", {"formats_removed": 1}),
        ({"dragons": 0, "creature sheets": 0}, "Creature art.", {"own_words_removed": 1, "formats_removed": 1}),
    ],
)
def test_a_word_of_the_artists_own_and_a_format_are_said_only_as_far_as_the_count_agrees(counts, sentence, removed):
    # every work is tagged "dragon" by the artist and picked as a creature sheet by the tagging stage
    sheets = DRAGON | {"formats": ["creature sheet"], "rating": "safe"}
    artist(ALPHA, [sheets] * 10, own_tags=dict.fromkeys(range(10), ["dragon"]))

    assert run(Fake(count=counts)) == {"profiled": 1} | removed
    assert profiles.profiles()[0]["template"] == sentence


def test_designs_that_no_picture_shows_are_not_said_on_the_artists_tag_alone():
    artist(ALPHA, [PLAIN] * 10, own_tags=dict.fromkeys(range(10), ["characterdesign"]))
    fake = Fake(count={"character designs": 0})  # ten finished scenes, each tagged "characterdesign" by the artist

    assert run(fake) == {"profiled": 1, "own_words_removed": 1}
    assert profiles.profiles()[0]["template"] == "Character art."
    assert (
        "Claim: character designs\nIt means: drawn to show a design" in calls(fake, "character designs")[0]["question"]
    )


@pytest.mark.parametrize(
    ("define", "count", "sentence", "leading"),
    [
        (("dragon head portraits", "subject", "creature"), 9, "Dragon head portraits.", 1),
        (("dragon head portraits", "subject", "creature"), 7, "Dragon head portraits.", 1),
        (("dragon head portraits", "subject", "creature"), 5, "Creature art, including dragon head portraits.", 0),
        (("dragon head portraits", "subject", "creature"), 3, "Creature art.", 0),
        (("pen-and-ink hatching", "look", "character"), 8, "Creature art in pen-and-ink hatching.", 1),
        (("pen-and-ink hatching", "look", "character"), 6, "Creature art.", 0),  # a look is said flat or not at all
        # counted as named in every picture: it leads, beside the subject the tags gave and it does not stand for
        (("gothic witches and demons", "subject", "character"), 10, "Gothic witches and demons and creature art.", 1),
        (("Dragon Age heroes", "subject", "creature"), 10, "Creature art.", 0),  # not plain lowercase words
        (("creatures", "subject", "creature"), 10, "Creature art.", 0),  # one general word says nothing
        (("fantasy creatures", "subject", "creature"), 10, "Creature art.", 0),  # nor two that the tags say already
        (("stunning creature art", "subject", "nonsense"), 10, "Creature art.", 0),
    ],
)
def test_the_defining_thing_leads_when_most_pictures_show_it_as_it_is_named(define, count, sentence, leading):
    artist(ALPHA, [DRAGON] * 10)
    fake = Fake(define=define, count={define[0]: count})

    counts = run(fake)

    assert counts.get("with_defining", 0) == leading and "written_fallback" not in counts
    assert profiles.profiles()[0]["template"] == sentence


@pytest.mark.parametrize(
    ("groups", "proposed", "counts", "sentence"),
    [
        # a recurring place or thing does not take the place of its subject: it is what most of the works are
        ([(10, SCENERY)], [("castles", "castle", "environment")], {"castles": 9}, "Environment art, mostly castles."),
        (
            [(10, SCENERY)],
            [("castles", "castle", "environment")],
            {"castles": 4},
            "Environment art, including castles.",
        ),
        # two recurring subjects lead together, and a subject neither stands for stays
        (
            [(10, tags(subjects=["character", "creature", "environment"], genres=[], style_words=[]))],
            [("dragons", "dragon", "creature"), ("armoured knights", "armoured knight", "character")],
            {"dragons": 8, "armoured knights": 9},
            "Dragon, armoured knight and environment art.",
        ),
        # what the model proposed and the artist also says is one claim, and leads
        ([(10, DRAGON, ["dragon"])], [("dragons", "dragon", "creature")], {"dragons": 10}, "Dragon art."),
        # the artist's broader word for what the recurring subject already names is not added after it
        ([(10, DRAGON, ["monster", "beast"])], [("dragons", "dragon", "creature")], {"dragons": 10}, "Dragon art."),
        (
            [(10, DRAGON, ["creaturedesign"])],
            [("dragons", "dragon", "creature")],
            {"dragons": 10},
            "Dragon art, mostly creature designs.",
        ),
        (
            [(10, DRAGON, ["monster"])],
            [("dragons", "dragon", "creature")],
            {"dragons": 5},
            "Creature art, mostly monsters, including dragons.",
        ),
        # a recurring thing is kept on its count, whatever subject the tags gave: it then leads nothing
        ([(10, PLAIN)], [("mechs", "mech", "vehicle or mech")], {"mechs": 6}, "Character art, including mechs."),
        ([(10, PLAIN)], [("mechs", "mech", "vehicle or mech")], {"mechs": 9}, "Character art, mostly mechs."),
        # two hedged things at most, those that most pictures show
        (
            [(10, DRAGON, ["dragon", "monster"])],
            [("wolves", "wolf", "creature")],
            {"dragons": 5, "monsters": 4, "wolves": 6},
            "Creature art, including wolves and dragons.",
        ),
        # "mostly" takes both: the tagging stage's pick, work by work, and the count of the sheet
        (
            [(4, PLAIN | {"formats": [SHEET]}), (6, PLAIN | {"formats": []})],
            [],
            {"character sheets": 10},
            "Character art, including design sheets.",
        ),
        (
            [(7, PLAIN | {"formats": [SHEET]}), (3, PLAIN | {"formats": []})],
            [],
            {"character sheets": 5},
            "Character art, including design sheets.",
        ),
    ],
)
def test_how_the_counted_subjects_are_worded(groups, proposed, counts, sentence):
    assert said(*groups, proposed=proposed, counts=counts) == sentence


@pytest.mark.parametrize(
    ("work", "own", "defining", "sentence"),
    [
        # nothing broader is added after the defining thing: not the artist's word for it, not its format
        (
            DRAGON | {"formats": ["creature sheet"]},
            ["creaturedesign", "monster"],
            ("dragon head portraits", "subject", "creature"),
            "Dragon head portraits.",
        ),
        # nor a genre or a style word that its own words say already, in any spelling
        (
            tags(subjects=["environment"], genres=["science fiction"], style_words=["monochrome"]),
            [],
            ("sci-fi streets and hangars", "subject", "environment"),
            "Monochrome sci-fi streets and hangars.",
        ),
        (
            tags(genres=["fantasy", "dark fantasy"], style_words=["monochrome"]),
            [],
            ("monochrome pen-and-ink hatching", "look", "character"),
            "Fantasy and dark fantasy character art in monochrome pen-and-ink hatching.",
        ),
    ],
)
def test_nothing_is_said_again_after_the_defining_thing_has_said_it(work, own, defining, sentence):
    assert said((10, work, own), defining=defining, counts={defining[0]: 10}) == sentence


def test_a_claim_is_counted_as_it_is_named_and_a_picture_once():
    artist(ALPHA, [DRAGON] * 10)
    # eight pictures show a horse-shaped creature of another kind; the model lists some of them under both
    like = {"closest": "FAKE-LOOK", "like_it_only": [1, 2, 3, 4, 5, 6, 7, 8], "shows": [1, 2, 3, 3, 9, 9, 10]}
    fake = Fake(
        propose=[("horses", "horse", "creature")], count=lambda claim, n: like if claim == "horses" else EVERY(claim, n)
    )

    assert run(fake) == {"profiled": 1, "phrases_removed": 1}
    (profile,) = profiles.profiles()
    assert profile["facts"]["sheet"]["counts"]["horses"] == 2  # pictures 9 and 10, each once
    assert profile["template"] == "Creature art."
    for told in (
        "A horse-shaped fantasy creature is not a horse",
        "A finished scene is not a design sheet",
        "AS IT IS NAMED",
    ):
        assert told in profiles.COUNT_PROMPT


@pytest.mark.parametrize(
    "bad",
    [
        {"closest": "", "like_it_only": [], "shows": "many"},
        {"closest": "", "like_it_only": [], "shows": [1, 2, 99]},  # a number that is not on the sheet
        {"closest": "", "like_it_only": [], "shows": [True, 2]},
        {"closest": "", "shows": [1, 2, 3, 4, 5, 6]},  # a list left out
        httpx.Response(200, json={"message": {"content": "not json"}}),
    ],
)
def test_a_count_that_is_no_whole_answer_counts_nothing_and_what_is_not_counted_is_not_said(bad):
    artist(ALPHA, [PLAIN] * 6, own_tags=WARRIORS)
    fake = Fake(count=lambda claim, pictures: bad if claim == "warriors" else EVERY(claim, pictures))

    assert run(fake) == {"profiled": 1, "own_words_removed": 1}
    assert profiles.profiles()[0]["template"] == "Character art."
    assert (len(calls(fake, "warriors")), len(calls(fake, "character"))) == (3, 1)  # asked again, and then left


@pytest.mark.parametrize(
    ("count", "sentence"),
    [
        (10, "Anime character art."),
        (9, "Anime character art."),  # nearly every picture: the manner is the body of work, whatever the tags missed
        (8, "Character art, partly in an anime style."),
        (0, "Character art, partly in an anime style."),  # the verifier saw it in 4 works: the count takes nothing away
    ],
)
def test_a_manner_the_tags_gave_part_of_the_works_is_stated_flat_when_nearly_every_picture_is_drawn_so(count, sentence):
    artist(ALPHA, [tags(genres=["anime style"], style_words=[])] * 4 + [PLAIN] * 6)
    fake = Fake(count={"anime": count})

    assert run(fake) == {"profiled": 1}
    assert profiles.profiles()[0]["template"] == sentence
    assert (
        "Claim: anime\nIt means: drawn in this manner. the manner of Japanese anime"
        in calls(fake, "anime")[0]["question"]
    )


def test_the_sheet_is_built_only_from_the_eligible_works():
    results = [tags()] * 3 + [tags(explicit=True)] + [tags()] * 2 + [tags(not_artwork=True)] + [tags()] * 3
    artist(ALPHA, results, declared={9: "yes"})
    fake = Fake(propose=[("knights", "knight", "character")])

    assert run(fake) == {"profiled": 1, "with_recurring": 1}

    assert len(fake.sheets) == 4 and all(
        asked["works"] == [0, 1, 2, 4, 5, 7, 8] for asked in fake.sheets
    )  # never 3, 6, 9
    assert all("7 pictures, numbered 1 to 7" in asked["question"] for asked in fake.sheets)
    assert profiles.profiles()[0]["pictures"] == 7


def test_without_a_sheet_nothing_is_counted_and_so_nothing_is_said():
    ids = artist(ALPHA, [PLAIN] * 6, own_tags=WARRIORS)
    for work_id in ids[:2]:
        (library.image_dir() / f"{work_id}.png").write_bytes(PNG + b"FAKE-NOT-A-PICTURE")
    fake = Fake(propose=[("knights", "knight", "character")])

    assert run(fake) == {"excluded_nothing_clear": 1}  # four pictures open: too few to count anything on
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
    # the artist's tag is not sent: the word of the list that it spells is, beside what the model proposed
    assert [asked["claims"] for asked in fake.sheets] == [
        [],
        ["character"],
        ["knights"],
        ["warriors"],
        [profiles.UNSUITABLE],
    ]
    assert profiles.profiles()[0]["template"] == "Knight art."  # "warriors" adds nothing after "knight"

    again = Fake(propose=[("dragons", "dragon", "creature")])
    assert run(again) == {"unchanged": 1}  # and a rerun asks nothing a second time
    assert again.requests == [] and again.sheets == []


def test_each_claim_is_counted_by_what_it_means_and_a_card_by_what_the_word_says():
    picked = PLAIN | {"formats": ["card or splash illustration", "character sheet or turnaround"], "rating": "safe"}
    artist(ALPHA, [picked] * 6, own_tags=dict.fromkeys(range(6), ["characterdesign"]))
    fake = Fake(count={"card illustrations": 0})

    assert run(fake) == {"profiled": 1, "formats_removed": 1}

    card = calls(fake, "card illustrations")[0]["question"]
    assert "It means: laid out as a playing card" in card and "name plate on a design sheet does not make one" in card
    sheets = calls(fake, "character sheets")[0]["question"]
    assert f"It means: {tagging.FORMATS['character sheet or turnaround']}" in sheets
    assert profiles.profiles()[0]["template"] == "Character designs, mostly design sheets."
    # and the writer is told it as the template says it: no word of the kind of art a second time
    (asked,) = (chat["messages"][-1]["content"] for chat in fake.chats)
    assert asked.splitlines() == ["Facts", "kinds of art: character designs", "most of the works: design sheets"]


def test_the_sexualised_or_gory_pictures_are_counted_and_handed_on_for_the_push_stage_to_weigh():
    artist(ALPHA, [PLAIN] * 10)
    artist(BRAVO, [PLAIN] * 10)
    seen = iter([7, 0])
    fake = Fake(count=lambda claim, pictures: next(seen) if claim == profiles.UNSUITABLE else pictures)

    assert run(fake) == {"profiled": 2}  # counted, stored, and nobody is left out here on the strength of it

    alpha, bravo = profiles.profiles()
    assert (alpha["pictures"], alpha["unsuitable_pictures"]) == (10, 7)
    assert (bravo["pictures"], bravo["unsuitable_pictures"]) == (10, 0)
    assert "sexualised" not in alpha["template"] + alpha["written"] + json.dumps(alpha["open_evidence"])
    question = calls(fake, profiles.UNSUITABLE)[0]["question"]
    assert "It means: a figure posed or dressed in a sexualised way, or strong gore" in question


def test_one_way_with_commas_and_one_plain_word_for_an_either_or_name():
    found = facts(
        ["vehicle or mech", "prop or weapon"],
        genres=["fantasy", "science fiction"],
        style_words=["painterly", "muted"],
        formats=["character sheet or turnaround", "environment keyframe or scene"],
        hedged=["science fiction", "environment keyframe or scene"],
    )

    assert profiles.template(found) == (
        "Painterly, muted fantasy vehicle and prop art with science fiction themes, mostly character sheets, "
        "including environment scenes."
    )
    said_first = [forms[0] for forms in (profiles.SUBJECT_FORMS | profiles.FORMAT_FORMS).values()]
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
