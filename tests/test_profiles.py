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
USUAL = "digitalart"  # the tag of every invented work that is given no other: a catch-all, which no card quotes
PNG = b"\x89PNG\r\n\x1a\n"  # how each invented picture begins: a small square of one colour, which says which it is
VERIFIED = tagging.result_set(profiles.TAGGER, profiles.TAGGER)
ACCOUNT, TOKEN = "FAKE-ACCOUNT-0123456789abcdef", "FAKE-TOKEN-zyxwvutsrqponmlk"
SECRETS = {"CLOUDFLARE_ACCOUNT_ID": ACCOUNT, "CLOUDFLARE_API_TOKEN": TOKEN, "OTHER_SECRET": "FAKE-OTHER-SECRET"}
COUNTS = re.compile(
    r'\{"profiled": \d+, "excluded_mostly_ai_declared": \d+, "excluded_mostly_explicit": \d+, '
    r'"excluded_too_few_works": \d+, "excluded_nothing_clear": \d+, "unchanged": \d+, "waiting_for_tags": \d+, '
    r'"left_for_next_run": \d+, "removed": \d+, "open_for_work": \d+, "with_own_tags": \d+, '
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
    # The invented tags below are on the reviewed list as far as these tests go, so that every other rule is seen
    # to hold them back. The list's own tests give it none.
    monkeypatch.setattr(profiles, "APPROVED", profiles.APPROVED | INVENTED)


INVENTED = {f"{letter}tag" for letter in "abcdefghijklmn"} | {"colorful", "fakefirstname", "fakesurname"}
INVENTED |= {"zzfakealpha", "zzfake", "alpha", "artofzzfakealpha", "yydarkraven", "darkraven"}
INVENTED |= {"fakename0", "fakename1", "fakename2", "call555x019x942", "mk123", "mk456", "mk789"}


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
    USUAL, declares no AI and was published at RECENT. contact: what the tagging stage found when it read
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
            "tags": (own_tags or {}).get(n, [USUAL]),
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


class Fake:
    """Ollama's vision model (/api/chat) and embedder (/api/embed), and Workers AI, told apart by host and path.

    count: in how many pictures of the sheet it finds a claim: {claim: how many, from the first, or which
           places}; a claim not named there it finds in none. Or a function (claim, pictures) -> the same, a
           whole answer to send, a Response, or an exception to raise.
    embed: a function (text) -> the vector Ollama gives. cloud: the same for Workers AI, or a Response.
    """

    def __init__(self, embed=vector, cloud=vector, count=None):
        self.embed, self.cloud, self.count = embed, cloud, count
        self.requests, self.embeds, self.clouds = [], [], []
        self.sheets = []  # each call about a sheet: the works on it, the question, the claim

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
        assert prompt == profiles.COUNT_PROMPT  # the one thing a model is still asked
        (sheet,) = body["messages"][-1]["images"]
        works = works_on(sheet)
        (claim,) = (line.removeprefix("Claim: ") for line in question.splitlines() if line.startswith("Claim: "))
        self.sheets.append({"works": works, "question": question, "claim": claim, "body": body})
        found = self.count(claim, len(works)) if callable(self.count) else (self.count or {}).get(claim, 0)
        if not isinstance(found, int | list) or isinstance(found, bool):
            return self.say(found)
        shows = list(range(1, found + 1)) if isinstance(found, int) else found
        return self.say({"closest": "FAKE-LOOK", "like_it_only": [], "shows": shows})

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


def said(*groups, **artist):
    """The card for such a body of work, which passes the print check."""
    found, reason = profiles.facts_of(body(*groups), **artist)
    assert reason is None and profiles.unfit(profiles.template(found)) is None, profiles.template(found)
    return profiles.template(found)


PLAIN = tags(genres=[], style_words=[])  # a character in digital painting, and nothing else to say of it
DRAGON = tags(subjects=["creature"], genres=[], style_words=[])
SCENERY = tags(subjects=["environment"], genres=[], style_words=[])
BOTH = tags(subjects=["character", "creature"], genres=[], style_words=[])
FANTASY = tags(style_words=[])  # a fantasy character


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
    assert (profile["style_words"], profile["own_words"], profile["hedged"]) == ([], [], [])
    # one text, in both columns: nothing of a description, nothing below its threshold, and no style word
    assert profile["template"] == profile["written"] == "Fantasy character and creature art."
    assert (profile["template_version"], profile["written_version"]) == (profiles.TEMPLATE_VERSION,) * 2
    assert profile["written_by_model"] is False


@pytest.mark.parametrize(
    ("eligible", "having", "field", "stated"),
    [
        # a subject is hedged from 4 works in 10 and never on fewer than 4 works, "mostly" from 6, flat from 7
        (10, 3, "subject", "no"),
        (10, 4, "subject", "hedged"),
        (10, 5, "subject", "hedged"),  # half is a part: round 5's flat claims at about half
        (10, 6, "subject", "mostly"),
        (10, 7, "subject", "flat"),
        (5, 3, "subject", "no"),
        (5, 4, "subject", "flat"),
        (20, 7, "subject", "no"),
        (20, 8, "subject", "hedged"),
        (11, 4, "subject", "no"),  # 4 of 11 is under 40 percent
        (11, 5, "subject", "hedged"),
        (14, 14, "subject", "flat"),  # round 6's critic: "mostly dragons" at 14 of 14
        # a genre is stated flat from 7 in 10, and below that not at all: no "with fantasy themes", no "mostly"
        (10, 4, "world", "no"),
        (10, 6, "world", "no"),
        (10, 7, "world", "flat"),
        (11, 4, "world", "no"),  # round 6's critic: "mostly fantasy" where 4 or 5 of 11 are
        (11, 5, "world", "no"),
        (11, 7, "world", "no"),
        (11, 8, "world", "flat"),
        (10, 10, "world", "flat"),  # and "mostly fantasy" at 10 of 10
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

    facts_stated = [term for name in profiles.STATED for term in found.get(name, [])]
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
        # and after a subject that is said by its share
        (
            [(6, tags("3d render", genres=[], style_words=[])), (4, tags("3d render", [], [], []))],
            "Mostly character art in 3D.",
        ),
        (
            [(5, tags("3d render", genres=[], style_words=[])), (5, tags("3d render", [], [], []))],
            "Character art in 3D, among other things.",
        ),
    ],
)
def test_a_medium_is_said_only_when_it_is_distinctive_and_a_clear_majority(groups, sentence):
    assert said(*groups) == sentence


@pytest.mark.parametrize(
    ("groups", "sentence"),
    [
        # round 6's critic: "mostly dragons" at 14 of 14. What most works show is never behind a hedge.
        ([(14, DRAGON)], "Creature art."),
        ([(7, PLAIN), (3, DRAGON)], "Character art."),  # flat from 7 in 10, and 3 in 10 are not named
        # the subjects are named together, by the works that show any of them: round 7's critic, "'Artwork' is
        # filler", where characters were 6 of 10, or creatures 4 of 8 and props 3 of 8
        ([(6, PLAIN), (4, DRAGON)], "Character and creature art."),
        ([(5, SCENERY), (4, PLAIN), (1, DRAGON)], "Environment and character art."),
        ([(6, PLAIN), (4, BOTH)], "Character and creature art."),
        ([(4, PLAIN), (6, BOTH)], "Character and creature art."),
        ([(8, BOTH), (2, PLAIN)], "Character and creature art."),
        (
            [(6, PLAIN), (4, tags(subjects=["vehicle or mech"], genres=[], style_words=[]))],
            "Character and vehicle art.",
        ),
        ([(6, PLAIN), (3, DRAGON), (1, SCENERY)], "Mostly character art."),
        ([(6, BOTH), (4, tags(subjects=[], genres=[], style_words=[]))], "Mostly character and creature art."),
        ([(5, PLAIN), (3, DRAGON), (2, SCENERY)], "Character art, among other things."),  # round 4: one of half
        (
            [(4, DRAGON), (3, tags(subjects=["prop or weapon"], genres=[], style_words=[])), (1, SCENERY)],
            "Creature art, among other things.",
        ),
    ],
)
def test_a_subject_is_said_by_its_share_of_the_works(groups, sentence):
    assert said(*groups) == sentence
    assert not sentence.startswith("Artwork")


@pytest.mark.parametrize(
    ("groups", "sentence"),
    [
        ([(10, FANTASY)], "Fantasy character art."),  # round 6's critic: "mostly fantasy" at 10 of 10
        ([(7, FANTASY), (3, PLAIN)], "Fantasy character art."),
        # and "mostly fantasy" where 4 or 5 of 11 are: under 7 in 10 a genre is not said, behind no hedge
        ([(4, FANTASY), (7, PLAIN)], "Character art."),
        ([(5, FANTASY), (6, PLAIN)], "Character art."),
        ([(6, FANTASY), (4, PLAIN)], "Character art."),
        ([(5, FANTASY), (5, tags(genres=["science fiction"], style_words=[]))], "Character art."),
        ([(7, FANTASY), (3, tags(genres=["science fiction"], style_words=[]))], "Fantasy character art."),
        (
            [(10, tags(genres=["fantasy", "science fiction"], style_words=[]))],
            "Fantasy and science fiction character art.",
        ),
        ([(6, FANTASY), (4, tags(subjects=["creature"], style_words=[]))], "Fantasy character and creature art."),
        # a genre holds for all the works, whatever share of them the subjects are: it leads, and they follow
        ([(6, FANTASY), (4, tags(subjects=[], style_words=[]))], "Fantasy art, mostly characters."),
        ([(5, FANTASY), (5, tags(subjects=[], style_words=[]))], "Fantasy art, including characters."),
    ],
)
def test_a_genre_is_stated_flat_or_not_at_all(groups, sentence):
    assert said(*groups) == sentence


def test_what_stands_flat_before_the_kind_of_art_is_true_of_most_works_together():
    """Seven fantasy works and seven characters may be four fantasy characters: that is no "fantasy character art"."""
    apart = [(4, FANTASY), (3, tags(subjects=["environment"], style_words=[])), (3, PLAIN)]
    together = [(6, FANTASY), (1, tags(subjects=["environment"], style_words=[])), (1, PLAIN), (2, SCENERY)]

    assert said(*apart) == "Character art."
    assert profiles.facts_of(body(*apart))[0]["genres"] == []  # nor is it a fact for search: 7 in 10, of other works
    assert said(*together) == "Fantasy character art."
    anime = tags(subjects=["environment"], genres=["anime style"], style_words=[])
    assert said((4, tags(genres=["anime style"], style_words=[])), (3, anime), (3, PLAIN)) == "Character art."
    # a manner that most works have and not 7 in 10 is no word before the noun, and holds on its own
    assert said((3, tags(genres=["anime style"], style_words=[])), (3, anime), (4, PLAIN)) == (
        "Character art, mainly in an anime style."
    )


@pytest.mark.parametrize(
    ("groups", "sentence"),
    [
        ([(7, tags(genres=["anime style"], style_words=[])), (3, PLAIN)], "Anime character art."),
        ([(6, tags(genres=["anime style"], style_words=[])), (4, PLAIN)], "Character art, mainly in an anime style."),
        ([(10, tags(genres=["fantasy", "realistic"], style_words=[]))], "Realistic fantasy character art."),
        # round 7's critic: "'Partly' is wrong on all four cards that use it". The hedge says "often", which
        # holds however many more works the tags missed: 5 of 11 tagged anime where the critic saw 9
        ([(5, tags(genres=["anime style"], style_words=[])), (6, PLAIN)], "Character art, often in an anime style."),
        (
            [(5, tags(genres=["stylized cartoon"], style_words=[])), (5, PLAIN)],
            "Character art, often in a cartoon style.",  # never "a stylized cartoon style"
        ),
        ([(2, tags(genres=["realistic"], style_words=[])), (8, PLAIN)], "Character art."),
        # and the critic's own counts: 9 of 11 anime, 10 of 10 realistic, 6 of 8 cartoon are flat
        ([(9, tags(genres=["anime style"], style_words=[])), (2, PLAIN)], "Anime character art."),
        ([(10, tags(subjects=["environment"], genres=["realistic"], style_words=[]))], "Realistic environment art."),
        ([(6, tags(subjects=[], genres=["stylized cartoon"], style_words=[])), (2, DRAGON)], "Cartoon art."),
        # with nothing else to name, the manner is named, by its share: round 7's "Artwork, partly in a cartoon style"
        (
            [(4, tags(subjects=[], genres=["stylized cartoon"], style_words=[])), (4, tags(subjects=[], genres=[]))],
            "Cartoon art, among other things.",
        ),
        # "mostly" once on a card: the manner has a word of its own
        (
            [(6, tags(genres=["anime style"], style_words=[])), (4, tags(subjects=[], genres=[], style_words=[]))],
            "Mostly character art, mainly in an anime style.",
        ),
    ],
)
def test_the_drawing_manner_leads_when_most_works_have_it_and_is_hedged_when_part_of_them_do(groups, sentence):
    assert said(*groups) == sentence
    assert "partly" not in sentence


def test_a_style_word_is_counted_for_search_and_never_said():
    found, _ = profiles.facts_of(body((10, tags(style_words=["muted", "painterly"]))))

    assert found["style_words"] == ["painterly", "muted"]
    assert profiles.template(found) == "Fantasy character art."
    assert "Style: painterly, muted." in profiles.embedded_text(profiles.template(found), found)


# 2. The artist's own tags: shown apart from the card, as theirs, and never in it (owner decision 11).

OWN = ["dinosaur", "paleoart", "creaturedesign", "digitalart"]


def shown(*groups, **artist):
    """The artist's own tags the site shows for such a body of work."""
    return profiles.facts_of(body(*groups), **artist)[0]["own_words"]


@pytest.mark.parametrize(
    ("groups", "own_words"),
    [
        ([(10, DRAGON, OWN)], ["dinosaur", "paleoart", "creaturedesign"]),
        # a tag on one work is the artist's as much as one on all of them
        ([(1, DRAGON, ["dinosaur"]), (9, DRAGON)], ["dinosaur"]),
        # the most used first; of two as often used, the one the artist writes earlier
        ([(6, DRAGON, ["wolf", "dinosaur"]), (4, DRAGON, ["theropod", "dinosaur"])], ["dinosaur", "wolf", "theropod"]),
        ([(10, DRAGON, ["theropod", "wolf"])], ["theropod", "wolf"]),
        ([(10, DRAGON, ["wolf", "theropod"])], ["wolf", "theropod"]),
        # twelve at most
        (
            [(10, DRAGON, [f"{letter}tag" for letter in "abcdefghijklmn"])],
            [f"{letter}tag" for letter in "abcdefghijkl"],
        ),
        # the same tag however it is written, and a work that has it twice counts once
        (
            [(2, DRAGON, ["Paleo_Art", "paleoart"]), (2, DRAGON, ["PALEOART"]), (6, DRAGON, ["paleo-art"])],
            ["paleoart"],
        ),
    ],
)
def test_the_tags_shown_are_the_ones_the_artist_put_on_the_most_works(groups, own_words):
    assert shown(*groups) == own_words


def test_the_card_is_one_sentence_and_quotes_no_tag_of_the_artists():
    found, _ = profiles.facts_of(body((10, DRAGON, OWN)))

    assert profiles.template(found) == "Creature art."
    assert found["own_words"] == ["dinosaur", "paleoart", "creaturedesign"]


def test_the_quoted_tags_are_kept_with_the_number_of_counted_works_that_carry_each():
    flagged = tags(subjects=["creature"], genres=[], style_words=[], explicit=True)
    found, _ = profiles.facts_of(
        body((6, DRAGON, ["dinosaur", "paleoart"]), (4, DRAGON, ["dinosaur", "wolf"]), (5, flagged, ["wolf", "fish"]))
    )

    assert found["own_words"] == ["dinosaur", "paleoart", "wolf"]
    assert found["counts"]["own_words"] == {"dinosaur": 10, "paleoart": 6, "wolf": 4}  # of the works that count


@pytest.mark.parametrize(
    ("own", "printed"),
    [
        (["character_design"], "characterdesign"),
        (["Dungeons_And_Dragons"], "dnd"),  # a long name written short, as the list has it
        (["sci-fi"], "scifi"),
        (["dnd_5e"], "dnd5e"),
        (["creaturedesign"], "creaturedesign"),
        (["Dark-Fantasy"], "darkfantasy"),
        # the fourth critic of the push stage: the artist's spacing made words of listed tags, that the list has not
        (["tit_an"], "titan"),
        (["jap_anese"], "japanese"),
        (["rac-coon"], "raccoon"),
        (["heroin_e"], "heroine"),
        (["paleoart"], "paleoart"),
        (["warhammer40k"], "warhammer40k"),
    ],
)
def test_a_tag_is_printed_as_the_reviewed_list_spells_it_never_in_the_artists_spacing(own, printed):
    found, _ = profiles.facts_of(body((10, tags("unclear", ["environment"], [], []), own)))

    assert found["own_words"][:1] == [printed]


@pytest.mark.parametrize(
    "own",
    [
        # the catch-alls every artist uses, joined any way
        ["art", "digitalart", "digital_art", "artwork", "drawing", "illustration", "digitalpainting", "2dart"],
        ["artworkdigital", "digitalartwork", "conceptart", "conceptdesign", "gameart", "visualdevelopment", "design"],
        ["artist", "digitalartist", "illustrationdigital", "mydrawing", "dailyart", "style", "mood", "girl", "hot"],
        # selling and availability, where it is posted, what it was made with, a statement about AI
        ["commission", "commissionsopen", "adoptablesopen", "forsale", "auction", "ych", "paypal", "freelance"],
        ["adoptable", "adopts", "characteradopt", "ocadopt"],
        ["deviantart", "dailydeviation", "artoftheday", "artstation", "instaart", "artistsondeviantart"],
        ["photoshop", "procreate", "blender3d", "clipstudiopaint", "noai", "notaigenerated", "humanmadeart"],
        # praise, a year
        ["epic", "epicfantasy", "beautiful", "beauty", "amazing", "highdetail", "detailed", "inktober2021"],
        # the real people whose names several artists tag their fan art with
        ["frankherbert", "tolkien", "lovecraft", "lovecraftian", "hrgiger"],
        # dress or body
        ["sexy", "sexygirl", "pinup", "fantasypinup", "bikini", "outfit", "outfitdesign", "dress", "bigboobs"],
        ["longtail", "caninetail", "mawshot", "tongue", "stocksfeet", "militaryuniform", "cape", "redcape", "nsfw"],
        [],
    ],
)
def test_a_tag_that_says_nothing_or_is_not_about_the_work_is_not_shown(own):
    found, _ = profiles.facts_of(body((10, FANTASY, own)))

    assert found["own_words"] == [] and found["counts"]["own_words"] == {}
    assert profiles.template(found) == "Fantasy character art."


def test_only_a_tag_on_the_reviewed_list_is_shown():
    """The third critic of the push stage: a list of banned words let "lolita", "yuri" and many more through.
    Every tag on the reviewed list is shown, however it is written, and no other, whatever it is."""
    own = ["soldier", "Landscape", "spaceship", "character_design", "lolita", "yuri", "midriff", "salem"]
    own += ["sapphire", "wheels", "spaceships", "bunnysuit", "croptop", "zzinventedtag"]
    assert shown((10, SCENERY, own)) == ["soldier", "landscape", "spaceship", "characterdesign"]
    assert all(
        profiles.approved(tag) for tag in ("Soldier", "character_design", "Character-Design", "dungeons_and_dragons")
    )
    assert not any(map(profiles.approved, ("lolita", "yuri", "midriff", "zzinventedtag", "")))


def test_no_tag_says_again_what_one_shown_before_it_has_said():
    assert shown((10, DRAGON, ["dragon", "dragons", "dragonart", "reddragon", "wolf"])) == ["dragon", "wolf"]
    assert shown((10, PLAIN, ["darkfantasy", "darkart", "fantasyart", "dark", "knight"])) == ["darkfantasy", "knight"]
    # nor in the plural, nor by its initials
    assert shown((10, PLAIN, ["elf", "elves", "elfart", "originalcharacter", "oc", "ocs"])) == [
        *("elf", "originalcharacter")
    ]
    assert shown((10, PLAIN, ["characterdesign", "characterart", "character", "characters"])) == ["characterdesign"]


@pytest.mark.parametrize(
    "own",
    [
        ["dnd", "dungeonsanddragons", "ttrpg"],  # round 7's critic: "three synonyms in a row"
        ["dungeonsanddragons", "dnd", "dungeons_and_dragons", "dnd_character"],
        ["DnD", "DungeonsAndDragons"],
    ],
)
def test_one_name_written_two_ways_is_shown_once(own):
    found = [re.sub(r"[\s_-]", "", tag.lower()) for tag in shown((10, FANTASY, own))]

    assert not {"dnd", "dungeonsanddragons"} <= set(found)
    assert found[0] == "dnd"  # as the reviewed list spells it, however the artist wrote it


def test_a_genre_or_a_word_written_two_ways_is_shown_once():
    assert shown((10, FANTASY, ["scifi", "sci-fi", "sciencefiction", "science_fiction"])) == ["scifi"]
    assert shown((10, FANTASY, ["science_fiction", "scifi"])) == ["scifi"]
    assert shown((10, FANTASY, ["colourful", "colorful"])) == ["colorful"]


def test_the_artists_own_handle_or_name_is_not_shown():
    own = ["zzfakealpha", "zzfake", "alpha", "artofzzfakealpha", "zz_fake_alpha", "paleoart"]

    assert shown((10, DRAGON, own), handle=ALPHA) == ["paleoart"]
    assert shown((10, DRAGON, own)) == ["zzfakealpha", "paleoart"]
    # a handle with "-" or "_" in it is compared as a tag is, without them (the second critic of the push stage)
    assert shown((10, DRAGON, ["yydarkraven", "darkraven", "paleoart"]), handle="yy-dark-raven") == ["paleoart"]
    assert shown((10, DRAGON, ["yy_dark_raven", "paleoart"]), handle="YY_Dark-Raven") == ["paleoart"]


# What the second critic of the push stage found shown: none of it is on the reviewed list.
@pytest.mark.parametrize(
    "own",
    [
        ["kimono", "shirtless", "musclegirl", "hips", "widehips", "panties", "ballgown"],
        ["catears", "blondehair", "glowingeyes", "tshirtdesign", "bunnygirl", "femboy", "succubusgirl", "hentai"],
        ["costumedesign", "fashion", "cosplay", "angelwings", "horns", "claws", "tattoo", "makeup", "redhead"],
    ],
)
def test_a_dress_body_or_sex_word_anywhere_in_a_tag_is_not_shown(own):
    assert shown((10, FANTASY, own)) == []


@pytest.mark.parametrize("how", ["marked", "taken_down"])
def test_a_work_that_no_longer_stands_is_not_counted_and_a_card_counted_over_it_waits(how):
    """The second critic of the push stage: a card counted from works that are another author's since, or that
    the artist took down, is out of date; the next run counts the works that stand, and nothing else."""
    ids = artist(ALPHA, [DRAGON] * 10)
    assert run(Fake()) == {"profiled": 1} and profiles.current() == {f"da:{ALPHA}"}
    if how == "marked":
        library.mark_works(f"da:{ALPHA}", dict.fromkeys(ids[7:], "another author"))
    else:  # missing from the artist's latest answer: last seen before it
        with closing(sqlite3.connect(config.LOCAL_DB)) as conn, conn:
            old = [("2026-01-01T00:00:00+00:00", work_id) for work_id in ids[7:]]
            conn.executemany("UPDATE works SET fetched_at = ? WHERE deviation_id = ?", old)

    assert profiles.current() == set()
    assert run(Fake()) == {"profiled": 1} and profiles.current() == {f"da:{ALPHA}"}
    (profile,) = profiles.profiles()
    assert profile["facts"]["counted"] == ids[:7] and profile["facts"]["works"] == 7


def test_a_tag_that_few_artists_in_the_store_use_is_not_shown():
    """A name is seldom anyone else's tag: the artist's own, a friend's, a director's."""
    for n, handle in enumerate((ALPHA, BRAVO, CHARLIE)):
        own = ["paleoart", "fakefirstname", *(["fakesurname"] if n else []), f"fakename{n}"]
        artist(handle, [DRAGON] * 6, own_tags=dict.fromkeys(range(6), own))

    assert run(Fake()) == {"profiled": 3, "with_own_tags": 3}
    assert profiles.SHARED == 3
    # "fakesurname" is on every work of two of them, and that is not enough
    for profile in profiles.profiles():
        assert (profile["template"], profile["own_words"]) == ("Creature art.", ["paleoart", "fakefirstname"])


def test_a_tag_the_plain_contact_rule_refuses_is_not_shown_alone_or_with_the_others():
    assert shown((10, DRAGON, ["call555x019x942", "paleoart"])) == ["paleoart"]
    # three tags that pass one by one and hold nine digits between them, side by side on the page
    assert library.holds_contact("mk123 mk456 mk789")
    assert shown((10, DRAGON, ["mk123", "mk456", "mk789", "paleoart"])) == ["mk123", "mk456", "paleoart"]


def test_tags_alone_make_no_profile():
    """A card must rest on what was checked: with nothing for the first sentence, the artist's word is not enough."""
    found, reason = profiles.facts_of(body((10, tags("traditional", [], [], []), ["dinosaur", "paleoart"])))

    assert (found["own_words"], reason) == (["dinosaur", "paleoart"], "nothing_clear")


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
    assert found["genres"] == ["cyberpunk", "realistic"]  # not the wider science fiction, and one manner; fantasy has 6
    assert (found["mostly"], found["hedged"]) == ([], [])  # 6 in 10 each, and named together: 10 show one of them
    assert found["style_words"] == ["painterly", "monochrome"]  # all have 10: the order of the list, and two
    assert all(len(found[name]) <= most for name, most in profiles.AT_MOST.items() if name in found)


def test_with_no_work_counted_nothing_is_a_fact():
    found, reason = profiles.facts_of([{"id": "a", "result": tags(not_artwork=True), "declared": "no"}])

    assert not any(found.get(name) for name in profiles.STATED)
    assert (found["eligible"], found["hedged"], reason) == (0, [], "too_few_works")


def test_a_work_whose_declaration_is_unclear_counts_and_one_with_no_result_does_not():
    works = [{"id": str(n), "result": tags(), "declared": "unclear"} for n in range(5)]
    works += [{"id": "failed", "result": None, "declared": "no"}]

    found, reason = profiles.facts_of(works)

    assert (found["works"], found["eligible"], reason) == (6, 5, None)


# 3. Exclusions: no profile, and the reason on record.


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


# 4. Open for work: the artist's own words, or unknown.


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


def test_open_for_work_is_stored_with_its_evidence_and_stated_after_the_card():
    artist(ALPHA, [tags()] * 6, tagline="NO AI | COMMISSIONS - OPEN", own_tags={2: ["commissionsopen"]})
    artist(BRAVO, [tags()] * 6, tagline="Professional artist for hire")

    assert run(Fake()) == {"profiled": 2, "open_for_work": 2}
    alpha, bravo = profiles.profiles()
    assert alpha["open_for_work"] is True and bravo["open_for_work"] is True
    assert [item["says"] for item in alpha["open_evidence"]] == ["NO AI | COMMISSIONS - OPEN", "commissionsopen"]
    assert alpha["template"] == alpha["written"] == "Fantasy character art. Open for commissions."
    assert bravo["template"] == bravo["written"] == "Fantasy character art. Available for hire."  # not commissions


def test_open_for_work_is_never_inferred_and_never_false(monkeypatch):
    # A professional who tags works as commissions, sells characters and has a price list has not said they are open.
    hints = ["commission", "commissioninfo", "commissionprices", "ychcommissionopen", "characterforsale", "freelance"]
    artist(ALPHA, [tags()] * 6, tagline="Freelance Illustrator", own_tags=dict.fromkeys(range(6), [*hints, "knight"]))
    artist(BRAVO, [tags()] * 6, tagline="Commissions closed", own_tags={0: ["commissionsopen"]})
    artist(CHARLIE, [tags()] * 6, own_tags={0: ["commissionsopen"]}, published={0: OLD})
    monkeypatch.setattr(profiles, "SHARED", 1)  # nor do the tags shown as theirs say it, whoever uses those

    assert run(Fake()) == {"profiled": 3, "with_own_tags": 1}
    for profile in profiles.profiles():
        assert profile["open_for_work"] is None and profile["open_evidence"] == []
        shown_words = " ".join([profile["template"], *profile["own_words"]]).lower()
        assert not re.search("open|commission|hire|available|freelance|sale", shown_words)
    assert (profiles.profiles()[0]["template"], profiles.profiles()[0]["own_words"]) == (
        *("Fantasy character art.", ["knight"]),
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


# 5. The print check: what a card may not read like, true or not.


@pytest.mark.parametrize(
    ("text", "why"),
    [
        ("Fantasy character art, mostly fantasy characters.", '"fantasy" is said twice'),  # round 5's four repeats
        ("Fantasy art and character art.", '"art" is said twice'),
        ("Character art, mostly with characters.", '"character" is said twice'),  # a plural is its singular
        ("Environments, mostly castles.", "no kind of art"),
        ("3D environment art.", "after the kind of art"),
        ("Realistic anime fantasy and horror character art.", "more than 3 words are stacked"),
        ("Fantasy character art and.", 'breaks off at "and"'),
        ("Fantasy art, mostly character.", 'breaks off at "character"'),
    ],
)
def test_a_card_that_is_not_fit_to_print_is_refused(text, why):
    assert why in profiles.unfit(text)
    assert why in profiles.unfit(f"{text} Open for commissions.")


def test_a_card_the_print_check_refuses_is_not_printed(monkeypatch):
    artist(ALPHA, [tags()] * 6)
    monkeypatch.setattr(profiles, "template", lambda facts: "Fantasy character art, mostly fantasy characters.")
    fake = Fake()

    assert run(fake) == {"excluded_nothing_clear": 1}
    assert profiles.profiles() == [] and fake.requests == []


def test_every_card_the_template_writes_passes_the_print_check_and_names_nothing_beyond_its_share():
    chooser = random.Random(7)
    own = ["dinosaur", "paleoart", "creaturedesign", "fantasyart", "scifi", "characterdesign", "anime", "sketch"]
    mediums = [*["digital painting"] * 3, *(medium for medium in profiles.MEDIUM_FORMS if medium != "pixel art")]
    cards = set()
    for _ in range(600):
        subjects, genres = (
            chooser.sample(list(profiles.SUBJECT_FORMS), 2),
            chooser.sample(list(profiles.GENRE_FORMS), 3),
        )
        medium = chooser.choice(mediums)
        works = [
            {
                "id": str(n),
                "result": tags(
                    medium if chooser.random() < 0.8 else "unclear",
                    [subject for subject in subjects if chooser.random() < 0.6],
                    [genre for genre in genres if chooser.random() < 0.6],
                    [],
                ),
                "declared": "no",
                "tags": chooser.sample(own, 3),
            }
            for n in range(chooser.randint(5, 14))
        ]
        found, reason = profiles.facts_of(works)
        if reason:
            continue
        text = profiles.template(found)
        cards.add(text)
        assert profiles.unfit(text) is None, text
        first, pieces = text.split(". ")[0].rstrip(".").lower(), found["eligible"]
        assert first.count("mostly") <= 1 and first.count("mainly") <= 1, text
        # round 7's critic: no "Artwork" for an opening, no "partly", and none of the artist's tags in it
        assert not re.match("art(work)?\\b", first) and "partly" not in first and '"' not in text, text
        for term, places in found["tagged"].items():
            named = any(re.search(rf"\b{re.escape(form.lower())}\b", first) for form in profiles.FORMS[term][:2])
            if len(places) < 4 or 100 * len(places) < 40 * pieces or term == profiles.COMMON_MEDIUM:
                assert not named, (term, text)
            elif term in (*profiles.WORLD_FORMS, *profiles.MEDIUM_FORMS) and 100 * len(places) < 70 * pieces:
                assert not named, (term, text)  # a genre and a medium behind no hedge
            elif term in profiles.SUBJECT_FORMS and 100 * len(places) >= 70 * pieces:
                # what most works show stands flat, in the kind of art itself: never behind "mostly"
                assert re.search(rf"\b{profiles.FORMS[term][0]}\b", first.split(",")[0]), (term, text)
                assert not first.startswith("mostly"), text
    assert len(cards) > 200


# 6. The embedding.


def test_the_vector_is_1024_numbers_of_unit_length_made_from_the_card_and_every_fact_by_name(monkeypatch):
    monkeypatch.setattr(profiles, "SHARED", 1)
    own = dict.fromkeys(range(6), ["warrior", "creaturedesign"])
    artist(ALPHA, [tags(genres=["fantasy", "anime style"])] * 6, tagline="Commissions open", own_tags=own)
    fake = Fake()

    assert run(fake) == {"profiled": 1, "open_for_work": 1, "with_own_tags": 1}

    (profile,) = profiles.profiles()
    text = (
        "Anime fantasy character art. Open for commissions.\n"
        "Specialties: character.\nIn the artist's words: warrior, creaturedesign.\n"
        "Mediums: digital painting.\nGenres: fantasy, anime style.\nStyle: painterly."
    )
    assert profile["template"] == text.splitlines()[0]
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

    def count(claim, pictures):  # the first artist's two counts are answered, and nothing after them
        asked.append(claim)
        return trouble if len(asked) > 2 else 0

    with pytest.raises(SystemExit) as stopped:
        profiles.main(["--max-artists", "5"], transport=httpx.MockTransport(Fake(count=count)))

    assert str(stopped.value).startswith("stopped: Ollama did not answer (")
    assert capsys.readouterr().out == ""
    assert [profile["artist_key"] for profile in profiles.profiles()] == [f"da:{ALPHA}"]


# 7. Where requests go: a loopback Ollama, and for --parity the eval queries to Workers AI.


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

    # All a model is asked about an artist now: no proposal, no look at each work, no sentence, no "fit to print".
    assert [(request.url.host, request.url.port, request.url.path) for request in fake.requests] == [
        ("127.0.0.1", 11434, "/api/chat"),  # the sheet: the count of what is unsuitable
        ("127.0.0.1", 11434, "/api/chat"),  # and of the one subject repeated most
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


# 8. Removal, reruns and what is printed.


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


def test_nothing_is_stored_for_an_artist_removed_while_a_model_is_at_work():
    artist(ALPHA, [tags()] * 6)
    artist(BRAVO, [tags()] * 6)

    def count(claim, pictures):
        store.mark_removed(ALPHA)
        return 0

    assert run(Fake(count=count)) == {"profiled": 1}
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
    assert len(fake.sheets) == 2 and len(table()) == 2  # the two counts on one artist's sheet
    assert profiles.profiles()[0]["facts"]["counts"]["subjects"] == {"character": 5, "creature": 1}

    with closing(sqlite3.connect(config.LOCAL_DB)) as conn, conn:  # the collector no longer holds their works
        conn.execute("DELETE FROM works WHERE artist_key = ?", (f"da:{BRAVO}",))
    assert run(Fake()) == {"unchanged": 1, "removed": 1}  # their works are gone, so their row is too

    monkeypatch.setattr(profiles, "TEMPLATE_VERSION", profiles.TEMPLATE_VERSION + 1)
    assert profiles.sample(5, 1) == []  # a card as an older version made it is not one to show the critic
    assert run(Fake()) == {"profiled": 1}  # a new version of the card makes it again
    assert profiles.profiles()[0]["template_version"] == profiles.TEMPLATE_VERSION and len(profiles.sample(5, 1)) == 1


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


def test_an_image_replaced_while_the_stage_is_at_work_is_not_counted_under_the_old_fingerprint(monkeypatch):
    """The row's basis names the images it was made from. A sheet of any other image would make a row that says
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
        "with_own_tags": 0,
        "seconds_per_artist": 0,
    }
    assert printed.err == ""


def test_the_sample_prints_one_json_object_a_line_for_the_critic(capsys, monkeypatch):
    monkeypatch.setattr(profiles, "SHARED", 1)
    own = {0: ["Warrior", "knight"], 1: ["warrior"], 2: ["warrior"], 3: ["warrior", "knight"], 4: ["knight"]}
    own |= {5: ["knight", "elf"], 6: ["WARRIOR", "elf"]}
    ids = artist(ALPHA, [tags()] * 6 + [tags(explicit=True)], tagline="Commissions open", own_tags=own)
    artist(BRAVO, [tags()] * 6)
    artist(CHARLIE, [tags()] * 4)
    run(Fake())
    capsys.readouterr()

    profiles.main(["--sample", "20", "--seed", "3"])

    lines = [json.loads(line) for line in capsys.readouterr().out.splitlines()]
    assert sorted(line["artist"] for line in lines) == [f"da:{ALPHA}", f"da:{BRAVO}"]  # never an excluded artist
    alpha = next(line for line in lines if line["artist"] == f"da:{ALPHA}")
    assert set(alpha) == {
        *("artist", "template", "written", "written_by_model", "facts", "open_for_work", "open_evidence"),
        *("images", "images_left_out", "own_tags"),
    }
    assert alpha["template"] == alpha["written"] == "Fantasy character art. Open for commissions."
    # exactly the tags the site shows as theirs, each with the number of the works that count which carry it
    assert alpha["own_tags"] == {"warrior": 4, "knight": 4, "elf": 1}
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


# 9. The sheet: two counts that no card states, for the push stage to weigh.


def asked_for(fake, claim):
    return [asked for asked in fake.sheets if asked["claim"] == claim]


def test_the_sheet_is_built_only_from_the_eligible_works():
    results = [tags()] * 3 + [tags(explicit=True)] + [tags()] * 2 + [tags(not_artwork=True)] + [tags()] * 3
    artist(ALPHA, results, declared={9: "yes"})
    fake = Fake()

    assert run(fake) == {"profiled": 1}

    assert len(fake.sheets) == 2 and all(
        asked["works"] == [0, 1, 2, 4, 5, 7, 8] for asked in fake.sheets
    )  # never 3, 6, 9
    assert all("7 pictures, numbered 1 to 7" in asked["question"] for asked in fake.sheets)
    assert profiles.profiles()[0]["pictures"] == 7


def test_an_artist_whose_pictures_will_not_open_has_no_sheet_and_no_profile():
    ids = artist(ALPHA, [PLAIN] * 6)
    for work_id in ids[:2]:
        (library.image_dir() / f"{work_id}.png").write_bytes(PNG + b"FAKE-NOT-A-PICTURE")
    fake = Fake()

    assert run(fake) == {"excluded_nothing_clear": 1}  # four pictures open: too few to weigh anything on
    assert fake.requests == [] and profiles.profiles() == []


def test_a_sheet_request_goes_to_the_loopback_model_and_carries_the_pictures_and_nothing_of_the_artists_own():
    own = dict.fromkeys(range(6), ["fakeowntag", "warrior", "digitalart"])
    artist(ALPHA, [PLAIN] * 6, tagline="FAKE-TAGLINE", own_tags=own)
    fake = Fake()

    assert run(fake) == {"profiled": 1}

    assert all((request.url.host, request.url.port) == ("127.0.0.1", 11434) for request in fake.requests)
    for asked in fake.sheets:
        system, user = asked["body"]["messages"]
        assert asked["body"]["model"] == profiles.TAGGER and set(user) == {"role", "content", "images"}
        assert len(user["images"]) == 1  # the one sheet, and no picture besides
        told = (system["content"] + user["content"]).lower()
        for kept_back in ("zz-fake", "fake-title", "fakeowntag", "fake-tagline", "digitalart", "00000000-0000"):
            assert kept_back not in told
    # nothing a card states is counted on the sheet, and nothing is proposed from it: the two for the push stage
    assert [asked["claim"] for asked in fake.sheets] == [profiles.UNSUITABLE, profiles.SAME]
    assert profiles.profiles()[0]["template"] == "Character art."

    again = Fake()
    assert run(again) == {"unchanged": 1}  # and a rerun asks nothing a second time
    assert again.requests == []


def test_the_sexualised_or_gory_pictures_are_counted_and_handed_on_for_the_push_stage_to_weigh():
    artist(ALPHA, [PLAIN] * 10)
    artist(BRAVO, [PLAIN] * 10)
    seen = {profiles.UNSUITABLE: iter([7, 0]), profiles.SAME: iter([10, 2])}
    fake = Fake(count=lambda claim, pictures: next(seen[claim]))

    assert run(fake) == {"profiled": 2}  # counted, stored, and nobody is left out here on the strength of it

    alpha, bravo = profiles.profiles()
    assert (alpha["pictures"], alpha["unsuitable_pictures"], alpha["same_subject_pictures"]) == (10, 7, 10)
    assert (bravo["pictures"], bravo["unsuitable_pictures"], bravo["same_subject_pictures"]) == (10, 0, 2)
    assert "sexualised" not in alpha["template"] + alpha["written"] + json.dumps(alpha["open_evidence"])
    question = asked_for(fake, profiles.UNSUITABLE)[0]["question"]
    for told in (  # round 4: fan service in 4 pictures of 10, and one fetish subject in 10 of 10, both under the bar
        "It means: the framing or the dress of a figure is sexualised, whether or not anything is explicit",
        "one fetish subject is the point of the picture",
        "strong gore",
        "in doubt, list it",
    ):
        assert told in question
    assert "the same part of a body in close-up" in asked_for(fake, profiles.SAME)[0]["question"]


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
    assert len(asked_for(fake, profiles.UNSUITABLE)) == 1 + tagging.MAX_ATTEMPTS  # asked again, and then left


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
        "tags": [USUAL],
        "thumb_url": thumb,
        "published": str(int(RECENT.timestamp())),
        "fetched": image is not None,
    }
    library.store_artist(
        f"da:{handle}", user_id=None, tagline=None, specialty=None, level="Professional", website=None, works=[row]
    )


def test_an_artist_whose_work_was_read_from_an_image_they_replaced_waits_and_the_row_is_made_again(monkeypatch):
    # One moment for every row these collections write. collected_again answers with one work, not the artist's
    # whole gallery, so a second between the two would leave the others "missing from the latest answer" (_FALLEN)
    # and fail the test at random, as it did one run in five.
    monkeypatch.setattr(store, "_now", lambda: "2026-10-14T12:00:00+00:00")
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


def test_a_row_is_not_current_while_a_work_it_counted_has_words_nobody_read_or_words_that_say_otherwise(monkeypatch):
    monkeypatch.setattr(store, "_now", lambda: "2026-10-14T12:00:00+00:00")  # one moment, as in the test above
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


def test_a_row_is_not_current_while_the_verified_tags_it_counted_are_not_the_ones_that_stand():
    """The third critic of the push stage: a card from the tags of an older prompt version went as current."""
    ids = artist(ALPHA, [tags()] * 6)
    assert run(Fake())["profiled"] == 1
    kept, key = table(), f"da:{ALPHA}"
    with closing(sqlite3.connect(config.LOCAL_DB)) as conn, conn:  # a new prompt version: none read under it yet
        conn.execute("UPDATE work_tags SET prompt_version = prompt_version - 1")
    assert profiles.current() == set()
    for work_id in ids:  # read again under it, from the same images, and found to say the same
        tag(work_id, tags())
    assert profiles.current() == {key} and run(Fake()) == {"unchanged": 1} and table() == kept
    tag(ids[0], tags(subjects=["creature"]))  # one of them read again, and otherwise
    assert profiles.current() == set()
    assert run(Fake())["profiled"] == 1 and profiles.current() == {key}


def test_a_work_collected_after_the_row_was_made_leaves_the_row_current(monkeypatch):
    # One moment for every row these collections write. collected_again answers with one work, not the artist's
    # whole gallery, so a second between the two would leave the others "missing from the latest answer" (_FALLEN)
    # and fail the test at random, as it did one run in five.
    monkeypatch.setattr(store, "_now", lambda: "2026-10-14T12:00:00+00:00")
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
