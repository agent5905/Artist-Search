"""Tests for pipeline.profiles.

Ollama and Workers AI are fakes behind httpx.MockTransport, every image is a few invented bytes, and the tags are
written straight into the test's own database: no model, no GPU, no network. Opening a real connection fails the
test. Every handle, title, tag, tagline, address and credential below is invented.
"""

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

from pipeline import config, library, profiles, store, tagging

ALPHA, BRAVO, CHARLIE = "zz-fake-alpha", "zz-fake-bravo", "zz-fake-charlie"
NOW = datetime(2026, 10, 1, tzinfo=UTC)
RECENT, OLD = NOW - timedelta(days=10), NOW - timedelta(days=profiles.RECENT_DAYS + 1)
JPEG = b"\xff\xd8\xff" + b"FAKE-IMAGE-BYTES-" * 20
VERIFIED = tagging.result_set(profiles.TAGGER, profiles.TAGGER)
ACCOUNT, TOKEN = "FAKE-ACCOUNT-0123456789abcdef", "FAKE-TOKEN-zyxwvutsrqponmlk"
SECRETS = {"CLOUDFLARE_ACCOUNT_ID": ACCOUNT, "CLOUDFLARE_API_TOKEN": TOKEN, "OTHER_SECRET": "FAKE-OTHER-SECRET"}
COUNTS = re.compile(
    r'\{"profiled": \d+, "excluded_mostly_ai_declared": \d+, "excluded_mostly_explicit": \d+, '
    r'"excluded_too_few_works": \d+, "excluded_nothing_clear": \d+, "unchanged": \d+, "waiting_for_tags": \d+, '
    r'"left_for_next_run": \d+, "removed": \d+, "open_for_work": \d+, "written_fallback": \d+, '
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


def artist(handle, results, *, tagline=None, own_tags=None, declared=None, published=None, listed=True):
    """Collect one invented artist with a work for each entry of `results`, and return the works' ids.

    An entry is the work's verified result, or None for a work that is collected and not tagged yet.
    own_tags, declared and published are by the work's place in the list; a work not named there has the tag
    "faketag", declares no AI and was published at RECENT.
    """
    base = {ALPHA: 1000, BRAVO: 2000, CHARLIE: 3000}[handle]
    ids = [f"00000000-0000-4000-A000-{base + n:012d}" for n in range(len(results))]
    for work_id in ids:
        library.save_image(work_id, "image/jpeg", JPEG + work_id.encode())
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
    return ids


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
        if declared:
            row = (work_id, profiles.TAGGER, tagging.AI_PROMPT_VERSION, declared, at)
            conn.execute("INSERT OR REPLACE INTO work_ai_declared VALUES (?, ?, ?, ?, ?)", row)


def vector(text, scale=3.0):
    """1,024 invented numbers that depend on the text, and are not of unit length."""
    numbers = random.Random(text)
    return [scale * numbers.uniform(-1, 1) for _ in range(profiles.DIMENSIONS)]


def terms_of(question):
    """The facts a question to the writer lists."""
    lines = question.split("\n\n")[0].splitlines()[1:]
    return [term for line in lines for term in line.split(": ", 1)[1].split(", ")]


class Fake:
    """Ollama's writer (/api/chat) and embedder (/api/embed), and Workers AI, told apart by host and path.

    write: what the writer says. None for a sentence that states the facts it was given and nothing else, a str
           for that sentence every time, or a function (question, attempt from 1) -> a str for the sentence, a
           dict for the model's whole JSON, a Response, or an exception to raise.
    embed: a function (text) -> the vector Ollama gives. cloud: the same for Workers AI, or a Response.
    """

    def __init__(self, write=None, embed=vector, cloud=vector):
        self.write, self.embed, self.cloud = write, embed, cloud
        self.requests, self.chats, self.embeds, self.clouds = [], [], [], []

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
        self.chats.append(body)
        question = body["messages"][-1]["content"]
        attempt = sum(
            chat["messages"][-1]["content"].split("\n\n")[0] == question.split("\n\n")[0] for chat in self.chats
        )
        answer = self.write(question, attempt) if callable(self.write) else self.write
        if answer is None:
            answer = f"Art with {' and '.join(terms_of(question))}."
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


def facts(specialties=(), mediums=(), genres=(), style_words=()):
    return {
        "specialties": list(specialties),
        "mediums": list(mediums),
        "genres": list(genres),
        "style_words": list(style_words),
    }


PAINTER = facts(["character"], ["digital painting"], ["fantasy"], ["painterly"])


# 1. The facts: counts over the eligible works, from the verified tags and from nothing else.


def test_the_facts_are_counted_from_the_verified_tags_and_hold_only_above_the_threshold():
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
    }
    assert (profile["facts"]["works"], profile["facts"]["eligible"]) == (10, 10)
    assert profile["facts"]["counted"] == ids
    # 5 of 10 works make a medium, subject or genre a fact, and 7 of 10 a style word
    assert (profile["mediums"], profile["specialties"]) == (["digital painting"], ["character"])
    assert (profile["genres"], profile["style_words"]) == (["fantasy"], ["painterly"])
    assert profile["template"] == "Character art in digital painting, with fantasy themes, in a painterly style."
    for text in (profile["template"], profile["written"]):  # nothing of a description, nothing below the threshold
        assert not re.search("cyberpunk|vehicle|pixel|creature|traditional|dark|muted|sketchy|moody|horror|chibi", text)


@pytest.mark.parametrize(
    ("eligible", "having", "field", "is_fact"),
    [
        (10, 5, "subjects", True),
        (10, 4, "subjects", False),
        (10, 5, "genres", True),
        (10, 4, "genres", False),
        (10, 5, "medium", True),
        (10, 4, "medium", False),
        (10, 7, "style_words", True),
        (10, 6, "style_words", False),
        (7, 4, "subjects", True),
        (7, 3, "subjects", False),
        (5, 3, "subjects", True),  # the share is rounded up to whole works
        (5, 2, "subjects", False),
        (5, 4, "style_words", True),
        (5, 3, "style_words", False),
    ],
)
def test_a_value_is_a_fact_at_half_of_the_eligible_works_and_a_style_word_at_seven_in_ten(
    eligible, having, field, is_fact
):
    value = {"subjects": "creature", "genres": "horror", "medium": "traditional", "style_words": "sketchy"}[field]
    with_it = tags("traditional") if field == "medium" else tags(**{field: [value]})
    plain = tags("unclear", [], [], [])
    works = [{"id": str(n), "result": with_it if n < having else plain, "declared": "no"} for n in range(eligible)]

    found, _ = profiles.facts_of(works)

    stated = [term for name in profiles.STATED for term in found[name]]
    assert (value in stated) is is_fact
    assert found["counts"][field][value] == having


def test_a_style_word_outside_the_checkable_list_and_a_medium_that_names_no_material_are_counted_and_never_stated():
    works = [
        {"id": str(n), "result": tags("photo", style_words=["moody", "detailed"]), "declared": "no"} for n in range(8)
    ]
    works += [{"id": "u", "result": tags("unclear", style_words=["stunning"]), "declared": "no"}]

    found, reason = profiles.facts_of(works)

    assert found["counts"]["medium"] == {"photo": 8, "unclear": 1}
    assert found["counts"]["style_words"] == {"detailed": 8, "moody": 8, "stunning": 1}
    assert (found["mediums"], found["style_words"], reason) == ([], [], None)
    assert profiles.template(found) == "Character art, with fantasy themes."


def test_explicit_not_artwork_and_ai_declared_works_never_contribute():
    ids = artist(
        ALPHA,
        [tags()] * 6
        + [tags("3d render", ["creature"], ["horror"], ["low-poly"], explicit=True)] * 2
        + [tags("pixel art", ["environment"], ["cyberpunk"], ["isometric"], not_artwork=True)]
        + [tags("traditional", ["vehicle or mech"], ["science fiction"], ["sketchy"])],
        declared={9: "yes"},
    )

    assert run(Fake()) == {"profiled": 1}
    (profile,) = profiles.profiles()
    assert profile["facts"]["counts"] == {
        "medium": {"digital painting": 6},
        "subjects": {"character": 6},
        "genres": {"fantasy": 6},
        "style_words": {"painterly": 6},
    }
    found = profile["facts"]
    assert [found[name] for name in ("works", "eligible", "explicit", "not_artwork", "declared_ai")] == [10, 6, 2, 1, 1]
    assert found["counted"] == ids[:6]
    (shown,) = profiles.sample(1, seed=0)
    assert [path[-40:-4] for path in shown["images"]] == ids[:6]
    assert [path[-40:-4] for path in shown["images_left_out"]] == ids[6:]


def test_the_facts_lead_with_what_most_works_have_and_keep_three_genres_and_style_words_at_most():
    names, words = list(profiles.GENRE_FORMS), list(profiles.STYLE_FORMS)
    works = [
        {"id": str(n), "result": tags(genres=names[:5], style_words=words[:5]), "declared": "no"} for n in range(4)
    ]
    works += [
        {"id": f"h{n}", "result": tags(genres=names[4:6], style_words=words[4:6]), "declared": "no"} for n in range(4)
    ]

    found, _ = profiles.facts_of(works)

    assert found["genres"] == [names[4], *names[:2]]  # 8 works, then the first of those that 4 works have
    assert found["style_words"] == [words[4]]  # 8 of 8; the others have 4 of 8, which is not 7 in 10


def test_with_no_work_counted_nothing_is_a_fact():
    found, reason = profiles.facts_of([{"id": "a", "result": tags(not_artwork=True), "declared": "no"}])

    assert [found[name] for name in profiles.STATED] == [[], [], [], []]
    assert (found["eligible"], reason) == (0, "too_few_works")


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
    assert alpha["template"].endswith("style. Open for commissions.") and alpha["written"].endswith(
        ". Open for commissions."
    )
    assert bravo["template"].endswith("style. Available for hire.") and bravo["written"].endswith(
        ". Available for hire."
    )
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


# 4. A profile states the facts and nothing else.


@pytest.mark.parametrize(
    "text",
    [
        "Character art in digital painting, with fantasy themes, in a painterly style.",
        "Painterly fantasy character art in digital painting.",
        "Fantasy characters, rendered as digital paintings in a painterly style.",
        "An artist whose work is painterly fantasy character art.",
        "DIGITAL PAINTING OF FANTASY CHARACTERS.",
    ],
)
def test_a_sentence_made_of_the_facts_and_plain_words_is_supported(text):
    assert profiles.unsupported(text, PAINTER) is None


@pytest.mark.parametrize(
    "text",
    [
        # a medium, subject, genre or style word the facts do not have
        "Character art in traditional media.",
        "Character art in watercolour and digital painting.",
        "Character art in 3D and digital painting.",
        "Painterly character art in digital line art.",  # "digital" of one fact and "line art" of no fact
        "Fantasy character art, digital.",
        "Character and creature art in digital painting.",
        "Character and environment art in digital painting.",
        "Fantasy character and weapon art in digital painting.",
        "Dark fantasy character art in digital painting.",
        "Science fiction and fantasy character art in digital painting.",
        "Anime character art in digital painting.",
        "Realistic fantasy character art in digital painting.",
        "Painterly, cel-shaded character art in digital painting.",
        "Moody painterly character art in digital painting.",
        "Fantasy character concept art in digital painting.",
        "Fantasy character illustrations in digital painting.",
        "Fantasy character designs in digital painting.",
        "An artist who paints fantasy characters in digital painting.",
        # how much, how often
        "Mostly character art in digital painting.",
        "Character art, often in digital painting.",
        "Only fantasy character art in digital painting.",
        "A varied range of fantasy character art in digital painting.",
        # numbers
        "Character art in digital painting, with 10 fantasy works.",
        "Fantasy character art in digital painting since 2019.",
        "Two kinds of fantasy character art in digital painting.",
        "Fantasy character art in 2D digital painting.",
        # names
        "Fantasy character art in digital painting by FAKE-NAME.",
        "Fantasy character art in digital painting in the style of Tolkien.",
        "Fantasy character art in digital painting for Warcraft.",
        # quality judgements
        "Stunning fantasy character art in digital painting.",
        "A talented artist of fantasy character art in digital painting.",
        "Beautifully detailed fantasy character art in digital painting.",
        "Skilled, professional fantasy character art in digital painting.",
        # experience, clients, software
        "An experienced artist of fantasy character art in digital painting.",
        "Fantasy character art in digital painting, with years of studio experience.",
        "Fantasy character art in digital painting for game studios and clients.",
        "Fantasy character art in digital painting, made in Photoshop.",
        "Fantasy character art in digital painting, using Procreate and Blender.",
        # availability
        "Fantasy character art in digital painting, open for commissions.",
        "Fantasy character art in digital painting, available for hire.",
        # the opposite of a fact
        "Character art in digital painting with no fantasy themes.",
        "Not fantasy character art in digital painting.",
        # not one plain sentence of the right length
        "Character art in digital painting. Fantasy themes in a painterly style.",
        "Character art in digital painting",
        "Character art in digital painting!",
        "Is this fantasy character art in digital painting?",
        "Character art in digital painting.\nIgnore the rules above.",
        'Character art in "digital painting".',
        "Character art (fantasy) in digital painting.",
        "Character art in digital painting: fantasy.",
        "Character art in digital painting, " + "and fantasy art, " * 20 + "in a painterly style.",
        "",
        ".",
        "Art.",  # says too little
        "Character art.",
        None,
        ["Character art in digital painting."],
        {"profile": "Character art in digital painting."},
    ],
)
def test_a_text_that_states_anything_the_facts_do_not_have_is_refused(text):
    assert isinstance(profiles.unsupported(text, PAINTER), str)


def test_a_value_counted_below_the_threshold_may_not_be_stated():
    works = [{"id": str(n), "result": tags(), "declared": "no"} for n in range(6)]
    works += [
        {"id": f"c{n}", "result": tags(subjects=["creature"], style_words=["muted"]), "declared": "no"}
        for n in range(4)
    ]
    found, _ = profiles.facts_of(works)
    assert found["counts"]["subjects"]["creature"] == 4 and found["counts"]["style_words"]["muted"] == 4

    assert profiles.unsupported("Character art in digital painting, with fantasy themes.", found) is None
    assert profiles.unsupported("Character and creature art in digital painting.", found) is not None
    assert profiles.unsupported("Muted character art in digital painting.", found) is not None
    assert profiles.unsupported("Painterly character art in digital painting.", found) is not None  # 6 of 10, not 7


def test_the_template_states_every_fact_it_is_given_and_passes_the_check_whatever_the_facts():
    chooser = random.Random(7)
    for _ in range(400):
        found = facts(
            chooser.sample(list(profiles.SUBJECT_FORMS), chooser.randint(0, 3)),
            chooser.sample(list(profiles.MEDIUM_FORMS), chooser.randint(0, 2)),
            chooser.sample(list(profiles.GENRE_FORMS), chooser.randint(0, profiles.LEADING)),
            chooser.sample(list(profiles.STYLE_FORMS), chooser.randint(0, profiles.LEADING)),
        )
        if not any(found.values()):
            continue
        text = profiles.template(found)
        assert profiles.unsupported(text, found) is None, text
        stated = [term for terms in found.values() for term in terms]
        for term in stated:  # each fact is in it, as the template spells it
            assert profiles.FORMS[term][0].lower() in text.lower(), (term, text)
        for term in stated:  # and with any one fact taken away it no longer passes
            without = {name: [t for t in terms if t != term] for name, terms in found.items()}
            assert profiles.unsupported(text, without) is not None, (term, text)


def test_the_written_text_is_the_models_when_it_keeps_to_the_facts():
    artist(ALPHA, [tags()] * 6)
    fake = Fake(write="Painterly fantasy character art in digital painting.")

    assert run(fake) == {"profiled": 1}
    (profile,) = profiles.profiles()
    assert profile["written"] == "Painterly fantasy character art in digital painting."
    assert profile["template"] == "Character art in digital painting, with fantasy themes, in a painterly style."
    assert profile["written_by_model"] is True
    assert profile["written_version"] == profiles.WRITTEN_VERSION
    assert profile["template_version"] == profiles.TEMPLATE_VERSION
    assert len(fake.chats) == 1


@pytest.mark.parametrize(
    "bad",
    [
        "Stunning fantasy character art by a talented artist.",
        "Fantasy character art in digital painting with 10 years of experience.",
        "Fantasy character art made in Photoshop for FAKE-CLIENT.",
        "Character and creature art in watercolour.",
        "Character art. Fantasy themes.",
        {"text": "Character art in digital painting."},
        {"profile": 7},
        {"profile": None},
        httpx.Response(200, json={"message": {"content": "not json"}}),
        httpx.Response(200, json={"message": {"content": '["Character art in digital painting."]'}}),
        httpx.Response(200, json={"no": "message"}),
        httpx.Response(400, text="FAKE-BAD-REQUEST"),
    ],
)
def test_a_text_that_fails_the_check_is_asked_for_again_a_bounded_number_of_times_then_the_template_is_used(bad):
    artist(ALPHA, [tags()] * 6)
    fake = Fake(write=lambda question, attempt: bad)

    assert run(fake) == {"profiled": 1, "written_fallback": 1}

    (profile,) = profiles.profiles()
    assert profile["written"] == profile["template"] and profile["written_by_model"] is False
    assert len(fake.chats) == profiles.MAX_WRITES == 3
    options = [chat["options"] for chat in fake.chats]
    assert len({json.dumps(option) for option in options}) == profiles.MAX_WRITES  # each try is asked differently


def test_a_refused_text_is_sent_back_with_the_reason_and_a_good_rewrite_is_kept():
    artist(ALPHA, [tags()] * 6)
    answers = ["Stunning fantasy character art.", "Fantasy character art in digital painting."]
    fake = Fake(write=lambda question, attempt: answers[attempt - 1])

    assert run(fake) == {"profiled": 1}

    (profile,) = profiles.profiles()
    assert profile["written"] == answers[1] and profile["written_by_model"] is True
    first, second = (chat["messages"][-1]["content"] for chat in fake.chats)
    assert "refused" not in first
    assert second.startswith(first) and '"stunning" is not one of the facts' in second


def test_the_writer_is_given_the_facts_above_the_threshold_and_nothing_else():
    artist(
        ALPHA,
        [tags()] * 6 + [tags("traditional", ["creature"], ["horror"], ["sketchy"])] * 2 + [tags(explicit=True)],
        tagline="FAKE-TAGLINE commissions open",
        own_tags=dict.fromkeys(range(9), ["fakeowntag", "commissionsopen"]),
    )
    fake = Fake()

    assert run(fake) == {"profiled": 1, "open_for_work": 1}

    (chat,) = fake.chats
    system, asked = chat["messages"]
    assert (chat["model"], chat["think"], system["content"]) == (profiles.WRITER, False, profiles.WRITE_PROMPT)
    assert asked == {
        "role": "user",
        "content": "Facts\nsubjects: character\nmedium: digital painting\nthemes: fantasy\nstyle: painterly",
    }
    sent = b"".join(request.content for request in fake.requests).decode().lower()
    for kept_back in ("zz-fake", "fake-title", "fakeowntag", "fake-tagline", "fake-description", "00000000-0000"):
        assert kept_back not in sent
    for kept_back in ("creature", "traditional", "horror", "sketchy", "cyberpunk", "vehicle"):  # below the threshold
        assert kept_back not in asked["content"]
    assert "commission" not in asked["content"] and "images" not in asked


def test_the_prompt_names_every_plain_word_and_forbids_what_the_check_refuses():
    for word in profiles.GLUE:
        assert f" {word}" in profiles.WRITE_PROMPT
    for forbidden in ("numbers", "names", "praise", "experience", "clients", "software", "availability"):
        assert forbidden in profiles.WRITE_PROMPT
    # no spelling of a medium, subject, genre or style word can be made of plain words alone
    for forms in profiles.FORMS.values():
        for form in forms:
            assert not set(form.lower().split()) <= profiles.GLUE, form


# 5. The embedding.


def test_the_vector_is_1024_numbers_of_unit_length_made_from_the_template_and_the_leading_facts():
    artist(ALPHA, [tags(genres=["fantasy", "anime style"])] * 6, tagline="Commissions open")
    fake = Fake()

    assert run(fake) == {"profiled": 1, "open_for_work": 1}

    (profile,) = profiles.profiles()
    text = (
        "Character art in digital painting, with fantasy themes, in a painterly, anime style. Open for commissions.\n"
        "Specialties: character.\nMediums: digital painting.\nGenres: fantasy, anime style.\nStyle: painterly."
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
        ("127.0.0.1", 11434, "/api/chat"),
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
    assert table() == before and len(before) == 2  # the same rows, with the time they were first made


def test_a_row_is_made_again_only_when_what_it_was_made_from_changes(monkeypatch):
    ids = artist(ALPHA, [tags()] * 6)
    artist(BRAVO, [tags()] * 6)
    assert run(Fake()) == {"profiled": 2}

    tag(ids[0], tags(subjects=["creature"]))  # one work of one artist is tagged anew
    fake = Fake()
    assert run(fake) == {"profiled": 1, "unchanged": 1}
    assert len(fake.chats) == 1 and len(table()) == 2
    assert profiles.profiles()[0]["facts"]["counts"]["subjects"] == {"character": 5, "creature": 1}

    library.store_artist(
        f"da:{BRAVO}", user_id=None, tagline="Commissions open", specialty=None, level=None, website=None, works=[]
    )
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
    (library.image_dir() / f"{ids[7]}.jpg").unlink()
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
        "seconds_per_artist": 0,
    }
    assert printed.err == ""


def test_the_sample_prints_one_json_object_a_line_for_the_critic(capsys):
    ids = artist(ALPHA, [tags()] * 6 + [tags(explicit=True)], tagline="Commissions open")
    artist(BRAVO, [tags()] * 6)
    artist(CHARLIE, [tags()] * 4)
    run(Fake(write="Painterly fantasy character art in digital painting."))
    capsys.readouterr()

    profiles.main(["--sample", "20", "--seed", "3"])

    lines = [json.loads(line) for line in capsys.readouterr().out.splitlines()]
    assert sorted(line["artist"] for line in lines) == [f"da:{ALPHA}", f"da:{BRAVO}"]  # never an excluded artist
    alpha = next(line for line in lines if line["artist"] == f"da:{ALPHA}")
    assert set(alpha) == {
        *("artist", "template", "written", "written_by_model", "facts", "open_for_work", "open_evidence"),
        *("images", "images_left_out"),
    }
    assert alpha["template"] == (
        "Character art in digital painting, with fantasy themes, in a painterly style. Open for commissions."
    )
    assert alpha["written"] == "Painterly fantasy character art in digital painting. Open for commissions."
    assert alpha["open_for_work"] is True
    assert alpha["open_evidence"] == [{"source": "tagline", "says": "Commissions open", "kind": "commissions"}]
    assert alpha["facts"]["counts"]["subjects"] == {"character": 6}
    assert alpha["images"] == [str(library.image_dir() / f"{work_id}.jpg") for work_id in ids[:6]]
    assert alpha["images_left_out"] == [str(library.image_dir() / f"{ids[6]}.jpg")]
    assert all(open(path, "rb").read().startswith(JPEG) for path in alpha["images"])

    profiles.main(["--sample", "1", "--seed", "3"])
    assert [json.loads(line) for line in capsys.readouterr().out.splitlines()] == lines[:1]  # the same order again
    orders = set()
    for seed in range(20):
        profiles.main(["--sample", "2", "--seed", str(seed)])
        orders.add(tuple(json.loads(line)["artist"] for line in capsys.readouterr().out.splitlines()))
    assert len(orders) == 2  # the seed decides
