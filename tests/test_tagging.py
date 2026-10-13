"""Tests for pipeline.tagging.

Ollama is a fake behind httpx.MockTransport and every image is a few invented bytes: no model, no GPU, no network.
Opening a real connection fails the test. Every handle, title, tag, address and answer below is invented.
"""

import base64
import contextlib
import json
import re
import shutil
import socket
import sqlite3
from contextlib import closing

import httpx
import pytest

from pipeline import config, library, tagging

ALPHA, BRAVO = "da:zz-fake-alpha", "da:zz-fake-bravo"
JPEG = b"\xff\xd8\xff" + b"FAKE-IMAGE-BYTES-" * 20
GOOD = {  # a model's first answer
    "description": "A red circle on a blue field.",
    "medium": "digital painting",
    "subjects": ["character"],
    "genres": ["fantasy"],
    "manner": "painted illustration",
    "style_words": ["flat-colour"],
    "explicit": False,
    "not_artwork": False,
}
KEPT = {name: value for name, value in GOOD.items() if name != "manner"}  # what is stored of it
READ = {"declared_ai": "no", "contact_in_text": "no", "formats": None, "rating": None}  # what results() adds
COUNTS = re.compile(
    r'\{"analyzed": \d+, "failed": \d+, "flagged": \d+, "skipped": \d+, "removed": \d+, '
    r'"descriptions_changed": \d+, "details_removed": \d+, "declared_yes": \d+, '
    r'"declared_no": \d+, "declared_unclear": \d+, "contact_yes": \d+, "contact_no": \d+, '
    r'"tagline_contact_yes": \d+, "tagline_contact_no": \d+, "seconds_per_image": \d+\.\d+, '
    r'"seconds_per_verification": \d+\.\d+\}\n'
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
    monkeypatch.setattr(sqlite3, "connect", unsynced)  # each work is its own commit, and the disk here is slow
    monkeypatch.setattr(tagging.time, "sleep", lambda seconds: None)  # the waits between tries of a failed request

    def no_network(*args, **kwargs):
        raise AssertionError("a real connection was attempted")

    monkeypatch.setattr(socket.socket, "connect", no_network)
    monkeypatch.setattr(socket, "create_connection", no_network)


def works(*numbers, artist=ALPHA, without_image=(), tags=None, tagline=None):
    """Collect these invented works as all the artist has. Each image is unique to its work: JPEG, then its number.

    A work's title is FAKE-TITLE-<n> and its tags are ["faketag<n>"], or tags[n] when that is given.
    """
    ids = [f"00000000-0000-4000-A000-{n:012d}" for n in numbers]
    for n, work_id in zip(numbers, ids, strict=True):
        if n not in without_image:
            library.save_image(work_id, "image/jpeg", JPEG + str(n).encode())
    rows = [
        {
            "work_id": work_id,
            "page_url": f"https://www.deviantart.com/zz-fake-alpha/art/FAKE-TITLE-{n}",
            "title": f"FAKE-TITLE-{n}",
            "tags": (tags or {}).get(n, [f"faketag{n}"]),
            "thumb_url": "https://images-wixmp-zzfake.wixmp.com/f/FAKE-THUMB.jpg",
            "published": "1700000000",
        }
        for n, work_id in zip(numbers, ids, strict=True)
    ]
    library.store_artist(artist, user_id=None, tagline=tagline, specialty=None, level=None, website=None, works=rows)
    return ids


class FakeOllama:
    """Answers /api/show and the four questions /api/chat is asked, told apart by their system prompt.

    answer:   the first pass. What the model says, or a function (work number, attempt, request) -> that. A dict
              is sent as the model's JSON, a str as it is, a Response whole, and an exception is raised.
    confirm:  the verifier on each tag of its one call: True, False, or a function (work number, tag, request) ->
              that. Anything else it gives for a tag is sent as the whole answer instead.
    look:     the verifier's own pick of manner and colour, in the same call.
    rewrite:  the verifier's check of the description: None to send it back as it was, a str to send that as the
              checked description, a function (work number, description, request) -> either or a whole answer.
    declared: the reader of title and tags for AI: "yes", "no", "unclear", or a function (request) -> an answer.
    contact:  the reader of a text for a private contact: "yes", "no", or a function (request) -> an answer.
    """

    def __init__(
        self,
        answer=GOOD,
        confirm=True,
        look=None,
        rewrite=None,
        declared="no",
        contact="no",
        formatted=None,
        second="A red circle.",
        agree=None,
        capabilities=("completion", "vision"),
    ):
        self.formatted = formatted or {"shows": "FAKE-LAYOUT", "format": "none of these", "also": "none of these"}
        self.formatted = self.formatted if callable(formatted) else {"rating": "safe"} | self.formatted
        self.second, self.agree = second, agree  # the second model's description, and the agreed one (None: the first)
        self.sorts, self.seconds, self.merges, self.order = [], [], [], []  # what round 4's passes asked, in order
        self.answer, self.confirm, self.declared, self.capabilities = answer, confirm, declared, capabilities
        self.contact, self.contact_readings = contact, []  # the text-only requests about a private contact
        self.look, self.rewrite = look or {"manner": "painted illustration", "colour": "mixed"}, rewrite
        self.requests, self.chats, self.attempts = [], [], {}  # chats and attempts: the first pass only
        self.checks, self.readings = [], []  # (work number, tag or "look", model) and the text-only requests
        self.rewrites = []  # (work number, the description sent, model)

    def __call__(self, request):
        self.requests.append(request)
        if request.url.path == "/api/show":
            return httpx.Response(200, json={"capabilities": list(self.capabilities)})
        assert request.url.path == "/api/chat"
        chat = json.loads(request.content)
        prompt, asked = chat["messages"][0]["content"], chat["messages"][-1]
        if prompt == tagging.AI_PROMPT:
            self.readings.append(chat)
            answer = self.declared(chat) if callable(self.declared) else self.declared
            return self.say({"says": "FAKE-REASON", "declared": answer} if answer in tagging.AI_ANSWERS else answer)
        if prompt == tagging.CONTACT_PROMPT:
            self.contact_readings.append(chat)
            answer = self.contact(chat) if callable(self.contact) else self.contact
            found = {"found": "FAKE-FOUND", "contact": answer}
            return self.say(found if answer in tagging.CONTACT_ANSWERS else answer)
        if prompt == tagging.AGREE_PROMPT:
            self.merges.append(chat)
            self.order.append("merge")
            answer = self.agree(chat) if callable(self.agree) else self.agree
            answer = asked["content"].splitlines()[0].removeprefix("First description: ") if answer is None else answer
            return self.say({"differ": [], "description": answer} if isinstance(answer, str) else answer)
        (image,) = asked["images"]
        n = int(base64.b64decode(image).removeprefix(JPEG))
        if prompt == tagging.FORMAT_PROMPT:
            self.sorts.append((n, chat["model"]))
            return self.say(self.formatted(n, chat) if callable(self.formatted) else self.formatted)
        if prompt == tagging.SECOND_PROMPT:
            self.seconds.append((n, chat["model"]))
            self.order.append("second")
            answer = self.second(n, chat) if callable(self.second) else self.second
            return self.say({"description": answer} if isinstance(answer, str) else answer)
        if prompt == tagging.CHECK_PROMPT:
            tags = [tag for tag in chat["format"]["properties"] if tag not in tagging._LOOK]
            self.checks += [(n, tag, chat["model"]) for tag in ("look", *tags)]
            said = {tag: self.confirm(n, tag, chat) if callable(self.confirm) else self.confirm for tag in tags}
            whole = [value for value in said.values() if not isinstance(value, bool)]
            if whole or not isinstance(self.look, dict):
                return self.say(whole[0] if whole else self.look)
            return self.say({"faces": "FAKE-FACES", "about": "FAKE-ABOUT", **self.look, **said})
        if prompt == tagging.DESCRIPTION_PROMPT:
            description = asked["content"].removeprefix("Description: ")
            self.rewrites.append((n, description, chat["model"]))
            answer = self.rewrite(n, description, chat) if callable(self.rewrite) else self.rewrite
            answer = description if answer is None else answer
            empty = dict.fromkeys(tagging._DOUBTS, [])
            return self.say(empty | {"description": answer} if isinstance(answer, str) else answer)
        assert prompt == tagging.PROMPT
        self.chats.append(chat)
        self.attempts[n] = self.attempts.get(n, 0) + 1
        return self.say(self.answer(n, self.attempts[n], chat) if callable(self.answer) else self.answer)

    @staticmethod
    def say(answer):
        if isinstance(answer, Exception):
            raise answer
        if isinstance(answer, httpx.Response):
            return answer
        content = json.dumps(answer) if isinstance(answer, dict) else answer
        return httpx.Response(200, json={"message": {"role": "assistant", "content": content}})


def run(fake, max_works=500, **kwargs):
    """Run the stage against the fake. The counts come back without the throughputs, which are real durations."""
    counts = tagging.run(max_works, transport=httpx.MockTransport(fake), **kwargs)
    assert isinstance(counts.pop("seconds_per_image"), float)
    assert isinstance(counts.pop("seconds_per_verification"), float)
    return counts


def stored(table="work_tags"):
    """Every row of a table of this stage, without its timestamp, read straight from the file."""
    tagging.results()  # so the tables exist even when the call under test refused before opening the file
    with closing(sqlite3.connect(config.LOCAL_DB)) as conn:
        rows = conn.execute(f"SELECT * FROM {table} ORDER BY 1, 2, 3").fetchall()
    return [(*row[:3], json.loads(row[3]) if table == "work_tags" and row[3] else row[3]) for row in rows]


def in_vocabulary(result):
    return (
        result["medium"] in tagging.MEDIUMS
        and set(result["subjects"]) <= set(tagging.SUBJECTS)
        and set(result["genres"]) <= set(tagging.GENRES)
        and len(result["style_words"]) <= tagging.MAX_STYLE_WORDS
        and all(re.fullmatch(r"[a-z -]{2,24}", word) for word in result["style_words"])
        and 0 < len(result["description"]) <= tagging.MAX_DESCRIPTION
        and isinstance(result["explicit"], bool)
        and isinstance(result["not_artwork"], bool)
    )


# 1. Hard rule 9: a loopback Ollama and nothing else.


@pytest.mark.parametrize(
    "url",
    [
        "http://example.com:11434",
        "https://ollama.example.com",
        "http://localhost:11434",  # a name: it goes wherever the resolver says
        "http://127.0.0.1.example.com:11434",
        "http://127.0.0.1:11434@example.com",
        "http://127.0.0.1:11434@example.com:80/",
        "http://example.com#@127.0.0.1",
        "http://example.com/?127.0.0.1",
        "http://192.168.1.10:11434",
        "http://10.0.0.5:11434",
        "http://0.0.0.0:11434",
        "http://8.8.8.8",
        "http://[2001:db8::1]:11434",
        "http://[::ffff:8.8.8.8]:11434",
        "http://2130706433:11434",
        "http://127.1:11434",
        "http://0x7f.0.0.1:11434",
        "ftp://127.0.0.1",
        "file:///127.0.0.1",
        "127.0.0.1:11434",
        "//example.com",
        "http://",
        "",
    ],
)
def test_any_host_but_loopback_is_refused_before_a_byte_is_sent(url):
    works(1)
    fake = FakeOllama()
    with pytest.raises(ValueError, match="loopback"):
        run(fake, verifier="model-v", ollama_url=url)
    assert fake.requests == []  # not the image, not the title and tags
    assert [stored(table) for table in tagging.TABLES] == [[] for _ in tagging.TABLES]


@pytest.mark.parametrize(
    "url, host",
    [("http://127.0.0.1:11434", "127.0.0.1"), ("http://127.0.0.9:8080/", "127.0.0.9"), ("http://[::1]:11434", "::1")],
)
def test_a_loopback_ollama_gets_the_image_and_a_prompt_and_nothing_about_the_work(url, host):
    (work_id,) = works(1)
    fake = FakeOllama()
    assert run(fake, verifier="model-v", ollama_url=url)["analyzed"] == 1
    assert {request.url.host for request in fake.requests} == {host}
    assert {request.url.path for request in fake.requests} == {"/api/show", "/api/chat"}
    (chat,) = fake.chats
    assert chat["messages"][0] == {"role": "system", "content": tagging.PROMPT}
    assert chat["messages"][1]["images"] == [base64.b64encode(JPEG + b"1").decode()]
    assert chat["think"] is False and chat["stream"] is False
    assert chat["format"]["properties"]["medium"]["enum"] == list(tagging.MEDIUMS)
    assert chat["format"]["properties"]["subjects"]["items"]["enum"] == list(tagging.SUBJECTS)
    offered = chat["format"]["properties"]["genres"]["items"]["enum"] + chat["format"]["properties"]["manner"]["enum"]
    assert set(tagging.GENRES) < set(offered)
    with_image = [request.content for request in fake.requests if b'"images"' in request.content]
    assert len(with_image) == 3  # the first pass, then the verifier's two calls: the tags and the description
    for kept_back in (work_id, "zz-fake-alpha", "fake-title", "faketag", "deviantart", "wixmp", ".jpg"):
        assert kept_back.lower() not in b"".join(with_image).decode().lower()


def test_the_connection_takes_no_proxy_from_the_environment_and_follows_no_redirect(monkeypatch):
    for name in ("HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY"):
        monkeypatch.setenv(name, "http://proxy.example.com:8080")
    with tagging._client(tagging.OLLAMA_URL) as client:
        assert client.trust_env is False and client.follow_redirects is False
        assert client._mounts == {}  # httpx's own table of proxies: empty, so every request goes direct

    works(1)
    elsewhere = httpx.Response(307, headers={"Location": "http://example.com/api/chat"})
    fake = FakeOllama(elsewhere)
    with pytest.raises(httpx.HTTPStatusError):
        run(fake)
    assert {request.url.host for request in fake.requests} == {"127.0.0.1"}
    assert stored() == []


@pytest.mark.parametrize("blind", ["model-a", "model-v"])
def test_a_model_that_cannot_read_images_is_refused_before_it_is_asked_about_one(blind):
    works(1)

    class OneBlind(FakeOllama):
        def __call__(self, request):
            if request.url.path == "/api/show" and json.loads(request.content)["model"] == blind:
                return httpx.Response(200, json={"capabilities": ["completion", "tools"]})
            return super().__call__(request)

    fake = OneBlind()
    with pytest.raises(ValueError, match="cannot read images"):
        run(fake, model="model-a", verifier="model-v")
    assert not any(chat["model"] == blind for chat in fake.chats) and not any(
        blind == check[2] for check in fake.checks
    )
    assert tagging.results("model-a", verifier="model-v") == {}


def test_the_prompts_name_every_vocabulary_value_and_forbid_guessing():
    for value in (*tagging.MEDIUMS, *tagging.SUBJECTS, *tagging.GENRES):
        assert f"- {value}: " in tagging.PROMPT
    for value in tagging.GENRES:
        assert (f"- {value}: " in tagging.CHECK_PROMPT) == (value in tagging._MANNERS)
    for rule in ("only what you can see", "leave it out", "Never name or guess the artist, the software"):
        assert rule in tagging.PROMPT
    for rule in ("No left or right", "No numbers", "No gender, age or species", "Not how figures stand to one another"):
        assert rule in tagging.PROMPT
    assert "leaves no doubt" in tagging.CHECK_PROMPT
    assert "Add nothing" in tagging.DESCRIPTION_PROMPT


# 2. Out-of-vocabulary values never reach the store; a malformed or empty answer never crashes the run.


def test_out_of_vocabulary_values_never_reach_the_store():
    (work_id,) = works(1)
    answer = {
        "description": "  A figure\n stands.   It holds a sword. A third sentence. A fourth.  ",
        "medium": "Oil On Velvet",
        "subjects": ["Character ", "spaceship", "character", 7, None, "prop or weapon", ["creature"]],
        "genres": ["grimdark", "FANTASY", {"genre": "horror"}, "anime", "fantasy", "realistic"],
        "manner": " Anime Style ",
        "style_words": [
            "Painterly",
            "painterly",
            "<b>bold</b>",
            "x",
            "a" * 25,
            5,
            "fantasy",  # a genre, a medium, a subject: each is said once, in its own field
            "Digital Painting",
            "character",
            "cel-shaded",
            "high contrast",
            "w3ird",
            "one two three four",
            "saturated",  # beside "muted": of the two it is the one that goes
            "muted",
            "sketchy",
            "textured",
            "glossy",
        ],
        "explicit": False,
        "not_artwork": False,
        "artist": "zz-fake-alpha",
        "software": "FakePaint",
    }
    assert run(FakeOllama(answer))["analyzed"] == 1
    expected = {
        "medium": "unclear",
        "subjects": ["character", "prop or weapon"],
        "genres": ["fantasy", "anime style"],  # "realistic" is a manner, and the manner picked was another
        "style_words": ["painterly", "cel-shaded", "high contrast", "muted", "sketchy", "textured"],
        "description": "A figure stands. It holds a sword.",
        "explicit": False,
        "not_artwork": False,
    }
    assert stored() == [(work_id, tagging.DEFAULT_MODEL, tagging.PROMPT_VERSION, expected)]
    assert in_vocabulary(expected)
    assert tagging.validated({**GOOD, "medium": " 3D Render "})["medium"] == "3d render"
    assert tagging.validated({**GOOD, "manner": "cubist"})["genres"] == ["fantasy"]
    assert tagging.validated(GOOD) == KEPT


MALFORMED = [
    "",
    "   ",
    "not json",
    "[]",
    "null",
    '"a string"',
    "{}",
    json.dumps(GOOD)[:60],  # cut off, as when the model runs out of tokens
    *({name: value for name, value in GOOD.items() if name != dropped} for dropped in GOOD),
    {**GOOD, "explicit": "false"},
    {**GOOD, "explicit": None},
    {**GOOD, "not_artwork": 0},
    {**GOOD, "subjects": "character"},
    {**GOOD, "genres": None},
    {**GOOD, "manner": ["anime style"]},
    {**GOOD, "style_words": {"0": "painterly"}},
    {**GOOD, "medium": ["digital painting"]},
    {**GOOD, "description": ""},
    {**GOOD, "description": " \n "},
    {**GOOD, "description": "... 1."},  # no words in it
    {**GOOD, "description": "​"},
    {**GOOD, "description": 42},
    {**GOOD, "description": "x" * (tagging.MAX_DESCRIPTION + 1)},
    httpx.Response(200, text="<html>not the api</html>"),
    httpx.Response(200, json=[]),
    httpx.Response(200, json={"message": {}}),
    httpx.Response(200, json={"message": {"content": None}}),
    httpx.Response(200, json={"message": {"content": 5}}),
    httpx.Response(400, json={"error": "Failed to load image or audio file"}),
]


@pytest.mark.parametrize("bad", MALFORMED, ids=lambda bad: repr(bad)[:50])
def test_a_malformed_or_empty_answer_fails_that_work_and_the_run_carries_on(bad):
    first, second = works(1, 2)
    fake = FakeOllama(lambda n, attempt, chat: bad if n == 1 else GOOD)
    counts = run(fake, verifier="model-v")
    assert (counts["analyzed"], counts["failed"], counts["skipped"]) == (1, 1, 0)
    assert fake.attempts == {1: tagging.MAX_ATTEMPTS, 2: 1}  # asked again a bounded number of times
    assert {check[0] for check in fake.checks} == {2}  # and there is nothing of it to verify
    for name in (tagging.DEFAULT_MODEL, f"{tagging.DEFAULT_MODEL}+model-v"):
        assert [(row[0], row[3] is None) for row in stored() if row[1] == name] == [(first, True), (second, False)]
    assert list(tagging.results(verifier="model-v")) == [second]
    again = FakeOllama()
    assert run(again, verifier="model-v")["skipped"] == 2 and again.requests == []  # the failure is not asked again


def test_a_retry_is_asked_differently_and_its_good_answer_is_kept():
    (work_id,) = works(1)
    fake = FakeOllama(lambda n, attempt, chat: "" if attempt == 1 else GOOD)
    assert run(fake)["analyzed"] == 1
    first, second = (chat["options"] for chat in fake.chats)
    assert first["temperature"] == 0 and second["temperature"] > 0
    assert tagging.results()[work_id]["description"] == GOOD["description"]


@pytest.mark.parametrize(
    "said",
    [
        "A figure holds a blade in its right hand.",
        "A dragon on the LEFT of a tower.",
        "Three figures in armour.",
        "A sheet with 2 poses.",
        "A beast facing a figure.",
        "A figure stands behind a wall.",
    ],
)
def test_a_description_that_names_a_side_a_count_or_a_position_is_asked_again(said):
    retried, stuck = works(1, 2)
    plain = "A figure holds a blade. Brightly lit, upright and tense."  # "bright" and "upright" are not "right"
    fake = FakeOllama(lambda n, attempt, chat: {**GOOD, "description": said if n == 2 or attempt == 1 else plain})
    assert run(fake)["analyzed"] == 2
    assert fake.attempts == {1: 2, 2: tagging.MAX_ATTEMPTS}
    results = tagging.results()
    assert results[retried]["description"] == plain
    assert results[stuck]["description"] == said  # a model that will not stop is not a failed work


@pytest.mark.parametrize(
    "trouble",
    [
        httpx.Response(500, json={"error": "out of memory"}),
        httpx.Response(404, json={"error": "model not found"}),
        httpx.ConnectError("refused"),
        httpx.ReadTimeout("slow"),
    ],
    ids=lambda trouble: repr(trouble)[:40],
)
def test_ollama_in_trouble_stops_the_run_and_writes_no_work_off(trouble):
    first, second = works(1, 2)
    with pytest.raises(httpx.HTTPError):
        run(FakeOllama(lambda n, attempt, chat: GOOD if n == 1 else trouble))
    assert [row[0] for row in stored()] == [first]  # kept, and the second is not recorded as failed
    counts = run(FakeOllama())
    assert (counts["analyzed"], counts["skipped"]) == (1, 1)
    assert sorted(tagging.results()) == [first, second]

    with pytest.raises(httpx.HTTPError):  # the same while verifying: no tag is dropped for it, the run stops
        run(FakeOllama(confirm=lambda n, tag, chat: trouble), verifier="model-v")
    assert tagging.results(verifier="model-v") == {}


# 3. A rerun creates no duplicates and re-analyzes nothing.


def test_a_rerun_asks_nothing_and_stores_nothing_new():
    works(1, 2, 3)
    fake = FakeOllama(lambda n, attempt, chat: "" if n == 2 else GOOD)
    counts = run(fake, verifier="model-v")
    assert (counts["analyzed"], counts["failed"], counts["skipped"]) == (2, 1, 0)
    with closing(sqlite3.connect(config.LOCAL_DB)) as conn:
        before = [conn.execute(f"SELECT * FROM {table} ORDER BY 1, 2").fetchall() for table in tagging.TABLES]
    again = FakeOllama()
    counts = run(again, verifier="model-v")
    assert (counts["analyzed"], counts["failed"], counts["skipped"]) == (0, 0, 3)
    assert again.requests == []  # not the failed work, not a tag, not a title
    with closing(sqlite3.connect(config.LOCAL_DB)) as conn:
        assert [conn.execute(f"SELECT * FROM {table} ORDER BY 1, 2").fetchall() for table in tagging.TABLES] == before
    assert [len(rows) for rows in before] == [
        6,
        3,
        3,
        0,
        0,
        0,
        1,
    ]  # three works, each: two results, two readings; one tagline


def test_max_works_bounds_a_run_and_the_next_run_does_the_rest():
    ids = works(1, 2, 3)
    fake = FakeOllama()
    assert run(fake, max_works=2)["analyzed"] == 2
    counts = run(fake, max_works=500)
    assert (counts["analyzed"], counts["skipped"]) == (1, 2)
    assert fake.attempts == {1: 1, 2: 1, 3: 1} and len(fake.readings) == 3
    assert [row[0] for row in stored()] == ids


def test_works_go_artist_by_artist_in_key_order_and_one_artist_is_finished_before_the_next_is_begun():
    # Work ids are handed out so that id order and artist order disagree: bravo holds 1 and 4, alpha 2 and 3.
    works(1, 4, artist=BRAVO)
    works(2, 3, artist=ALPHA)
    steps = []

    class Watching(FakeOllama):
        def __call__(self, request):
            chat = json.loads(request.content)
            images = chat.get("messages", [{}])[-1].get("images")
            if images:
                kind = "first" if chat["messages"][0]["content"] == tagging.PROMPT else "check"
                steps.append((kind, int(base64.b64decode(images[0]).removeprefix(JPEG))))
            return super().__call__(request)

    run(Watching(), model="model-a", verifier="model-a", max_works=3)
    one_artist = [("first", 2), ("first", 3), ("check", 2), ("check", 2), ("check", 3), ("check", 3)]
    assert steps == [*one_artist, ("first", 1), ("check", 1), ("check", 1)]  # alpha whole, then bravo as far as it goes
    assert len(tagging.results("model-a", verifier="model-a")) == 3

    steps.clear()
    run(Watching(), model="model-a", verifier="model-v")  # another model checks: no going back and forth
    assert [step for step in steps if step[0] == "first"] == [("first", 4)]
    assert [n for kind, n in steps if kind == "check"] == [2, 2, 3, 3, 1, 1, 4, 4]


def test_each_model_and_each_prompt_version_keeps_its_own_result_set(monkeypatch):
    (work_id,) = works(1)
    fake = FakeOllama(lambda n, attempt, chat: {**GOOD, "description": f"Said by {chat['model']}."})
    run(fake, model="model-a")
    run(fake, model="model-b")
    run(fake, model="model-a")
    assert [chat["model"] for chat in fake.chats] == ["model-a", "model-b"]
    assert tagging.results("model-a")[work_id]["description"] == "Said by model-a."
    assert tagging.results("model-b")[work_id]["description"] == "Said by model-b."
    assert tagging.results() == {}  # nothing was asked of the default model

    version = tagging.PROMPT_VERSION
    monkeypatch.setattr(tagging, "PROMPT_VERSION", version + 1)
    assert run(fake, model="model-a")["analyzed"] == 1  # a changed prompt is asked again
    assert [row[1:3] for row in stored()] == [("model-a", version), ("model-a", version + 1), ("model-b", version)]
    assert set(tagging.results("model-a", prompt_version=version)) == {work_id}
    assert len(fake.readings) == 2  # the titles and tags have a prompt version of their own: one reading a model


# 4. Flagged works are stored with the flag, not dropped.


def test_explicit_and_not_artwork_works_are_stored_with_their_flag():
    explicit, not_artwork, plain = works(1, 2, 3)
    flags = {1: {"explicit": True}, 2: {"not_artwork": True}, 3: {}}
    counts = run(FakeOllama(lambda n, attempt, chat: {**GOOD, **flags[n]}), verifier="model-v")
    assert (counts["analyzed"], counts["flagged"], counts["failed"]) == (3, 2, 0)
    for results in (tagging.results(), tagging.results(verifier="model-v")):
        assert {key: (value["explicit"], value["not_artwork"]) for key, value in results.items()} == {
            explicit: (True, False),
            not_artwork: (False, True),
            plain: (False, False),
        }
        assert all(in_vocabulary(result) for result in results.values())


# 5. Stdout carries counts and throughput only.


def test_stdout_carries_counts_and_throughput_only(capsys):
    works(1, 2, 3)
    answers = {1: {**GOOD, "explicit": True}, 2: "", 3: GOOD}
    fake = FakeOllama(lambda n, *_: answers[n], confirm=False, declared="yes")
    tagging.main(["--max-works", "500", "--verifier", "model-v"], transport=httpx.MockTransport(fake))
    captured = capsys.readouterr()
    assert COUNTS.fullmatch(captured.out)
    assert captured.err == ""
    counts = json.loads(captured.out)
    assert (counts["analyzed"], counts["failed"], counts["flagged"], counts["skipped"]) == (2, 1, 1, 0)
    assert (counts["removed"], counts["declared_yes"]) == (4, 3)  # a subject and a genre from each of two works


@pytest.mark.parametrize(
    "arguments, fake",
    [
        (["--max-works", "5", "--ollama-url", "http://example.com:11434"], FakeOllama()),
        (["--max-works", "5"], FakeOllama(httpx.Response(500, text="FAKE-SERVER-TEXT"))),
        (["--max-works", "5"], FakeOllama(capabilities=())),
    ],
)
def test_a_stopped_run_prints_nothing_on_stdout_and_exits_with_an_error(capsys, arguments, fake):
    works(1)
    with pytest.raises(SystemExit) as stopped:
        tagging.main(arguments, transport=httpx.MockTransport(fake))
    assert str(stopped.value.code).startswith("stopped: ")
    assert "FAKE" not in str(stopped.value.code)
    assert capsys.readouterr().out == ""


def test_the_sample_prints_one_json_object_a_line_with_the_image_path_and_the_stored_result(capsys):
    works(*range(1, 9))
    answer = FakeOllama(lambda n, attempt, chat: "" if n == 4 else {**GOOD, "description": f"Work {n}."})
    run(answer, model="model-a", verifier="model-v")
    run(answer, model="model-b")
    capsys.readouterr()

    def printed(*arguments):
        tagging.main(list(arguments))  # no transport: a sample that connected anywhere would fail the test
        return [json.loads(line) for line in capsys.readouterr().out.splitlines()]

    rows = printed("--sample", "5", "--seed", "7", "--model", "model-a")
    assert len(rows) == 5 and len({row["deviation_id"] for row in rows}) == 5
    by_work = {key: value | READ for key, value in tagging.results("model-a").items()}
    for row in rows:
        path = library.image_dir() / f"{row['deviation_id']}.jpg"
        assert row["image"] == str(path) and path.is_file()
        assert row["model"] == "model-a"
        assert (row["result"] and row["result"] | READ) == by_work.get(row["deviation_id"])
    assert printed("--sample", "5", "--seed", "7", "--model", "model-a") == rows
    for other in (["--model", "model-b"], ["--model", "model-a", "--verifier", "model-v"]):
        same = printed("--sample", "5", "--seed", "7", *other)
        assert [row["deviation_id"] for row in same] == [row["deviation_id"] for row in rows]
        assert {row["model"] for row in same} == {"+".join(other[1::2])}
    assert printed("--sample", "5", "--seed", "8", "--model", "model-a") != rows
    everything = printed("--sample", "40", "--seed", "7", "--model", "model-a")
    assert len(everything) == 8 and sum(row["result"] is None for row in everything) == 1


# The second pass: the verifier is asked about the tags in one call and about the description in another.


def test_the_verifier_is_asked_about_every_tag_in_one_call_and_what_it_does_not_confirm_is_taken_out():
    (work_id,) = works(1)
    first = {
        **GOOD,
        "subjects": ["character", "prop or weapon", "environment"],
        "genres": ["fantasy", "horror"],
        "manner": "anime style",
        "style_words": ["painterly", "saturated", "textured"],
        "not_artwork": True,
    }
    refused = {"prop or weapon", "horror", "not artwork"}
    fake = FakeOllama(
        first, confirm=lambda n, tag, chat: tag not in refused, look={"manner": "anime style", "colour": "vivid"}
    )
    counts = run(fake, model="model-a", verifier="model-v")
    assert (counts["analyzed"], counts["removed"], counts["flagged"]) == (1, 3, 0)
    assert [chat["model"] for chat in fake.chats] == ["model-a"]  # the model looks once
    asked = ["look", "character", "environment", "prop or weapon", "fantasy", "horror", "not artwork"]
    assert fake.checks == [(1, tag, "model-v") for tag in asked]  # one call for all of them, by the verifier
    kept = tagging.validated(first)
    assert tagging.results("model-a")[work_id] == kept | READ  # the first pass is a set of its own
    assert tagging.results("model-a", verifier="model-v")[work_id] == kept | {
        "subjects": ["character", "environment"],
        "genres": ["fantasy", "anime style"],
        "not_artwork": False,
        **READ,
    }


@pytest.mark.parametrize(
    "look, genres, style_words",
    [
        ({"manner": "anime style", "colour": "vivid"}, ["fantasy", "anime style"], ["painterly", "saturated"]),
        ({"manner": "stylized cartoon", "colour": "muted"}, ["fantasy"], ["painterly"]),
        ({"manner": "painted illustration", "colour": "mixed"}, ["fantasy"], ["painterly"]),
        ({"manner": "cubist", "colour": "vivid"}, [], ["painterly"]),  # not an answer: nothing is confirmed
        ({"colour": "vivid"}, [], ["painterly"]),
        ("not json", [], ["painterly"]),
    ],
    ids=repr,
)
def test_a_manner_and_a_colour_word_stay_only_when_the_verifier_picks_the_same_for_itself(look, genres, style_words):
    (work_id,) = works(1)
    first = {**GOOD, "manner": "anime style", "style_words": ["painterly", "saturated"]}
    fake = FakeOllama(first, look=look)
    run(fake, verifier="model-v")
    result = tagging.results(verifier="model-v")[work_id]
    assert (result["genres"], result["style_words"]) == (genres, style_words)
    assert sum(check[1] == "look" for check in fake.checks) == (1 if genres else tagging.MAX_ATTEMPTS)


def test_the_verifier_never_adds_a_tag_of_its_own():
    (work_id,) = works(1)
    fake = FakeOllama(look={"manner": "anime style", "colour": "vivid"})  # what it sees, the first pass did not say
    run(fake, verifier="model-v")
    assert [check[1] for check in fake.checks] == ["look", "character", "fantasy"]
    assert tagging.results(verifier="model-v")[work_id] == KEPT | READ


LOOKED = {"faces": "FAKE-FACES", "manner": "painted illustration", "colour": "mixed", "about": "FAKE-ABOUT"}


@pytest.mark.parametrize(
    "bad",
    [
        "",
        "not json",
        "[]",
        {},
        {**LOOKED, "character": "true", "fantasy": True},
        {**LOOKED, "character": 1, "fantasy": True},
        {**LOOKED, "character": True},  # one tag is not answered
        {"character": True, "fantasy": True},  # it did not look
        httpx.Response(400),
    ],
    ids=repr,
)
def test_a_malformed_answer_about_the_tags_confirms_nothing_and_the_run_carries_on(bad):
    first, second = works(1, 2)
    fake = FakeOllama(confirm=lambda n, tag, chat: bad if n == 1 else True)
    counts = run(fake, verifier="model-v")
    assert (counts["analyzed"], counts["failed"], counts["removed"]) == (2, 0, 2)
    assert fake.checks.count((1, "look", "model-v")) == tagging.MAX_ATTEMPTS  # asked again, a bounded number
    results = tagging.results(verifier="model-v")
    assert (results[first]["subjects"], results[first]["genres"]) == ([], [])
    assert (results[second]["subjects"], results[second]["genres"]) == (["character"], ["fantasy"])


def test_the_verifier_sees_the_image_the_meaning_of_each_tag_and_of_the_first_answer_only_the_description():
    works(1)

    class Recording(FakeOllama):
        def __call__(self, request):
            chat = json.loads(request.content)
            if chat.get("messages", [{}])[0].get("content") in (tagging.CHECK_PROMPT, tagging.DESCRIPTION_PROMPT):
                seen.append(chat)
            return super().__call__(request)

    seen = []
    first = {**GOOD, "description": "FAKE-DESCRIPTION of a figure.", "manner": "realistic", "style_words": ["fakeword"]}
    run(Recording(first), verifier="model-v")
    describe, check = seen  # two calls an image, and no more
    assert describe["messages"][0]["content"] == tagging.DESCRIPTION_PROMPT
    for chat in seen:
        assert chat["model"] == "model-v" and chat["think"] is False
        assert chat["messages"][1]["images"] == [base64.b64encode(JPEG + b"1").decode()]
        assert "fakeword" not in json.dumps(chat) and "faketag" not in json.dumps(chat)
    assert "FAKE-DESCRIPTION" not in json.dumps(check)  # the tags are checked against the image alone
    assert check["messages"][1]["content"].splitlines() == [
        "Tags to check:",
        f"- character (what the image is about): {tagging.SUBJECTS['character']}",
        f"- fantasy (the kind of world shown): {tagging.GENRES['fantasy']}",
    ]
    assert describe["messages"][1]["content"] == "Description: FAKE-DESCRIPTION of a figure."


def test_a_second_verifier_checks_the_same_first_pass_into_a_result_set_of_its_own():
    ids = works(1, 2)
    fake = FakeOllama()
    run(fake, model="model-a", verifier="model-a")
    run(fake, model="model-a", verifier="model-v")
    run(fake, model="model-a", verifier="model-v")
    assert fake.attempts == {1: 1, 2: 1}  # one first pass a work, shared by both verifiers
    assert sorted({check[2] for check in fake.checks}) == ["model-a", "model-v"]
    assert len(fake.checks) == 2 * 3 * 2  # two works, its own look and two tags each, each verifier once
    both = [(n, model) for n in (1, 2) for model in ("model-a", "model-v")]
    assert sorted(rewrite[::2] for rewrite in fake.rewrites) == both
    assert sorted({row[1] for row in stored()}) == ["model-a", "model-a+model-a", "model-a+model-v"]
    for verifier in (None, "model-a", "model-v"):
        assert sorted(tagging.results("model-a", verifier=verifier)) == ids


# The description is checked too: the verifier may leave a detail out or put it more generally, and never add one.

SAID = "A woman holds a spiked mace in her right hand, standing on a mound of debris. Three figures watch from behind."


def test_the_checked_description_is_stored_and_the_first_one_is_kept_beside_it():
    (work_id,) = works(1)
    checked = "A figure holds a large weapon, standing on a mound of debris. Figures watch."
    answer = dict.fromkeys(tagging._DOUBTS, []) | {
        "things": ["a spiked mace: a long axe"],
        "places": ["in her right hand: both hands", "from behind: beside"],
        "numbers": ["Three figures: several"],
        "kinds": ["woman: cannot be told"],
        "description": checked,
    }
    fake = FakeOllama({**GOOD, "description": SAID}, rewrite=lambda n, description, chat: answer)
    counts = run(fake, model="model-a", verifier="model-v")
    assert (counts["analyzed"], counts["descriptions_changed"], counts["details_removed"]) == (1, 1, 5)
    assert fake.rewrites == [(1, SAID, "model-v")]
    assert tagging.results("model-a")[work_id]["description"] == SAID  # the original, in the first pass's own set
    assert tagging.results("model-a", verifier="model-v")[work_id]["description"] == checked
    assert [row["result"]["description"] for row in tagging.sample(5, 7, "model-a", verifier="model-v")] == [checked]


def test_a_description_with_nothing_wrong_is_stored_as_it_was():
    (work_id,) = works(1)
    counts = run(FakeOllama({**GOOD, "description": SAID}), verifier="model-v")
    assert (counts["descriptions_changed"], counts["details_removed"]) == (0, 0)
    assert tagging.results(verifier="model-v")[work_id]["description"] == SAID


@pytest.mark.parametrize(
    "answer",
    [
        "A figure holds a glowing sword.",  # a new thing
        f"{SAID[:-1]} at night.",  # everything it said, and one detail more
        "A queen holds a mace.",
        "A man holds a mace.",
        "",
        "...",
        {},
        {"description": 42},
        {"things": ["mace"], "description": None},
        httpx.Response(400),
        httpx.Response(200, json={"message": {"content": "not json"}}),
        httpx.Response(200, text="<html>not the api</html>"),
    ],
    ids=repr,
)
def test_a_check_that_adds_a_detail_or_is_malformed_leaves_the_description_as_it_was(answer):
    first, second = works(1, 2)
    fake = FakeOllama(
        {**GOOD, "description": SAID},
        rewrite=lambda n, description, chat: answer if n == 1 else "A figure holds a weapon.",
    )
    counts = run(fake, verifier="model-v")
    assert (counts["analyzed"], counts["failed"], counts["descriptions_changed"]) == (2, 0, 1)
    assert [rewrite[0] for rewrite in fake.rewrites].count(1) == tagging.MAX_ATTEMPTS  # asked again, a bounded number
    results = tagging.results(verifier="model-v")
    assert results[first]["description"] == SAID
    assert results[second]["description"] == "A figure holds a weapon."


def test_a_checked_description_may_leave_out_and_generalise_but_never_add():
    for less in (
        SAID,
        "A figure holds a large weapon, standing on a mound of debris.",
        "A figure holding a weapon. Several figures.",
        "A woman stands on debris.",  # another form of a word it had
        "Figures.",
    ):
        assert tagging.plainer(SAID, less) == less
    for more in (
        "A woman holds a spiked mace and a shield.",
        "A queen holds a mace.",
        "A woman holds a mace at night.",
        "A man holds a mace.",
        "A woman holds a mace. She smiles.",
        "A woman holds a spiked , standing on a mound of debris.",  # a word cut out, the hole left showing
        "A woman holds a, standing on a mound of debris.",
        "A woman holds a spiked mace in her right hand, , standing.",
        "",
        "  ",
        "?!",
        None,
        5,
        ["A woman."],
        "A woman " + "holds " * 100 + "a mace.",  # too long to be a description
    ):
        assert tagging.plainer(SAID, more) is None
    assert tagging.plainer(SAID, "A  woman.   Holds a mace. In her hand.") == "A woman. Holds a mace."  # two sentences


# The words: whether the artist's own title and tags declare the work AI-generated.


def test_the_title_and_tags_are_read_for_a_declaration_of_ai_and_the_answer_is_stored_and_read_back():
    said = {"faketag-says-yes": "yes", "faketag-says-no": "no", "faketag-who-knows": "unclear"}
    yes, no, unclear, untagged = works(
        1, 2, 3, 4, tags={1: ["a", "faketag-says-yes"], 2: ["faketag-says-no"], 3: ["faketag-who-knows"], 4: []}
    )
    fake = FakeOllama(
        declared=lambda chat: next(answer for tag, answer in said.items() if tag in chat["messages"][1]["content"])
    )
    counts = run(fake, model="model-a")
    assert (counts["declared_yes"], counts["declared_no"], counts["declared_unclear"]) == (1, 1, 2)
    assert len(fake.readings) == 3  # no tags: "unclear", without asking
    expected = {yes: "yes", no: "no", unclear: "unclear", untagged: "unclear"}
    assert tagging.declared_ai("model-a") == expected
    assert {key: value["declared_ai"] for key, value in tagging.results("model-a").items()} == expected
    assert stored("work_ai_declared") == [
        (key, "model-a", tagging.AI_PROMPT_VERSION, expected[key]) for key in sorted(expected)
    ]

    again = FakeOllama()
    counts = run(again, model="model-a", verifier="model-v")  # another result set of the same model: nothing is re-read
    assert again.readings == [] and (counts["declared_yes"], counts["declared_no"], counts["declared_unclear"]) == (
        0,
        0,
        0,
    )
    assert {value["declared_ai"] for value in tagging.results("model-a", verifier="model-v").values()} == {
        "yes",
        "no",
        "unclear",
    }


def test_the_reader_of_tags_gets_the_title_and_the_tags_and_no_image_handle_or_address():
    (work_id,) = works(1, tags={1: ["faketag-one", "faketag_two"]})
    fake = FakeOllama()
    run(fake, model="model-a", verifier="model-v")
    (reading,) = fake.readings
    assert reading["model"] == "model-a" and reading["think"] is False and reading["stream"] is False
    assert reading["messages"] == [
        {"role": "system", "content": tagging.AI_PROMPT},
        {"role": "user", "content": "Title: FAKE-TITLE-1\nTags: faketag-one, faketag_two"},
    ]
    assert reading["format"]["properties"]["declared"]["enum"] == ["yes", "no", "unclear"]
    sent = json.dumps(reading).lower()
    for kept_back in (
        work_id,
        "zz-fake-alpha",
        "deviantart",
        "wixmp",
        ".jpg",
        "images",
        base64.b64encode(JPEG).decode()[:40],
    ):
        assert kept_back.lower() not in sent
    assert all("faketag" not in json.dumps(chat) and "FAKE-TITLE" not in json.dumps(chat) for chat in fake.chats)


@pytest.mark.parametrize(
    "bad",
    ["", "not json", "[]", {}, {"declared": "maybe"}, {"declared": True}, {"says": "yes"}, httpx.Response(400)],
    ids=repr,
)
def test_a_malformed_reading_is_unclear_after_a_bounded_number_of_tries(bad):
    first, second = works(1, 2)
    fake = FakeOllama(declared=lambda chat: bad if "faketag1" in chat["messages"][1]["content"] else "yes")
    counts = run(fake)
    assert (counts["declared_yes"], counts["declared_unclear"], counts["analyzed"]) == (1, 1, 2)
    assert len(fake.readings) == tagging.MAX_ATTEMPTS + 1
    assert tagging.declared_ai() == {first: "unclear", second: "yes"}
    again = FakeOllama()
    run(again)
    assert again.requests == []


# The words again, asked a second question in a call of its own: is there a way to contact someone privately?


def test_titles_tags_and_taglines_are_read_for_a_private_contact_and_the_answers_are_stored_and_read_back():
    plain, phone = works(1, 2, artist=ALPHA, tags={2: ["faketag-call-me"]}, tagline="FAKE-TAGLINE with a handle")
    (other,) = works(3, artist=BRAVO)  # no tagline: nothing to read, so "no" without asking
    fake = FakeOllama(
        contact=lambda chat: (
            "yes" if "call-me" in chat["messages"][1]["content"] or "handle" in chat["messages"][1]["content"] else "no"
        )
    )
    counts = run(fake, model="model-a")
    assert (counts["contact_yes"], counts["contact_no"]) == (1, 2)
    assert (counts["tagline_contact_yes"], counts["tagline_contact_no"]) == (1, 1)
    assert len(fake.contact_readings) == 4  # three works and one tagline
    expected = {plain: "no", phone: "yes", other: "no", ALPHA: "yes", BRAVO: "no"}
    assert tagging.contacts("model-a") == expected
    assert {key: value["contact_in_text"] for key, value in tagging.results("model-a").items()} == {
        plain: "no",
        phone: "yes",
        other: "no",
    }
    version = tagging.CONTACT_PROMPT_VERSION
    assert stored("work_contact") == [(key, "model-a", version, expected[key]) for key in sorted((plain, phone, other))]
    assert stored("artist_contact") == [(ALPHA, "model-a", version, "yes"), (BRAVO, "model-a", version, "no")]

    again = FakeOllama()
    counts = run(again, model="model-a", verifier="model-v")  # another result set of the same model: nothing re-read
    assert again.contact_readings == [] and again.readings == []
    assert (counts["contact_yes"], counts["contact_no"], counts["tagline_contact_yes"]) == (0, 0, 0)
    assert {value["contact_in_text"] for value in tagging.results("model-a", verifier="model-v").values()} == {
        "yes",
        "no",
    }


def test_the_contact_question_is_a_call_of_its_own_that_carries_the_text_and_nothing_else():
    (work_id,) = works(1, tags={1: ["faketag-one", "faketag_two"]}, tagline="FAKE-TAGLINE")
    fake = FakeOllama()
    run(fake, model="model-a", verifier="model-v")
    about_work, about_artist = fake.contact_readings
    for reading in fake.contact_readings:
        assert reading["model"] == "model-a" and reading["think"] is False and reading["stream"] is False
        assert reading["messages"][0] == {"role": "system", "content": tagging.CONTACT_PROMPT}
        assert set(reading["messages"][1]) == {"role", "content"}  # no image
        assert reading["format"]["properties"]["contact"]["enum"] == ["yes", "no"]
        sent = json.dumps(reading).lower()
        for kept_back in (
            work_id,
            "zz-fake-alpha",
            "deviantart",
            "wixmp",
            ".jpg",
            base64.b64encode(JPEG).decode()[:40],
        ):
            assert kept_back.lower() not in sent
    assert about_work["messages"][1]["content"].splitlines() == [
        "Title: FAKE-TITLE-1",
        "Tags: faketag-one, faketag_two",
    ]
    assert about_artist["messages"][1]["content"] == "Tagline: FAKE-TAGLINE"
    (about_ai,) = fake.readings  # the other question about the same words: asked apart, so neither colours the other
    assert about_ai["messages"][0]["content"] == tagging.AI_PROMPT
    assert "contact" not in tagging.AI_PROMPT.lower() and " ai " not in tagging.CONTACT_PROMPT.lower()
    assert all("FAKE-TAGLINE" not in json.dumps(chat) for chat in fake.chats)  # and the image calls see none of it


@pytest.mark.parametrize(
    "bad",
    ["", "not json", "[]", {}, {"contact": "maybe"}, {"contact": True}, {"found": "yes"}, httpx.Response(400)],
    ids=repr,
)
def test_a_malformed_answer_about_a_contact_is_yes_after_a_bounded_number_of_tries(bad):
    first, second = works(1, 2)
    fake = FakeOllama(contact=lambda chat: bad if "faketag1" in chat["messages"][1]["content"] else "no")
    counts = run(fake)
    assert (counts["contact_yes"], counts["contact_no"], counts["analyzed"]) == (1, 1, 2)
    assert len(fake.contact_readings) == tagging.MAX_ATTEMPTS + 1
    assert tagging.contacts() == {first: "yes", second: "no", ALPHA: "no"}  # the cautious answer: left out, not leaked
    again = FakeOllama()
    run(again)
    assert again.requests == []


# Round 4. A request that fails is tried again, a bounded number of times, in every pass.


@pytest.mark.parametrize(
    "trouble",
    [httpx.Response(500), httpx.Response(503), httpx.ConnectError("refused"), httpx.ReadTimeout("slow")],
    ids=repr,
)
def test_a_failed_request_is_tried_again_with_backoff_and_stops_the_run_only_when_every_try_fails(monkeypatch, trouble):
    (work_id,) = works(1)
    slept = []
    monkeypatch.setattr(tagging.time, "sleep", slept.append)
    fake = FakeOllama(lambda n, attempt, chat: trouble if attempt <= 2 else GOOD)
    assert run(fake)["analyzed"] == 1  # two failures, then an answer: the work is not lost and the run goes on
    assert slept == list(tagging.RETRY_AFTER[:2]) and fake.attempts == {1: 3}
    assert list(tagging.results()) == [work_id]

    works(1, 2)
    slept.clear()
    never = FakeOllama(lambda n, attempt, chat: GOOD if n == 1 else trouble)
    with pytest.raises(httpx.HTTPError):
        run(never)
    assert slept == list(tagging.RETRY_AFTER) and never.attempts[2] == len(tagging.RETRY_AFTER) + 1
    assert [row[0] for row in stored()] == [work_id]  # and still nothing is written off as failed


# Round 4. How a work is presented and rated: a pass of its own, asked for by name.

SHEET = {
    "shows": "FAKE-LAYOUT",
    "format": "character sheet or turnaround",
    "also": "adoptable or design-for-sale sheet",
    "rating": "suggestive",
}


def formats_run(fake, max_works=500, **kwargs):
    counts = tagging.run_formats(max_works, transport=httpx.MockTransport(fake), **kwargs)
    assert isinstance(counts.pop("seconds_per_image"), float)
    return counts


def test_works_are_sorted_by_format_and_rating_in_a_pass_of_their_own_and_read_back_with_the_results():
    sheet, plain, odd, broken = works(1, 2, 3, 4)
    answers = {
        1: SHEET,
        2: {**SHEET, "format": "none of these", "also": "none of these", "rating": "safe"},
        3: {
            **SHEET,
            "format": "cubist collage",
            "also": "portrait or bust",
            "rating": "explicit",
        },  # one is not a format
        4: {**SHEET, "rating": "adults only"},  # not a rating: no answer at all
    }
    run(FakeOllama(), model="model-a")
    assert {result["formats"] for result in tagging.results("model-a").values()} == {None}  # not run by default
    fake = FakeOllama(formatted=lambda n, chat: answers[n])
    counts = formats_run(fake, model="model-a")
    assert counts == {
        "sorted": 3,
        "failed": 1,
        "skipped": 0,
        "with_a_format": 2,
        "rating_safe": 1,
        "rating_suggestive": 1,
        "rating_explicit": 1,
    }
    assert fake.sorts.count((4, "model-a")) == tagging.MAX_ATTEMPTS and fake.chats == [] and fake.checks == []
    read = {key: (value["formats"], value["rating"]) for key, value in tagging.results("model-a").items()}
    assert read == {
        sheet: (["character sheet or turnaround", "adoptable or design-for-sale sheet"], "suggestive"),
        plain: ([], "safe"),
        odd: (["portrait or bust"], "explicit"),
        broken: (None, None),
    }
    assert [(row[0], row[3] is None) for row in stored("work_format")] == [
        (sheet, False),
        (plain, False),
        (odd, False),
        (broken, True),
    ]
    again = FakeOllama()
    assert formats_run(again, model="model-a")["skipped"] == 4 and again.requests == []  # nothing is asked twice


def test_the_format_question_carries_the_image_and_its_own_prompt_and_prints_counts_only(capsys):
    (work_id,) = works(1, tagline="FAKE-TAGLINE")
    fake = FakeOllama(formatted=SHEET)
    tagging.main(["--max-works", "5", "--formats", "--model", "model-a"], transport=httpx.MockTransport(fake))
    out = capsys.readouterr().out
    assert re.fullmatch(r'\{("[a-z_]+": \d+(\.\d+)?(, )?)+\}\n', out) and json.loads(out)["rating_suggestive"] == 1
    (asked,) = [json.loads(request.content) for request in fake.requests if request.url.path == "/api/chat"]
    assert asked["messages"][0] == {"role": "system", "content": tagging.FORMAT_PROMPT}
    assert asked["messages"][1]["images"] == [base64.b64encode(JPEG + b"1").decode()] and asked["model"] == "model-a"
    assert asked["format"]["properties"]["rating"]["enum"] == list(tagging.RATINGS)
    assert asked["format"]["properties"]["format"]["enum"] == [*tagging.FORMATS, *tagging._OTHER_FORMATS]
    for kept_back in (work_id, "zz-fake-alpha", "fake-title", "faketag", "fake-tagline", "deviantart", ".jpg"):
        assert kept_back not in json.dumps(asked).lower()
    for value in (*tagging.FORMATS, *tagging.RATINGS):
        assert f"- {value}: " in tagging.FORMAT_PROMPT
    with pytest.raises(ValueError, match="loopback"):
        tagging.run_formats(5, ollama_url="http://example.com:11434", transport=httpx.MockTransport(fake))


# Round 4. A description two models agree on, stored beside the checked one.

CHECKED = "A horned beast with dark scales holds a spiked mace. A watering can on the ground."


def agreed_run(fake, second="model-g", max_works=500, **kwargs):
    counts = tagging.run_agreed(max_works, second, transport=httpx.MockTransport(fake), **kwargs)
    assert isinstance(counts.pop("seconds_per_second_description"), float)
    assert isinstance(counts.pop("seconds_per_agreement"), float)
    return counts


def test_a_second_model_describes_each_image_and_what_both_descriptions_say_is_stored_beside_the_checked_one():
    one, two, three = works(1, 2, 3)
    first = FakeOllama({**GOOD, "description": CHECKED})
    run(first, model="model-a", verifier="model-v")
    agreed = "A horned beast holds a large weapon."
    merged = {1: agreed, 2: "A horned beast with dark fur holds a hammer.", 3: agreed}  # the second adds two words
    fake = FakeOllama(
        second=lambda n, chat: "" if n == 3 else f"A shaggy horned beast with a hammer. Work {n}.",
        agree=lambda chat: next(text for n, text in merged.items() if f"Work {n}." in chat["messages"][1]["content"]),
    )
    counts = agreed_run(fake, model="model-a", verifier="model-v")
    assert counts == {"described": 3, "agreed": 3, "changed": 1, "unmatched": 1, "skipped": 0}
    assert [model for _, model in fake.seconds] == ["model-g"] * (2 + tagging.MAX_ATTEMPTS)  # every image, then
    assert [chat["model"] for chat in fake.merges] == ["model-a"] * (
        1 + tagging.MAX_ATTEMPTS
    )  # the words, by the first
    assert fake.order == ["second"] * (2 + tagging.MAX_ATTEMPTS) + ["merge"] * (1 + tagging.MAX_ATTEMPTS)
    both = tagging.results("model-a", verifier="model-v", second="model-g")
    assert {key: value["description"] for key, value in both.items()} == {one: agreed, two: CHECKED, three: CHECKED}
    untouched = tagging.results("model-a", verifier="model-v")
    assert {value["description"] for value in untouched.values()} == {CHECKED}
    assert all(both[key] == untouched[key] | {"description": both[key]["description"]} for key in both)
    rows = tagging.sample(5, 7, "model-a", verifier="model-v", second="model-g")
    assert {(row["result"]["description"], row["checked"], row["model"]) for row in rows} == {
        (agreed, CHECKED, "model-a+model-v~model-g"),
        (CHECKED, CHECKED, "model-a+model-v~model-g"),
    }
    assert tagging.results("model-a", verifier="model-v", second="model-x") == {}  # no such variant
    again = FakeOllama()
    assert agreed_run(again, model="model-a", verifier="model-v")["agreed"] == 0 and again.requests == []


def test_the_second_model_sees_only_the_image_and_the_agreement_is_reached_on_words_alone(capsys):
    (work_id,) = works(1)
    run(FakeOllama({**GOOD, "description": CHECKED}), model="model-a", verifier="model-v")
    fake = FakeOllama(second="A shaggy beast with a hammer.")
    arguments = ["--max-works", "5", "--model", "model-a", "--verifier", "model-v", "--second", "model-g"]
    tagging.main(arguments, transport=httpx.MockTransport(fake))
    out = capsys.readouterr().out
    assert re.fullmatch(r'\{("[a-z_]+": \d+(\.\d+)?(, )?)+\}\n', out) and json.loads(out)["agreed"] == 1
    describe, merge = (json.loads(request.content) for request in fake.requests if request.url.path == "/api/chat")
    assert describe["messages"][0] == {"role": "system", "content": tagging.SECOND_PROMPT}
    assert describe["messages"][1]["images"] == [base64.b64encode(JPEG + b"1").decode()]
    assert "watering" not in json.dumps(describe)  # nothing of the first model's answer
    assert (
        merge["messages"][0] == {"role": "system", "content": tagging.AGREE_PROMPT}
        and "images" not in merge["messages"][1]
    )
    assert merge["messages"][1]["content"].splitlines() == [
        f"First description: {CHECKED}",
        "Second description: A shaggy beast with a hammer.",
    ]
    for request in (describe, merge):
        for kept_back in (work_id, "zz-fake-alpha", "fake-title", "faketag", "deviantart", ".jpg"):
            assert kept_back not in json.dumps(request).lower()
    for rule in ("No left or right", "No numbers", "No gender, age or species"):
        assert rule in tagging.SECOND_PROMPT  # the same plain-words rules as the first pass
    tagging.main(["--sample", "5", "--seed", "1", "--model", "model-a", "--verifier", "model-v", "--second", "model-g"])
    (row,) = [json.loads(line) for line in capsys.readouterr().out.splitlines()]
    assert row["checked"] == CHECKED and row["result"]["description"] == CHECKED and row["deviation_id"] == work_id


# Works that are not there to analyze, works that were removed, and a database another stage is holding.


def test_a_work_without_a_cached_image_is_left_alone():
    no_image, gone, outside = works(1, 2, 3, without_image=(1,))
    (library.image_dir() / f"{gone}.jpg").unlink()
    with closing(sqlite3.connect(config.LOCAL_DB)) as conn, conn:
        conn.execute("UPDATE works SET image_file = '../pipeline.sqlite' WHERE deviation_id = ?", (outside,))
    fake = FakeOllama()
    counts = run(fake, verifier="model-v")
    assert (counts["analyzed"], counts["failed"], counts["skipped"]) == (0, 0, 2)
    assert fake.chats == [] and fake.checks == []
    assert stored() == []
    assert no_image not in tagging.results() and no_image not in tagging.declared_ai()


def test_a_removed_artist_is_not_read_back_and_their_rows_go_at_the_next_run():
    (removed,), (kept,) = works(1, artist=ALPHA), works(2, artist=BRAVO)
    run(FakeOllama(), verifier="model-v")
    library.purge(ALPHA)
    assert list(tagging.results()) == list(tagging.results(verifier="model-v")) == list(tagging.declared_ai()) == [kept]
    assert [row["deviation_id"] for row in tagging.sample(40, 7)] == [kept]
    run(FakeOllama(), verifier="model-v")
    for table in tagging.TABLES:
        assert {row[0] for row in stored(table)} <= {BRAVO, kept}
        assert removed not in json.dumps(stored(table)) and ALPHA not in json.dumps(stored(table))


def test_nothing_is_stored_for_a_work_once_it_is_removed_even_while_a_model_is_looking_at_it():
    for step in ("declared", "contact", "answer", "rewrite", "confirm"):
        works(1, artist=ALPHA, tagline="FAKE-TAGLINE")

        def purging(*args, step=step):
            library.purge(ALPHA)  # which also deletes what this stage had stored about the artist until then
            return {"declared": "yes", "contact": "yes", "answer": GOOD, "rewrite": None, "confirm": True}[step]

        run(FakeOllama(**{step: purging}), verifier="model-v")
        assert [stored(table) for table in tagging.TABLES] == [[] for _ in tagging.TABLES], step
        assert tagging.results() == tagging.results(verifier="model-v") == tagging.declared_ai() == {}
        assert tagging.contacts() == {}


def test_a_database_another_stage_is_holding_is_waited_for(monkeypatch):
    (work_id,) = works(1)
    opened, slept = [], []
    real = tagging._open

    @contextlib.contextmanager
    def held_at_first(db):
        opened.append(db)
        if len(opened) in (1, 2, 5):
            raise sqlite3.OperationalError("database is locked")
        with real(db) as conn:
            yield conn

    monkeypatch.setattr(tagging, "_open", held_at_first)
    monkeypatch.setattr(tagging.time, "sleep", slept.append)
    assert run(FakeOllama())["analyzed"] == 1
    assert len(slept) == 3 and list(tagging.results()) == [work_id]

    monkeypatch.setattr(tagging, "LOCK_WAIT", -1)  # and not for ever
    opened.clear()
    with pytest.raises(sqlite3.OperationalError, match="locked"):
        tagging.results()


# A reading is tied to what was read. Each of these is a sequence: read, change, run again.
#
# The first critic of the push stage: "Tagging keys 'no contact', 'not AI', the rating and the tags by work id or
# artist key, once. Collect keeps bringing taglines, titles, tags and thumbnail addresses up to date."

THUMB = "https://images-wixmp-zzfake.wixmp.com/f/FAKE-THUMB.jpg"
ANOTHER = "https://images-wixmp-zzfake.wixmp.com/f/FAKE-ANOTHER-FILE.jpg"  # the file an artist puts in its place
RATED = {"shows": "FAKE-LAYOUT", "format": "none of these", "also": "none of these"}


def collected_again(*numbers, artist=ALPHA, tagline=None, titles=None, tags=None, thumbs=None, images=None):
    """The collect stage's next turn for the artist: the same works, with what the artist has changed since.
    images: the bytes of the file an artist put under a work, which the collector has fetched and says so."""
    for n, data in (images or {}).items():
        library.save_image(f"00000000-0000-4000-A000-{n:012d}", "image/jpeg", data)
    rows = [
        {
            "work_id": f"00000000-0000-4000-A000-{n:012d}",
            "page_url": f"https://www.deviantart.com/zz-fake-alpha/art/FAKE-TITLE-{n}",
            "title": (titles or {}).get(n, f"FAKE-TITLE-{n}"),
            "tags": (tags or {}).get(n, [f"faketag{n}"]),
            "thumb_url": (thumbs or {}).get(n, THUMB),
            "published": "1700000000",
            "fetched": n in (images or {}),
        }
        for n in numbers
    ]
    library.store_artist(artist, user_id=None, tagline=tagline, specialty=None, level=None, website=None, works=rows)


def asked(readings):
    """The texts a fake was asked about, in order."""
    return [chat["messages"][-1]["content"] for chat in readings]


def test_a_tagline_that_changes_after_it_was_read_is_read_again():
    (work_id,) = works(1, tagline="FAKE-TAGLINE one")
    assert run(FakeOllama())["tagline_contact_no"] == 1 and tagging.contacts() == {ALPHA: "no", work_id: "no"}
    said = "FAKE-TAGLINE two, message me on Discord fakepainter_77"
    collected_again(1, tagline=said)
    assert tagging.contacts() == {work_id: "no"}  # the "no" there is of other words: of these, nothing is known

    again = FakeOllama(contact="yes")
    counts = run(again)

    assert counts["tagline_contact_yes"] == 1 and counts["contact_yes"] == counts["analyzed"] == 0
    assert asked(again.contact_readings) == [f"Tagline: {said}"] and again.chats == again.readings == []
    assert tagging.contacts() == {ALPHA: "yes", work_id: "no"}
    assert stored("artist_contact") == [(ALPHA, tagging.DEFAULT_MODEL, tagging.CONTACT_PROMPT_VERSION, "yes")]
    quiet = FakeOllama()
    assert run(quiet)["tagline_contact_yes"] == 0 and quiet.requests == []  # and it is read once


def test_a_title_or_tags_that_change_after_they_were_read_are_read_again_and_the_image_is_not():
    one, two, three = works(1, 2, 3)
    run(FakeOllama(), verifier="model-v")
    before = tagging.results(verifier="model-v")
    collected_again(1, 2, 3, titles={1: "FAKE-TITLE-1 telegram fakepainter"}, tags={2: ["faketag2", "fakediscord77"]})
    now = tagging.results(verifier="model-v")
    for work_id in (one, two):  # what the image was read as stands; what the old words were read as does not
        assert now[work_id] == before[work_id] | {"declared_ai": None, "contact_in_text": None}
    assert now[three] == before[three]
    assert tagging.declared_ai() == {three: "no"} and tagging.contacts() == {ALPHA: "no", three: "no"}

    again = FakeOllama(declared="yes", contact="yes")
    counts = run(again, verifier="model-v")

    assert (counts["declared_yes"], counts["contact_yes"], counts["analyzed"]) == (2, 2, 0)
    assert again.chats == again.checks == again.rewrites == []  # no image was read again: about 9 seconds each
    changed = ["Title: FAKE-TITLE-1 telegram fakepainter\nTags: faketag1"]
    changed += ["Title: FAKE-TITLE-2\nTags: faketag2, fakediscord77"]
    assert asked(again.readings) == asked(again.contact_readings) == changed
    assert tagging.declared_ai() == {one: "yes", two: "yes", three: "no"}
    assert tagging.contacts() == {ALPHA: "no", one: "yes", two: "yes", three: "no"}
    assert len(stored("work_ai_declared")) == len(stored("work_contact")) == 3  # the new took the old one's place


def test_an_image_the_artist_replaced_is_left_alone_until_it_is_fetched_and_is_read_again_then():
    one, two = works(1, 2)
    run(FakeOllama(), verifier="model-v")
    formats_run(FakeOllama())
    before = tagging.results(verifier="model-v")
    assert before[one]["rating"] == "safe" and [row["deviation_id"] for row in tagging.sample(5, 1)] == [one, two]

    collected_again(1, 2, thumbs={1: ANOTHER})  # the thumbnail shows another file now; the image is still the old one
    assert tagging.results(verifier="model-v") == {two: before[two]} and list(tagging.results()) == [two]
    assert [row["deviation_id"] for row in tagging.sample(5, 1, verifier="model-v")] == [two]
    waiting = FakeOllama()
    assert run(waiting, verifier="model-v")["analyzed"] == 0 and formats_run(waiting)["sorted"] == 0
    assert waiting.requests == []  # nobody reads an image that is not what the work shows

    collected_again(1, 2, thumbs={1: ANOTHER}, images={1: JPEG + b"101"})  # the collector has fetched the new file
    again = FakeOllama({**GOOD, "description": "A green square."}, formatted=RATED | {"rating": "explicit"})
    counts = run(again, verifier="model-v")

    assert counts["analyzed"] == 1 and again.attempts == {101: 1} and {n for n, _, _ in again.checks} == {101}
    assert tagging.results(verifier="model-v")[one]["rating"] is None  # "safe" was said of the other picture
    assert formats_run(again)["sorted"] == 1 and again.sorts == [(101, tagging.DEFAULT_MODEL)]
    assert again.readings == again.contact_readings == []  # the words did not change: their reading stands
    after = tagging.results(verifier="model-v")
    assert after[two] == before[two]  # an image that did not change keeps its reading
    assert after[one] == before[one] | {"description": "A green square.", "rating": "explicit"}
    assert len(stored()) == 4 and len(stored("work_format")) == 2  # the new rows took the old ones' place
    quiet = FakeOllama()
    assert run(quiet, verifier="model-v")["analyzed"] == 0 and formats_run(quiet)["sorted"] == 0
    assert quiet.requests == []


def test_a_change_that_lands_while_a_model_is_at_work_is_not_taken_for_read():
    """A run reads the words when it begins and asks about them for hours. The fingerprint is of what was sent."""
    (work_id,) = works(1, tagline="FAKE-TAGLINE one")
    changes = {"titles": {1: "FAKE-TITLE-1 CHANGED"}}

    def new_words(chat):  # the collector brings new words while a model reads the old ones
        collected_again(1, tagline="FAKE-TAGLINE two", **changes)
        return "no"

    def new_image(n, attempt, chat):  # and another image while a model looks at the old one
        collected_again(1, thumbs={1: ANOTHER}, images={1: JPEG + b"101"}, **changes)
        return GOOD

    first = FakeOllama(answer=new_image, declared=new_words)
    assert run(first)["analyzed"] == 1 and first.attempts == {1: 1}
    assert asked(first.readings) == ["Title: FAKE-TITLE-1\nTags: faketag1"]
    assert asked(first.contact_readings) == ["Title: FAKE-TITLE-1\nTags: faketag1", "Tagline: FAKE-TAGLINE one"]
    # Four answers were stored, and not one of them is about the work or the artist as they are now.
    assert tagging.results() == tagging.declared_ai() == tagging.contacts() == {}

    again = FakeOllama()
    assert run(again)["analyzed"] == 1 and again.attempts == {101: 1}
    assert asked(again.readings) == ["Title: FAKE-TITLE-1 CHANGED\nTags: faketag1"]
    assert asked(again.contact_readings) == ["Title: FAKE-TITLE-1 CHANGED\nTags: faketag1", "Tagline: FAKE-TAGLINE two"]
    assert tagging.results() == {work_id: KEPT | READ} and tagging.contacts() == {ALPHA: "no", work_id: "no"}


def test_a_first_pass_of_another_image_is_not_what_the_verifier_checks():
    """The verifier is given the first description and the image. Both must be of the same image."""
    (work_id,) = works(1)
    run(FakeOllama({**GOOD, "description": "A red circle."}), model="model-a")  # the first pass, and no verifier yet

    class Meanwhile(FakeOllama):  # the image is replaced after the run has listed its works, before the check
        def __call__(self, request):
            if request.url.path == "/api/show":
                collected_again(1, thumbs={1: ANOTHER}, images={1: JPEG + b"101"})
            return super().__call__(request)

    fake = Meanwhile()
    assert run(fake, model="model-a", verifier="model-v")["analyzed"] == 0
    assert fake.rewrites == fake.checks == []  # never the old description held against the new image
    assert tagging.results("model-a", verifier="model-v") == {}
    fake = FakeOllama({**GOOD, "description": "A green square."})
    assert run(fake, model="model-a", verifier="model-v")["analyzed"] == 1
    assert fake.rewrites == [(101, "A green square.", "model-v")]
    assert tagging.results("model-a", verifier="model-v")[work_id]["description"] == "A green square."


def test_a_description_two_models_agreed_on_is_of_one_image_and_is_agreed_again_for_another():
    one, two = works(1, 2)
    run(FakeOllama({**GOOD, "description": CHECKED}), model="model-a", verifier="model-v")
    agreed_run(FakeOllama(second="A horned beast."), max_works=1, model="model-a", verifier="model-v")  # one only
    variant = {"verifier": "model-v", "second": "model-g"}
    assert list(tagging.results("model-a", **variant)) == [one]

    replaced = {"thumbs": {1: ANOTHER, 2: f"{ANOTHER}-2.jpg"}, "images": {1: JPEG + b"101", 2: JPEG + b"102"}}
    collected_again(1, 2, **replaced)
    assert tagging.results("model-a", **variant) == {} and tagging.sample(5, 1, "model-a", **variant) == []
    waiting = FakeOllama()  # no checked description of either new image yet: there is nothing to agree on
    assert agreed_run(waiting, model="model-a", verifier="model-v")["agreed"] == 0 and waiting.requests == []
    run(FakeOllama({**GOOD, "description": "A green square."}), model="model-a", verifier="model-v")
    assert tagging.results("model-a", **variant) == {}  # what was agreed of the other picture is not this one's
    fake = FakeOllama(second="A green square on a field.")
    counts = agreed_run(fake, model="model-a", verifier="model-v")
    assert (counts["described"], counts["agreed"]) == (2, 2) and fake.seconds == [(101, "model-g"), (102, "model-g")]
    assert tagging.results("model-a", **variant)[one]["description"] == "A green square."
    assert len(stored("work_second")) == len(stored("work_agreed")) == 2  # one each: the new took the old one's place


# The store's shape, and the one command that changes it.


def before_fingerprints():
    """Take the store back to what every store was before readings carried fingerprints: no such column, no note
    of which image a work has, and no mark."""
    with closing(sqlite3.connect(config.LOCAL_DB)) as conn:
        for table in tagging.TABLES:
            conn.execute(f"ALTER TABLE {table} DROP COLUMN fingerprint")
        conn.execute("DROP TABLE work_images")
        conn.execute("PRAGMA user_version = 0")


def raw(statement, *values, db=None):
    with closing(sqlite3.connect(db or config.LOCAL_DB)) as conn, conn:
        return conn.execute(statement, values).fetchall()


def test_a_store_made_before_readings_carried_fingerprints_stops_every_command_before_a_model_is_asked(capsys):
    works(1)
    run(FakeOllama(), verifier="model-v")
    before_fingerprints()
    before, fake = config.LOCAL_DB.read_bytes(), FakeOllama()
    transport = httpx.MockTransport(fake)
    calls = [lambda: run(fake, verifier="model-v"), lambda: formats_run(fake), lambda: agreed_run(fake)]
    calls += [tagging.results, tagging.contacts, tagging.declared_ai, lambda: tagging.sample(5, 1)]
    calls += [lambda: tagging.main(["--max-works", "5"], transport), lambda: tagging.main(["--sample", "5"])]
    for call in calls:
        with pytest.raises(SystemExit) as stopped:
            call()
        assert "uv run python -m pipeline.tagging --migrate --backup" in str(stopped.value)
    assert fake.requests == [] and config.LOCAL_DB.read_bytes() == before and capsys.readouterr().out == ""
    with pytest.raises(SystemExit) as stopped:  # and by hand the migration takes nothing on trust
        tagging.main(["--migrate"])
    assert stopped.value.code == 2 and config.LOCAL_DB.read_bytes() == before


def test_the_migration_gives_each_reading_the_fingerprint_of_what_it_read_and_assumes_none(tmp_path, capsys):
    one, two, three, four = works(1, 2, 3, 4, tagline="FAKE-TAGLINE one")
    new, again = works(5, 6, artist=BRAVO, tagline="FAKE-TAGLINE bravo")
    run(FakeOllama(), verifier="model-v")
    formats_run(FakeOllama())
    was = tagging.results(verifier="model-v")
    before_fingerprints()
    backup = tmp_path / "before.sqlite"
    shutil.copyfile(config.LOCAL_DB, backup)
    # What was not there yet when the copy was taken: BRAVO and their works, and two of ALPHA's readings.
    for table in ("works", *tagging.WORK_TABLES):
        raw(f"DELETE FROM {table} WHERE deviation_id IN (?, ?)", new, again, db=backup)
    raw("DELETE FROM profiles WHERE artist_key = ?", BRAVO, db=backup)
    raw("DELETE FROM artist_contact WHERE artist_key = ?", BRAVO, db=backup)
    raw("DELETE FROM work_contact WHERE deviation_id IN (?, ?)", one, four, db=backup)
    # Then a collector of before brought words and a thumbnail up to date, and no reading knew.
    raw("UPDATE works SET title = 'FAKE-TITLE-1 CHANGED' WHERE deviation_id = ?", one)
    raw("""UPDATE works SET tags = '["faketag2", "fakenew"]' WHERE deviation_id = ?""", two)
    raw("UPDATE works SET thumb_url = ? WHERE deviation_id = ?", ANOTHER, three)
    raw("UPDATE works SET thumb_url = ? WHERE deviation_id = ?", f"{THUMB}?token=FAKE-SIGNED-AGAIN", four)
    raw("UPDATE profiles SET tagline = 'FAKE-TAGLINE two' WHERE artist_key = ?", ALPHA)
    # And one of the works the copy does not have was stored again after it was read: with what words, nobody knows.
    raw("UPDATE works SET fetched_at = '2099-01-01T00:00:00+00:00' WHERE deviation_id = ?", again)

    tagging.main(["--migrate", "--backup", str(backup)])

    assert json.loads(capsys.readouterr().out) == {
        "stores migrated": 1,
        "images fingerprinted": 6,
        # 6 works, each with a first pass, a checked one and a format; and the readings of words that are known
        "readings fingerprinted": 18 + (5 + 4 + 2),
        "readings stale: title changed": 1,  # what one's title was read to declare; its contact reading is below
        "readings stale: tags changed": 2,
        "readings stale: tagline changed": 1,
        "readings left stale: nothing says which words they read": 1 + 2,
        "works with a replaced image": 1,
        "profiles tied to what they rest on": 0,
    }
    now = tagging.results(verifier="model-v")
    assert list(now) == [one, two, four, new, again]  # three's image is out of date: its thumbnail shows another file
    assert library.image_prints().keys() == {one, two, four, new, again}
    for work_id in (one, two, again):  # the image's reading stands; of the words, nobody is known to have read them
        assert now[work_id] == was[work_id] | {"declared_ai": None, "contact_in_text": None}
    assert (now[four], now[new]) == (was[four], was[new])  # nothing changed, nothing to do again
    # Of words, only those that are as they were read: not one's, not two's, and not ALPHA's tagline.
    assert tagging.contacts() == {BRAVO: "no", three: "no", four: "no", new: "no"}
    assert tagging.declared_ai() == {three: "no", four: "no", new: "no"}

    settled = config.LOCAL_DB.read_bytes()
    tagging.main(["--migrate", "--backup", str(backup)])  # again: nothing
    assert json.loads(capsys.readouterr().out)["stores migrated"] == 0 and config.LOCAL_DB.read_bytes() == settled

    fake = FakeOllama()  # and the stage runs on it, and asks only what is stale
    counts = run(fake, verifier="model-v")
    assert counts["analyzed"] == 0 and fake.chats == fake.checks == []
    stale = ["Title: FAKE-TITLE-1 CHANGED\nTags: faketag1", "Title: FAKE-TITLE-2\nTags: faketag2, fakenew"]
    stale += ["Title: FAKE-TITLE-6\nTags: faketag6"]
    assert asked(fake.readings) == stale
    assert asked(fake.contact_readings) == [stale[0], "Tagline: FAKE-TAGLINE two", *stale[1:]]


def test_the_migration_is_all_or_nothing(tmp_path, monkeypatch):
    works(1)
    run(FakeOllama(), verifier="model-v")
    before_fingerprints()
    backup = tmp_path / "before.sqlite"
    shutil.copyfile(config.LOCAL_DB, backup)
    monkeypatch.setattr(tagging, "tagline_print", lambda tagline: 1 / 0)  # it fails near its end
    with pytest.raises(ZeroDivisionError):
        tagging.migrate(backup)
    assert raw("PRAGMA user_version") == [(0,)] and raw("SELECT * FROM work_images") == []
    assert all("fingerprint" not in column for table in tagging.TABLES for column in raw(f"PRAGMA table_info({table})"))
    with pytest.raises(SystemExit):
        tagging.results()  # still a store that nothing opens
