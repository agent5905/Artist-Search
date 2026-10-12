"""Tests for pipeline.tagging.

Ollama is a fake behind httpx.MockTransport and every image is a few invented bytes: no model, no GPU, no network.
Opening a real connection fails the test. Every handle, title, tag, address and answer below is invented.
"""

import base64
import contextlib
import json
import re
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
COUNTS = re.compile(
    r'\{"analyzed": \d+, "failed": \d+, "flagged": \d+, "skipped": \d+, "removed": \d+, "declared_yes": \d+, '
    r'"declared_no": \d+, "declared_unclear": \d+, "seconds_per_image": \d+\.\d+, '
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

    def no_network(*args, **kwargs):
        raise AssertionError("a real connection was attempted")

    monkeypatch.setattr(socket.socket, "connect", no_network)
    monkeypatch.setattr(socket, "create_connection", no_network)


def works(*numbers, artist=ALPHA, without_image=(), tags=None):
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
    library.store_artist(artist, user_id=None, tagline=None, specialty=None, level=None, website=None, works=rows)
    return ids


class FakeOllama:
    """Answers /api/show and the four questions /api/chat is asked, told apart by their system prompt.

    answer:   the first pass. What the model says, or a function (work number, attempt, request) -> that. A dict
              is sent as the model's JSON, a str as it is, a Response whole, and an exception is raised.
    confirm:  the verifier on one tag: True, False, or a function (work number, tag, request) -> an answer.
    look:     the verifier's own pick of manner and colour, or a function (work number, request) -> that.
    declared: the reader of title and tags: "yes", "no", "unclear", or a function (request) -> an answer.
    """

    def __init__(self, answer=GOOD, confirm=True, look=None, declared="no", capabilities=("completion", "vision")):
        self.answer, self.confirm, self.declared, self.capabilities = answer, confirm, declared, capabilities
        self.look = look or {"manner": "painted illustration", "colour": "mixed"}
        self.requests, self.chats, self.attempts = [], [], {}  # chats and attempts: the first pass only
        self.checks, self.readings = [], []  # (work number, tag or "look", model) and the text-only requests

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
        (image,) = asked["images"]
        n = int(base64.b64decode(image).removeprefix(JPEG))
        if prompt == tagging.VERIFY_PROMPT:
            tag = asked["content"].splitlines()[0].partition(": ")[2]
            self.checks.append((n, tag, chat["model"]))
            answer = self.confirm(n, tag, chat) if callable(self.confirm) else self.confirm
            return self.say({"shown_by": "FAKE-EVIDENCE", "confirmed": answer} if isinstance(answer, bool) else answer)
        if prompt == tagging.LOOK_PROMPT:
            self.checks.append((n, "look", chat["model"]))
            answer = self.look(n, chat) if callable(self.look) else self.look
            return self.say({"faces": "FAKE-FACES", **answer} if isinstance(answer, dict) else answer)
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
    assert stored() == [] and stored("work_ai_declared") == []


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
    assert len(with_image) == 1 + len(fake.checks) > 1  # the first pass and every question of the verifier
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
        assert (f"- {value}: " in tagging.LOOK_PROMPT) == (value in tagging._MANNERS)
    for rule in ("only what you can see", "leave it out", "Never name or guess the artist, the software"):
        assert rule in tagging.PROMPT
    assert "leaves no doubt" in tagging.VERIFY_PROMPT


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
    assert [len(rows) for rows in before] == [6, 3]  # three works: a first pass and a verified row each, one reading


def test_max_works_bounds_a_run_and_the_next_run_does_the_rest():
    ids = works(1, 2, 3)
    fake = FakeOllama()
    assert run(fake, max_works=2)["analyzed"] == 2
    counts = run(fake, max_works=500)
    assert (counts["analyzed"], counts["skipped"]) == (1, 2)
    assert fake.attempts == {1: 1, 2: 1, 3: 1} and len(fake.readings) == 3
    assert [row[0] for row in stored()] == ids


def test_works_another_result_set_covers_go_first_so_that_sets_line_up():
    ids = works(1, 2, 3, 4)
    run(FakeOllama(), model="model-old", max_works=0)
    with closing(sqlite3.connect(config.LOCAL_DB)) as conn, conn:  # an older result set holds the last two works
        for work_id in ids[2:]:
            conn.execute("INSERT INTO work_tags VALUES (?, 'model-old', 1, NULL, 'FAKE-TIME')", (work_id,))
    fake = FakeOllama()
    run(fake, max_works=2)
    assert sorted(fake.attempts) == [3, 4]
    assert sorted(tagging.results()) == ids[2:]


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
    by_work = {key: value | {"declared_ai": "no"} for key, value in tagging.results("model-a").items()}
    for row in rows:
        path = library.image_dir() / f"{row['deviation_id']}.jpg"
        assert row["image"] == str(path) and path.is_file()
        assert row["model"] == "model-a"
        assert (row["result"] and row["result"] | {"declared_ai": "no"}) == by_work.get(row["deviation_id"])
    assert printed("--sample", "5", "--seed", "7", "--model", "model-a") == rows
    for other in (["--model", "model-b"], ["--model", "model-a", "--verifier", "model-v"]):
        same = printed("--sample", "5", "--seed", "7", *other)
        assert [row["deviation_id"] for row in same] == [row["deviation_id"] for row in rows]
        assert {row["model"] for row in same} == {"+".join(other[1::2])}
    assert printed("--sample", "5", "--seed", "8", "--model", "model-a") != rows
    everything = printed("--sample", "40", "--seed", "7", "--model", "model-a")
    assert len(everything) == 8 and sum(row["result"] is None for row in everything) == 1


# The second pass: a verifier is asked about each tag, and what it does not confirm is taken out.


def test_the_verifier_is_asked_about_each_tag_and_what_it_does_not_confirm_is_taken_out():
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
    asked = ["character", "prop or weapon", "environment", "fantasy", "horror", "not artwork", "look"]
    assert sorted(fake.checks) == sorted((1, tag, "model-v") for tag in asked)  # each tag on its own, by the verifier
    kept = tagging.validated(first)
    assert tagging.results("model-a")[work_id] == kept | {"declared_ai": "no"}  # the first pass is a set of its own
    assert tagging.results("model-a", verifier="model-v")[work_id] == kept | {
        "subjects": ["character", "environment"],
        "genres": ["fantasy", "anime style"],
        "not_artwork": False,
        "declared_ai": "no",
    }


@pytest.mark.parametrize(
    "look, genres, style_words",
    [
        ({"manner": "anime style", "colour": "vivid"}, ["fantasy", "anime style"], ["painterly", "saturated"]),
        ({"manner": "stylized cartoon", "colour": "muted"}, ["fantasy"], ["painterly"]),
        ({"manner": "painted illustration", "colour": "mixed"}, ["fantasy"], ["painterly"]),
        ({"manner": "cubist", "colour": "vivid"}, ["fantasy"], ["painterly"]),  # not an answer: nothing confirmed
        ({"colour": "vivid"}, ["fantasy"], ["painterly"]),
        ("not json", ["fantasy"], ["painterly"]),
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
    whole = isinstance(look, dict) and look.get("manner") in tagging._MANNER_CHOICES and "colour" in look
    assert sum(check[1] == "look" for check in fake.checks) == (1 if whole else tagging.MAX_ATTEMPTS)


def test_a_work_with_no_manner_and_no_colour_word_is_not_asked_for_them_and_nothing_is_ever_added():
    (work_id,) = works(1)
    fake = FakeOllama(look={"manner": "anime style", "colour": "vivid"})
    run(fake, verifier="model-v")
    assert [check[1] for check in fake.checks] == ["character", "fantasy"]
    assert tagging.results(verifier="model-v")[work_id] == KEPT | {"declared_ai": "no"}


@pytest.mark.parametrize(
    "bad",
    [
        "",
        "not json",
        "[]",
        {},
        {"confirmed": "true"},
        {"confirmed": 1},
        {"shown_by": "FAKE-EVIDENCE"},
        httpx.Response(400),
    ],
    ids=repr,
)
def test_a_malformed_answer_from_the_verifier_confirms_nothing_and_the_run_carries_on(bad):
    first, second = works(1, 2)
    fake = FakeOllama(confirm=lambda n, tag, chat: bad if n == 1 else True)
    counts = run(fake, verifier="model-v")
    assert (counts["analyzed"], counts["failed"], counts["removed"]) == (2, 0, 2)
    assert fake.checks.count((1, "character", "model-v")) == tagging.MAX_ATTEMPTS  # asked again, a bounded number
    results = tagging.results(verifier="model-v")
    assert (results[first]["subjects"], results[first]["genres"]) == ([], [])
    assert (results[second]["subjects"], results[second]["genres"]) == (["character"], ["fantasy"])


def test_the_verifier_sees_the_image_and_the_meaning_of_the_tag_and_nothing_of_the_first_answer():
    works(1)

    class Recording(FakeOllama):
        def __call__(self, request):
            chat = json.loads(request.content)
            if chat.get("messages", [{}])[0].get("content") in (tagging.VERIFY_PROMPT, tagging.LOOK_PROMPT):
                seen.append(chat)
            return super().__call__(request)

    seen = []
    first = {**GOOD, "description": "FAKE-DESCRIPTION of a figure.", "manner": "realistic", "style_words": ["fakeword"]}
    run(Recording(first), verifier="model-v")
    assert len(seen) == 3  # the subject, the genre, the manner
    for chat in seen:
        assert chat["messages"][1]["images"] == [base64.b64encode(JPEG + b"1").decode()]
        assert "FAKE-DESCRIPTION" not in json.dumps(chat) and "fakeword" not in json.dumps(chat)
    questions = [chat["messages"][1]["content"] for chat in seen]
    assert f"Tag (main subject): character\nMeaning: {tagging.SUBJECTS['character']}" in questions
    assert f"Tag (kind of world): fantasy\nMeaning: {tagging.GENRES['fantasy']}" in questions


def test_a_second_verifier_checks_the_same_first_pass_into_a_result_set_of_its_own():
    ids = works(1, 2)
    fake = FakeOllama()
    run(fake, model="model-a", verifier="model-a")
    run(fake, model="model-a", verifier="model-v")
    run(fake, model="model-a", verifier="model-v")
    assert fake.attempts == {1: 1, 2: 1}  # one first pass a work, shared by both verifiers
    assert sorted({check[2] for check in fake.checks}) == ["model-a", "model-v"]
    assert len(fake.checks) == 2 * 2 * 2  # two works, two tags each, each verifier once
    assert sorted({row[1] for row in stored()}) == ["model-a", "model-a+model-a", "model-a+model-v"]
    for verifier in (None, "model-a", "model-v"):
        assert sorted(tagging.results("model-a", verifier=verifier)) == ids


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
        assert {row[0] for row in stored(table)} == {kept}
        assert removed not in json.dumps(stored(table))


def test_nothing_is_stored_for_a_work_once_it_is_removed_even_while_a_model_is_looking_at_it():
    for step, tables_written_before in (("declared", 0), ("answer", 1), ("confirm", 2)):
        works(1, artist=ALPHA)

        def purging(*args, step=step):
            library.purge(ALPHA)
            return {"declared": "yes", "answer": GOOD, "confirm": True}[step]

        run(FakeOllama(**{step: purging}), verifier="model-v")
        # What was stored before the removal is swept by the next run; what came after it was never stored.
        assert sum(bool(stored(table)) for table in tagging.TABLES) == tables_written_before
        assert f"{tagging.DEFAULT_MODEL}+model-v" not in {row[1] for row in stored()}
        assert tagging.results() == tagging.results(verifier="model-v") == tagging.declared_ai() == {}
        run(FakeOllama(), verifier="model-v")
        assert [stored(table) for table in tagging.TABLES] == [[], []]


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
