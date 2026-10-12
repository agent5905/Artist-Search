"""Tests for pipeline.tagging.

Ollama is a fake behind httpx.MockTransport and every image is a few invented bytes: no model, no GPU, no network.
Opening a real connection fails the test. Every handle, title, address and answer below is invented.
"""

import base64
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
GOOD = {
    "description": "A red circle on a blue field.",
    "medium": "digital painting",
    "subjects": ["character"],
    "genres": ["fantasy"],
    "style_words": ["flat-colour"],
    "explicit": False,
    "not_artwork": False,
}
COUNTS = re.compile(
    r'\{"analyzed": \d+, "failed": \d+, "flagged": \d+, "skipped": \d+, "seconds_per_image": \d+\.\d+\}\n'
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


def works(*numbers, artist=ALPHA, without_image=()):
    """Collect these invented works as all the artist has. Each image is unique to its work: JPEG, then its number."""
    ids = [f"00000000-0000-4000-A000-{n:012d}" for n in numbers]
    for n, work_id in zip(numbers, ids, strict=True):
        if n not in without_image:
            library.save_image(work_id, "image/jpeg", JPEG + str(n).encode())
    rows = [
        {
            "work_id": work_id,
            "page_url": f"https://www.deviantart.com/zz-fake-alpha/art/FAKE-TITLE-{n}",
            "title": f"FAKE-TITLE-{n}",
            "tags": ["faketag"],
            "thumb_url": "https://images-wixmp-zzfake.wixmp.com/f/FAKE-THUMB.jpg",
            "published": "1700000000",
        }
        for n, work_id in zip(numbers, ids, strict=True)
    ]
    library.store_artist(artist, user_id=None, tagline=None, specialty=None, level=None, website=None, works=rows)
    return ids


class FakeOllama:
    """Answers /api/show and /api/chat. `answer` is what the model says, or a function (work number, attempt,
    request body) -> that. A dict is sent as the model's JSON, a str as it is, a Response whole, an error raised."""

    def __init__(self, answer=GOOD, capabilities=("completion", "vision")):
        self.answer, self.capabilities = answer, capabilities
        self.requests, self.chats, self.attempts = [], [], {}

    def __call__(self, request):
        self.requests.append(request)
        if request.url.path == "/api/show":
            return httpx.Response(200, json={"capabilities": list(self.capabilities)})
        assert request.url.path == "/api/chat"
        chat = json.loads(request.content)
        self.chats.append(chat)
        (image,) = chat["messages"][-1]["images"]
        n = int(base64.b64decode(image).removeprefix(JPEG))
        self.attempts[n] = self.attempts.get(n, 0) + 1
        answer = self.answer(n, self.attempts[n], chat) if callable(self.answer) else self.answer
        if isinstance(answer, Exception):
            raise answer
        if isinstance(answer, httpx.Response):
            return answer
        content = json.dumps(answer) if isinstance(answer, dict) else answer
        return httpx.Response(200, json={"message": {"role": "assistant", "content": content}})


def run(fake, max_works=500, **kwargs):
    """Run the stage against the fake. The counts come back without the throughput, which is a real duration."""
    counts = tagging.run(max_works, transport=httpx.MockTransport(fake), **kwargs)
    assert isinstance(counts.pop("seconds_per_image"), float)
    return counts


def stored():
    """Every row of the table as (work, model, prompt version, result or None), read straight from the file."""
    tagging.results()  # so the table exists even when the call under test refused before opening the file
    with closing(sqlite3.connect(config.LOCAL_DB)) as conn:
        rows = conn.execute("SELECT deviation_id, model, prompt_version, result FROM work_tags ORDER BY 1, 2, 3")
        return [(*row[:3], row[3] and json.loads(row[3])) for row in rows]


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
        run(fake, ollama_url=url)
    assert fake.requests == []
    assert stored() == []


@pytest.mark.parametrize(
    "url, host",
    [("http://127.0.0.1:11434", "127.0.0.1"), ("http://127.0.0.9:8080/", "127.0.0.9"), ("http://[::1]:11434", "::1")],
)
def test_a_loopback_ollama_gets_the_image_and_the_prompt_and_nothing_about_the_work(url, host):
    (work_id,) = works(1)
    fake = FakeOllama()
    assert run(fake, ollama_url=url)["analyzed"] == 1
    assert {request.url.host for request in fake.requests} == {host}
    assert [request.url.path for request in fake.requests] == ["/api/show", "/api/chat"]
    (chat,) = fake.chats
    assert chat["messages"][0] == {"role": "system", "content": tagging.PROMPT}
    assert chat["messages"][1]["images"] == [base64.b64encode(JPEG + b"1").decode()]
    assert chat["think"] is False and chat["stream"] is False
    assert chat["format"]["properties"]["medium"]["enum"] == list(tagging.MEDIUMS)
    assert chat["format"]["properties"]["subjects"]["items"]["enum"] == list(tagging.SUBJECTS)
    assert chat["format"]["properties"]["genres"]["items"]["enum"] == list(tagging.GENRES)
    sent = b"".join(request.content for request in fake.requests).decode().lower()
    for kept_back in (work_id, "zz-fake-alpha", "fake-title", "faketag", "deviantart", "wixmp", ".jpg"):
        assert kept_back.lower() not in sent


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


def test_a_model_that_cannot_read_images_is_refused_before_any_work_is_asked():
    works(1)
    fake = FakeOllama(capabilities=("completion", "tools"))
    with pytest.raises(ValueError, match="cannot read images"):
        run(fake)
    assert fake.chats == []
    assert stored() == []


def test_the_prompt_names_every_vocabulary_value_and_forbids_guessing():
    for value in (*tagging.MEDIUMS, *tagging.SUBJECTS, *tagging.GENRES):
        assert f"- {value}: " in tagging.PROMPT
    for rule in ("only what you can see", "leave it out", "Never name or guess the artist, the software"):
        assert rule in tagging.PROMPT


# 2. Out-of-vocabulary values never reach the store; a malformed or empty answer never crashes the run.


def test_out_of_vocabulary_values_never_reach_the_store():
    (work_id,) = works(1)
    answer = {
        "description": "  A figure\n stands.   It holds a sword. A third sentence. A fourth.  ",
        "medium": "Oil On Velvet",
        "subjects": ["Character ", "spaceship", "character", 7, None, "prop or weapon", ["creature"]],
        "genres": ["grimdark", "FANTASY", {"genre": "horror"}, "anime", "fantasy"],
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
    assert run(FakeOllama(answer)) == {"analyzed": 1, "failed": 0, "flagged": 0, "skipped": 0}
    expected = {
        "medium": "unclear",
        "subjects": ["character", "prop or weapon"],
        "genres": ["fantasy"],
        "style_words": ["painterly", "cel-shaded", "high contrast", "muted", "sketchy", "textured"],
        "description": "A figure stands. It holds a sword.",
        "explicit": False,
        "not_artwork": False,
    }
    assert stored() == [(work_id, tagging.DEFAULT_MODEL, tagging.PROMPT_VERSION, expected)]
    assert in_vocabulary(expected)
    assert tagging.validated({**GOOD, "medium": " 3D Render "})["medium"] == "3d render"


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
    counts = run(fake)
    assert (counts["analyzed"], counts["failed"], counts["skipped"]) == (1, 1, 0)
    assert fake.attempts == {1: tagging.MAX_ATTEMPTS, 2: 1}  # asked again a bounded number of times
    assert [(row[0], row[3] is None) for row in stored()] == [(first, True), (second, False)]
    assert list(tagging.results()) == [second]


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


# 3. A rerun creates no duplicates and re-analyzes nothing.


def test_a_rerun_asks_nothing_and_stores_nothing_new():
    works(1, 2, 3)
    fake = FakeOllama(lambda n, attempt, chat: "" if n == 2 else GOOD)
    assert run(fake) == {"analyzed": 2, "failed": 1, "flagged": 0, "skipped": 0}
    with closing(sqlite3.connect(config.LOCAL_DB)) as conn:
        before = conn.execute("SELECT * FROM work_tags ORDER BY 1").fetchall()
    again = FakeOllama()
    assert run(again) == {"analyzed": 0, "failed": 0, "flagged": 0, "skipped": 3}
    assert again.requests == []  # the failed work is not asked again either
    with closing(sqlite3.connect(config.LOCAL_DB)) as conn:
        assert conn.execute("SELECT * FROM work_tags ORDER BY 1").fetchall() == before
    assert len(before) == 3


def test_max_works_bounds_a_run_and_the_next_run_does_the_rest():
    ids = works(1, 2, 3)
    fake = FakeOllama()
    assert run(fake, max_works=2)["analyzed"] == 2
    counts = run(fake, max_works=500)
    assert (counts["analyzed"], counts["skipped"]) == (1, 2)
    assert fake.attempts == {1: 1, 2: 1, 3: 1}
    assert [row[0] for row in stored()] == ids


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

    monkeypatch.setattr(tagging, "PROMPT_VERSION", tagging.PROMPT_VERSION + 1)
    assert run(fake, model="model-a")["analyzed"] == 1  # a changed prompt is asked again
    assert [row[1:3] for row in stored()] == [("model-a", 1), ("model-a", 2), ("model-b", 1)]
    assert set(tagging.results("model-a", prompt_version=1)) == {work_id}


# 4. Flagged works are stored with the flag, not dropped.


def test_explicit_and_not_artwork_works_are_stored_with_their_flag():
    explicit, not_artwork, plain = works(1, 2, 3)
    flags = {1: {"explicit": True}, 2: {"not_artwork": True}, 3: {}}
    counts = run(FakeOllama(lambda n, attempt, chat: {**GOOD, **flags[n]}))
    assert (counts["analyzed"], counts["flagged"], counts["failed"]) == (3, 2, 0)
    results = tagging.results()
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
    tagging.main(["--max-works", "500"], transport=httpx.MockTransport(FakeOllama(lambda n, *_: answers[n])))
    captured = capsys.readouterr()
    assert COUNTS.fullmatch(captured.out)
    assert captured.err == ""
    counts = json.loads(captured.out)
    assert (counts["analyzed"], counts["failed"], counts["flagged"], counts["skipped"]) == (2, 1, 1, 0)


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
    run(answer, model="model-a")
    run(answer, model="model-b")
    capsys.readouterr()

    def printed(*arguments):
        tagging.main(list(arguments))  # no transport: a sample that connected anywhere would fail the test
        return [json.loads(line) for line in capsys.readouterr().out.splitlines()]

    rows = printed("--sample", "5", "--seed", "7", "--model", "model-a")
    assert len(rows) == 5 and len({row["deviation_id"] for row in rows}) == 5
    by_work = dict(tagging.results("model-a"))
    for row in rows:
        path = library.image_dir() / f"{row['deviation_id']}.jpg"
        assert row["image"] == str(path) and path.is_file()
        assert row["model"] == "model-a"
        assert row["result"] == by_work.get(row["deviation_id"])  # None for the work that failed
    assert printed("--sample", "5", "--seed", "7", "--model", "model-a") == rows
    other = printed("--sample", "5", "--seed", "7", "--model", "model-b")
    assert [row["deviation_id"] for row in other] == [row["deviation_id"] for row in rows]
    assert printed("--sample", "5", "--seed", "8", "--model", "model-a") != rows
    everything = printed("--sample", "40", "--seed", "7", "--model", "model-a")
    assert len(everything) == 8 and sum(row["result"] is None for row in everything) == 1


# Works that are not there to analyze, and works that were removed.


def test_a_work_without_a_cached_image_is_left_alone():
    no_image, gone, outside = works(1, 2, 3, without_image=(1,))
    (library.image_dir() / f"{gone}.jpg").unlink()
    with closing(sqlite3.connect(config.LOCAL_DB)) as conn, conn:
        conn.execute("UPDATE works SET image_file = '../pipeline.sqlite' WHERE deviation_id = ?", (outside,))
    fake = FakeOllama()
    counts = run(fake)
    assert (counts["analyzed"], counts["failed"], counts["skipped"]) == (0, 0, 2)
    assert fake.chats == []
    assert stored() == []
    assert no_image not in tagging.results()


def test_a_removed_artist_is_not_read_back_and_their_rows_go_at_the_next_run():
    (removed,), (kept,) = works(1, artist=ALPHA), works(2, artist=BRAVO)
    run(FakeOllama())
    library.purge(ALPHA)
    assert list(tagging.results()) == [kept]
    assert tagging.sample(40, 7) == [row for row in tagging.sample(40, 7) if row["deviation_id"] == kept]
    run(FakeOllama())
    assert [row[0] for row in stored()] == [kept]
    assert removed not in json.dumps(stored())


def test_a_work_removed_while_the_model_was_looking_at_it_is_not_stored():
    works(1, artist=ALPHA)

    def answer(n, attempt, chat):
        library.purge(ALPHA)
        return GOOD

    run(FakeOllama(answer))
    assert stored() == []
