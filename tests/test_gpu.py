"""The owner's pause: no request reaches a local model while the pause file is there, and status reads any store."""

import json
import pathlib
import sqlite3
import sys
import time

import httpx
import pytest

from pipeline import config, library, profiles, tagging

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent / "scripts"))
import gpu  # noqa: E402


def test_a_request_to_a_model_waits_while_the_pause_file_is_there(tmp_path, monkeypatch):
    monkeypatch.setattr(config, "CACHE_DIR", tmp_path)
    (tmp_path / config.PAUSE_FILE).touch()
    sent, waits = [], []

    def ollama(request):
        sent.append(request.url.path)
        return httpx.Response(200, json={})

    def sleep(seconds):  # the owner resumes during the third wait
        assert sent == []  # nothing was asked of a model while paused
        waits.append(seconds)
        if len(waits) == 3:
            (tmp_path / config.PAUSE_FILE).unlink()

    monkeypatch.setattr(time, "sleep", sleep)
    with tagging._client(tagging.OLLAMA_URL, httpx.MockTransport(ollama)) as client:
        tagging._post(client, "/api/chat", {})
    assert sent == ["/api/chat"] and waits == [15, 15, 15]


def test_the_artwork_check_waits_while_paused_before_every_request(tmp_path, monkeypatch):
    monkeypatch.setattr(config, "CACHE_DIR", tmp_path)
    monkeypatch.setattr(config, "LOCAL_DB", tmp_path / "pipeline.sqlite")
    work_id = "00000000-0000-4000-A000-000000000001"
    library.save_image(work_id, "image/jpeg", b"\xff\xd8\xff" + b"FAKE-IMAGE-BYTES")
    work = {"work_id": work_id, "page_url": "https://www.deviantart.com/zz-fake-alpha/art/FAKE-TITLE-1"}
    work |= {"title": "FAKE-TITLE", "tags": [], "thumb_url": "https://images-wixmp-zzfake.wixmp.com/f/FAKE.jpg"}
    work |= {"published": "1"}
    library.store_artist(
        "da:zz-fake-alpha", user_id=None, tagline=None, specialty=None, level=None, website=None, works=[work]
    )
    (tmp_path / config.PAUSE_FILE).touch()
    sent, waits = [], []

    def ollama(request):
        sent.append(request.url.path)
        (tmp_path / config.PAUSE_FILE).touch()  # the owner pauses again after every request
        answer = {"kind": "screenshot", "artwork": "no"}
        return httpx.Response(200, json={"capabilities": ["vision"], "message": {"content": json.dumps(answer)}})

    def sleep(seconds):  # and resumes after one wait
        waits.append((seconds, list(sent)))
        (tmp_path / config.PAUSE_FILE).unlink()

    monkeypatch.setattr(time, "sleep", sleep)
    assert tagging.run_artwork(5, transport=httpx.MockTransport(ollama))["not_artwork"] == 1
    assert waits == [(15, []), (15, ["/api/show"])] and sent == ["/api/show", "/api/chat"]


ANSWERS = {  # each question's answer, by its prompt, and the count it shows up in
    tagging.CONTENT_PROMPT: ({"shows": "fetish", "unfit": "yes"}, "unfit"),
    tagging.SUGGESTIVE_PROMPT: ({"shows": tagging.BLOOD, "blood_only": "yes"}, "blood_only"),
    tagging.POLITICAL_PROMPT: ({"shows": tagging.REAL_PERSON, "real_person": "yes"}, "political"),
    tagging.NAMED_PROMPT: ({"says": "FAKE-WORDS", "names": "no"}, None),
}


def one_work(tmp_path, monkeypatch, tags=()):
    """A store with one invented work and its cached image. Returns its id and the image's fingerprint."""
    monkeypatch.setattr(config, "CACHE_DIR", tmp_path)
    monkeypatch.setattr(config, "LOCAL_DB", tmp_path / "pipeline.sqlite")
    work_id, jpeg = "00000000-0000-4000-A000-000000000001", b"\xff\xd8\xff" + b"FAKE-IMAGE-BYTES"
    library.save_image(work_id, "image/jpeg", jpeg)
    work = {"work_id": work_id, "page_url": "https://www.deviantart.com/zz-fake-alpha/art/FAKE-TITLE-1"}
    work |= {"title": "FAKE-TITLE", "tags": list(tags), "thumb_url": "https://images-wixmp-zzfake.wixmp.com/f/FAKE.jpg"}
    work |= {"published": "1"}
    library.store_artist(
        "da:zz-fake-alpha", user_id=None, tagline=None, specialty=None, level=None, website=None, works=[work]
    )
    return work_id, library.fingerprint(jpeg)


@pytest.mark.parametrize("stage", ["content", "suggestive", "political"])
def test_an_image_check_waits_while_paused_before_every_request(tmp_path, monkeypatch, capsys, stage):
    work_id, read = one_work(tmp_path, monkeypatch, ["propaganda"] if stage == "political" else [])
    if stage == "suggestive":  # asked only of a work rated suggestive
        tagging.results()  # so that the tables exist
        row = (work_id, profiles.TAGGER, tagging.FORMAT_VERSION, '{"formats": [], "rating": "suggestive"}', "1", read)
        with sqlite3.connect(config.LOCAL_DB) as conn:
            conn.execute("INSERT INTO work_format VALUES (?, ?, ?, ?, ?, ?)", row)
        conn.close()
    (tmp_path / config.PAUSE_FILE).touch()
    sent, waits, counted = [], [], []

    def ollama(request):
        sent.append(request.url.path)
        (tmp_path / config.PAUSE_FILE).touch()  # the owner pauses again after every request
        prompt = json.loads(request.content).get("messages", [{}])[0].get("content")  # none for /api/show
        answer, count = ANSWERS.get(prompt, ({}, None))
        counted.append(count)
        return httpx.Response(200, json={"capabilities": ["vision"], "message": {"content": json.dumps(answer)}})

    def sleep(seconds):  # and resumes after one wait
        waits.append((seconds, list(sent)))
        (tmp_path / config.PAUSE_FILE).unlink()

    monkeypatch.setattr(time, "sleep", sleep)
    tagging.main([*gpu.STAGES[stage][1:]], transport=httpx.MockTransport(ollama))  # as the worker runs it
    assert json.loads(capsys.readouterr().out)[counted[1]] == 1
    asked = ["/api/show", "/api/chat", *(["/api/chat"] if stage == "political" else [])]  # the image, then its words
    assert sent == asked and waits == [(15, asked[:n]) for n in range(len(asked))]


def test_status_counts_why_and_the_political_check_of_the_works_they_ask_of(tmp_path, monkeypatch):
    work_id, read = one_work(tmp_path, monkeypatch, ["propaganda"])
    tagging.results()  # so that the tables exist
    words = tagging.political_print(read, "FAKE-TITLE", json.dumps(["propaganda"]))
    rows = {
        "work_format": (work_id, profiles.TAGGER, tagging.FORMAT_VERSION, '{"rating": "suggestive"}', "1", read),
        "work_suggestive": (work_id, profiles.TAGGER, tagging.SUGGESTIVE_VERSION, '{"blood_only": true}', "1", read),
        "work_political": (work_id, profiles.TAGGER, tagging.POLITICAL_VERSION, '{"political": true}', "1", words),
    }
    with sqlite3.connect(config.LOCAL_DB) as conn:
        for table, row in rows.items():
            conn.execute(f"INSERT INTO {table} VALUES (?, ?, ?, ?, ?, ?)", row)
    conn.close()
    assert "  suggestive works asked why: 1 done, 0 to go; 0 in the last hour" in gpu.status()
    assert "  political candidates checked: 1 done, 0 to go; 0 in the last hour" in gpu.status()
    with sqlite3.connect(config.LOCAL_DB) as conn:  # its words changed since the political check read them
        conn.execute("UPDATE works SET tags = ?", (json.dumps(["propaganda", "zzmore"]),))
    conn.close()
    assert "  political candidates checked: 0 done, 1 to go; 0 in the last hour" in gpu.status()


def test_status_reads_a_cache_with_no_store_and_a_store_no_stage_has_filled(tmp_path, monkeypatch):
    monkeypatch.setattr(config, "CACHE_DIR", tmp_path)
    monkeypatch.setattr(config, "LOCAL_DB", tmp_path / "pipeline.sqlite")
    assert set(gpu.counts().values()) == {0}
    sqlite3.connect(config.LOCAL_DB).close()  # a store with no table in it yet
    assert set(gpu.counts().values()) == {0}
    assert gpu.status()[0] == "Not paused." and gpu.worker() is None
    (tmp_path / config.PAUSE_FILE).touch()
    assert gpu.status()[0].startswith("PAUSED")


def test_status_counts_a_reading_as_done_only_while_it_is_of_the_image_the_work_shows(tmp_path, monkeypatch):
    monkeypatch.setattr(config, "CACHE_DIR", tmp_path)
    monkeypatch.setattr(config, "LOCAL_DB", tmp_path / "pipeline.sqlite")
    work_id, key, at = "00000000-0000-4000-A000-000000000001", "da:zz-fake-alpha", "2026-01-01T00:00:00+00:00"
    jpeg = b"\xff\xd8\xff" + b"FAKE-IMAGE-BYTES"

    def collected(thumb):
        work = {"work_id": work_id, "page_url": "https://www.deviantart.com/zz-fake-alpha/art/FAKE-TITLE-1"}
        work |= {"title": "FAKE-TITLE", "tags": [], "thumb_url": thumb, "published": "1"}
        library.store_artist(key, user_id=None, tagline=None, specialty=None, level=None, website=None, works=[work])

    library.save_image(work_id, "image/jpeg", jpeg)
    collected("https://images-wixmp-zzfake.wixmp.com/f/FAKE-THUMB.jpg")
    tagging.results(), profiles.profiles()  # so that their tables exist
    read, rests = library.fingerprint(jpeg), tagging.read_as([work_id], {work_id: library.fingerprint(jpeg)}, {})
    verified = tagging.result_set(profiles.TAGGER, profiles.TAGGER)
    result = {"medium": "digital painting", "subjects": ["character"], "genres": [], "style_words": []}
    result |= {"description": "FAKE-DESCRIPTION.", "explicit": False, "not_artwork": False}
    facts, _ = profiles.facts_of([{"id": work_id, "result": result, "declared": None}])  # as the profile stage keeps
    rows = {
        "work_tags": (work_id, verified, tagging.PROMPT_VERSION, json.dumps(result), at, read),
        "work_format": (work_id, profiles.TAGGER, tagging.FORMAT_VERSION, "{}", at, read),
        "work_artwork": (work_id, profiles.TAGGER, tagging.ARTWORK_VERSION, '{"artwork": true}', at, read),
        "work_content": (work_id, profiles.TAGGER, tagging.CONTENT_VERSION, '{"fit": true}', at, read),
        "artist_profiles": (key, "FAKE-BASIS" + rests, "nothing_clear", json.dumps(facts), "[]"),
    }
    with sqlite3.connect(config.LOCAL_DB) as conn:
        conn.execute("CREATE TABLE style_vectors (deviation_id TEXT, model TEXT, vector BLOB, fingerprint TEXT)")
        conn.execute("INSERT INTO style_vectors VALUES (?, 'csd', x'00', ?)", (work_id, read))
        for table, row in rows.items():
            row = (*row, *[None] * 6, at) if table == "artist_profiles" else row
            conn.execute(f"INSERT INTO {table} VALUES ({', '.join('?' * len(row))})", row)
    conn.close()
    done = ("style vectors", "tagged and checked", "sorted by format", "checked for artwork")
    done += ("checked for unfit content", "artists profiled, or left out for a reason")
    assert [gpu.counts()[label] for label in done] == [1, 1, 1, 1, 1, 1]
    assert "  checked for artwork: 1 done, 0 to go; 0 in the last hour" in gpu.status()
    assert "  checked for unfit content: 1 done, 0 to go; 0 in the last hour" in gpu.status()

    collected("https://images-wixmp-zzfake.wixmp.com/f/FAKE-ANOTHER-FILE.jpg")  # the artist replaced the image
    assert [gpu.counts()[label] for label in done] == [0, 0, 0, 0, 0, 0] and gpu.counts()["works"] == 1
    assert "  tagged and checked: 0 done, 1 to go; 0 in the last hour" in gpu.status()
    assert "  checked for artwork: 0 done, 1 to go; 0 in the last hour" in gpu.status()
    assert "  checked for unfit content: 0 done, 1 to go; 0 in the last hour" in gpu.status()

    with sqlite3.connect(config.LOCAL_DB) as conn:
        conn.execute("DELETE FROM artist_profiles")  # nothing for status to ask the profile stage about
        conn.execute("PRAGMA user_version = 0")  # a store from before readings carried fingerprints
    conn.close()
    with pytest.raises(SystemExit) as stopped:
        gpu.status()
    assert "uv run python -m pipeline.tagging --migrate --backup" in str(stopped.value)
