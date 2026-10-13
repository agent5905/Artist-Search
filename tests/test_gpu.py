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
    rows = {
        "work_tags": (work_id, verified, tagging.PROMPT_VERSION, "{}", at, read),
        "work_format": (work_id, profiles.TAGGER, tagging.FORMAT_VERSION, "{}", at, read),
        "artist_profiles": (key, "FAKE-BASIS" + rests, "nothing_clear", json.dumps({"counted": [work_id]}), "[]"),
    }
    with sqlite3.connect(config.LOCAL_DB) as conn:
        conn.execute("CREATE TABLE style_vectors (deviation_id TEXT, model TEXT, vector BLOB, fingerprint TEXT)")
        conn.execute("INSERT INTO style_vectors VALUES (?, 'csd', x'00', ?)", (work_id, read))
        for table, row in rows.items():
            row = (*row, *[None] * 6, at) if table == "artist_profiles" else row
            conn.execute(f"INSERT INTO {table} VALUES ({', '.join('?' * len(row))})", row)
    conn.close()
    done = ("style vectors", "tagged and checked", "sorted by format", "artists profiled, or left out for a reason")
    assert [gpu.counts()[label] for label in done] == [1, 1, 1, 1]

    collected("https://images-wixmp-zzfake.wixmp.com/f/FAKE-ANOTHER-FILE.jpg")  # the artist replaced the image
    assert [gpu.counts()[label] for label in done] == [0, 0, 0, 0] and gpu.counts()["works"] == 1
    assert "  tagged and checked: 0 done, 1 to go; 0 in the last hour" in gpu.status()

    with sqlite3.connect(config.LOCAL_DB) as conn:
        conn.execute("DELETE FROM artist_profiles")  # nothing for status to ask the profile stage about
        conn.execute("PRAGMA user_version = 0")  # a store from before readings carried fingerprints
    conn.close()
    with pytest.raises(SystemExit) as stopped:
        gpu.status()
    assert "uv run python -m pipeline.tagging --migrate --backup" in str(stopped.value)
