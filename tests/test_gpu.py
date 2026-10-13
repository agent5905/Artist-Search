"""The owner's pause: no request reaches a local model while the pause file is there, and status reads any store."""

import pathlib
import sqlite3
import sys
import time

import httpx

from pipeline import config, tagging

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
