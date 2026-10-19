"""Tests for pipeline.style.

Every handle, address and image below is invented here: flat 8x8 squares made in memory. The model is a
stand-in that turns a square's red value into a vector, so nothing touches the GPU and nothing is downloaded.
The cache directory and the database are moved into pytest's temporary directory.
"""

import inspect
import io
import json
import os
import re
import socket
import sqlite3
import subprocess
import sys
from contextlib import closing

import numpy as np
import pytest
from PIL import Image

from pipeline import config, library, style

ALPHA, BRAVO, CHARLIE, DELTA, ECHO, FOXTROT, GOLF = (
    f"da:zz-fake-{name}" for name in ("alpha", "bravo", "charlie", "delta", "echo", "foxtrot", "golf")
)
# Begins the way a PNG does, so the library keeps it, and is not an image.
BROKEN = b"\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR" + b"FAKE-BYTES-THAT-ARE-NOT-AN-IMAGE"


def work_id(n):
    return f"00000000-0000-4000-A000-{n:012d}"


def square(shade):
    buffer = io.BytesIO()
    Image.new("RGB", (8, 8), (shade, 0, 0)).save(buffer, "PNG")
    return buffer.getvalue()


def collect(artist, *squares, broken=(), bare=()):
    """Collect one invented artist. Each number is a work of theirs.

    Its cached image is a flat square whose red value is that number, bytes that are not an image (`broken`),
    or nothing at all (`bare`).
    """
    for n in squares:
        library.save_image(work_id(n), "image/png", square(n))
    for n in broken:
        library.save_image(work_id(n), "image/png", BROKEN)
    works = [
        {
            "work_id": work_id(n),
            "page_url": f"https://www.deviantart.com/zz-fake/art/FAKE-TITLE-{n}",
            "title": "FAKE-TITLE",
            "tags": [],
            "thumb_url": "https://images-wixmp-zzfake.wixmp.com/f/FAKE-THUMB.jpg",
            "published": "1",
        }
        for n in (*squares, *broken, *bare)
    ]
    library.store_artist(artist, user_id=None, tagline=None, specialty=None, level=None, website=None, works=works)


class FakeModel:
    """Stands in for style.load_model. A square's red value picks its vector; it notes what it is asked for."""

    def __init__(self):
        self.table = {}  # red value -> the first few numbers of its vector; the rest are zero
        self.loads = 0
        self.batches = []  # the red values of each batch it was given
        self.meanwhile = None  # something to happen while a batch is in the model

    def __call__(self, model):
        self.loads += 1
        return self.embed

    def embed(self, images):
        self.batches.append([image.getpixel((0, 0))[0] for image in images])
        if self.meanwhile:
            self.meanwhile()
        # Like a GPU, it answers differently when the batch is short: the stage must always send a full one.
        return np.array([self.vector(shade) for shade in self.batches[-1]]) + (style.BATCH - len(images))

    def vector(self, shade):
        if shade in self.table:
            return np.pad(np.array(self.table[shade], dtype=float), (0, 768 - len(self.table[shade])))
        return np.random.default_rng(shade).normal(size=768) * 3  # not unit length: the stage has to make it so


def unsynced(*args, connect=sqlite3.connect, **kwargs):
    """sqlite3.connect that does not wait for the disk after each commit, as in test_library.py."""
    conn = connect(*args, **kwargs)
    conn.execute("PRAGMA synchronous = OFF")
    return conn


@pytest.fixture(autouse=True)
def cache(tmp_path, monkeypatch):
    monkeypatch.setattr(config, "CACHE_DIR", tmp_path / "cache")
    monkeypatch.setattr(config, "LOCAL_DB", tmp_path / "cache" / "pipeline.sqlite")
    monkeypatch.setattr(sqlite3, "connect", unsynced)  # a busy disk makes each commit take seconds


@pytest.fixture
def model(monkeypatch):
    fake = FakeModel()
    monkeypatch.setattr(style, "load_model", fake)
    return fake


@pytest.fixture
def no_network(monkeypatch):
    """Every way to open a connection is refused, and each attempt is written down."""
    attempts = []

    def refuse(*args, **kwargs):
        attempts.append(args)
        raise OSError("the network is off in this test")

    for name in ("connect", "connect_ex"):
        monkeypatch.setattr(socket.socket, name, refuse)
    for name in ("getaddrinfo", "create_connection"):
        monkeypatch.setattr(socket, name, refuse)
    return attempts


def stored():
    with closing(sqlite3.connect(config.LOCAL_DB)) as conn:
        return conn.execute("SELECT deviation_id, model, vector FROM style_vectors ORDER BY 1, 2").fetchall()


def unit(values):
    return np.asarray(values) / np.linalg.norm(values)


# 1. Hard rules 2 and 9: the stage works with the network unavailable, and never reaches for it.


def test_a_run_opens_no_connection(model, no_network):
    collect(ALPHA, 1, 2)
    collect(BRAVO, 3)
    assert style.main(["--model", "csd"]) == 0
    assert style.main(["--neighbors", ALPHA, "--model", "csd"]) == 0
    assert len(stored()) == 3
    assert no_network == []
    with pytest.raises(OSError):  # and the guard is not a dud
        socket.socket().connect(("127.0.0.1", 9))
    assert len(no_network) == 1


def test_the_real_loaders_set_the_offline_switches_and_never_reach_for_the_network(tmp_path):
    """The real load_model, in its own process, with the network refused and no weights on the machine.

    Each model must stop with the note about fetching its weights, without one attempt to connect.
    """
    probe = """
import os, socket
def refuse(*args, **kwargs):
    print("CONNECTION ATTEMPT", flush=True)
    raise OSError("the network is off in this test")
socket.socket.connect = socket.socket.connect_ex = socket.getaddrinfo = socket.create_connection = refuse
from pipeline import style
for model in sorted(style.WEIGHTS):
    try:
        style.load_model(model)
        print(model, "LOADED")
    except SystemExit as stop:
        print(model, "stopped:", stop)
print("switches", os.environ["HF_HUB_OFFLINE"], os.environ["TRANSFORMERS_OFFLINE"])
"""
    empty = str(tmp_path / "no-weights-here")
    env = {**os.environ, "HF_HOME": empty, "HF_HUB_CACHE": empty, "HF_HUB_OFFLINE": "0", "TRANSFORMERS_OFFLINE": "0"}
    result = subprocess.run(
        [sys.executable, "-c", probe], cwd=config.ROOT, env=env, capture_output=True, text=True, timeout=300
    )
    fetch = "No local weights. Fetch them once: python -m pipeline.style --download --model"
    assert result.stdout.splitlines() == [
        f"csd stopped: {fetch} csd",
        f"siglip2 stopped: {fetch} siglip2",
        "switches 1 1",
    ]


# 2. Vectors are unit length, 768 numbers, finite, and the same image gives the same vector again.


def test_vectors_are_unit_length_768_finite_float16_and_the_same_on_a_rerun(model, monkeypatch):
    monkeypatch.setattr(style, "BATCH", 2)
    collect(ALPHA, 1, 2, 3, 4, 5)
    style.run("csd", 500)
    first = stored()
    assert len(first) == 5 and all(len(blob) == 768 * 2 for _, _, blob in first)
    for n in range(1, 6):
        vector = style.vector(work_id(n), "csd")
        assert vector.dtype == np.float16 and vector.shape == (768,)
        assert np.isfinite(vector).all()
        assert abs(np.linalg.norm(vector.astype(np.float64)) - 1) < 1e-3
        assert np.allclose(vector, unit(model.vector(n)), atol=1e-3)  # this work's own vector, made unit length
    assert len({blob for _, _, blob in first}) == 5

    def forget():
        with closing(sqlite3.connect(config.LOCAL_DB)) as conn, conn:
            conn.execute("DELETE FROM style_vectors")

    forget()  # from nothing again: written after the first four works and at the end
    monkeypatch.setattr(style, "WRITE_EVERY", 4)
    assert style.run("csd", 500)["embedded"] == 5
    assert stored() == first
    forget()  # and again, one work a run: each image has other company in its batch, or none
    for _ in range(5):
        style.run("csd", 1)
    assert stored() == first
    assert {len(batch) for batch in model.batches} == {2}  # the model was only ever given full batches


# 3. A rerun creates no duplicates and computes nothing.


def test_a_rerun_adds_nothing_and_computes_nothing(model):
    collect(ALPHA, 1, 2, 3, 4, 5)
    assert style.run("csd", 2)["embedded"] == 2
    assert style.run("csd", 500)["embedded"] == 3  # the rest, and not the first two again
    assert [sorted(set(batch)) for batch in model.batches] == [[1, 2], [3, 4, 5]] and model.loads == 2
    first = stored()
    assert [(work, name) for work, name, _ in first] == [(work_id(n), "csd") for n in range(1, 6)]

    assert style.run("csd", 500) == {"embedded": 0, "failed": 0}
    assert (len(model.batches), model.loads) == (2, 2)  # nothing computed: the model was not even loaded
    assert stored() == first

    assert style.run("siglip2", 500)["embedded"] == 5  # the other model keeps its own vectors for the same works
    assert len(stored()) == 10 and [row for row in stored() if row[1] == "csd"] == first


# 4. The ranking is the database's rule, worked by hand.


def test_neighbors_is_the_mean_over_the_seeds_works_of_the_best_match_among_the_candidates(model, capsys):
    model.table = {
        1: [1, 0, 0],  # ALPHA
        2: [0, 1, 0],  # ALPHA
        3: [2, 0, 0],  # BRAVO: ALPHA's first work exactly, twice as long (length must not count)
        4: [1, 1, 0],  # BRAVO: halfway between ALPHA's two
        5: [3, 4, 0],  # CHARLIE: cosine 0.6 with ALPHA's first, 0.8 with its second
        6: [0, 0, 1],  # DELTA: at right angles to all of the above
        7: [0, 0, 5],  # GOLF: the same direction as DELTA
    }
    for artist, shades in ((ALPHA, (1, 2)), (BRAVO, (3, 4)), (CHARLIE, (5,)), (DELTA, (6,)), (GOLF, (7,))):
        collect(artist, *shades)
    collect(ECHO, broken=[8])  # collected, but no vector could be made
    collect(FOXTROT, bare=[9])  # collected, but no image
    style.run("csd", 500)
    capsys.readouterr()

    def nearest(seed, top=10):
        return [(key, pytest.approx(score, abs=1e-3)) for key, score in style.neighbors(seed, "csd", top, "best")]

    half = 2**-0.5
    # ALPHA's first work finds itself in BRAVO (1.0), its second finds BRAVO's halfway work (0.7071).
    # In CHARLIE they find 0.6 and 0.8. DELTA and GOLF tie at nothing, and the lower key goes first.
    assert nearest(ALPHA) == [(BRAVO, (1 + half) / 2), (CHARLIE, 0.7), (DELTA, 0.0), (GOLF, 0.0)]
    assert nearest(ALPHA, top=2) == [(BRAVO, (1 + half) / 2), (CHARLIE, 0.7)]
    # The mean runs over the SEED's works: CHARLIE's one work finds 0.8 in ALPHA, though ALPHA found 0.7 in it.
    assert nearest(CHARLIE) == [(BRAVO, 7 * half / 5), (ALPHA, 0.8), (DELTA, 0.0), (GOLF, 0.0)]
    assert nearest(BRAVO)[:2] == [(ALPHA, (1 + half) / 2), (CHARLIE, (0.6 + 7 * half / 5) / 2)]
    assert nearest(DELTA)[0] == (GOLF, 1.0)
    for seed in (ALPHA, BRAVO, CHARLIE, DELTA, GOLF):  # never the seed, never an artist without vectors
        assert not {seed, ECHO, FOXTROT} & {key for key, _ in style.neighbors(seed, "csd")}
    assert style.neighbors(ECHO, "csd") == style.neighbors(FOXTROT, "csd") == style.neighbors("da:nobody", "csd") == []
    assert style.neighbors(ALPHA, "siglip2") == []  # another model's vectors are another matter

    assert style.main(["--neighbors", ALPHA, "--model", "csd", "--top", "2", "--rule", "best"]) == 0
    printed = [json.loads(line) for line in capsys.readouterr().out.splitlines()]
    assert printed == [{"artist": BRAVO, "score": 0.8536}, {"artist": CHARLIE, "score": 0.7}]
    assert style.main(["--neighbors", ECHO, "--model", "csd"]) == 1
    assert capsys.readouterr().out == ""


def test_closest_works_are_the_other_artists_works_nearest_any_of_the_seeds_best_first(model):
    model.table = {
        1: [1, 0, 0],  # ALPHA
        2: [0, 1, 0],  # ALPHA, in another manner
        3: [0, 0, 1],  # BRAVO: like neither
        4: [3, 4, 0],  # BRAVO: 0.8 with ALPHA's second work
        5: [0, 2, 0],  # BRAVO: ALPHA's second work exactly
        6: [5, 0, 0],  # BRAVO: ALPHA's first work exactly, so a tie that goes to the lower id
    }
    collect(ALPHA, 1, 2)
    collect(BRAVO, 3, 4, 5, 6)
    collect(ECHO, broken=[8])  # collected, but no vector could be made
    style.run("csd", 500)
    assert style.closest_works(ALPHA, BRAVO, "csd") == [work_id(5), work_id(6), work_id(4), work_id(3)]
    assert style.closest_works(ALPHA, BRAVO, "csd", top=2) == [work_id(5), work_id(6)]
    assert style.closest_works(BRAVO, ALPHA, "csd") == [work_id(1), work_id(2)]
    assert style.closest_works(ALPHA, ECHO, "csd") == style.closest_works(ECHO, ALPHA, "csd") == []
    assert style.closest_works(ALPHA, BRAVO, "siglip2") == []  # another model's vectors are another matter


def test_all_closest_works_is_closest_works_for_many_pairs_from_one_read_and_only_the_works_given_count(
    model, monkeypatch
):
    model.table = {
        1: [1, 0, 0],  # ALPHA
        2: [0, 1, 0],  # ALPHA, in another manner
        3: [0, 0, 1],  # BRAVO: like neither
        4: [3, 4, 0],  # BRAVO: 0.8 with ALPHA's second work, 0.6 with its first
        5: [0, 2, 0],  # BRAVO: ALPHA's second work exactly
        6: [5, 0, 0],  # BRAVO: ALPHA's first work exactly
        7: [0, 0, 3],  # CHARLIE: BRAVO's first work exactly, and a tie between the rest of BRAVO's
    }
    collect(ALPHA, 1, 2)
    collect(BRAVO, 3, 4, 5, 6)
    collect(CHARLIE, 7)
    collect(ECHO, broken=[8])  # collected, but no vector could be made
    style.run("csd", 500)
    every = {ALPHA: [work_id(1), work_id(2)], BRAVO: [work_id(n) for n in (6, 5, 4, 3)], CHARLIE: [work_id(7)]}
    every[ECHO] = [work_id(8)]
    pairs = [(ALPHA, BRAVO), (BRAVO, ALPHA), (CHARLIE, BRAVO), (ALPHA, ECHO), (ECHO, ALPHA), (ALPHA, "da:nobody")]
    reads, read = [], style._vectors
    monkeypatch.setattr(style, "_vectors", lambda *args: reads.append(args) or read(*args))

    found = style.all_closest_works(pairs, every, "csd")
    assert len(reads) == 1  # however many pairs
    # With all their works counting it is closest_works for each pair: the order they are given in changes nothing.
    assert found == {pair: style.closest_works(*pair, "csd") for pair in pairs}
    assert found[ALPHA, BRAVO] == [work_id(5), work_id(6), work_id(4), work_id(3)]
    assert found[CHARLIE, BRAVO] == [work_id(3), work_id(4), work_id(5), work_id(6)] and found[ALPHA, ECHO] == []
    assert style.all_closest_works(pairs[:1], every, "csd", top=2) == {(ALPHA, BRAVO): [work_id(5), work_id(6)]}
    # Only the works given count, on both sides. Without ALPHA's second work, BRAVO's copy of its first leads and
    # the work halfway scores 0.6; BRAVO's copy of the second is not given, so it is not answered with at all.
    shown = {ALPHA: [work_id(1)], BRAVO: [work_id(3), work_id(4), work_id(6)], ECHO: [work_id(8)]}
    assert style.all_closest_works([(ALPHA, BRAVO), (BRAVO, ALPHA), (ALPHA, CHARLIE), (ALPHA, ECHO)], shown, "csd") == {
        (ALPHA, BRAVO): [work_id(6), work_id(4), work_id(3)],
        (BRAVO, ALPHA): [work_id(1)],
        (ALPHA, CHARLIE): [],  # none of CHARLIE's works counts
        (ALPHA, ECHO): [],  # the one that counts has no vector
    }
    assert style.all_closest_works(pairs, every, "siglip2") == dict.fromkeys(pairs, [])
    # With a floor, only the works whose best match reaches it: BRAVO's two copies and the work at 0.8, then
    # the copies alone. A pair can be left with none.
    floored = [(ALPHA, BRAVO), (BRAVO, ALPHA), (CHARLIE, BRAVO)]
    assert style.all_closest_works(floored, every, "csd", floor=0.7) == {
        (ALPHA, BRAVO): [work_id(5), work_id(6), work_id(4)],
        (BRAVO, ALPHA): [work_id(1), work_id(2)],
        (CHARLIE, BRAVO): [work_id(3)],
    }
    assert style.all_closest_works(floored, every, "csd", floor=0.9)[ALPHA, BRAVO] == [work_id(5), work_id(6)]
    assert style.all_closest_works(floored, every, "csd", top=1, floor=1.5) == dict.fromkeys(floored, [])
    # The push critic's: a vector stored at another length is the same direction, and only the cosine counts.
    # BRAVO's work at 0.8, stored twice as long, once passed the floor at 1.6.
    longer = (2 * style.vector(work_id(4), "csd").astype(np.float32)).astype(np.float16).tobytes()
    with closing(sqlite3.connect(config.LOCAL_DB)) as conn, conn:
        conn.execute("UPDATE style_vectors SET vector = ? WHERE deviation_id = ?", (longer, work_id(4)))
    assert style.all_closest_works(floored, every, "csd", floor=0.9)[ALPHA, BRAVO] == [work_id(5), work_id(6)]
    assert style.closest_works(ALPHA, BRAVO, "csd") == found[ALPHA, BRAVO]  # the same order as at unit length


def test_the_mutual_rule_puts_a_consistent_artist_above_one_with_a_bit_of_everything(model, capsys):
    model.table = {
        1: [1, 0, 0],  # ALPHA, the seed
        2: [0, 1, 0],  # ALPHA
        3: [1, 0, 0],  # BRAVO, a mixed portfolio: ALPHA's first work exactly,
        4: [0, 1, 0],  # ALPHA's second exactly,
        5: [0, 0, 1],  # and two works like nothing of ALPHA's
        6: [0, 0, 0, 1],
        7: [4, 3, 0],  # CHARLIE, consistent: cosine 0.8 and 0.6 with ALPHA's two
        8: [3, 4, 0],  # CHARLIE: 0.6 and 0.8
        9: [0, 1, 1],  # DELTA, one work: cosine 0 and 0.7071 with ALPHA's two
    }
    for artist, shades in ((ALPHA, (1, 2)), (BRAVO, (3, 4, 5, 6)), (CHARLIE, (7, 8)), (DELTA, (9,))):
        collect(artist, *shades)
    style.run("csd", 500)
    capsys.readouterr()

    def nearest(seed, rule):
        return [(key, pytest.approx(score, abs=1e-3)) for key, score in style.neighbors(seed, "csd", rule=rule)]

    half = 2**-0.5
    # "best", from ALPHA: both its works find themselves in BRAVO (1.0); in CHARLIE each finds 0.8;
    # in DELTA they find 0 and 0.7071.
    assert nearest(ALPHA, "best") == [(BRAVO, 1.0), (CHARLIE, 0.8), (DELTA, half / 2)]
    # "mutual" adds the way back. BRAVO's four works find 1, 1, 0 and 0 in ALPHA: (1.0 + 0.5) / 2 = 0.75.
    # CHARLIE's two each find 0.8: (0.8 + 0.8) / 2 = 0.8. DELTA's one finds 0.7071: (0.3536 + 0.7071) / 2.
    assert nearest(ALPHA, "mutual") == [(CHARLIE, 0.8), (BRAVO, 0.75), (DELTA, 3 * half / 4)]
    # It is the same score from either end, which "best" is not. From BRAVO, "best": its four works find
    # 1, 1, 0 and 0 in ALPHA (0.5), 0.8, 0.8, 0 and 0 in CHARLIE (0.4), and 0, 0.7071, 0.7071 and 0 in DELTA.
    assert nearest(BRAVO, "best") == [(ALPHA, 0.5), (CHARLIE, 0.4), (DELTA, half / 2)]
    assert nearest(BRAVO, "mutual") == [(ALPHA, 0.75), (CHARLIE, 0.6), (DELTA, 3 * half / 4)]
    # From CHARLIE: its works find 0.8 and 0.8 in BRAVO, and BRAVO's find 0.8, 0.8, 0 and 0 in it: (0.8 + 0.4) / 2.
    # In DELTA they find 0.4243 and 0.5657, and DELTA's one finds 0.5657 in it: (0.4950 + 0.5657) / 2.
    assert nearest(CHARLIE, "mutual") == [(ALPHA, 0.8), (BRAVO, 0.6), (DELTA, 3 * half / 4)]
    for seed in (ALPHA, BRAVO, CHARLIE, DELTA):
        assert seed not in [key for key, _ in style.neighbors(seed, "csd", rule="mutual")]
    assert style.neighbors("da:nobody", "csd", rule="mutual") == []
    with pytest.raises(ValueError):
        style.neighbors(ALPHA, "csd", rule="closest")

    def printed(*extra):
        assert style.main(["--neighbors", ALPHA, "--model", "csd", "--top", "2", *extra]) == 0
        rows = [json.loads(line) for line in capsys.readouterr().out.splitlines()]
        return [(row["artist"], pytest.approx(row["score"], abs=1e-3)) for row in rows]

    assert printed("--rule", "best") == [(BRAVO, 1.0), (CHARLIE, 0.8)]
    assert printed("--rule", "mutual") == [(CHARLIE, 0.8), (BRAVO, 0.75)]
    with pytest.raises(SystemExit):
        style.main(["--neighbors", ALPHA, "--model", "csd", "--rule", "closest"])


def test_the_few_rule_judges_each_artist_by_their_closest_works_alone(model, monkeypatch):
    model.table = {  # as in the test above
        1: [1, 0, 0],  # ALPHA, the seed
        2: [0, 1, 0],  # ALPHA
        3: [1, 0, 0],  # BRAVO, a mixed portfolio: ALPHA's first work exactly,
        4: [0, 1, 0],  # ALPHA's second exactly,
        5: [0, 0, 1],  # and two works like nothing of ALPHA's
        6: [0, 0, 0, 1],
        7: [4, 3, 0],  # CHARLIE, consistent: cosine 0.8 and 0.6 with ALPHA's two
        8: [3, 4, 0],  # CHARLIE: 0.6 and 0.8
        9: [0, 1, 1],  # DELTA, one work: cosine 0 and 0.7071 with ALPHA's two
    }
    for artist, shades in ((ALPHA, (1, 2)), (BRAVO, (3, 4, 5, 6)), (CHARLIE, (7, 8)), (DELTA, (9,))):
        collect(artist, *shades)
    style.run("csd", 500)

    def nearest(seed, rule):
        return [(key, pytest.approx(score, abs=1e-3)) for key, score in style.neighbors(seed, "csd", rule=rule)]

    # Nobody has more than FEW (4) works, so each is judged by all of them: "few" is "local".
    few = style.all_neighbors("csd", rule="few")
    assert {key: [(other, pytest.approx(score, abs=1e-6)) for other, score in rows] for key, rows in few.items()} == (
        style.all_neighbors("csd", rule="local")
    )
    assert [key for key, _ in few[ALPHA]] == [CHARLIE, BRAVO, DELTA]

    # By their two closest works, BRAVO's two unrelated ones count for nothing. Each pair, both ways round:
    # ALPHA and BRAVO 1 (two exact matches each way), ALPHA and CHARLIE 0.8, BRAVO and CHARLIE 0.8 (BRAVO's
    # two copies find 0.8, and CHARLIE's two find 0.8), BRAVO and DELTA 0.7071 (BRAVO's works 4 and 5 find it,
    # it finds them). ALPHA and DELTA, and CHARLIE and DELTA, as with "mutual": 0.5303.
    monkeypatch.setattr(style, "FEW", 2)
    half = 2**-0.5
    usual = {  # the mean of each one's scores with the other three: fewer than NEAREST
        ALPHA: (1 + 0.8 + 3 * half / 4) / 3,
        BRAVO: (1 + 0.8 + half) / 3,
        CHARLIE: (0.8 + 0.8 + 3 * half / 4) / 3,
        DELTA: (3 * half / 4 + half + 3 * half / 4) / 3,
    }
    assert nearest(ALPHA, "few") == [
        (BRAVO, 2 * 1 - usual[ALPHA] - usual[BRAVO]),  # first now: "local" put CHARLIE first
        (CHARLIE, 2 * 0.8 - usual[ALPHA] - usual[CHARLIE]),
        (DELTA, 2 * 3 * half / 4 - usual[ALPHA] - usual[DELTA]),
    ]
    assert nearest(DELTA, "few")[0] == (BRAVO, 2 * half - usual[BRAVO] - usual[DELTA])
    assert style.main(["--neighbors", ALPHA, "--model", "csd", "--rule", "few", "--top", "1"]) == 0


def collect_an_artist_near_everyone(model):
    """Five invented artists, one of them (CHARLIE) fairly near all the others. Their mutual scores:

    ALPHA and BRAVO: ALPHA's work finds 0.8, BRAVO's two find 0.8 and 0: (0.8 + 0.4) / 2. BRAVO and CHARLIE:
    (0.5333 + 0.2667) / 2. Every other pair is one work against one work: their cosine.
                 ALPHA   BRAVO   CHARLIE  DELTA   ECHO
      ALPHA        -      0.6    0.6667    0       0
      BRAVO       0.6      -     0.4       0       0
      CHARLIE    0.6667   0.4      -      0.3333  0.6667
      DELTA       0       0      0.3333    -       0
      ECHO        0       0      0.6667    0       -
    """
    model.table = {
        1: [1, 0, 0, 0, 0],  # ALPHA, the seed
        2: [4, 0, 0, 3, 0],  # BRAVO, the specific match: cosine 0.8 with ALPHA's work, 0.5333 with CHARLIE's,
        3: [0, 0, 0, 0, 1],  # and a second work like nothing else here
        4: [2, 1, 2, 0, 0],  # CHARLIE, near everyone: 0.6667 with ALPHA, 0.3333 with DELTA, 0.6667 with ECHO
        5: [0, 1, 0, 0, 0],  # DELTA
        6: [0, 0, 1, 0, 0],  # ECHO
    }
    for artist, shades in ((ALPHA, (1,)), (BRAVO, (2, 3)), (CHARLIE, (4,)), (DELTA, (5,)), (ECHO, (6,))):
        collect(artist, *shades)


def test_the_local_rule_puts_a_specific_match_above_an_artist_who_is_near_everyone(model, monkeypatch, capsys):
    collect_an_artist_near_everyone(model)
    style.run("csd", 500)
    capsys.readouterr()

    def nearest(seed, rule):
        return [(key, pytest.approx(score, abs=1e-3)) for key, score in style.neighbors(seed, "csd", rule=rule)]

    assert nearest(ALPHA, "mutual") == [(CHARLIE, 2 / 3), (BRAVO, 0.6), (DELTA, 0.0), (ECHO, 0.0)]
    # "local" starts from those and not from "best", which has 0.8 for ALPHA and BRAVO: every figure below
    # would then be another.
    assert nearest(ALPHA, "best")[0] == (BRAVO, 0.8)

    # Fewer other artists than NEAREST (10), so usual closeness is the mean over all four others:
    # ALPHA (0.6667 + 0.6) / 4 = 0.3167, BRAVO (0.6 + 0.4) / 4 = 0.25, CHARLIE (0.6667 + 0.4 + 0.3333 + 0.6667) / 4
    # = 0.5167, DELTA 0.3333 / 4 = 0.0833, ECHO 0.6667 / 4 = 0.1667.
    # From ALPHA: BRAVO 2 x 0.6 - 0.3167 - 0.25 = 0.6333, CHARLIE 2 x 0.6667 - 0.3167 - 0.5167 = 0.5,
    # DELTA 0 - 0.3167 - 0.0833 = -0.4, ECHO 0 - 0.3167 - 0.1667 = -0.4833.
    assert nearest(ALPHA, "local") == [(BRAVO, 19 / 30), (CHARLIE, 0.5), (DELTA, -0.4), (ECHO, -29 / 60)]

    # Over the two nearest only: ALPHA (0.6667 + 0.6) / 2 = 0.6333, BRAVO (0.6 + 0.4) / 2 = 0.5,
    # CHARLIE (0.6667 + 0.6667) / 2 = 0.6667, DELTA (0.3333 + 0) / 2 = 0.1667, ECHO (0.6667 + 0) / 2 = 0.3333.
    monkeypatch.setattr(style, "NEAREST", 2)
    # From ALPHA: BRAVO 1.2 - 0.6333 - 0.5 = 0.0667, CHARLIE 1.3333 - 0.6333 - 0.6667 = 0.0333,
    # DELTA 0 - 0.6333 - 0.1667 = -0.8, ECHO 0 - 0.6333 - 0.3333 = -0.9667.
    assert nearest(ALPHA, "local") == [(BRAVO, 1 / 15), (CHARLIE, 1 / 30), (DELTA, -0.8), (ECHO, -29 / 30)]
    # From CHARLIE: ECHO 1.3333 - 0.6667 - 0.3333 = 0.3333, ALPHA 0.0333 (the same from either end),
    # DELTA 0.6667 - 0.6667 - 0.1667 = -0.1667, BRAVO 0.8 - 0.6667 - 0.5 = -0.3667.
    assert nearest(CHARLIE, "local") == [(ECHO, 1 / 3), (ALPHA, 1 / 30), (DELTA, -1 / 6), (BRAVO, -11 / 30)]
    assert style.neighbors("da:nobody", "csd", rule="local") == []
    assert style.neighbors(ALPHA, "csd") == style.neighbors(ALPHA, "csd", rule="local")  # and it is the default

    def printed(*extra):
        assert style.main(["--neighbors", ALPHA, "--model", "csd", "--top", "2", *extra]) == 0
        return [json.loads(line)["artist"] for line in capsys.readouterr().out.splitlines()]

    assert printed() == printed("--rule", "local") == [BRAVO, CHARLIE]
    assert printed("--rule", "mutual") == [CHARLIE, BRAVO]


def test_all_neighbors_is_every_artists_list_in_one_pass_and_among_narrows_the_pool(model, monkeypatch):
    collect_an_artist_near_everyone(model)
    collect(FOXTROT, bare=[7])  # collected, but no image
    collect(GOLF, broken=[8])  # collected, but no vector could be made
    style.run("csd", 500)
    artists = [ALPHA, BRAVO, CHARLIE, DELTA, ECHO]

    def near(found):
        return {key: [(other, pytest.approx(score, abs=1e-3)) for other, score in rows] for key, rows in found.items()}

    # Every artist's list is what neighbors() gives for them, and an artist without vectors has no entry.
    for rule in style.RULES:
        for top in (1, 3, 24):
            assert style.all_neighbors("csd", top, rule) == {
                key: style.neighbors(key, "csd", top, rule) for key in artists
            }
    # The defaults, in the order the push stage passes them. Five artists cannot show a top of 24.
    given = {name: parameter.default for name, parameter in inspect.signature(style.all_neighbors).parameters.items()}
    assert given == {"model": inspect.Parameter.empty, "top": 24, "rule": "local", "among": None, "db": None}
    assert style.all_neighbors("csd") == style.all_neighbors("csd", 24, "local")
    # The figures worked out in the test above, for the first of them.
    assert near(style.all_neighbors("csd"))[ALPHA] == [
        (BRAVO, 19 / 30),
        (CHARLIE, 0.5),
        (DELTA, -0.4),
        (ECHO, -29 / 60),
    ]

    # Over the two nearest, the whole pool, as in the test above: CHARLIE's usual closeness is 0.6667, the
    # mean of its scores with ALPHA and ECHO, and it comes second for ALPHA.
    monkeypatch.setattr(style, "NEAREST", 2)
    assert near(style.all_neighbors("csd"))[ALPHA] == [
        (BRAVO, 1 / 15),
        (CHARLIE, 1 / 30),
        (DELTA, -0.8),
        (ECHO, -29 / 30),
    ]
    # A pool without ECHO. CHARLIE's two nearest are now ALPHA and BRAVO: (0.6667 + 0.4) / 2 = 0.5333. The
    # others' are as they were: ALPHA 0.6333, BRAVO 0.5, DELTA (0.3333 + 0) / 2 = 0.1667.
    #   ALPHA and BRAVO    2 x 0.6    - 0.6333 - 0.5    =  0.0667
    #   ALPHA and CHARLIE  2 x 0.6667 - 0.6333 - 0.5333 =  0.1667   (first for ALPHA now)
    #   ALPHA and DELTA    0          - 0.6333 - 0.1667 = -0.8
    #   BRAVO and CHARLIE  2 x 0.4    - 0.5    - 0.5333 = -0.2333
    #   BRAVO and DELTA    0          - 0.5    - 0.1667 = -0.6667
    #   CHARLIE and DELTA  2 x 0.3333 - 0.5333 - 0.1667 = -0.0333
    pool = {ALPHA, BRAVO, CHARLIE, DELTA, FOXTROT, "da:zz-fake-nobody"}  # two of them have no vectors
    assert near(style.all_neighbors("csd", among=pool)) == {
        ALPHA: [(CHARLIE, 1 / 6), (BRAVO, 1 / 15), (DELTA, -0.8)],
        BRAVO: [(ALPHA, 1 / 15), (CHARLIE, -7 / 30), (DELTA, -2 / 3)],
        CHARLIE: [(ALPHA, 1 / 6), (DELTA, -1 / 30), (BRAVO, -7 / 30)],
        DELTA: [(CHARLIE, -1 / 30), (BRAVO, -2 / 3), (ALPHA, -0.8)],
    }
    # The other rules have no correction: the pool only decides who is listed.
    assert near(style.all_neighbors("csd", rule="mutual", among=pool))[CHARLIE] == [
        (ALPHA, 2 / 3),
        (BRAVO, 0.4),
        (DELTA, 1 / 3),
    ]
    assert style.all_neighbors("csd", among={ALPHA, FOXTROT}) == {ALPHA: []}
    assert style.all_neighbors("csd", among=set()) == style.all_neighbors("siglip2") == {}
    with pytest.raises(ValueError):
        style.all_neighbors("csd", rule="closest")


# 5. Stdout carries counts and throughput only.


def test_stdout_is_counts_and_throughput_only(model, capsys):
    collect(ALPHA, 1, 2)
    collect(BRAVO, broken=[3])
    assert style.main(["--model", "csd", "--max-works", "500"]) == 0
    lines = capsys.readouterr().out.splitlines()
    assert lines[:2] == ["embedded: 2", "failed: 1"]
    assert all(re.fullmatch(r"(embedded|failed|images_per_second|peak_gpu_mb): \d+(\.\d+)?", line) for line in lines)
    assert style.main(["--model", "csd"]) == 0
    assert capsys.readouterr().out.splitlines() == ["embedded: 0", "failed: 0"]


# An image that will not open is recorded as failed and skipped, and the run carries on.


def test_a_bad_image_is_recorded_as_failed_and_the_rest_each_get_their_own_vector(model):
    model.table = {5: [0, 0, 0]}  # a vector with no direction: nothing to make unit length
    collect(ALPHA, 1, 3, 5, 6, broken=[2], bare=[4])  # 4 has no cached image: not this stage's business
    (library.image_dir() / f"{work_id(6)}.png").unlink()  # named on its row, gone from the disk
    counts = style.run("csd", 500)
    assert (counts["embedded"], counts["failed"]) == (2, 3)
    assert [(work, blob is None) for work, _, blob in stored()] == [
        (work_id(1), False),
        (work_id(2), True),
        (work_id(3), False),
        (work_id(5), True),
        (work_id(6), True),
    ]
    for n in (1, 3):  # the gap in the batch did not shift anyone's vector onto a neighbor
        assert np.allclose(style.vector(work_id(n), "csd"), unit(model.vector(n)), atol=1e-3)
    assert all(style.vector(work_id(n), "csd") is None for n in (2, 4, 5, 6))
    assert list(style.artist_vectors(ALPHA, "csd")) == [work_id(1), work_id(3)]
    assert style.artist_vectors(BRAVO, "csd") == {}

    assert style.run("csd", 500) == {"embedded": 0, "failed": 0}  # a failed work is not tried again
    assert len(model.batches) == 1


# Hard rule 7: what was computed from a removed artist's work is not kept.


def test_a_removed_artists_vectors_are_never_read_again_and_are_deleted_by_the_next_run(model):
    collect(ALPHA, 1)
    collect(BRAVO, 2)
    style.run("csd", 500)
    assert style.neighbors(BRAVO, "csd")[0][0] == ALPHA

    library.purge(ALPHA)
    assert style.vector(work_id(1), "csd") is None
    assert style.artist_vectors(ALPHA, "csd") == {}
    assert style.neighbors(BRAVO, "csd") == []
    style.run("csd", 500)
    assert [work for work, _, _ in stored()] == [work_id(2)]

    collect(CHARLIE, 3)
    model.meanwhile = lambda: library.purge(CHARLIE)  # removed while their work is in the model
    style.run("csd", 500)
    assert model.batches[-1][0] == 3 and [work for work, _, _ in stored()] == [work_id(2)]


# A vector is tied to the image it was made from. A sequence: embed, the artist replaces the image, run again.

ANOTHER = "https://images-wixmp-zzfake.wixmp.com/f/FAKE-ANOTHER-FILE.jpg"  # the file an artist puts in its place


def collect_again(artist, n, thumb, image=None):
    """The collect stage's next turn for one work: its thumbnail shows this file now, and with `image` the
    collector has fetched the new file and says so."""
    if image is not None:
        library.save_image(work_id(n), "image/png", image)
    work = {
        "work_id": work_id(n),
        "page_url": f"https://www.deviantart.com/zz-fake/art/FAKE-TITLE-{n}",
        "title": "FAKE-TITLE",
        "tags": [],
        "thumb_url": thumb,
        "published": "1",
        "fetched": image is not None,
    }
    library.store_artist(artist, user_id=None, tagline=None, specialty=None, level=None, website=None, works=[work])


def test_a_vector_of_an_image_the_artist_replaced_is_not_used_and_is_made_again_from_the_new_one(model):
    collect(ALPHA, 1, 2)
    collect(BRAVO, 3)
    style.run("csd", 500)
    assert list(style.artist_vectors(ALPHA, "csd")) == [work_id(1), work_id(2)]

    collect_again(ALPHA, 1, ANOTHER)  # the thumbnail shows another file; the image in the cache is the old one
    assert style.vector(work_id(1), "csd") is None and list(style.artist_vectors(ALPHA, "csd")) == [work_id(2)]
    shown = {ALPHA: [work_id(1), work_id(2)], BRAVO: [work_id(3)]}
    assert style.all_closest_works([(BRAVO, ALPHA)], shown, "csd") == {(BRAVO, ALPHA): [work_id(2)]}
    assert style.run("csd", 500) == {"embedded": 0, "failed": 0} and model.loads == 1  # and nobody embeds the old one

    collect_again(ALPHA, 1, ANOTHER, square(9))  # the collector has fetched the new file
    assert style.vector(work_id(1), "csd") is None  # the vector that is stored is still the old image's
    assert style.run("csd", 500)["embedded"] == 1 and set(model.batches[-1]) == {9}  # the one work, and no other
    assert np.allclose(style.vector(work_id(1), "csd"), unit(model.vector(9)), atol=1e-3)
    assert len(stored()) == 3  # the new vector took the old one's place
    assert style.run("csd", 500) == {"embedded": 0, "failed": 0} and model.loads == 2


def test_an_image_replaced_while_it_is_in_the_model_does_not_pass_for_embedded(model):
    collect(ALPHA, 1)
    model.meanwhile = lambda: collect_again(ALPHA, 1, ANOTHER, square(9))
    assert style.run("csd", 500)["embedded"] == 1 and model.batches[0][0] == 1
    assert style.vector(work_id(1), "csd") is None  # it was made from the bytes that were read, not from these
    model.meanwhile = None
    assert style.run("csd", 500)["embedded"] == 1 and model.batches[-1][0] == 9
    assert np.allclose(style.vector(work_id(1), "csd"), unit(model.vector(9)), atol=1e-3)


def test_a_vector_carries_the_fingerprint_of_the_bytes_it_was_made_from_not_of_the_image_expected(model, monkeypatch):
    """The collector saves a new file before it stores the row that says so. A run in between reads the new
    bytes while the store still knows the old image."""
    collect(ALPHA, 1)

    def loading(name):  # after the run has listed its works, before it opens their files
        library.save_image(work_id(1), "image/png", square(9))
        return model(name)

    monkeypatch.setattr(style, "load_model", loading)
    assert style.run("csd", 500)["embedded"] == 1 and model.batches[0][0] == 9
    assert style.vector(work_id(1), "csd") is None  # a vector of the new picture is not the old picture's
    collect_again(ALPHA, 1, ANOTHER, square(9))  # the collector stores the row: the work shows the new picture
    assert np.allclose(style.vector(work_id(1), "csd"), unit(model.vector(9)), atol=1e-3)
    assert style.run("csd", 500) == {"embedded": 0, "failed": 0}  # and it was made from exactly those bytes


def test_a_store_made_before_readings_carried_fingerprints_stops_the_stage_before_a_model_is_loaded(model):
    collect(ALPHA, 1)
    style.run("csd", 500)
    with closing(sqlite3.connect(config.LOCAL_DB)) as conn:
        conn.execute("ALTER TABLE style_vectors DROP COLUMN fingerprint")
        conn.execute("DROP TABLE work_images")
        conn.execute("PRAGMA user_version = 0")  # as every store was before the migration
    before = config.LOCAL_DB.read_bytes()
    calls = [lambda: style.run("csd", 500), lambda: style.vector(work_id(1), "csd")]
    calls += [lambda: style.neighbors(ALPHA, "csd"), lambda: style.main(["--model", "csd"])]
    for call in calls:
        with pytest.raises(SystemExit) as stopped:
            call()
        assert "uv run python -m pipeline.tagging --migrate --backup" in str(stopped.value)
    assert model.loads == 1 and config.LOCAL_DB.read_bytes() == before
