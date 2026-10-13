"""A style vector for every collected work, and "more like this artist" ranked by those vectors.

Two models are kept side by side for the blind A/B. Both give 768 numbers a work:
  csd      tomg-group-umd/CSD-ViT-L: OpenAI's CLIP ViT-L/14 image tower, tuned to tell styles apart.
  siglip2  google/siglip2-base-patch16-384: a general image model, the baseline.

Hard rules 2 and 9: an image is read from the local cache, run through a local model, and goes nowhere else.
The one use of the network is `--download`, which fetches a model's pinned weight files from huggingface.co.
Every other command loads weights from local files only, with the hub's offline switches set.

    uv run python -m pipeline.style --download --model csd        once per model
    uv run python -m pipeline.style --model csd --max-works 500
    uv run python -m pipeline.style --neighbors da:somebody --model csd --top 10 --rule mutual

Stdout is counts and throughput only, except `--neighbors`: one JSON object per line.
"""

import argparse
import contextlib
import json
import os
import pathlib
import sqlite3
import sys
import time
from collections.abc import Callable, Container, Iterator

import numpy as np
import torch
from PIL import Image

from pipeline import library

DIMENSIONS = 768  # sql/001_schema.sql: works.style_embedding is halfvec(768)
BATCH = 16
WRITE_EVERY = 256  # works a transaction. The file is shared, and a commit can take seconds when the disk is busy
RULES = ("best", "mutual", "local")  # how all_neighbors() scores a candidate
NEAREST = 10  # rule "local": how many of an artist's nearest artists their usual closeness is taken over

# model -> (repository on huggingface.co, pinned commit, the files needed). Nothing else is ever fetched.
WEIGHTS = {
    "csd": ("tomg-group-umd/CSD-ViT-L", "5bc26a6fb0487f3f00a2a7313135103a005b1b67", ["pytorch_model.bin"]),
    "siglip2": (
        "google/siglip2-base-patch16-384",
        "f775b65a79762255128c981547af89addcfe0f88",
        ["config.json", "preprocessor_config.json", "model.safetensors"],
    ),
}

_SCHEMA = """
CREATE TABLE IF NOT EXISTS style_vectors (
    deviation_id TEXT NOT NULL,      -- works.deviation_id
    model TEXT NOT NULL,             -- a key of WEIGHTS
    vector BLOB,                     -- 768 float16 numbers, unit length. NULL: no vector could be made (failed)
    PRIMARY KEY (deviation_id, model)
) STRICT;
"""
# Hard rule 7: when a work is removed (library.purge), what was computed from it goes too. The library does not
# know this table, so every run does it, first and after each write; until then no read returns such a vector.
_SWEEP = "DELETE FROM style_vectors WHERE deviation_id NOT IN (SELECT deviation_id FROM works)"

Embed = Callable[[list[Image.Image]], np.ndarray]  # RGB images in, one row of 768 numbers for each out


@contextlib.contextmanager
def _open(db: pathlib.Path | str | None) -> Iterator[sqlite3.Connection]:
    """One transaction on the library's file (default config.LOCAL_DB), with this stage's table."""
    with library._open(db) as conn:
        conn.executescript(_SCHEMA)
        yield conn


def download(model: str) -> None:
    """The only network use in this stage: fetch one model's pinned weight files from huggingface.co."""
    from huggingface_hub import snapshot_download

    repo, revision, files = WEIGHTS[model]
    snapshot_download(repo, revision=revision, allow_patterns=files)


def load_model(model: str) -> Embed:
    """The real model, from weight files already on this machine. Never opens a connection."""
    os.environ["HF_HUB_OFFLINE"] = os.environ["TRANSFORMERS_OFFLINE"] = "1"  # read when the hub libraries load
    from huggingface_hub import snapshot_download
    from huggingface_hub.errors import LocalEntryNotFoundError

    repo, revision, files = WEIGHTS[model]
    try:
        folder = pathlib.Path(snapshot_download(repo, revision=revision, allow_patterns=files, local_files_only=True))
    except LocalEntryNotFoundError:
        fetch = f"python -m pipeline.style --download --model {model}"
        raise SystemExit(f"No local weights. Fetch them once: {fetch}") from None
    device = "cuda" if torch.cuda.is_available() else "cpu"

    if model == "siglip2":
        from transformers import AutoImageProcessor, SiglipVisionModel

        processor = AutoImageProcessor.from_pretrained(folder, local_files_only=True)
        vision = SiglipVisionModel.from_pretrained(folder, local_files_only=True).eval().to(device)

        def siglip2(images: list[Image.Image]) -> np.ndarray:
            pixels = processor(images=images, return_tensors="pt")["pixel_values"].to(device)
            return vision(pixel_values=pixels).pooler_output.cpu().numpy()

        return siglip2

    import open_clip  # type: ignore[import-untyped]

    # CSD is OpenAI's CLIP ViT-L/14 image tower (so: quickgelu, CLIP's own preprocessing) with its projection
    # replaced by a style head and a content head. Only the tower and the style head are used. open_clip
    # builds the tower untrained, and warns so on stderr; the weights are loaded just below.
    clip, _, preprocess = open_clip.create_model_and_transforms("ViT-L-14-quickgelu", pretrained=None)
    tower = clip.visual
    tower.proj = None
    # The file is a training snapshot: beside the tensors it pickles the run's arguments and a few numpy
    # numbers. weights_only stays on. These four inert data types are the only extra names it may build.
    scalar = np._core.multiarray.scalar  # type: ignore[attr-defined]  # how numpy pickles one number
    with torch.serialization.safe_globals(
        [argparse.Namespace, np.dtype, type(np.dtype(np.float64)), (scalar, "numpy.core.multiarray.scalar")]
    ):
        snapshot = torch.load(folder / "pytorch_model.bin", map_location="cpu", weights_only=True, mmap=True)
    saved = {key.removeprefix("module."): value for key, value in snapshot["model_state_dict"].items()}
    # Strict: every tensor of the tower must be in the file, and the file must hold no other.
    tower.load_state_dict({k.removeprefix("backbone."): v for k, v in saved.items() if k.startswith("backbone.")})
    tower.eval().to(device)
    style_head = saved["last_layer_style"].to(device)

    def csd(images: list[Image.Image]) -> np.ndarray:
        pixels = torch.stack([preprocess(image) for image in images]).to(device)
        return (tower(pixels) @ style_head).cpu().numpy()

    return csd


def run(model: str, max_works: int, db: pathlib.Path | str | None = None) -> dict[str, float]:
    """Give up to max_works works a vector: those with a cached image and no row yet for this model.

    A work whose image will not open, or whose vector comes out broken, gets a row with no vector and is not
    tried again. Returns the counts and the throughput.
    """
    with _open(db) as conn:
        conn.execute(_SWEEP)
        todo = conn.execute(
            "SELECT deviation_id, image_file FROM works WHERE image_file IS NOT NULL AND deviation_id NOT IN "
            "(SELECT deviation_id FROM style_vectors WHERE model = ?) ORDER BY deviation_id LIMIT ?",
            (model, max_works),
        ).fetchall()
    counts: dict[str, float] = {"embedded": 0, "failed": 0}
    if not todo:
        return counts
    embed = load_model(model)
    on_gpu = torch.cuda.is_initialized()  # not with a CPU, and not with a test's stand-in model
    if on_gpu:
        torch.cuda.reset_peak_memory_stats()
    started = time.perf_counter()
    rows: dict[str, bytes | None] = {}
    for start in range(0, len(todo), BATCH):
        opened: dict[str, Image.Image] = {}
        for work_id, name in todo[start : start + BATCH]:
            try:
                # ponytail: decoded one at a time on this thread, and transparency is dropped, not laid on
                # white. Fine for JPEG previews; use a DataLoader if decoding ever limits the GPU.
                with Image.open(library.image_dir() / name) as image:
                    opened[work_id] = image.convert("RGB")
            except Exception:  # Pillow raises many kinds for a bad file; none of them may stop the run
                rows[work_id] = None
        if opened:
            # Always a full batch, the last image repeated to fill it. On a GPU the last digits of a vector
            # depend on how many images are computed together (measured: a short batch changed every vector
            # in it), and the same image must give the same vector whatever else is waiting.
            images = list(opened.values())
            images += images[-1:] * (BATCH - len(images))
            with torch.inference_mode():
                vectors = np.asarray(embed(images), dtype=np.float32)
            if vectors.shape != (BATCH, DIMENSIONS):
                raise ValueError(f"the model must give {DIMENSIONS} numbers an image")
            with np.errstate(all="ignore"):  # a zero or broken vector becomes NaN here and is failed below
                units = (vectors / np.linalg.norm(vectors, axis=1, keepdims=True)).astype(np.float16)
            for work_id, unit in zip(opened, units, strict=False):  # the filler's vectors are dropped
                rows[work_id] = unit.tobytes() if np.isfinite(unit).all() else None
        if len(rows) < WRITE_EVERY and start + BATCH < len(todo):
            continue
        with _open(db) as conn:
            conn.executemany(
                "INSERT OR IGNORE INTO style_vectors (deviation_id, model, vector) VALUES (?, ?, ?)",
                [(work_id, model, blob) for work_id, blob in rows.items()],
            )
            conn.execute(_SWEEP)  # a work removed while its vector was being computed
        counts["embedded"] += sum(blob is not None for blob in rows.values())
        counts["failed"] += sum(blob is None for blob in rows.values())
        rows = {}
    counts["images_per_second"] = round(len(todo) / (time.perf_counter() - started), 1)
    del embed  # the GPU is shared: give the memory back now, not when the process ends
    if on_gpu:
        counts["peak_gpu_mb"] = torch.cuda.max_memory_reserved() // 2**20
        torch.cuda.empty_cache()
    return counts


def _vectors(
    model: str, db: pathlib.Path | str | None, artist_key: str | None = None
) -> dict[str, dict[str, np.ndarray]]:
    """artist key -> deviation id -> vector, for every work that is still collected and has one."""
    query = (
        "SELECT w.artist_key, w.deviation_id, v.vector FROM works w JOIN style_vectors v USING (deviation_id) "
        "WHERE v.model = ? AND v.vector IS NOT NULL AND w.artist_key = COALESCE(?, w.artist_key) ORDER BY 1, 2"
    )
    found: dict[str, dict[str, np.ndarray]] = {}
    with _open(db) as conn:
        for key, work_id, blob in conn.execute(query, (model, artist_key)):
            found.setdefault(key, {})[work_id] = np.frombuffer(blob, dtype=np.float16)
    return found


def vector(work_id: str, model: str, db: pathlib.Path | str | None = None) -> np.ndarray | None:
    """The work's style vector, 768 float16 numbers of unit length. None when it has none."""
    query = (
        "SELECT v.vector FROM works w JOIN style_vectors v USING (deviation_id) "
        "WHERE w.deviation_id = ? AND v.model = ? AND v.vector IS NOT NULL"
    )
    with _open(db) as conn:
        row = conn.execute(query, (work_id, model)).fetchone()
    return np.frombuffer(row[0], dtype=np.float16) if row else None


def artist_vectors(artist_key: str, model: str, db: pathlib.Path | str | None = None) -> dict[str, np.ndarray]:
    """Every style vector of the artist's works, by deviation id. Empty when there are none."""
    return _vectors(model, db, artist_key).get(artist_key, {})


def all_neighbors(
    model: str,
    top: int = 24,
    rule: str = "local",
    among: Container[str] | None = None,
    db: pathlib.Path | str | None = None,
) -> dict[str, list[tuple[str, float]]]:
    """For every artist with vectors: the artists most like them, best first, as (artist key, score).

    `among`, when given, is the pool: the artist keys that count. An artist outside it has no entry, is in
    nobody's list, and is not one of anybody's nearest artists for rule "local".

    rule "best" is the mean, over the seed's works, of the best cosine match among the candidate's works. One
    work of a candidate can be the best match for all of them, so it favors an artist with a bit of everything.
    rule "mutual" is the average of that and the same the other way round: the mean, over the candidate's
    works, of the best match among the seed's. A candidate's works that match nothing of the seed's count
    against them.
    rule "local" corrects "mutual" for artists who are fairly near everyone, and so turn up for every seed:
    twice the pair's mutual score, less each one's usual closeness, which is the mean of their mutual scores
    with their NEAREST nearest artists (all the others, when there are fewer). It can be negative.

    Ties go to the lower key. Never the artist themselves, and never an artist without vectors, who has no
    entry either. ValueError for any other rule.
    """
    if rule not in RULES:
        raise ValueError(f"rule must be one of {RULES}")
    vectors = {key: works for key, works in _vectors(model, db).items() if among is None or key in among}
    keys = sorted(vectors)
    if len(keys) < 2:
        return {key: [] for key in keys}
    # ponytail: reads every vector and scores every pair of artists, whatever the rule. Measured: 0.25 s at
    # 442 artists; 7 s and 0.8 GB at 3,000 artists with 10 works each (invented vectors). It grows with the
    # square of the works. Right for every artist at once; neighbors() pays all of it for one artist. If
    # that gets slow, score the seed's pairs alone for "best" and "mutual", or keep each artist's usual
    # closeness in a table.
    counts = np.array([len(vectors[key]) for key in keys])
    starts = np.cumsum(counts) - counts  # where each artist's works begin in the stack
    works = np.stack([vector for key in keys for vector in vectors[key].values()]).astype(np.float32)
    works /= np.linalg.norm(works, axis=1, keepdims=True)
    # best[w, a]: the best cosine match for work w among artist a's works. Made 2,048 works at a time, so
    # the works-by-works table of cosines is never whole in memory.
    best = np.concatenate(
        [np.maximum.reduceat(works[at : at + 2048] @ works.T, starts, axis=1) for at in range(0, len(works), 2048)]
    )
    scores = np.add.reduceat(best, starts, axis=0) / counts[:, None]  # scores[a, b]: "best", seed a, candidate b
    if rule != "best":
        scores = (scores + scores.T) / 2
    np.fill_diagonal(scores, -np.inf)  # an artist is not their own neighbor, nor one of their own nearest
    if rule == "local":
        usual = np.sort(scores, axis=1)[:, -min(NEAREST, len(keys) - 1) :].mean(axis=1)
        scores = 2 * scores - usual[:, None] - usual
    found = {}
    for key, row in zip(keys, scores, strict=True):
        order = np.argsort(-row, kind="stable")[: min(top, len(keys) - 1)]  # stable: a tie goes to the lower key
        found[key] = [(keys[at], float(row[at])) for at in order]
    return found


def neighbors(
    artist_key: str, model: str, top: int = 10, rule: str = "local", db: pathlib.Path | str | None = None
) -> list[tuple[str, float]]:
    """One artist's entry in all_neighbors(), with every artist as the pool. Empty when they have no vectors."""
    return all_neighbors(model, top, rule, None, db).get(artist_key, [])


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Style vectors for the collected works, computed on this machine.")
    parser.add_argument("--model", choices=sorted(WEIGHTS), required=True)
    parser.add_argument("--max-works", type=int, default=500, help="embed at most this many works in this run")
    parser.add_argument("--neighbors", metavar="ARTIST_KEY", help="print the artists nearest to this one")
    parser.add_argument("--top", type=int, default=10, help="how many neighbors to print")
    parser.add_argument("--rule", choices=RULES, default="local", help="how --neighbors scores (see all_neighbors)")
    parser.add_argument("--download", action="store_true", help="fetch the model's weights from huggingface.co")
    args = parser.parse_args(argv)
    if args.download:
        download(args.model)
    elif args.neighbors:
        nearest = neighbors(args.neighbors, args.model, args.top, args.rule)
        for key, score in nearest:
            print(json.dumps({"artist": key, "score": round(score, 4)}))
        if not nearest:
            print("no neighbors: that artist has no vectors for this model, or is the only one", file=sys.stderr)
            return 1
    else:
        for name, count in run(args.model, args.max_works).items():
            print(f"{name}: {count}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
