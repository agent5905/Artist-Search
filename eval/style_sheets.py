"""Blind A/B contact sheets for the style critic.

    uv run python eval/style_sheets.py --a csd --b siglip2 --seeds 12 --name round1
    uv run python eval/style_sheets.py --a csd:best --b csd:mutual --seeds 12 --name round2
    uv run python eval/style_sheets.py --a csd:mutual --b csd:local --frozen --seeds 12 --name round3
    uv run python eval/style_sheets.py --a csd:local --b reference --frozen --seeds 30 --name style_ab_r1
    uv run python eval/style_sheets.py --a csd:local:closest --b reference --reference style_ab_r1 \
        --frozen --seeds 30 --name style_ab_r2

"reference" is DeviantArt's own More Like This, as captured by eval/style_reference.py under --reference (--name
when not given). A third part, "closest", shows each similar artist's works nearest the seed's, not their newest.
Against it both sheets show the same number of works in shuffled order, so one row per artist cannot give our
side away.

--frozen takes the first N seed artists from eval/seeds.jsonl (stored as hashes) instead of drawing new ones.

For each randomly drawn seed artist it writes three images under cache/review/<name>/ (git-ignored, local only):
seed_NN.jpg (the seed's works), seed_NN_A.jpg and seed_NN_B.jpg (the artists each model ranks nearest, one row
per artist). Which model is A and which is B is shuffled per seed; the answer key goes to cache/review/keys/,
which the critic is not given.
"""

import argparse
import hashlib
import itertools
import json
import pathlib
import random
import sqlite3
import sys

from PIL import Image

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))
from pipeline import config, library, style  # noqa: E402

CELL = 300  # pixels per thumbnail
PER_ARTIST = 4  # works shown per neighbor
NEIGHBORS = 4  # neighbors shown per model
MIN_WORKS = 6  # a seed needs enough work to show a style


def works(conn: sqlite3.Connection, artist: str, limit: int) -> list[pathlib.Path]:
    rows = conn.execute(
        "select image_file from works where artist_key = ? and image_file is not null "
        "order by published_at desc limit ?",
        (artist, limit),
    )
    return [library.image_dir() / name for (name,) in rows]


def closest(conn: sqlite3.Connection, artist: str, other: str, model: str) -> list[pathlib.Path]:
    """The other artist's works nearest the seed artist's in style, best first."""
    ids = style.closest_works(artist, other, model, PER_ARTIST)
    marks = ",".join("?" * len(ids))
    files = dict(conn.execute(f"select deviation_id, image_file from works where deviation_id in ({marks})", ids))
    return [library.image_dir() / files[work] for work in ids if files.get(work)]


def sheet(rows: list[list[pathlib.Path]], out: pathlib.Path) -> None:
    width = max(len(row) for row in rows)
    canvas = Image.new("RGB", (width * CELL, len(rows) * CELL), "white")
    for y, row in enumerate(rows):
        for x, path in enumerate(row):
            with Image.open(path) as image:
                thumb = image.convert("RGB")
                thumb.thumbnail((CELL - 8, CELL - 8))
                canvas.paste(thumb, (x * CELL + 4, y * CELL + 4))
    canvas.save(out, quality=85)


def eligible(conn: sqlite3.Connection) -> list[str]:
    rows = conn.execute(
        "select artist_key from works where image_file is not null group by artist_key having count(*) >= ? "
        "order by artist_key",
        (MIN_WORKS,),
    )
    return [key for (key,) in rows]


def frozen_seeds(artists: list[str]) -> list[str]:
    """The seed artists of eval/seeds.jsonl, in file order. A removed or uncollected seed is skipped."""
    by_hash = {hashlib.sha256(key.encode()).hexdigest(): key for key in artists}
    lines = (pathlib.Path(__file__).parent / "seeds.jsonl").read_text(encoding="utf-8").splitlines()
    return [key for key in (by_hash.get(json.loads(line)["artist_sha256"]) for line in lines) if key]


def reference(folder: pathlib.Path, artist: str) -> list[pathlib.Path]:
    """DeviantArt's suggestions for this seed, in its order; empty when the seed was not captured."""
    listing = folder / f"{hashlib.sha256(artist.encode()).hexdigest()[:16]}.json"
    if not listing.exists():
        return []
    return [folder / row["file"] for row in json.loads(listing.read_text(encoding="utf-8"))]


def mixed(rows: list[list[pathlib.Path]], count: int, rng: random.Random) -> list[list[pathlib.Path]]:
    """The first `count` works taken across the rows in turn, shuffled, PER_ARTIST to a row."""
    cells = [path for column in itertools.zip_longest(*rows) for path in column if path][:count]
    rng.shuffle(cells)
    return [cells[i : i + PER_ARTIST] for i in range(0, len(cells), PER_ARTIST)]


def main() -> int:
    parser = argparse.ArgumentParser(description="Blind A/B contact sheets for the style critic.")
    parser.add_argument("--a", required=True, help="first style model")
    parser.add_argument("--b", required=True, help="second style model")
    parser.add_argument("--seeds", type=int, default=12)
    parser.add_argument("--rng", type=int, default=20261012)
    parser.add_argument("--name", required=True, help="folder name under cache/review/")
    parser.add_argument("--frozen", action="store_true", help="use the frozen seed artists in eval/seeds.jsonl")
    parser.add_argument("--reference", help="folder under cache/review/ that holds the captured reference")
    args = parser.parse_args()

    out = config.CACHE_DIR / "review" / args.name
    keys = config.CACHE_DIR / "review" / "keys"
    out.mkdir(parents=True, exist_ok=True)
    keys.mkdir(parents=True, exist_ok=True)
    rng = random.Random(args.rng)
    conn = sqlite3.connect(f"file:{config.LOCAL_DB.as_posix()}?mode=ro", uri=True, timeout=60)
    artists = eligible(conn)
    answer = []
    seeds = frozen_seeds(artists)[: args.seeds] if args.frozen else rng.sample(artists, args.seeds)
    for n, artist in enumerate(seeds, start=1):
        models = [args.a, args.b]
        rng.shuffle(models)
        rows = {}
        for model in models:
            if model == "reference":
                captured = config.CACHE_DIR / "review" / (args.reference or args.name) / "reference"
                rows[model] = [reference(captured, artist)]
                continue
            name, _, rest = model.partition(":")  # "csd", "csd:mutual" or "csd:local:closest"
            rule, _, pick = rest.partition(":")
            nearest = [key for key, _ in style.neighbors(artist, name, top=NEIGHBORS, rule=rule or "best")]
            rows[model] = [
                closest(conn, artist, key, name) if pick == "closest" else works(conn, key, PER_ARTIST)
                for key in nearest
            ]
        if "reference" in rows:
            count = min(sum(len(row) for row in side) for side in rows.values())
            if not count:
                print(f"seed {n}: no reference captured, skipped")
                continue
            rows = {model: mixed(side, count, rng) for model, side in rows.items()}
        sheet([works(conn, artist, 6)], out / f"seed_{n:02d}.jpg")
        for label, model in zip("AB", models, strict=True):
            sheet(rows[model], out / f"seed_{n:02d}_{label}.jpg")
        answer.append({"n": n, "artist": artist, "A": models[0], "B": models[1]})
    (keys / f"{args.name}.json").write_text(json.dumps({"seeds": answer}, indent=1), encoding="utf-8")
    print(f"sheets: {len(answer)} seeds in {out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
