"""Blind A/B contact sheets for the style critic.

    uv run python eval/style_sheets.py --a csd --b siglip2 --seeds 12 --name round1

For each randomly drawn seed artist it writes three images under cache/review/<name>/ (git-ignored, local only):
seed_NN.jpg (the seed's works), seed_NN_A.jpg and seed_NN_B.jpg (the artists each model ranks nearest, one row
per artist). Which model is A and which is B is shuffled per seed; the answer key goes to cache/review/keys/,
which the critic is not given.
"""

import argparse
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


def main() -> int:
    parser = argparse.ArgumentParser(description="Blind A/B contact sheets for the style critic.")
    parser.add_argument("--a", required=True, help="first style model")
    parser.add_argument("--b", required=True, help="second style model")
    parser.add_argument("--seeds", type=int, default=12)
    parser.add_argument("--rng", type=int, default=20261012)
    parser.add_argument("--name", required=True, help="folder name under cache/review/")
    args = parser.parse_args()

    out = config.CACHE_DIR / "review" / args.name
    keys = config.CACHE_DIR / "review" / "keys"
    out.mkdir(parents=True, exist_ok=True)
    keys.mkdir(parents=True, exist_ok=True)
    rng = random.Random(args.rng)
    conn = sqlite3.connect(f"file:{config.LOCAL_DB.as_posix()}?mode=ro", uri=True, timeout=60)
    eligible = [
        key
        for (key,) in conn.execute(
            "select artist_key from works where image_file is not null group by artist_key having count(*) >= ? "
            "order by artist_key",
            (MIN_WORKS,),
        )
    ]
    answer = []
    for n, artist in enumerate(rng.sample(eligible, args.seeds), start=1):
        sheet([works(conn, artist, 6)], out / f"seed_{n:02d}.jpg")
        models = [args.a, args.b]
        rng.shuffle(models)
        for label, model in zip("AB", models, strict=True):
            nearest = [key for key, _ in style.neighbors(artist, model, top=NEIGHBORS)]
            sheet([works(conn, key, PER_ARTIST) for key in nearest], out / f"seed_{n:02d}_{label}.jpg")
        answer.append({"n": n, "artist": artist, "A": models[0], "B": models[1]})
    (keys / f"{args.name}.json").write_text(json.dumps({"seeds": answer}, indent=1), encoding="utf-8")
    print(f"sheets: {len(answer)} seeds in {out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
