"""Blind A/B pairs for the profile critic.

    uv run python eval/profile_pairs.py --artists 20 --seed 3 --name round1

For each sampled artist it writes a contact sheet of their works and one line in items.jsonl under
cache/review/<name>/ (git-ignored, local only) with the two profile forms as "A" and "B", shuffled per artist.
The answer key goes to cache/review/keys/, which the critic is not given.
"""

import argparse
import json
import pathlib
import random
import subprocess
import sys

from PIL import Image

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))
from pipeline import config  # noqa: E402

CELL = 300  # pixels per thumbnail
PER_ROW = 5


def sheet(images: list[str], out: pathlib.Path) -> None:
    rows = -(-len(images) // PER_ROW)
    canvas = Image.new("RGB", (PER_ROW * CELL, rows * CELL), "white")
    for index, path in enumerate(images):
        with Image.open(path) as image:
            thumb = image.convert("RGB")
            thumb.thumbnail((CELL - 8, CELL - 8))
            canvas.paste(thumb, (index % PER_ROW * CELL + 4, index // PER_ROW * CELL + 4))
    canvas.save(out, quality=85)


def main() -> int:
    parser = argparse.ArgumentParser(description="Blind A/B pairs for the profile critic.")
    parser.add_argument("--artists", type=int, default=20)
    parser.add_argument("--seed", type=int, default=3)
    parser.add_argument("--name", required=True, help="folder name under cache/review/")
    args = parser.parse_args()

    out = config.CACHE_DIR / "review" / args.name
    keys = config.CACHE_DIR / "review" / "keys"
    out.mkdir(parents=True, exist_ok=True)
    keys.mkdir(parents=True, exist_ok=True)
    command = [sys.executable, "-m", "pipeline.profiles", "--sample", str(args.artists), "--seed", str(args.seed)]
    printed = subprocess.run(command, cwd=config.ROOT, capture_output=True, text=True, check=True).stdout
    rng = random.Random(args.seed)
    items, answer = [], []
    for n, row in enumerate((json.loads(line) for line in printed.splitlines() if line.strip()), start=1):
        sheet(row["images"], out / f"artist_{n:02d}.jpg")
        pair = [("template", row["template"]), ("written", row["written"])]
        rng.shuffle(pair)
        items.append(
            {
                "n": n,
                "sheet": str(out / f"artist_{n:02d}.jpg"),
                "A": pair[0][1],
                "B": pair[1][1],
                "open_for_work": row["open_for_work"],
                "open_evidence": row["open_evidence"],
            }
        )
        answer.append({"n": n, "artist": row["artist"], "A": pair[0][0], "B": pair[1][0]})
    (out / "items.jsonl").write_text("".join(json.dumps(item) + "\n" for item in items), encoding="utf-8")
    (keys / f"{args.name}.json").write_text(json.dumps({"items": answer}, indent=1), encoding="utf-8")
    print(f"pairs: {len(items)} artists in {out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
