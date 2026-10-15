"""A blind sample for the content-rating critic.

    uv run python eval/rating_sample.py --name ratings_r1

Draws works by the rating the tagging stage gave them (safe, suggestive, explicit) and writes their cached image
paths, shuffled and without the rating, to cache/review/<name>/items.jsonl (git-ignored, local only). The critic
rates each image itself; the answer key goes to cache/review/keys/, which the critic is not given.
"""

import argparse
import json
import pathlib
import random
import sqlite3
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))
from pipeline import config, library, profiles, tagging  # noqa: E402

DRAW = {"safe": 45, "suggestive": 20, "explicit": 10}  # safe is drawn most: a wrong "safe" is what goes public


def main() -> int:
    parser = argparse.ArgumentParser(description="A blind sample for the content-rating critic.")
    parser.add_argument("--seed", type=int, default=20261014)
    parser.add_argument("--name", required=True, help="folder name under cache/review/")
    args = parser.parse_args()

    out, keys = config.CACHE_DIR / "review" / args.name, config.CACHE_DIR / "review" / "keys"
    out.mkdir(parents=True, exist_ok=True)
    keys.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(f"file:{config.LOCAL_DB.as_posix()}?mode=ro", uri=True, timeout=60)
    rated: dict[str, list[tuple[str, str]]] = {rating: [] for rating in DRAW}
    query = (
        "SELECT w.deviation_id, w.image_file, f.result FROM work_format f JOIN works w USING (deviation_id) "
        "WHERE f.model = ? AND f.prompt_version = ? AND w.image_file IS NOT NULL ORDER BY w.deviation_id"
    )
    for work_id, image, result in conn.execute(query, (profiles.TAGGER, tagging.FORMAT_VERSION)):
        rating = json.loads(result).get("rating")
        if rating in rated:
            rated[rating].append((work_id, image))
    rng = random.Random(args.seed)
    drawn = [(rating, *work) for rating, works in rated.items() for work in rng.sample(works, DRAW[rating])]
    rng.shuffle(drawn)
    items = [{"n": n, "image": str(library.image_dir() / image)} for n, (_, _, image) in enumerate(drawn, 1)]
    answer = [{"n": n, "deviation_id": work_id, "rating": r} for n, (r, work_id, _) in enumerate(drawn, 1)]
    (out / "items.jsonl").write_text("".join(json.dumps(item) + "\n" for item in items), encoding="utf-8")
    (keys / f"{args.name}.json").write_text(json.dumps({"items": answer}, indent=1), encoding="utf-8")
    print(f"sample: {len(items)} works in {out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
