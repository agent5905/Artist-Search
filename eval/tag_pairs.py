"""Blind A/B pairs for the tagging critic.

    uv run python eval/tag_pairs.py --a qwen3.8:latest --b gemma4:26b --works 30 --name round1

Writes cache/review/<name>/items.jsonl (git-ignored, local only): one line per work with the cached image path
and the two models' results as "A" and "B", shuffled per work with the model names removed. The answer key goes
to cache/review/keys/, which the critic is not given.
"""

import argparse
import json
import pathlib
import random
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))
from pipeline import config, tagging  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description="Blind A/B pairs for the tagging critic.")
    parser.add_argument("--a", required=True, help="first model")
    parser.add_argument("--b", required=True, help="second model")
    parser.add_argument("--works", type=int, default=30)
    parser.add_argument("--seed", type=int, default=7)
    parser.add_argument("--name", required=True, help="folder name under cache/review/")
    args = parser.parse_args()

    out = config.CACHE_DIR / "review" / args.name
    keys = config.CACHE_DIR / "review" / "keys"
    out.mkdir(parents=True, exist_ok=True)
    keys.mkdir(parents=True, exist_ok=True)
    first = tagging.sample(args.works, args.seed, model=args.a)
    second = {row["deviation_id"]: row for row in tagging.sample(args.works, args.seed, model=args.b)}
    rng = random.Random(args.seed)
    items, answer = [], []
    for n, row in enumerate(first, start=1):
        pair = [(args.a, row["result"]), (args.b, second[row["deviation_id"]]["result"])]
        rng.shuffle(pair)
        items.append({"n": n, "image": row["image"], "A": pair[0][1], "B": pair[1][1]})
        answer.append({"n": n, "deviation_id": row["deviation_id"], "A": pair[0][0], "B": pair[1][0]})
    (out / "items.jsonl").write_text("".join(json.dumps(item) + "\n" for item in items), encoding="utf-8")
    (keys / f"{args.name}.json").write_text(json.dumps({"items": answer}, indent=1), encoding="utf-8")
    print(f"pairs: {len(items)} works in {out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
