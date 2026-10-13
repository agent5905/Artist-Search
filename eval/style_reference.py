"""DeviantArt's own "More Like This" for the frozen seed artists: the reference side of the style A/B.

    uv run python eval/style_reference.py --seeds 30 --name style_ab_r1

Reads the public More Like This page of each seed artist's two newest works (the website, not the API: the API
License bars benchmarking through the API, and robots.txt allows these pages), keeps the works by other artists
in DeviantArt's order, and saves their thumbnails under cache/review/<name>/reference/ (git-ignored, local only).
One request every few seconds. A seed already captured is skipped, so a capture stays frozen once made.
"""

import argparse
import hashlib
import itertools
import json
import pathlib
import sqlite3
import sys
import time

import httpx

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))
import style_sheets  # noqa: E402  (same folder)

from pipeline import config  # noqa: E402

WORKS = 16  # reference works kept per seed, the same number our side shows
SEED_WORKS = 2  # ponytail: the two newest works stand for the artist; one odd piece would skew DeviantArt's answer
THUMB = "375W"  # fits a 375 px box; the sheets shrink it to 300
STATE = "window.__INITIAL_STATE__ = JSON.parse("


def results(page: str, seed_handle: str) -> list[dict[str, str]]:
    """Works by other artists on a More Like This page, in the page's order."""
    start = page.index(STATE) + len(STATE)
    literal, _ = json.JSONDecoder().raw_decode(page[start:].replace("\\'", "'"))  # \' is JavaScript, not JSON
    state = json.loads(literal)
    entities = state["@@entities"]
    found = []
    for work_id in state["@@streams"]["@@BROWSE_PAGE_STREAM"]["items"]:
        work = entities["deviation"][str(work_id)]
        artist = entities["user"][str(work["author"])]["username"]
        media = work.get("media") or {}
        kind = next((kind for kind in media.get("types", []) if kind["t"] == THUMB and "c" in kind), None)
        if work["type"] != "image" or work["isMature"] or artist.lower() == seed_handle.lower() or not kind:
            continue
        tokens = media.get("token") or []
        thumb = media["baseUri"] + kind["c"].replace("<prettyName>", media["prettyName"])
        found.append(
            {
                "artist": artist,
                "page_url": work["url"],
                "thumb": thumb + (f"?token={tokens[kind['r']]}" if tokens else ""),
            }
        )
    return found


def main() -> int:
    parser = argparse.ArgumentParser(description="Capture DeviantArt's More Like This for the frozen seeds.")
    parser.add_argument("--seeds", type=int, default=30)
    parser.add_argument("--name", required=True, help="folder name under cache/review/")
    args = parser.parse_args()

    folder = config.CACHE_DIR / "review" / args.name / "reference"
    folder.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(f"file:{config.LOCAL_DB.as_posix()}?mode=ro", uri=True, timeout=60)
    seeds = style_sheets.frozen_seeds(style_sheets.eligible(conn))[: args.seeds]
    with httpx.Client(headers={"User-Agent": config.USER_AGENT}, follow_redirects=True, timeout=30) as http:

        def get(url: str) -> httpx.Response:
            time.sleep(config.MIN_SECONDS_PER_HOST)  # ponytail: one request at a time, so every host gets its gap
            return http.get(url)

        for artist in seeds:
            name = hashlib.sha256(artist.encode()).hexdigest()[:16]
            if (folder / f"{name}.json").exists():
                continue
            newest = conn.execute(
                "select page_url from works where artist_key = ? and image_file is not null "
                "order by published_at desc limit ?",
                (artist, SEED_WORKS),
            )
            pages = []
            for (page_url,) in newest:
                handle, _, slug = page_url.removeprefix("https://www.deviantart.com/").partition("/art/")
                answer = get(f"https://www.deviantart.com/morelikethis/{handle}/{slug.rsplit('-', 1)[-1]}")
                answer.raise_for_status()  # a refused page stops the run; a rerun resumes at this seed
                pages.append(results(answer.text, handle))
            rows: list[dict[str, str]] = []
            for work in itertools.chain.from_iterable(itertools.zip_longest(*pages)):  # the pages take turns
                if not work or len(rows) == WORKS or any(row["page_url"] == work["page_url"] for row in rows):
                    continue
                image = get(work["thumb"])
                if image.status_code != 200 or not image.headers.get("content-type", "").startswith("image/"):
                    continue  # a thumbnail that will not load is passed over for the next suggestion
                file = f"{name}_{len(rows) + 1:02d}.jpg"
                (folder / file).write_bytes(image.content)
                rows.append({"artist": work["artist"], "page_url": work["page_url"], "file": file})
            (folder / f"{name}.json").write_text(json.dumps(rows, indent=1), encoding="utf-8")
            print(f"seed {name}: {len(rows)} works", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
