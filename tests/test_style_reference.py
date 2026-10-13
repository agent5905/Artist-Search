"""The reference capture reads DeviantArt's More Like This page data: other artists' works, in page order."""

import json
import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent / "eval"))
import style_reference  # noqa: E402


def _work(work_id: int, author: int, **changes: object) -> dict:
    work = {
        "type": "image",
        "isMature": False,
        "author": author,
        "url": f"https://www.deviantart.com/someone/art/Piece-{work_id}",
        "media": {
            "baseUri": f"https://images.example/f/{work_id}.jpg",
            "prettyName": f"piece_{work_id}",
            "token": ["first", "second"],
            "types": [
                {"t": "150", "r": 0, "c": "/v1/fit/w_150/<prettyName>-150.jpg"},
                {"t": "375W", "r": 1, "c": "/v1/fit/w_375,h_375/<prettyName>-375w.jpg"},
                {"t": "fullview", "r": 1},
            ],
        },
    }
    return work | changes


def _page(works: dict[int, dict], order: list[int]) -> str:
    state = {
        "@@entities": {
            "deviation": {str(key): work for key, work in works.items()},
            "user": {"1": {"username": "SeedArtist"}, "2": {"username": "Other"}, "3": {"username": "Third"}},
        },
        "@@streams": {"@@BROWSE_PAGE_STREAM": {"items": order}},
        "@@HEAD": {"script": "mark('x');"},  # the real page carries script text with JavaScript's \' escape
    }
    literal = json.dumps(json.dumps(state)).replace("'", "\\'")
    return f'<html><script>window.__INITIAL_STATE__ = JSON.parse({literal});\nmore("); text</script></html>'


def test_other_artists_works_in_page_order() -> None:
    works = {
        10: _work(10, 1),  # the seed artist's own work
        11: _work(11, 2),
        12: _work(12, 3, isMature=True),
        13: _work(13, 3, type="literature"),
        14: _work(14, 3, media={"baseUri": "https://images.example/f/14.gif", "types": [{"t": "fullview", "r": 0}]}),
        15: _work(15, 3, media=_work(15, 3)["media"] | {"token": []}),
    }
    rows = style_reference.results(_page(works, [15, 10, 12, 11, 13, 14]), "seedartist")
    assert [(row["artist"], row["page_url"].rsplit("-", 1)[-1]) for row in rows] == [("Third", "15"), ("Other", "11")]
    assert rows[0]["thumb"] == "https://images.example/f/15.jpg/v1/fit/w_375,h_375/piece_15-375w.jpg"
    assert rows[1]["thumb"] == "https://images.example/f/11.jpg/v1/fit/w_375,h_375/piece_11-375w.jpg?token=second"
