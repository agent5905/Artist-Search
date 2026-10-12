"""Tags and a short description for each collected work, from a vision model running on this machine.

    uv run python -m pipeline.tagging --max-works 500 [--model gemma4:26b]
    uv run python -m pipeline.tagging --sample 40 --seed 7 [--model gemma4:26b]

One table, work_tags, in the local SQLite file beside the library's: one row per work, model and prompt version,
so each model keeps its own result set and a changed prompt (bump PROMPT_VERSION) is asked again.
  - hard rule 9: the only thing this stage talks to is Ollama on a loopback address (_client). The model gets the
    image bytes and PROMPT, nothing else: no file name, title, artist or address.
  - every answer goes through validated() before it is stored: the fixed vocabularies below, or nothing.
  - hard rule 7: rows of works the library no longer holds are deleted at the start of every run and never read.
Every function takes an optional `db` path; the default is config.LOCAL_DB.
"""

import argparse
import base64
import contextlib
import hashlib
import ipaddress
import json
import pathlib
import re
import sqlite3
import sys
import time
from collections.abc import Iterator
from typing import Any

import httpx

from pipeline import library, store

OLLAMA_URL = "http://127.0.0.1:11434"
DEFAULT_MODEL = "qwen3.8:latest"  # or --model gemma4:26b. Not a verdict: which one is better is for a critic to say
PROMPT_VERSION = 1  # bump when PROMPT, the vocabularies or the request change what a model would answer
MAX_ATTEMPTS = 3  # per work; then it is recorded as failed
MAX_DESCRIPTION = 400  # characters

# The fixed vocabularies: value -> what the model is told it means. Search filters on these values.
MEDIUMS = {
    "digital painting": "a 2D picture built up in colour or tone with shading, painterly or cel-shaded, with no "
    "physical materials showing",
    "digital line art": "a 2D picture carried by its lines: a sketch, an inked drawing or clean line work with "
    "little or no shading, with no physical materials showing",
    "3d render": "an image rendered from 3D models: a digital sculpt, a game model, a CG scene",
    "pixel art": "an image built from visibly large square pixels",
    "traditional": "physical materials can be seen: pencil, ink, marker, watercolour or paint on paper or canvas, "
    "or a photographed or scanned page",
    "photo": "a photograph of real people, places or objects, including a photo of a sculpture, costume or craft",
    "unclear": "you cannot tell which of these it is, or none fits",
}
SUBJECTS = {
    "character": "a person or humanoid figure",
    "creature": "an animal, monster or other non-human being",
    "environment": "a place shown for its own sake: a landscape, city, building or interior. Not the backdrop "
    "behind a figure",
    "prop or weapon": "an object presented on its own, as in a weapon or item design. Not one that a figure holds, "
    "wears or uses",
    "vehicle or mech": "a vehicle, spacecraft, mech or other large machine",
    "interface or icons": "game or app interface elements, icons or emblems",
}
_WORLDS = {
    "fantasy": "magic in use, a mythical being such as a dragon, elf or demon, or medieval-style arms and armour "
    "in an invented world",
    "dark fantasy": "fantasy that is plainly grim: undead, demons, decay, the macabre",
    "science fiction": "a spacecraft, a robot or mech, an alien world, or futuristic weapons, suits or buildings",
    "cyberpunk": "a neon-lit high-tech city, or people with visible cybernetic parts",
    "horror": "gore, corpses, or a monster or threat shown to frighten",
    "historical": "a real past era, with no fantasy or science-fiction elements",
    "modern": "the present-day real world: its clothes, objects, streets or rooms",
}
_MANNERS = {
    "anime style": "drawn in the manner of Japanese anime or manga",
    "stylized cartoon": "clearly simplified or exaggerated shapes and proportions, as in animation and comics "
    "outside anime",
    "realistic": "lifelike proportions, lighting and detail",
}
GENRES = _WORLDS | _MANNERS
MAX_STYLE_WORDS = 6


def _listed(vocabulary: dict[str, str]) -> str:
    return "\n".join(f"- {value}: {meaning}" for value, meaning in vocabulary.items())


PROMPT = f"""You label one image for a search index of artists. A reviewer will hold every word you write against \
the image. Anything the image does not plainly show counts against you; leaving something out never does.

Rules
- Use only what you can see in the image. When you are not sure something applies, leave it out: an empty list is \
a good answer, and so is the medium "unclear".
- Never name or guess the artist, the software or tools used, or any franchise, game, film or named character.
- Never quote or mention a signature, watermark, logo, caption or other writing in the image.
- Prefer the plain word you are sure of to the specific one you would be guessing: "a figure in plate armour", not \
"a paladin". State gender, age, material or mood only when it is plain to see. Count things only up to three; \
beyond that say "several".

Answer with one JSON object with these fields.

description: one or two short sentences saying what the image shows: the main subject, its pose or action, and \
the setting if there is one. Keep to what a viewer sees first and cannot mistake: leave out small details, and \
anything you would hedge with "or", "perhaps" or "appears". No praise, no interpretation, no story.

medium: exactly one of
{_listed(MEDIUMS)}

subjects: each of these that is a main focus of the image. Most images have one or two: a figure holding a sword \
in front of a forest is "character" and nothing else.
{_listed(SUBJECTS)}

genres: each of these that the image plainly shows.
About the world shown. Give one only when your description names the thing that shows it. A plain portrait shows \
none of them, and neither does an invented creature on its own, a gloomy scene, a ruin or a weapon.
{_listed(_WORLDS)}
About how the image is drawn. At most one, and none when it sits between them.
{_listed(_MANNERS)}

style_words: up to {MAX_STYLE_WORDS} lowercase words for visible qualities of technique, line, colour and tone, \
each one checkable at a glance, such as painterly, cel-shaded, monochrome, sketchy, flat-colour, lineart, \
high-contrast, muted, saturated, textured, soft-shaded, hard-edged, chibi, isometric, low-poly. No mood and no \
praise (moody, dramatic, atmospheric, detailed), and no word from the lists above. Fewer is better than a \
doubtful one.

explicit: true only if the image shows nudity with exposed genitals or nipples, a sexual act, or graphic gore such \
as dismemberment or exposed innards. Revealing clothing, weapons, monsters and a little blood are not explicit.

not_artwork: true only if the image is not itself a piece of visual art: a photograph of a real person, a meme, \
a price list, an advertisement or announcement such as a commission sheet (even when it shows artwork), a \
screenshot of text, chat or an app. A photo of a drawing, painting or sculpture is artwork."""


def _choices(vocabulary: dict[str, str]) -> dict[str, Any]:
    # maxItems is enforced by Ollama 0.34.2: without it a model can repeat one value until it runs out of tokens.
    return {"type": "array", "items": {"type": "string", "enum": list(vocabulary)}, "maxItems": len(vocabulary)}


# What a whole answer has. A flag must be true or false: 0, "false" and null are not.
_FIELDS: dict[str, type] = {
    "description": str,
    "medium": str,
    "subjects": list,
    "genres": list,
    "style_words": list,
    "explicit": bool,
    "not_artwork": bool,
}
# Ollama's structured output: it constrains the answer to this shape, in this order (description first, so the
# model looks before it tags). validated() still checks everything, because nothing here is taken on trust.
_ANSWER = {
    "type": "object",
    "properties": {
        "description": {"type": "string"},
        "medium": {"type": "string", "enum": list(MEDIUMS)},
        "subjects": _choices(SUBJECTS),
        "genres": _choices(GENRES),
        "style_words": {"type": "array", "items": {"type": "string"}, "maxItems": MAX_STYLE_WORDS},
        "explicit": {"type": "boolean"},
        "not_artwork": {"type": "boolean"},
    },
    "required": list(_FIELDS),
}

_SCHEMA = """
CREATE TABLE IF NOT EXISTS work_tags (
    deviation_id TEXT NOT NULL,      -- works.deviation_id
    model TEXT NOT NULL,             -- the Ollama model that was asked
    prompt_version INTEGER NOT NULL, -- PROMPT_VERSION when it was asked
    result TEXT,                     -- JSON object, exactly what validated() returned; NULL when the work failed
    analyzed_at TEXT NOT NULL,       -- ISO-8601 UTC
    PRIMARY KEY (deviation_id, model, prompt_version)
) STRICT;
"""

# ponytail: splits on any full stop followed by a space, so "Dr. Who" counts as a sentence end. A sentence
# tokenizer is the upgrade if descriptions start coming back cut short.
_SENTENCE_END = re.compile(r"(?<=[.!?])\s+")
_STYLE_WORD = re.compile(r"(?=.{2,24}$)[a-z]+(?:[ -][a-z]+){0,2}")


def validated(answer: object) -> dict[str, Any] | None:
    """A model's answer with everything outside the vocabularies dropped. None when it is not a whole answer.

    Dropped: a subject or genre that is not in its vocabulary, a style word that is not one to three plain words
    or that is a medium, subject or genre (those are said once, where search can filter on them), repeats, a
    third sentence. A medium outside the vocabulary becomes "unclear". None, so that the work is asked
    again: a missing or mistyped field (a flag must be true or false, never absent), a description with no
    words in it, or one longer than MAX_DESCRIPTION.
    """
    if not isinstance(answer, dict) or not all(isinstance(answer.get(field), kind) for field, kind in _FIELDS.items()):
        return None
    description = " ".join(_SENTENCE_END.split(" ".join(answer["description"].split()))[:2])
    if len(description) > MAX_DESCRIPTION or not any(map(str.isalpha, description)):
        return None

    def said(field: str) -> list[str]:
        return [value.strip().lower() for value in answer[field] if isinstance(value, str)]

    medium = answer["medium"].strip().lower()
    subjects, genres, fixed = said("subjects"), said("genres"), {*MEDIUMS, *SUBJECTS, *GENRES}
    words = [word for word in said("style_words") if _STYLE_WORD.fullmatch(word) and word not in fixed]
    return {
        "medium": medium if medium in MEDIUMS else "unclear",
        "subjects": [value for value in SUBJECTS if value in subjects],
        "genres": [value for value in GENRES if value in genres],
        "style_words": list(dict.fromkeys(words))[:MAX_STYLE_WORDS],
        "description": description,
        "explicit": answer["explicit"],
        "not_artwork": answer["not_artwork"],
    }


def _client(url: str, transport: httpx.BaseTransport | None = None) -> httpx.Client:
    """The only connection this stage makes. ValueError unless the address is a loopback IP address (hard rule 9).

    A host name is refused, "localhost" included: a name goes wherever the resolver sends it. No proxy is taken
    from the environment and no redirect is followed, so a request reaches that address or nothing.
    """
    try:
        address = httpx.URL(url)
        local = address.scheme in ("http", "https") and ipaddress.ip_address(address.host).is_loopback
    except (httpx.InvalidURL, ValueError):
        local = False
    if not local:
        raise ValueError("the vision model must be on a loopback address, such as http://127.0.0.1:11434")
    timeout = httpx.Timeout(600, connect=5)  # the first answer waits for 17 GB of model to load
    return httpx.Client(base_url=address, transport=transport, timeout=timeout, trust_env=False, follow_redirects=False)


def _ask(client: httpx.Client, model: str, image: bytes, attempt: int) -> dict[str, Any] | None:
    """Ask the model about one image. The validated answer, or None when the answer is empty or malformed.

    httpx.HTTPError when Ollama itself is in trouble (unreachable, out of memory, no such model): the run stops
    there, because recording that as the work's failure would write off every work after it.
    """
    request = {
        "model": model,
        "stream": False,
        # ponytail: no thinking. On 60 works it tripled the time (2.2 s to 6.5 s an image) and gave about one
        # world genre in eight fewer. Switch it on, with num_predict near 3000, if unsupported tags keep turning up.
        "think": False,
        "format": _ANSWER,
        "keep_alive": "1m",  # the GPU is shared: give the memory back soon after the last image
        "messages": [
            {"role": "system", "content": PROMPT},
            {"role": "user", "content": "Label this image.", "images": [base64.b64encode(image).decode("ascii")]},
        ],
        # The first try is the model's single most likely answer. A retry at the same setting would repeat it.
        "options": {"temperature": 0.3 * attempt, "seed": attempt, "num_ctx": 8192, "num_predict": 400},
    }
    response = client.post("/api/chat", json=request)
    # ponytail: the bytes go as they are, and 400 is what Ollama answers for bytes it cannot decode. A file cut
    # short still decodes, and is described as the half picture it is. Opening each file with Pillow first, and
    # deleting the ones that do not open, is the upgrade.
    if response.status_code == 400:
        return None
    response.raise_for_status()
    try:
        return validated(json.loads(response.json()["message"]["content"]))
    except (ValueError, KeyError, TypeError):
        return None


@contextlib.contextmanager
def _open(db: pathlib.Path | str | None) -> Iterator[sqlite3.Connection]:
    """One transaction on the library's file, opened the library's way, with this table."""
    with library._open(db) as conn:
        conn.executescript(_SCHEMA)
        yield conn


def run(
    max_works: int,
    model: str = DEFAULT_MODEL,
    *,
    ollama_url: str = OLLAMA_URL,
    db: pathlib.Path | str | None = None,
    transport: httpx.BaseTransport | None = None,
) -> dict[str, float]:
    """Analyze up to `max_works` works that have a cached image and no row yet for this model and prompt version.

    Returns counts only: analyzed (stored with a result), failed (stored without one, after MAX_ATTEMPTS), flagged
    (analyzed, and explicit or not_artwork), skipped (already had a row, or its image file is gone), and
    seconds_per_image. Each row is committed as it is made, so a stopped run loses nothing and a rerun asks nothing
    twice. ValueError before anything is sent if the address is not loopback or the model cannot read images.
    """
    counts = {"analyzed": 0, "failed": 0, "flagged": 0, "skipped": 0}
    key = (model, PROMPT_VERSION)
    with _client(ollama_url, transport) as client:
        with _open(db) as conn:
            # Hard rule 7: library.purge() does not know this table, so a removed artist's rows go here.
            conn.execute("DELETE FROM work_tags WHERE deviation_id NOT IN (SELECT deviation_id FROM works)")
            works = conn.execute(
                "SELECT w.deviation_id, w.image_file, t.deviation_id IS NOT NULL FROM works w LEFT JOIN work_tags t "
                "ON t.deviation_id = w.deviation_id AND t.model = ? AND t.prompt_version = ? "
                "WHERE w.image_file IS NOT NULL ORDER BY w.deviation_id",
                key,
            ).fetchall()
        pending = [(work_id, name) for work_id, name, done in works if not done][:max_works]
        counts["skipped"] = sum(done for _, _, done in works)
        if pending:
            shown = client.post("/api/show", json={"model": model})
            shown.raise_for_status()
            if "vision" not in shown.json().get("capabilities", []):
                raise ValueError("that model cannot read images")  # it would answer anyway, from nothing
        started = time.monotonic()
        for work_id, name in pending:
            path = library.image_dir() / name
            if pathlib.PurePath(name).name != name or not path.is_file():  # a file of the cache, and still there
                counts["skipped"] += 1
                continue
            image = path.read_bytes()
            result = next(
                (answer for attempt in range(MAX_ATTEMPTS) if (answer := _ask(client, model, image, attempt))), None
            )
            with _open(db) as conn:  # not held open while the model works: other stages write to this file too
                conn.execute(
                    "INSERT OR IGNORE INTO work_tags SELECT ?, ?, ?, ?, ? "
                    "WHERE EXISTS (SELECT 1 FROM works WHERE deviation_id = ?)",  # not if it was removed meanwhile
                    (work_id, *key, result and json.dumps(result), store._now(), work_id),
                )
            counts["analyzed" if result else "failed"] += 1
            counts["flagged"] += bool(result and (result["explicit"] or result["not_artwork"]))
    asked = counts["analyzed"] + counts["failed"]
    return {**counts, "seconds_per_image": round((time.monotonic() - started) / asked, 2) if asked else 0.0}


def results(
    model: str = DEFAULT_MODEL, prompt_version: int = PROMPT_VERSION, db: pathlib.Path | str | None = None
) -> dict[str, dict[str, Any]]:
    """Each analyzed work's result, by deviation id, for one model and prompt version.

    A result has medium, subjects, genres, style_words, description, explicit and not_artwork. Flagged works are
    in it, for the caller to exclude. Failed works, and works the library no longer holds, are not.
    """
    query = (
        "SELECT t.deviation_id, t.result FROM work_tags t JOIN works w ON w.deviation_id = t.deviation_id "
        "WHERE t.model = ? AND t.prompt_version = ? AND t.result IS NOT NULL"
    )
    with _open(db) as conn:
        return {work_id: json.loads(result) for work_id, result in conn.execute(query, (model, prompt_version))}


def sample(
    count: int, seed: int, model: str = DEFAULT_MODEL, db: pathlib.Path | str | None = None
) -> list[dict[str, Any]]:
    """A random sample of the stored rows for the critic, failed ones included (their result is null).

    The order is a hash of the seed and the work, not of the row's place in a list: two models that have covered
    the same works give the same works for the same seed, and a work added later does not reshuffle the rest.
    """
    query = (
        "SELECT t.deviation_id, w.image_file, t.result FROM work_tags t "
        "JOIN works w ON w.deviation_id = t.deviation_id "
        "WHERE t.model = ? AND t.prompt_version = ?"
    )
    with _open(db) as conn:
        rows = conn.execute(query, (model, PROMPT_VERSION)).fetchall()
    rows.sort(key=lambda row: hashlib.sha256(f"{seed}:{row[0]}".encode()).digest())
    return [
        {
            "image": name and str(library.image_dir() / name),
            "deviation_id": work_id,
            "model": model,
            "result": result and json.loads(result),
        }
        for work_id, name, result in rows[: max(count, 0)]
    ]


def main(argv: list[str] | None = None, transport: httpx.BaseTransport | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--max-works", type=int, help="analyze at most this many works that have no result yet")
    mode.add_argument("--sample", type=int, help="print this many stored results, one JSON object a line")
    parser.add_argument("--seed", type=int, default=0, help="which sample")
    parser.add_argument("--model", default=DEFAULT_MODEL, help="the Ollama model; each has its own result set")
    parser.add_argument("--ollama-url", default=OLLAMA_URL, help="a loopback address, nothing else is accepted")
    args = parser.parse_args(argv)
    if args.sample is not None:
        for row in sample(args.sample, args.seed, args.model):
            print(json.dumps(row))
        return
    try:
        counts = run(args.max_works, args.model, ollama_url=args.ollama_url, transport=transport)
    except ValueError as error:
        sys.exit(f"stopped: {error}")
    except httpx.HTTPError as error:  # the name of the error only; what was stored before it is kept
        sys.exit(f"stopped: Ollama did not answer ({type(error).__name__}). Rerun to carry on.")
    print(json.dumps(counts))  # counts and throughput, nothing about any work


if __name__ == "__main__":
    main()
