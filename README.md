# Artist Search

A search engine for game concept and character artists. Describe the work you need the way a studio would brief it, for example "dark fantasy creature designer, painterly, open for commissions", and get back **artists** rather than single images.

**Live:** [artist-search.pages.dev](https://artist-search.pages.dev)

## Features

- **Search by brief.** Plain-language queries match on meaning and keywords. Results are artist cards with a short profile, their own tags and four of their works.
- **Filters** for open-for-work, specialty (character, environment, creature, vehicle) and medium.
- **More like this.** For any artist, other artists who work in a similar style, each shown with their works closest to that style.
- **Artist profiles** with a link back to every work at its source.
- **Claim or remove** on every profile. An artist can remove themselves with a short proof code, or send a request for the site owner to answer.

## How it works

```
discover -> collect -> analyze (local GPU) -> push -> Postgres -> search function -> site
```

| Stage | What it does | Runs on |
|---|---|---|
| Discover | Finds artists in the niche through DeviantArt's official API | Local machine |
| Collect | Reads each artist's public profile and recent works. Preview images go to a local, git-ignored cache | Local machine |
| Analyze | Local models tag and describe each work, check every claim against the image, rate the content, embed the style, and write a one-sentence profile per artist | Local GPU |
| Push | Sends text, links, thumbnail addresses and vectors to the database from one consistent snapshot, holding back anything the content rules leave out | Local machine |
| Store | Postgres with `pgvector` on Supabase. Tables are locked; only the site's server can query them, through a few database functions | Supabase |
| Search | A Cloudflare Pages Function embeds the query and calls the database | Cloudflare |
| Site | React pages: search, similar style, profile, claim or remove | Cloudflare Pages |

All image analysis runs locally. No paid AI service is used, no model is trained, and no image is generated.

## Tech stack

- **Pipeline:** Python 3.12 (`uv`, `httpx`, SQLite for the local store, PyTorch)
- **Models:** `qwen3.8` (vision) and `bge-m3` (text embeddings) in [Ollama](https://ollama.com); CSD ViT-L for style vectors
- **Database:** Supabase Postgres with `pgvector`
- **Site:** React, TypeScript, Vite, Cloudflare Pages Functions, Workers AI (`bge-m3` for queries)

## Getting started

### Requirements

- Python 3.12 and [uv](https://docs.astral.sh/uv/)
- Node 24
- Ollama with `qwen3.8` and `bge-m3`
- A GPU that can run a 27B vision model (developed on an RTX 5090, 32 GB)
- A Supabase project, a Cloudflare account, and a DeviantArt API application

### Setup

```bash
cp .env.example .env                 # fill in your own keys; .env is git-ignored
git config core.hooksPath .githooks  # secret scan before every commit
uv sync
uv run python scripts/doctor.py      # checks database and Cloudflare access
uv run python -m pipeline.db migrate # applies sql/ to the database
```

`.env.example` lists every variable. `SITE_TOKEN` is any long random string. The database stores only its hash and answers the site's functions only when they present it.

## Running the pipeline

Each stage can be rerun safely; it skips what is already done.

```bash
uv run python -m pipeline.discover --max-artists 200
uv run python -m pipeline.collect --max-artists 200
uv run python -m pipeline.style --download --model csd
```

The analysis stages run in turns under a background worker that can be paused, since a full run takes days on one GPU:

```bash
uv run python scripts/gpu.py start     # run every analysis stage in the background
uv run python scripts/gpu.py status    # progress and pace
uv run python scripts/gpu.py pause     # free the GPU; nothing is lost
uv run python scripts/gpu.py resume
```

Then publish to the database:

```bash
uv run python -m pipeline.push --dry-run   # what would change, changing nothing
uv run python -m pipeline.push             # stops before deleting much at once; --allow-delete N to confirm
```

To hold back a work or an artist by hand, add its deviation id or `da:<handle>` to `cache/held_by_hand.txt`, one per line.

## Removal requests

```bash
uv run python -m pipeline.removals            # check proof codes and remove proven artists
uv run python -m pipeline.removals --pending  # open requests without proof
uv run python -m pipeline.removals --grant ID # remove for good
uv run python -m pipeline.removals --reject ID
```

Run the checker every 15 minutes, for example with Windows Task Scheduler, set so that two runs never overlap. The claim page promises a code is found within a day.

## The site

```bash
cd web
npm install
npm run dev      # Vite on :5173, with the search function on :8788
npm test
```

Deploy:

```bash
npm run build
npx wrangler pages deploy dist --project-name artist-search --branch main
```

The Pages project needs `VITE_SUPABASE_URL`, `VITE_SUPABASE_PUBLISHABLE_KEY` and `SITE_TOKEN` as secrets. The Workers AI binding comes from `web/wrangler.toml`.

## Tests and checks

```bash
uv run pytest -q --ignore=tests/test_db.py   # unit tests; test_db.py runs against the live database
uv run mypy pipeline scripts eval tests
uv run ruff check . && uv run ruff format --check .
python scripts/secret_scan.py --all
```

`tests/test_db.py` writes only `zz-test-` rows to the database and removes them afterwards.

## How artists' data is handled

- **Nothing is rehosted.** The database holds links and thumbnail addresses; images load from the source with credit and a link back. Cached previews stay on the local machine.
- **Only public, professional information** is kept: handle, public bio, specialties, public links and stated availability. Contact details are left out of everything that is shown.
- **Collection is polite:** the official API, paced and backing off when asked, and one client per host.
- **Content rules.** The site shows ordinary game art, including moderate blood and gore. It leaves out:
  - sexual, fetish and hateful content;
  - images that are not artwork;
  - political content about real people;
  - artists who declare AI-made work, and accounts that declare themselves adult.
- **Claim or remove.** A proven removal is immediate and permanent, and the artist is never collected again.

## Project layout

| Path | Contents |
|---|---|
| `pipeline/` | The pipeline stages, one module each |
| `scripts/` | The GPU worker, setup checks and the secret scan |
| `sql/` | Schema and database functions |
| `web/` | The site and its search function |
| `eval/` | Frozen test queries and seed artists, and scripts that build blind comparisons |
| `tests/` | Tests for every stage |
| `docs/` | Operational notes (for example the edge rate-limit rule) |
| `ARTIST_SEARCH_SPEC.md` | The original brief |
| `PROGRESS.md` | Design decisions, test results and the build log |
