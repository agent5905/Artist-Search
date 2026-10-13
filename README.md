# Artist Search

A search engine for game concept and character artists.

Type a brief the way a studio would, such as "dark fantasy creature designer, painterly, open for commissions", and get **artists**, not single images. Open an artist and see others who draw in a similar style.

**Status, 2026-10-13: not finished, and not deployed.** It runs on the owner's machine against a real database of 119 artists. The target is 2,000 or more. [PROGRESS.md](PROGRESS.md) is the running log, updated after every round of work.

## Why

Portfolio sites answer a search with artworks. Someone hiring wants to know who the artist is, what they mostly draw, how they draw it, and whether they take work. That takes a profile of each artist built from their whole portfolio, and a way to compare styles that does not depend on tags the artist happened to type.

## How it works

```
discover -> collect -> analyze (local GPU) -> store (Postgres) -> search (edge function) -> site
```

| Stage | What it does | Where it runs |
|---|---|---|
| Discover | Finds artists with several works under niche tags, through DeviantArt's official API | Owner's machine |
| Collect | Reads each artist's public profile and about ten recent works. Preview images go to a local cache that git ignores | Owner's machine |
| Tag | A local vision model (`qwen3.8` in Ollama) tags and describes each work. A second pass checks every claim against the image and drops what it cannot see | Owner's machine |
| Style | A style model (CSD ViT-L) turns each work into a vector. Each artist's nearest artists are computed locally | Owner's machine |
| Profile | The model looks at one sheet of an artist's works, proposes what recurs, then counts it. Only claims that pass the count are written into one or two plain sentences, which are embedded with `bge-m3` | Owner's machine |
| Push | Sends text, links, thumbnail addresses and vectors to the database. It holds back mature, AI-declared and flagged works, and stops if a run would delete more than 10% of what is public | Owner's machine |
| Store | Supabase Postgres with `pgvector`, free tier. Every table is locked; visitors reach the data only through five database functions | Supabase |
| Search | A Cloudflare Pages Function embeds the query with the same `bge-m3` weights on Workers AI, and a database function blends meaning with keywords | Cloudflare |
| Site | React pages: search, similar style, artist profile, claim or remove | Cloudflare Pages |

No image analysis runs in the cloud, and no paid AI service is used.

## How it was built: the Gauntlet Loop

One lead agent split the work into pieces small enough to judge alone. Each piece got a builder and a separate critic with fresh context. A critic sees the goal, the bar, the hard rules and the real output, and never the builder's reasoning. Where two options existed, the critic compared them blind, labeled A and B in shuffled order. The critic names the biggest gap, and that becomes the builder's next task. When the same gap survives several rounds, the approach changes.

Examples from the log:

- **The style model was chosen blind.** Two models ranked "artists like this one" for 12 seed artists. The critic picked CSD over SigLIP 2 on 7 seeds to 4, with 1 tie. Two more blind tests then chose the ranking rule.
- **Tags improved by measurement.** Works with a claim the image does not support went from 15 of 30, to 11, to 7, as a verification pass was added and tightened. Wrong genres and subjects went from 12 to 0. About one description in five still names a detail wrongly.
- **Profiles changed approach.** The first critic called them "too generic to choose by: 19 of 20 say digital painting". Adding specifics from single works brought new errors, such as "mostly elves" on a portfolio with 3 elves in 10 works. The fix was to make the model count each claim across the artist's works before stating it.
- **Collect failed seven critics in a row.** Each found a way that a bad answer from the API could delete good data. After four rounds of guarding the deletions, the approach changed: the stage stopped deleting and marks what looks wrong. The seventh critic ran 660 failure sequences and lost nothing.
- **The removal rule was rewritten.** "Any request hides the profile at once" failed twice, because anyone could hide anyone. Removal now needs proof of control and is then immediate and permanent.

## How artists' data is handled

- **Nothing is rehosted.** The database holds links and thumbnail addresses. The site loads thumbnails from the source's own image host, with credit and a link back.
- **Images are cached only on the owner's machine,** in a folder git ignores. A commit hook rejects image files and anything that looks like a secret.
- **Collection is polite.** It uses the official API at no more than one request a second, identifies itself with a contact address, and backs off when told to. The crawler for artists' own sites obeys robots.txt and waits three seconds between requests to a host.
- **Only public, professional information is kept:** handle, public bio, specialties, public links, and whether the artist says they are open for work, in their own words. Contact details found in titles or tags are held back.
- **Left out:** works marked mature or rated suggestive, works the artist declares AI-made, and artists with a large share of either.
- **No model is trained and no image is generated.** Models only describe and compare.
- **Claim or remove is on every profile.** An artist proves control by putting a short code on their site or DeviantArt profile. The profile is then removed at once and never collected again.

**One open question, stated plainly.** DeviantArt's Terms of Service, section 24, restrict using works labeled "noai" as input to AI systems. The agent leading the build read that as covering the local tagging and stopped to ask. The owner reads the section as aimed at training and image generation, which this project does not do, and decided to proceed. The decision and both readings are in [PROGRESS.md](PROGRESS.md), under "Owner decisions".

## What works and what does not yet

| | State |
|---|---|
| Discover, collect, tag, style, profile, push | Run end to end on real data: 442 artists collected, 4,381 works tagged |
| Public database | 119 artists, 1,130 works, 17 MB of a 400 MB budget |
| Search and similar style | Work locally against the real database |
| Site | Runs locally. Beat ArtStation and Behance pages on 7 of 8 pairs, twice, with the caveat that logos made the test not truly blind |
| 2,000 or more artists | Not yet. Collection at scale has just been cleared by its critic |
| Blind tests against DeviantArt's own search and "More Like This" | Not yet run. The reference results are captured |
| Profile claims | Roughly 9 artists in 10 have no unsupported claim. The bar is all of them |
| Removal proof | The request and code steps exist. The step that checks the code is not built |
| Public deployment | Not done. It waits on the owner's approval |
| Crawling artists' own portfolio sites | The polite crawler is built and tested but not used at scale |

## Run it locally

Needs Python 3.12 with [uv](https://docs.astral.sh/uv/), a recent Node (built with 24), [Ollama](https://ollama.com) with `qwen3.8` and `bge-m3`, a GPU that can run a 27B vision model (built on an RTX 5090 with 32 GB), a free Supabase project, a Cloudflare account, and a DeviantArt API application.

```bash
cp .env.example .env                 # fill in your own keys; .env is git-ignored
git config core.hooksPath .githooks  # secret scan before every commit
uv sync
uv run python scripts/doctor.py      # checks the database and Cloudflare access
uv run python -m pipeline.db migrate
```

Then the stages, in order. Each can be rerun without making duplicates.

```bash
uv run python -m pipeline.discover --max-artists 200
uv run python -m pipeline.collect --max-artists 200
uv run python -m pipeline.style --download --model csd
uv run python -m pipeline.style --model csd --max-works 500
uv run python -m pipeline.tagging --max-works 500 --verifier qwen3.8:latest
uv run python -m pipeline.tagging --max-works 500 --formats
uv run python -m pipeline.profiles --max-artists 500
uv run python -m pipeline.push --dry-run
uv run python -m pipeline.push
```

The site:

```bash
cd web
npm install
npm run dev
```

The checks:

```bash
uv run ruff check .
uv run mypy
uv run pytest -q
python scripts/secret_scan.py --all
```

## Where things are

| Path | What |
|---|---|
| `ARTIST_SEARCH_SPEC.md` | The brief and its hard rules |
| `PROGRESS.md` | The plan, the owner's decisions, and the log of every round |
| `pipeline/` | The stages above, one module each |
| `sql/` | Schema and the public database functions |
| `web/` | The site and its search function |
| `eval/` | The frozen queries and seed artists, and the scripts that build blind test packages |
| `tests/` | Unit tests for every stage |
