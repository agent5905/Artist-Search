# Artist Search: Progress

**Status, 2026-10-11:** Setup is done. The build is **paused on one owner decision** (item 1 below). No DeviantArt request has been made and no artist data has been collected.

## Waiting on the owner

1. **DeviantArt's terms forbid the Analyze step as the spec describes it.** Details in [The DeviantArt finding](#the-deviantart-finding). Pick a path: A, B or C.
2. **Credentials.**
   - `DEVIANTART_CLIENT_ID` and `DEVIANTART_CLIENT_SECRET` are not in `.env`. They come from registering an app at deviantart.com/developers/register (needed for paths A and B).
   - `CLOUDFLARE_API_TOKEN` is rejected by Cloudflare (HTTP 401, "Invalid API Token"). Not needed until the site deploys.
   - `CLOUDFLARE_PAGES_PROJECT_NAME` and `APP_BASE_URL` are empty. Not needed until the site deploys.
3. **Pushing.** Everything is committed locally. Nothing has been pushed to GitHub yet.

## The plan

### Breakdown and bars

Each row is one piece with its own builder and its own fresh critic. Critics get the goal, the bar, the hard rules and the real output, nothing else.

| # | Piece | What the critic inspects | Bar |
|---|---|---|---|
| 1 | Collect | Test output | Pass or fail: rerun creates no duplicates, rate limit holds, removed artists stay removed |
| 2 | Work tags | Each tag next to the image | Any unsupported tag is a loss |
| 3 | Work descriptions | Each description next to the image | Any unsupported claim is a loss |
| 4 | Artist profiles | Each claim next to the works and bio | Any unsupported claim is a loss |
| 5 | Text search ranking | Two result pages per query, labeled A and B in random order | Beat the reference on a clear majority of 30+ queries |
| 6 | Style similarity | Two result sets per seed, A and B in random order | Beat the reference on a clear majority of 30 seeds |
| 7 | Search page | Screenshots, desktop and phone | Blind A/B against ArtStation and Behance search pages |
| 8 | Similar-style page | Screenshots, desktop and phone | Same |
| 9 | Profile page | Screenshots, desktop and phone | Blind A/B against ArtStation and Behance profile pages; "claim or remove" visible |
| 10 | Schema and search functions | Test output and measured size | Pass or fail: under 400 MB, no image bytes stored |
| 11 | Claim or remove | Test output | Pass or fail: a removed artist is gone and is skipped on every later collect |

The reference for pieces 5 and 6 is DeviantArt's own search and "more like this" in the spec. Whether that reference can be used depends on the decision above.

### The eval set (`eval/`, frozen before the first judged round)

- **`eval/queries.jsonl`, 36 queries.** 12 plain briefs in the spec's style (style + subject + availability), 12 written the way game studios phrase art roles (for example "stylized hand-painted prop concepts"), 12 hard cases (a medium filter, a negation, a misspelling, a rare style). Written before any search code exists, so they cannot be tuned to it.
- **`eval/seeds.jsonl`, 30 seed artists.** Drawn at random with a fixed seed from the first 200 collected artists, not hand-picked.
- **Claim sample.** 50 artists per round, drawn at random with a recorded seed, for the tag and profile checks.
- **Reference results** are captured once per query and seed and frozen with the set.
- The SHA-256 of each eval file is recorded here when it is frozen. A changed hash without an owner note is a failed round.

### Local models (all confirmed to exist on 2026-10-11)

| Job | Choice | Status |
|---|---|---|
| Describe and tag an image | `qwen3.8` (27B, vision) in Ollama, with `gemma4:26b` as the A/B challenger | Both already installed |
| Text embeddings | `qwen3-embedding:4b` in Ollama, 2,560 dimensions, stored half-precision | Already installed |
| Style embeddings | `tomg-group-umd/CSD-ViT-L` (built for style) against `google/siglip2-so400m-patch16-384`, chosen by A/B on the seeds | Both on Hugging Face, not gated. DINOv3 is gated, so skipped |
| Runtime | PyTorch 2.14.1 with CUDA 13.0 wheels, Python 3.12 | Wheel exists for Windows; not installed yet |

Size estimate for 3,000 artists and 30,000 works: about 45 MB of 768-dimension half-precision style vectors plus an index of similar size or larger, and about 15 MB of profile vectors. That fits the 400 MB budget on paper. The real size is measured every round.

### Order of work

1. Setup. **Done.**
2. Collect a first 200 artists. **Blocked on the decision.**
3. Analyze, then store and search, with critics starting as soon as there is real output.
4. The three pages, on a private preview.
5. Scale to 2,000+ and add portfolio-site crawling.
6. Public launch and the keep-alive Worker, after asking the owner.

### Fixed checks (run every round)

| Check | Now |
|---|---|
| Lint (`ruff`), types (`mypy`), unit tests (`pytest`) | Pass, 5 tests |
| Secret scan | Clean. Proven to block a real `.env` value before the first commit |
| Each pipeline stage run twice makes no duplicates | No pipeline yet |
| Crawler obeys robots.txt and its rate limit | No crawler yet |
| No image files tracked in git | Pass. The pre-commit hook rejects image files |
| No images stored in Supabase | Pass. The database has no tables |
| Database under 400 MB | 10.3 MB |

Screenshots contain artwork, so they are never committed. Evidence in this file is text: result lists, verdicts and scores. Screenshots are sent to the owner directly.

## The DeviantArt finding

Hard rule 3 says to read DeviantArt's terms before writing code against the API. Read on 2026-10-11:

- **Terms of Service, section 24** ([source](https://www.deviantart.com/about/policy/service/)). Every deviation carries a `noai` or `noimageai` label unless the artist actively opts in. Labeled content may not be used to train an AI system, or "as input into any previously trained artificial intelligence system". The same section says a missing label does not mean the artist consented.
- **What that covers here.** Tagging an image with a local vision model, embedding it, embedding a bio, and writing a profile with a language model all feed DeviantArt content into a trained model. So does showing DeviantArt images or search results to a critic agent. That is the whole Analyze step and most of the judging.
- **The API does not expose the label.** The deviation and metadata responses have no AI field, so opted-in works cannot be picked out through the API.
- **API License, section 2** ([source](https://www.deviantart.com/about/policy/api/)). The API may not be used for benchmarking DeviantArt. Using its "more like this" endpoint as the A/B reference is at least close to that.
- **No keyword search in the API.** The current reference lists browse by tag, topic and daily picks, plus "more like this". Collection would run on tags, and the text-search reference would have to come from the website, not the API.

### Paths

- **A. Ask DeviantArt for written permission.** The owner sends a request describing the use: a discovery index that links back, no training, no image generation. The spec stays as written. Nothing can be analyzed until they answer.
- **B. DeviantArt for discovery only, analysis on artists' own sites.** The API finds artists in the niche and their public links. No DeviantArt image or text goes into a model or in front of a critic. Analysis runs on an artist's own portfolio site, and only when it carries no AI opt-out (robots.txt, `noai` or `noimageai`). Costs: only artists with their own site qualify, so 2,000 is not assured; pieces 5 and 6 need a different reference; "honor AI opt-outs everywhere" becomes a hard rule.
- **C. A different primary source the owner approves,** such as openly licensed game art or an opt-in list. Its terms get the same reading first.

Analyzing only opted-in DeviantArt works is not on the list: the label is not in the API, and section 24 says its absence is not consent.

## Log

### Round 0: setup (2026-10-11)

**Changed**
- `.env` confirmed git-ignored. `.env.example` added with names only.
- `scripts/secret_scan.py` runs as a pre-commit hook: blocks any real `.env` value, common key formats, and image files.
- `scripts/doctor.py` checks Supabase, pgvector, database size and the Cloudflare token without printing secrets.
- Python 3.12 environment through `uv`, with `ruff`, `mypy` and `pytest`.
- Supabase: `pgvector` 0.8.2 enabled, half-precision vectors confirmed working. No tables yet; the schema waits on the data decision.

**Machine**
- Windows 11, RTX 5090 (32 GB), driver 610.88, CUDA 13.3.
- Python 3.14 system-wide; 3.12 used for the project through `uv` 0.12.
- Ollama 0.34.2 with the vision and embedding models listed above.
- Node 24, `gh` 2.96. No `wrangler`, Supabase CLI or `psql`; `npx wrangler` and the Supabase API cover those.
- Disk: 98 GB free on F:, 68 GB on C:. Enough for the image cache, but both drives are over 90% full.

**Verdict:** no critic round yet. There is nothing real to judge until data can be collected.

**Database:** 10.3 MB of 400 MB. Artists 0, works 0.
