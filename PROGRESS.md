# Artist Search: Progress

**Status, 2026-10-11 (late):** Round 1 is in. Two of three pieces failed their critics and are in round 2; the third critic is still probing. DeviantArt credentials work. The first live discovery run is held until the discovery fix passes a fresh critic.

## Waiting on the owner

1. **Cloudflare token.** `CLOUDFLARE_API_TOKEN` is still rejected (HTTP 401, "Invalid API Token"). The token's record shows it active, so the string in `.env` is most likely not its current secret. Suggested: create a new, narrow token (Pages edit, Workers Scripts edit, Workers AI read). Needed for the site preview and the search box, not before.
2. **Image hosts run by site builders.** Custom-domain Wix, Squarespace, Shopify and WordPress.com sites serve their images from the platform's own servers. Fetching those may fall under the platform's terms, so the default is to refuse them until the owner says otherwise. Cost: those artists yield pages but no images, so they drop out of the index.
3. **Removal policy (default in place).** A removal request hides the artist at once, up to 20 artists a day; past that, requests are logged and wait for review. Nothing is deleted until the owner reviews it. There is no proof of identity yet.
4. **The reference for the search and style A/B tests** (decision 3 below). Proceeding with the stated default.
5. **Optional: the DeviantArt permission request** in `docs/`. Only needed if DeviantArt artwork itself should be analyzed later.

## Owner decisions (these amend the spec)

Made 2026-10-11, after the DeviantArt terms were read.

1. **Path B.** The DeviantArt API is used only to find artists in the niche and their public links. No DeviantArt image or text goes into any model or in front of any agent. Analysis runs on artists' own portfolio sites.
2. **New hard rule: honor AI opt-outs everywhere.** A site or page is skipped when robots.txt disallows us, when it blocks known AI crawlers, or when it carries `noai`, `noimageai`, `noindex` or a text-and-data-mining reservation. Only custom-domain sites are crawled; hosted platforms and social networks are never fetched.
3. **Reference for pieces 5 and 6 (default, pending the owner).** DeviantArt results cannot be shown to a critic. The blind A/B reference becomes a plain keyword search (text) and a generic image-embedding ranking (style) over the same index, and the critic also grades every result on its own against the artist's real work. This is a weaker bar than the spec's. It is the strongest one the terms allow.
4. **Push `main` after every round.**

## The plan

### Breakdown and bars

Each row is one piece with its own builder and its own fresh critic. Critics get the goal, the bar, the hard rules and the real output, nothing else.

| # | Piece | What the critic inspects | Bar |
|---|---|---|---|
| 1a | Discover (DeviantArt API: handles and public links only) | Test output | Pass or fail: rerun creates no duplicates, backs off on rate limits, stores no DeviantArt content, removed artists stay removed |
| 1b | Crawl (artists' own sites) | Test output and adversarial probes | Pass or fail: robots.txt obeyed, 3 seconds per host holds, AI opt-outs honored, platforms never fetched |
| 1c | Portfolio extraction | Extracted works next to the live site | Every work is the artist's own, with a working source link |
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

The reference for pieces 5 and 6 changed under path B. See owner decision 3.

### The eval set (`eval/`, frozen before the first judged round)

- **`eval/queries.jsonl`, 36 queries. Frozen 2026-10-11, SHA-256 `e1a7f9b59a66c62b88166aa2d8fe55bd5c3abf3703de78acfd78e165e3ff13ee`.** 12 plain briefs in the spec's style (style + subject + availability), 12 written the way game studios phrase art roles (for example "stylized hand-painted prop concepts"), 12 hard cases (a medium filter, a negation, a misspelling, a rare style). Written before any search code exists, so they cannot be tuned to it.
- **`eval/seeds.jsonl`, 30 seed artists.** Drawn at random with a fixed seed from the first 200 collected artists, not hand-picked.
- **Claim sample.** 50 artists per round, drawn at random with a recorded seed, for the tag and profile checks.
- **Reference results** are captured once per query and seed and frozen with the set.
- The SHA-256 of each eval file is recorded here when it is frozen. A changed hash without an owner note is a failed round.

### Local models (all confirmed to exist on 2026-10-11)

| Job | Choice | Status |
|---|---|---|
| Describe and tag an image | `qwen3.8` (27B, vision) in Ollama, with `gemma4:26b` as the A/B challenger | Both already installed |
| Text embeddings | Must run in two places with the same weights: locally for profiles, and on Cloudflare Workers AI for the search box. Candidates on both: `bge-m3` and `qwen3-embedding-0.6b` (1,024 dimensions). Chosen by A/B | Listed in the Workers AI catalog; local copies not downloaded yet |
| Style embeddings | `tomg-group-umd/CSD-ViT-L` (built for style) against `google/siglip2-so400m-patch16-384`, chosen by A/B on the seeds | Both on Hugging Face, not gated. DINOv3 is gated, so skipped |
| Runtime | PyTorch 2.14.1 with CUDA 13.0 wheels, Python 3.12 | Wheel exists for Windows; not installed yet |

Size estimate for 3,000 artists and 30,000 works: about 45 MB of 768-dimension half-precision style vectors plus about 6 MB of profile vectors. At this scale an exact scan is fast, so no vector index is built, which removes the main size risk. That fits the 400 MB budget on paper. The real size is measured every round.

### Order of work

1. Setup. **Done.**
2. Discover a first 200 artists and crawl their own sites. **Code in progress; the live run waits on DeviantArt credentials.**
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

**After the owner's answers (same day)**
- Path B and "push main each round" recorded above. `main` pushed to GitHub.
- `eval/queries.jsonl` written and frozen before any search code exists.
- Permission request drafted in `docs/`.
- `scripts/ship.sh` runs every fixed check and then pushes.
- Wave 1 builders started: crawler, DeviantArt discovery, database schema. Each gets a fresh critic when it reports.

### Round 1: crawler, discovery, database (2026-10-11)

Three builders worked in parallel. Each piece then went to a fresh critic that saw only the goal, the bar, the hard rules and the real code.

| Piece | Builder's tests | Critic | Biggest gap the critic named |
|---|---|---|---|
| 1a Discover | 64 pass | **Fail** (4 of 8 bar items) | Hosts were compared as raw text. A DeviantArt URL with a trailing dot was stored as an "external link", which put a deviation title on disk |
| 10 and 11 Database | 15 pass on the live project | **Fail** (3 of 11 bar items) | Any anonymous visitor could hide the whole index (40 artists in 3.8 seconds), and the purge then deleted and blocked them for good |
| 1b Crawl | 446 pass | Still probing | |

**What the critics found that the builders' own tests missed**
- Discover: a site removal did not bar the artist's handle, so the artist came back on the next run with a new website. An email address inside a website URL was stored. The 1-second wait between requests had no test: removing it passed all 64.
- Database: a purged artist got back in through a trailing space, a non-breaking space, or a trailing dot in the URL, or by having no handle and no site at all. `artists.links` accepted a 220 KB `data:` image and served it publicly, which breaks hard rule 1. A 20,000-word query returned HTTP 500.
- Both builders ran their own mutation checks first (31 and 4 deliberate rule breaks, all caught). The critics still found real failures, which is the case for a separate critic.

**What held under attack**
- Discover: no duplicates across case variants, backoff and token refresh, mature works never counted, credentials never in a URL or in output.
- Database: the public key cannot read or write any table; hidden artists never appeared in about 200 calls; search and similar-style functions survived hostile input.

**Measured at full scale** (3,000 synthetic artists, 30,000 works, loaded into the real tables and then deleted): 92 to 99 MB of the 400 MB budget. Search 20 to 50 ms, similar-style 0.44 to 0.49 s against a 3 s public limit.

**Now:** both failed pieces are in round 2 with the critic's gap as the task. The removal flow changes to owner-reviewed purges, a reject path, and a daily cap on automatic hiding.

**DeviantArt:** credentials verified (token issued, validity check passed). No content has been requested.

**Database:** 27.9 MB of 400 MB (empty tables; the extra is index space left by the full-scale test, which is reused). Artists 0, works 0.
