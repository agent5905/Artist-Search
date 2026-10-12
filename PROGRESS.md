# Artist Search: Progress

**Status, 2026-10-12 (night):** Real artwork is on disk for the first time: 5 artists, 50 works, 50 cached images, collected through the API. The analysis pieces have started on them (tags and descriptions, style vectors). Discovery is rerunning live so every niche candidate is stored. Collect is with its first critic.

## Waiting on the owner

Nothing is blocking. One optional item: the draft note to DeviantArt in `docs/` can still be sent as a courtesy question.

The first public deployment will be asked for separately when the site reaches its preview stage.

## Owner decisions (these amend the spec)

Made 2026-10-11, after the DeviantArt terms were read.

1. **Path B.** The DeviantArt API is used only to find artists in the niche and their public links. No DeviantArt image or text goes into any model or in front of any agent. Analysis runs on artists' own portfolio sites.
2. **New hard rule: honor AI opt-outs everywhere.** A site or page is skipped when robots.txt disallows us, when it blocks known AI crawlers, or when it carries `noai`, `noimageai`, `noindex` or a text-and-data-mining reservation. Only custom-domain sites are crawled; hosted platforms and social networks are never fetched.
3. **Reference for pieces 5 and 6 (default, pending the owner).** DeviantArt results cannot be shown to a critic. The blind A/B reference becomes a plain keyword search (text) and a generic image-embedding ranking (style) over the same index, and the critic also grades every result on its own against the artist's real work. This is a weaker bar than the spec's. It is the strongest one the terms allow.
4. **Push `main` after every round.**
5. **Hybrid corpus (2026-10-12).** The owner requires at least 2,000 artists. Every artist comes from the DeviantArt API and is indexed without AI: the artist's own tags, titles, tagline and stated specialty, searched by keyword, with "similar" artists found by tag overlap and thumbnails embedded from DeviantArt with attribution and a link back. AI analysis (vision tags, style vectors, written profiles) runs only for artists whose own site passes the ownership and opt-out checks. No DeviantArt content goes into a model, an agent's context, or a log an agent reads: pipeline output stays counts only, and builders and critics work with synthetic data. This replaces spec section 4.2 for most artists.
6. **Verified removal (2026-10-12).** An artist who proves control of their site or DeviantArt profile, by adding a short code that is then checked, is removed at once and never re-added. A request without proof is logged for the owner and changes nothing until the owner acts. This replaces "any request hides at once", which failed its critic twice on abuse.
7. **Full AI pipeline on DeviantArt works (2026-10-12).** The lead read Terms of Service section 24 as barring any use of labeled content as input to a trained model, which covers the Analyze step, and stopped to ask. The owner reads the section as aimed at training and image generation, neither of which this project does, and accepts the risk of going ahead. So: DeviantArt works are tagged, described and embedded by local models, profiles are written from them, and critics may view them. This supersedes decisions 1 and 5 and the "no DeviantArt content near a model" part of decision 2. Still in force: no training, no generation, no rehosting, local image cache only, verified removal, rate limits, and AI opt-outs honored on artists' own sites.
8. **A/B references (2026-10-12, default).** With critics able to view DeviantArt content, pieces 5 and 6 return to the spec's bars: DeviantArt's own search and "more like this". The reference is captured from the public website, not through the API, because the API License bars using the API for benchmarking. This supersedes decision 3.
9. **Cloudflare (2026-10-12).** The owner replaced the API token; it verifies, and Pages, Workers and Workers AI are reachable. The Pages project name `artist-search` is approved. Approving the name is not approval to deploy.

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

### Round 2: crawler and database (2026-10-12)

Fresh critics, same bars. Both pieces failed again, on narrower gaps.

| Piece | Tests | Critic | Bar items passed | Biggest gap |
|---|---|---|---|---|
| 1b Crawl | 1,277 pass (was 446) | **Fail** | 4 of 8 (was 2 of 8) | A platform still counts as an "own site" when the profile sits after a `#`, and any platform missing from the list can be fetched directly |
| 10 and 11 Database | 63 pass (was 15) | **Fail** | 5 of 11 (was 8 of 11, against easier probes) | With the daily cap used up, a real artist's removal request returns "ok" but hides nothing and is not logged |
| 1a Discover | 269 pass (was 64) | With its critic | | |

**Gaps that survived two rounds, and the change of approach** (spec section 5: write down what was tried, then try something different)

- **Crawler, "platforms are never requested".**
  - Round 1 tried a hand-written list of platforms. The critic fetched 115 platforms that were not on it.
  - Round 2 tried a shape test (bare custom domain, no path, no subdomain) plus a longer list. The critic got through with a `#` route and with country variants of listed names.
  - Round 3 stops relying on lists: no request can be made unless it is anchored to a site that passed the own-site test, each site has a request budget, and the seeding step drops any domain that more than one artist lists.
- **Database, "the removal flow cannot be abused".**
  - Round 1 tried instant hiding for anyone. One visitor hid 40 artists in 3.8 seconds.
  - Round 2 tried a cap of 20 a day. A visitor can use it up, and real requests are then silently dropped.
  - A cap is the wrong tool: it either lets a prankster hide artists or lets them block real requests. The fix needs proof of who is asking, which changes the bar, so it is with the owner (item 1 above).
- **Database, "similar style answers in time".** Round 2 passed when quiet but failed for about 25 seconds after every data reload. Round 3 ranks artists by one stored style vector first, then re-ranks a shortlist, so it no longer scans every work.

**Other findings now with the builders**
- Database: titles, descriptions and profiles accepted a 200 KB embedded image (hard rule 1). A purged artist got back in 5 ways out of 355 tried. A filter list of 100,000 values caused an error and slowed other visitors.
- Crawler: one robots.txt precedence case with non-ASCII paths; the address check and the connection resolved a name separately; the crawler kept requesting a site after it answered "slow down".

**What held under heavier attack**
- Crawler: the 3-second gap across 27 mixed requests, 38,028 opted-out markup variants with none missed, 71 URL tricks and 47 redirect targets with no off-site request.
- Database: the public key cannot touch a table, no hidden artist in 494 search combinations, 102 to 105 MB at full scale.

**Database:** 11.7 MB of 400 MB. Artists 0, works 0.

### Round 3: crawler and discovery; first live run (2026-10-12)

| Piece | Tests | Critic | Bar items passed | Biggest gap |
|---|---|---|---|---|
| 1b Crawl | 2,229 pass | **Fail** | 6 of 8 (was 4, was 2) | A platform's bare home page still passes as an own site: 281 of 308 real platforms tried, 58 pages of one fetched before the budget stopped it |
| 1a Discover | 352 pass | **Fail** | 6 of 9 (same six as round 2, confirmed by a second critic) | A domain four artists listed was still handed to the crawler, because profile-shaped links were dropped before they could be counted |
| 10 and 11 Database | | Round 3 with its builder | | |

**Crawler, third change of approach on "platforms are never requested".** A list (round 1), a shape test (round 2) and anchoring fetches to an own-site root with a 60-request budget (round 3) each failed, because none can tell a platform's home page from an artist's site. Round 4 requires proof: only the front page may be fetched until it is seen to link back to the same artist's DeviantArt profile. This also stops an account from listing someone else's site as its own. Cost: artists whose sites do not link back are left out, and one front-page fetch of an unknown site is unavoidable.

**Discovery, what passed twice:** no DeviantArt content stored, printed or logged (a 300,000-string fuzz in round 2, 49 hostile fields and zero log records in round 3); no private data; no duplicates; 1.0 s minimum gap in real time; mature works never counted; endpoints match the documentation.

**First live discovery run.** Run from the exact code the critic judged, because the six bar items that govern what a run fetches and stores had passed twice and the removal list was empty. Output is counts only.

| Count | Value |
|---|---|
| Browse pages fetched (6 tags, 40 pages each) | 240 |
| Deviations counted | 9,836 |
| Authors seen | 7,791 |
| Candidates (3 or more works under the niche tags) | 396 |
| Profiles looked up | 396 |
| No own-site link (no website, a platform link, or a link with a path or subdomain) | 336 |
| **Artists stored (handle and own-site front page)** | **60** |
| Rate-limit responses | 0 |
| Run time | 12.5 minutes |

**Yield.** 60 of 396 candidates (15%) list a bare custom domain. Still to apply, unmeasured: the link back to DeviantArt, AI opt-outs, the shared-domain rule, and whether the site serves its own images (most custom-domain portfolios are built on Squarespace, Wix or similar, whose image hosts are refused pending the owner). Deeper paging and more tags can raise the candidate count many times over, since the API allows 1,000 pages per tag and this run used 40. Even so, if the remaining filters keep one site in five, 2,000 artists would need on the order of 65,000 candidates.

**Pipeline counts:** artists discovered 60, sites crawled 0, works 0. **Database:** 11.7 MB of 400 MB, artists 0, works 0.

### Round 4: discovery, crawler, database; first collected works (2026-10-12)

| Piece | Tests | Critic | Bar items passed | Biggest gap |
|---|---|---|---|---|
| 1a Discover | 429 pass | **Fail** | 6 of 9 (the same six, for the third time) | Site names with non-ASCII letters (a German sharp s, a Greek final sigma, Thai, Hindi) are read differently by the removal check than by the link check, so a removed site written that way is not recognised |
| 1b Crawl | 2,519 pass | Deferred | | Built with proof of ownership by link-back. Its critic waits until portfolio-site crawling is scheduled, since nothing depends on it now |
| 10 and 11 Database | 108 pass on the live project | Queued | | Round 4 built: verified removal, schema for DeviantArt works, misspelling fallback |
| 1c Collect (new) | 350 pass | With its first critic | | |

**Discovery, the remaining gap is narrow.** Everything that governs what a live run fetches and stores has passed three independent critics. The failures are confined to internationalized domain names in the removal and shared-domain checks. The critic also showed a removal can spread: removing one artist bars every domain their profile lists and everyone else who lists it, so an account could name a victim's site and then remove itself. Round 5 fixes both. The second by changing the rule to match the owner's verified-removal decision: a removal proven through a DeviantArt profile bars that handle, and a removal proven through a site bars that site and everyone who lists it. One no longer implies the other.

**Second live discovery run (stopped early, restarted).** It stopped on a local database lock after 245 of 405 candidates, most likely because another stage was writing to the same file. The partial counts answer the yield question:

| Website field of 245 stored artists | Artists | Share |
|---|---|---|
| Their own bare domain | 37 | 15% |
| None given | 96 | 39% |
| A listed platform or DeviantArt itself | 106 | 43% |
| Another domain, not a bare front page | 6 | 2% |

The strict own-site rule costs about 2%. Most artists simply have no site of their own, which is why the own-site route could not reach 2,000 and why the owner's decision 7 was needed.

**Collect, first live run (5 artists).**

| Count | Value |
|---|---|
| Artists processed | 5 |
| Works stored | 50, each with a title, a publish time and 2 to 18 of the artist's own tags |
| Images cached locally | 50, all JPEG, 32 to 196 KB, long side 894 to 1,414 px |
| Works skipped as mature or unrated | 4 |
| Works skipped as AI-tagged | 0 |
| Run time | 2 min 9 s, about 25 s per artist |

- Live responses matched the documentation; no code change was needed.
- Thumbnail URLs are signed and carry no expiry, so they can be embedded on the site.
- None of the 5 profiles had bio text. If that holds at scale, written profiles rest on tagline, specialty and the works themselves.
- At 25 seconds per artist, 2,500 artists is about 17 hours of collection. Fine as an overnight run; worth speeding up before scale.

**Database, round 4 (builder's measurements, not yet judged).** 158 MB of 400 MB with 3,000 analyzed artists and 30,000 works; 248 MB right after every row is rewritten, before cleanup. Warm calls 0.11 to 0.4 s as the public. The first search with an embedding right after a full rewrite took 3.27 s against a 3 s limit: the one known weak spot.

**Environment.** PyTorch 2.14.1 with CUDA runs on the RTX 5090. The Cloudflare token now verifies, and both shortlisted text-embedding models exist on Workers AI.

**Pipeline counts:** artists discovered 245 and rising, artists collected 5, works 50, images cached 50. **Database:** 12.6 MB of 400 MB, artists 0, works 0.
