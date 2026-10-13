# Artist Search: Progress

**Status, 2026-10-13, evening:** Scaling is paused on the API side: DeviantArt rate-limited our client after a day at one request a second, and our pacing was wrong for its adaptive limit. No API requests are being made while that clears, and the client is being rebuilt to slow down when asked. Push stays closed until a critic passes it; two of its three fix rounds are in. Style lost to DeviantArt's "More Like This", 11 seeds to 17. Profiles changed approach in round 6 and await their critic. The GPU work continues and can be paused. 119 artists are public; nothing is deployed.

## Waiting on the owner

Nothing is blocking. Three things to weigh when convenient; each has a default in place:

1. **When is collect good enough?** It has failed seven critics, each on narrower cases, and the seventh said it would run this code on the real data. Default: fix that critic's three findings, then run and scale without another full critic round, and record the remaining limits (Round 10).
2. **How strict should the content filter be?** Default in place: works rated suggestive or explicit are held back, and an artist is held back when 30% or more of their works are. The profile critic still found 2 of 20 artists it judged unsuitable for a studio-facing site, so the count is being tightened to include sexualised framing and dress.
3. **Brief slow-downs during a data load.** Default: the site retries a failed call once, and loads run at quiet hours. The last three test runs had no failed visitor calls.

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
10. **GPU time (2026-10-13).** Several days of full GPU use for tagging is fine. The owner needs to be able to pause the work, see its status and start it again, to use the PC in between. Built the same day: `scripts/gpu.py` with `status`, `pause`, `resume`, `start` and `stop`. Tried for real: a pause took the GPU from 23 GB in use to 4 GB within 25 seconds and held; a resume carried on with the same work.

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
- **`eval/seeds.jsonl`, 30 seed artists. Frozen 2026-10-12, SHA-256 `a0943b593a284174d262105af5da111acd2fd78008642152c68b0567fbc953e3`.** Drawn at random with a fixed seed (20261013) from the 437 collected artists with at least 6 works, not hand-picked. Stored as hashes of the artist key, so no name sits in the repo's history and a removed artist leaves nothing behind there.
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

### Round 5: first blind A/B tests; collect's first critic (2026-10-12)

**Piece 6, style similarity: two blind tests on real artwork.** For each of 12 seed artists drawn at random, the critic saw three contact sheets: the seed's works, and two competing sets of the four nearest artists, labeled A and B in an order shuffled per seed. The critic judged style only (rendering, line, palette, finish), not subject. The answer key was kept where the critic was not told to look. The sheet maker is `eval/style_sheets.py`; the sheets themselves stay in the git-ignored cache.

| Test | Contender | Seeds won | Rows judged a real style match (of 48) |
|---|---|---|---|
| 1. Which model | CSD (trained for style) | **7** | **23** |
| | SigLIP2 (general image model) | 4, with 1 tie | 19 |
| 2. Which scoring rule, both on CSD | Two-way ("mutual") | **8** | **29** |
| | One-way ("best"), the original | 3, with 1 tie | 21 |

- **Test 1 verdict:** the losing sets "matched genre, not finish": photoreal matte painting offered for a loose painterly seed, glossy saturated card art for a muted one. The critic also named what hurt both: artists with mixed portfolios, and a small pool.
- **What changed between the tests:** the original rule scored a candidate by how well the seed's works found a match among the candidate's. An artist with a bit of everything could rank first on one or two works. The new rule also asks how well the candidate's works match the seed's.
- **Test 2 verdict:** the two-way rule won clearly, and its most common remaining failures are a mixed-portfolio artist still taking a top-three slot, and the best match being present but not ranked first. The same few artists recur across unrelated seeds.
- **Adopted:** CSD with the two-way rule. The database's similar-style function will follow.
- **Still short of the bar:** 29 of 48 rows is 60%. Next: correct for artists that show up everywhere, and rerun on a larger pool. Then the spec's bar, against DeviantArt's own "more like this".

**Unsuitable content.** The first critic flagged about ten cases at thumbnail size (sexualised character sheets, pin-ups, one moderately gory image) and one artist whose four images were blurred placeholders. DeviantArt's "not mature" flag is not strict enough for a studio-facing site. The tagging stage's "explicit" flag will exclude such works, and will flag blurred placeholders.

**Piece 1c, collect: first critic, fail, 4 of 9 bar items.**

| Passed | Failed |
|---|---|
| No duplicates across three runs | AI-tagged works stored: 22 in the real cache, two artists with 10 of 10 |
| 1.00 s minimum between API requests; at most 2 image downloads a second | Obscured emails and phone numbers got through text scrubbing (made-up data; the real cache was clean) |
| Output is counts only | A tiny image written as text was stored in a text field (made-up data) |
| Requests match the documentation | A removed artist's images could survive a lost database, a renamed account, or a removal landing mid-run |

- **Biggest gap:** the AI-art filter knew 11 tag prefixes and missed plain spellings. Round 2 widens it and the rerun purges what got in.
- **Change of approach on free text:** scrubbing pieces out of text does not work. The bio field is removed (empty in all 41 real profiles), and a tagline is dropped whole if it contains an `@`, a messaging link or a phone-length run of digits.
- **A correction to the lead's own bar:** the critic was told that no country, age or location may appear even inside a stored field. That is stricter than spec rule 6, which allows an artist's public bio and bars emails and phone numbers. The next round words the bar as rule 6 does.

**Collection so far.** A full run stopped a third of the way through on a database constraint error (cause being found by the builder). Before stopping: 151 artists processed, 143 profiles, 1,420 works with images; 146 works and 7 whole artists skipped as AI-tagged, 82 works as mature or unrated. AI-tagged art is common in this niche, which raises how many candidates 2,000 artists will need.

**Discovery.** A clean third run stored 378 candidates; the store holds 472 artists across runs. 15% list their own bare domain, 39% no website, 45% a platform, 2% another domain that is not a bare front page.

**Pipeline counts:** artists discovered 472, artists collected 148, works 1,470, images cached 1,470, style vectors 1,470 per model. **Database:** 12.6 MB of 400 MB, artists 0, works 0.

### Round 6: first full collection; blind tagging test; collect's second critic (2026-10-12)

**First full collection (collect round 2 code), exit 0 after about 2 hours 50 minutes.**

| Count | Value |
|---|---|
| Artists processed | 472 |
| Artists stored with a profile and works | 442 |
| Works stored, each with a cached image | 4,381 |
| Artists skipped as mostly AI-tagged | 30 |
| Works skipped as AI-tagged | 741 |
| Works skipped as mature or unrated | 174 |
| Works skipped as not a public image | 118 |
| Malformed entries | 6 |
| Download failures, refused rows, unexpected errors | 0 |

- About 1 artist in 16 is skipped as mostly AI-tagged, and 14% of otherwise eligible works carry an AI tag. For 2,500 indexed artists, plan on roughly 2,700 candidates before the stricter AI check in the analysis stage removes more.
- The run that stopped earlier was not a duplicate work, as the lead had guessed. The builder traced it: its own unfinished change to the profiles table reached the live database through another stage's process while the old collector was writing. Fixed: opening the store never changes a table, and inserts name their columns. Lesson recorded for the lead: no schema change to the shared local store while a run is in progress.

**Piece 2 and 3, tags and descriptions: first blind test.** 30 random works; for each, the image and both local vision models' results, labeled A and B in an order shuffled per work. Bar: any unsupported claim is a loss.

| | qwen3.8 | gemma4:26b |
|---|---|---|
| Works won | **17** | 11, with 2 ties |
| Works with at least one unsupported claim | 15 of 30 | 15 of 30 |
| Unsupported claims in total | 21 | 17 |
| Important things left out | 7 | 34 |
| Seconds per image | 2.28 | 1.63 |

- **Verdict: both fail the bar.** Half the works carry an unsupported claim under each model. qwen3.8 says far more and gets slightly more wrong; gemma leaves out most of what matters. qwen3.8 is the base from here.
- **Biggest gap:** genres the image gives no sign of ("anime style" on western-style dragon heads; "fantasy" on a plain forest landscape and an age-of-sail ship). Then guessed details in descriptions (a sword that is not there), subjects over-tagged ("prop or weapon" for something a character holds), and illustrated cards with a game-style overlay flagged "not artwork".
- **Round 2, a different approach rather than a reworded prompt:** a verification pass. Each proposed genre and subject is asked about directly and kept only if confirmed, in two variants: the model checking itself, and the more cautious second model checking it.

**Piece 1c, collect: second critic, fail, 5 of 10 (was 4 of 9).**
- **Passed:** no duplicates; counts-only output; documented requests; opening the store never changes a table; all 66 rule breaks the critic tried on the built code were caught by tests.
- **The gap that came back a third time: AI-tagged art.** Round 1 knew 11 tag prefixes. Round 2 was wider and still missed "synthography" (11 stored works, one artist's whole set) and read any tag containing "free" as a protest. A list of spellings cannot be closed, so the decision moves to the analysis stage, where a local language model reads each work's own tags and title and answers one question: do these declare the work AI-made?
- **The failure that matters most at scale:** one artist whose profile returns a server error stopped the whole run, and every later run at the same artist. Round 3 retries, then skips that artist and moves them to the back.
- **Narrow, found with made-up data, real cache clean:** a dropped title surviving inside the page address; messaging links kept as a website (now an allow-list of portfolio hosts instead of a deny-list); an artist removed and then renamed being collected again (the removal list now remembers the account's hashed user id).

**Pipeline counts:** artists discovered 472, artists collected 442, works 4,381, images cached 4,381, tagged 500 per model, style vectors being computed for all. **Database:** 12.6 MB of 400 MB, artists 0, works 0.

### Round 7: verification passes, first profiles, style correction (2026-10-12)

**Tags and descriptions: second blind test** (30 fresh works).

| | Round 1, one pass | Round 2, checked by the same model | Round 2, checked by the second model |
|---|---|---|---|
| Works with at least one unsupported claim | 15 of 30 | **11 of 30** | 13 of 30 |
| Unsupported claims | 21 | **13** | 16 |
| Of those, in genres | 8 | **2** | 4 |
| Of those, in the description | 4 | 9 | 9 |

- **What changed:** a verification pass. Each proposed subject and genre is asked about on its own with the image and kept only if confirmed. Drawing manner needed a different check: asked "is this anime style?", both models said yes to a sheet of dragon heads, so the verifier picks a manner itself and the tag survives only if the two agree.
- **Verdict:** tags are mostly right now. Descriptions became the biggest gap (an axe called a mace "in its right hand").
- **Round 3** applies the same approach to descriptions: a pass that may only remove or generalise a detail, never add one. A third blind test is running. Cost: about 8 seconds a work.

**Deciding with a model instead of a list.** Two rules failed their critics three and four rounds running as lists of spellings: "is this work declared AI-made?" and "does this text contain contact details?". Both moved to the tagging stage, where the local language model reads the artist's own title and tags. First numbers on 500 works: 7 declared AI-made, of which the tag list catches 5.

**Style similarity: third blind test** (12 frozen seed artists, pool of 442).

| | Two-way rule | With a correction for artists near everyone |
|---|---|---|
| Seeds won | 4 | **7**, with 1 tie |
| Rows judged a real style match, of 48 | 35 | **37** |
| Mixed-portfolio rows, of 48 | **4** | 8 |

The correction is adopted. The larger gain came from the pool: real matches rose from 29 of 48 at 148 artists to 35 and 37 at 442.

**Artist profiles: built, first real output.** 55 artists profiled from verified tags counted across each artist's works; 6 say in their own words that they are open for work. The local embedding model and Cloudflare's copy agree on the 36 eval queries (cosine 0.99999, same nearest neighbour 36 of 36). A blind critic is reading 20 profiles next to the artists' works.

**Collect: third critic, fail, 4 of 10; round 4 built.**
- **Caught before it touched real data:** if DeviantArt refused every profile lookup, the second run would have deleted the whole local library and reported success. Now a refusal counts as the API failing, five in a row stop the run, and an account is purged only on a documented "user is gone" answer on two healthy runs.
- **The AI rule failed in both directions,** purging artists whose works carry anti-AI protest tags. All pattern matching is deleted; collect keeps an exact list of 20 tags.

**Database: fourth critic, fail, 4 of 11; round 5 built.**
- **The lead's own mistake:** "keep the newest five requests" let anonymous calls push out a real artist's request, and "asking again replaces the proof code" let one anonymous call cancel a real removal. Fixed: the code is derived from the slug and a secret, so it cannot be replaced, and a request never deletes anything.
- The links field is gone; loads are incremental in batches of 10 artists. Builder's full-scale numbers: 172 MB loaded, 200 MB peak; one of two runs had 5 timeouts in 1,812 visitor calls during a load.

**A scheduling mistake by the lead:** the full tagging run was started on round 2 code while round 3 was changing the prompt version. It was stopped and restarted on round 3, with one artist's works tagged together so profiles can start early.

**Pipeline counts:** artists discovered 472, collected 442, works 4,381, images cached 4,381, style vectors 4,381, works with checked tags 600 and rising, artists profiled 55. **Database:** 12.1 MB of 400 MB, artists 0, works 0.

### Round 8: first real data in the public database (2026-10-12)

**End to end.** Discover, collect, style vectors, tags, profiles, push, and the database's public functions have now all run on real data.

| In the public database | Count |
|---|---|
| Artists | 119 |
| Works (source links and thumbnail URLs only) | 1,130 |
| Similar-style neighbour links | 2,856 (24 per artist) |
| Artists with a search embedding | 119 |
| Artists whose own words say open for work | 11 |
| Database size | 16.8 MB of 400 MB |

Held back by the push stage's rules: 300 artists not yet analysed, 21 with adult-oriented portfolios (30% or more of rated works suggestive or explicit), 169 works rated suggestive, 13 declared AI-made, 12 not artwork.

**First real search results, through the public key** (profile lines as served):
- "dark fantasy creature designer": "Monster art, mostly creature designs, concept sketches, and creature sheets, with fantasy and dark fantasy themes"; "Fantasy creature and character art, including concept sketches and dragons."
- "sci-fi environment concept artist": "Environments with science fiction themes, including keyframes or scenes and buildings."
- "concpet artist charcter desing" (misspelled on purpose): three character artists.

**Tags and descriptions: third blind test.** Works with an unsupported claim: 15, then 11, then **7 of 30**. Genres and subjects: 12 unsupported claims, then 3, then **0**. Six of the seven remaining errors are a thing misnamed in the description ("dark scales" on a shaggy beast). A two-model agreed description removes about half of those on development works but costs 5 seconds a work; it is built and not run at scale by default.

**Profiles: two blind tests.**

| | Round 1 | Round 2 |
|---|---|---|
| Artists with an unsupported claim | 2 of 20 | 3 of 20 |
| Critic's verdict on the set | "Too generic to choose by: 19 of 20 say digital painting" | "Only into about six buckets, not within them" |

- Round 2 added specifics from a per-work format pick and the artist's own tags, and those brought new errors ("mostly elves" at 3 of 10 works; a card format that was not there).
- Round 3 changes the source: the model looks at one sheet of the artist's works, proposes what recurs, and a second question makes it count. A claim is stated only at the share its wording implies. On 140 artists the count removed 37 of 96 proposed phrases, 42 own-tag words and 26 formats. Not yet judged.

**Database, round 6: a simplification.** Similar-style results are computed on the owner's machine with the rule the blind tests chose, and the database stores each artist's 24 nearest. Style vectors left the database.

| Full scale (3,000 artists, 30,000 works) | Round 5 | Round 6 |
|---|---|---|
| Size loaded | 172 MB | 88 MB |
| Peak during a reload | 200 MB | 103 MB |
| Visitor calls failing during a reload | 5 of 1,812 in one run | 0 of 4,016 over three runs |

**Push: built with two safety stops.** It writes nothing if more than 10% of the artists already in the database would be deleted, and it refuses to run while test rows are present. Both came from the builder's own review of what a half-finished analysis run would do.

**A process error by the lead, corrected.** Two commits went in after combined test runs that had failures, because the commit command checked the last line of output, not the test result. Both sets of failures were in another builder's in-progress files. Commit commands now stop on a failing run.

**Pipeline counts:** artists discovered 472, collected 442, works 4,381, works with checked tags about 1,500 and rising, artists profiled 140, artists public 119. **Database:** 16.8 MB of 400 MB.

### Round 9: the site, and its first comparison (2026-10-12)

**Pieces 7 to 9, site pages: built.** Search, similar style, artist profile and a claim-or-remove flow, in React on Cloudflare Pages' stack, running locally against the real database. A search function embeds the query on Workers AI and calls the database, falls back to keywords if the embedding fails, and retries a failed call once. Thumbnails load straight from the source's image host; nothing is proxied or stored. Screenshots stay in the git-ignored cache and were sent to the owner.

**First comparison against the spec's bar.** Eight pairs: each of our pages against the same page type at the same width (1280 and 390) on ArtStation or Behance, A and B shuffled per pair. The critic judged first screens as someone at a studio trying to find and choose an artist to hire.

| Against | Our pages won | Lost |
|---|---|---|
| ArtStation | 3 of 4 | Desktop profile |
| Behance | 4 of 4 | None |

- **Why ours won, in the critic's words:** "results are artists, not artworks. Each card answers 'who is this and what do they do' in one plain sentence plus tags, with four samples and two obvious actions." The platforms' search pages return anonymous artwork tiles, with blurred mature-content placeholders on one and an ad tile on the other.
- **The loss and the biggest gap:** ArtStation's desktop profile has a complete hiring card (open status, role, location, Message). Ours "gives a hirer no way to tell whether the artist is available or how to reach them", its one filled button leads away from the artist, and the claim-or-remove box sits among the actions.
- **Honest limits of this test:** it is not truly blind, because each platform's logo is visible; it covers first screens only; and it judges pages for our job, which those platforms' search pages were not designed for.
- **Round 2 for the site:** contacting the artist becomes the primary action (their own site, otherwise their DeviantArt profile), availability is stated or marked "not stated", and claim-or-remove stays visible but leaves the action column. Phone fixes: a cue that filters scroll, tighter cards, works higher on the profile.
- **Needs the data stages:** thumbnails are 300 pixels wide at the source, so they are soft on dense screens, and works are cropped to a fixed tile because the database holds no image dimensions.

**Pipeline counts:** artists discovered 472, collected 442, works 4,381, artists public 119, works public 1,130. **Database:** 16.8 MB of 400 MB.

### Round 10: collect through four more critics; every work tagged; the site's second comparison (2026-10-13)

**A correction to this log.** Rounds 5 to 9 were dated up to two days late. Their headings now match the commit times: all five were on 2026-10-12.

**Collect and store: critics 4 to 7.** All four failed, each on narrower cases than the one before. The approach changed in round 5, as the loop requires when a gap survives: the stage stopped trying to guard its deletions and stopped deleting.

| Round | What changed |
|---|---|
| 5 | Nothing is deleted on what the API says. A work the API reports as mature, AI-tagged or carrying contact details, and an artist it calls gone or renamed, is marked with a reason and held back from publishing. Only the removal list and stray image files delete |
| 6 | A stored account id is never replaced. Stored work fields are kept when a later answer is worse |
| 7 | A removal no longer travels to another artist. The run stops when the image host refuses everywhere |

**The seventh critic's numbers.** 660 sequences, about 3,300 runs and 1,900 removals, with wrong account ids for stored artists and every kind of failed answer:
- nothing of a non-removed artist was lost;
- no removal reached anyone else;
- every removed artist left no row and no file;
- the tests caught 76 of 77 deliberate breaks.

Asked directly whether it would run this code on the real data, for the 442 artists and for several thousand new ones: "Yes. It cannot lose what is kept."

**What still failed, and round 8 (in progress)**
- An image host answering 403 to everything never stopped the run, which stored artists with no works. An expired signed address would look the same at scale. Being fixed.
- A site removal could miss an artist who added their own site after discovery first saw them. Being fixed.
- A value our own rules refuse blanked the stored one, where a work's title in the same case is marked and kept. Being fixed.
- A phone number typed across several tags would be stored. No real work has this. Recorded, not fixed.

**Known limits, recorded and not fixed**
- A rename and a removal landing between two runs.
- A wrong account id on an artist's very first collection could link two artists, so a later removal takes both. It needs the API to return a false id; the critic rated it very unlikely.
- Site names with non-ASCII letters such as "ß" are refused loudly in removal matching.
- Any removal clears every review sheet in the local cache.

**Tags: complete.** 4,381 of 4,381 works have checked tags, a format and a content rating; none failed. The run took about 9 seconds a work.

**Profiles: critics 3 and 4** (20 fresh artists each)

| | Round 1 | Round 2 | Round 3 | Round 4 |
|---|---|---|---|---|
| Artists with an unsupported claim | 2 | 3 | 3 | 2 |
| Cards judged useful | Not scored | 15 | 18 | 14 |
| Groups a studio could tell apart | Barely any | About 6 | About 12 | About 10 |

- Both round 4 errors came from a claim that skipped the count on the sheet: a fallback subject stated when every counted one was rejected, and a genre taken from per-work tags. Every claim that went through the count held up.
- Usefulness fell because the stricter 40% rule left 6 of 20 cards saying only "Fantasy character art."
- Round 5 (in progress, fresh builder): no fallback, so an artist with nothing that passes gets no profile; genres are counted too; the drawing manner and kind of output are named when the works show them; a check that each line reads as a sentence; a stricter suitability count.

**Push, rounds 3 and 4.** It holds back whatever collect marks, applies the artist-level suitability count, and sends image sizes so the site can stop cropping. Not yet judged.

**Database, round 7.** A result card leads with the works that matched the query. Not yet judged.

**Site, round 2: second comparison.** Contacting the artist is now the primary action, availability is stated or shown as "not stated", and claim-or-remove sits apart from the actions. Result: 7 of 8 pairs again. The one loss is the same, ArtStation's desktop profile with its hiring card. Hard rule 6 limits what our profile can show (no location, no direct message), so that pair may be the ceiling. Same caveats as before: logos are visible, and first screens only.

**References for the text-search test.** DeviantArt's own search results are captured from the public website for all 36 frozen queries. Our side of each pair waits on the larger push.

**Deferred, to stay on the critical path:** critics for the crawler (round 4), push, database rounds 6 and 7, and the tagging format pass; discovery's non-ASCII fix; a rate limit on the search endpoint; the removal-proof checker.

**Process.** The usage limit cut agents off five times; each was resumed against its task list. A builder that has run about four rounds is now replaced by a fresh one with a handover note.

**Pipeline counts:** artists discovered 472, collected 442, works 4,381, works with checked tags 4,381, artists public 119, works public 1,130. **Database:** 16.8 MB of 400 MB.

### Round 11: the style test against DeviantArt's "More Like This" (2026-10-13)

**Piece 6, the spec's own bar, run for the first time.** Lost, twice.

| | Round 1 | Round 2 |
|---|---|---|
| Seeds our side won | 8 | 11 |
| Seeds DeviantArt won | 21 | 17 |
| Ties | 0 | 1 |
| Our works judged a real style match | 192 of 459 (42%) | 255 of 459 (56%) |
| DeviantArt's works judged a real style match | 270 of 459 (59%) | 264 of 459 (58%) |

**How it was run**
- **Reference:** DeviantArt's public More Like This page for each frozen seed artist's two newest works, taking turns, keeping the first 16 works by other artists. Read from the website with our crawler's name, one request every three seconds; robots.txt allows these pages. The API was not used, because its license bars benchmarking through it. 29 of the 30 seeds had a reference; for one, DeviantArt returned nothing by other artists.
- **Blind:** both sides are 16 works in the same shuffled grid, A and B swapped at random per seed. Two fresh critics per round split the seeds. They were told to judge how the work is made, not what it shows.
- **The judges agree with each other.** DeviantArt's side was identical in both rounds, and four different critics scored it 59% and 58%.

**Why we lost round 1, from the critics' notes matched to the key**
- "Unrelated, or a different medium" was our side in 8 of 9 cases, and "right genre, wrong handling" in all 7. Several of our sheets had 1 to 6 matches of 16. With 442 artists, many seeds have nobody close, so the nearest artist is generic or unrelated.
- "Matches the subject, not the style" was DeviantArt's side in all 11 cases. That is how it loses when it loses.
- Our side showed more unsuitable content, 7 sheets to 4, because the test drew from every collected work, including what the push stage holds back.

**Round 2 changed one thing.** For each similar artist we had shown their four newest works. Now we show their four works closest to the seed's. That alone lifted our matches from 42% to 56%, level with DeviantArt's, and turned three seeds. It did not turn the result: a set that is half right still loses to a set that is consistently close.

**What follows**
- **Scale is the fix for the main gap.** The pool goes from 442 to 3,000-plus artists. The test is rerun on the same frozen seeds after that.
- **The closest-works rule goes into the product:** push, the database's similar-style function and the site's page. Today the site shows a similar artist's newest works.
- **The test will draw from published artists and works only,** so it shows what the site shows.
- **A caveat that stays:** DeviantArt's feature answers "more like this work" and ours answers "more like this artist". The spec names it as the bar, so it is the bar.

**A new fact about the "noai" label.** DeviantArt's website marks each work with whether the artist disallows AI use and whether the work is AI-generated. The API does not expose either, which is why the earlier finding said the label could not be read. Nothing changed because of this; owner decision 7 stands. The label could now be checked per work at one page request each, and that would be a new use of the site, so it waits for the owner.

**Collect and store, round 8: committed.** The seventh critic's three findings are fixed (see Round 10), discovery now covers 18 tags at 120 pages each, and both stages retry a server error three times before stopping. 1,708 tests pass. The rerun over the 442 existing artists is in progress as the check on real data.

**Also done:** a first real README, and the progress log's dates corrected.

**Pipeline counts:** artists discovered 472, collected 442, artists public 119, works public 1,130. **Database:** 16.8 MB of 400 MB.

### Round 12: collection checked on real data; profiles' fifth critic; closest works in the product (2026-10-13)

**Collect, round 8, on real data.** The rerun over every discovered artist finished cleanly.

| | Count |
|---|---|
| Artists processed, and profiles stored | 472 of 472 |
| Works stored | 4,618 |
| New images downloaded | 745 |
| Artists purged, files deleted, deletions failed | 0, 0, 0 |
| Artists skipped for any reason | 0 |
| Works skipped as mature or unrated | 183 |
| Works skipped as not a public image | 111 |
| Works skipped for "contact details" | 397 |
| Stored works marked as no longer qualifying | 198 |

Nothing was lost. A consistent backup of the local store was taken first.

**A false positive, found and fixed.** 397 works skipped for contact details was ten times the forecast.
- Of the 198 stored works that were marked, 177 have a clean title and page address.
- A sample of 25 asked of the API showed why: when DeviantArt serves a preview as the file itself, its name is DeviantArt's own id plus a UUID. A UUID has more than nine digits, and the rule read it as a phone number.
- The fix is narrow: a file name in exactly that shape holds nothing the artist wrote, so the title's rule is not asked of it. An id that is itself a long number is still refused. The next collection run clears the wrong marks and stores the works.
- 17 other marks are a second, smaller false positive: a title with a colon in it, such as an emoticon, read as a link. Left as it is, at about 0.3% of works, because that rule is deliberately broad.
- No hard rule was loosened: the rule exists to keep out what an artist typed, and this name is machine-made.

**Profiles: fifth blind test.**

| | R1 | R2 | R3 | R4 | R5 |
|---|---|---|---|---|---|
| Artists with an unsupported claim | 2 | 3 | 3 | 2 | 2, plus 2 borderline |
| Cards judged useful | not scored | 15 | 18 | 14 | 9 |
| Groups a studio could tell apart | few | 6 | 12 | 10 | 13 |
| Cards not fit to print | - | - | - | 4 | 3 |
| Artists judged unsuitable | - | - | - | 2 | 0, plus 1 borderline |

- Round 5 profiled 415 of 442 artists and excluded 27 (17 with no claim that passed the count).
- The stricter suitability count worked: it flags 23 artists, and the critic found none clearly unsuitable.
- The error rate has not moved in five rounds, and 11 of 20 cards are now generic. The two errors: "mostly dark sci-fi cityscapes" where 5 of 10 are cityscapes and 3 are dark; "including concept sketches" on finished colour prop sheets.
- The critic's biggest gap: "the cards never say what kind of output the artist makes." Design sheets, turnarounds, maps and prop sheets go unnamed.
- A bug: every card not fit to print repeats a word, and the check written to catch that missed all of them.

**Profiles change approach (round 6, in progress).** Each round's errors had a new cause, so fixing causes one by one is not converging. Both of this round's errors were counted wrongly on one sheet of ten small thumbnails. Now every specific claim is checked on each work at full size, as the tagging stage does, where genre and subject errors went from 12 to 0. The kind of output becomes the head of the card. Cost: up to about 40 seconds an artist, from 14.

**Profiles no longer vanish while a new work waits for its tags.** The stage deleted an artist's profile whenever collection brought a new work, and 99 went that way during the rerun. With collection about to run for hours, artists would have dropped off the site each time they posted. A waiting artist now keeps the profile they have.

**Closest works, in the product.** Push sends, with each similar artist, their four published works closest in style to the artist being viewed, and the database's similar-style function leads with them. The site needed no change. Measured at full scale: 2 seconds more per push and 2.3 MB. The live database got one added column; all 119 artists return identical answers before and after, and carry no picks until the next push. Not yet judged on its own.

**Running now:** the wide discovery (18 tags, 120 pages each, then about 3,500 profile lookups), tagging of the 650 new works, and profiles round 6. Collection of the new artists follows discovery and will take most of a day.

**Pipeline counts:** artists discovered 472, collected 472, works 5,034, artists public 119, works public 1,130. **Database:** 16.8 MB of 400 MB.

**Added the same day: pausing the GPU work.** The owner confirmed that days of GPU time are fine, provided the work can be paused. `uv run python scripts/gpu.py pause` frees the GPU, `resume` carries on, `status` shows what is done, what is left and the pace, and `start` begins the background worker again after a reboot. Every stage holds before its next request to a model while paused, whoever started it, so nothing is lost. Collection and discovery do not use the GPU and keep running.

### Round 13: the push stage's first critic (2026-10-13)

Push had four unjudged rounds. It is the last gate before the public, so it was judged before the larger push. **Verdict: fail, on five of ten properties, each with a real counterexample.** The critic's answer to "would you run this on 3,000 artists today?": no.

| # | Property | Verdict | Counterexample |
|---|---|---|---|
| 1 | Nothing forbidden is sent | Fail | A tagline was read once and found clean. The artist changed it to name a messaging handle; nothing read it again, and push sent it. A model-written description holding an email also went out unchecked |
| 2 | Held-back works stay back | Fail | An artist replaced a work's image. The new thumbnail was sent under the old image's rating |
| 3 | Held-back artists stay back | Fail | 4 of 10 works rated explicit but not yet tagged: only tagged works were counted, so the share came to 0 and the artist was sent |
| 4 | Removal first, and for good | Fail | A removal whose file deletion hit a locked file was never recorded; the artist stayed public. An account renamed before its removal survived under the new handle |
| 5 | A bad local state cannot wipe public data | Fail | An emptied table deleted every similar-artist row, unguarded. A store with its artist list emptied was read as "everyone removed", during a dry run too, and a real run would have written that to the permanent removal list |
| 6 | A rerun changes nothing | Pass | 200 reruns identical |
| 7 | Similar artists and their picked works are only ones that are sent | Pass | |
| 8 | A dry run writes nothing public | Pass | It does write locally, which it should not |
| 9 | No secret printed | Pass | One error path cuts a message before scrubbing it |
| 10 | The tests catch regressions | Pass | 51 of 52 deliberate breaks caught |

- **How it was tried:** 15 targeted sequences and 40 random ones, 400 runs in all, on invented stores with a stand-in database. No real push and no call to the real database.
- **The critic's biggest gap:** "A reading is not tied to what was read." A model's "no contact details", "not AI", rating and tags are stored once. Collection keeps the artist's words and thumbnails up to date. Push then pairs today's words with yesterday's reading.
- **Why this had not shown yet:** the live database was loaded once, before any second collection. The gap opens with repeated collection, which is exactly what scaling does.
- **The tests' blind spot, in the critic's words:** "Every fixture is read once and never changes afterwards."

**What follows**
- **Push round 5 (in progress):** every rating counts toward an artist's share, and an unrated work counts against; a removal is recorded before any file is deleted and covers every handle of the account; a missing row is never read as a removal; similar-artist deletions are guarded; many removals at once stop the run; a dry run changes nothing anywhere; every text field passes the contact rule at the moment it is sent.
- **Readings tied to what was read (next, after profiles round 6 lands):** each reading carries a fingerprint of the words or image it was made from, and anything whose words or image changed since is held back until it is read again. A replaced image is fetched and read again.
- **No push until both are in and a critic has passed them.** This does not hold up discovery, collection or tagging.

**Discovery, first wide run: stopped by an outage.** After 202 of 2,160 browse pages DeviantArt's API answered 504, and minutes later 503 on the token request. A probe a few minutes on found every depth answering in about a second, so depth was not the cause. Discovery stores nothing until browsing ends, so 45 minutes were lost and nothing else. It now waits out a server error for up to about 16 minutes (retries after 2, 4 and 8 seconds, then 1, 5 and 10 minutes) and has been started again.

**Pipeline counts:** artists discovered 472, collected 472, works 5,126, works tagged about 4,800, artists public 119, works public 1,130. **Database:** 16.8 MB of 400 MB.

### Round 14: rate-limited by DeviantArt; readings tied to what was read; profiles round 6 (2026-10-13)

**The API rate-limited us, and our pacing was at fault.**
- The second wide discovery run crawled for four hours without finishing its browsing or storing one artist. A measurement showed every request answered HTTP 429.
- DeviantArt's documentation says the limit is adaptive: bursts are allowed, a sustained rate is throttled, a 429 is "a request to slow down", and a client that carries on "at the same or even a lower fixed rate" stays limited longer.
- Our client backed off on a 429 for that one request and then returned to one request a second. Collection and discovery ran that way for most of the day. That is the pattern the documentation warns against.
- The lead's own measurement made it slightly worse: 30 requests a second apart, continued after the first 429s. It should have stopped at the first.
- Discovery was stopped. No request is being made to the API while the limit clears.

**What changes (in progress, then a critic, since staying within the API's terms is hard rule 3)**
- Pacing for the whole run: a slower base rate, an interval that doubles on each 429 and comes down only gradually, and a stop if the API keeps refusing for about an hour. One client at a time.
- Discovery no longer looks up each candidate's profile. It did so only for the website field, which collection reads anyway. About 3,500 requests fewer.
- Browsing saves its progress, so a stop loses nothing. Two runs lost all their browsing today.
- Long runs report where they are. For four hours there was no way to tell whether discovery was working, waiting or stuck.

**Effect on the schedule.** About 12,000 requests are needed for 3,000 more artists. At a pace the API accepts that is likely one to two days.

**Readings are now tied to what was read (the push critic's biggest gap).**
- Every reading by a model carries a fingerprint of exactly what it was given: the image bytes, or the words.
- A reading that does not match the current image or words counts as never made. Tagging makes it again, profiles waits, push holds the work or the text back. A replaced image is fetched again and its old readings go stale with it.
- All three of the critic's stale-reading counterexamples now behave correctly. 2,258 tests pass on the merged code.
- The migration measured the real store against the backup from before today's second collection:

| | Count |
|---|---|
| Readings fingerprinted | 35,666 |
| Works whose title had changed since it was read | 16 |
| Works whose tags had changed | 3 |
| Artists whose tagline had changed | 4 |
| Works with a replaced image | 0 |

- None of those reached the public database: nothing has been pushed since the second collection. They are what the next push would have sent under old readings.
- Merging this with profiles round 6 produced two collisions that the text merge did not flag and the type checker did. Both are fixed, and the per-work look now waits when an image changes mid-run.

**Push, round 5: 10 of the critic's 14 counterexamples fixed.** Every rating counts toward an artist's share and an unrated work counts against; a removal is recorded before any file is deleted and covers every handle of the account; a missing row is never read as a removal; many deletions or removals at once stop the run; a dry run changes nothing anywhere; every text field passes the contact rule when it is sent. With the fingerprints, 13 of 14 are fixed. The last is disputed: when ten artists list one removed site, the critic expects nine to stay and an existing store test requires the opposite; push now stops and asks. A fresh critic judges the stage before any push.

**Profiles, round 6: landed, not yet judged.** Claims are counted on each work at full size; the kind of output leads the card ("Character design sheets, mostly fantasy"); the repeated-word bug is fixed; wording follows the share. Of 52 genres or manners that round 5 stated flat on the sheet count, 18 no longer qualify. 21 seconds an artist. The worker is redoing every card; the critic follows.

**GPU control.** `scripts/gpu.py` pauses, resumes and reports (owner decision 10).

**Pipeline counts:** artists discovered 472, collected 472, works 5,126, all tagged, rated and with style vectors; artists public 119. **Database:** 16.8 MB of 400 MB.

### Round 15: profiles' sixth critic, a step back; the pacing rebuild lands (2026-10-13)

**Profiles, round 6: judged, and worse on the bar that matters.**

| | R1 | R2 | R3 | R4 | R5 | R6 |
|---|---|---|---|---|---|---|
| Artists with an unsupported claim | 2 | 3 | 3 | 2 | 2 | 9 |
| Cards judged useful | - | 15 | 18 | 14 | 9 | 14 |
| Groups a studio could tell apart | few | 6 | 12 | 10 | 13 | 12 |
| Cards not fit to print | - | - | - | 4 | 3 | 6 |
| Artists judged unsuitable | - | - | - | 2 | 0 | 1 |

- A caveat: this critic's brief was stricter. It defined a flat statement as about 7 works in 10, where earlier briefs said "most". That accounts for some of the rise, not most.
- **The kind-of-output word was the most frequent error.** "Designs", "design sheets" or "character illustrations" failed on five cards and were marginal on three: "designs" where 2 of 14 works are designs.
- **The per-work check says yes too easily.** "Including dragons" where 2 of 15 works are dragons and 8 or 9 are dinosaurs; "trees" at 2 of 10; "mostly demons" at 3 or 4 of 10. Asked whether a picture shows a dragon, the model agrees for a dinosaur.
- Ten cards understate, such as "mostly dragons" at 14 of 14.
- The lead's expectation was wrong. Checking each work at full size works for the tagging stage's fixed list of tags. It does not work for free-text claims put to the model as yes-or-no questions.

**Round 7 changes direction: say less, and take specifics from the artist.**
- The first sentence states only what the checked per-work tags can count: subject, genre, manner, medium, worded by share. No proposed things and no kind-of-output claim; those two account for nearly all nine errors.
- A second sentence gives the artist's own most-used tags, attributed to them. That is an exact count from data, not a model's judgement, and it names what the critic said the cards miss (dinosaurs, anthropomorphic animals).
- The per-work look is deleted, which takes the stage back to about 14 seconds an artist.
- If this holds at one or two errors in twenty, that is likely the practical ceiling for this model, and "good enough" goes to the owner.

**Collect and discovery, round 9: merged, not yet judged or run.**
- Pace: 2 seconds between API requests. Every 429 or server error doubles the interval for all later requests, up to 5 minutes; each success brings it down by a tenth. A burst of six 429s takes about 19 minutes to recover from. An hour of refusals at the ceiling stops the run, with everything stored kept.
- Discovery makes no profile lookups. Each rule the lookup enforced now lives in collection, where the profile is read: the removed-site check, the shared-domain listing, the own-site shape rule.
- Browsing is saved page by page, valid for seven days. A handle is written down only for an author who reaches three works; everyone else is counted by a hash.
- Progress goes to a status file that `scripts/gpu.py status` shows.
- A full run is about 12,700 API requests, down from 16,200: roughly 12 hours at the base pace if nothing throttles.
- 2,258-plus tests pass on the merged code. No request was made to DeviantArt in building it. A critic judges it before it runs.
- One point left for the lead: an artist who lists a site that was removed before they were ever stored now gets a bare handle row, marked and never collected. The old lookup stored nothing for them.

**Pipeline counts:** artists discovered 472, collected 472, works 5,126; profiles 433, left out 37 (30 mostly AI-declared); artists public 119. **Database:** 16.8 MB of 400 MB.
