# Artist Search: Build Spec for a Gauntlet Loop

**Owner:** DAG
**Status:** Ready to build
**Builder:** a coding agent (Claude Code or similar) running a Gauntlet Loop
**Working name:** Artist Search (rename freely)

Read this whole file before starting. It's the source of truth. The owner can stop the run at any time.

---

## 1. The goal

Build a search engine for finding artists, like Google but for illustrators and concept artists.

A studio or small business types what they need ("painterly fantasy character artist, open for commissions") or picks an artist whose style they like, and gets back the best-matching artists. Each result shows a short profile, examples of the artist's work, and links back to where that work lives.

The purpose is to prove the concept in public. This repo is public, and the code and AI workflows are part of what's being shown. It's a portfolio project first. A real product would be a bonus.

### Scope for this version

- **One niche:** game concept artists and character artists.
- **Size:** about 2,000 to 3,000 artists, with about 10 works each.
- **Three pages:** search, "similar style" results, and an artist profile page.

Don't expand past this scope without the owner's approval.

---

## 2. Hard rules (not negotiable)

These aren't quality targets. They're limits, and the critic loop never trades them away.

### Data and artists' rights
1. **Never rehost artwork.** Don't store artists' images in the database, in storage buckets, or in the repo. Show work by linking or embedding from the original source, so the artist gets the traffic and credit.
2. **Local image cache only.** Images can be downloaded to a local, git-ignored cache for analysis on the owner's desktop. They never leave that machine.
3. **Prefer official APIs.** Start with DeviantArt's official API. Read its terms of use before writing code against it, and stay within them, including rules about displaying content and attribution.
4. **Crawl politely.** For artists' own portfolio sites: obey robots.txt, identify the crawler with a clear user agent and contact URL, limit to one request every few seconds per site, and only read public pages.
5. **No gig-platform scraping.** Don't scrape Fiverr, Upwork, or similar sites. Their terms restrict it.
6. **No private data.** Collect only what an artist has made public for professional purposes: name or handle, public bio, specialties, public links, and whether they say they're open for work. No emails or phone numbers unless the artist publishes them for business contact.
7. **Claim or remove from day one.** Every profile has a visible way for the artist to request removal. Removal requests are honored, and the artist isn't re-added later.

### Engineering
8. **No secrets in the repo.** All keys live in an untracked `.env`. Commit a `.env.example` with placeholder values only. Run a secret scan before every commit.
9. **Image analysis runs locally** on the owner's GPU. No paid vision APIs.
10. **Stay inside the free tiers.** Supabase free tier (500 MB database) and Cloudflare free tier. Budget the database at 400 MB, to leave headroom.

### Stop and ask the owner before
- adding any new data source
- anything that might break a site's terms of use
- adding credentials, or spending money
- deploying publicly for the first time
- loosening a gate that keeps failing

---

## 3. The stack

| Piece | Choice |
|---|---|
| Language | Python for the pipeline. Your choice for the site (keep it simple). |
| Database | Supabase (Postgres with the pgvector extension for vector search) |
| Image and text analysis | Local models on the owner's desktop |
| Website hosting | Cloudflare (Pages or Workers) |
| Source control | GitHub, public repo |
| Agent access | Supabase and Cloudflare MCP servers, where connected |

### The owner's machine
- Windows desktop, NVIDIA RTX 5090 (32 GB VRAM), 64 GB RAM.
- Check what's already installed (Python, Ollama, CUDA) before installing anything.
- Pick current local models yourself, and confirm they exist before relying on them. You'll need:
  - a vision model that can describe and tag an image
  - an image embedding model for style similarity
  - a text embedding model for meaning-based search

### Free-tier limits to design around
- **Database size.** Vector indexes can grow several times larger than the row data. Store about 10 works per artist, use half-precision vectors, and measure the real size as you go.
- **Supabase pauses idle free projects after about a week.** Add a small scheduled Cloudflare Worker that pings the database so the public demo stays up.

---

## 4. How the product works

This section describes what the product must do. How to build it is your call; the notes under "Suggested" are suggestions, not requirements.

### 4.1 Collect
Gather artists in the niche and a sample of their public work, from the DeviantArt API first, and then from portfolio sites the artists link to.

### 4.2 Analyze (local)
For each work, produce:
- tags (medium, subject, genre, style words)
- a one or two sentence description of what the image shows
- a style vector (image embedding)

For each artist, produce a short **plain-language profile** written from their work. For example: "Character artist. Painterly fantasy armor and creature designs, mostly digital. Open for commissions." Every claim in a profile must be supported by the artist's actual work or public bio. No guessing.

### 4.3 Store
Push the results to Supabase: artists, works (with source links and source thumbnail URLs, not images), tags, profiles, and vectors.

**These profiles are the RAG data.** The search and any AI features retrieve from them first, then answer from what they retrieved.

### 4.4 Search, in two passes
1. **Rule out:** filter on hard facts, such as medium, specialty, and open for work.
2. **Rank:** order the rest by meaning (text query against profiles) or by style (image vectors against a seed artist's work).

"Similar style" starts from an artist already in the index. No image uploads in this version.

### 4.5 The site
- **Search page:** one search box, simple filters, and results as cards (name, profile line, 3 or 4 work thumbnails from the source, link out).
- **Similar-style page:** "More like this artist" results.
- **Profile page:** the profile, works linking to their sources, the artist's public links, and the "claim or remove" link.

It should feel fast and effortless. Someone should find a good artist in under a minute without reading instructions.

### 4.6 Later (only after the above works)
- An MCP server over the search, so anyone can ask an AI assistant "find me painterly character artists open for commissions."
- Artist-claimed profiles that the artists edit themselves.

### Suggested (not required)
- Pipeline stages as separate commands (collect, analyze, push) that can each be rerun safely.
- Search logic as Postgres functions, so the site and the future MCP server share it.

---

## 5. The Gauntlet Loop

Credit: the Gauntlet Loop method is Matt Shumer's ("How to Run a Gauntlet Loop," somethingbig.ai/gauntlet-loop). This section adapts it to this project.

### The idea
You're the **lead agent**. Break the goal into the smallest pieces that can be improved and judged on their own. Give each important piece its own **builder** and a separate **critic** with fresh context. The builder never grades its own work.

Decide the breakdown yourself. Likely pieces include collection, tagging accuracy, profile writing, text search ranking, style similarity, and each page of the site, but you know the work best.

### The round
1. **Build.** The builder makes or improves one piece.
2. **Critique.** Spawn a **fresh** critic sub-agent. Give it only the goal, the bar for that piece, the hard rules, and the real output. Never give it the builder's history, reasoning, or a summary written by the builder.
3. **Blind A/B where possible.** Show the critic our output and the reference side by side, labeled A and B in random order, without saying which is ours. It picks the better one.
4. **If ours loses,** the critic names the single biggest remaining gap, specifically enough to act on. That becomes the builder's next task.
5. **Repeat.** No fixed number of rounds. Keep going until our output wins consistently, improvements stop mattering, or the owner stops the run.

The critic always inspects the real thing: actual search results, rendered pages and screenshots, real profiles next to the real images, real test output.

### The bars

| Piece | Bar (what ours must beat) | How the critic judges |
|---|---|---|
| Text search | DeviantArt's own search results for the same query, within the niche | Blind A/B on a fixed set of at least 30 real queries. Which results page better answers the query? |
| Style similarity | DeviantArt's "more like this" results for the same seed work | Blind A/B on the result images. Which set looks more like the seed's style? |
| Tags and profiles | The artist's actual images and public bio | Every tag and profile claim checked against the source images. Any unsupported claim is a loss. |
| Site pages | The search and profile pages of the best real art platforms (for example ArtStation and Behance) | Blind A/B on screenshots at desktop and phone widths. Which is easier to use and better looking? |
| Pipeline | Its own tests | Pass or fail (see the fixed checks below). |

Build the query set and seed artists early, and save them in the repo (`eval/`), so every round uses the same tests. Don't change them to make a round pass.

A bar doesn't need to be reachable. Beating a major platform's search is ambitious on purpose: it keeps the loop pulling upward instead of stopping at "pretty good."

### Fixed checks (run every round, must always pass)
These protect the hard rules. They aren't part of the A/B judging.
- Lint, type check, and unit tests pass.
- Secret scan finds nothing.
- Running each pipeline stage twice creates no duplicates.
- Crawler tests confirm robots.txt is obeyed and rate limits hold.
- No image files are tracked in git, and none are stored in Supabase.
- Database size is under the 400 MB budget.

### Smoothing pass
At the end of each major wave of parallel work, spawn one fresh agent to look at the whole product. Its job is to make the pieces fit together (consistent naming, styles, and data shapes) and fix conflicts. It doesn't redesign anything.

### When a piece keeps losing
If the same gap survives several rounds, write down what was tried, and try a different approach rather than the same fix again. If it's blocked by a hard rule or a missing resource, stop and ask the owner. Never loosen a hard rule or edit the eval set to get a pass.

---

## 6. Live progress page

Keep a simple progress page the owner can check from a phone without interrupting the run. Use `PROGRESS.md` in the repo (or a small HTML page if that shows the work better). Update it after every round with:
- what changed, and which piece
- the critic's verdict and the biggest remaining gap
- before and after evidence (screenshots, result lists, scores)
- the current database size and pipeline counts
- anything waiting on the owner

Show how the work evolves over time, not just the latest state.

---

## 7. Suggested order of work

You decide the plan, but the dependencies run roughly this way:

1. **Setup:** repo, `.env.example`, Supabase project with pgvector, a check of the local machine and models.
2. **Collect** from the DeviantArt API, for a first 200 artists.
3. **Analyze** locally: tags, descriptions, vectors, profiles.
4. **Store and search:** schema, the two-pass search, and style similarity.
5. **Site:** the three pages, deployed to a private preview first.
6. **Scale** to the full 2,000 to 3,000 artists, and add portfolio-site crawling.
7. **Public launch** (ask the owner first), plus the keep-alive Worker.

Start the Gauntlet Loop as soon as there's something real to judge. Don't wait for everything to be built.

---

## 8. The README

The public README should explain, in plain language:
- what the project does and why
- the architecture: collect, analyze locally, store, search, site
- how the Gauntlet Loop was used, with examples from the progress log
- how artists' data is handled: no rehosting, polite crawling, claim or remove
- how to run it locally

Reviewers will read this to judge the work, so make it clear and honest about what works and what doesn't yet.

---

## 9. Done means

This version is done when all of these are true:
- The search runs publicly on Cloudflare over 2,000+ artists in the niche.
- All fixed checks pass.
- The critic picks our results over the reference in a clear majority of the blind A/B tests for text search and style similarity, or the owner decides it's good enough.
- Every profile claim the critic samples is supported by the artist's work.
- The README and progress log tell the story.

The owner makes the final call.
