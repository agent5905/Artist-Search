# Artist Search

A search engine for game concept and character artists.

Type a brief the way a studio would, such as "dark fantasy creature designer, painterly, open for commissions", and get **artists**, not single images. Open an artist and see others who draw in a similar style.

**Live at [artist-search.pages.dev](https://artist-search.pages.dev)** since 2026-10-21. It lists 2,978 artists and 27,443 of their works, drawn from 3,969 collected artists after every content check. In blind tests it beat DeviantArt's own search on 28 of 36 briefs, and its "more like this" beat DeviantArt's on 14 seed artists to 12. [PROGRESS.md](PROGRESS.md) is the running log, updated after every round of work.

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
| Tag | A local vision model (`qwen3.8` in Ollama) tags and describes each work. A second pass checks every claim against the image and drops what it cannot see. Focused readings follow:<br>• the content rating, and for a "suggestive" work whether it is only blood and wounds;<br>• whether the image is artwork at all;<br>• fetish content, hate symbols and sexualised minors;<br>• political content about real people;<br>• what the artist's own words say (AI, adult, contact) | Owner's machine |
| Style | A style model (CSD ViT-L) turns each work into a vector. Each artist's nearest artists are computed locally | Owner's machine |
| Profile | One plain sentence per artist, counted from the checked per-work tags (subject, genre, drawing manner, medium) and worded by share: "Character and environment art." The artist's own tags are shown beside it, labelled as theirs. The text is embedded with `bge-m3` for search | Owner's machine |
| Push | Sends text, links, thumbnail addresses and vectors to the database, from one consistent snapshot of the local store. Every model reading carries a fingerprint of the words or image it was made from, and nothing goes out under a reading of something that has since changed. It holds back what the owner's content rules leave out (see below), honours removals first, and stops rather than delete much at once | Owner's machine |
| Store | Supabase Postgres with `pgvector`, free tier. Every table is locked with row-level security; the site's server reaches the data only through seven database functions, each of which checks the site's own token | Supabase |
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
- **We lost the first three tests against DeviantArt's "More Like This", then won.**
  - It beat our similar-style results on 21 of 29 seed artists.
  - Showing each similar artist's closest works lifted ours to 11 to 17.
  - The full pool of 3,000 artists made it 11 to 15.
  - Every round's critics said the same thing: our sets were half right, a neighbour bringing one or two outliers. A closeness floor, so a similar artist shows only works that pass it and drops out without two, won 14 to 12. A second pair of fresh critics confirmed it at 14 to 12.
- **Our search beat DeviantArt's own on 28 of 36 briefs.**
  - **Where we won:** named artist cards, and first screens free of text posts, locked tiles and ads. DeviantArt also returned nothing for long briefs and for a misspelling.
  - **Where we lost:** where the brief named one exact kind of piece (icon sheets, pixel sprites), and on "no anime", a negation our search does not yet understand.
- **Profiles stopped at a ceiling the owner accepted.** Seven critic rounds tried four ways of putting specifics on a card: per-work tags, a count on a contact sheet, a per-work yes-or-no check, and the artist's own tags. The plain sentence stayed reliable; every way of adding specifics added errors (the per-work check agreed a dinosaur was a dragon). The owner chose plain cards, with the artist's own tags shown beside them, at about one unsupported claim in 20 under a strict critic.
- **DeviantArt rate-limited us, and we deserved it.** A day at a fixed one request a second tripped its adaptive limit. The client backed off for one request at a time and then went straight back to full speed, which is what the API's documentation warns against. The pacing was rebuilt to slow down for the whole run, keep its pace across runs, and stop after an hour of refusals; two critics and three rounds later, discovery read 1,659 pages with no rate-limit answer.
- **Push took nine critics.**
  - **The first two found holes in the data handling.** A reading was not tied to what it read, so a tagline changed after it was checked would go out under the old "no contact details". Every reading now carries a fingerprint. Changes landing during a push could also slip through, so the push now builds from one snapshot.
  - **The last five built from a copy of the real store and opened what would be sent.** They found AI accounts, fetish art the content rating called safe, a swastika poster, sale sheets leading cards, and "Now on Bluesky!" banners. Each became a focused check or a word rule, and the owner settled what the site shows (blood yes, pin-ups no; any AI-declared work holds the artist back).
  - **The ninth** recomputed all 208,473 similar-style picks and found every one sound.
- **Claim-or-remove took ten critics.** Each found a way the removal of one artist could reach another, through a shared website, an old handle DeviantArt gave to someone new, a stranger's note, or a flood of fake codes.
  - The fix that held was to bar an account, never a handle, whenever the account is known, and to send no artist without one.
  - The loop ended under limits the owner accepted, for example that whoever controls a domain an artist names can prove a removal.
  - The live database's own test suite (165 tests) was its last gate.

## How artists' data is handled

- **Nothing is rehosted.** The database holds links and thumbnail addresses. The site loads thumbnails from the source's own image host, with credit and a link back.
- **Images are cached only on the owner's machine,** in a folder git ignores. A commit hook rejects image files and anything that looks like a secret.
- **Collection is polite.** It uses the official API at no more than one request every two seconds, identifies itself with a contact address, and when the API asks it to slow down it slows down for the rest of the run and the next one, not only for the request that was refused. One process talks to a host at a time. The crawler for artists' own sites obeys robots.txt and waits three seconds between requests to a host.
- **Only public, professional information is kept:** handle, public bio, specialties, public links, and whether the artist says they are open for work, in their own words. Contact details found in titles or tags are held back.
- **Left out**, under the owner's content rules:
  - works marked mature or rated explicit or sexually suggestive (moderate blood and wounds are shown: that is game art);
  - fetish content, hate symbols (in fiction too), and anything sexualising a minor;
  - images that are not artwork (screenshots, notices, price sheets, contact cards);
  - political content about real people;
  - works whose own title or tags are sexual, fetish, hateful or profane, or announce a social account;
  - artists with a large adult share;
  - an artist with any work declared AI-made, or whose own words declare AI art or an adult account;
  - an artist with no DeviantArt account of their own.

  The checks lean toward holding back, and the owner keeps a short hand-held list for anything they miss.
- **No model is trained and no image is generated.** Models only describe and compare.
- **Claim or remove is on every profile.** An artist proves control by putting a short code on their site or DeviantArt profile. The profile is then removed at once and never collected again.

**One open question, stated plainly.** DeviantArt's Terms of Service, section 24, restrict using works labeled "noai" as input to AI systems. The agent leading the build read that as covering the local tagging and stopped to ask. The owner reads the section as aimed at training and image generation, which this project does not do, and decided to proceed. The decision and both readings are in [PROGRESS.md](PROGRESS.md), under "Owner decisions".

## What works and what does not yet

| | State |
|---|---|
| Discover and collect | Done at scale: 3,969 artists and 39,744 works, with no artist lost |
| Tag, rate, style, profile, and the content checks | Done on the full set, about four days on one GPU in all |
| Public database | 2,978 artists, 27,443 works and 63,230 similar-style links: 84.6 MB of a 400 MB budget |
| Push | Through nine critics; the first real load ran on 2026-10-21 |
| Search and similar style | Live |
| Site | Live at [artist-search.pages.dev](https://artist-search.pages.dev). Beat ArtStation and Behance pages on 7 of 8 pairs, twice, with the caveat that logos made the test not truly blind |
| Blind test against DeviantArt's "More Like This" | Won 14 to 12 on the full pool, and a second pair of critics confirmed it at 14 to 12. Lost the three earlier rounds |
| Blind test against DeviantArt's own search | Won 28 to 8 on 36 frozen briefs |
| Profile claims | About one unsupported claim in 20 cards under a strict critic; the owner accepted that in place of the spec's "every claim" |
| Removal proof | Built, and rebuilt after two critics: `pipeline.removals` looks for the code only where the artist alone can write (their DeviantArt bio or tagline, a tag in their site's head, or a file on it) and removes them at once. A request is a lasting record that no push or rename touches; only a proof or the owner's answer closes it. Checked hourly for a day, every six hours for two weeks, then weekly, 30 a run; asking for the code again starts that over. A proof bars the artist's account, never only a handle. Runs every 15 minutes on the owner's machine (Windows Task Scheduler) |
| Limits | Live, after several critics: counted in the database, and a refused request costs nothing. A day's reads (a page of results counts 1, a profile 0.1): 100 per visitor (an IPv6 /64 counts as one), 1,000 per network (an IPv4 /24, an IPv6 /48: its neighbours share it), 3,000 for everyone: what the free tier's 5 GB of egress a month allows, so it takes 30 visitors in three networks or more to use up a day. Removal requests: 10 a day per visitor, 20 per network. Every database call goes through the site's functions, with a token of their own; the browser holds no key, and a call from another site is refused. There is no edge rate-limiting rule without a domain of the site's own, a risk the owner accepted |
| Public deployment | Live since 2026-10-21 |
| Known gaps | Search does not understand a negation ("no anime"); AI-made work that never says so is not caught |
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

The removal check reads the removal requests that a proof code was given out for and are due, and removes every artist whose code is in their DeviantArt bio or tagline, in a `<meta name="artist-search-removal">` in their own site's head, or in `/.well-known/artist-search-removal.txt` on it. Run it on a schedule, every 15 minutes, with Windows Task Scheduler: the site tells artists a code put up is usually found within a few hours, and at most within a day, and that holds only with the schedule (it checks each code hourly for a day, then every six hours for two weeks, then weekly). While a collect or discovery run holds the API, that run makes these checks itself, every 15 minutes between two of its own requests. With nothing due it asks DeviantArt nothing. Each run prints how many requests are open with a code and without one, proven, and closed. Requests without proof are the owner's to answer:

```bash
uv run python -m pipeline.removals --dry-run
uv run python -m pipeline.removals
uv run python -m pipeline.removals --pending              # the open requests: slug, id, public handle, notes
uv run python -m pipeline.removals --grant ID --reject ID
```

Anything the checks miss can be held back by hand: one deviation id or `da:<handle>` per line in `cache/held_by_hand.txt` (git-ignored). An artist's entry follows their account through a change of handle.

The analysis stages take days on one GPU at full scale, so a small worker runs them in turns and can be paused. A pause loses nothing: each stage saves every work as it finishes it and holds before its next request to a model.

```bash
uv run python scripts/gpu.py start     # run the GPU stages in the background, over and over
uv run python scripts/gpu.py status    # what is done, what is left, and the pace
uv run python scripts/gpu.py pause     # free the GPU; models are unloaded
uv run python scripts/gpu.py resume    # carry on from where it held
```

The site:

```bash
cd web
npm install
npm run dev
```

The site's functions call the database with the publishable key and `SITE_TOKEN`, a long random string made once and kept in `.env`; `uv run python -m pipeline.db migrate` gives the database its hash. The publishable key alone opens nothing, and nothing the site can call deletes or hides an artist. On Cloudflare all three are secrets of the Pages project (the project's settings otherwise come from `web/wrangler.toml`). Migrate before deploying the site that needs it, then build and deploy:

```bash
cd web
npm run build
npx wrangler pages deploy dist --project-name artist-search --branch main
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
