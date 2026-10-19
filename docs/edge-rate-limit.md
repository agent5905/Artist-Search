# The edge rule, for when the site has a domain

Nothing here is set up. It needs a domain of the owner's on Cloudflare, which there is not yet.

**Why.** The database counts every visitor, network and the day's total before it answers (`client_wait` in `sql/003_limits.sql`). A refused request still runs the site's server function, though, and Cloudflare's free plan allows 100,000 function requests a day for the whole account. One address can spend them at a few requests a second, and when they run out every `/api/` call fails until the next day: search, profiles, and the claim-or-remove page too. Only a rule in front of the functions can stop that. Rate limiting rules apply only to a zone of one's own, so the rule needs the domain, and `artist-search.pages.dev` must send everyone to it, or the rule can be gone around.

## 1. The domain

1. Add the domain to the Cloudflare account (Free plan) and switch its nameservers.
2. Workers & Pages → `artist-search` → Custom domains → add the domain.

## 2. The rate limiting rule

Domain → Security → WAF → Rate limiting rules → Create rule. The Free plan allows one rule, a 10-second period and a 10-second block, by IP:

| Setting | Value |
|---|---|
| Rule name | `api-per-address` |
| Expression | `(starts_with(http.request.uri.path, "/api/"))` (if the editor refuses the function on this plan: `(http.request.uri.path contains "/api/")`) |
| Characteristics | IP |
| Requests | 10 |
| Period | 10 seconds |
| Action | Block (answers 429) |
| Duration | 10 seconds |

A page of the site makes one or two `/api/` calls, so 10 in 10 seconds is more than a person clicking fast. The ceiling it sets: one address can still make 86,400 calls a day at the limit, so the rule stops bursts, and two addresses holding at the limit could still use up 100,000. Past that, the answer is Workers Paid ($5 a month, 10 million requests), which is the owner's call.

## 3. The redirect from pages.dev

Account home → Bulk Redirects → Create a list `artist_search_pages_dev` with one URL redirect:

| Setting | Value |
|---|---|
| Source URL | `artist-search.pages.dev` |
| Target URL | `https://<the domain>` |
| Status | 301 |
| Preserve query string | on |
| Include subdomains | on (every preview and branch deployment, `*.artist-search.pages.dev`, runs the same functions) |
| Subpath matching | on |
| Preserve path suffix | on |

Then Create Bulk Redirect Rule → the list `artist_search_pages_dev` → Deploy.

## 4. Check it

- `curl -sI "https://artist-search.pages.dev/api/search?q=knight"` answers 301 with `location: https://<the domain>/api/search?q=knight`, and so does a preview address.
- Eleven quick `curl -s -o /dev/null -w "%{http_code}\n" "https://<the domain>/api/search?q=knight"` from one address: the last one is 429.
