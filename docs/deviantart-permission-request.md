# Draft: permission request to DeviantArt

For the owner to review, edit and send through DeviantArt's Help Center contact form, from the account that will register the API application. Nothing here has been sent.

---

**Subject:** Permission request under Terms of Service section 24: artist discovery search (no training, no generation)

Hello,

I am building Artist Search, a small non-commercial, open-source search engine that helps game studios find concept and character artists to hire. The code is public at https://github.com/agent5905/Artist-Search.

I have read the Terms of Service and the API License Agreement. Section 24 prohibits using content labeled `noai` or `noimageai` as input to a trained AI system. I am writing to ask for written permission for the limited use below, and I will not do any of it without that permission.

**What I would like to do with public deviations from artists in this niche**

- Fetch public deviations through the official API, about 10 works each for roughly 2,000 to 3,000 artists.
- Run each image once through models on my own desktop to produce search tags, a one or two sentence description, and a style-similarity vector.
- Write a short profile of each artist from those tags and the artist's public bio.

**What I will not do**

- No model training or fine-tuning, and no image generation of any kind.
- No rehosting. Images are cached only on my own machine for the analysis and are never uploaded, stored in my database, or redistributed. The site shows work by embedding or linking the original on DeviantArt, with attribution and a link to the artist's page.
- No collection of private data. Only public handles, public bios and public links.
- No use of the API for benchmarking DeviantArt.

**Protections for artists**

- Every profile has a visible "claim or remove" link. Removal requests are honored and the artist is never re-added.
- I will honor any opt-out or opt-in signal you can expose. The API does not currently return the `noai` status of a deviation. If there is a supported way to read it, I would index only artists who have opted in.
- I will stay within your rate limits and send an identifying User-Agent.

**What I am asking**

1. Is this use permitted, with your written approval, for artists who have not opted in?
2. If not, is there an approved way to identify artists who have opted in through the API, so I can limit the index to them?
3. Is the attribution in Schedule 1 of the API License the right attribution to show next to embedded work? I could not find Schedule 1 on the published page.

Until I hear from you, the project uses the API only to find artists and their public links. No DeviantArt image or text is passed to any model.

Thank you for your time.

[Your name]
[DeviantArt username]
[Contact email]
