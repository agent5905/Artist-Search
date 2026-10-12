"""Settings shared by every pipeline stage."""

import pathlib

ROOT = pathlib.Path(__file__).resolve().parent.parent
CACHE_DIR = ROOT / "cache"  # git-ignored. Hard rule 2: images never leave this machine.
LOCAL_DB = CACHE_DIR / "pipeline.sqlite"

# Hard rule 4: a clear user agent with a contact URL, and one request every few seconds per site.
CONTACT_URL = "https://github.com/agent5905/Artist-Search"
ROBOTS_TOKEN = "ArtistSearchBot"
USER_AGENT = f"{ROBOTS_TOKEN}/0.1 (+{CONTACT_URL})"
MIN_SECONDS_PER_HOST = 3.0


def load_env() -> dict[str, str]:
    """Read .env. Callers must never print, log or store the values."""
    env = {}
    for line in (ROOT / ".env").read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            key, value = line.split("=", 1)
            env[key.strip()] = value.strip().strip("\"'")
    return env
