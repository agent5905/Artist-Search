"""Run, pause and watch the local GPU work: style vectors, tags, formats and profiles for what has been collected.

    uv run python scripts/gpu.py status              what is done, what is left, and whether it is running
    uv run python scripts/gpu.py pause               free the GPU: every stage holds where it is, models are unloaded
    uv run python scripts/gpu.py resume              carry on from where it held
    uv run python scripts/gpu.py start [stage ...]   start the worker in the background (first time, or after a reboot)
    uv run python scripts/gpu.py stop                end the worker altogether

A pause loses nothing. Each stage saves every work as it finishes it, and a paused stage waits before its next
request to a model (pipeline/config.py, wait_while_paused), whoever started it. The worker runs the stages in
short turns, over and over, and sleeps when nothing is left, so works that collection brings later are picked up.
Collection and discovery use the network, not the GPU, and are not paused by this.

`start`, `stop` and the worker's line in `status` are written for Windows, where the owner's machine runs. `run`
is the worker in the foreground, anywhere.
"""

import argparse
import pathlib
import sqlite3
import subprocess
import sys
import time
from datetime import UTC, datetime, timedelta

import httpx

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))
from pipeline import config, library, profiles, tagging  # noqa: E402

TURN = 100  # works a stage takes on in one turn: a quarter of an hour of tagging before the next stage has its turn
IDLE = 300  # seconds the worker sleeps when a whole round changed nothing
STAGES = {
    "style": ["pipeline.style", "--model", "csd", "--max-works", str(5 * TURN)],
    "tagging": ["pipeline.tagging", "--max-works", str(TURN), "--verifier", profiles.TAGGER],
    "formats": ["pipeline.tagging", "--max-works", str(TURN), "--formats"],
    "profiles": ["pipeline.profiles", "--max-artists", str(TURN // 4)],
}
_ALL = "SELECT count(*) FROM works w WHERE w.image_file IS NOT NULL"
_ARTISTS = "SELECT count(DISTINCT artist_key) FROM works"
# What each stage has done, of the works with an image or of the artists. Where its rows are timed, the query
# takes :since, and counts what was done after it. A reading of an image the work no longer shows is not done:
# library.IMAGE is the rule, as in the stages themselves.
_PROFILED = "artists profiled, or left out for a reason"
_DONE = {
    "style vectors": "SELECT count(*) FROM works w JOIN style_vectors v USING (deviation_id) "
    f"WHERE w.image_file IS NOT NULL AND v.model = 'csd' AND v.vector IS NOT NULL AND v.fingerprint = {library.IMAGE}",
    "tagged and checked": "SELECT count(*) FROM works w JOIN work_tags t USING (deviation_id) WHERE "
    "w.image_file IS NOT NULL AND t.model = :set AND t.prompt_version = :tags AND t.analyzed_at > :since "
    f"AND t.fingerprint = {library.IMAGE}",
    "sorted by format": "SELECT count(*) FROM works w JOIN work_format f USING (deviation_id) WHERE "
    "w.image_file IS NOT NULL AND f.model = :model AND f.prompt_version = :formats AND f.asked_at > :since "
    f"AND f.fingerprint = {library.IMAGE}",
    _PROFILED: "SELECT count(*) FROM artist_profiles WHERE made_at > :since",
}


def _file(name: str) -> pathlib.Path:
    return config.CACHE_DIR / name


def counts(since: str = "") -> dict[str, int]:
    """How much is collected and how much of it each stage has done; with `since`, what it did after that time.
    A store or a table that no stage has made yet counts as nothing. SystemExit, with the command to run, for a
    store whose readings carry no fingerprints yet: nothing in it can be called done."""
    names = {
        "set": tagging.result_set(profiles.TAGGER, profiles.TAGGER),
        "tags": tagging.PROMPT_VERSION,
        "model": profiles.TAGGER,
        "formats": tagging.FORMAT_VERSION,
        "since": since,
    }
    found = {}
    if not config.LOCAL_DB.exists():
        return dict.fromkeys(("works", "artists", *_DONE), 0)
    conn = sqlite3.connect(f"file:{config.LOCAL_DB.as_posix()}?mode=ro", uri=True, timeout=60)
    try:
        conn.create_function("own_file", 1, library.own_file, deterministic=True)
        for label, query in {"works": _ALL, "artists": _ARTISTS, **_DONE}.items():
            try:
                found[label] = int(conn.execute(query, names).fetchone()[0])
            except sqlite3.OperationalError:
                found[label] = 0
        unmigrated = found["works"] and conn.execute("PRAGMA user_version").fetchone()[0] < library.SHAPE
    finally:
        conn.close()
    if unmigrated:
        raise SystemExit(library.UNMIGRATED)
    if found[_PROFILED] and not since:  # a row made from a reading that has gone stale is to be made again
        found[_PROFILED] = len(profiles.current())
    return found


def worker() -> int | None:
    """The worker's process id while it is alive."""
    try:
        pid = int(_file("gpu.pid").read_text())
    except (OSError, ValueError):
        return None
    listed = subprocess.run(["tasklist", "/FI", f"PID eq {pid}", "/NH"], capture_output=True, text=True).stdout
    return pid if "python" in listed.lower() and str(pid) in listed.split() else None


def status() -> list[str]:
    pid = worker()
    lines = [
        "PAUSED. The GPU is free; `resume` carries on." if _file(config.PAUSE_FILE).exists() else "Not paused.",
        f"Worker running (process {pid})." if pid else "No worker running; `start` begins one.",
    ]
    now, hour = counts(), counts((datetime.now(UTC) - timedelta(hours=1)).isoformat(timespec="seconds"))
    lines.append(f"Collected: {now['artists']} artists, {now['works']} works with an image.")
    for label, query in _DONE.items():
        left = max(now["artists" if label.startswith("artists") else "works"] - now[label], 0)
        line = f"  {label}: {now[label]} done, {left} to go"
        if ":since" in query:
            line += f"; {hour[label]} in the last hour"
            line += f", so about {left / hour[label]:.0f} h left at that pace" if hour[label] and left else ""
        lines.append(line)
    for told in sorted(config.CACHE_DIR.glob("*.status")):  # where a discovery or a collection run says it is
        try:
            when, *said = told.read_text(encoding="utf-8").splitlines() or [""]
        except OSError:  # the run ended just now, and took its file with it
            continue
        lines += [f"{told.stem} is running, or was killed. As of {when}:", *(f"  {line}" for line in said)]
    try:
        asked = ["nvidia-smi", "--query-gpu=memory.used,memory.total,utilization.gpu", "--format=csv,noheader"]
        lines.append("GPU in use: " + subprocess.run(asked, capture_output=True, text=True).stdout.strip())
    except OSError:  # no such program on this machine
        pass
    return lines


def pause() -> None:
    _file(config.PAUSE_FILE).touch()
    # Every stage now holds before its next request. Unloading what Ollama has loaded is what frees the memory;
    # a request it was in the middle of fails, and the stage asks again once the pause is over.
    try:
        with tagging._client(tagging.OLLAMA_URL) as ollama:
            for model in ollama.get("/api/ps").json().get("models", []):
                ollama.post("/api/generate", json={"model": model["name"], "keep_alive": 0, "stream": False})
    except httpx.HTTPError:  # Ollama is not running, so nothing is loaded
        pass


def run(stages: list[str]) -> None:
    """The worker: every stage in turn, for ever. A stage that fails is tried again on the next round."""
    while True:
        before = counts()
        for name in stages:
            config.wait_while_paused()  # no stage is begun while paused; one that is running holds itself
            code = subprocess.run([sys.executable, "-m", *STAGES[name]], cwd=config.ROOT).returncode
            print(f"{datetime.now().isoformat(timespec='seconds')} {name}: exit {code}", flush=True)
        if counts() == before:
            time.sleep(IDLE)


def main() -> int:
    parser = argparse.ArgumentParser(description="Run, pause and watch the local GPU work.")
    parser.add_argument("action", choices=["status", "pause", "resume", "start", "stop", "run"])
    parser.add_argument("stages", nargs="*", help=f"for start and run: any of {', '.join(STAGES)}; all when none")
    args = parser.parse_args()
    if set(args.stages) - set(STAGES):
        parser.error(f"a stage is one of: {', '.join(STAGES)}")
    stages = args.stages or list(STAGES)
    if args.action == "run":
        run(stages)
    elif args.action == "pause":
        pause()
    elif args.action == "resume":
        _file(config.PAUSE_FILE).unlink(missing_ok=True)
    elif args.action == "stop" and (pid := worker()):
        subprocess.run(["taskkill", "/PID", str(pid), "/T", "/F"], capture_output=True)
    elif args.action == "start" and not worker():
        config.CACHE_DIR.mkdir(exist_ok=True)
        with _file("gpu.log").open("a", encoding="utf-8") as log:
            child = subprocess.Popen(
                [sys.executable, __file__, "run", *stages],
                stdin=subprocess.DEVNULL,
                stdout=log,
                stderr=subprocess.STDOUT,
                # Its own process group and no console, so it outlives the terminal it was started from.
                creationflags=subprocess.CREATE_NEW_PROCESS_GROUP | subprocess.CREATE_NO_WINDOW,
                cwd=config.ROOT,
            )
        _file("gpu.pid").write_text(str(child.pid))
    print("\n".join(status()))
    return 0


if __name__ == "__main__":
    sys.exit(main())
