import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))

from secret_scan import env_values, problems  # noqa: E402

FAKE_TOKEN = "gh" + "p_" + "a" * 36  # built at runtime so this file passes its own scan


def test_clean_file_passes():
    assert problems("src/app.py", "print('hello')", ["s3cret-value-0123456789"]) == []


def test_env_value_is_blocked():
    assert problems("notes.md", "key = s3cret-value-0123456789", ["s3cret-value-0123456789"])


def test_key_pattern_is_blocked():
    assert problems("config.py", f"TOKEN = '{FAKE_TOKEN}'", [])


def test_images_and_env_file_are_blocked():
    assert problems("web/public/art.PNG", "", [])
    assert problems(".env", "", [])
    assert problems(".env.example", "GH_TOKEN=", []) == []


def test_short_env_values_are_ignored(tmp_path):
    env = tmp_path / ".env"
    env.write_text("# comment\nBRANCH=main\nTOKEN='abcdefghijklmnopqrstuvwxyz'\n", encoding="utf-8")
    assert env_values(env) == ["abcdefghijklmnopqrstuvwxyz"]
