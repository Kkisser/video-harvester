import os
import time

import pytest

os.environ.setdefault("CONFIG_PATH", "missing-test-config.yaml")
os.environ.setdefault("DATABASE_URL", "sqlite:///unit_test.db")
os.environ.setdefault("SECRET_KEY", "unit-secret-key-123456789")
os.environ.setdefault("AGENT_TOKEN", "unit-agent-token-123456789")

from agent.agent import RateLimiter, backoff_delay, retry_after_seconds
from agent.downloader import _safe_folder, normalize_name
from coordinator.security import hash_password, verify_password
from coordinator.service import (
    can_transition,
    make_chunks,
    parse_cli_range,
    parse_range,
)


@pytest.mark.parametrize(
    "text,expected",
    [
        ("[10,20]", (10, 20)),
        ("50", (0, 50)),
        ("[100,]", (100, None)),
        ("", (0, None)),
    ],
)
def test_range_formats(text, expected):
    assert parse_range(text) == expected


def test_invalid_range():
    with pytest.raises(ValueError):
        parse_range("[20,10]")
    with pytest.raises(ValueError):
        parse_range("[-1,10]")


def test_cli_range():
    assert parse_cli_range(None, None) == (0, None)
    assert parse_cli_range(10, 15) == (10, 15)


def test_chunks_have_no_gap_or_overlap():
    assert make_chunks(0, 25, 10) == [(0, 9), (10, 19), (20, 25)]
    assert make_chunks(100, None, 5) == [(100, 104)]


def test_state_transitions():
    assert can_transition("created", "running")
    assert can_transition("paused", "running")
    assert not can_transition("completed", "running")


def test_backoff_and_retry_after():
    assert backoff_delay(3) > backoff_delay(0)
    assert retry_after_seconds({"retry-after": "2"}) == 2
    assert retry_after_seconds({}) is None


def test_rate_limiter_spaces_requests():
    limiter = RateLimiter(20)
    started = time.monotonic()
    for _ in range(4):
        limiter.wait()
    assert time.monotonic() - started >= 0.12


def test_name_normalization_and_safe_path(tmp_path):
    assert normalize_name("Лекция/5:*?") == "Лекция_5___"
    assert len(normalize_name("я" * 500)) == 120
    folder = _safe_folder(str(tmp_path), 12345)
    assert tmp_path.resolve() in folder.parents


def test_password_hash_uses_random_salt():
    first = hash_password("secret")
    second = hash_password("secret")
    assert first != second
    assert verify_password("secret", first)
    assert not verify_password("wrong", first)
