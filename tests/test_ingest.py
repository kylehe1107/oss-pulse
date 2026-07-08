"""Unit tests for incremental planning, state, and idempotent paths."""

from datetime import UTC, datetime
from pathlib import Path

from oss_pulse.ingest import gharchive_url, plan_hours
from oss_pulse.state import IngestState
from oss_pulse.writer import partition_dir


def utc(y, m, d, h):
    return datetime(y, m, d, h, tzinfo=UTC)


def test_gharchive_hour_is_not_zero_padded():
    # https://data.gharchive.org/2026-07-06-0.json.gz — the upstream quirk
    assert gharchive_url(utc(2026, 7, 6, 0)).endswith("/2026-07-06-0.json.gz")
    assert gharchive_url(utc(2026, 7, 6, 13)).endswith("/2026-07-06-13.json.gz")


def test_state_advances_monotonically(tmp_path: Path):
    state = IngestState(tmp_path / "state.json")
    assert state.last_ingested_hour() is None
    state.advance(utc(2026, 7, 6, 5))
    state.advance(utc(2026, 7, 6, 3))  # a re-run of an old hour must not rewind
    assert state.last_ingested_hour() == utc(2026, 7, 6, 5)


def test_plan_resumes_after_high_water_mark(tmp_path: Path):
    state = IngestState(tmp_path / "state.json")
    state.advance(utc(2026, 7, 6, 9))
    hours = plan_hours(state, start=None, end=None, max_hours=24, now=utc(2026, 7, 6, 14))
    # publication lag = 2h, so latest published is 12:00
    assert hours[0] == utc(2026, 7, 6, 10)
    assert hours[-1] == utc(2026, 7, 6, 12)


def test_plan_caps_at_max_hours(tmp_path: Path):
    state = IngestState(tmp_path / "state.json")
    hours = plan_hours(
        state, start=utc(2026, 7, 1, 0), end=utc(2026, 7, 2, 23), max_hours=6,
        now=utc(2026, 7, 6, 0),
    )
    assert len(hours) == 6
    assert hours[-1] == utc(2026, 7, 1, 5)


def test_plan_is_empty_when_caught_up(tmp_path: Path):
    state = IngestState(tmp_path / "state.json")
    state.advance(utc(2026, 7, 6, 12))
    hours = plan_hours(state, start=None, end=None, max_hours=24, now=utc(2026, 7, 6, 14))
    assert hours == []


def test_partition_path_is_deterministic(tmp_path: Path):
    p1 = partition_dir(tmp_path, utc(2026, 7, 6, 3))
    p2 = partition_dir(tmp_path, utc(2026, 7, 6, 3))
    assert p1 == p2  # one hour → one path → re-runs overwrite, never duplicate
    assert str(p1).endswith("event_date=2026-07-06/event_hour=03")
