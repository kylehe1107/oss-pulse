"""Unit tests for the bronze projection logic — the same suite CI runs."""

import json

import pytest

from oss_pulse.parse import MalformedRecordError, parse_line


def make_event(**overrides):
    base = {
        "id": "45000000000",
        "type": "PushEvent",
        "created_at": "2026-07-06T12:34:56Z",
        "actor": {"id": 123, "login": "octocat"},
        "repo": {"id": 456, "name": "octo-org/octo-repo"},
        "payload": {"size": 3, "distinct_size": 2},
        "public": True,
    }
    base.update(overrides)
    return json.dumps(base)


def test_push_event_projected():
    row = parse_line(make_event())
    assert row["event_id"] == "45000000000"
    assert row["event_type"] == "PushEvent"
    assert row["actor_login"] == "octocat"
    assert row["repo_name"] == "octo-org/octo-repo"
    assert row["push_distinct_size"] == 2
    assert row["created_at"].isoformat() == "2026-07-06T12:34:56+00:00"
    # PR-only columns are NULL for pushes
    assert row["pr_merged"] is None
    assert row["repo_language"] is None


def test_pull_request_event_extracts_language_and_merge():
    payload = {
        "action": "closed",
        "pull_request": {
            "merged": True,
            "additions": 10,
            "deletions": 2,
            "changed_files": 3,
            "base": {"repo": {"language": "Rust", "stargazers_count": 4200}},
        },
    }
    row = parse_line(make_event(type="PullRequestEvent", payload=payload))
    assert row["payload_action"] == "closed"
    assert row["pr_merged"] is True
    assert row["pr_additions"] == 10
    assert row["repo_language"] == "Rust"
    assert row["repo_stars"] == 4200


def test_watch_event_action():
    row = parse_line(make_event(type="WatchEvent", payload={"action": "started"}))
    assert row["payload_action"] == "started"


def test_unwanted_event_type_is_filtered_not_quarantined():
    assert parse_line(make_event(type="GollumEvent")) is None


def test_invalid_json_is_malformed():
    with pytest.raises(MalformedRecordError) as exc:
        parse_line("{this is not json")
    assert "invalid_json" in exc.value.reason


def test_missing_actor_is_malformed():
    event = json.loads(make_event())
    del event["actor"]
    with pytest.raises(MalformedRecordError) as exc:
        parse_line(json.dumps(event))
    assert "actor" in exc.value.reason


def test_bad_timestamp_is_malformed():
    with pytest.raises(MalformedRecordError) as exc:
        parse_line(make_event(created_at="not-a-time"))
    assert "bad_created_at" in exc.value.reason


def test_non_object_line_is_malformed():
    with pytest.raises(MalformedRecordError):
        parse_line('[1, 2, 3]')
