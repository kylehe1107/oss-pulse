"""Parse raw GH Archive NDJSON lines into flat, explicitly-typed bronze rows.

Design decisions:

- **Filter + project at ingest ("typed bronze").** GH Archive is ~4–5 GB compressed
  per day; landing it untouched would blow the free-tier budget within a week. We
  keep the 7 event types the marts need and project ~18 typed columns. Tradeoff:
  analyses needing unprojected payload fields require a re-download — acceptable
  because GH Archive itself is a durable, replayable archive.

- **Consistent nullable schema across event types.** One wide table with
  type-specific columns (pr_*, push_*) NULL where inapplicable, instead of one
  table per event type. Keeps the raw layer and dbt sources simple at this scale.

- **Fail loudly.** A line that can't be decoded, or that lacks identity fields,
  raises MalformedRecordError and is quarantined by the caller with its reason —
  never silently dropped.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any

KEEP_EVENT_TYPES = frozenset(
    {
        "PushEvent",
        "PullRequestEvent",
        "IssuesEvent",
        "WatchEvent",
        "ForkEvent",
        "CreateEvent",
        "ReleaseEvent",
    }
)

REQUIRED_FIELDS = ("id", "type", "created_at", "actor", "repo")


class MalformedRecordError(Exception):
    def __init__(self, reason: str):
        super().__init__(reason)
        self.reason = reason


def _get(obj: Any, *path: str) -> Any:
    """Safe nested lookup: _get(event, "payload", "pull_request", "merged")."""
    for key in path:
        if not isinstance(obj, dict):
            return None
        obj = obj.get(key)
    return obj


def _opt_int(value: Any) -> int | None:
    try:
        return int(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def parse_line(line: str) -> dict[str, Any] | None:
    """Return a projected row dict, or None if the event type is filtered out.

    Raises MalformedRecordError for undecodable or structurally invalid lines.
    """
    try:
        event = json.loads(line)
    except json.JSONDecodeError as err:
        raise MalformedRecordError(f"invalid_json: {err.msg}") from err
    if not isinstance(event, dict):
        raise MalformedRecordError("not_an_object")

    etype = event.get("type")
    if etype not in KEEP_EVENT_TYPES:
        return None  # deliberately filtered, not malformed

    missing = [f for f in REQUIRED_FIELDS if not event.get(f)]
    if missing:
        raise MalformedRecordError("missing_fields: " + ",".join(missing))

    raw_created = str(event["created_at"])
    try:
        created_at = datetime.fromisoformat(raw_created.replace("Z", "+00:00"))
    except ValueError as err:
        raise MalformedRecordError(f"bad_created_at: {raw_created!r}") from err
    if created_at.tzinfo is None:
        created_at = created_at.replace(tzinfo=UTC)

    actor_id = _opt_int(_get(event, "actor", "id"))
    repo_id = _opt_int(_get(event, "repo", "id"))
    if actor_id is None or repo_id is None:
        raise MalformedRecordError("missing_fields: actor.id,repo.id")

    merged = _get(event, "payload", "pull_request", "merged")
    return {
        "event_id": str(event["id"]),
        "event_type": etype,
        "created_at": created_at,
        "actor_id": actor_id,
        "actor_login": _get(event, "actor", "login"),
        "repo_id": repo_id,
        "repo_name": _get(event, "repo", "name"),
        "org_login": _get(event, "org", "login"),
        "payload_action": _get(event, "payload", "action"),
        # PullRequestEvent payloads embed the full base-repo object — the only
        # place the events firehose exposes repo language and stars. dim_repo
        # is enriched from these in the dbt layer.
        "pr_merged": bool(merged) if merged is not None else None,
        "pr_additions": _opt_int(_get(event, "payload", "pull_request", "additions")),
        "pr_deletions": _opt_int(_get(event, "payload", "pull_request", "deletions")),
        "pr_changed_files": _opt_int(_get(event, "payload", "pull_request", "changed_files")),
        "repo_language": _get(event, "payload", "pull_request", "base", "repo", "language"),
        "repo_stars": _opt_int(
            _get(event, "payload", "pull_request", "base", "repo", "stargazers_count")
        ),
        "push_distinct_size": _opt_int(_get(event, "payload", "distinct_size")),
        "create_ref_type": _get(event, "payload", "ref_type"),
    }
