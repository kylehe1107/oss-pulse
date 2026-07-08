"""High-water-mark state for incremental ingestion.

The pipeline's unit of work is one UTC hour (GH Archive publishes one file per
hour). State records the newest hour that was fully ingested; the next run starts
at the following hour. It is stored as JSON on disk and updated only AFTER an
hour's Parquet file is atomically in place — so the mark can never point past
data that doesn't exist, and a crashed run resumes exactly where it stopped.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

HOUR_FMT = "%Y-%m-%dT%H"


def parse_hour(s: str) -> datetime:
    return datetime.strptime(s, HOUR_FMT).replace(tzinfo=UTC)


def format_hour(dt: datetime) -> str:
    return dt.astimezone(UTC).strftime(HOUR_FMT)


class IngestState:
    def __init__(self, path: Path):
        self.path = path

    def last_ingested_hour(self) -> datetime | None:
        if not self.path.exists():
            return None
        raw = json.loads(self.path.read_text()).get("last_ingested_hour")
        return parse_hour(raw) if raw else None

    def advance(self, hour: datetime) -> None:
        """Move the high-water mark forward. Never moves backward: an explicit
        re-run of an old hour is an idempotent overwrite, not a rewind."""
        current = self.last_ingested_hour()
        if current is not None and hour <= current:
            return
        self.path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.path.with_suffix(".tmp")
        tmp.write_text(json.dumps({"last_ingested_hour": format_hour(hour)}, indent=2) + "\n")
        tmp.replace(self.path)
