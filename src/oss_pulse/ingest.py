"""CLI entrypoint: incremental, idempotent ingestion of GH Archive hours.

Incrementality: a high-water-mark state file records the last fully ingested UTC
hour. Each run processes hours (mark, latest_published], where latest_published
is now minus a publication-lag buffer, capped at --max-hours per run.

Idempotency: each hour maps to one deterministic partition path that is
atomically overwritten, so re-running any range cannot duplicate data.

Error handling: downloads retry with exponential backoff; a 404 (hour not yet
published) ends the run cleanly WITHOUT advancing the mark; malformed records
are quarantined with their failure reason, never silently dropped.
"""

from __future__ import annotations

import argparse
import gzip
import json
import logging
import os
import sys
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path

from .http import FileNotPublishedError, download_file
from .logging_utils import get_logger, log
from .parse import MalformedRecordError, parse_line
from .state import IngestState, format_hour, parse_hour
from .writer import partition_dir, write_hour_parquet

logger = get_logger("oss_pulse.ingest")

GHARCHIVE_URL = "https://data.gharchive.org/{d:%Y-%m-%d}-{h}.json.gz"
# GH Archive publishes an hour's file shortly after the hour closes; a 2h buffer
# keeps scheduled runs from hammering 404s at the publication boundary.
PUBLICATION_LAG_HOURS = 2
FIRST_RUN_LOOKBACK_HOURS = 3
QUARANTINE_RAW_MAX_CHARS = 10_000


def gharchive_url(hour: datetime) -> str:
    hour = hour.astimezone(UTC)
    return GHARCHIVE_URL.format(d=hour, h=hour.hour)  # hour is NOT zero-padded upstream


def floor_hour(dt: datetime) -> datetime:
    return dt.replace(minute=0, second=0, microsecond=0)


def plan_hours(
    state: IngestState,
    start: datetime | None,
    end: datetime | None,
    max_hours: int,
    now: datetime | None = None,
) -> list[datetime]:
    """Compute the inclusive list of UTC hours this run should process."""
    now = now or datetime.now(UTC)
    latest_published = floor_hour(now) - timedelta(hours=PUBLICATION_LAG_HOURS)

    if start is None:
        mark = state.last_ingested_hour()
        if mark is not None:
            start = mark + timedelta(hours=1)
        else:
            start = latest_published - timedelta(hours=FIRST_RUN_LOOKBACK_HOURS - 1)
    end = min(end or latest_published, start + timedelta(hours=max_hours - 1))

    hours: list[datetime] = []
    cursor = start
    while cursor <= end:
        hours.append(cursor)
        cursor += timedelta(hours=1)
    return hours


def quarantine_path(data_dir: Path, hour: datetime) -> Path:
    hour = hour.astimezone(UTC)
    return (
        data_dir
        / "quarantine"
        / "gharchive"
        / f"event_date={hour:%Y-%m-%d}"
        / f"bad_records_hour={hour:%H}.ndjson"
    )


def ingest_hour(hour: datetime, data_dir: Path, keep_gz: bool = False) -> dict:
    """Download, parse, and land one UTC hour. Returns run metrics."""
    t0 = time.monotonic()
    url = gharchive_url(hour)
    raw_root = data_dir / "raw" / "gharchive" / "events"
    gz_dir = data_dir / "tmp"
    gz_path = gz_dir / Path(url).name

    gz_bytes = download_file(url, gz_path)

    rows: list[dict] = []
    bad: list[dict] = []
    filtered = 0
    with gzip.open(gz_path, "rt", encoding="utf-8", errors="replace") as f:
        for line_no, line in enumerate(f, start=1):
            if not line.strip():
                continue
            try:
                row = parse_line(line)
            except MalformedRecordError as err:
                bad.append(
                    {
                        "source_file": url,
                        "line_number": line_no,
                        "reason": err.reason,
                        "raw": line[:QUARANTINE_RAW_MAX_CHARS],
                    }
                )
                continue
            if row is None:
                filtered += 1
            else:
                rows.append(row)

    out_path = write_hour_parquet(rows, raw_root, hour, source_file=url)

    # Quarantine is rewritten per hour (idempotent, like the data itself).
    q_path = quarantine_path(data_dir, hour)
    if bad:
        q_path.parent.mkdir(parents=True, exist_ok=True)
        tmp = q_path.with_suffix(".tmp")
        with open(tmp, "w", encoding="utf-8") as f:
            for record in bad:
                f.write(json.dumps(record) + "\n")
        os.replace(tmp, q_path)
    else:
        q_path.unlink(missing_ok=True)

    if not keep_gz:
        gz_path.unlink(missing_ok=True)

    return {
        "hour": format_hour(hour),
        "gz_bytes": gz_bytes,
        "rows_kept": len(rows),
        "rows_filtered": filtered,
        "rows_quarantined": len(bad),
        "parquet_bytes": out_path.stat().st_size,
        "parquet_path": str(out_path),
        "duration_s": round(time.monotonic() - t0, 1),
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start", type=parse_hour, help="first UTC hour, e.g. 2026-07-01T00")
    parser.add_argument("--end", type=parse_hour, help="last UTC hour (inclusive)")
    parser.add_argument("--max-hours", type=int, default=24, help="cap hours per run")
    parser.add_argument("--data-dir", type=Path, default=Path("data"))
    parser.add_argument("--keep-gz", action="store_true", help="keep downloaded .json.gz files")
    args = parser.parse_args(argv)

    state = IngestState(args.data_dir / "state" / "ingest_state.json")
    hours = plan_hours(state, args.start, args.end, args.max_hours)
    if not hours:
        log(logger, logging.INFO, "nothing_to_do", last_ingested=str(state.last_ingested_hour()))
        return 0

    log(
        logger,
        logging.INFO,
        "run_planned",
        first_hour=format_hour(hours[0]),
        last_hour=format_hour(hours[-1]),
        n_hours=len(hours),
    )

    processed = 0
    for hour in hours:
        try:
            metrics = ingest_hour(hour, args.data_dir, keep_gz=args.keep_gz)
        except FileNotPublishedError:
            # Not an error: the hour simply isn't out yet. Stop without
            # advancing the mark; the next run retries from this hour.
            log(logger, logging.INFO, "hour_not_published_yet", hour=format_hour(hour))
            break
        # State advances only after the hour's parquet is durably in place.
        state.advance(hour)
        processed += 1
        log(logger, logging.INFO, "hour_complete", **metrics)

    log(
        logger,
        logging.INFO,
        "run_complete",
        hours_processed=processed,
        last_ingested=str(state.last_ingested_hour()),
        raw_partition_example=str(partition_dir(args.data_dir / "raw", hours[0])),
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
