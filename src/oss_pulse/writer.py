"""Write projected rows to the hive-partitioned Parquet raw lake (bronze layer).

The schema is declared explicitly rather than inferred: inference from a batch
where a nullable column happens to be all-NULL would produce a different type
than the previous batch, silently breaking downstream readers. Explicit schemas
make every file in the lake type-identical.
"""

from __future__ import annotations

import os
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

EVENT_SCHEMA = pa.schema(
    [
        ("event_id", pa.string()),
        ("event_type", pa.string()),
        ("created_at", pa.timestamp("us", tz="UTC")),
        ("actor_id", pa.int64()),
        ("actor_login", pa.string()),
        ("repo_id", pa.int64()),
        ("repo_name", pa.string()),
        ("org_login", pa.string()),
        ("payload_action", pa.string()),
        ("pr_merged", pa.bool_()),
        ("pr_additions", pa.int64()),
        ("pr_deletions", pa.int64()),
        ("pr_changed_files", pa.int64()),
        ("repo_language", pa.string()),
        ("repo_stars", pa.int64()),
        ("push_distinct_size", pa.int64()),
        ("create_ref_type", pa.string()),
        ("ingested_at", pa.timestamp("us", tz="UTC")),
        ("source_file", pa.string()),
    ]
)


def partition_dir(raw_root: Path, hour: datetime) -> Path:
    """Deterministic hive-style partition path for one UTC hour.

    One hour → one path is the core of the pipeline's idempotency: re-running an
    hour overwrites the same file instead of appending a duplicate.
    """
    hour = hour.astimezone(UTC)
    return raw_root / f"event_date={hour:%Y-%m-%d}" / f"event_hour={hour:%H}"


def write_hour_parquet(
    rows: list[dict[str, Any]],
    raw_root: Path,
    hour: datetime,
    source_file: str,
    batch_rows: int = 100_000,
) -> Path:
    """Atomically write one hour of projected rows as a single Parquet file."""
    ingested_at = datetime.now(UTC)
    for row in rows:
        row["ingested_at"] = ingested_at
        row["source_file"] = source_file

    tables = [
        pa.Table.from_pylist(rows[i : i + batch_rows], schema=EVENT_SCHEMA)
        for i in range(0, len(rows), batch_rows)
    ] or [EVENT_SCHEMA.empty_table()]
    table = pa.concat_tables(tables)

    out_dir = partition_dir(raw_root, hour)
    out_dir.mkdir(parents=True, exist_ok=True)
    final = out_dir / "events.parquet"
    tmp = out_dir / "events.parquet.tmp"
    pq.write_table(table, tmp, compression="zstd")
    os.replace(tmp, final)
    return final
