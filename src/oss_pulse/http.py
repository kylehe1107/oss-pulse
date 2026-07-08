"""HTTP download with bounded exponential-backoff retries.

Hand-rolled rather than pulling in a retry library: the policy is ~20 lines,
dependency-free, and every behavior is explicit and defensible.
"""

from __future__ import annotations

import logging
import os
import random
import time
from pathlib import Path

import requests

from .logging_utils import get_logger, log

logger = get_logger("oss_pulse.http")

RETRYABLE_STATUS = {429, 500, 502, 503, 504}


class FileNotPublishedError(Exception):
    """The requested GH Archive hour does not exist (HTTP 404).

    GH Archive publishes each hour's file shortly after the hour closes. A 404
    almost always means "not published yet", so we surface it as a distinct,
    non-retryable condition: the caller stops cleanly without advancing the
    high-water mark, and the next scheduled run picks the hour up.
    """


def download_file(
    url: str,
    dest: Path,
    max_attempts: int = 5,
    backoff_base_s: float = 2.0,
    timeout_s: float = 120.0,
    chunk_bytes: int = 1 << 20,
) -> int:
    """Download ``url`` to ``dest`` atomically. Returns bytes written.

    - 404 raises FileNotPublishedError immediately (retrying can't create the file).
    - Retryable statuses (429/5xx) and connection/timeout errors get exponential
      backoff with jitter: ~2s, 4s, 8s, 16s between attempts.
    - Streams to ``dest.tmp`` then ``os.replace`` so a crashed download can never
      be mistaken for a complete file.
    """
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_suffix(dest.suffix + ".tmp")
    last_err: Exception | None = None

    for attempt in range(1, max_attempts + 1):
        try:
            with requests.get(url, stream=True, timeout=timeout_s) as resp:
                if resp.status_code == 404:
                    raise FileNotPublishedError(url)
                if resp.status_code in RETRYABLE_STATUS:
                    raise requests.HTTPError(
                        f"retryable status {resp.status_code}", response=resp
                    )
                resp.raise_for_status()
                written = 0
                with open(tmp, "wb") as f:
                    for chunk in resp.iter_content(chunk_size=chunk_bytes):
                        f.write(chunk)
                        written += len(chunk)
            os.replace(tmp, dest)
            log(logger, logging.INFO, "download_complete", url=url, bytes=written, attempt=attempt)
            return written
        except FileNotPublishedError:
            tmp.unlink(missing_ok=True)
            raise
        except (requests.RequestException, OSError) as err:
            tmp.unlink(missing_ok=True)
            last_err = err
            if attempt == max_attempts:
                break
            sleep_s = backoff_base_s**attempt + random.uniform(0, 1)
            log(
                logger,
                logging.WARNING,
                "download_retry",
                url=url,
                attempt=attempt,
                error=str(err),
                sleep_s=round(sleep_s, 1),
            )
            time.sleep(sleep_s)

    raise RuntimeError(f"download failed after {max_attempts} attempts: {url}") from last_err
