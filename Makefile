.PHONY: install lint test ingest backfill

install:
	pip install -e ".[dev]"

lint:
	ruff check .

test:
	pytest -q

## Incremental run: from the high-water mark to the newest published hour
ingest:
	python -m oss_pulse.ingest

## Explicit range: make backfill START=2026-07-01T00 END=2026-07-01T23
backfill:
	python -m oss_pulse.ingest --start $(START) --end $(END)
