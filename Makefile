.PHONY: install lint test ingest backfill dbt-build dbt-test dbt-docs

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
## (--max-hours raised past the default 24h cap so multi-day backfills work)
backfill:
	python -m oss_pulse.ingest --start $(START) --end $(END) --max-hours 9000

## Build + test every dbt model (staging -> intermediate -> marts) on DuckDB
dbt-build:
	mkdir -p data/warehouse
	cd transform && dbt build --profiles-dir .

## Run only the dbt tests
dbt-test:
	cd transform && dbt test --profiles-dir .

## Generate and serve the dbt documentation site (http://localhost:8080)
dbt-docs:
	cd transform && dbt docs generate --profiles-dir . && dbt docs serve --profiles-dir .
