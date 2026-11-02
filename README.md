# FactoryPulse Lakehouse

> Contract-aware bronze/silver/gold telemetry pipeline for FactoryPulse.

[![Lakehouse CI](https://github.com/david2526IA/factorypulse-lakehouse/actions/workflows/ci.yml/badge.svg)](https://github.com/david2526IA/factorypulse-lakehouse/actions/workflows/ci.yml)
[![DuckDB](https://img.shields.io/badge/DuckDB-lakehouse-FFF000?logo=duckdb&logoColor=111827)](https://duckdb.org/)
[![Contracts](https://img.shields.io/badge/uses-FactoryPulse%20Contracts-2563EB)](https://github.com/david2526IA/factorypulse-contracts)

FactoryPulse Lakehouse consumes telemetry JSON Lines from [FactoryPulse Ingestion](https://github.com/david2526IA/factorypulse-ingestion), validates every event against the shared contract, keeps rejected records with their reason and materialises analytical data in DuckDB and Parquet.

## Data flow

```text
factorypulse-ingestion JSONL
             │
             ▼
 contract validation ──► rejected_events (reason preserved)
             │
             ▼
bronze_events → silver_telemetry → gold_asset_health
                    │                    │
                    ▼                    ▼
                 Parquet          dashboard / intelligence
             │
             ▼
validated `data_quality.v1` run event
```

## Quick start

```bash
git clone https://github.com/david2526IA/factorypulse-lakehouse.git
cd factorypulse-lakehouse
python -m venv .venv
```

Create source data with the upstream repository, activate the environment, then:

```bash
pip install -e ".[dev]"
factorypulse-lakehouse ../factorypulse-ingestion/artifacts/telemetry.jsonl --warehouse warehouse
pytest -q
```

The generated warehouse contains:

- `warehouse.duckdb`: `bronze_events`, `rejected_events`, `silver_telemetry`, `gold_asset_health`.
- `silver_telemetry.parquet` and `gold_asset_health.parquet`.
- `data_quality_event.json`, validated against the shared contract.
- `run_summary.json`.

## Data-quality principle

Malformed JSON and contract-breaking events are quarantined, not silently discarded or repaired. This keeps data failures inspectable and lets consumers distinguish a real zero-reading interval from a pipeline problem.

## Container run

```bash
docker build -t factorypulse-lakehouse .
docker run --rm -v "${PWD}:/data" factorypulse-lakehouse /data/telemetry.jsonl --warehouse /data/warehouse
```

## Scope

This is a local integration reference. It demonstrates data contracts, quality boundaries, lineage-friendly run events and analytical layers. A production deployment would add object storage, orchestration, access control, observability and retention policies.

## License

MIT © 2026 David
