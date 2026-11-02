"""Build traceable bronze/silver/gold layers from FactoryPulse telemetry JSONL."""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from pathlib import Path

import duckdb
from jsonschema import ValidationError

from factorypulse_contracts import validate_event


def run_pipeline(source_jsonl: str | Path, warehouse: str | Path) -> dict[str, object]:
    source = Path(source_jsonl)
    root = Path(warehouse)
    root.mkdir(parents=True, exist_ok=True)
    valid: list[dict[str, object]] = []
    rejected: list[tuple[int, str, str]] = []
    raw_lines = source.read_text(encoding="utf-8").splitlines()
    for line_number, raw in enumerate(raw_lines, start=1):
        try:
            event = json.loads(raw)
            validate_event("telemetry.v1", event)
            valid.append(event)
        except (json.JSONDecodeError, ValidationError, TypeError, ValueError) as error:
            rejected.append((line_number, raw, str(error)))

    connection = duckdb.connect(str(root / "warehouse.duckdb"))
    try:
        # Bronze preserves the raw payload verbatim, including malformed JSON.
        connection.execute("CREATE OR REPLACE TABLE bronze_events(line_number INTEGER, raw_event VARCHAR)")
        connection.executemany("INSERT INTO bronze_events VALUES (?, ?)", [(index, line) for index, line in enumerate(raw_lines, start=1)])
        connection.execute("CREATE OR REPLACE TABLE rejected_events(line_number INTEGER, raw_event VARCHAR, rejection_reason VARCHAR)")
        if rejected:
            connection.executemany("INSERT INTO rejected_events VALUES (?, ?, ?)", rejected)
        connection.execute(
            """
            CREATE OR REPLACE TABLE silver_telemetry (
              event_id UUID, occurred_at TIMESTAMPTZ, asset_id VARCHAR,
              temperature_c DOUBLE, vibration_mm_s DOUBLE, pressure_bar DOUBLE, power_kw DOUBLE, source VARCHAR
            )
            """
        )
        silver_rows = [(
                event["event_id"], event["occurred_at"], event["asset_id"], event["measurements"].get("temperature_c"),
                event["measurements"].get("vibration_mm_s"), event["measurements"].get("pressure_bar"),
                event["measurements"].get("power_kw"), event.get("source"),
            ) for event in valid]
        if silver_rows:
            connection.executemany("INSERT INTO silver_telemetry VALUES (?, ?, ?, ?, ?, ?, ?, ?)", silver_rows)
        connection.execute(
            """
            CREATE OR REPLACE TABLE gold_asset_health AS
            SELECT date_trunc('hour', occurred_at) AS hour, asset_id, count(*) AS readings,
              round(avg(temperature_c), 3) AS avg_temperature_c, round(avg(vibration_mm_s), 3) AS avg_vibration_mm_s,
              round(avg(pressure_bar), 3) AS avg_pressure_bar, round(avg(power_kw), 3) AS avg_power_kw,
              sum(CASE WHEN temperature_c >= 92 OR vibration_mm_s >= 11 THEN 1 ELSE 0 END) AS critical_signal_count
            FROM silver_telemetry GROUP BY hour, asset_id ORDER BY hour, asset_id
            """
        )
        silver_path = str(root / "silver_telemetry.parquet").replace("'", "''")
        gold_path = str(root / "gold_asset_health.parquet").replace("'", "''")
        connection.execute(f"COPY silver_telemetry TO '{silver_path}' (FORMAT PARQUET)")
        connection.execute(f"COPY gold_asset_health TO '{gold_path}' (FORMAT PARQUET)")
        quality_event = {
            "event_id": str(uuid.uuid4()), "event_type": "factorypulse.data_quality", "schema_version": "1.0",
            "occurred_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            "pipeline": "factorypulse-lakehouse", "accepted_rows": len(valid), "rejected_rows": len(rejected),
            "run_ref": str(source),
        }
        validate_event("data-quality.v1", quality_event)
        (root / "data_quality_event.json").write_text(json.dumps(quality_event, indent=2), encoding="utf-8")
        summary = {"source_rows": len(raw_lines), "accepted_rows": len(valid), "rejected_rows": len(rejected), "warehouse": str(root)}
        (root / "run_summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
        return summary
    finally:
        connection.close()
