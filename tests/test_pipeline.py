import json
import uuid

import duckdb

from factorypulse_lakehouse.pipeline import run_pipeline


def test_pipeline_builds_layers_and_quality_event(tmp_path) -> None:
    source = tmp_path / "telemetry.jsonl"
    events = []
    for index in range(10):
        events.append({
            "event_id": str(uuid.uuid4()), "event_type": "factorypulse.telemetry", "schema_version": "1.0",
            "occurred_at": f"2026-01-01T00:{index:02d}:00Z", "asset_id": "PUMP-01",
            "measurements": {"temperature_c": 62 + index, "vibration_mm_s": 2.5 + index / 10, "pressure_bar": 7.4, "power_kw": 18.0},
            "source": "test",
        })
    source.write_text("\n".join(json.dumps(event) for event in events), encoding="utf-8")
    result = run_pipeline(source, tmp_path / "warehouse")
    assert result["accepted_rows"] == 10
    assert result["rejected_rows"] == 0
    event = json.loads((tmp_path / "warehouse" / "data_quality_event.json").read_text())
    assert event["event_type"] == "factorypulse.data_quality"
    connection = duckdb.connect(str(tmp_path / "warehouse" / "warehouse.duckdb"))
    assert connection.execute("SELECT count(*) FROM gold_asset_health").fetchone()[0] == 1
    connection.close()


def test_pipeline_quarantines_invalid_json_and_contracts(tmp_path) -> None:
    source = tmp_path / "telemetry.jsonl"
    source.write_text("not-json\n{\"event_type\":\"wrong\"}\n", encoding="utf-8")
    result = run_pipeline(source, tmp_path / "warehouse")
    assert result["accepted_rows"] == 0
    assert result["rejected_rows"] == 2
