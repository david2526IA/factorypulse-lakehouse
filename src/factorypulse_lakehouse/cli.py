from __future__ import annotations

import argparse

from .pipeline import run_pipeline


def main() -> None:
    parser = argparse.ArgumentParser(prog="factorypulse-lakehouse")
    parser.add_argument("input", help="telemetry JSONL produced by factorypulse-ingestion")
    parser.add_argument("--warehouse", default="warehouse")
    args = parser.parse_args()
    print(run_pipeline(args.input, args.warehouse))


if __name__ == "__main__":
    main()
