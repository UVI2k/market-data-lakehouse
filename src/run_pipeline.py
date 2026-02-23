# src/run_pipeline.py
from __future__ import annotations

import subprocess
import sys


def run_step(module: str, name: str) -> None:
    print(f"\n===== Running: {name} =====")
    result = subprocess.run([sys.executable, "-m", module])
    if result.returncode != 0:
        raise RuntimeError(f"❌ Step failed: {name}")


def main() -> None:
    run_step("src.ingest_sector_indexes_yf", "Bronze Ingestion")
    run_step("src.build_sector_indexes_silver", "Silver Build")
    run_step("src.quality.check_sector_indexes", "Quality Checks")
    run_step("src.build_sector_rankings_gold", "Gold Build")

    print("\n🚀 Pipeline completed successfully")


if __name__ == "__main__":
    main()