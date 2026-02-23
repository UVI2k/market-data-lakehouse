# src/build_sector_indexes_silver.py
from __future__ import annotations

from pathlib import Path
from typing import List, Optional

import pandas as pd

from src.utils.config import load_config


def latest_run_folder(raw_base: Path) -> Path:
    run_folders = sorted([p for p in raw_base.glob("run_date=*") if p.is_dir()])
    if not run_folders:
        raise FileNotFoundError(f"No run_date folders found in {raw_base}")
    return run_folders[-1]


def load_run_parquets(run_folder: Path) -> pd.DataFrame:
    files = sorted(run_folder.glob("*.parquet"))
    if not files:
        raise FileNotFoundError(f"No parquet files found in {run_folder}")

    dfs: List[pd.DataFrame] = []
    for fp in files:
        dfs.append(pd.read_parquet(fp))

    return pd.concat(dfs, ignore_index=True)


def normalize_schema(df: pd.DataFrame, expected_cols: List[str]) -> pd.DataFrame:
    # Ensure expected columns exist
    for c in expected_cols:
        if c not in df.columns:
            df[c] = pd.NA

    # Keep only expected columns (stable order)
    df = df[expected_cols].copy()

    # Types (minimal safe casting)
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.tz_localize(None)

    for c in ["open", "high", "low", "close", "adj_close", "volume"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    return df


def upsert(existing: Optional[pd.DataFrame], new: pd.DataFrame, dedup_keys: List[str], sort_keys: List[str]) -> pd.DataFrame:
    if existing is None or existing.empty:
        combined = new.copy()
    else:
        combined = pd.concat([existing, new], ignore_index=True)

    combined = (
        combined.drop_duplicates(subset=dedup_keys, keep="last")
                .sort_values(sort_keys)
                .reset_index(drop=True)
    )
    return combined


def main() -> None:
    cfg = load_config("configs/config.yaml")

    # Paths
    bronze_base = Path(cfg["paths"]["bronze"]["sector_indexes_raw"])
    silver_path = Path(cfg["paths"]["silver"]["sector_indexes"])

    # Silver rules
    expected_cols = cfg["silver"]["sector_indexes"]["expected_columns"]
    dedup_keys = cfg["silver"]["sector_indexes"]["dedup_keys"]
    sort_keys = cfg["silver"]["sector_indexes"]["sort_keys"]

    run_folder = latest_run_folder(bronze_base)
    print(f"Using RAW run folder: {run_folder}")

    raw_df = load_run_parquets(run_folder)
    raw_df = normalize_schema(raw_df, expected_cols)

    # Basic sanity: drop rows missing essential keys
    raw_df = raw_df.dropna(subset=["symbol", "date", "close"])

    silver_path.parent.mkdir(parents=True, exist_ok=True)

    existing = None
    if silver_path.exists():
        existing = pd.read_parquet(silver_path)

    final_df = upsert(existing, raw_df, dedup_keys=dedup_keys, sort_keys=sort_keys)
    final_df.to_parquet(silver_path, index=False)

    print(f"✅ Silver table written: {silver_path}")
    print(f"   Rows added this run: {len(raw_df):,}")
    print(f"   Total rows in silver: {len(final_df):,}")
    if "symbol" in final_df.columns:
        print(f"   Symbols: {sorted(final_df['symbol'].dropna().unique().tolist())}")


if __name__ == "__main__":
    main()