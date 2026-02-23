# src/quality/check_sector_indexes.py
from __future__ import annotations

from pathlib import Path
import pandas as pd

from src.utils.config import load_config


def check_duplicates(df: pd.DataFrame, keys: list[str]) -> None:
    dup = df.duplicated(subset=keys).sum()
    if dup > 0:
        raise ValueError(f"❌ Duplicate rows found for keys {keys}: {dup}")
    print("✅ No duplicate keys")


def check_missing_sector(df: pd.DataFrame) -> None:
    missing = df["sector"].isna().sum()
    if missing > 0:
        raise ValueError(f"❌ Missing sector values: {missing}")
    print("✅ No missing sector")


def check_non_negative(df: pd.DataFrame, cols: list[str]) -> None:
    for c in cols:
        if c in df.columns:
            bad = (df[c] < 0).sum()
            if bad > 0:
                raise ValueError(f"❌ Negative values in {c}: {bad}")
    print("✅ No negative numeric values")


def check_freshness(df: pd.DataFrame, max_days: int) -> None:
    latest = pd.to_datetime(df["date"]).max()
    days_old = (pd.Timestamp.now(tz="UTC") - latest.tz_localize("UTC")).days
    if days_old > max_days:
        raise ValueError(f"❌ Data too old: {days_old} days")
    print(f"✅ Freshness OK (latest date {latest.date()})")


def main() -> None:
    cfg = load_config("configs/config.yaml")

    silver_path = Path(cfg["paths"]["silver"]["sector_indexes"])
    dedup_keys = cfg["silver"]["sector_indexes"]["dedup_keys"]
    freshness_days = cfg["quality"]["sector_indexes"]["freshness_days"]
    non_neg_cols = cfg["quality"]["sector_indexes"]["non_negative_columns"]

    df = pd.read_parquet(silver_path)

    print("\n--- Running Quality Checks ---")
    check_duplicates(df, dedup_keys)
    check_missing_sector(df)
    check_non_negative(df, non_neg_cols)
    check_freshness(df, freshness_days)

    print("\n🎉 All quality checks passed")


if __name__ == "__main__":
    main()