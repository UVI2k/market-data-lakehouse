from __future__ import annotations

from pathlib import Path
import pandas as pd
import streamlit as st

from src.utils.config import load_config


st.set_page_config(page_title="Sector Rotation Dashboard", layout="wide")


@st.cache_data
def load_rankings(parquet_path: str) -> pd.DataFrame:
    path = Path(parquet_path)
    if not path.exists():
        raise FileNotFoundError(f"Gold rankings file not found: {path}")
    df = pd.read_parquet(path)
    df["week_end"] = pd.to_datetime(df["week_end"])
    return df


def main() -> None:
    st.title("📈 Sector Rotation Dashboard")
    st.caption("Built from your Bronze → Silver → Gold data pipeline")

    # Load config
    cfg = load_config("configs/config.yaml")
    gold_path = cfg["paths"]["gold"]["weekly_sector_rankings"]
    default_top_n = int(cfg["gold"]["sector_rankings"]["top_n"])
    lookback_weeks = int(cfg["gold"]["sector_rankings"]["lookback_weeks"])

    # Load gold dataset
    try:
        rankings = load_rankings(gold_path)
    except Exception as e:
        st.error(f"Failed to load rankings: {e}")
        st.stop()

    # Sidebar controls
    st.sidebar.header("Controls")

    available_weeks = sorted(rankings["week_end"].dropna().unique(), reverse=True)
    latest_week = available_weeks[0]

    selected_week = st.sidebar.selectbox(
        "Select week ending",
        options=available_weeks,
        index=0,
        format_func=lambda d: pd.to_datetime(d).date().isoformat(),
    )

    top_n = st.sidebar.slider("Top N sectors", min_value=3, max_value=11, value=default_top_n)

    # Column names depend on lookback (ret_12w, vol_12w, dd_12w)
    ret_col = f"ret_{lookback_weeks}w"
    vol_col = f"vol_{lookback_weeks}w"
    dd_col = f"dd_{lookback_weeks}w"

    # Filter to selected week
    week_df = rankings[rankings["week_end"] == selected_week].copy()
    top_df = week_df[week_df["rank"] <= top_n].sort_values("rank")

    # Header summary
    st.subheader("🏆 Sector Leaderboard")
    st.write(
        f"**Week ending:** {pd.to_datetime(selected_week).date()}  |  "
        f"**Lookback:** {lookback_weeks} weeks  |  "
        f"**Rows:** {len(week_df):,} sectors"
    )

    # Quick context (data range)
    c0, c1, c2, c3 = st.columns(4)
    c0.metric("Latest available week", str(pd.to_datetime(latest_week).date()))
    c1.metric("Best Sector", top_df.iloc[0]["sector"])
    c2.metric("Best Score", f'{top_df.iloc[0]["score"]:.4f}')
    c3.metric("Best Return", f'{top_df.iloc[0][ret_col]*100:.2f}%')

    # Table
    st.dataframe(
        top_df[["rank", "sector", "symbol", ret_col, vol_col, dd_col, "score"]]
        .rename(columns={
            ret_col: f"Return ({lookback_weeks}w)",
            vol_col: f"Volatility ({lookback_weeks}w)",
            dd_col: f"Drawdown ({lookback_weeks}w)",
        }),
        use_container_width=True,
        hide_index=True,
    )

    # Charts
    st.subheader("📊 Score & Return (Top Sectors)")

    left, right = st.columns(2)

    with left:
        score_series = top_df.set_index("sector")["score"]
        st.bar_chart(score_series)

    with right:
        ret_series = top_df.set_index("sector")[ret_col]
        st.bar_chart(ret_series)

    st.subheader("📉 Sector Trend")

    # Sector selector
    sector_list = sorted(rankings["sector"].dropna().unique())
    selected_sector = st.selectbox("Select sector for trend", sector_list)

    sector_df = rankings[rankings["sector"] == selected_sector].sort_values("week_end")

    c1, c2 = st.columns(2)

    with c1:
        st.write("Weekly Close")
        price_series = sector_df.set_index("week_end")["weekly_close"]
        st.line_chart(price_series)

    with c2:
        st.write("Rank Over Time (lower = better)")
        rank_series = sector_df.set_index("week_end")["rank"]
        st.line_chart(rank_series)

if __name__ == "__main__":
    main()