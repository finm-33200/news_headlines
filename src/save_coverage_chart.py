"""Generate a Plotly HTML chart of the daily RavenPack match rate.

Combines newswire and GDELT crosswalks to show what fraction of RavenPack
headlines are matched by scraped sources each day.
"""

from pathlib import Path

import plotly.graph_objects as go
import polars as pl

from settings import config

DATA_DIR = Path(config("DATA_DIR"))
OUTPUT_DIR = Path(config("OUTPUT_DIR"))


def main():
    # Load crosswalks
    nw_cw = pl.read_parquet(DATA_DIR / "newswire_ravenpack_crosswalk.parquet")
    gd_cw = pl.read_parquet(DATA_DIR / "gdelt_ravenpack_crosswalk.parquet")

    # Date range from newswire crosswalk
    date_min = nw_cw["date"].min()
    date_max = nw_cw["date"].max()

    # Load RavenPack headlines in the crosswalk date range
    rp = (
        pl.scan_parquet(DATA_DIR / "ravenpack_djpr.parquet")
        .with_columns(pl.col("timestamp_utc").cast(pl.Date).alias("date"))
        .filter((pl.col("date") >= date_min) & (pl.col("date") <= date_max))
        .select("date", "rp_story_id")
        .collect()
    )

    rp_daily = rp.group_by("date").agg(pl.len().alias("rp_total")).sort("date")

    # Union of matched RP story IDs from both crosswalks
    combined_matched = pl.concat(
        [
            nw_cw.select("date", "rp_story_id"),
            gd_cw.select("date", "rp_story_id"),
        ]
    ).unique(subset=["date", "rp_story_id"])

    combined_daily = (
        combined_matched.group_by("date")
        .agg(pl.col("rp_story_id").n_unique().alias("matched"))
        .sort("date")
    )

    match_rate = (
        rp_daily.join(combined_daily, on="date", how="left")
        .with_columns(pl.col("matched").fill_null(0))
        .with_columns((pl.col("matched") / pl.col("rp_total") * 100).alias("match_pct"))
        .sort("date")
    )

    MA_WINDOW = 5

    # Apply 5-day moving average
    match_rate_pd = match_rate.to_pandas().set_index("date").sort_index()
    match_rate_pd["match_pct_ma"] = match_rate_pd["match_pct"].rolling(MA_WINDOW).mean()

    # Build Plotly chart
    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=match_rate_pd.index.tolist(),
            y=match_rate_pd["match_pct_ma"].tolist(),
            mode="lines",
            line=dict(color="steelblue", width=1),
            name="Match rate",
        )
    )
    fig.update_layout(
        title=f"RavenPack Match Rate ({MA_WINDOW}-day MA, newswire + GDELT combined)",
        xaxis_title="Date",
        yaxis_title="% of RP Headlines Matched",
        yaxis=dict(rangemode="tozero"),
        template="plotly_white",
    )

    output_path = OUTPUT_DIR / "rp_match_rate_coverage.html"
    fig.write_html(str(output_path))
    print(f"Saved chart to {output_path}")


if __name__ == "__main__":
    main()
