"""Streamlit dashboard for the Irish Property Price Register pipeline.

Reads the dbt-built property_price_summary table from BigQuery and
presents it as: headline metrics, a price trend chart, a county-level
map, and the underlying data table.
"""

import pandas as pd
import plotly.express as px
import streamlit as st

from county_coordinates import COUNTY_COORDINATES
from data import load_price_summary

st.set_page_config(page_title="Irish Property Price Register", layout="wide")
st.title("Irish Property Price Register")

df = load_price_summary()

all_counties = sorted(df["county"].unique())
all_property_types = sorted(df["property_type"].unique())

default_counties = [c for c in ("Dublin", "Cork", "Galway") if c in all_counties] or all_counties[:3]

st.sidebar.header("Filters")
selected_counties = st.sidebar.multiselect("Counties", options=all_counties, default=default_counties)
selected_property_types = st.sidebar.multiselect(
    "Property type", options=all_property_types, default=all_property_types
)

filtered = df[
    df["county"].isin(selected_counties) & df["property_type"].isin(selected_property_types)
]

if filtered.empty:
    st.warning("No data for the current filter selection. Pick at least one county and property type.")
    st.stop()

# --- Headline metrics (only meaningful for a single selected county) ---
if len(selected_counties) == 1:
    county_df = filtered[filtered["county"] == selected_counties[0]]
    latest_year = county_df["year"].max()
    latest_rows = county_df[county_df["year"] == latest_year]

    st.subheader(f"{selected_counties[0]} — {latest_year}")
    cols = st.columns(len(latest_rows))
    for col, (_, row) in zip(cols, latest_rows.iterrows()):
        change = row["median_price_change_pct"]
        col.metric(
            label=row["property_type"],
            value=f"€{row['median_price_eur']:,.0f}",
            delta=f"{change:+.1f}% YoY" if pd.notna(change) else "first year on record",
        )
elif len(selected_counties) > 1:
    st.caption("Select exactly one county in the sidebar to see its headline price metrics.")

# --- Trend chart ---
st.subheader("Median price over time")
trend_fig = px.line(
    filtered,
    x="year",
    y="median_price_eur",
    color="county",
    line_dash="property_type",
    markers=True,
    labels={"median_price_eur": "Median price (EUR)", "year": "Year"},
)
st.plotly_chart(trend_fig, use_container_width=True)

# --- Map view ---
st.subheader("Median price by county (latest year, all selected property types)")
map_latest_year = filtered["year"].max()
map_df = filtered[filtered["year"] == map_latest_year].copy()
map_df["_weighted_price"] = map_df["median_price_eur"] * map_df["transaction_count"]
county_agg = (
    map_df.groupby("county", as_index=False)
    .agg(_weighted_price_sum=("_weighted_price", "sum"), total_transactions=("transaction_count", "sum"))
)
county_agg["weighted_median_price_eur"] = (
    county_agg["_weighted_price_sum"] / county_agg["total_transactions"]
)
county_agg["lat"] = county_agg["county"].map(lambda c: COUNTY_COORDINATES.get(c, (None, None))[0])
county_agg["lon"] = county_agg["county"].map(lambda c: COUNTY_COORDINATES.get(c, (None, None))[1])
county_agg = county_agg.dropna(subset=["lat", "lon"])

map_fig = px.scatter_geo(
    county_agg,
    lat="lat",
    lon="lon",
    size="weighted_median_price_eur",
    color="weighted_median_price_eur",
    hover_name="county",
    hover_data={"weighted_median_price_eur": ":.0f", "total_transactions": True, "lat": False, "lon": False},
    color_continuous_scale="YlOrRd",
    labels={"weighted_median_price_eur": "Median price (EUR)"},
)
map_fig.update_geos(
    lataxis_range=[51, 55.5],
    lonaxis_range=[-10.5, -5.5],
    showcountries=True,
    showland=True,
    landcolor="rgb(235, 235, 230)",
)
map_fig.update_layout(margin={"r": 0, "t": 0, "l": 0, "b": 0}, height=500)
st.plotly_chart(map_fig, use_container_width=True)

# --- Data table ---
st.subheader("Underlying data")
st.dataframe(filtered.sort_values(["county", "property_type", "year"]), use_container_width=True)
