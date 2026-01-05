import datetime as dt
import os
from pathlib import Path
import sys

import altair as alt
import duckdb
import pandas as pd
import streamlit as st

st.set_page_config(page_title="Ecuador Economy & Poverty Monitor", layout="wide")

st.title("Ecuador Economy & Poverty Monitor")

st.markdown(
    """
This dashboard reads local marts produced by the pipeline.

- Official published series only (no microdata ingestion)
- ENEMDU comparability break (2020–May 2021) is highlighted
"""
)

default_duckdb_path = os.environ.get("ECMON_DUCKDB_PATH", "data/mart/ecuador_monitor.duckdb")
duckdb_path = st.text_input("DuckDB path", value=default_duckdb_path)

duckdb_file = Path(duckdb_path)
if not duckdb_file.exists():
    autobuild = os.environ.get("ECMON_AUTOBUILD", "").strip().lower() in {"1", "true", "yes"}
    if autobuild:
        config_path = Path(os.environ.get("ECMON_CONFIG_PATH", "config/config.yaml"))
        st.info("DuckDB file not found; building marts now…")
        try:
            try:
                from ec_poverty_monitor.pipeline import run_pipeline
            except ModuleNotFoundError:
                # When running as `streamlit run src/.../app.py`, `src/` may not be on sys.path.
                sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
                from ec_poverty_monitor.pipeline import run_pipeline

            run_pipeline(config_path=config_path, force=False)
        except Exception as exc:
            st.error(f"Failed to build marts: {exc}")
            st.stop()
    else:
        st.info("Run the pipeline first, or set ECMON_AUTOBUILD=1 to build marts automatically.")
        st.stop()

try:
    con = duckdb.connect(duckdb_path, read_only=True)
    indicators = con.execute("SELECT * FROM mart_indicators").df()
    con.close()
except Exception:
    st.info("Run the pipeline first: `poetry run ecmon --config config/config.yaml`")
    st.stop()

if indicators.empty:
    st.warning("No indicators found in mart_indicators.")
    st.stop()

indicators["period_date"] = pd.to_datetime(indicators["period_date"])

series_list = sorted(indicators["series"].dropna().unique().tolist())
selected = st.multiselect("Series", options=series_list, default=series_list[:1])

view = indicators[indicators["series"].isin(selected)].copy()
view = view.sort_values(["series", "period_date"])

units = (
    view[["series", "unit"]]
    .dropna(subset=["unit"])
    .drop_duplicates()["unit"]
    .unique()
    .tolist()
)

if len(selected) == 1:
    selected_unit = units[0] if len(units) == 1 else None
    y_title = f"{selected[0]}" + (f" ({selected_unit})" if selected_unit else "")
elif len(units) == 1:
    y_title = f"Value ({units[0]})"
else:
    y_title = "Value"

break_start = pd.to_datetime(dt.date(2020, 1, 1))
break_end = pd.to_datetime(dt.date(2021, 5, 31))

if "comparability_break" in view.columns and view["comparability_break"].fillna(False).any():
    st.warning("Selected data includes the ENEMDU comparability window (2020–May 2021).")

base = alt.Chart(view).encode(
    x=alt.X("period_date:T", title="Date"),
    y=alt.Y("value:Q", title=y_title),
    color=alt.Color("series:N", title="Series"),
    tooltip=["series", alt.Tooltip("period_date:T"), "value", "unit", "source"],
)

shade = (
    alt.Chart(pd.DataFrame({"start": [break_start], "end": [break_end]}))
    .mark_rect(opacity=0.15)
    .encode(x="start:T", x2="end:T")
)

lines = base.mark_line()

st.altair_chart(shade + lines, use_container_width=True)
