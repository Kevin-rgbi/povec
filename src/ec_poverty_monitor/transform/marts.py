from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from typing import Any

import duckdb
import pandas as pd


@dataclass(frozen=True)
class MartResult:
    indicators_path: str
    sources_path: str
    duckdb_path: str


def _to_quarter_end(d: dt.date) -> dt.date:
    q = (d.month - 1) // 3 + 1
    if q == 1:
        return dt.date(d.year, 3, 31)
    if q == 2:
        return dt.date(d.year, 6, 30)
    if q == 3:
        return dt.date(d.year, 9, 30)
    return dt.date(d.year, 12, 31)


def apply_comparability_break(
    df: pd.DataFrame,
    *,
    start: dt.date,
    end: dt.date,
) -> pd.DataFrame:
    out = df.copy()
    out["comparability_break"] = out["period_date"].apply(lambda d: start <= d <= end)
    return out


def canonicalize_period(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["period_date"] = pd.to_datetime(out["period_date"]).dt.date
    out["canonical_period_date"] = out["period_date"].apply(_to_quarter_end)
    return out


def build_duckdb_and_parquet(
    *,
    indicators: pd.DataFrame,
    sources: pd.DataFrame,
    duckdb_path: str,
    indicators_parquet: str,
    sources_parquet: str,
) -> MartResult:
    con = duckdb.connect(duckdb_path)

    con.register("indicators", indicators)
    con.register("sources", sources)

    con.execute("CREATE OR REPLACE TABLE mart_indicators AS SELECT * FROM indicators")
    con.execute("CREATE OR REPLACE TABLE mart_sources AS SELECT * FROM sources")

    con.execute(
        "COPY mart_indicators TO ? (FORMAT PARQUET)",
        [indicators_parquet],
    )
    con.execute(
        "COPY mart_sources TO ? (FORMAT PARQUET)",
        [sources_parquet],
    )

    con.close()

    return MartResult(indicators_path=indicators_parquet, sources_path=sources_parquet, duckdb_path=duckdb_path)


def stack_sources(source_lists: list[list[dict[str, Any]]]) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for lst in source_lists:
        rows.extend(lst)
    return pd.DataFrame(rows)
