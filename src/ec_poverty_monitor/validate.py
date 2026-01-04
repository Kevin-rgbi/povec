from __future__ import annotations

import pandas as pd


def validate_indicators(df: pd.DataFrame) -> list[str]:
    issues: list[str] = []
    if df.empty:
        issues.append("no indicators produced")
        return issues

    required = ["period_date", "series", "value", "source", "unit"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        issues.append(f"missing columns: {missing}")

    dup = df.duplicated(subset=["period_date", "series", "source"]).sum()
    if dup:
        issues.append(f"duplicate (period_date, series, source) rows: {dup}")

    pct = df[df["unit"] == "%"].copy()
    if not pct.empty and "series" in pct.columns:
        # Only enforce 0–100 for percentages that are rates/headcounts.
        # Percent-change series (e.g., growth) can legitimately be negative.
        bounded = pct[pct["series"].astype(str).str.contains(r"(?:rate|headcount)", case=False, regex=True)]
        if not bounded.empty:
            bad = bounded[(bounded["value"] < 0) | (bounded["value"] > 100)]
            if not bad.empty:
                issues.append(f"rate/headcount percent values out of bounds: {len(bad)}")

    return issues
