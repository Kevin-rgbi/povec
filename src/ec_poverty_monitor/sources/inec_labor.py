from __future__ import annotations

import datetime as dt
import re
from dataclasses import dataclass
from typing import Any

import pandas as pd

from ec_poverty_monitor.sources.inec_wp import WpPost, extract_urls, fetch_posts_multi
from ec_poverty_monitor.util.text import parse_float_maybe


@dataclass(frozen=True)
class LaborExtract:
    indicators: pd.DataFrame
    sources: list[dict[str, Any]]


_MONTH_ABBR = {
    "ene": 1,
    "feb": 2,
    "mar": 3,
    "abr": 4,
    "may": 5,
    "jun": 6,
    "jul": 7,
    "ago": 8,
    "sep": 9,
    "oct": 10,
    "nov": 11,
    "dic": 12,
}


def _parse_period(col: str) -> dt.date | None:
    s = str(col).strip().lower()
    s = s.replace("–", "-").replace("—", "-")

    # Formats observed: "oct-25", "oct-2025", sometimes with spaces.
    m = re.match(r"^([a-z]{3})\s*-\s*(\d{2}|\d{4})$", s)
    if not m:
        return None

    mon = _MONTH_ABBR.get(m.group(1))
    if mon is None:
        return None

    year_str = m.group(2)
    year = int(year_str)
    if year < 100:
        year = 2000 + year

    # Use month-end date.
    if mon == 12:
        return dt.date(year, 12, 31)
    next_month = dt.date(year, mon + 1, 1)
    return next_month - dt.timedelta(days=1)


def _pick_indicator_table(tables: list[pd.DataFrame]) -> pd.DataFrame | None:
    for t in tables:
        if t.empty:
            continue
        first_col = t.columns[0]
        values = t[first_col].astype(str).str.lower()
        if values.str.contains("tasa de desempleo").any() and values.str.contains("tasa de empleo adecuado").any():
            return t
    return None


def extract_labor_from_posts(posts: list[WpPost]) -> LaborExtract:
    rows: list[dict[str, Any]] = []
    sources: list[dict[str, Any]] = []

    for post in posts:
        html = post.content_html
        try:
            tables = pd.read_html(html)
        except ValueError:
            continue

        table = _pick_indicator_table(tables)
        if table is None:
            continue

        sources.append({"source": "INEC", "post_url": post.link, "post_date": post.date, "post_title": post.title})

        indicator_col = table.columns[0]
        table[indicator_col] = table[indicator_col].astype(str)

        # Normalize indicator names.
        indicators = {
            "tasa de desempleo": "unemployment_rate_pct",
            "tasa de empleo adecuado": "adequate_employment_rate_pct",
            "tasa de subempleo": "underemployment_rate_pct",
        }

        for raw_name, series in indicators.items():
            mask = table[indicator_col].str.lower().str.contains(raw_name)
            if not mask.any():
                continue
            r = table.loc[mask].iloc[0]

            for col in table.columns[1:]:
                period = _parse_period(str(col))
                if period is None:
                    continue
                value = parse_float_maybe(r[col])
                if value is None:
                    continue

                rows.append(
                    {
                        "period_date": period,
                        "period_grain": "month",
                        "series": series,
                        "value": float(value),
                        "unit": "%",
                        "source": "INEC",
                        "source_url": post.link,
                    }
                )

    df = pd.DataFrame(rows)
    if not df.empty:
        df["period_date"] = pd.to_datetime(df["period_date"]).dt.date
        df = df.drop_duplicates(subset=["period_date", "series"], keep="last")
        df = df.sort_values(["series", "period_date"])

    return LaborExtract(indicators=df, sources=sources)


def run_inec_labor(settings: dict[str, Any]) -> LaborExtract:
    wp = settings["inec_wp_api"]
    labor_cfg = settings["inec_labor"]

    posts = fetch_posts_multi(
        base_url=wp["base_url"],
        search_terms=labor_cfg.get("wp_search_terms", ["Empleo"]),
        max_pages=int(labor_cfg.get("max_pages", 30)),
        timeout_s=float(wp.get("timeout_s", 60)),
        user_agent=str(wp.get("user_agent", "ec-pov-monitor")),
    )

    # Keep only posts that look like ENEMDU employment pages.
    filtered: list[WpPost] = []
    include = re.compile(str(labor_cfg.get("include_regex", "")))
    exclude = re.compile(str(labor_cfg.get("exclude_regex", "")))

    for p in posts:
        urls = extract_urls(p.content_html)
        if any(include.search(u) for u in urls) and not any(exclude.search(u) for u in urls):
            filtered.append(p)

    return extract_labor_from_posts(filtered)
