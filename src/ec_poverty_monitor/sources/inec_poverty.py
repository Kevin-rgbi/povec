from __future__ import annotations

import datetime as dt
import re
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

from ec_poverty_monitor.sources.inec_wp import WpPost, extract_urls, fetch_posts_multi
from ec_poverty_monitor.util.http import download
from ec_poverty_monitor.util.text import parse_float_maybe


@dataclass(frozen=True)
class PovertyExtract:
    indicators: pd.DataFrame
    sources: list[dict[str, Any]]


_SPANISH_MONTH = {
    "enero": 1,
    "febrero": 2,
    "marzo": 3,
    "abril": 4,
    "mayo": 5,
    "junio": 6,
    "julio": 7,
    "agosto": 8,
    "septiembre": 9,
    "setiembre": 9,
    "octubre": 10,
    "noviembre": 11,
    "diciembre": 12,
}


def _quarter_end(year: int, q: int) -> dt.date:
    if q == 1:
        return dt.date(year, 3, 31)
    if q == 2:
        return dt.date(year, 6, 30)
    if q == 3:
        return dt.date(year, 9, 30)
    return dt.date(year, 12, 31)


def _parse_quarter_token(token: str) -> tuple[int, int] | None:
    s = str(token).strip().upper()
    s = s.replace("T", "Q")
    m = re.match(r"^(\d{4})\s*Q([1-4])$", s)
    if m:
        return int(m.group(1)), int(m.group(2))

    # Spanish roman numerals like "2023 II".
    m2 = re.match(r"^(\d{4})\s*(I{1,3}|IV)$", s)
    if m2:
        roman = m2.group(2)
        qmap = {"I": 1, "II": 2, "III": 3, "IV": 4}
        if roman in qmap:
            return int(m2.group(1)), qmap[roman]

    return None


def _month_end(year: int, month: int) -> dt.date:
    if month == 12:
        return dt.date(year, 12, 31)
    return dt.date(year, month + 1, 1) - dt.timedelta(days=1)


def _parse_year_cell(value: object) -> int | None:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    s = str(value).strip()
    if not s:
        return None
    s = re.sub(r"\([^)]*\)", "", s)  # remove footnote markers like "(2)"
    m = re.search(r"\b(19\d{2}|20\d{2})\b", s)
    if not m:
        return None
    return int(m.group(1))


def _parse_period_label(value: object) -> str | None:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    s = str(value).strip().lower()
    if not s:
        return None
    # normalize common variants
    s = re.sub(r"\s+", " ", s)
    return s


def _infer_area_from_sheet(sheet_name: str) -> str | None:
    s = sheet_name.lower()
    if "nacional" in s:
        return "national"
    if "urbana" in s:
        return "urban"
    if "rural" in s:
        return "rural"
    return None


def _pick_zip_member(zf: zipfile.ZipFile) -> str | None:
    members = [m for m in zf.namelist() if m.lower().endswith((".xlsx", ".xls"))]
    if not members:
        return None

    def score(name: str) -> tuple[int, int]:
        n = name.lower()
        bonus = 0
        if "tabulad" in n:
            bonus += 10
        if "pobre" in n:
            bonus += 5
        if "indice" in n or "index" in n:
            bonus -= 3
        size = zf.getinfo(name).file_size
        return (bonus, size)

    return sorted(members, key=score, reverse=True)[0]


def _parse_timeseries_sheet(
    *,
    grid: pd.DataFrame,
    sheet_name: str,
) -> list[dict[str, Any]]:
    """Parse a single INEC poverty workbook sheet into standardized rows.

    Handles the common INEC format:
    - Header row includes "Período" and a metric column (e.g., "Incidencia", "Índice de Gini")
    - Data rows use a period label (e.g., "Junio", "Diciembre") with year in the next column
      and the metric value in the metric column.
    """

    sheet_lower = sheet_name.lower().strip()
    area = _infer_area_from_sheet(sheet_name)

    if "pobre_" in sheet_lower or ".pobre_" in sheet_lower:
        series_base = "poverty_rate_pct"
        value_header_substr = "incid"
        unit = "%"
    elif "extpob_" in sheet_lower or ".extpob_" in sheet_lower:
        series_base = "extreme_poverty_rate_pct"
        value_header_substr = "incid"
        unit = "%"
    elif "desigualdad" in sheet_lower:
        series_base = "gini"
        value_header_substr = "gini"
        unit = "index"
    else:
        return []

    # Find header row (the one containing "Período").
    header_row = None
    for i in range(min(80, len(grid))):
        row_vals = grid.iloc[i].astype(str).str.lower()
        if row_vals.str.contains("período", na=False).any() or row_vals.str.contains("periodo", na=False).any():
            header_row = i
            break
    if header_row is None:
        return []

    header = grid.iloc[header_row].astype(str).str.strip().str.lower().tolist()
    period_col = None
    for j, h in enumerate(header):
        if "período" in h or "periodo" in h:
            period_col = j
            break
    if period_col is None:
        return []

    # Find value column by substring.
    value_col = None
    for j, h in enumerate(header):
        if value_header_substr in h:
            value_col = j
            break
    if value_col is None:
        return []

    # Find year column: first column to the right of period_col with mostly year-like values.
    year_col = None
    candidate_cols = list(range(period_col + 1, min(period_col + 4, grid.shape[1])))
    for j in candidate_cols:
        years = []
        for v in grid.iloc[header_row + 1 : header_row + 40, j].tolist():
            y = _parse_year_cell(v)
            if y is not None:
                years.append(y)
        if len(years) >= 2:
            year_col = j
            break
    if year_col is None:
        return []

    rows: list[dict[str, Any]] = []
    current_period_label: str | None = None

    for i in range(header_row + 1, len(grid)):
        period_label = _parse_period_label(grid.iat[i, period_col])
        if period_label and (period_label.startswith("*") or period_label.startswith("fuente") or period_label.startswith("nota")):
            break

        if period_label:
            current_period_label = period_label

        year = _parse_year_cell(grid.iat[i, year_col])
        if year is None:
            continue

        if not current_period_label:
            continue

        month = _SPANISH_MONTH.get(current_period_label.strip().lower())
        if month is None:
            # Some sheets use "Junio" and "Diciembre" only; ignore unknown labels.
            continue

        value = parse_float_maybe(grid.iat[i, value_col])
        if value is None:
            continue

        series = series_base
        if area:
            series = f"{series_base}_{area}"

        rows.append(
            {
                "period_date": _month_end(year, month),
                "period_grain": "month",
                "series": series,
                "value": float(value),
                "unit": unit,
                "source": "INEC",
            }
        )

        # Add national alias without suffix for convenience.
        if area == "national":
            rows.append(
                {
                    "period_date": _month_end(year, month),
                    "period_grain": "month",
                    "series": series_base,
                    "value": float(value),
                    "unit": unit,
                    "source": "INEC",
                }
            )

    return rows


def _parse_poverty_tables_from_excel(path: Path) -> pd.DataFrame:
    xls = pd.ExcelFile(path)
    rows: list[dict[str, Any]] = []

    for sheet in xls.sheet_names:
        # Read as raw grid so we can detect header rows robustly.
        grid = pd.read_excel(xls, sheet_name=sheet, header=None)
        if grid.empty or grid.shape[1] < 3:
            continue
        rows.extend(_parse_timeseries_sheet(grid=grid, sheet_name=sheet))

    return pd.DataFrame(rows)


def run_inec_poverty(settings: dict[str, Any], *, cache_dir: Path, force: bool = False) -> PovertyExtract:
    wp = settings["inec_wp_api"]
    cfg = settings["inec_poverty"]

    posts = fetch_posts_multi(
        base_url=wp["base_url"],
        search_terms=cfg.get("wp_search_terms", ["Pobreza"]),
        max_pages=int(cfg.get("max_pages", 30)),
        timeout_s=float(wp.get("timeout_s", 60)),
        user_agent=str(wp.get("user_agent", "ec-pov-monitor")),
    )

    include = re.compile(str(cfg.get("include_regex", "")))
    urls: list[tuple[str, WpPost]] = []
    for p in posts:
        for u in extract_urls(p.content_html):
            if include.search(u):
                urls.append((u, p))

    # Prefer newest posts first.
    urls = sorted(urls, key=lambda t: (t[1].date or ""), reverse=True)

    sources: list[dict[str, Any]] = []
    indicators_frames: list[pd.DataFrame] = []

    for url, post in urls[: int(cfg.get("max_assets", 5))]:
        sources.append(
            {
                "source": "INEC",
                "asset_url": url,
                "post_url": post.link,
                "post_date": post.date,
                "post_title": post.title,
            }
        )

        try:
            dl = download(
                url=url,
                out_dir=cache_dir,
                user_agent=str(wp.get("user_agent", "ec-pov-monitor")),
                timeout_s=float(wp.get("timeout_s", 60)),
                force=force,
            )
        except Exception:
            continue

        # Attempt to parse ZIP -> best XLS/XLSX member.
        try:
            with zipfile.ZipFile(dl.path) as zf:
                member = _pick_zip_member(zf)
                if not member:
                    continue
                extracted = zf.extract(member, path=cache_dir)
        except Exception:
            continue

        try:
            parsed = _parse_poverty_tables_from_excel(Path(extracted))
        except Exception:
            continue

        if not parsed.empty:
            parsed["source_url"] = url
            indicators_frames.append(parsed)

    indicators = pd.concat(indicators_frames, ignore_index=True) if indicators_frames else pd.DataFrame()
    if not indicators.empty:
        indicators["period_date"] = pd.to_datetime(indicators["period_date"]).dt.date
        indicators = indicators.drop_duplicates(subset=["period_date", "series"], keep="last")
        indicators = indicators.sort_values(["series", "period_date"])

    return PovertyExtract(indicators=indicators, sources=sources)
