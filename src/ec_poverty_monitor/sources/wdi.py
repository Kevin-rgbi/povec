from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from typing import Any

import httpx
import pandas as pd


@dataclass(frozen=True)
class WdiExtract:
    indicators: pd.DataFrame
    sources: list[dict[str, Any]]


def fetch_wdi_indicator(*, base_url: str, country: str, indicator: str, user_agent: str) -> pd.DataFrame:
    url = f"{base_url}/country/{country}/indicator/{indicator}"
    params = {"format": "json", "per_page": 20000}

    with httpx.Client(follow_redirects=True, timeout=60, headers={"User-Agent": user_agent}) as client:
        resp = client.get(url, params=params)
        resp.raise_for_status()
        payload = resp.json()

    if not isinstance(payload, list) or len(payload) < 2:
        return pd.DataFrame()

    rows = payload[1]
    out = []
    for r in rows:
        year = r.get("date")
        value = r.get("value")
        if year is None or value is None:
            continue
        out.append({"year": int(year), "value": float(value)})

    df = pd.DataFrame(out)
    if df.empty:
        return df

    df = df.sort_values("year")
    df["period_date"] = df["year"].apply(lambda y: dt.date(y, 12, 31))
    df["period_grain"] = "year"
    return df[["period_date", "period_grain", "value"]]


def run_wdi(settings: dict[str, Any], country: str) -> WdiExtract:
    cfg = settings["world_bank_wdi"]
    user_agent = settings["inec_wp_api"].get("user_agent", "ec-pov-monitor")

    rows: list[dict[str, Any]] = []
    sources: list[dict[str, Any]] = []

    for series, indicator_code in cfg.get("indicators", {}).items():
        df = fetch_wdi_indicator(base_url=cfg["base_url"], country=country, indicator=indicator_code, user_agent=user_agent)
        if df.empty:
            continue

        unit = "%" if series.endswith("_pct") else "USD"

        for _, r in df.iterrows():
            rows.append(
                {
                    "period_date": r["period_date"],
                    "period_grain": r["period_grain"],
                    "series": series,
                    "value": float(r["value"]),
                    "unit": unit,
                    "source": "WDI",
                    "source_url": f"{cfg['base_url']}/country/{country}/indicator/{indicator_code}?format=json",
                }
            )

        sources.append(
            {
                "source": "WDI",
                "indicator": indicator_code,
                "series": series,
                "url": f"{cfg['base_url']}/country/{country}/indicator/{indicator_code}?format=json",
            }
        )

    out = pd.DataFrame(rows)
    if not out.empty:
        out["period_date"] = pd.to_datetime(out["period_date"]).dt.date

    return WdiExtract(indicators=out, sources=sources)
