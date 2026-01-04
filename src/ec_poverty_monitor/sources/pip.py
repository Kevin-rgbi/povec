from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from typing import Any

import httpx
import pandas as pd


@dataclass(frozen=True)
class PipExtract:
    indicators: pd.DataFrame
    sources: list[dict[str, Any]]


def fetch_pip_year(*, base_url: str, country: str, year: int, user_agent: str) -> dict[str, Any] | None:
    params = {"country": country, "year": year, "format": "json"}
    with httpx.Client(follow_redirects=True, timeout=60, headers={"User-Agent": user_agent}) as client:
        resp = client.get(base_url, params=params)
        if resp.status_code == 404:
            return None
        resp.raise_for_status()
        data = resp.json()

    if isinstance(data, list) and len(data) >= 1 and isinstance(data[0], dict):
        return data[0]
    if isinstance(data, dict):
        return data
    return None


def run_pip(settings: dict[str, Any], *, start_year: int = 2007) -> PipExtract:
    cfg = settings["world_bank_pip"]
    user_agent = settings["inec_wp_api"].get("user_agent", "ec-pov-monitor")

    current_year = dt.date.today().year
    rows: list[dict[str, Any]] = []

    for year in range(start_year, current_year + 1):
        rec = fetch_pip_year(base_url=cfg["base_url"], country=cfg["country"], year=year, user_agent=user_agent)
        if not rec:
            continue

        period_date = dt.date(year, 12, 31)

        # Always keep gini if present.
        if rec.get("gini") is not None:
            rows.append(
                {
                    "period_date": period_date,
                    "period_grain": "year",
                    "series": "gini",
                    "value": float(rec["gini"]),
                    "unit": "index",
                    "source": "PIP",
                    "source_url": f"{cfg['base_url']}?country={cfg['country']}&year={year}&format=json",
                }
            )

        # Headcount: PIP values depend on which poverty line record is returned.
        # If headcount exists in the record, store it but label as generic.
        if rec.get("headcount") is not None:
            rows.append(
                {
                    "period_date": period_date,
                    "period_grain": "year",
                    "series": "poverty_headcount" ,
                    "value": float(rec["headcount"]),
                    "unit": "%",
                    "source": "PIP",
                    "source_url": f"{cfg['base_url']}?country={cfg['country']}&year={year}&format=json",
                }
            )

    out = pd.DataFrame(rows)
    if not out.empty:
        out["period_date"] = pd.to_datetime(out["period_date"]).dt.date
        out = out.sort_values(["series", "period_date"])

    sources = [{"source": "PIP", "base_url": cfg["base_url"], "country": cfg["country"], "start_year": start_year}]
    return PipExtract(indicators=out, sources=sources)
