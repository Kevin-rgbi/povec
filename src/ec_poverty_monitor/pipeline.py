from __future__ import annotations

import datetime as dt
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

from ec_poverty_monitor.settings import Settings, load_settings
from ec_poverty_monitor.sources.inec_labor import run_inec_labor
from ec_poverty_monitor.sources.inec_poverty import run_inec_poverty
from ec_poverty_monitor.sources.pip import run_pip
from ec_poverty_monitor.sources.wdi import run_wdi
from ec_poverty_monitor.transform.marts import (
    MartResult,
    apply_comparability_break,
    build_duckdb_and_parquet,
    canonicalize_period,
    stack_sources,
)
from ec_poverty_monitor.util.fs import ensure_dirs
from ec_poverty_monitor.util.logging import configure_logging
from ec_poverty_monitor.validate import validate_indicators


@dataclass(frozen=True)
class PipelineResult:
    started_at: str
    finished_at: str
    status: str
    manifest_path: str
    indicators_rows: int
    sources_rows: int


def run_pipeline(config_path: Path, force: bool = False) -> dict[str, Any]:
    settings: Settings = load_settings(config_path)
    ensure_dirs(settings)
    logger = configure_logging(settings)

    started = dt.datetime.now(dt.timezone.utc)
    logger.info("pipeline.start", extra={"started_at": started.isoformat()})

    country = str(settings.project.get("country_iso3", "ECU"))

    labor = run_inec_labor(settings.sources)
    poverty = run_inec_poverty(settings.sources, cache_dir=settings.paths.data_raw, force=force)
    wdi = run_wdi(settings.sources, country=country)
    pip = run_pip(settings.sources)

    frames = [labor.indicators, poverty.indicators, wdi.indicators, pip.indicators]
    indicators = pd.concat([df for df in frames if df is not None and not df.empty], ignore_index=True)

    if not indicators.empty:
        indicators = canonicalize_period(indicators)

        # Comparability window (used mainly for INEC ENEMDU-derived indicators)
        start = dt.date.fromisoformat(settings.comparability.enemdu_break.start)
        end = dt.date.fromisoformat(settings.comparability.enemdu_break.end)
        indicators = apply_comparability_break(indicators, start=start, end=end)

    issues = validate_indicators(indicators)
    if issues:
        logger.warning("pipeline.validation_issues", extra={"issues": issues})

    sources = stack_sources([labor.sources, poverty.sources, wdi.sources, pip.sources])

    # Write marts via DuckDB
    duckdb_path = str(settings.paths.duckdb_path)
    indicators_parquet = str(settings.paths.data_mart / "mart_indicators.parquet")
    sources_parquet = str(settings.paths.data_mart / "mart_sources.parquet")

    mart_result: MartResult = build_duckdb_and_parquet(
        indicators=indicators,
        sources=sources,
        duckdb_path=duckdb_path,
        indicators_parquet=indicators_parquet,
        sources_parquet=sources_parquet,
    )

    finished = dt.datetime.now(dt.timezone.utc)

    manifest = {
        "started_at": started.isoformat(),
        "finished_at": finished.isoformat(),
        "force": force,
        "config": str(config_path),
        "row_counts": {"indicators": int(len(indicators)), "sources": int(len(sources))},
        "validation_issues": issues,
        "outputs": {
            "duckdb": mart_result.duckdb_path,
            "mart_indicators": mart_result.indicators_path,
            "mart_sources": mart_result.sources_path,
        },
    }

    manifest_path = settings.paths.runs / f"run_{started.strftime('%Y%m%dT%H%M%SZ')}.json"
    manifest_path.write_text(json.dumps(manifest, indent=2, ensure_ascii=False))

    logger.info(
        "pipeline.finish",
        extra={"finished_at": finished.isoformat(), "manifest_path": str(manifest_path)},
    )

    result = PipelineResult(
        started_at=started.isoformat(),
        finished_at=finished.isoformat(),
        status="ok" if not issues else "ok_with_warnings",
        manifest_path=str(manifest_path),
        indicators_rows=int(len(indicators)),
        sources_rows=int(len(sources)),
    )
    return result.__dict__
