from __future__ import annotations

from ec_poverty_monitor.settings import Settings


def ensure_dirs(settings: Settings) -> None:
    settings.paths.data_raw.mkdir(parents=True, exist_ok=True)
    settings.paths.data_staging.mkdir(parents=True, exist_ok=True)
    settings.paths.data_mart.mkdir(parents=True, exist_ok=True)
    settings.paths.runs.mkdir(parents=True, exist_ok=True)
    settings.paths.logs.mkdir(parents=True, exist_ok=True)
    settings.paths.duckdb_path.parent.mkdir(parents=True, exist_ok=True)
