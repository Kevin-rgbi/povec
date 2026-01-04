from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml


@dataclass(frozen=True)
class Paths:
    data_raw: Path
    data_staging: Path
    data_mart: Path
    runs: Path
    logs: Path
    duckdb_path: Path


@dataclass(frozen=True)
class ComparabilityWindow:
    start: str
    end: str
    note: str


@dataclass(frozen=True)
class Comparability:
    enemdu_break: ComparabilityWindow


@dataclass(frozen=True)
class Settings:
    project: dict[str, Any]
    paths: Paths
    comparability: Comparability
    sources: dict[str, Any]


def load_settings(config_path: Path) -> Settings:
    cfg = yaml.safe_load(config_path.read_text())

    paths = Paths(
        data_raw=Path(cfg["paths"]["data_raw"]),
        data_staging=Path(cfg["paths"]["data_staging"]),
        data_mart=Path(cfg["paths"]["data_mart"]),
        runs=Path(cfg["paths"]["runs"]),
        logs=Path(cfg["paths"]["logs"]),
        duckdb_path=Path(cfg["paths"]["duckdb_path"]),
    )

    enemdu_cfg = cfg["comparability"]["enemdu_break"]
    comparability = Comparability(
        enemdu_break=ComparabilityWindow(
            start=str(enemdu_cfg["start"]),
            end=str(enemdu_cfg["end"]),
            note=str(enemdu_cfg["note"]),
        )
    )

    return Settings(
        project=dict(cfg.get("project", {})),
        paths=paths,
        comparability=comparability,
        sources=dict(cfg.get("sources", {})),
    )
