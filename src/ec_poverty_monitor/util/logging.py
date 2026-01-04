from __future__ import annotations

import logging
from pathlib import Path

from ec_poverty_monitor.settings import Settings


def configure_logging(settings: Settings) -> logging.Logger:
    logger = logging.getLogger("ec_poverty_monitor")
    if logger.handlers:
        return logger

    logger.setLevel(logging.INFO)

    log_path = Path(settings.paths.logs) / "pipeline.log"

    fmt = logging.Formatter("%(asctime)s %(levelname)s %(message)s")

    sh = logging.StreamHandler()
    sh.setFormatter(fmt)

    fh = logging.FileHandler(log_path)
    fh.setFormatter(fmt)

    logger.addHandler(sh)
    logger.addHandler(fh)

    return logger
