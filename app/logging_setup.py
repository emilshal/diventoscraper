from __future__ import annotations

import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path

from app.config import settings


def init_logging() -> None:
    level = getattr(logging, str(settings.LOG_LEVEL).upper(), logging.INFO)
    root = logging.getLogger()
    if root.handlers:
        root.setLevel(level)
        return

    root.setLevel(level)

    log_dir = Path(settings.LOG_DIR)
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / "divento.log"

    fmt = logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s")

    file_handler = RotatingFileHandler(log_path, maxBytes=5_000_000, backupCount=3)
    file_handler.setFormatter(fmt)
    file_handler.setLevel(level)

    console = logging.StreamHandler()
    console.setFormatter(fmt)
    console.setLevel(level)

    root.addHandler(file_handler)
    root.addHandler(console)

