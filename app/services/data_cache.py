"""Helpers to read processed habitat cell caches."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Optional

from app.config import get_settings
from app.models import HabitatCellCollection


def _read_json(path: Path) -> dict:
    if not path.exists():
        raise FileNotFoundError(f"Processed cache missing: {path}")
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def load_processed_cells() -> Optional[HabitatCellCollection]:
    """Return processed habitat cells if the ingestion pipeline produced them."""

    settings = get_settings()
    processed_path = settings.processed_grid_path
    if not processed_path.exists():
        return None

    payload = _read_json(processed_path)
    return HabitatCellCollection.model_validate(payload)
