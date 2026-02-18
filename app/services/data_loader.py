"""Data loading utilities for species profiles and environmental cells."""
from __future__ import annotations

import json
from functools import lru_cache
from pathlib import Path

from app.config import get_settings
from app.models import HabitatCellCollection, SpeciesCatalog
from app.services import data_cache


def _load_json(path: Path) -> dict:
    if not path.exists():
        raise FileNotFoundError(f"Required data file missing: {path}")
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


@lru_cache(1)
def load_species_catalog() -> SpeciesCatalog:
    settings = get_settings()
    payload = _load_json(settings.species_profile_path)
    return SpeciesCatalog.model_validate(payload)


def load_habitat_cells() -> HabitatCellCollection:
    cached = data_cache.load_processed_cells()
    if cached:
        return cached

    settings = get_settings()
    payload = _load_json(settings.sample_cells_path)
    return HabitatCellCollection.model_validate(payload)
