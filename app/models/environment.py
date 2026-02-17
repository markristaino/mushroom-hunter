"""Environmental grid cell models."""
from datetime import datetime
from typing import List

from pydantic import BaseModel, Field


class HabitatCell(BaseModel):
    """Represents the latest environmental readings for a spatial grid cell."""

    cell_id: str
    latitude: float
    longitude: float
    host_species_present: List[str] = Field(default_factory=list)
    soil_temperature_c: float
    precipitation_mm_last_7d: float
    last_observation: datetime


class HabitatCellCollection(BaseModel):
    """Wrapper around a list of habitat cells."""

    cells: List[HabitatCell]

    def __iter__(self):  # pragma: no cover - simple delegation
        return iter(self.cells)
