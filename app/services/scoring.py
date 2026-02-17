"""Deterministic scoring engine for mushroom habitats."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List

from app.models import HabitatCell, SpeciesProfile


@dataclass
class ScoreComponent:
    name: str
    passed: bool
    detail: str
    weight: float


@dataclass
class ScoredCell:
    cell: HabitatCell
    species_id: str
    score: float
    components: List[ScoreComponent]


class NowcastScorer:
    """Simple rule-based scorer for current habitat suitability."""

    def __init__(self, weight_config: Dict[str, float] | None = None):
        self.weight_config = weight_config or {
            "soil_temperature": 0.3,
            "precipitation": 0.3,
            "host_species": 0.2,
            "phenology": 0.2,
        }

    def score_cell(self, cell: HabitatCell, profile: SpeciesProfile, reference_time: datetime) -> ScoredCell:
        components: List[ScoreComponent] = []

        # Soil temperature
        soil_ok = profile.soil_temperature_c.contains(cell.soil_temperature_c)
        components.append(
            ScoreComponent(
                name="soil_temperature",
                passed=soil_ok,
                detail=f"observed={cell.soil_temperature_c:.1f}°C," f" range={profile.soil_temperature_c.minimum}-{profile.soil_temperature_c.maximum}",
                weight=self.weight_config["soil_temperature"],
            )
        )

        # Precipitation window
        precip_ok = profile.precipitation_mm_last_7d.contains(cell.precipitation_mm_last_7d)
        components.append(
            ScoreComponent(
                name="precipitation",
                passed=precip_ok,
                detail=f"observed={cell.precipitation_mm_last_7d:.1f}mm," f" range={profile.precipitation_mm_last_7d.minimum}-{profile.precipitation_mm_last_7d.maximum}",
                weight=self.weight_config["precipitation"],
            )
        )

        # Host species presence
        host_ok = any(
            host.scientific_name in cell.host_species_present for host in profile.host_species
        )
        components.append(
            ScoreComponent(
                name="host_species",
                passed=host_ok,
                detail="matched" if host_ok else "no host present",
                weight=self.weight_config["host_species"],
            )
        )

        # Phenology month
        phenology_ok = reference_time.month in profile.phenology_months
        components.append(
            ScoreComponent(
                name="phenology",
                passed=phenology_ok,
                detail=f"month={reference_time.month}",
                weight=self.weight_config["phenology"],
            )
        )

        score = sum(component.weight for component in components if component.passed)
        return ScoredCell(cell=cell, species_id=profile.id, score=score, components=components)
