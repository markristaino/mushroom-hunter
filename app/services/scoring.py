"""Deterministic scoring engine for mushroom habitats."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List

from app.models import HabitatCell, SpeciesProfile
from app.models.species import EnvironmentalThreshold


def _range_score(threshold: EnvironmentalThreshold, value: float) -> float:
    """Score 1.0 at the midpoint of the range, fading linearly to 0 at the edges."""
    half_range = (threshold.maximum - threshold.minimum) / 2
    if half_range == 0:
        return 1.0 if value == threshold.minimum else 0.0
    midpoint = threshold.minimum + half_range
    return max(0.0, 1.0 - abs(value - midpoint) / half_range)


def _phenology_score(month: int, phenology_months: List[int]) -> float:
    """1.0 in season, 0.5 for the immediately adjacent shoulder months, 0 otherwise."""
    if month in phenology_months:
        return 1.0
    prev_month = 12 if month == 1 else month - 1
    next_month = 1 if month == 12 else month + 1
    if prev_month in phenology_months or next_month in phenology_months:
        return 0.5
    return 0.0


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
    """Continuous scorer for current habitat suitability."""

    def __init__(self, weight_config: Dict[str, float] | None = None):
        self.weight_config = weight_config or {
            "soil_temperature": 0.3,
            "precipitation": 0.3,
            "host_species": 0.2,
            "phenology": 0.2,
        }

    def score_cell(self, cell: HabitatCell, profile: SpeciesProfile, reference_time: datetime) -> ScoredCell:
        components: List[ScoreComponent] = []

        # Soil temperature — continuous score based on proximity to ideal midpoint
        soil_partial = _range_score(profile.soil_temperature_c, cell.soil_temperature_c)
        components.append(ScoreComponent(
            name="soil_temperature",
            passed=soil_partial > 0,
            detail=f"observed={cell.soil_temperature_c:.1f}°C, range={profile.soil_temperature_c.minimum}-{profile.soil_temperature_c.maximum}, score={soil_partial:.2f}",
            weight=self.weight_config["soil_temperature"] * soil_partial,
        ))

        # Precipitation — continuous score based on proximity to ideal midpoint
        precip_partial = _range_score(profile.precipitation_mm_last_7d, cell.precipitation_mm_last_7d)
        components.append(ScoreComponent(
            name="precipitation",
            passed=precip_partial > 0,
            detail=f"observed={cell.precipitation_mm_last_7d:.1f}mm, range={profile.precipitation_mm_last_7d.minimum}-{profile.precipitation_mm_last_7d.maximum}, score={precip_partial:.2f}",
            weight=self.weight_config["precipitation"] * precip_partial,
        ))

        # Host species — fraction of the species' hosts present in this cell
        matching = sum(1 for host in profile.host_species if host.scientific_name in (cell.host_species_present or []))
        total_hosts = len(profile.host_species)
        host_partial = matching / total_hosts if total_hosts > 0 else 0.0
        components.append(ScoreComponent(
            name="host_species",
            passed=host_partial > 0,
            detail=f"{matching}/{total_hosts} hosts present, score={host_partial:.2f}",
            weight=self.weight_config["host_species"] * host_partial,
        ))

        # Phenology — 1.0 in season, 0.5 shoulder, 0 outside
        pheno_partial = _phenology_score(reference_time.month, profile.phenology_months)
        components.append(ScoreComponent(
            name="phenology",
            passed=pheno_partial > 0,
            detail=f"month={reference_time.month}, score={pheno_partial:.2f}",
            weight=self.weight_config["phenology"] * pheno_partial,
        ))

        score = round(sum(component.weight for component in components), 4)
        return ScoredCell(cell=cell, species_id=profile.id, score=score, components=components)
