"""Species profile domain models."""
from datetime import date
from typing import List, Optional

from pydantic import BaseModel, Field


class EnvironmentalThreshold(BaseModel):
    """Represents a numeric threshold range for an environmental metric."""

    minimum: float
    maximum: float

    def contains(self, value: float) -> bool:
        return self.minimum <= value <= self.maximum


class HostAssociation(BaseModel):
    """Represents a known mycorrhizal host species."""

    scientific_name: str
    common_name: Optional[str] = None
    notes: Optional[str] = None


class SpeciesProfile(BaseModel):
    """Structured ecological profile for a target mushroom species."""

    id: str = Field(..., description="Stable identifier used in APIs.")
    common_name: str
    scientific_name: Optional[str] = None
    soil_temperature_c: EnvironmentalThreshold
    precipitation_mm_last_7d: EnvironmentalThreshold
    soil_moisture_index: Optional[EnvironmentalThreshold] = Field(
        default=None,
        description="Normalized (0-1) soil moisture band when fruiting is likely.",
    )
    canopy_density_pct: Optional[EnvironmentalThreshold] = Field(
        default=None,
        description="Acceptable canopy closure percentage.",
    )
    elevation_m: Optional[EnvironmentalThreshold] = Field(
        default=None, description="Typical elevation band for fruiting habitat."
    )
    phenology_months: List[int] = Field(..., description="Months (1-12) when fruiting is typical.")
    host_species: List[HostAssociation] = Field(
        ..., description="List of host tree species with optional notes."
    )
    soil_type_notes: Optional[str] = None
    fauna_partners: List[str] = Field(default_factory=list)
    sources: List[str] = Field(default_factory=list, description="Reference citations.")
    last_updated: Optional[date] = None
    manual_notes: Optional[str] = None


class SpeciesCatalog(BaseModel):
    """Collection wrapper for multiple species profiles."""

    species: List[SpeciesProfile]

    def get(self, species_id: str) -> SpeciesProfile:
        for profile in self.species:
            if profile.id == species_id:
                return profile
        raise KeyError(f"Species '{species_id}' not found.")

    def list_ids(self) -> List[str]:
        return [profile.id for profile in self.species]
