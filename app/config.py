"""Application configuration settings."""
from functools import lru_cache
from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Central configuration for the mushroom nowcast service."""

    data_dir: Path = Path("data")
    default_species_id: str = "chanterelle"

    # Ingestion + caching configuration
    weather_seed_endpoint: str = "local"
    canopy_seed_endpoint: str = "local"
    expected_ingest_interval_minutes: int = 24 * 60

    model_config = SettingsConfigDict(env_prefix="MUSHROOM_", env_file=".env", env_file_encoding="utf-8")

    @property
    def species_profile_path(self) -> Path:
        return self.data_dir / "species_profiles.json"

    @property
    def sample_cells_path(self) -> Path:
        return self.data_dir / "sample_cells.json"

    @property
    def data_raw_dir(self) -> Path:
        return self.data_dir / "raw"

    @property
    def data_staging_dir(self) -> Path:
        return self.data_dir / "staging"

    @property
    def data_processed_dir(self) -> Path:
        return self.data_dir / "processed"

    @property
    def processed_grid_path(self) -> Path:
        return self.data_processed_dir / "habitat_cells.json"

    @property
    def freshness_path(self) -> Path:
        return self.data_dir / "freshness.json"

    @property
    def seed_data_dir(self) -> Path:
        return self.data_dir / "seeds"

    @property
    def nlcd_canopy_path(self) -> Path:
        return self.data_dir / "raw" / "nlcd_canopy_pnw.tif"

    @property
    def refined_grid_path(self) -> Path:
        return self.data_dir / "processed" / "habitat_cells_300m.parquet"


@lru_cache(1)
def get_settings() -> Settings:
    return Settings()
