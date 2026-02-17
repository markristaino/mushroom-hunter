"""Application configuration settings."""
from functools import lru_cache
from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Central configuration for the mushroom nowcast service."""

    data_dir: Path = Path("data")
    default_species_id: str = "chanterelle"

    model_config = SettingsConfigDict(env_prefix="MUSHROOM_", env_file=".env", env_file_encoding="utf-8")

    @property
    def species_profile_path(self) -> Path:
        return self.data_dir / "species_profiles.json"

    @property
    def sample_cells_path(self) -> Path:
        return self.data_dir / "sample_cells.json"


@lru_cache(1)
def get_settings() -> Settings:
    return Settings()
