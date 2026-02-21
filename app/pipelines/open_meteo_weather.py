"""Ingestion pipeline: fetch real weather + soil data from Open-Meteo for PNW grid cells."""
from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import List

import requests

from app.config import get_settings
from app.models import HabitatCell, HabitatCellCollection
from app.pipelines.base import IngestionResult, update_freshness, write_collection

# ---------------------------------------------------------------------------
# Static grid: PNW locations with curated host species (forest type is fixed
# geography; weather data is fetched live from Open-Meteo).
# ---------------------------------------------------------------------------
GRID = [
    {
        "cell_id": "wa-001",
        "latitude": 47.4254,
        "longitude": -121.4133,
        "canopy_density_pct": 85.0,
        "host_species_present": ["Pseudotsuga menziesii", "Tsuga heterophylla"],
        "label": "Snoqualmie Pass foothills",
    },
    {
        "cell_id": "wa-002",
        "latitude": 47.8601,
        "longitude": -123.9344,
        "canopy_density_pct": 90.0,
        "host_species_present": ["Pseudotsuga menziesii", "Tsuga heterophylla", "Picea sitchensis"],
        "label": "Olympic Peninsula / Hoh Rainforest",
    },
    {
        "cell_id": "wa-003",
        "latitude": 46.7445,
        "longitude": -121.9982,
        "canopy_density_pct": 80.0,
        "host_species_present": ["Pseudotsuga menziesii", "Abies amabilis"],
        "label": "Mt. Rainier foothills (Ashford / Nisqually entrance)",
    },
    {
        "cell_id": "wa-004",
        "latitude": 47.7454,
        "longitude": -121.0899,
        "canopy_density_pct": 82.0,
        "host_species_present": ["Pseudotsuga menziesii", "Tsuga heterophylla", "Pinus ponderosa"],
        "label": "Stevens Pass / North Cascades",
    },
    {
        "cell_id": "wa-005",
        "latitude": 46.5770,
        "longitude": -123.5260,
        "canopy_density_pct": 78.0,
        "host_species_present": ["Pseudotsuga menziesii", "Tsuga heterophylla"],
        "label": "Willapa Hills",
    },
    {
        "cell_id": "wa-006",
        "latitude": 45.7054,
        "longitude": -121.5220,
        "canopy_density_pct": 65.0,
        "host_species_present": ["Pseudotsuga menziesii", "Quercus garryana"],
        "label": "Columbia River Gorge (WA)",
    },
    {
        "cell_id": "wa-007",
        "latitude": 48.4102,
        "longitude": -119.5050,
        "canopy_density_pct": 55.0,
        "host_species_present": ["Populus trichocarpa", "Pinus ponderosa"],
        "label": "Okanogan Highlands (morel country)",
    },
    {
        "cell_id": "wa-008",
        "latitude": 47.6062,
        "longitude": -122.3321,
        "canopy_density_pct": 45.0,
        "host_species_present": ["Acer macrophyllum", "Pseudotsuga menziesii"],
        "label": "Puget Sound lowlands",
    },
    {
        "cell_id": "wa-009",
        "latitude": 48.6734,
        "longitude": -121.2424,
        "canopy_density_pct": 88.0,
        "host_species_present": ["Pseudotsuga menziesii", "Tsuga heterophylla", "Abies lasiocarpa"],
        "label": "North Cascades / Skagit Valley (Newhalem)",
    },
    {
        "cell_id": "wa-010",
        "latitude": 46.1558,
        "longitude": -122.9007,
        "canopy_density_pct": 75.0,
        "host_species_present": ["Pseudotsuga menziesii", "Picea sitchensis"],
        "label": "Naselle / SW Washington coast",
    },
    {
        "cell_id": "or-001",
        "latitude": 45.4553,
        "longitude": -123.8370,
        "canopy_density_pct": 80.0,
        "host_species_present": ["Pseudotsuga menziesii", "Picea sitchensis", "Tsuga heterophylla"],
        "label": "Tillamook State Forest",
    },
    {
        "cell_id": "or-002",
        "latitude": 44.9429,
        "longitude": -123.0351,
        "canopy_density_pct": 40.0,
        "host_species_present": ["Quercus garryana", "Acer macrophyllum", "Fraxinus latifolia"],
        "label": "Willamette Valley",
    },
    {
        "cell_id": "or-003",
        "latitude": 45.3700,
        "longitude": -121.9700,
        "canopy_density_pct": 78.0,
        "host_species_present": ["Pseudotsuga menziesii", "Abies amabilis", "Tsuga heterophylla"],
        "label": "Mt. Hood foothills (Zigzag / Sandy River valley)",
    },
    {
        "cell_id": "or-004",
        "latitude": 43.2165,
        "longitude": -123.3614,
        "canopy_density_pct": 72.0,
        "host_species_present": ["Pseudotsuga menziesii", "Quercus garryana"],
        "label": "Umpqua / Rogue River area",
    },
    {
        "cell_id": "or-005",
        "latitude": 42.1561,
        "longitude": -123.3494,
        "canopy_density_pct": 70.0,
        "host_species_present": ["Pseudotsuga menziesii", "Pinus ponderosa", "Quercus garryana"],
        "label": "Klamath Mountains",
    },
    {
        "cell_id": "or-006",
        "latitude": 45.5200,
        "longitude": -118.4200,
        "canopy_density_pct": 50.0,
        "host_species_present": ["Populus trichocarpa", "Pinus ponderosa", "Pseudotsuga menziesii"],
        "label": "Blue Mountains / Umatilla (morel country)",
    },
    {
        "cell_id": "or-007",
        "latitude": 42.9600,
        "longitude": -122.4400,
        "canopy_density_pct": 68.0,
        "host_species_present": ["Abies lasiocarpa", "Pinus ponderosa"],
        "label": "Crater Lake foothills (Union Creek / Prospect)",
    },
    {
        "cell_id": "or-008",
        "latitude": 44.0582,
        "longitude": -121.3153,
        "canopy_density_pct": 55.0,
        "host_species_present": ["Pinus ponderosa", "Populus trichocarpa"],
        "label": "Deschutes / Bend area",
    },
    {
        "cell_id": "or-009",
        "latitude": 44.6365,
        "longitude": -124.0534,
        "canopy_density_pct": 75.0,
        "host_species_present": ["Picea sitchensis", "Tsuga heterophylla", "Acer macrophyllum"],
        "label": "Oregon Coast / Lincoln City",
    },
    {
        "cell_id": "id-001",
        "latitude": 47.6777,
        "longitude": -116.7805,
        "canopy_density_pct": 70.0,
        "host_species_present": ["Pseudotsuga menziesii", "Pinus ponderosa", "Betula spp."],
        "label": "Idaho Panhandle / Coeur d'Alene",
    },
    {
        "cell_id": "id-002",
        "latitude": 46.4165,
        "longitude": -115.5281,
        "canopy_density_pct": 72.0,
        "host_species_present": ["Populus trichocarpa", "Pinus ponderosa", "Pseudotsuga menziesii"],
        "label": "Clearwater / Moose Creek (morel country)",
    },
]

OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"


def _fetch_cell(point: dict) -> HabitatCell:
    """Call Open-Meteo for a single lat/lon and return a HabitatCell."""
    params = {
        "latitude": point["latitude"],
        "longitude": point["longitude"],
        "hourly": "soil_temperature_6cm,soil_moisture_3_to_9cm",
        "daily": "precipitation_sum",
        "past_days": 7,
        "forecast_days": 1,
        "timezone": "America/Los_Angeles",
    }
    resp = requests.get(OPEN_METEO_URL, params=params, timeout=15)
    resp.raise_for_status()
    data = resp.json()

    # Most recent non-null soil temperature (walk back from end of hourly array)
    soil_temps: List[float | None] = data["hourly"]["soil_temperature_6cm"]
    soil_temp = next((v for v in reversed(soil_temps) if v is not None), 0.0)

    # Most recent non-null soil moisture
    soil_moisture_vals: List[float | None] = data["hourly"]["soil_moisture_3_to_9cm"]
    soil_moisture = next((v for v in reversed(soil_moisture_vals) if v is not None), None)

    # Sum of daily precipitation over the last 7 days (indices 0-6; index 7 = today forecast)
    precip_daily: List[float | None] = data["daily"]["precipitation_sum"]
    precip_7d = sum(v for v in precip_daily[:7] if v is not None)

    return HabitatCell(
        cell_id=point["cell_id"],
        latitude=point["latitude"],
        longitude=point["longitude"],
        host_species_present=point["host_species_present"],
        soil_temperature_c=round(soil_temp, 2),
        precipitation_mm_last_7d=round(precip_7d, 2),
        soil_moisture_index=round(soil_moisture, 3) if soil_moisture is not None else None,
        canopy_density_pct=point.get("canopy_density_pct"),
        last_observation=datetime.now(timezone.utc),
    )


def run() -> IngestionResult:
    settings = get_settings()
    cells = []
    errors = []

    for point in GRID:
        try:
            cell = _fetch_cell(point)
            cells.append(cell)
            print(f"  OK  {point['cell_id']} ({point['label']}): "
                  f"soil={cell.soil_temperature_c}°C  precip={cell.precipitation_mm_last_7d}mm")
        except Exception as exc:
            errors.append(point["cell_id"])
            print(f"  ERR {point['cell_id']} ({point['label']}): {exc}")

    if not cells:
        raise RuntimeError("All Open-Meteo requests failed — cannot write empty dataset.")

    collection = HabitatCellCollection(cells=cells)
    rows = write_collection(collection, settings.processed_grid_path)

    result = IngestionResult(
        source_id="open_meteo_weather",
        output_path=settings.processed_grid_path,
        rows_written=rows,
        last_ingested=datetime.now(timezone.utc),
    )
    update_freshness(result, settings.freshness_path)

    if errors:
        print(f"\nWarning: {len(errors)} cell(s) failed: {errors}")

    return result


def main() -> None:
    print("Fetching real weather data from Open-Meteo for PNW grid...")
    result = run()
    print(f"\nWrote {result.rows_written} habitat cells to {result.output_path}")


if __name__ == "__main__":
    main()
