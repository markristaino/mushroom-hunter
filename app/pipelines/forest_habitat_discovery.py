"""Ingestion pipeline: auto-discover NF habitat cells and fetch real weather data.

Stages:
  1. Generate ~6,400 candidate grid points at 0.1° spacing across the PNW.
  2. Batch-query Open-Meteo Elevation API; keep 200–1,600 m points only.
  3. Fetch USFS National Forest polygons; keep only points inside NF land.
  4. Fetch real soil-temp / precip / soil-moisture from Open-Meteo (parallel).
  5. Score every cell against every species profile; select best-scoring subset.
  6. Write output using existing write_collection / update_freshness helpers.
"""
from __future__ import annotations

import json
import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from itertools import islice
from typing import Dict, List, Optional, Tuple

import requests
from shapely.geometry import Point, shape

from app.config import get_settings
from app.models import HabitatCell, HabitatCellCollection, SpeciesCatalog
from app.pipelines.base import IngestionResult, update_freshness, write_collection
from app.services.scoring import NowcastScorer

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# PNW bounding box for candidate grid generation (Stage 1)
# ---------------------------------------------------------------------------
LAT_MIN, LAT_MAX = 42.0, 49.0
LON_MIN, LON_MAX = -125.0, -116.0
GRID_STEP = 0.1  # degrees (~8 km)

# Elevation filter thresholds in metres (Stage 2)
ELEV_MIN_M = 200
ELEV_MAX_M = 1600

# ESRI Living Atlas — National Forest Boundaries (public, no auth required)
USFS_NF_URL = (
    "https://services1.arcgis.com/gGHDlz6USftL5Pau/arcgis/rest/services/"
    "National_Forest_Boundaries/FeatureServer/0/query"
)
# Bounding box for spatial query (minX,minY,maxX,maxY in WGS84)
PNW_ENVELOPE = f"{LON_MIN},{LAT_MIN},{LON_MAX},{LAT_MAX}"

# Open-Meteo endpoints
ELEVATION_URL = "https://api.open-meteo.com/v1/elevation"
FORECAST_URL = "https://api.open-meteo.com/v1/forecast"

# Weather fetch concurrency (Stage 4)
MAX_WEATHER_WORKERS = 10

# Scoring thresholds (Stage 5)
TOP_N_PER_SPECIES = 15
HIGH_SCORE_THRESHOLD = 0.5

# ---------------------------------------------------------------------------
# National Forest host-species and canopy lookup tables
# Scientific names must match those used in species_profiles.json
# ---------------------------------------------------------------------------
DEFAULT_HOST_SPECIES: List[str] = ["Pseudotsuga menziesii"]
DEFAULT_CANOPY_PCT: float = 70.0

NF_HOST_SPECIES: Dict[str, List[str]] = {
    # Washington — names match FOREST_GRA field in ESRI Living Atlas dataset
    "Olympic National Forest": [
        "Pseudotsuga menziesii", "Tsuga heterophylla",
        "Picea sitchensis", "Abies amabilis",
    ],
    "Mt. Baker National Forest": [
        "Pseudotsuga menziesii", "Tsuga heterophylla",
        "Abies amabilis", "Abies lasiocarpa",
    ],
    "Snoqualmie National Forest": [
        "Pseudotsuga menziesii", "Tsuga heterophylla",
        "Abies amabilis", "Abies lasiocarpa",
    ],
    "Gifford Pinchot National Forest": [
        "Pseudotsuga menziesii", "Tsuga heterophylla",
        "Abies amabilis", "Populus trichocarpa",
    ],
    "Okanogan National Forest": [
        "Pseudotsuga menziesii", "Pinus ponderosa",
        "Tsuga heterophylla", "Abies lasiocarpa", "Populus trichocarpa",
    ],
    "Wenatchee National Forest": [
        "Pseudotsuga menziesii", "Pinus ponderosa",
        "Tsuga heterophylla", "Abies lasiocarpa", "Populus trichocarpa",
    ],
    "Colville National Forest": [
        "Pseudotsuga menziesii", "Pinus ponderosa",
        "Betula spp.", "Populus trichocarpa",
    ],
    # Oregon
    "Siuslaw National Forest": [
        "Pseudotsuga menziesii", "Tsuga heterophylla",
        "Picea sitchensis", "Acer macrophyllum",
    ],
    "Mt. Hood National Forest": [
        "Pseudotsuga menziesii", "Tsuga heterophylla",
        "Abies amabilis", "Acer macrophyllum",
    ],
    "Willamette National Forest": [
        "Pseudotsuga menziesii", "Tsuga heterophylla",
        "Abies amabilis",
    ],
    "Umpqua National Forest": [
        "Pseudotsuga menziesii", "Tsuga heterophylla",
        "Quercus garryana",
    ],
    "Rogue River National Forest": [
        "Pseudotsuga menziesii", "Pinus ponderosa",
        "Quercus garryana", "Fraxinus latifolia",
    ],
    "Siskiyou National Forest": [
        "Pseudotsuga menziesii", "Pinus ponderosa",
        "Quercus garryana",
    ],
    "Klamath National Forest": [
        "Pseudotsuga menziesii", "Pinus ponderosa",
        "Quercus garryana",
    ],
    "Deschutes National Forest": [
        "Pinus ponderosa", "Pseudotsuga menziesii",
        "Populus trichocarpa",
    ],
    "Fremont National Forest": [
        "Pinus ponderosa", "Abies lasiocarpa",
    ],
    "Winema National Forest": [
        "Pinus ponderosa", "Abies lasiocarpa",
    ],
    "Ochoco National Forest": [
        "Pinus ponderosa", "Pseudotsuga menziesii",
    ],
    "Umatilla National Forest": [
        "Pseudotsuga menziesii", "Pinus ponderosa",
        "Populus trichocarpa",
    ],
    "Malheur National Forest": [
        "Pseudotsuga menziesii", "Pinus ponderosa",
    ],
    "Wallowa National Forest": [
        "Pseudotsuga menziesii", "Pinus ponderosa",
        "Populus trichocarpa", "Betula spp.",
    ],
    "Whitman National Forest": [
        "Pseudotsuga menziesii", "Pinus ponderosa",
        "Populus trichocarpa",
    ],
    # Idaho
    "Kaniksu National Forest": [
        "Pseudotsuga menziesii", "Pinus ponderosa",
        "Betula spp.", "Populus trichocarpa",
    ],
    "Coeur D Alene National Forest": [
        "Pseudotsuga menziesii", "Pinus ponderosa",
        "Betula spp.", "Populus trichocarpa",
    ],
    "St. Joe National Forest": [
        "Pseudotsuga menziesii", "Pinus ponderosa",
        "Betula spp.", "Populus trichocarpa",
    ],
    "Nezperce National Forest": [
        "Pseudotsuga menziesii", "Pinus ponderosa",
        "Populus trichocarpa",
    ],
    "Clearwater National Forest": [
        "Pseudotsuga menziesii", "Pinus ponderosa",
        "Populus trichocarpa",
    ],
    "Payette National Forest": [
        "Pinus ponderosa", "Pseudotsuga menziesii",
    ],
    "Boise National Forest": [
        "Pseudotsuga menziesii", "Pinus ponderosa",
    ],
    # Montana/Idaho border
    "Kootenai National Forest": [
        "Pseudotsuga menziesii", "Pinus ponderosa",
        "Betula spp.", "Populus trichocarpa",
    ],
}

CANOPY_BY_FOREST: Dict[str, float] = {
    "Olympic National Forest": 90.0,
    "Mt. Baker National Forest": 85.0,
    "Snoqualmie National Forest": 85.0,
    "Gifford Pinchot National Forest": 80.0,
    "Okanogan National Forest": 65.0,
    "Wenatchee National Forest": 68.0,
    "Colville National Forest": 65.0,
    "Siuslaw National Forest": 80.0,
    "Mt. Hood National Forest": 78.0,
    "Willamette National Forest": 82.0,
    "Umpqua National Forest": 72.0,
    "Rogue River National Forest": 70.0,
    "Siskiyou National Forest": 68.0,
    "Klamath National Forest": 65.0,
    "Deschutes National Forest": 55.0,
    "Fremont National Forest": 50.0,
    "Winema National Forest": 52.0,
    "Ochoco National Forest": 50.0,
    "Umatilla National Forest": 55.0,
    "Malheur National Forest": 55.0,
    "Wallowa National Forest": 60.0,
    "Whitman National Forest": 58.0,
    "Kaniksu National Forest": 70.0,
    "Coeur D Alene National Forest": 70.0,
    "St. Joe National Forest": 72.0,
    "Nezperce National Forest": 68.0,
    "Clearwater National Forest": 72.0,
    "Payette National Forest": 62.0,
    "Boise National Forest": 60.0,
    "Kootenai National Forest": 72.0,
}


# ---------------------------------------------------------------------------
# Stage 1 — Candidate Grid
# ---------------------------------------------------------------------------

def _generate_candidates() -> List[dict]:
    """Return all 0.1° grid points within the PNW bounding box."""
    candidates = []
    lat = LAT_MIN
    while lat <= LAT_MAX + 1e-9:
        lon = LON_MIN
        while lon <= LON_MAX + 1e-9:
            candidates.append({
                "cell_id": f"disc-{lat:.2f}-{lon:.2f}",
                "latitude": round(lat, 2),
                "longitude": round(lon, 2),
            })
            lon = round(lon + GRID_STEP, 2)
        lat = round(lat + GRID_STEP, 2)
    return candidates


# ---------------------------------------------------------------------------
# Stage 2 — Elevation Filter
# ---------------------------------------------------------------------------

def _batched(iterable, n: int):
    """Yield successive n-sized chunks from iterable."""
    it = iter(iterable)
    while batch := list(islice(it, n)):
        yield batch


def _filter_by_elevation(candidates: List[dict]) -> List[dict]:
    """Keep only points within ELEV_MIN_M–ELEV_MAX_M via Open-Meteo Elevation API."""
    surviving = []
    batches = list(_batched(candidates, 100))
    print(f"Stage 2: checking elevation for {len(candidates)} candidates "
          f"in {len(batches)} batches...")

    for i, batch in enumerate(batches, start=1):
        lats = ",".join(str(p["latitude"]) for p in batch)
        lons = ",".join(str(p["longitude"]) for p in batch)
        # Retry up to 3 times with backoff on rate-limit responses
        for attempt in range(3):
            try:
                resp = requests.get(
                    ELEVATION_URL,
                    params={"latitude": lats, "longitude": lons},
                    timeout=15,
                )
                if resp.status_code == 429:
                    wait = 2 ** attempt * 2  # 2s, 4s, 8s
                    logger.warning(
                        "Elevation batch %d/%d rate-limited; retrying in %ds",
                        i, len(batches), wait,
                    )
                    time.sleep(wait)
                    continue
                resp.raise_for_status()
                elevations = resp.json().get("elevation", [])
                break
            except Exception as exc:
                if attempt == 2:
                    logger.warning("Elevation batch %d/%d failed: %s", i, len(batches), exc)
                    elevations = []
                else:
                    time.sleep(2 ** attempt)
        else:
            elevations = []

        for point, elev in zip(batch, elevations):
            if elev is not None and ELEV_MIN_M <= elev <= ELEV_MAX_M:
                point["elevation_m"] = elev
                surviving.append(point)

        # Polite pause between batches to stay under Open-Meteo rate limits
        time.sleep(1.0)

    print(f"Stage 2: {len(surviving)} points survive elevation filter "
          f"({ELEV_MIN_M}–{ELEV_MAX_M} m)")
    return surviving


# ---------------------------------------------------------------------------
# Stage 3 — National Forest Filter
# ---------------------------------------------------------------------------

def _fetch_nf_polygons() -> List[Tuple[str, object]]:
    """Fetch NF boundary polygons from USFS ArcGIS REST API.

    Returns list of (forest_name, shapely_geometry) tuples.
    Paginates until all features are received.
    """
    features: List[Tuple[str, object]] = []
    offset = 0
    page_size = 100
    print("Stage 3: fetching National Forest boundaries from USFS ArcGIS...")

    while True:
        params = {
            "where": "UNITCLASSI='National Forest'",
            "geometry": PNW_ENVELOPE,
            "geometryType": "esriGeometryEnvelope",
            "spatialRel": "esriSpatialRelIntersects",
            "inSR": "4326",
            "outFields": "FOREST_GRA",
            "f": "geojson",
            "resultRecordCount": page_size,
            "resultOffset": offset,
            "geometryPrecision": 4,
            "outSR": "4326",
        }
        try:
            resp = requests.get(USFS_NF_URL, params=params, timeout=30)
            resp.raise_for_status()
            data = resp.json()
        except Exception as exc:
            logger.error("Failed to fetch NF boundaries at offset %d: %s", offset, exc)
            break

        page_features = data.get("features", [])
        if not page_features:
            break

        for feat in page_features:
            name = feat.get("properties", {}).get("FOREST_GRA", "Unknown Forest")
            try:
                geom = shape(feat["geometry"])
                features.append((name, geom))
            except Exception as exc:
                logger.warning("Could not parse geometry for '%s': %s", name, exc)

        print(f"  fetched {len(page_features)} NF polygons at offset={offset} "
              f"(total so far: {len(features)})")

        if len(page_features) < page_size:
            break  # last page
        offset += page_size

    print(f"Stage 3: loaded {len(features)} National Forest polygon(s)")
    return features


def _filter_by_national_forest(
    candidates: List[dict],
    nf_polygons: List[Tuple[str, object]],
) -> List[dict]:
    """Keep only points inside a NF polygon; attach host_species and canopy."""
    surviving = []
    for point in candidates:
        pt = Point(point["longitude"], point["latitude"])
        for forest_name, geom in nf_polygons:
            if geom.contains(pt):
                point["forest_name"] = forest_name
                point["host_species_present"] = NF_HOST_SPECIES.get(
                    forest_name, DEFAULT_HOST_SPECIES
                )
                point["canopy_density_pct"] = CANOPY_BY_FOREST.get(
                    forest_name, DEFAULT_CANOPY_PCT
                )
                surviving.append(point)
                break  # assign to first matching forest only

    print(f"Stage 3: {len(surviving)} points inside National Forest boundaries")
    return surviving


# ---------------------------------------------------------------------------
# Stage 4 — Real Weather Fetch (parallel)
# ---------------------------------------------------------------------------

def _fetch_weather(point: dict) -> HabitatCell:
    """Fetch soil-temp / precip / soil-moisture from Open-Meteo for one point."""
    params = {
        "latitude": point["latitude"],
        "longitude": point["longitude"],
        "hourly": "soil_temperature_6cm,soil_moisture_3_to_9cm",
        "daily": "precipitation_sum",
        "past_days": 7,
        "forecast_days": 1,
        "timezone": "America/Los_Angeles",
    }
    resp = requests.get(FORECAST_URL, params=params, timeout=15)
    resp.raise_for_status()
    data = resp.json()

    soil_temps: List[Optional[float]] = data["hourly"]["soil_temperature_6cm"]
    soil_temp = next((v for v in reversed(soil_temps) if v is not None), 0.0)

    soil_moisture_vals: List[Optional[float]] = data["hourly"]["soil_moisture_3_to_9cm"]
    soil_moisture = next((v for v in reversed(soil_moisture_vals) if v is not None), None)

    precip_daily: List[Optional[float]] = data["daily"]["precipitation_sum"]
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


def _fetch_all_weather(points: List[dict]) -> List[HabitatCell]:
    """Fetch weather for all NF-filtered points in parallel."""
    cells: List[HabitatCell] = []
    errors = 0
    total = len(points)
    print(f"Stage 4: fetching weather for {total} points "
          f"({MAX_WEATHER_WORKERS} parallel workers)...")

    with ThreadPoolExecutor(max_workers=MAX_WEATHER_WORKERS) as executor:
        future_to_point = {executor.submit(_fetch_weather, p): p for p in points}
        for future in as_completed(future_to_point):
            point = future_to_point[future]
            try:
                cells.append(future.result())
                if len(cells) % 50 == 0:
                    print(f"  {len(cells)}/{total} weather fetches complete...")
            except Exception as exc:
                errors += 1
                logger.warning(
                    "Weather fetch failed for %s: %s", point["cell_id"], exc
                )

    print(f"Stage 4: {len(cells)} OK, {errors} failed")
    return cells


# ---------------------------------------------------------------------------
# Stage 5 — Score + Select
# ---------------------------------------------------------------------------

def _score_and_select(
    cells: List[HabitatCell],
    profiles,
) -> List[HabitatCell]:
    """Score every cell against every profile; return the best-scoring subset.

    Selection is the union of:
      - Top TOP_N_PER_SPECIES cells per species (ensures off-season coverage)
      - Any cell scoring >= HIGH_SCORE_THRESHOLD on any species
    """
    scorer = NowcastScorer()
    reference_time = datetime.now(timezone.utc)

    selected_ids: set[str] = set()
    high_score_ids: set[str] = set()

    for profile in profiles:
        scored = [scorer.score_cell(cell, profile, reference_time) for cell in cells]
        scored.sort(key=lambda sc: sc.score, reverse=True)

        # Top N per species
        for sc in scored[:TOP_N_PER_SPECIES]:
            selected_ids.add(sc.cell.cell_id)

        # High-score threshold
        for sc in scored:
            if sc.score >= HIGH_SCORE_THRESHOLD:
                high_score_ids.add(sc.cell.cell_id)

    selected_ids.update(high_score_ids)

    if not selected_ids:
        logger.warning("No cells selected — returning all NF-filtered cells as fallback")
        return cells

    cell_by_id = {cell.cell_id: cell for cell in cells}
    output = [cell_by_id[cid] for cid in selected_ids if cid in cell_by_id]

    print(f"Stage 5: selected {len(output)} cells "
          f"(top-{TOP_N_PER_SPECIES}/species + {len(high_score_ids)} high-score "
          f"≥{HIGH_SCORE_THRESHOLD})")
    return output


# ---------------------------------------------------------------------------
# Pipeline entry point
# ---------------------------------------------------------------------------

def run() -> IngestionResult:
    settings = get_settings()

    # Load species profiles
    with settings.species_profile_path.open(encoding="utf-8") as fh:
        catalog = SpeciesCatalog.model_validate(json.load(fh))
    profiles = catalog.species

    # Stage 1
    print("Stage 1: generating candidate grid...")
    candidates = _generate_candidates()
    print(f"Stage 1: {len(candidates)} candidate points generated")

    # Stage 2
    elev_filtered = _filter_by_elevation(candidates)
    if not elev_filtered:
        raise RuntimeError("No candidates survived elevation filter")

    # Stage 3
    nf_polygons = _fetch_nf_polygons()
    if not nf_polygons:
        raise RuntimeError("Failed to fetch any National Forest polygons")
    nf_filtered = _filter_by_national_forest(elev_filtered, nf_polygons)
    if not nf_filtered:
        raise RuntimeError("No candidates fall inside National Forest boundaries")

    # Stage 4
    cells = _fetch_all_weather(nf_filtered)
    if not cells:
        raise RuntimeError("All weather fetches failed — cannot produce output")

    # Stage 5
    output_cells = _score_and_select(cells, profiles)

    # Stage 6 — Write output
    collection = HabitatCellCollection(cells=output_cells)
    rows = write_collection(collection, settings.processed_grid_path)

    result = IngestionResult(
        source_id="forest_habitat_discovery",
        output_path=settings.processed_grid_path,
        rows_written=rows,
        last_ingested=datetime.now(timezone.utc),
    )
    update_freshness(result, settings.freshness_path)
    return result


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    print("=== Forest Habitat Discovery Pipeline ===")
    result = run()
    print(f"\nWrote {result.rows_written} cells to {result.output_path}")


if __name__ == "__main__":
    main()
