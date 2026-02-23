"""300m habitat refinement pipeline.

Stages:
  1. Load weather anchors from habitat_cells.json (output of forest_habitat_discovery).
  2. Generate candidate 300m (~0.003°) grid across PNW bounding box.
  3. Filter candidates by NLCD 2021 canopy cover >= 30% (real per-pixel values).
  4. Filter candidates to National Forest polygons; attach forest metadata.
  5. Assign weather from nearest 0.05° anchor via KDTree.
  6. Score all cells vectorized across every species profile; keep max_score per cell.
  7. Write parquet (snappy, row_group_size=10_000) of cells with max_score >= 0.3.

Run after forest_habitat_discovery:
    python3 -m app.pipelines.habitat_refinement

Key gotcha: NLCD raster is Albers (EPSG:5070), not WGS84.
Use pyproj.Transformer to convert lat/lon query coordinates before pixel lookup.
"""
from __future__ import annotations

import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests
from scipy.spatial import KDTree
import shapely
from shapely.geometry import shape

from app.config import get_settings
from app.models import SpeciesCatalog
from app.pipelines.forest_habitat_discovery import (
    DEFAULT_CANOPY_PCT,
    DEFAULT_HOST_SPECIES,
    ELEV_MIN_M,
    ELEV_MAX_M,
    LON_MIN,
    LON_MAX,
    LAT_MIN,
    LAT_MAX,
    NF_HOST_SPECIES,
    CANOPY_BY_FOREST,
    PNW_ENVELOPE,
    USFS_NF_URL,
)

logger = logging.getLogger(__name__)

# 300m grid step in degrees (~0.003° ≈ 333m at 45°N)
GRID_STEP_300M = 0.003

# Minimum NLCD canopy cover to keep a cell
CANOPY_MIN_PCT = 30

# Minimum max_score across species to include in parquet output
MIN_SCORE_OUTPUT = 0.3

# Parquet write settings
PARQUET_ROW_GROUP_SIZE = 10_000
PARQUET_COMPRESSION = "snappy"


# ---------------------------------------------------------------------------
# Stage 1 — Load weather anchors
# ---------------------------------------------------------------------------

def _load_weather_anchors(json_path: Path) -> pd.DataFrame:
    """Load habitat_cells.json into a DataFrame of weather anchor points."""
    print(f"Stage 1: loading weather anchors from {json_path}...", flush=True)
    with json_path.open(encoding="utf-8") as fh:
        data = json.load(fh)

    cells = data.get("cells", [])
    if not cells:
        raise RuntimeError(f"No cells found in {json_path}")

    rows = []
    for cell in cells:
        rows.append({
            "anchor_id": cell["cell_id"],
            "anchor_lat": cell["latitude"],
            "anchor_lon": cell["longitude"],
            "soil_temperature_c": cell.get("soil_temperature_c", 0.0),
            "precipitation_mm_last_7d": cell.get("precipitation_mm_last_7d", 0.0),
            "soil_moisture_index": cell.get("soil_moisture_index"),
            "last_observation": cell.get("last_observation"),
        })

    df = pd.DataFrame(rows)
    print(f"Stage 1: loaded {len(df)} weather anchors", flush=True)
    return df


# ---------------------------------------------------------------------------
# Stages 2+4 — Generate 300m grid per-NF-polygon and filter (fast)
# ---------------------------------------------------------------------------

def _generate_nf_cells(
    nf_polygons: List[Tuple[str, object]],
    canopy_sentinel: np.uint8 = np.uint8(255),
) -> pd.DataFrame:
    """Generate 300m grid points inside each NF polygon.

    Strategy: for each polygon, generate a 0.003° grid within its bounding box,
    then keep only points that fall inside the polygon using shapely.contains_xy
    (vectorized C call — no Python loop over points, no STRtree overhead).

    Returns DataFrame with columns:
        latitude, longitude, canopy_pct_nlcd (None = not sampled),
        forest_name, host_species_str (pipe-delimited), canopy_density_pct
    """
    frames = []
    total_candidates = 0

    for i, (fname, geom) in enumerate(nf_polygons):
        minx, miny, maxx, maxy = geom.bounds
        # Snap bbox to 0.003° grid
        lons = np.arange(
            np.floor(minx / GRID_STEP_300M) * GRID_STEP_300M,
            maxx + GRID_STEP_300M,
            GRID_STEP_300M,
        )
        lats = np.arange(
            np.floor(miny / GRID_STEP_300M) * GRID_STEP_300M,
            maxy + GRID_STEP_300M,
            GRID_STEP_300M,
        )
        # Clip to PNW bounding box
        lons = lons[(lons >= LON_MIN) & (lons <= LON_MAX)]
        lats = lats[(lats >= LAT_MIN) & (lats <= LAT_MAX)]

        if len(lons) == 0 or len(lats) == 0:
            continue

        lon_grid, lat_grid = np.meshgrid(lons, lats)
        cand_lons = lon_grid.ravel()
        cand_lats = lat_grid.ravel()
        total_candidates += len(cand_lons)

        # shapely.contains_xy: vectorized C call, fast even for 100k+ points
        inside = shapely.contains_xy(geom, cand_lons, cand_lats)

        if not inside.any():
            continue

        frames.append(pd.DataFrame({
            "latitude": cand_lats[inside],
            "longitude": cand_lons[inside],
            "canopy_pct_nlcd": None,  # filled later if NLCD available
            "forest_name": fname,
            "host_species_str": "|".join(NF_HOST_SPECIES.get(fname, DEFAULT_HOST_SPECIES)),
            "canopy_density_pct": float(CANOPY_BY_FOREST.get(fname, DEFAULT_CANOPY_PCT)),
        }))

        if (i + 1) % 5 == 0 or (i + 1) == len(nf_polygons):
            n_so_far = sum(len(f) for f in frames)
            print(f"  {i+1}/{len(nf_polygons)} polygons processed | "
                  f"{n_so_far:,} cells inside NF so far", flush=True)

    if not frames:
        raise RuntimeError("No cells fell inside any National Forest polygon")

    df = pd.concat(frames, ignore_index=True)
    # Drop exact duplicates from overlapping forest bboxes (keep first assignment)
    df = df.drop_duplicates(subset=["latitude", "longitude"], keep="first")
    print(f"Stages 2+4: {total_candidates:,} bbox candidates → "
          f"{len(df):,} unique cells inside NF boundaries", flush=True)
    return df


# ---------------------------------------------------------------------------
# Stage 3 — NLCD canopy filter
# ---------------------------------------------------------------------------

def _load_nlcd_canopy(tif_path: Path):
    """Open NLCD tif; return (band_array, transform, nodata, crs)."""
    try:
        import rasterio
    except ImportError:
        raise RuntimeError("rasterio is required: pip install rasterio")

    print(f"Stage 3: opening NLCD raster {tif_path}...", flush=True)
    ds = rasterio.open(tif_path)
    print(f"  CRS: {ds.crs}  size: {ds.width}×{ds.height}  dtype: {ds.dtypes[0]}", flush=True)
    return ds


def _sample_nlcd_canopy(
    lats: np.ndarray,
    lons: np.ndarray,
    ds,
) -> np.ndarray:
    """Sample NLCD canopy % at each (lat, lon) point.

    NLCD raster is Albers (EPSG:5070). We use pyproj to transform WGS84 coords
    to the raster CRS, then use rasterio.transform.rowcol for pixel lookup.
    Returns array of uint8 canopy values (0–100); out-of-bounds → 0.
    """
    try:
        import rasterio
        from pyproj import Transformer
    except ImportError as exc:
        raise RuntimeError(f"Missing dependency: {exc}")

    # Transform WGS84 → raster CRS
    transformer = Transformer.from_crs("EPSG:4326", ds.crs.to_epsg() or ds.crs.to_wkt(),
                                        always_xy=True)
    xs, ys = transformer.transform(lons, lats)

    # Convert projected coords to row/col
    rows_px, cols_px = rasterio.transform.rowcol(ds.transform, xs, ys)
    rows_px = np.asarray(rows_px)
    cols_px = np.asarray(cols_px)

    # Mask out-of-bounds pixels
    valid = (
        (rows_px >= 0) & (rows_px < ds.height) &
        (cols_px >= 0) & (cols_px < ds.width)
    )

    canopy = np.zeros(len(lats), dtype=np.uint8)
    if valid.any():
        # Read only the rows we need (windowed, sorted for locality)
        # For simplicity, read the whole band once (≈700 MB uint8 in memory).
        # If memory is tight, switch to windowed reads per lat slice.
        band = ds.read(1)
        canopy[valid] = band[rows_px[valid], cols_px[valid]]

    n_valid = valid.sum()
    nodata = ds.nodata
    if nodata is not None:
        nodata_mask = canopy == int(nodata)
        canopy[nodata_mask] = 0

    print(f"Stage 3: sampled {n_valid:,} valid pixels from NLCD band", flush=True)
    return canopy


# ---------------------------------------------------------------------------
# Stage 4 — National Forest boundary filter (vectorized with STRtree)
# ---------------------------------------------------------------------------

def _fetch_nf_polygons_for_refinement() -> List[Tuple[str, object]]:
    """Fetch NF boundary polygons (same logic as forest_habitat_discovery)."""
    features: List[Tuple[str, object]] = []
    offset = 0
    page_size = 100
    print("Stage 4: fetching National Forest boundaries...", flush=True)

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
                if not geom.is_valid:
                    geom = geom.buffer(0)
                features.append((name, geom))
            except Exception as exc:
                logger.warning("Could not parse geometry for '%s': %s", name, exc)

        if len(page_features) < page_size:
            break
        offset += page_size

    print(f"Stage 4: loaded {len(features)} NF polygon(s)", flush=True)
    return features


# ---------------------------------------------------------------------------
# Stage 5 — Weather assignment via KDTree
# ---------------------------------------------------------------------------

def _build_weather_kdtree(anchors: pd.DataFrame) -> Tuple[KDTree, pd.DataFrame]:
    """Build KDTree from anchor (lat, lon) pairs for fast nearest-neighbor lookup."""
    coords = anchors[["anchor_lat", "anchor_lon"]].to_numpy()
    tree = KDTree(coords)
    return tree, anchors


def _assign_weather(cells_df: pd.DataFrame, anchors: pd.DataFrame, tree: KDTree) -> pd.DataFrame:
    """Assign weather fields from nearest anchor to each cell."""
    print("Stage 5: assigning weather from nearest anchors...", flush=True)
    query_coords = cells_df[["latitude", "longitude"]].to_numpy()
    _, idx = tree.query(query_coords)

    matched = anchors.iloc[idx].reset_index(drop=True)
    cells_df = cells_df.reset_index(drop=True)
    cells_df["weather_anchor_id"] = matched["anchor_id"].values
    cells_df["soil_temperature_c"] = matched["soil_temperature_c"].values
    cells_df["precipitation_mm_last_7d"] = matched["precipitation_mm_last_7d"].values
    cells_df["soil_moisture_index"] = matched["soil_moisture_index"].values
    cells_df["last_observation"] = matched["last_observation"].values

    print(f"Stage 5: weather assigned for {len(cells_df):,} cells", flush=True)
    return cells_df


# ---------------------------------------------------------------------------
# Stage 6 — Vectorized scoring
# ---------------------------------------------------------------------------

def _range_score_vec(values: np.ndarray, minimum: float, maximum: float) -> np.ndarray:
    """Vectorized continuous range score: 1.0 at midpoint, fades to 0 at edges."""
    half_range = (maximum - minimum) / 2.0
    if half_range == 0:
        return np.where(values == minimum, 1.0, 0.0).astype(np.float32)
    midpoint = minimum + half_range
    return np.clip(1.0 - np.abs(values - midpoint) / half_range, 0.0, 1.0).astype(np.float32)


def _phenology_score_scalar(month: int, phenology_months: List[int]) -> float:
    """1.0 in season, 0.5 shoulder, 0 otherwise."""
    if month in phenology_months:
        return 1.0
    prev_m = 12 if month == 1 else month - 1
    next_m = 1 if month == 12 else month + 1
    if prev_m in phenology_months or next_m in phenology_months:
        return 0.5
    return 0.0


def _score_cells_vectorized(
    cells_df: pd.DataFrame,
    profiles,
    reference_time: datetime,
) -> np.ndarray:
    """Score all cells against all species; return max_score per cell (float32 array).

    Weights mirror NowcastScorer defaults:
        soil_temperature: 0.3
        precipitation:    0.3
        host_species:     0.2
        phenology:        0.2
    """
    print(f"Stage 6: scoring {len(cells_df):,} cells × {len(profiles)} species...", flush=True)

    soil_temps = cells_df["soil_temperature_c"].to_numpy(dtype=np.float32)
    precips = cells_df["precipitation_mm_last_7d"].to_numpy(dtype=np.float32)
    host_species_lists = cells_df["host_species_str"].tolist()  # pipe-delimited strings
    month = reference_time.month

    max_scores = np.zeros(len(cells_df), dtype=np.float32)

    for profile in profiles:
        # Soil temperature component
        st_score = _range_score_vec(
            soil_temps,
            profile.soil_temperature_c.minimum,
            profile.soil_temperature_c.maximum,
        ) * 0.3

        # Precipitation component
        pr_score = _range_score_vec(
            precips,
            profile.precipitation_mm_last_7d.minimum,
            profile.precipitation_mm_last_7d.maximum,
        ) * 0.3

        # Host species component — fraction of profile hosts present in each cell
        profile_hosts = {h.scientific_name for h in profile.host_species}
        total_hosts = len(profile_hosts)
        if total_hosts > 0:
            host_fracs = np.array(
                [
                    len(profile_hosts.intersection(set(s.split("|")))) / total_hosts
                    for s in host_species_lists
                ],
                dtype=np.float32,
            )
        else:
            host_fracs = np.zeros(len(cells_df), dtype=np.float32)
        hs_score = host_fracs * 0.2

        # Phenology component — scalar, same for all cells
        pheno = _phenology_score_scalar(month, profile.phenology_months)
        pheno_score = np.full(len(cells_df), pheno * 0.2, dtype=np.float32)

        species_score = st_score + pr_score + hs_score + pheno_score
        np.maximum(max_scores, species_score, out=max_scores)

    print(f"Stage 6: scored — max={max_scores.max():.3f}, "
          f"mean={max_scores.mean():.3f}, "
          f"cells >= 0.3: {(max_scores >= 0.3).sum():,}", flush=True)
    return max_scores


# ---------------------------------------------------------------------------
# Stage 7 — Write parquet
# ---------------------------------------------------------------------------

def _write_parquet(cells_df: pd.DataFrame, output_path: Path) -> int:
    """Write cells to snappy-compressed parquet; return row count written."""
    output_path.parent.mkdir(parents=True, exist_ok=True)

    table = pa.Table.from_pandas(cells_df, preserve_index=False)
    pq.write_table(
        table,
        str(output_path),
        compression=PARQUET_COMPRESSION,
        row_group_size=PARQUET_ROW_GROUP_SIZE,
    )
    rows = len(cells_df)
    size_mb = output_path.stat().st_size / (1024 * 1024)
    print(f"Stage 7: wrote {rows:,} rows → {output_path} ({size_mb:.1f} MB)", flush=True)
    return rows


# ---------------------------------------------------------------------------
# Pipeline entry point
# ---------------------------------------------------------------------------

def run() -> int:
    """Run the 300m habitat refinement pipeline. Returns number of rows written."""
    settings = get_settings()

    if not settings.processed_grid_path.exists():
        raise RuntimeError(
            f"Weather anchors not found at {settings.processed_grid_path}. "
            "Run forest_habitat_discovery pipeline first."
        )

    # Load species profiles
    with settings.species_profile_path.open(encoding="utf-8") as fh:
        catalog = SpeciesCatalog.model_validate(json.load(fh))
    profiles = catalog.species

    # Stage 1 — weather anchors
    anchors = _load_weather_anchors(settings.processed_grid_path)

    # Stages 2+4 — fetch NF polygons then generate 300m grid per-polygon
    nf_polygons = _fetch_nf_polygons_for_refinement()
    if not nf_polygons:
        raise RuntimeError("Failed to fetch any National Forest polygons")

    print("Stages 2+4: generating 300m grid inside NF polygons...", flush=True)
    cells_df = _generate_nf_cells(nf_polygons)

    # Stage 3 — NLCD canopy filter (optional: applied to cells_df if TIF present)
    if settings.nlcd_canopy_path.exists():
        ds = _load_nlcd_canopy(settings.nlcd_canopy_path)
        lats_arr = cells_df["latitude"].to_numpy()
        lons_arr = cells_df["longitude"].to_numpy()
        canopy_vals = _sample_nlcd_canopy(lats_arr, lons_arr, ds)
        ds.close()
        canopy_mask = canopy_vals >= CANOPY_MIN_PCT
        cells_df = cells_df[canopy_mask].copy()
        cells_df["canopy_pct_nlcd"] = canopy_vals[canopy_mask].astype(object)
        print(f"Stage 3: {canopy_mask.sum():,} cells with NLCD canopy >= {CANOPY_MIN_PCT}%", flush=True)
        if len(cells_df) == 0:
            raise RuntimeError("No cells pass the canopy filter — check NLCD raster coverage")
    else:
        print("Stage 3: NLCD raster not present — per-forest canopy lookup used (canopy_pct_nlcd=null)", flush=True)

    # Stage 5 — weather assignment
    tree, anchors_df = _build_weather_kdtree(anchors)
    cells_df = _assign_weather(cells_df, anchors_df, tree)

    # Stage 6 — vectorized scoring
    reference_time = datetime.now(timezone.utc)
    max_scores = _score_cells_vectorized(cells_df, profiles, reference_time)
    cells_df["max_score"] = max_scores

    # Filter to min score threshold
    output_df = cells_df[cells_df["max_score"] >= MIN_SCORE_OUTPUT].copy()
    output_df = output_df.sort_values("latitude").reset_index(drop=True)
    print(f"Stage 6→7: {len(output_df):,} cells with max_score >= {MIN_SCORE_OUTPUT}", flush=True)

    if len(output_df) == 0:
        logger.warning("No cells meet minimum score threshold — writing empty parquet")

    # Stage 7 — write parquet
    rows = _write_parquet(output_df, settings.refined_grid_path)
    return rows


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(levelname)s %(asctime)s %(message)s",
        datefmt="%H:%M:%S",
    )
    print("=== Habitat Refinement Pipeline (300m) ===", flush=True)
    rows = run()
    settings = get_settings()
    print(f"\nComplete: {rows:,} cells written to {settings.refined_grid_path}", flush=True)


if __name__ == "__main__":
    main()
