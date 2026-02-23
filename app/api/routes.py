"""API routes for the mushroom nowcast service."""
from datetime import datetime
from typing import List, Optional

import pyarrow.parquet as pq
from fastapi import APIRouter, HTTPException, Query

from app.config import get_settings
from app.models import SpeciesCatalog
from app.services import data_loader
from app.services.scoring import NowcastScorer, ScoredCell

router = APIRouter(prefix="/api", tags=["nowcast"])

REFINED_MAX_ROWS = 5_000


def _get_species_catalog() -> SpeciesCatalog:
    return data_loader.load_species_catalog()


def _score_species(species_id: str, reference_time: datetime) -> List[ScoredCell]:
    catalog = _get_species_catalog()
    try:
        profile = catalog.get(species_id)
    except KeyError as exc:  # pragma: no cover - FastAPI handles
        raise HTTPException(status_code=404, detail=str(exc)) from exc

    cells = data_loader.load_habitat_cells()
    scorer = NowcastScorer()
    return [scorer.score_cell(cell, profile, reference_time) for cell in cells]


@router.get("/health", summary="Health check")
def health() -> dict:
    return {"status": "ok", "timestamp": datetime.utcnow()}


@router.get("/species", summary="List supported species")
def list_species() -> dict:
    catalog = _get_species_catalog()
    return {"species": catalog.list_ids()}


@router.get("/nowcast_refined", summary="Get 300m resolution nowcast for a viewport")
def nowcast_refined(
    species_id: str = Query(default_factory=lambda: get_settings().default_species_id),
    min_score: float = Query(0.3, ge=0.0, le=1.0),
    min_lat: float = Query(..., description="Viewport south boundary"),
    max_lat: float = Query(..., description="Viewport north boundary"),
    min_lon: float = Query(..., description="Viewport west boundary"),
    max_lon: float = Query(..., description="Viewport east boundary"),
) -> dict:
    settings = get_settings()
    parquet_path = settings.refined_grid_path

    if not parquet_path.exists():
        raise HTTPException(
            status_code=503,
            detail="300m habitat data not available. Run habitat_refinement pipeline first.",
        )

    # Predicate pushdown at C++ layer — only reads matching row groups
    filters = [
        ("latitude", ">=", min_lat),
        ("latitude", "<=", max_lat),
        ("longitude", ">=", min_lon),
        ("longitude", "<=", max_lon),
        ("max_score", ">=", min_score),
    ]
    try:
        table = pq.read_table(str(parquet_path), filters=filters)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to read parquet: {exc}") from exc

    if len(table) > REFINED_MAX_ROWS:
        raise HTTPException(
            status_code=400,
            detail=f"Viewport contains {len(table):,} cells (limit {REFINED_MAX_ROWS:,}). Zoom in further.",
        )

    df = table.to_pandas()

    # Re-score for the requested species so components are included in response
    catalog = _get_species_catalog()
    try:
        profile = catalog.get(species_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc

    from app.models import HabitatCell
    reference_time = datetime.utcnow()
    scorer = NowcastScorer()

    result_cells = []
    for _, row in df.iterrows():
        host_species = row["host_species_str"].split("|") if row.get("host_species_str") else []
        last_obs = row.get("last_observation")
        if last_obs is None:
            last_obs = reference_time

        cell = HabitatCell(
            cell_id=f"ref-{row['latitude']:.4f}-{row['longitude']:.4f}",
            latitude=row["latitude"],
            longitude=row["longitude"],
            host_species_present=host_species,
            soil_temperature_c=float(row["soil_temperature_c"]),
            precipitation_mm_last_7d=float(row["precipitation_mm_last_7d"]),
            soil_moisture_index=row.get("soil_moisture_index"),
            canopy_density_pct=row.get("canopy_density_pct"),
            canopy_pct_nlcd=int(row["canopy_pct_nlcd"]) if row.get("canopy_pct_nlcd") is not None else None,
            weather_anchor_id=row.get("weather_anchor_id"),
            last_observation=last_obs,
        )
        sc = scorer.score_cell(cell, profile, reference_time)
        if sc.score >= min_score:
            result_cells.append({
                "cell_id": cell.cell_id,
                "latitude": cell.latitude,
                "longitude": cell.longitude,
                "score": sc.score,
                "canopy_pct_nlcd": cell.canopy_pct_nlcd,
                "weather_anchor_id": cell.weather_anchor_id,
                "components": [
                    {
                        "name": c.name,
                        "passed": c.passed,
                        "detail": c.detail,
                        "weight": c.weight,
                    }
                    for c in sc.components
                ],
                "last_observation": cell.last_observation,
            })

    return {
        "species_id": species_id,
        "as_of": reference_time,
        "count": len(result_cells),
        "resolution": "300m",
        "cells": result_cells,
    }


@router.get("/nowcast", summary="Get nowcast scores for a species")
def nowcast(
    species_id: str = Query(default_factory=lambda: get_settings().default_species_id),
    as_of: Optional[datetime] = None,
    min_score: float = Query(0.0, ge=0.0, le=1.0),
) -> dict:
    reference_time = as_of or datetime.utcnow()
    scored_cells = _score_species(species_id, reference_time)
    filtered = [cell for cell in scored_cells if cell.score >= min_score]
    return {
        "species_id": species_id,
        "as_of": reference_time,
        "count": len(filtered),
        "cells": [
            {
                "cell_id": sc.cell.cell_id,
                "latitude": sc.cell.latitude,
                "longitude": sc.cell.longitude,
                "score": sc.score,
                "components": [
                    {
                        "name": component.name,
                        "passed": component.passed,
                        "detail": component.detail,
                        "weight": component.weight,
                    }
                    for component in sc.components
                ],
                "last_observation": sc.cell.last_observation,
            }
            for sc in filtered
        ],
    }
