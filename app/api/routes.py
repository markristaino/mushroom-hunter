"""API routes for the mushroom nowcast service."""
from datetime import datetime
from typing import List

from fastapi import APIRouter, HTTPException, Query

from app.config import get_settings
from app.models import SpeciesCatalog
from app.services import data_loader
from app.services.scoring import NowcastScorer, ScoredCell

router = APIRouter(prefix="/api", tags=["nowcast"])


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


@router.get("/nowcast", summary="Get nowcast scores for a species")
def nowcast(
    species_id: str = Query(default_factory=lambda: get_settings().default_species_id),
    as_of: datetime | None = None,
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
