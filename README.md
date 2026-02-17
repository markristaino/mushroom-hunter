# Mushroom Nowcast Service

A FastAPI-based backend that scores current ("right now") habitat likelihood for
select NW mushroom species (chanterelle, morel, gypsy, hedgehog, lion's mane)
using deterministic rules derived from curated species profiles and recent
environmental readings.

## Features
- Species profiles stored as editable JSON (soil temps, precipitation windows,
  phenology months, host species).
- Simple data loaders that read species profiles and habitat cells from the
  `data/` directory.
- Deterministic scoring engine with interpretable components (soil temperature,
  precipitation, host species presence, phenology) and weights.
- FastAPI endpoints for health checks, species lists, and `/api/nowcast` queries
  filtered by species and minimum score.

## Project Structure
```
app/
  api/            # FastAPI routers
  models/         # Pydantic models for species and habitat cells
  services/       # Data loading and scoring services
  config.py       # Settings (data paths, defaults)
  main.py         # FastAPI application entrypoint
```
`data/` contains editable JSON sources:
- `species_profiles.json`: curated ecological parameters for each species.
- `sample_cells.json`: demo environmental measurements for a few grid cells.

### Species profile fields
Each entry in `species_profiles.json` includes:
- `soil_temperature_c`, `precipitation_mm_last_7d`, `soil_moisture_index`, `canopy_density_pct`, `elevation_m`: numeric ranges describing current-condition tolerances.
- `host_species`: list of `{scientific_name, common_name?, notes?}` objects for editable host associations.
- `soil_type_notes`, `fauna_partners`, `sources`, `last_updated`, `manual_notes`: narrative context for manual curation and traceability.
After editing the JSON, restart the API (or clear the loader cache) so changes take effect.

## Getting Started
1. **Install dependencies**
   ```bash
   python3 -m venv .venv
   source .venv/bin/activate
   pip install -r requirements.txt
   ```
2. **Run the API**
   ```bash
   uvicorn app.main:app --reload
   ```
3. **Test endpoints**
   - Health: `GET http://127.0.0.1:8000/api/health`
   - Species list: `GET http://127.0.0.1:8000/api/species`
   - Nowcast (default species): `GET http://127.0.0.1:8000/api/nowcast`
   - Nowcast filtered by species/min score:
     `GET http://127.0.0.1:8000/api/nowcast?species_id=morel&min_score=0.5`

## Customization
- Edit `data/species_profiles.json` to adjust ecological parameters or add new
  species (IDs must be unique; restart the app to reload cached data).
- Replace `data/sample_cells.json` with outputs from real ingestion pipelines
  (ensure the schema matches the `HabitatCell` model).
- Tweak scoring weights in `app/services/scoring.py` to emphasize different
  components (e.g., host species vs. precipitation).

## Next Steps
- Wire ingestion pipelines to refresh data at least daily and track freshness.
- Add authentication + subscription gating for production deployments on GCP
  (Cloud Run/GKE, Cloud SQL + PostGIS, Cloud Storage).
- Layer validation dashboards for manual spot checking and profile edits.
