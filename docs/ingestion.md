---
description: Daily environmental data ingestion
---

# Data Ingestion Architecture

## Objectives
1. Refresh weather, soil, canopy, and host inputs at least once every 24 hours for the 30 m habitat grid.
2. Preserve provenance for every field so we can audit species scores back to the upstream feed.
3. Expose freshness metadata to the API/UI so stale data can be flagged or withheld automatically.
4. Keep the design “GCP-first,” but runnable locally for development and QA.

## Source catalog
| Feed | Provider | Delivery | Cadence | Key fields | Notes |
|------|----------|----------|---------|------------|-------|
| Historical precip + soil temp | NOAA Climate Data Online (CDO) / nClimGrid | REST (token) + daily grib download | Daily | 7‑day precip sums, soil temp @5 cm, station metadata | Use county/station aggregation for quick bootstrap; move to gridded raster once quotas allow. |
| Short-term forecast | Open-Meteo API | REST (no key) | Hourly | Temp, precip, dewpoint | Blend with NOAA to backfill missing hours; cached for 24 h. |
| Soil moisture + temp | USDA NRCS SCAN + NASA SMAP L4 | CSV/API + HDF5 | Daily | Volumetric moisture, surface temp | SCAN stations for calibration; SMAP raster resampled to 30 m grid. |
| Canopy density | USGS NLCD 2019 | GeoTIFF (30 m) | Annual | Percent tree canopy | Preload to Cloud Storage; reproject to project CRS once. |
| Host species probability | USFS FIA plots + LANDFIRE EVT | Shapefiles / CSV | Annual | Dominant species, basal area | Build per-host rasters (Douglas-fir, hemlock, etc.) to feed `host_species_present`. |
| Disturbance layers | MTBS burn severity, NAIP logging polygons | GeoTIFF / shapefile | Event-driven | Burn year, severity | Needed for burn morels and canopy changes. |

## Storage layout
```
./data/
  raw/<source>/<YYYY>/<MM>/<DD>/...  # exact download from provider
  staging/<date>/<source>.parquet     # normalized but not yet spatially joined
  grid/                               # DuckDB/Parquet partitions keyed by cell_id
metadata/
  freshness.sqlite or Cloud SQL table
```
- **Prod (GCP)**: mirror the same layout in `gs://mushroom-hunter-raw` (raw) and `gs://mushroom-hunter-processed` (processed). Expose the processed layer to BigQuery or PostGIS for API reads.

## Habitat grid schema
Each 30 m cell stores:
```
cell_id TEXT PRIMARY KEY
latitude / longitude (centroid)
geom (PostGIS geography)
soil_temperature_c REAL
precipitation_mm_last_7d REAL
soil_moisture_index REAL
canopy_density_pct REAL
host_species_present TEXT[]
source_attribution JSONB   -- {"soil_temperature": {"source": "NOAA_CDO", "timestamp": "..."}, ...}
last_observation TIMESTAMP WITH TIME ZONE
```
The scoring service continues to consume `HabitatCell`, but now the backing store is the processed table rather than `data/sample_cells.json`.

## Pipeline stages
For each source, the orchestrator executes:
1. **Fetch** – download to `data/raw/...` (or Cloud Storage). Include checksum + size in metadata table.
2. **Validate** – schema checks (required columns, CRS). Reject out-of-window timestamps.
3. **Transform** – reproject/aggregate to 30 m grid, compute rolling windows (e.g., precip over last 7 days).
4. **Merge** – join all metrics into the grid table, keeping per-field provenance.
5. **Publish** – update freshness table, emit event (Pub/Sub or webhook) so API pods know to invalidate caches.

## Orchestration & scheduling
- **Local dev**: invoke `python -m pipelines.fetch_noaa` etc. via `make ingest`. Runs synchronously and writes to local `data/`.
- **Prod**: Cloud Scheduler → Cloud Run job / Cloud Functions -> Prefect/Temporal worker. Each feed runs in its own DAG so a stuck source does not block others. Shared retry policy (exponential backoff, max 6 hours) and alert hook (PagerDuty/Slack).

## Freshness tracking
Create a `data_freshness` table with:
```
source_id TEXT PRIMARY KEY
expected_interval_minutes INT
last_ingested TIMESTAMP WITH TIME ZONE
status ENUM('ok','warning','stale','failed')
notes TEXT
```
Logic:
- “warning” when `now - last_ingested > 1.25 * expected_interval`.
- “stale” after 2 intervals.
Expose via `/api/health` and include in `/api/nowcast` response header `X-Data-Freshness: soil=ok;precip=warning` so the UI can surface alerts.

## Configuration & secrets
- Extend `Settings` with provider endpoints, dataset IDs, and `SecretStr` API keys (`noaa_token`, `open_meteo_base_url`, etc.).
- `.env.example` documents required values; production secrets live in Secret Manager.
- Allow `MUSHROOM_USE_LOCAL_CACHE=1` to keep working offline (pipelines read from `data/raw/cache`).

## Failure & alerting
- Each pipeline emits structured logs (JSON) with `source_id`, `stage`, `duration_ms`, `row_count`.
- Failures send a Slack message containing actionable steps and direct link to the Cloud Logging entry.
- Provide a CLI command `python -m pipelines.replay --source noaa --date 2026-02-15` to backfill missed days.

## Integration points
- `app/services/data_loader.py` will be updated later to read from the processed grid (DuckDB/PostGIS) rather than static JSON.
- The scoring engine can consume the new provenance metadata to explain which source/backfill fueled each component.
- README “Next Steps” now points to this document for onboarding.
