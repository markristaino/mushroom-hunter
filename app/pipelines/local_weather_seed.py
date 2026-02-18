"""Local placeholder pipeline to simulate weather + soil metrics."""
from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path

from app.config import get_settings
from app.models import HabitatCellCollection
from app.pipelines.base import IngestionResult


def load_seed_cells(seed_path: Path) -> HabitatCellCollection:
    with seed_path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    return HabitatCellCollection.model_validate(payload)


def write_processed(collection: HabitatCellCollection, output_path: Path) -> int:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as handle:
        handle.write(collection.model_dump_json(indent=2))
    return len(collection.cells)


def record_freshness(result: IngestionResult, freshness_path: Path) -> None:
    freshness_path.parent.mkdir(parents=True, exist_ok=True)
    entries = {}
    if freshness_path.exists():
        with freshness_path.open("r", encoding="utf-8") as f:
            entries = json.load(f)
    entries[result.source_id] = result.as_dict()
    with freshness_path.open("w", encoding="utf-8") as f:
        json.dump(entries, f, indent=2)


def run(seed_override: Path | None = None) -> IngestionResult:
    settings = get_settings()
    seed_path = seed_override or (settings.seed_data_dir / "local_weather_seed.json")
    collection = load_seed_cells(seed_path)
    rows = write_processed(collection, settings.processed_grid_path)
    result = IngestionResult(
        source_id="local_weather_seed",
        output_path=settings.processed_grid_path,
        rows_written=rows,
        last_ingested=datetime.utcnow(),
    )
    record_freshness(result, settings.freshness_path)
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description="Load local seed weather data into processed cache")
    parser.add_argument("--seed", type=Path, default=None, help="Optional override path for the seed JSON")
    args = parser.parse_args()
    result = run(seed_override=args.seed)
    print(f"Wrote {result.rows_written} habitat cells to {result.output_path}")


if __name__ == "__main__":
    main()
