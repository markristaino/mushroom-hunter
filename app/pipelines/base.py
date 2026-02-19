"""Common helpers for ingestion pipelines."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import json
from pathlib import Path


@dataclass
class IngestionResult:
    source_id: str
    output_path: Path
    rows_written: int
    last_ingested: datetime

    def as_dict(self) -> dict:
        return {
            "source_id": self.source_id,
            "output_path": str(self.output_path),
            "rows_written": self.rows_written,
            "last_ingested": self.last_ingested.isoformat(),
        }


def write_collection(collection, output_path: Path) -> int:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as handle:
        handle.write(collection.model_dump_json(indent=2))
    return len(collection.cells)


def update_freshness(result: IngestionResult, freshness_path: Path) -> None:
    freshness_path.parent.mkdir(parents=True, exist_ok=True)
    entries = {}
    if freshness_path.exists():
        with freshness_path.open("r", encoding="utf-8") as handle:
            entries = json.load(handle)
    entries[result.source_id] = result.as_dict()
    with freshness_path.open("w", encoding="utf-8") as handle:
        json.dump(entries, handle, indent=2)
