"""Common helpers for ingestion pipelines."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
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
