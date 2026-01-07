from __future__ import annotations

import csv
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from ..utils import Timer, exception_payload, write_json
from .base import SourceResult


@dataclass(frozen=True)
class FileCsvSource:
    source_id: str
    out_dir: Path
    file_path: Path
    encoding: str
    delimiter: str
    debug: bool

    def run(self) -> SourceResult:
        t = Timer.start_new()
        raw_saved = False
        try:
            rows: list[dict[str, Any]] = []
            with self.file_path.open("r", encoding=self.encoding, newline="") as f:
                reader = csv.DictReader(f, delimiter=self.delimiter)
                for r in reader:
                    rows.append(dict(r))

            # Para ficheros, "raw" = filas tal cual
            write_json(self.out_dir / "raw.json", rows)
            raw_saved = True

            return SourceResult(
                source_id=self.source_id,
                ok=True,
                records=rows,
                raw_saved=raw_saved,
                elapsed_ms=t.elapsed_ms(),
                error=None,
            )
        except Exception as exc:
            err = exception_payload(exc, debug=self.debug)
            write_json(self.out_dir / "error.json", err)
            return SourceResult(
                source_id=self.source_id,
                ok=False,
                records=None,
                raw_saved=raw_saved,
                elapsed_ms=t.elapsed_ms(),
                error=err,
            )


