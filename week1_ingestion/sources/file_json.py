from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from ..utils import Timer, exception_payload, write_json
from .base import SourceResult
from .extract import normalize_to_records


@dataclass(frozen=True)
class FileJsonSource:
    source_id: str
    out_dir: Path
    file_path: Path
    extract_cfg: dict[str, Any] | None
    debug: bool

    def run(self) -> SourceResult:
        t = Timer.start_new()
        raw_saved = False
        try:
            payload = json.loads(self.file_path.read_text(encoding="utf-8"))
            write_json(self.out_dir / "raw.json", payload)
            raw_saved = True
            records = normalize_to_records(payload, self.extract_cfg)
            return SourceResult(
                source_id=self.source_id,
                ok=True,
                records=records,
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


