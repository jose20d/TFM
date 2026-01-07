from __future__ import annotations

import json
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import requests

from ..utils import Timer, exception_payload, write_json
from .base import SourceResult
from .extract import normalize_to_records


@dataclass(frozen=True)
class HttpJsonSource:
    source_id: str
    out_dir: Path
    request_cfg: dict[str, Any]
    extract_cfg: dict[str, Any] | None
    timeout_seconds: int
    max_retries: int
    debug: bool

    def run(self) -> SourceResult:
        t = Timer.start_new()
        raw_saved = False
        try:
            payload = self._fetch_with_retries()
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

    def _fetch_with_retries(self) -> Any:
        method = (self.request_cfg.get("method") or "GET").upper()
        url = self.request_cfg["url"]
        params = self.request_cfg.get("params")
        headers = self.request_cfg.get("headers")

        last_exc: Exception | None = None
        for attempt in range(1, self.max_retries + 1):
            try:
                resp = requests.request(
                    method=method,
                    url=url,
                    params=params,
                    headers=headers,
                    timeout=self.timeout_seconds,
                )
                resp.raise_for_status()
                # Parse JSON with a clear message if it fails.
                try:
                    return resp.json()
                except json.JSONDecodeError as je:
                    raise ValueError(f"Response is not valid JSON from {url}") from je
            except Exception as exc:
                last_exc = exc if isinstance(exc, Exception) else Exception(str(exc))
                if attempt < self.max_retries:
                    time.sleep(min(2 ** (attempt - 1), 8))
                    continue
                break

        assert last_exc is not None
        raise last_exc


