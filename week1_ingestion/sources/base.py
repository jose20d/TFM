from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol


@dataclass(frozen=True)
class SourceResult:
    source_id: str
    ok: bool
    records: list[dict[str, Any]] | None
    raw_saved: bool
    elapsed_ms: int
    error: dict[str, Any] | None


class Source(Protocol):
    def run(self) -> SourceResult: ...


