from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class SourceError(Exception):
    source_id: str
    message: str

    def __str__(self) -> str:  # pragma: no cover
        return f"[{self.source_id}] {self.message}"


