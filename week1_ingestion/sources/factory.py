from __future__ import annotations

from pathlib import Path
from typing import Any

from ..errors import SourceError
from .file_csv import FileCsvSource
from .file_json import FileJsonSource
from .http_json import HttpJsonSource


def build_source(
    source_cfg: dict[str, Any],
    *,
    base_dir: Path,
    out_dir: Path,
    timeout_seconds: int,
    max_retries: int,
    debug: bool,
) -> Any:
    source_id = source_cfg.get("id")
    source_type = source_cfg.get("type")
    if not source_id or not source_type:
        raise SourceError(source_id or "unknown", "Each source requires both 'id' and 'type'")

    extract_cfg = source_cfg.get("extract")

    if source_type == "http_json":
        req = source_cfg.get("request") or {}
        if "url" not in req:
            raise SourceError(source_id, "http_json requires request.url")
        return HttpJsonSource(
            source_id=source_id,
            out_dir=out_dir,
            request_cfg=req,
            extract_cfg=extract_cfg,
            timeout_seconds=timeout_seconds,
            max_retries=max_retries,
            debug=debug,
        )

    if source_type == "file_json":
        fcfg = source_cfg.get("file") or {}
        path = fcfg.get("path")
        if not path:
            raise SourceError(source_id, "file_json requires file.path")
        file_path = (base_dir / path).resolve() if not Path(path).is_absolute() else Path(path)
        return FileJsonSource(
            source_id=source_id,
            out_dir=out_dir,
            file_path=file_path,
            extract_cfg=extract_cfg,
            debug=debug,
        )

    if source_type == "file_csv":
        fcfg = source_cfg.get("file") or {}
        path = fcfg.get("path")
        if not path:
            raise SourceError(source_id, "file_csv requires file.path")
        encoding = fcfg.get("encoding") or "utf-8"
        delimiter = fcfg.get("delimiter") or ","
        file_path = (base_dir / path).resolve() if not Path(path).is_absolute() else Path(path)
        return FileCsvSource(
            source_id=source_id,
            out_dir=out_dir,
            file_path=file_path,
            encoding=encoding,
            delimiter=delimiter,
            debug=debug,
        )

    raise SourceError(source_id, f"Unsupported type: {source_type}")


