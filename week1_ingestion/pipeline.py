from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .utils import (
    Timer,
    environment_info,
    ensure_dir,
    exception_payload,
    json_dumps,
    sha256_json,
    try_get_git_commit,
    utc_now_iso,
    utc_run_id,
    write_json,
)
from .sources.factory import build_source


@dataclass(frozen=True)
class PipelineResult:
    run_dir: Path
    ok: bool
    exit_code: int


def run_pipeline(
    *,
    config: dict[str, Any],
    out_root: Path,
    debug: bool,
    allow_partial_override: bool,
) -> PipelineResult:
    t = Timer.start_new()
    base_dir = Path.cwd()

    run_cfg = config.get("run") or {}
    allow_partial = bool(run_cfg.get("allow_partial", False)) or bool(allow_partial_override)
    timeout_seconds = int(run_cfg.get("timeout_seconds", 20))
    max_retries = int(run_cfg.get("max_retries", 3))

    run_dir = out_root / f"run_{utc_run_id()}"
    sources_dir = run_dir / "sources"
    ensure_dir(sources_dir)

    manifest: dict[str, Any] = {
        "started_at": utc_now_iso(),
        "ended_at": None,
        "elapsed_ms": None,
        "ok": None,
        "exit_code": None,
        "run_dir": str(run_dir),
        "config_sha256": sha256_json(config),
        "git_commit": try_get_git_commit(base_dir),
        "environment": environment_info(),
        "settings": {
            "allow_partial": allow_partial,
            "timeout_seconds": timeout_seconds,
            "max_retries": max_retries,
            "debug": debug,
        },
        "sources": [],
        "summary": {"sources_ok": 0, "sources_failed": 0, "records_total": 0},
    }

    all_ok = True
    sources_cfg = config.get("sources") or []
    if not isinstance(sources_cfg, list) or len(sources_cfg) == 0:
        err = {"type": "ValueError", "message": "config.sources must be a non-empty list"}
        write_json(run_dir / "manifest.json", {**manifest, "ok": False, "exit_code": 2, "fatal_error": err})
        return PipelineResult(run_dir=run_dir, ok=False, exit_code=2)

    for source_cfg in sources_cfg:
        source_id = str(source_cfg.get("id", "unknown"))
        out_dir = sources_dir / source_id
        ensure_dir(out_dir)

        src_timer = Timer.start_new()
        src_entry: dict[str, Any] = {
            "id": source_id,
            "type": source_cfg.get("type"),
            "ok": False,
            "elapsed_ms": None,
            "records": 0,
            "paths": {
                "data_json": str(out_dir / "data.json"),
                "raw_json": str(out_dir / "raw.json"),
                "error_json": str(out_dir / "error.json"),
            },
        }

        try:
            src = build_source(
                source_cfg,
                base_dir=base_dir,
                out_dir=out_dir,
                timeout_seconds=timeout_seconds,
                max_retries=max_retries,
                debug=debug,
            )
            result = src.run()
            src_entry["ok"] = bool(result.ok)
            src_entry["elapsed_ms"] = int(result.elapsed_ms)

            if result.ok and result.records is not None:
                write_json(out_dir / "data.json", result.records)
                src_entry["records"] = len(result.records)
                manifest["summary"]["records_total"] += len(result.records)
                manifest["summary"]["sources_ok"] += 1
            else:
                manifest["summary"]["sources_failed"] += 1
                all_ok = False
        except Exception as exc:
            # fallo al construir/ejecutar
            err = exception_payload(exc, debug=debug)
            write_json(out_dir / "error.json", err)
            src_entry["ok"] = False
            src_entry["elapsed_ms"] = src_timer.elapsed_ms()
            src_entry["records"] = 0
            manifest["summary"]["sources_failed"] += 1
            all_ok = False

        manifest["sources"].append(src_entry)

    exit_code = 0 if (all_ok or allow_partial) else 1
    manifest["ok"] = bool(all_ok)
    manifest["exit_code"] = int(exit_code)
    manifest["elapsed_ms"] = t.elapsed_ms()
    manifest["ended_at"] = utc_now_iso()

    write_json(run_dir / "manifest.json", manifest)
    return PipelineResult(run_dir=run_dir, ok=all_ok, exit_code=exit_code)


