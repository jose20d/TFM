from __future__ import annotations

import argparse
import json
from pathlib import Path

from .pipeline import run_pipeline


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    p = argparse.ArgumentParser(prog="week1_ingestion", add_help=True)
    sub = p.add_subparsers(dest="cmd", required=True)

    run = sub.add_parser("run", help="Run ingestion and write local JSON outputs.")
    run.add_argument("--config", required=True, help="Path to the JSON configuration file.")
    run.add_argument("--out", required=True, help="Output root directory (a run_... folder is created within).")
    run.add_argument(
        "--debug",
        action="store_true",
        help="Include tracebacks in error.json files (useful for troubleshooting).",
    )
    run.add_argument(
        "--allow-partial",
        action="store_true",
        help="Do not fail the whole run if a source fails (exit code 0).",
    )
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)

    if args.cmd == "run":
        config_path = Path(args.config).expanduser()
        out_root = Path(args.out).expanduser()
        cfg = json.loads(config_path.read_text(encoding="utf-8"))
        result = run_pipeline(
            config=cfg,
            out_root=out_root,
            debug=args.debug,
            allow_partial_override=args.allow_partial,
        )
        return int(result.exit_code)

    raise RuntimeError(f"Unsupported command: {args.cmd}")


