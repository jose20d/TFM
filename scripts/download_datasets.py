#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path
from typing import Any

try:
    import requests  # type: ignore
except ModuleNotFoundError as exc:  # pragma: no cover
    requests = None  # type: ignore
    _IMPORT_ERROR = str(exc)
else:
    _IMPORT_ERROR = None


def read_config(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def safe_filename(name: str) -> str:
    return name.replace("/", "_").replace("\\", "_")


def download_file(url: str, dest: Path, timeout: int, retries: int) -> None:
    if requests is None:
        print(
            f"ERROR: requests is required. {_IMPORT_ERROR}",
            file=sys.stderr,
        )
        raise SystemExit(2)

    backoff = 2
    last_error: str | None = None
    for attempt in range(1, retries + 1):
        try:
            with requests.get(url, stream=True, timeout=timeout) as resp:
                resp.raise_for_status()
                dest.parent.mkdir(parents=True, exist_ok=True)
                with dest.open("wb") as f:
                    for chunk in resp.iter_content(chunk_size=1024 * 1024):
                        if chunk:
                            f.write(chunk)
            return
        except Exception as exc:
            last_error = str(exc)
            if attempt < retries:
                time.sleep(backoff)
                backoff *= 2
                continue
            raise RuntimeError(last_error or "Unknown error") from exc


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(
        prog="download_datasets",
        description="Download raw datasets for traceability (no processing).",
    )
    p.add_argument(
        "--config",
        default="configs/datasets.json",
        help="Path to datasets config JSON.",
    )
    p.add_argument(
        "--out-dir",
        default="data/raw",
        help="Output directory for raw downloads.",
    )
    p.add_argument(
        "--ids",
        help="Comma-separated dataset ids to download (optional).",
    )
    p.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite files if they already exist.",
    )
    p.add_argument(
        "--timeout",
        type=int,
        default=60,
        help="HTTP timeout in seconds.",
    )
    p.add_argument(
        "--retries",
        type=int,
        default=3,
        help="Number of retries for failed downloads.",
    )
    args = p.parse_args(argv)

    config_path = Path(args.config).expanduser()
    if not config_path.exists():
        print(f"ERROR: config not found at {config_path}", file=sys.stderr)
        return 2

    cfg = read_config(config_path)
    datasets = cfg.get("datasets")
    if not isinstance(datasets, list):
        print("ERROR: config.datasets must be a list", file=sys.stderr)
        return 2

    wanted = None
    if args.ids:
        wanted = {x.strip() for x in args.ids.split(",") if x.strip()}

    out_root = Path(args.out_dir).expanduser()
    downloaded = 0

    for ds in datasets:
        if not isinstance(ds, dict):
            continue
        ds_id = str(ds.get("id", "")).strip()
        if not ds_id:
            continue
        if wanted and ds_id not in wanted:
            continue

        url = ds.get("url")
        filename = ds.get("output_filename") or f"{ds_id}.bin"
        if not isinstance(url, str) or not url:
            print(f"[skip] {ds_id}: missing url", file=sys.stderr)
            continue

        dest_dir = out_root / ds_id
        dest = dest_dir / safe_filename(str(filename))
        if dest.exists() and not args.overwrite:
            print(f"[skip] {ds_id}: already exists -> {dest}")
            continue

        print(f"[download] {ds_id} -> {dest}")
        try:
            download_file(url, dest, timeout=args.timeout, retries=args.retries)
        except Exception as exc:
            print(f"[error] {ds_id}: {exc}", file=sys.stderr)
            continue

        downloaded += 1
        print(f"[ok] {ds_id}: saved {dest}")

    if downloaded == 0:
        print("No datasets downloaded.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
