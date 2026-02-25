#!/usr/bin/env python3
from __future__ import annotations

"""Main entrypoint: download, normalize, and launch Streamlit."""

import json
import subprocess
import sys
import zipfile
from pathlib import Path

from scripts import download_datasets


REPO_ROOT = Path(__file__).resolve().parent
CONFIG_PATH = REPO_ROOT / "configs" / "datasets.json"
RAW_DIR = REPO_ROOT / "data" / "raw"


def _read_config(path: Path) -> dict:
    """Read the datasets configuration JSON."""
    return json.loads(path.read_text(encoding="utf-8"))


def _find_dataset(datasets: list[dict], ds_id: str) -> dict | None:
    """Return a dataset entry by id."""
    for ds in datasets:
        if ds.get("id") == ds_id:
            return ds
    return None


def _extract_zip(zip_path: Path, dest_dir: Path) -> None:
    """Extract a zip file if the destination is empty."""
    if dest_dir.exists() and any(dest_dir.iterdir()):
        return
    dest_dir.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(zip_path, "r") as zf:
        zf.extractall(dest_dir)


def _run_script(path: Path, args: list[str]) -> int:
    """Run a Python script as a subprocess."""
    cmd = [sys.executable, str(path), *args]
    return subprocess.call(cmd)


def main() -> int:
    """Orchestrate the end-to-end pipeline and Streamlit UI."""
    if not CONFIG_PATH.exists():
        print(f"ERROR: missing config at {CONFIG_PATH}", file=sys.stderr)
        return 2

    cfg = _read_config(CONFIG_PATH)
    datasets = cfg.get("datasets") or []
    if not isinstance(datasets, list):
        print("ERROR: datasets list is missing in config", file=sys.stderr)
        return 2

    # Keep raw downloads intact for traceability and auditing.
    download_datasets.main(
        [
            "--config",
            str(CONFIG_PATH),
            "--out-dir",
            str(RAW_DIR),
        ]
    )

    mrds = _find_dataset(datasets, "mrds_csv")
    if mrds:
        zip_name = mrds.get("output_filename", "rdbms-tab-all.zip")
        zip_path = RAW_DIR / "mrds_csv" / str(zip_name)
        if zip_path.exists():
            extract_dir = RAW_DIR / "mrds_csv" / "extracted"
            _extract_zip(zip_path, extract_dir)
        else:
            print(f"[warn] MRDS zip not found at {zip_path}", file=sys.stderr)

    # Normalize directly into PostgreSQL (no JSON staging).
    load_script = REPO_ROOT / "scripts" / "load_to_db.py"
    try:
        if load_script.exists():
            exit_code = _run_script(load_script, [])
            if exit_code != 0:
                print("ERROR: database load failed. Streamlit will not start.", file=sys.stderr)
                return exit_code

        cmd = [
            sys.executable,
            "-m",
            "streamlit",
            "run",
            str(REPO_ROOT / "streamlit_app.py"),
        ]
        return subprocess.call(cmd)
    except KeyboardInterrupt:
        print("\n[info] Interrupted by user (Ctrl+C). Exiting cleanly.")
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
