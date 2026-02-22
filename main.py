#!/usr/bin/env python3
from __future__ import annotations

import json
import subprocess
import sys
import zipfile
from pathlib import Path

from scripts import download_datasets


REPO_ROOT = Path(__file__).resolve().parent
CONFIG_PATH = REPO_ROOT / "configs" / "datasets.json"
RAW_DIR = REPO_ROOT / "data" / "raw"
REFERENCES_DIR = REPO_ROOT / "references"
NORMALIZED_DIR = REPO_ROOT / "data" / "normalized"


def _read_config(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _find_dataset(datasets: list[dict], ds_id: str) -> dict | None:
    for ds in datasets:
        if ds.get("id") == ds_id:
            return ds
    return None


def _extract_zip(zip_path: Path, dest_dir: Path) -> None:
    if dest_dir.exists() and any(dest_dir.iterdir()):
        return
    dest_dir.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(zip_path, "r") as zf:
        zf.extractall(dest_dir)


def _copy_file(src: Path, dest: Path) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_bytes(src.read_bytes())


def _prepare_mrds_files(extract_dir: Path) -> None:
    required = [
        "MRDS.csv",
        "Location.csv",
        "Rocks.csv",
        "Commodity.csv",
        "Materials.csv",
        "Ownership.csv",
        "Physiography.csv",
        "Ages.csv",
    ]
    for name in required:
        src = extract_dir / name
        if src.exists():
            _copy_file(src, REFERENCES_DIR / name)


def _run_script(path: Path, args: list[str]) -> int:
    cmd = [sys.executable, str(path), *args]
    return subprocess.call(cmd)


def main() -> int:
    if not CONFIG_PATH.exists():
        print(f"ERROR: missing config at {CONFIG_PATH}", file=sys.stderr)
        return 2

    cfg = _read_config(CONFIG_PATH)
    datasets = cfg.get("datasets") or []
    if not isinstance(datasets, list):
        print("ERROR: datasets list is missing in config", file=sys.stderr)
        return 2

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
            _prepare_mrds_files(extract_dir)
        else:
            print(f"[warn] MRDS zip not found at {zip_path}", file=sys.stderr)

    normalize_script = REPO_ROOT / "scripts" / "normalize_indicators.py"
    if normalize_script.exists():
        _run_script(normalize_script, [])

    mrds_map_script = REPO_ROOT / "scripts" / "build_mrds_country_map.py"
    if mrds_map_script.exists():
        _run_script(mrds_map_script, [])

    cmd = [sys.executable, "-m", "streamlit", "run", str(REPO_ROOT / "streamlit_app.py")]
    return subprocess.call(cmd)


if __name__ == "__main__":
    raise SystemExit(main())
