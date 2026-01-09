#!/usr/bin/env python3
from __future__ import annotations

import argparse
import shutil
import sys
from pathlib import Path


def repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def remove_dir(p: Path) -> bool:
    if not p.exists():
        return False
    if not p.is_dir():
        raise RuntimeError(f"Refusing to remove non-directory path: {p}")
    shutil.rmtree(p)
    return True


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="clean_data", add_help=True)
    p.add_argument(
        "--yes",
        action="store_true",
        help="Actually delete folders. Without --yes, only prints what would be removed.",
    )
    args = p.parse_args(argv)

    root = repo_root()
    targets = [
        root / "data" / "demo",
        root / "data" / "out",
        root / "scripts" / "data",
        root / "output",
    ]

    if not args.yes:
        print("Dry run (no deletion). These folders would be removed if they exist:")
        for t in targets:
            print(f"- {t}")
        print("\nRun with --yes to delete them.")
        return 0

    removed_any = False
    for t in targets:
        try:
            removed = remove_dir(t)
            removed_any = removed_any or removed
        except Exception as exc:
            print(f"ERROR: failed to remove {t}: {exc}", file=sys.stderr)
            return 2

    print("OK: data folders removed." if removed_any else "OK: nothing to remove.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


