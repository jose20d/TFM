from __future__ import annotations

import hashlib
import json
import platform
import subprocess
import sys
import time
import traceback
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def utc_run_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%SZ")


def json_dumps(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, indent=2, sort_keys=True)


def sha256_json(obj: Any) -> str:
    raw = json.dumps(obj, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def write_json(path: Path, payload: Any) -> None:
    ensure_dir(path.parent)
    path.write_text(json_dumps(payload) + "\n", encoding="utf-8")


def try_get_git_commit(cwd: Path) -> str | None:
    try:
        out = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=str(cwd), stderr=subprocess.DEVNULL)
        return out.decode("utf-8").strip()
    except Exception:
        return None


@dataclass
class Timer:
    start: float

    @classmethod
    def start_new(cls) -> "Timer":
        return cls(start=time.time())

    def elapsed_ms(self) -> int:
        return int((time.time() - self.start) * 1000)


def environment_info() -> dict[str, Any]:
    return {
        "python_version": sys.version,
        "platform": platform.platform(),
        "executable": sys.executable,
    }


def exception_payload(exc: BaseException, debug: bool) -> dict[str, Any]:
    payload: dict[str, Any] = {"type": type(exc).__name__, "message": str(exc)}
    if debug:
        payload["traceback"] = traceback.format_exc()
    return payload


def dataclass_to_dict(obj: Any) -> Any:
    # helper (mantener manifest simple)
    if hasattr(obj, "__dataclass_fields__"):
        return asdict(obj)
    return obj


