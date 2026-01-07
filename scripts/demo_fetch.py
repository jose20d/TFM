#!/usr/bin/env python3
from __future__ import annotations

import argparse
import io
import json
import os
import random
import re
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable
from xml.etree import ElementTree as ET

import pandas as pd
import requests


LOGIN_HTML_HINTS = re.compile(r"(login|sign in|access denied)", re.IGNORECASE)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def write_text(path: Path, text: str) -> None:
    ensure_dir(path.parent)
    path.write_text(text, encoding="utf-8")


def write_json(path: Path, payload: Any) -> None:
    write_text(path, json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n")


def write_jsonl(path: Path, records: list[dict[str, Any]]) -> None:
    ensure_dir(path.parent)
    with path.open("w", encoding="utf-8") as f:
        for r in records:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")


def payload_snippet_from_bytes(b: bytes, limit: int = 2000) -> str:
    try:
        s = b.decode("utf-8", errors="replace")
    except Exception:
        s = repr(b[:limit])
    return s[:limit]


def looks_like_html(content_type: str | None, body: bytes) -> bool:
    ct = (content_type or "").lower()
    if "text/html" in ct or "application/xhtml" in ct:
        return True
    head = body.lstrip()[:200].lower()
    return head.startswith(b"<!doctype html") or head.startswith(b"<html") or b"<html" in head


def load_sources(sources_path: Path) -> list[dict[str, Any]]:
    if not sources_path.exists():
        raise FetchError(code="MISSING_SOURCES_FILE", message=f"Sources file not found: {sources_path}")
    try:
        payload = json.loads(sources_path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise FetchError(code="SOURCES_FILE_INVALID", message=f"Failed to parse sources file: {exc}")

    if not isinstance(payload, list) or not payload:
        raise FetchError(code="SOURCES_FILE_INVALID", message="Sources file must be a non-empty JSON list.")

    required_keys = {
        "source_name",
        "source_type",
        "base_url",
        "endpoint",
        "expected_format",
        "auth_required",
        "env_vars",
    }
    for i, src in enumerate(payload):
        if not isinstance(src, dict):
            raise FetchError(code="SOURCES_FILE_INVALID", message=f"Source at index {i} is not an object.")
        missing = sorted(required_keys.difference(src.keys()))
        if missing:
            raise FetchError(code="SOURCES_FILE_INVALID", message=f"Source '{src.get('source_name')}' missing keys: {missing}")
        if not isinstance(src.get("env_vars"), list):
            raise FetchError(code="SOURCES_FILE_INVALID", message=f"Source '{src.get('source_name')}' env_vars must be a list.")
    return payload


@dataclass(frozen=True)
class FetchError(Exception):
    code: str
    message: str
    http_status: int | None = None
    content_type: str | None = None
    auth_required_detected: bool = False
    debug_snippet: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "code": self.code,
            "message": self.message,
            "http_status": self.http_status,
            "content_type": self.content_type,
            "auth_required_detected": self.auth_required_detected,
        }


def request_with_retries(
    session: requests.Session,
    method: str,
    url: str,
    *,
    params: dict[str, Any] | None = None,
    headers: dict[str, str] | None = None,
    timeout_seconds: int = 30,
    max_retries: int = 4,
) -> tuple[requests.Response, int]:
    last_exc: Exception | None = None
    retries_used = 0

    for attempt in range(0, max_retries + 1):
        try:
            resp = session.request(
                method=method,
                url=url,
                params=params,
                headers=headers,
                timeout=timeout_seconds,
            )

            if resp.status_code in (429,) or 500 <= resp.status_code <= 599:
                if attempt < max_retries:
                    retries_used += 1
                    backoff = min(2**attempt, 16) + random.random()
                    time.sleep(backoff)
                    continue

            return resp, retries_used
        except (requests.Timeout, requests.ConnectionError) as exc:
            last_exc = exc
            if attempt < max_retries:
                retries_used += 1
                backoff = min(2**attempt, 16) + random.random()
                time.sleep(backoff)
                continue
            break

    raise FetchError(code="NETWORK_ERROR", message=str(last_exc or "network error"))


def detect_auth_required(resp: requests.Response, body: bytes) -> bool:
    if resp.status_code in (401, 403):
        return True
    snippet = payload_snippet_from_bytes(body, limit=4000)
    return bool(LOGIN_HTML_HINTS.search(snippet))


def handler_worldbank_wgi(session: requests.Session, limit: int, source: dict[str, Any]) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    url = source["base_url"].rstrip("/") + source["endpoint"]
    resp, retries_used = request_with_retries(session, "GET", url, timeout_seconds=30, max_retries=4)
    body = resp.content
    ct = resp.headers.get("Content-Type")

    if detect_auth_required(resp, body):
        raise FetchError(
            code="AUTH_REQUIRED",
            message="Authentication appears to be required.",
            http_status=resp.status_code,
            content_type=ct,
            auth_required_detected=True,
            debug_snippet=payload_snippet_from_bytes(body),
        )

    if looks_like_html(ct, body):
        raise FetchError(
            code="UNEXPECTED_CONTENT_TYPE",
            message="Received HTML when JSON was expected.",
            http_status=resp.status_code,
            content_type=ct,
            debug_snippet=payload_snippet_from_bytes(body),
        )

    if resp.status_code >= 400:
        raise FetchError(
            code="HTTP_ERROR",
            message=f"HTTP error {resp.status_code}",
            http_status=resp.status_code,
            content_type=ct,
            debug_snippet=payload_snippet_from_bytes(body),
        )

    try:
        payload = resp.json()
    except Exception:
        raise FetchError(
            code="PARSE_ERROR",
            message="Failed to parse JSON response.",
            http_status=resp.status_code,
            content_type=ct,
            debug_snippet=payload_snippet_from_bytes(body),
        )

    # World Bank v2 typical shape: [meta, data]
    if not isinstance(payload, list) or len(payload) < 2 or not isinstance(payload[1], list):
        raise FetchError(
            code="SCHEMA_ERROR",
            message="Unexpected JSON schema from World Bank API.",
            http_status=resp.status_code,
            content_type=ct,
            debug_snippet=json.dumps(payload, ensure_ascii=False)[:2000],
        )

    df = pd.DataFrame(payload[1])
    if df.empty:
        records: list[dict[str, Any]] = []
    else:
        records = df.head(limit).to_dict(orient="records")

    meta = {
        "request_url": url,
        "http_status": resp.status_code,
        "content_type": ct,
        "retries_used": retries_used,
        "records_available": int(len(df)),
    }
    return records, meta


def handler_usgs_mrds(session: requests.Session, limit: int, source: dict[str, Any]) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    env_name = "MRDS_CSV_URL"
    url = os.environ.get(env_name, "").strip()
    if not url:
        raise FetchError(
            code="MISSING_ENV_VAR",
            message=f"Missing required environment variable: {env_name}",
        )

    resp, retries_used = request_with_retries(session, "GET", url, timeout_seconds=60, max_retries=4)
    body = resp.content
    ct = resp.headers.get("Content-Type")

    if detect_auth_required(resp, body):
        raise FetchError(
            code="AUTH_REQUIRED",
            message="Authentication appears to be required.",
            http_status=resp.status_code,
            content_type=ct,
            auth_required_detected=True,
            debug_snippet=payload_snippet_from_bytes(body),
        )

    if looks_like_html(ct, body):
        raise FetchError(
            code="UNEXPECTED_CONTENT_TYPE",
            message="Received HTML when CSV was expected.",
            http_status=resp.status_code,
            content_type=ct,
            debug_snippet=payload_snippet_from_bytes(body),
        )

    if resp.status_code >= 400:
        raise FetchError(
            code="HTTP_ERROR",
            message=f"HTTP error {resp.status_code}",
            http_status=resp.status_code,
            content_type=ct,
            debug_snippet=payload_snippet_from_bytes(body),
        )

    # CSV parsing (best-effort, robust to encoding)
    try:
        text = body.decode("utf-8")
    except UnicodeDecodeError:
        text = body.decode("latin-1", errors="replace")

    try:
        df = pd.read_csv(io.StringIO(text))
    except Exception:
        raise FetchError(
            code="PARSE_ERROR",
            message="Failed to parse CSV payload.",
            http_status=resp.status_code,
            content_type=ct,
            debug_snippet=text[:2000],
        )

    records = df.head(limit).to_dict(orient="records")
    meta = {
        "request_url": url,
        "http_status": resp.status_code,
        "content_type": ct,
        "retries_used": retries_used,
        "records_available": int(len(df)),
    }
    return records, meta


def handler_onegeology_wms(session: requests.Session, limit: int, source: dict[str, Any]) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    env_name = "ONEGEOLOGY_WMS_URL"
    url = os.environ.get(env_name, "").strip()
    if not url:
        raise FetchError(
            code="MISSING_ENV_VAR",
            message=f"Missing required environment variable: {env_name}",
        )

    resp, retries_used = request_with_retries(session, "GET", url, timeout_seconds=60, max_retries=4)
    body = resp.content
    ct = resp.headers.get("Content-Type")

    if detect_auth_required(resp, body):
        raise FetchError(
            code="AUTH_REQUIRED",
            message="Authentication appears to be required.",
            http_status=resp.status_code,
            content_type=ct,
            auth_required_detected=True,
            debug_snippet=payload_snippet_from_bytes(body),
        )

    if looks_like_html(ct, body):
        raise FetchError(
            code="UNEXPECTED_CONTENT_TYPE",
            message="Received HTML when XML was expected.",
            http_status=resp.status_code,
            content_type=ct,
            debug_snippet=payload_snippet_from_bytes(body),
        )

    if resp.status_code >= 400:
        raise FetchError(
            code="HTTP_ERROR",
            message=f"HTTP error {resp.status_code}",
            http_status=resp.status_code,
            content_type=ct,
            debug_snippet=payload_snippet_from_bytes(body),
        )

    try:
        root = ET.fromstring(body)
    except Exception:
        raise FetchError(
            code="PARSE_ERROR",
            message="Failed to parse XML payload.",
            http_status=resp.status_code,
            content_type=ct,
            debug_snippet=payload_snippet_from_bytes(body),
        )

    # Extract top N layer names/titles from WMS GetCapabilities-like documents.
    # Namespaces vary; we handle them via wildcard searches.
    records: list[dict[str, Any]] = []
    for layer in root.findall(".//{*}Layer"):
        name_el = layer.find("{*}Name")
        title_el = layer.find("{*}Title")
        name = (name_el.text or "").strip() if name_el is not None else ""
        title = (title_el.text or "").strip() if title_el is not None else ""
        if not name and not title:
            continue
        records.append({"layer_name": name or None, "layer_title": title or None})
        if len(records) >= limit:
            break

    meta = {
        "request_url": url,
        "http_status": resp.status_code,
        "content_type": ct,
        "retries_used": retries_used,
        "layers_found": len(records),
    }
    return records, meta


HANDLERS: dict[str, Callable[[requests.Session, int, dict[str, Any]], tuple[list[dict[str, Any]], dict[str, Any]]]] = {
    "worldbank_wgi": handler_worldbank_wgi,
    "usgs_mrds": handler_usgs_mrds,
    "onegeology_wms": handler_onegeology_wms,
}


def parse_sources_arg(value: str) -> list[str]:
    v = value.strip()
    if v.lower() == "all":
        return ["all"]
    parts = [p.strip() for p in v.split(",") if p.strip()]
    return parts


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="demo_fetch", add_help=True)
    p.add_argument("--limit", type=int, default=100, help="Max records per source (default: 100).")
    p.add_argument(
        "--sources",
        type=str,
        default="all",
        help="Sources to run: all or comma-separated list (e.g. worldbank_wgi,usgs_mrds).",
    )
    p.add_argument(
        "--sources-file",
        type=str,
        default="configs/sources.json",
        help="Path to the approved sources definition JSON file.",
    )
    args = p.parse_args(argv)

    limit = int(args.limit)
    sources_file = Path(args.sources_file)

    report: dict[str, Any] = {"started_at": utc_now_iso(), "ended_at": None, "limit": limit, "sources": {}}

    try:
        sources = load_sources(sources_file)
    except FetchError as fe:
        # If sources cannot be loaded, still produce a report for reproducibility.
        report["sources"] = {}
        report["ended_at"] = utc_now_iso()
        report["error"] = fe.to_dict()
        write_json(Path("data/demo") / "demo_report.json", report)
        return 0

    wanted_list = parse_sources_arg(args.sources)
    wanted = {s["source_name"] for s in sources} if wanted_list == ["all"] else set(wanted_list)

    demo_root = Path("data/demo")
    ensure_dir(demo_root)

    run_started = report["started_at"]

    session = requests.Session()
    session.headers.update({"User-Agent": "week1-demo-fetch/1.0"})

    for source in sources:
        name = source["source_name"]
        if name not in wanted:
            continue

        t0 = time.time()
        out_dir = demo_root / name
        ensure_dir(out_dir)

        records_path = out_dir / "records_100.json"
        jsonl_path = out_dir / "records_100.jsonl"
        metadata_path = out_dir / "metadata.json"
        debug_snippet_path = out_dir / "debug_payload_snippet.txt"

        status = "failed"
        error: dict[str, Any] | None = None
        handler_meta: dict[str, Any] = {}
        auth_required_detected = False
        records: list[dict[str, Any]] = []

        try:
            handler = HANDLERS.get(name)
            if handler is None:
                raise FetchError(code="NO_HANDLER", message=f"No handler implemented for source: {name}")

            records, handler_meta = handler(session, limit, source)
            status = "success"
        except FetchError as fe:
            error = fe.to_dict()
            auth_required_detected = bool(fe.auth_required_detected)
            status = "failed"
            if fe.debug_snippet:
                write_text(debug_snippet_path, fe.debug_snippet + "\n")
        except Exception as exc:
            # Never leak tracebacks; capture as structured error.
            error = {"code": "UNHANDLED_ERROR", "message": str(exc)}
            status = "failed"

        # Always write outputs (even on failure) to keep the demo reproducible.
        write_json(records_path, records[:limit])
        write_jsonl(jsonl_path, records[:limit])

        elapsed_ms = int((time.time() - t0) * 1000)
        metadata: dict[str, Any] = {
            "source": source,
            "run": {"started_at": run_started, "source_elapsed_ms": elapsed_ms, "limit": limit},
            "status": status,
            "records_written": len(records[:limit]),
            "auth_required_detected": auth_required_detected,
            "handler_metadata": handler_meta,
            "error": error,
            "outputs": {
                "records_100_json": str(records_path),
                "records_100_jsonl": str(jsonl_path),
                "metadata_json": str(metadata_path),
                "debug_payload_snippet_txt": str(debug_snippet_path) if debug_snippet_path.exists() else None,
            },
        }
        write_json(metadata_path, metadata)

        report["sources"][name] = {
            "status": status,
            "records_written": len(records[:limit]),
            "error": error,
            "auth_required_detected": auth_required_detected,
        }

    report["ended_at"] = utc_now_iso()
    write_json(demo_root / "demo_report.json", report)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


