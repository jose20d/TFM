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
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit
from xml.etree import ElementTree as ET

try:
    import pandas as pd  # type: ignore
    import requests  # type: ignore
except ModuleNotFoundError as _e:  # pragma: no cover
    pd = None  # type: ignore
    requests = None  # type: ignore
    _IMPORT_ERROR = str(_e)
else:
    _IMPORT_ERROR = None


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
    base_url = source["base_url"].rstrip("/") + source["endpoint"]

    def _with_params(u: str, extra: dict[str, Any]) -> str:
        parts = urlsplit(u)
        q = dict(parse_qsl(parts.query, keep_blank_values=True))
        q.update({k: str(v) for k, v in extra.items() if v is not None})
        return urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(q), parts.fragment))

    per_page = max(50, min(int(limit), 1000))
    page = 1
    pages_fetched = 0
    total_retries = 0
    http_status: int | None = None
    content_type: str | None = None
    total_available: int | None = None

    collected: list[dict[str, Any]] = []

    while len(collected) < limit:
        url = _with_params(base_url, {"per_page": per_page, "page": page})
        resp, retries_used = request_with_retries(session, "GET", url, timeout_seconds=30, max_retries=4)
        total_retries += retries_used
        pages_fetched += 1

        body = resp.content
        ct = resp.headers.get("Content-Type")
        content_type = ct
        http_status = resp.status_code

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

        meta0 = payload[0] if isinstance(payload[0], dict) else {}
        if isinstance(meta0, dict):
            try:
                total_available = int(meta0.get("total")) if meta0.get("total") is not None else total_available
            except Exception:
                pass

        df = pd.DataFrame(payload[1])
        if not df.empty:
            collected.extend(df.to_dict(orient="records"))

        # Stop conditions: no data returned or reached last page according to meta.
        if df.empty:
            break

        pages = meta0.get("pages") if isinstance(meta0, dict) else None
        try:
            pages_int = int(pages) if pages is not None else None
        except Exception:
            pages_int = None
        if pages_int is not None and page >= pages_int:
            break

        page += 1

    records = collected[:limit]
    meta = {
        "request_url": base_url,
        "http_status": http_status,
        "content_type": content_type,
        "retries_used": total_retries,
        "per_page_used": per_page,
        "pages_fetched": pages_fetched,
        "records_available": total_available,
        "records_collected": len(collected),
    }
    return records, meta


def handler_usgs_mrds(session: requests.Session, limit: int, source: dict[str, Any]) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    # Prefer source-configured URL; allow optional override via env var for advanced usage.
    env_name = "MRDS_CSV_URL"
    url = os.environ.get(env_name, "").strip()
    if not url:
        endpoint = (source.get("endpoint") or "").lstrip("/")
        if endpoint:
            url = source["base_url"].rstrip("/") + "/" + endpoint
        else:
            # Sensible default (public, stable)
            url = "https://mrdata.usgs.gov/mrds/mrds.csv"

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
        df = pd.read_csv(io.StringIO(text), low_memory=False)
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
    # Prefer source-configured URL; allow optional override via env var for advanced usage.
    env_name = "ONEGEOLOGY_WMS_URL"
    env_url = os.environ.get(env_name, "").strip()

    endpoint = source.get("endpoint")
    if not (isinstance(endpoint, str) and endpoint.strip()):
        raise FetchError(
            code="CONFIG_ERROR",
            message="onegeology_wms requires source.endpoint with a WMS GetCapabilities request.",
        )

    candidates: list[str] = []
    if env_url:
        candidates.append(env_url)
    candidates.append(source["base_url"].rstrip("/") + "/" + endpoint.lstrip("/"))

    last_error: FetchError | None = None
    for url in candidates:
        try:
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

            tag_lower = (root.tag or "").lower()

            # Expect WMS GetCapabilities-like document
            cap = root.find(".//{*}Capability")
            if not ("wms_capabilities" in tag_lower or cap is not None):
                raise FetchError(
                    code="SCHEMA_ERROR",
                    message="XML is not a WMS GetCapabilities document (missing Capability).",
                    http_status=resp.status_code,
                    content_type=ct,
                    debug_snippet=payload_snippet_from_bytes(body),
                )

            wms_version = root.attrib.get("version") or root.attrib.get("{http://www.w3.org/2001/XMLSchema-instance}schemaLocation")

            records: list[dict[str, Any]] = []
            for layer in root.findall(".//{*}Layer"):
                name_el = layer.find("{*}Name")
                title_el = layer.find("{*}Title")
                abs_el = layer.find("{*}Abstract")

                name = (name_el.text or "").strip() if name_el is not None else ""
                title = (title_el.text or "").strip() if title_el is not None else ""
                abstract = (abs_el.text or "").strip() if abs_el is not None else ""

                crs_vals: list[str] = []
                for el in layer.findall("{*}CRS"):
                    v = (el.text or "").strip()
                    if v:
                        crs_vals.append(v)
                for el in layer.findall("{*}SRS"):
                    v = (el.text or "").strip()
                    if v:
                        crs_vals.append(v)
                crs_vals = sorted(set(crs_vals))

                bboxes: list[dict[str, Any]] = []
                for bb in layer.findall("{*}BoundingBox"):
                    bboxes.append(
                        {
                            "crs": bb.attrib.get("CRS") or bb.attrib.get("SRS"),
                            "minx": bb.attrib.get("minx"),
                            "miny": bb.attrib.get("miny"),
                            "maxx": bb.attrib.get("maxx"),
                            "maxy": bb.attrib.get("maxy"),
                        }
                    )

                geo = layer.find("{*}EX_GeographicBoundingBox")
                geo_bbox = None
                if geo is not None:
                    def _t(tag: str) -> str | None:
                        el = geo.find(f"{{*}}{tag}")
                        val = (el.text or "").strip() if el is not None else ""
                        return val or None
                    geo_bbox = {
                        "west": _t("westBoundLongitude"),
                        "east": _t("eastBoundLongitude"),
                        "south": _t("southBoundLatitude"),
                        "north": _t("northBoundLatitude"),
                    }

                if not name and not title:
                    continue

                records.append(
                    {
                        "layer_name": name or None,
                        "title": title or None,
                        "abstract": abstract or None,
                        "crs": crs_vals,
                        "bounding_boxes": bboxes,
                        "ex_geographic_bounding_box": geo_bbox,
                    }
                )
                if len(records) >= limit:
                    break

            meta = {
                "request_url": url,
                "http_status": resp.status_code,
                "content_type": ct,
                "retries_used": retries_used,
                "extraction_mode": "wms_getcapabilities_layers",
                "wms_version": wms_version,
                "records_found": len(records),
            }
            return records, meta
        except FetchError as fe:
            last_error = fe
            continue

    assert last_error is not None
    raise last_error


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

    # Always write outputs relative to the repository root, regardless of current working directory.
    repo_root = Path(__file__).resolve().parents[1]

    limit = int(args.limit)
    sources_file = Path(args.sources_file)
    if not sources_file.is_absolute():
        sources_file = (repo_root / sources_file).resolve()

    report: dict[str, Any] = {"started_at": utc_now_iso(), "ended_at": None, "limit": limit, "sources": {}}

    if _IMPORT_ERROR is not None or pd is None or requests is None:
        # Never print tracebacks; produce a deterministic report instead.
        report["ended_at"] = utc_now_iso()
        report["error"] = {
            "code": "MISSING_DEPENDENCY",
            "message": "Missing dependency. Activate your venv and install requirements.txt.",
            "details": _IMPORT_ERROR,
        }
        demo_root = repo_root / "data" / "demo"
        ensure_dir(demo_root)
        write_json(demo_root / "demo_report.json", report)
        return 0

    try:
        sources = load_sources(sources_file)
    except FetchError as fe:
        # If sources cannot be loaded, still produce a report for reproducibility.
        report["sources"] = {}
        report["ended_at"] = utc_now_iso()
        report["error"] = fe.to_dict()
        demo_root = repo_root / "data" / "demo"
        ensure_dir(demo_root)
        write_json(demo_root / "demo_report.json", report)
        return 0

    wanted_list = parse_sources_arg(args.sources)
    wanted = {s["source_name"] for s in sources} if wanted_list == ["all"] else set(wanted_list)

    demo_root = repo_root / "data" / "demo"
    ensure_dir(demo_root)

    run_started = report["started_at"]

    session = requests.Session()
    session.headers.update({"User-Agent": "week1-demo-fetch/1.0"})

    for source in sources:
        name = source["source_name"]
        if name not in wanted:
            continue

        print(f"[download] {name}: starting...")
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
        print(f"[download] {name}: {status} (records_written={len(records[:limit])})")

    report["ended_at"] = utc_now_iso()
    write_json(demo_root / "demo_report.json", report)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

