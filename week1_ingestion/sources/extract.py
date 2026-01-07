from __future__ import annotations

from typing import Any


def _get_by_dotted_path(payload: Any, path: str) -> Any:
    """
    Traverse a JSON-like object using a dotted path like "a.b.c".
    Returns None if any segment is missing.
    """
    cur = payload
    if path == "":
        return cur
    for part in path.split("."):
        if isinstance(cur, dict) and part in cur:
            cur = cur[part]
        else:
            return None
    return cur


def normalize_to_records(payload: Any, extract_cfg: dict[str, Any] | None) -> list[dict[str, Any]]:
    """
    Normalize any payload into a list of dict records:
    - If payload is already a list, each item becomes a record (dict items are kept as-is).
    - If payload is a dict, it becomes a single record.
    - If extract_cfg provides record_path/value_paths, the lists are zipped into records.
    """
    if extract_cfg:
        record_path = extract_cfg.get("record_path")
        value_paths = extract_cfg.get("value_paths") or {}
        if record_path:
            base = _get_by_dotted_path(payload, record_path)
            if not isinstance(base, list):
                raise ValueError(f"record_path '{record_path}' did not return a list")

            value_lists: dict[str, list[Any]] = {}
            for out_key, vp in value_paths.items():
                v = _get_by_dotted_path(payload, vp)
                if not isinstance(v, list):
                    raise ValueError(f"value_path '{vp}' did not return a list")
                value_lists[out_key] = v

            n = len(base)
            for k, v in value_lists.items():
                if len(v) != n:
                    raise ValueError(f"Length mismatch: {k} has {len(v)} but base has {n}")

            records: list[dict[str, Any]] = []
            for i in range(n):
                rec: dict[str, Any] = {"_index": i, "value": base[i]}
                for k, v in value_lists.items():
                    rec[k] = v[i]
                records.append(rec)
            return records

    # fallback simple
    if isinstance(payload, list):
        out: list[dict[str, Any]] = []
        for item in payload:
            if isinstance(item, dict):
                out.append(item)
            else:
                out.append({"value": item})
        return out
    if isinstance(payload, dict):
        return [payload]
    return [{"value": payload}]


