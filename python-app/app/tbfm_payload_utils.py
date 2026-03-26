from __future__ import annotations

from typing import Any

from .tbfm_parser_adapter import build_tbfm_projections, parse_tbfm_xml


def strip_raw_fields(value: Any) -> Any:
    if isinstance(value, dict):
        return {k: strip_raw_fields(v) for k, v in value.items() if k != 'raw'}
    if isinstance(value, list):
        return [strip_raw_fields(item) for item in value]
    return value


def only_raw_fields(value: Any) -> Any:
    if isinstance(value, dict):
        out: dict[str, Any] = {}
        if 'raw' in value:
            out['raw'] = value['raw']
        for key, inner in value.items():
            if key == 'raw':
                continue
            child = only_raw_fields(inner)
            if child not in (None, {}, []):
                out[key] = child
        return out
    if isinstance(value, list):
        items = [only_raw_fields(item) for item in value]
        return [item for item in items if item not in (None, {}, [])]
    return None


def projection_raw_by_key_from_xml(xml_text: str, projection_key: str) -> dict[str, Any] | None:
    parsed = parse_tbfm_xml(xml_text)
    for projection in build_tbfm_projections(parsed):
        key = projection.get('projection_key') or projection.get('key')
        if key != projection_key:
            continue
        data = projection.get('data') or {}
        raw_only = only_raw_fields(data)
        if isinstance(raw_only, dict) and raw_only:
            return raw_only
    return None
