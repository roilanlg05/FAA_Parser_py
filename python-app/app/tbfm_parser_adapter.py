from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from typing import Any

from .config import settings

_parser_module = None
_projections_module = None


def _load_module(module_name: str, path: Path):
    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f'Unable to load module {module_name} from {path}')
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _load_parser_module():
    global _parser_module
    if _parser_module is not None:
        return _parser_module
    parser_path = Path(settings.tbfm_parser_path)
    if not parser_path.exists():
        raise FileNotFoundError(f'TBFM parser file not found: {parser_path}')
    _parser_module = _load_module('tbfm_parser_external', parser_path)
    return _parser_module


def _load_projections_module():
    global _projections_module
    if _projections_module is not None:
        return _projections_module
    projections_path = Path(settings.tbfm_projections_path)
    if not projections_path.exists():
        raise FileNotFoundError(f'TBFM projections file not found: {projections_path}')
    _projections_module = _load_module('tbfm_projections_external', projections_path)
    return _projections_module


def parse_tbfm_xml(xml_text: str | bytes) -> dict[str, Any]:
    parser_module = _load_parser_module()
    return parser_module.parse_tbfm_text(xml_text if isinstance(xml_text, str) else xml_text.decode('utf-8', errors='replace'))


def build_tbfm_projections(parsed_payload: dict[str, Any]) -> list[dict[str, Any]]:
    projections_module = _load_projections_module()
    return list(projections_module.build_tbfm_projections(parsed_payload))
