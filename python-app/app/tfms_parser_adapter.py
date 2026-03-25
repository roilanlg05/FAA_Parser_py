from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from typing import Any

from .config import settings

_module = None


def _load_module():
    global _module
    if _module is not None:
        return _module
    parser_path = Path(settings.tfms_parser_path)
    if not parser_path.exists():
        raise FileNotFoundError(f'TFMS parser file not found: {parser_path}')
    spec = importlib.util.spec_from_file_location('tfms_parser_external', parser_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f'Unable to load TFMS parser from {parser_path}')
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    _module = module
    return module


def parse_tfms_xml(xml_text: str | bytes) -> dict[str, Any]:
    module = _load_module()
    return module.parse_tfms_xml(xml_text)


def build_tfms_projections(parsed_payload: dict[str, Any]) -> list[dict[str, Any]]:
    module = _load_module()
    return list(module.build_projections(parsed_payload))
