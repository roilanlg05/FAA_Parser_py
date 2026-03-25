from __future__ import annotations

import importlib.util
import sys
from pathlib import Path


def _load(module_name: str, path: Path):
    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f'Unable to load {module_name} from {path}')
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


ROOT = Path(__file__).resolve().parents[2]
PARSER = _load('tbfm_parser_external_for_tests', ROOT / 'tbfm/app/tbfm_parser.py')
PROJECTIONS = _load('tbfm_projections_external_for_tests', ROOT / 'tbfm/app/projections.py')


def test_tbfm_parse_and_projection() -> None:
    xml = """<?xml version='1.0' encoding='UTF-8'?>
<env xmlns='urn:us:gov:dot:faa:atm:tfm:tbfmmeteringpublication' envSrce='TBFM' envTime='2026-03-25T12:00:00Z'>
  <tma msgId='abc123' msgTime='2026-03-25T12:00:00Z'>
    <air aid='UPS1326' tmaId='M90' airType='arrival'>
      <flt aid='UPS1326' dap='KSDF' apt='KMCO' />
    </air>
  </tma>
</env>
"""
    parsed = PARSER.parse_tbfm_text(xml)
    assert parsed['parsed_count'] == 1
    first_doc = parsed['documents'][0]
    assert first_doc['payload_type'] == 'tbfm_metering_publication'
    projections = PROJECTIONS.build_tbfm_projections(parsed)
    assert len(projections) > 0
    assert projections[0]['acid'] == 'UPS1326'
