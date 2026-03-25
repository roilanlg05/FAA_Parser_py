from __future__ import annotations

import asyncio
from pathlib import Path
import sys

sys.path.append(str(Path(__file__).resolve().parents[1]))

from app.parser import parse_faa_xml
from app.projection import extract_projected_flights


def test_attached_asdex_file() -> None:
    xml = Path('/mnt/data/Pasted text.txt').read_text(encoding='utf-8')
    parsed = parse_faa_xml(xml)
    assert parsed['payload_type'] == 'asdex_surface_movement_event'
    assert parsed['airport'] == 'KCLT'
    assert parsed['report_count'] == 51


def test_sfdps_projection() -> None:
    xml = Path('/mnt/data/sfdps_sample.xml').read_text(encoding='utf-8')
    parsed = parse_faa_xml(xml)
    flights = extract_projected_flights(parsed)
    assert parsed['payload_type'] == 'sfdps_message_collection'
    assert len(flights) == 1
    assert flights[0]['flight_id'] == 'DAL565'
    assert flights[0]['gufi']
