from __future__ import annotations

import json
import sys
from pathlib import Path
import unittest

ROOT = Path(__file__).resolve().parents[1]
APP_ROOT = ROOT / "python-app"
sys.path.insert(0, str(APP_ROOT))

from app.tfms_parser import parse_tfms_xml, build_projections  # noqa: E402

DATA_DIR = Path("/mnt/data")
FILE_FLIGHT_SECTORS = ROOT / "examples_flight_sectors.xml"
FILE_TRACK_INFO = DATA_DIR / "Pasted text (2).txt"
FILE_STATUS = DATA_DIR / "Pasted text (3).txt"
FILE_TMI = ROOT / "examples_tmi_flight_list.xml"


class TfmsParserTests(unittest.TestCase):
    def test_flight_sectors(self) -> None:
        parsed = parse_tfms_xml(FILE_FLIGHT_SECTORS.read_text(encoding="utf-8"))
        self.assertEqual(parsed["payload_type"], "tfms_flight_data_output")
        self.assertGreaterEqual(parsed["messageCount"], 1)
        first = parsed["messages"][0]
        self.assertEqual(first["msgType"], "FlightSectors")
        self.assertIn("fixes", first["body"]["flightTraversalData2"])
        self.assertGreater(len(first["body"]["flightTraversalData2"]["fixes"]), 0)
        projections = build_projections(parsed)
        self.assertGreater(len(projections), 0)

    def test_track_information(self) -> None:
        parsed = parse_tfms_xml(FILE_TRACK_INFO.read_text(encoding="utf-8"))
        self.assertEqual(parsed["payload_type"], "tfms_flight_data_output")
        first = parsed["messages"][0]
        self.assertEqual(first["msgType"], "trackInformation")
        self.assertIsNotNone(first["body"]["qualifiedAircraftId"])
        self.assertIn("position", first["body"])
        projections = build_projections(parsed)
        self.assertGreater(len(projections), 0)

    def test_status_output(self) -> None:
        parsed = parse_tfms_xml(FILE_STATUS.read_text(encoding="utf-8"))
        self.assertEqual(parsed["payload_type"], "tfms_status_output")
        self.assertGreater(parsed["statusCount"], 0)
        projections = build_projections(parsed)
        self.assertGreater(len(projections), 0)


if __name__ == "__main__":
    unittest.main()


class TfmsTmiTests(unittest.TestCase):
    def test_tmi_flight_list(self) -> None:
        parsed = parse_tfms_xml(FILE_TMI.read_text(encoding="utf-8"))
        self.assertEqual(parsed["payload_type"], "tfms_flow_information_output")
        self.assertEqual(parsed["messages"][0]["msgType"], "TMI_FLIGHT_LIST")
        self.assertEqual(parsed["messages"][0]["tmiFlightDataList"][0]["flight"]["aircraftId"], "TWY888")
        projections = build_projections(parsed)
        self.assertGreater(len(projections), 0)

    def test_rstr_restriction_message(self) -> None:
        xml = """<?xml version='1.0' encoding='UTF-8'?>
<ds:tfmDataService
  xmlns:ds='urn:us:gov:dot:faa:atm:tfm:tfmdataservice'
  xmlns:fi='urn:us:gov:dot:faa:atm:tfm:flowinformation'
  xmlns:fcm='urn:us:gov:dot:faa:atm:tfm:ficommonmessages'>
  <ds:fiOutput>
    <fi:fiMessage sourceFacility='DCC' sourceTimeStamp='2026-03-24T23:55:18Z' msgType='RSTR'>
      <fi:restrictionMessage>
        <fcm:restrictionId>16784362</fcm:restrictionId>
        <fcm:facility>DCC</fcm:facility>
        <fcm:startTime>2026-03-24T23:45:00Z</fcm:startTime>
        <fcm:stopTime>2026-03-25T02:00:00Z</fcm:stopTime>
        <fcm:airports>MCO</fcm:airports>
        <fcm:restrictionType>DEPARTURE</fcm:restrictionType>
        <fcm:restrictionCategory>STOP</fcm:restrictionCategory>
      </fi:restrictionMessage>
    </fi:fiMessage>
  </ds:fiOutput>
</ds:tfmDataService>
"""
        parsed = parse_tfms_xml(xml)
        self.assertEqual(parsed["payload_type"], "tfms_flow_information_output")
        msg = parsed["messages"][0]
        self.assertEqual(msg["msgType"], "RSTR")
        self.assertEqual(msg["restrictionCount"], 1)
        self.assertEqual(msg["restrictionMessage"]["restrictionId"], "16784362")
        self.assertEqual(msg["restrictionMessage"]["airports"], "MCO")

        projections = build_projections(parsed)
        self.assertGreater(len(projections), 0)
        self.assertEqual(projections[0]["projection_type"], "restriction_message")
        self.assertEqual(projections[0]["msgType"], "RSTR")
