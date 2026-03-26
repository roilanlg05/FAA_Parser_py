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
PARSER = _load('tfms_parser_external_for_mapping_tests', ROOT / 'tfms/app/tfms_parser.py')


def test_parse_flight_plan_information_into_structured_body() -> None:
    xml = """<?xml version='1.0' encoding='UTF-8'?>
<ds:tfmDataService
  xmlns:ds='urn:us:gov:dot:faa:atm:tfm:tfmdataservice'
  xmlns:fdm='urn:us:gov:dot:faa:atm:tfm:flightdata'
  xmlns:nxce='urn:us:gov:dot:faa:atm:tfm:tfmdatacoreelements'
  xmlns:nxcm='urn:us:gov:dot:faa:atm:tfm:flightdatacommonmessages'>
  <ds:fltdOutput>
    <fdm:fltdMessage msgType='flightPlanInformation' sourceFacility='KZFW' sourceTimeStamp='2026-03-26T03:53:54Z' flightRef='142534846' acid='AAL2987'>
      <fdm:flightPlanInformation>
        <nxcm:qualifiedAircraftId>
          <nxce:aircraftId>AAL2987</nxce:aircraftId>
          <nxce:gufi>KF140345UV</nxce:gufi>
          <nxce:igtd>2026-03-26T01:20:00Z</nxce:igtd>
          <nxce:departurePoint><nxce:airport>KOKC</nxce:airport></nxce:departurePoint>
          <nxce:arrivalPoint><nxce:airport>KDFW</nxce:airport></nxce:arrivalPoint>
        </nxcm:qualifiedAircraftId>
        <nxcm:routeOfFlight>KOKC DCT KDFW</nxcm:routeOfFlight>
        <nxcm:ncsmRouteData>
          <nxcm:routeOfFlight>KOKC DCT KDFW</nxcm:routeOfFlight>
        </nxcm:ncsmRouteData>
      </fdm:flightPlanInformation>
    </fdm:fltdMessage>
  </ds:fltdOutput>
</ds:tfmDataService>
"""

    parsed = PARSER.parse_tfms_xml(xml)
    first = parsed['messages'][0]
    body = first['body']

    assert first['msgType'] == 'flightPlanInformation'
    assert body['qualifiedAircraftId']['gufi'] == 'KF140345UV'
    assert body['qualifiedAircraftId']['departurePoint']['airport'] == 'KOKC'
    assert body['qualifiedAircraftId']['arrivalPoint']['airport'] == 'KDFW'
    assert body['routeOfFlight'] == 'KOKC DCT KDFW'
    assert body['ncsmRouteData']['routeOfFlight'] == 'KOKC DCT KDFW'

    projections = PARSER.build_projections(parsed)
    assert len(projections) == 1
    assert projections[0]['key'] == 'KF140345UV'
    assert projections[0]['gufi'] == 'KF140345UV'


def test_parse_flight_plan_amendment_information_amendment_data() -> None:
    xml = """<?xml version='1.0' encoding='UTF-8'?>
<ds:tfmDataService
  xmlns:ds='urn:us:gov:dot:faa:atm:tfm:tfmdataservice'
  xmlns:fdm='urn:us:gov:dot:faa:atm:tfm:flightdata'
  xmlns:nxce='urn:us:gov:dot:faa:atm:tfm:tfmdatacoreelements'
  xmlns:nxcm='urn:us:gov:dot:faa:atm:tfm:flightdatacommonmessages'>
  <ds:fltdOutput>
    <fdm:fltdMessage msgType='flightPlanAmendmentInformation' sourceFacility='KZFW' sourceTimeStamp='2026-03-26T04:03:54Z' flightRef='142534846' acid='AAL2987'>
      <fdm:flightPlanAmendmentInformation>
        <nxcm:qualifiedAircraftId>
          <nxce:aircraftId>AAL2987</nxce:aircraftId>
          <nxce:gufi>KF140345UV</nxce:gufi>
        </nxcm:qualifiedAircraftId>
        <nxcm:amendmentData>
          <nxcm:newRouteOfFlight>DCT TTT</nxcm:newRouteOfFlight>
          <nxcm:newCoordinationPoint>KZFW</nxcm:newCoordinationPoint>
          <nxcm:newCoordinationTime>2026-03-26T04:20:00Z</nxcm:newCoordinationTime>
          <nxcm:newSpeed><nxce:filedTrueAirSpeed>420</nxce:filedTrueAirSpeed></nxcm:newSpeed>
        </nxcm:amendmentData>
      </fdm:flightPlanAmendmentInformation>
    </fdm:fltdMessage>
  </ds:fltdOutput>
</ds:tfmDataService>
"""

    parsed = PARSER.parse_tfms_xml(xml)
    body = parsed['messages'][0]['body']

    assert body['qualifiedAircraftId']['gufi'] == 'KF140345UV'
    assert body['amendmentData']['newRouteOfFlight'] == 'DCT TTT'
    assert body['amendmentData']['newCoordinationPoint'] == 'KZFW'
    assert body['amendmentData']['newCoordinationTime'] == '2026-03-26T04:20:00Z'
    assert body['amendmentData']['newSpeed']['filedTrueAirSpeed'] == 420


def test_parse_flow_general_advisory_blocks() -> None:
    xml = """<?xml version='1.0' encoding='UTF-8'?>
<ds:tfmDataService
  xmlns:ds='urn:us:gov:dot:faa:atm:tfm:tfmdataservice'
  xmlns:fi='urn:us:gov:dot:faa:atm:tfm:flowinformation'
  xmlns:fcm='urn:us:gov:dot:faa:atm:tfm:ficommonmessages'
  xmlns:fcd='urn:us:gov:dot:faa:atm:tfm:ficommondatatypes'>
  <ds:fiOutput>
    <fi:fiMessage sourceFacility='DCC' sourceTimeStamp='2026-03-26T00:01:53Z' msgType='GADV'>
      <fi:generalAdvisory>
        <fcm:advisoryNumber>0001</fcm:advisoryNumber>
        <fcm:origin>ATCSCC</fcm:origin>
        <fcm:dateSent>2026-03-26T00:01:53Z</fcm:dateSent>
        <fcm:facilities>ZDC</fcm:facilities>
        <fcm:advisoryTitle>Test Advisory</fcm:advisoryTitle>
        <fcm:advisoryText>Text</fcm:advisoryText>
        <fcm:effectivePeriod>
          <fcd:startTime>2026-03-26T00:01:53Z</fcd:startTime>
          <fcd:endTime>2026-03-27T02:00:00Z</fcd:endTime>
        </fcm:effectivePeriod>
      </fi:generalAdvisory>
    </fi:fiMessage>
  </ds:fiOutput>
</ds:tfmDataService>
"""

    parsed = PARSER.parse_tfms_xml(xml)
    msg = parsed['messages'][0]

    assert msg['msgType'] == 'GADV'
    assert msg['generalAdvisoryCount'] == 1
    advisory = msg['generalAdvisories'][0]
    assert advisory['advisoryNumber'] == '0001'
    assert advisory['origin'] == 'ATCSCC'
    assert advisory['effectivePeriod']['startTime'] == '2026-03-26T00:01:53Z'
    assert advisory['effectivePeriod']['endTime'] == '2026-03-27T02:00:00Z'

    projections = PARSER.build_projections(parsed)
    assert any(p['projection_type'] == 'general_advisory' for p in projections)


def test_parse_flow_airport_config_blocks() -> None:
    xml = """<?xml version='1.0' encoding='UTF-8'?>
<ds:tfmDataService
  xmlns:ds='urn:us:gov:dot:faa:atm:tfm:tfmdataservice'
  xmlns:fi='urn:us:gov:dot:faa:atm:tfm:flowinformation'
  xmlns:fcm='urn:us:gov:dot:faa:atm:tfm:ficommonmessages'>
  <ds:fiOutput>
    <fi:fiMessage sourceFacility='MEM' sourceTimeStamp='2026-03-25T16:15:00Z' msgType='APTC'>
      <fi:airportConfigMessage>
        <fcm:eventTime>2026-03-25T16:15:00Z</fcm:eventTime>
        <fcm:entryTime>2026-03-25T16:15:00Z</fcm:entryTime>
        <fcm:facility>MEM</fcm:facility>
        <fcm:airport>MEM</fcm:airport>
        <fcm:arrRunwayConf>18L/18R</fcm:arrRunwayConf>
        <fcm:depRunwayConf>18C/18R</fcm:depRunwayConf>
        <fcm:arrRate>66</fcm:arrRate>
        <fcm:depRate>58</fcm:depRate>
      </fi:airportConfigMessage>
    </fi:fiMessage>
  </ds:fiOutput>
</ds:tfmDataService>
"""

    parsed = PARSER.parse_tfms_xml(xml)
    msg = parsed['messages'][0]

    assert msg['msgType'] == 'APTC'
    assert msg['airportConfigCount'] == 1
    config = msg['airportConfigMessages'][0]
    assert config['facility'] == 'MEM'
    assert config['airport'] == 'MEM'
    assert config['arrRunwayConf'] == '18L/18R'
    assert config['depRunwayConf'] == '18C/18R'

    projections = PARSER.build_projections(parsed)
    assert any(p['projection_type'] == 'airport_config' for p in projections)


def test_parse_flow_fxa_blocks_and_projection() -> None:
    xml = """<?xml version='1.0' encoding='UTF-8'?>
<ds:tfmDataService
  xmlns:ds='urn:us:gov:dot:faa:atm:tfm:tfmdataservice'
  xmlns:fi='urn:us:gov:dot:faa:atm:tfm:flowinformation'
  xmlns:fcm='urn:us:gov:dot:faa:atm:tfm:ficommonmessages'>
  <ds:fiOutput>
    <fi:fiMessage sourceFacility='DCC' sourceTimeStamp='2026-03-26T05:00:00Z' msgType='FXA'>
      <fi:feaFca>
        <fcm:fcaId>fca.dccops.sample.1</fcm:fcaId>
        <fcm:fcaName>JAM_E</fcm:fcaName>
        <fcm:tmiStatus>UPDATED</fcm:tmiStatus>
      </fi:feaFca>
    </fi:fiMessage>
  </ds:fiOutput>
</ds:tfmDataService>
"""

    parsed = PARSER.parse_tfms_xml(xml)
    msg = parsed['messages'][0]

    assert msg['msgType'] == 'FXA'
    assert msg['feaFcaCount'] == 1
    assert msg['feaFca'][0]['fcaId'] == 'fca.dccops.sample.1'

    projections = PARSER.build_projections(parsed)
    assert any(p['projection_type'] == 'fea_fca' for p in projections)
