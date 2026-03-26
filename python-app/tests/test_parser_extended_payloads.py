from __future__ import annotations

from app.parser import parse_faa_xml


def test_parse_fdps_status_message() -> None:
    xml = """<?xml version="1.0" encoding="UTF-8"?>
<FDPSMsg xmlns="us:gov:dot:faa:atm:enroute:entities:flightdata" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
  <properties>
    <propSourceSystem>SLC</propSourceSystem>
    <propSourceFacility>ZKC</propSourceFacility>
    <propMessageType>status</propMessageType>
    <propRcvdTime>2026-03-26T11:46:41.143Z</propRcvdTime>
    <propSentTime>2026-03-26T11:46:41.199Z</propSentTime>
    <propSeqNo>0045371874</propSeqNo>
    <propDataType>STATUS</propDataType>
    <propTestMsg xsi:nil="true"/>
  </properties>
  <center>ZKC</center>
  <msgTimes>
    <arrivalTime xsi:nil="true"/>
    <arrivalTimeEpoch xsi:nil="true"/>
    <departureTime xsi:nil="true"/>
    <departureTimeEpoch xsi:nil="true"/>
  </msgTimes>
  <status>
    <classification>Public</classification>
    <time>2026-03-26T11:46:41Z</time>
    <statusType>ARTCC Status</statusType>
    <source>FDPS Monitor</source>
    <artcc state="up">ZAB</artcc>
    <artcc state="down">ZBW</artcc>
    <details>Track Correlation Path: OPERATIONAL</details>
    <numberOfMsgs>2</numberOfMsgs>
  </status>
</FDPSMsg>
"""

    parsed = parse_faa_xml(xml)

    assert parsed['payload_type'] == 'sfdps_fdps_status'
    assert parsed['variant'] == 'status'
    assert parsed['center'] == 'ZKC'
    assert parsed['properties']['propTestMsg'] is None
    assert parsed['msg_times']['arrivalTime'] is None

    status = parsed['status']
    assert isinstance(status, dict)
    assert status['statusType'] == 'ARTCC Status'
    assert status['numberOfMsgs'] == 2
    assert status['artcc_count'] == 2
    assert status['artcc_state_counts']['up'] == 1
    assert status['artcc_state_counts']['down'] == 1


def test_parse_fdps_hs_message() -> None:
    xml = """<?xml version="1.0" encoding="UTF-8"?>
<FDPSMsg xmlns="us:gov:dot:faa:atm:enroute:entities:flightdata" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
  <properties>
    <propSourceSystem>SLC</propSourceSystem>
    <propSourceFacility>ZAB</propSourceFacility>
    <propMessageType>HS</propMessageType>
    <propDataType>ERGMP</propDataType>
  </properties>
  <center>ZAB</center>
  <HS>
    <sourceId_00e>ZAB</sourceId_00e>
    <sourceSeqNo_00e2>0024891546</sourceSeqNo_00e2>
    <meteringStatus_140d>UP</meteringStatus_140d>
  </HS>
</FDPSMsg>
"""

    parsed = parse_faa_xml(xml)

    assert parsed['payload_type'] == 'sfdps_fdps_status'
    assert parsed['variant'] == 'hs'
    assert parsed['data_type'] == 'ERGMP'
    assert parsed['status'] is None

    hs = parsed['hs']
    assert isinstance(hs, dict)
    assert hs['sourceId_00e'] == 'ZAB'
    assert hs['sourceSeqNo_00e2'] == '0024891546'
    assert hs['meteringStatus_140d'] == 'UP'


def test_parse_aixm_basic_message_members() -> None:
    xml = """<?xml version="1.0" encoding="UTF-8"?>
<ns7:AIXMBasicMessage
  xmlns:ns7="http://www.aixm.aero/schema/5.1/message"
  xmlns:ns1="http://www.opengis.net/gml/3.2"
  xmlns:ns3="http://www.aixm.aero/schema/5.1"
  xmlns:ns8="urn:us:gov:dot:faa:atm:enroute:entities:saasectorassignmentstatusextension"
  xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
  id="SH_aixmBasicMessage">
  <ns7:name>SH_AIXM</ns7:name>
  <ns7:boundedBy xsi:nil="true"/>
  <ns7:hasMember>
    <ns3:Airspace id="AS_1">
      <ns3:boundedBy xsi:nil="true"/>
      <ns3:timeSlice>
        <ns3:AirspaceTimeSlice id="TS_1">
          <ns3:validTime>
            <ns1:TimePeriod id="TP_1">
              <ns1:beginPosition>2026-03-26T00:00:00Z</ns1:beginPosition>
              <ns1:endPosition>2026-03-27T00:00:00Z</ns1:endPosition>
            </ns1:TimePeriod>
          </ns3:validTime>
          <ns3:interpretation>SNAPSHOT</ns3:interpretation>
          <ns3:designator>30</ns3:designator>
          <ns3:extension>
            <ns8:SectorAssignmentStatusExtension id="E_1">
              <ns8:FAVnumber>14046</ns8:FAVnumber>
            </ns8:SectorAssignmentStatusExtension>
          </ns3:extension>
        </ns3:AirspaceTimeSlice>
      </ns3:timeSlice>
    </ns3:Airspace>
  </ns7:hasMember>
  <ns7:hasMember>
    <ns3:RouteSegment id="RS_1">
      <ns3:timeSlice>
        <ns3:RouteSegmentTimeSlice id="RTS_1">
          <ns3:validTime>
            <ns1:TimePeriod id="TP_2">
              <ns1:beginPosition>2026-03-26T00:00:00Z</ns1:beginPosition>
              <ns1:endPosition>2026-03-27T00:00:00Z</ns1:endPosition>
            </ns1:TimePeriod>
          </ns3:validTime>
          <ns3:interpretation>SNAPSHOT</ns3:interpretation>
          <ns3:availability>AVAILABLE</ns3:availability>
        </ns3:RouteSegmentTimeSlice>
      </ns3:timeSlice>
    </ns3:RouteSegment>
  </ns7:hasMember>
</ns7:AIXMBasicMessage>
"""

    parsed = parse_faa_xml(xml)

    assert parsed['payload_type'] == 'sfdps_aixm_basic_message'
    assert parsed['name'] == 'SH_AIXM'
    assert parsed['id'] == 'SH_aixmBasicMessage'
    assert parsed['member_count'] == 2
    assert parsed['member_type_counts']['Airspace'] == 1
    assert parsed['member_type_counts']['RouteSegment'] == 1
    assert parsed['extension_counts']['SectorAssignmentStatusExtension'] == 1

    first_member = parsed['members'][0]
    assert first_member['entity_type'] == 'Airspace'
    assert '30' in first_member['summary']['designators']
    assert 'SNAPSHOT' in first_member['summary']['interpretations']

    second_member = parsed['members'][1]
    assert second_member['entity_type'] == 'RouteSegment'
    assert 'AVAILABLE' in second_member['summary']['availability']
