from __future__ import annotations

from typing import Dict, List, Optional, Tuple, Union
import xml.etree.ElementTree as ET


def local_name(tag: str) -> str:
    return tag.rsplit('}', 1)[1] if '}' in tag else tag


def child(parent: Optional[ET.Element], name: str) -> Optional[ET.Element]:
    if parent is None:
        return None
    for elem in list(parent):
        if local_name(elem.tag) == name:
            return elem
    return None


def children(parent: Optional[ET.Element], name: str) -> List[ET.Element]:
    if parent is None:
        return []
    return [elem for elem in list(parent) if local_name(elem.tag) == name]


def text_of(elem: Optional[ET.Element]) -> Optional[str]:
    if elem is None or elem.text is None:
        return None
    value = elem.text.strip()
    return value or None


def to_int(value: Optional[str]) -> Optional[int]:
    if value is None or value == '':
        return None
    try:
        return int(value)
    except ValueError:
        return None


def to_float(value: Optional[str]) -> Optional[float]:
    if value is None or value == '':
        return None
    try:
        return float(value)
    except ValueError:
        return None


def parse_pos_text(pos_text: Optional[str]) -> Tuple[Optional[float], Optional[float]]:
    if not pos_text:
        return (None, None)
    parts = pos_text.strip().split()
    if len(parts) != 2:
        return (None, None)
    return (to_float(parts[0]), to_float(parts[1]))


def parse_simple_block(elem: Optional[ET.Element]) -> Dict[str, object]:
    if elem is None:
        return {}
    result: Dict[str, object] = {}
    for sub in list(elem):
        value = text_of(sub)
        if value is None:
            result[local_name(sub.tag)] = parse_simple_block(sub)
        else:
            if value.lower() in {'true', 'false'}:
                result[local_name(sub.tag)] = value.lower() == 'true'
            else:
                maybe_int = to_int(value)
                maybe_float = to_float(value)
                if maybe_int is not None and str(maybe_int) == value:
                    result[local_name(sub.tag)] = maybe_int
                elif maybe_float is not None:
                    result[local_name(sub.tag)] = maybe_float
                else:
                    result[local_name(sub.tag)] = value
    return result


# --- SFDPS ---

def parse_name_value_pairs(parent: Optional[ET.Element]) -> Dict[str, str]:
    result: Dict[str, str] = {}
    for nv in children(parent, 'nameValue'):
        name = nv.attrib.get('name')
        value = nv.attrib.get('value')
        if name:
            result[name] = value if value is not None else (text_of(nv) or '')
    return result


def parse_runway_time(block: Optional[ET.Element], kind: str) -> Optional[str]:
    runway_position_and_time = child(block, 'runwayPositionAndTime')
    runway_time = child(runway_position_and_time, 'runwayTime')
    time_node = child(runway_time, kind)
    return time_node.attrib.get('time') if time_node is not None else None


def parse_arrival(arrival: Optional[ET.Element]) -> Dict[str, object]:
    return {
        'airport': arrival.attrib.get('arrivalPoint') if arrival is not None else None,
        'estimated_runway_time': parse_runway_time(arrival, 'estimated'),
    }


def parse_departure(departure: Optional[ET.Element]) -> Dict[str, object]:
    return {
        'airport': departure.attrib.get('departurePoint') if departure is not None else None,
        'actual_runway_time': parse_runway_time(departure, 'actual'),
    }


def parse_controlling_unit(unit: Optional[ET.Element]) -> Optional[Dict[str, Optional[str]]]:
    if unit is None:
        return None
    return {
        'unit_identifier': unit.attrib.get('unitIdentifier'),
        'sector_identifier': unit.attrib.get('sectorIdentifier'),
    }


def parse_handoff(enroute: Optional[ET.Element]) -> Optional[Dict[str, Optional[str]]]:
    boundary_crossings = child(enroute, 'boundaryCrossings')
    handoff = child(boundary_crossings, 'handoff')
    receiving_unit = child(handoff, 'receivingUnit')
    if receiving_unit is None:
        return None
    return {
        'unit_identifier': receiving_unit.attrib.get('unitIdentifier'),
        'sector_identifier': receiving_unit.attrib.get('sectorIdentifier'),
    }


def parse_position(position: Optional[ET.Element]) -> Optional[Dict[str, object]]:
    if position is None:
        return None

    actual_speed = child(child(position, 'actualSpeed'), 'surveillance')
    altitude = child(position, 'altitude')
    target_altitude = child(position, 'targetAltitude')
    target_position = child(position, 'targetPosition')
    position_node = child(position, 'position')
    location = child(position_node, 'location')
    pos = child(location, 'pos')
    target_pos = child(target_position, 'pos')
    lat, lon = parse_pos_text(text_of(pos))
    target_lat, target_lon = parse_pos_text(text_of(target_pos))
    track_velocity = child(position, 'trackVelocity')

    return {
        'position_time': position.attrib.get('positionTime'),
        'target_position_time': position.attrib.get('targetPositionTime'),
        'report_source': position.attrib.get('reportSource'),
        'speed_knots': to_float(text_of(actual_speed)),
        'speed_uom': actual_speed.attrib.get('uom') if actual_speed is not None else None,
        'altitude_ft': to_float(text_of(altitude)),
        'altitude_uom': altitude.attrib.get('uom') if altitude is not None else None,
        'lat': lat,
        'lon': lon,
        'target_altitude_ft': to_float(text_of(target_altitude)),
        'target_altitude_uom': target_altitude.attrib.get('uom') if target_altitude is not None else None,
        'target_lat': target_lat,
        'target_lon': target_lon,
        'velocity_x_knots': to_float(text_of(child(track_velocity, 'x'))),
        'velocity_y_knots': to_float(text_of(child(track_velocity, 'y'))),
    }


def parse_enroute(enroute: Optional[ET.Element]) -> Dict[str, object]:
    return {
        'handoff_receiving_unit': parse_handoff(enroute),
        'position': parse_position(child(enroute, 'position')),
    }


def parse_flight_identification(elem: Optional[ET.Element]) -> Dict[str, Optional[str]]:
    if elem is None:
        return {
            'computer_id': None,
            'site_specific_plan_id': None,
            'aircraft_identification': None,
        }
    return {
        'computer_id': elem.attrib.get('computerId'),
        'site_specific_plan_id': elem.attrib.get('siteSpecificPlanId'),
        'aircraft_identification': elem.attrib.get('aircraftIdentification'),
    }


def parse_operator(elem: Optional[ET.Element]) -> Optional[str]:
    org = child(child(elem, 'operatingOrganization'), 'organization')
    return org.attrib.get('name') if org is not None else None


def parse_assigned_altitude(elem: Optional[ET.Element]) -> Optional[Dict[str, object]]:
    if elem is None:
        return None
    for sub in list(elem):
        return {
            'type': local_name(sub.tag),
            'value_ft': to_float(text_of(sub)),
            'uom': sub.attrib.get('uom'),
        }
    return None


def parse_sfdps_message(message: ET.Element) -> Dict[str, object]:
    flight = child(message, 'flight')
    if flight is None:
        return {'message_type': message.attrib.get('{http://www.w3.org/2001/XMLSchema-instance}type'), 'flight': None}

    supplemental = child(child(flight, 'supplementalData'), 'additionalFlightInformation')
    flight_status = child(flight, 'flightStatus')
    gufi = child(flight, 'gufi')
    flight_plan = child(flight, 'flightPlan')

    return {
        'message_type': message.attrib.get('{http://www.w3.org/2001/XMLSchema-instance}type'),
        'flight': {
            'meta': {
                'centre': flight.attrib.get('centre'),
                'source': flight.attrib.get('source'),
                'system': flight.attrib.get('system'),
                'timestamp': flight.attrib.get('timestamp'),
            },
            'arrival': parse_arrival(child(flight, 'arrival')),
            'controlling_unit': parse_controlling_unit(child(flight, 'controllingUnit')),
            'departure': parse_departure(child(flight, 'departure')),
            'enroute': parse_enroute(child(flight, 'enRoute')),
            'flight_identification': parse_flight_identification(child(flight, 'flightIdentification')),
            'flight_status': flight_status.attrib.get('fdpsFlightStatus') if flight_status is not None else None,
            'gufi': text_of(gufi),
            'gufi_code_space': gufi.attrib.get('codeSpace') if gufi is not None else None,
            'operator': parse_operator(child(flight, 'operator')),
            'supplemental_data': parse_name_value_pairs(supplemental),
            'assigned_altitude': parse_assigned_altitude(child(flight, 'assignedAltitude')),
            'flight_plan_identifier': flight_plan.attrib.get('identifier') if flight_plan is not None else None,
        },
    }


def parse_sfdps_collection(root: ET.Element) -> Dict[str, object]:
    messages = [elem for elem in list(root) if local_name(elem.tag) == 'message']
    return {
        'payload_type': 'sfdps_message_collection',
        'message_count': len(messages),
        'messages': [parse_sfdps_message(message) for message in messages],
    }


# --- ASDE-X ---

def parse_flight_id(elem: Optional[ET.Element]) -> Optional[Dict[str, Optional[str]]]:
    if elem is None:
        return None
    return {'aircraft_id': text_of(child(elem, 'aircraftId'))}


def parse_position_report(pr: ET.Element) -> Dict[str, object]:
    position = child(pr, 'position')
    movement = child(pr, 'movement')
    status = child(pr, 'status')
    target_extent = child(pr, 'targetExtent')
    flight_info = child(pr, 'flightInfo')
    enhanced = child(pr, 'enhancedData')

    return {
        'full': pr.attrib.get('full') == 'true',
        'seq_num': to_int(text_of(child(pr, 'seqNum'))),
        'time': text_of(child(pr, 'time')),
        'track': to_int(text_of(child(pr, 'track'))),
        'stid': to_int(text_of(child(pr, 'stid'))),
        'flight_id': parse_flight_id(child(pr, 'flightId')),
        'flight_info': {
            'target_type': text_of(child(flight_info, 'tgtType')),
            'runway': text_of(child(flight_info, 'runway')),
        } if flight_info is not None else None,
        'position': {
            'x': to_int(text_of(child(position, 'x'))),
            'y': to_int(text_of(child(position, 'y'))),
            'latitude': to_float(text_of(child(position, 'latitude'))),
            'longitude': to_float(text_of(child(position, 'longitude'))),
            'altitude': to_float(text_of(child(position, 'altitude'))),
            'flight_level': to_float(text_of(child(position, 'flightLevel'))),
        } if position is not None else None,
        'movement': {
            'speed': to_float(text_of(child(movement, 'speed'))),
            'heading': to_float(text_of(child(movement, 'heading'))),
            'vx': to_float(text_of(child(movement, 'vx'))),
            'vy': to_float(text_of(child(movement, 'vy'))),
            'ax': to_float(text_of(child(movement, 'ax'))),
            'ay': to_float(text_of(child(movement, 'ay'))),
        } if movement is not None else None,
        'status': parse_simple_block(status) if status is not None else None,
        'target_extent': parse_simple_block(target_extent) if target_extent is not None else None,
        'plot_count': to_int(text_of(child(pr, 'plotCount'))),
        'enhanced_data': parse_simple_block(enhanced) if enhanced is not None else None,
    }


def parse_asdex(root: ET.Element) -> Dict[str, object]:
    reports = [elem for elem in list(root) if local_name(elem.tag) == 'positionReport']
    return {
        'payload_type': 'asdex_surface_movement_event',
        'airport': text_of(child(root, 'airport')),
        'report_count': len(reports),
        'reports': [parse_position_report(pr) for pr in reports],
    }


# --- Track record ---

def parse_record(root: ET.Element) -> Dict[str, object]:
    track = child(root, 'track')
    return {
        'payload_type': 'surveillance_track_record',
        'record': {
            'rec_seq_num': to_int(text_of(child(root, 'recSeqNum'))),
            'rec_src': text_of(child(root, 'recSrc')),
            'rec_type': to_int(text_of(child(root, 'recType'))),
            'rec_stars_timestamp': to_int(text_of(child(root, 'recSTARSTimestamp'))),
            'rec_stars_src_id': to_int(text_of(child(root, 'recSTARSSrcID'))),
            'rec_stars_time_sync': text_of(child(root, 'recSTARSTimeSync')),
            'rec_safa_receipt_time': text_of(child(root, 'recSAFAReceiptTime')),
            'rec_safa_time_sync': text_of(child(root, 'recSAFATimeSync')),
            'track': {
                'track_num': to_int(text_of(child(track, 'trackNum'))),
                'mrt_time': text_of(child(track, 'mrtTime')),
                'status': text_of(child(track, 'status')),
                'ac_address': text_of(child(track, 'acAddress')),
                'x_pos': to_int(text_of(child(track, 'xPos'))),
                'y_pos': to_int(text_of(child(track, 'yPos'))),
                'lat': to_float(text_of(child(track, 'lat'))),
                'lon': to_float(text_of(child(track, 'lon'))),
                'v_vert': to_int(text_of(child(track, 'vVert'))),
                'vx': to_int(text_of(child(track, 'vx'))),
                'vy': to_int(text_of(child(track, 'vy'))),
                'v_vert_raw': to_int(text_of(child(track, 'vVertRaw'))),
                'vx_raw': to_int(text_of(child(track, 'vxRaw'))),
                'vy_raw': to_int(text_of(child(track, 'vyRaw'))),
                'frozen': to_int(text_of(child(track, 'frozen'))),
                'new': to_int(text_of(child(track, 'new'))),
                'pseudo': to_int(text_of(child(track, 'pseudo'))),
                'adsb': to_int(text_of(child(track, 'adsb'))),
                'reported_beacon_code': text_of(child(track, 'reportedBeaconCode')),
                'reported_altitude': to_int(text_of(child(track, 'reportedAltitude'))),
            },
        },
    }


def detect_payload_type(root: ET.Element) -> str:
    name = local_name(root.tag)
    if name == 'MessageCollection':
        return 'sfdps_message_collection'
    if name == 'asdexMsg':
        return 'asdex_surface_movement_event'
    if name == 'record':
        return 'surveillance_track_record'
    return 'unknown'


def parse_faa_xml(xml_text: Union[str, bytes]) -> Dict[str, object]:
    root = ET.fromstring(xml_text if isinstance(xml_text, bytes) else xml_text.encode('utf-8'))
    payload_type = detect_payload_type(root)
    if payload_type == 'sfdps_message_collection':
        return parse_sfdps_collection(root)
    if payload_type == 'asdex_surface_movement_event':
        return parse_asdex(root)
    if payload_type == 'surveillance_track_record':
        return parse_record(root)
    return {
        'payload_type': 'unknown',
        'root_tag': local_name(root.tag),
        'children': [local_name(elem.tag) for elem in list(root)],
    }
