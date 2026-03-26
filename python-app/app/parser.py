from __future__ import annotations

from collections import Counter
from typing import Any, Dict, List, Optional, Tuple, Union
import xml.etree.ElementTree as ET


def split_tag(tag: str) -> Tuple[Optional[str], str]:
    if tag.startswith('{'):
        ns, local = tag[1:].split('}', 1)
        return ns, local
    return None, tag


def local_name(tag: str) -> str:
    return split_tag(tag)[1]


def strip_ns_dict(raw: Dict[str, str]) -> Dict[str, str]:
    return {local_name(key): value for key, value in raw.items()}


def is_nil_element(elem: Optional[ET.Element]) -> bool:
    if elem is None:
        return False
    for key, value in elem.attrib.items():
        if local_name(key) == 'nil' and value.lower() in {'true', '1'}:
            return True
    return False


def coerce_scalar(value: Optional[str]) -> object:
    if value is None:
        return None
    lowered = value.lower()
    if lowered in {'true', 'false'}:
        return lowered == 'true'
    maybe_int = to_int(value)
    if maybe_int is not None and str(maybe_int) == value:
        return maybe_int
    maybe_float = to_float(value)
    if maybe_float is not None and any(ch in value for ch in {'.', 'e', 'E'}):
        return maybe_float
    return value


def parse_xml_node(elem: Optional[ET.Element]) -> object:
    if elem is None:
        return None

    attrs = strip_ns_dict(dict(elem.attrib))
    text = text_of(elem)
    groups: Dict[str, List[ET.Element]] = {}
    for child_elem in list(elem):
        groups.setdefault(local_name(child_elem.tag), []).append(child_elem)

    if not groups:
        value: object = None if is_nil_element(elem) else coerce_scalar(text)
        if attrs:
            return {
                'attributes': attrs,
                'value': value,
            }
        return value

    out: Dict[str, object] = {}
    if attrs:
        out['attributes'] = attrs
    if text is not None:
        out['text'] = coerce_scalar(text)
    if is_nil_element(elem):
        out['nil'] = True

    for name, elems in groups.items():
        values = [parse_xml_node(child_elem) for child_elem in elems]
        out[name] = values[0] if len(values) == 1 else values
    return out


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


def parse_fdps_properties(properties: Optional[ET.Element]) -> Dict[str, object]:
    out: Dict[str, object] = {}
    if properties is None:
        return out
    for elem in list(properties):
        name = local_name(elem.tag)
        if list(elem):
            out[name] = parse_xml_node(elem)
        elif is_nil_element(elem):
            out[name] = None
        else:
            out[name] = coerce_scalar(text_of(elem))
    return out


def parse_fdps_msg_times(msg_times: Optional[ET.Element]) -> Dict[str, object]:
    out: Dict[str, object] = {}
    if msg_times is None:
        return out
    for elem in list(msg_times):
        name = local_name(elem.tag)
        if list(elem):
            out[name] = parse_xml_node(elem)
        elif is_nil_element(elem):
            out[name] = None
        else:
            out[name] = coerce_scalar(text_of(elem))
    return out


def parse_fdps_status(status: Optional[ET.Element]) -> Optional[Dict[str, object]]:
    if status is None:
        return None

    out: Dict[str, object] = {}
    artcc_items: List[Dict[str, object]] = []

    for elem in list(status):
        name = local_name(elem.tag)
        if name == 'artcc':
            artcc_items.append(
                {
                    'center': text_of(elem),
                    'state': strip_ns_dict(dict(elem.attrib)).get('state'),
                }
            )
            continue

        if list(elem):
            out[name] = parse_xml_node(elem)
        elif is_nil_element(elem):
            out[name] = None
        else:
            out[name] = coerce_scalar(text_of(elem))

    if artcc_items:
        out['artcc'] = artcc_items
        out['artcc_count'] = len(artcc_items)
        state_counts = Counter((item.get('state') or 'unknown') for item in artcc_items)
        out['artcc_state_counts'] = dict(state_counts)

    return out


def parse_fdps_hs(hs: Optional[ET.Element]) -> Optional[Dict[str, object]]:
    if hs is None:
        return None

    out: Dict[str, object] = {}
    for elem in list(hs):
        name = local_name(elem.tag)
        if list(elem):
            out[name] = parse_xml_node(elem)
        elif is_nil_element(elem):
            out[name] = None
        else:
            out[name] = coerce_scalar(text_of(elem))
    return out


def parse_fdps_msg(root: ET.Element) -> Dict[str, object]:
    properties = parse_fdps_properties(child(root, 'properties'))
    message_type = properties.get('propMessageType')
    status = parse_fdps_status(child(root, 'status'))
    hs = parse_fdps_hs(child(root, 'HS'))
    variant = 'status'
    if isinstance(message_type, str) and message_type:
        variant = message_type.lower()
    elif hs is not None:
        variant = 'hs'

    known = {'properties', 'center', 'msgTimes', 'status', 'HS'}
    extras: Dict[str, object] = {}
    for elem in list(root):
        name = local_name(elem.tag)
        if name in known:
            continue
        extras.setdefault(name, [])
        current = extras[name]
        if isinstance(current, list):
            current.append(parse_xml_node(elem))
    for key, value in list(extras.items()):
        if isinstance(value, list) and len(value) == 1:
            extras[key] = value[0]

    return {
        'payload_type': 'sfdps_fdps_status',
        'variant': variant,
        'center': text_of(child(root, 'center')),
        'properties': properties,
        'message_type': message_type,
        'data_type': properties.get('propDataType'),
        'msg_times': parse_fdps_msg_times(child(root, 'msgTimes')),
        'status': status,
        'hs': hs,
        'extras': extras,
    }


def _unique_values(values: List[str]) -> List[str]:
    seen: set[str] = set()
    out: List[str] = []
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        out.append(value)
    return out


def _texts_by_local_name(root: ET.Element, tag_name: str) -> List[str]:
    values: List[str] = []
    for elem in root.iter():
        if local_name(elem.tag) != tag_name:
            continue
        value = text_of(elem)
        if value is not None:
            values.append(value)
    return values


def parse_aixm_member(has_member: ET.Element) -> Dict[str, object]:
    member_children = list(has_member)
    if not member_children:
        return {
            'entity_type': None,
            'entity': None,
            'summary': {},
        }

    entity = member_children[0]
    entity_type = local_name(entity.tag)
    entity_attributes = strip_ns_dict(dict(entity.attrib))
    summary: Dict[str, object] = {
        'id': entity_attributes.get('id'),
    }

    interpretations = _texts_by_local_name(entity, 'interpretation')
    if interpretations:
        summary['interpretations'] = _unique_values(interpretations)

    if entity_type == 'Airspace':
        designators = _texts_by_local_name(entity, 'designator')
        if designators:
            summary['designators'] = _unique_values(designators)
        fav_numbers = _texts_by_local_name(entity, 'FAVnumber')
        if fav_numbers:
            summary['fav_numbers'] = _unique_values(fav_numbers)

    if entity_type == 'RouteSegment':
        availability = _texts_by_local_name(entity, 'availability')
        if availability:
            summary['availability'] = _unique_values(availability)

    extension_counts = Counter(
        local_name(elem.tag)
        for elem in entity.iter()
        if local_name(elem.tag).endswith('Extension')
    )
    if extension_counts:
        summary['extension_counts'] = dict(extension_counts)

    return {
        'entity_type': entity_type,
        'entity': parse_xml_node(entity),
        'summary': summary,
    }


def parse_aixm_basic_message(root: ET.Element) -> Dict[str, object]:
    root_attrs = strip_ns_dict(dict(root.attrib))
    members = [parse_aixm_member(member) for member in children(root, 'hasMember')]

    member_type_counts = Counter(
        str(member.get('entity_type'))
        for member in members
        if member.get('entity_type') is not None
    )

    extension_counts: Counter[str] = Counter()
    for member in members:
        summary = member.get('summary')
        if not isinstance(summary, dict):
            continue
        nested = summary.get('extension_counts')
        if not isinstance(nested, dict):
            continue
        for key, value in nested.items():
            if isinstance(key, str) and isinstance(value, int):
                extension_counts[key] += value

    known = {'name', 'boundedBy', 'hasMember'}
    extras: Dict[str, object] = {}
    for elem in list(root):
        name = local_name(elem.tag)
        if name in known:
            continue
        extras.setdefault(name, [])
        current = extras[name]
        if isinstance(current, list):
            current.append(parse_xml_node(elem))
    for key, value in list(extras.items()):
        if isinstance(value, list) and len(value) == 1:
            extras[key] = value[0]

    return {
        'payload_type': 'sfdps_aixm_basic_message',
        'id': root_attrs.get('id'),
        'name': text_of(child(root, 'name')),
        'attributes': root_attrs,
        'bounded_by': parse_xml_node(child(root, 'boundedBy')),
        'member_count': len(members),
        'member_type_counts': dict(member_type_counts),
        'extension_counts': dict(extension_counts),
        'members': members,
        'extras': extras,
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
    if name == 'FDPSMsg':
        return 'sfdps_fdps_status'
    if name == 'AIXMBasicMessage':
        return 'sfdps_aixm_basic_message'
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
    if payload_type == 'sfdps_fdps_status':
        return parse_fdps_msg(root)
    if payload_type == 'sfdps_aixm_basic_message':
        return parse_aixm_basic_message(root)
    if payload_type == 'asdex_surface_movement_event':
        return parse_asdex(root)
    if payload_type == 'surveillance_track_record':
        return parse_record(root)
    return {
        'payload_type': 'unknown',
        'root_tag': local_name(root.tag),
        'children': [local_name(elem.tag) for elem in list(root)],
    }
