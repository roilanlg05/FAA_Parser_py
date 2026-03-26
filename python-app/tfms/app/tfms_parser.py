from __future__ import annotations

import json
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from typing import Any, Dict, Iterable, Iterator, List, Optional, Tuple, Union


# ------------------------------------------------------------
# XML helpers
# ------------------------------------------------------------


def split_tag(tag: str) -> Tuple[Optional[str], str]:
    if tag.startswith("{"):
        ns, local = tag[1:].split("}", 1)
        return ns, local
    return None, tag



def local_name(tag: str) -> str:
    return split_tag(tag)[1]



def ns_name(tag: str) -> Optional[str]:
    return split_tag(tag)[0]



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



def first_child(parent: Optional[ET.Element]) -> Optional[ET.Element]:
    if parent is None:
        return None
    elems = list(parent)
    return elems[0] if elems else None



def text_of(elem: Optional[ET.Element]) -> Optional[str]:
    if elem is None or elem.text is None:
        return None
    value = elem.text.strip()
    return value or None



def to_int(value: Optional[str]) -> Optional[int]:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except Exception:
        return None



def to_float(value: Optional[str]) -> Optional[float]:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except Exception:
        return None



def to_bool(value: Optional[str]) -> Optional[bool]:
    if value is None:
        return None
    lowered = value.strip().lower()
    if lowered in {"true", "1", "yes", "y"}:
        return True
    if lowered in {"false", "0", "no", "n"}:
        return False
    return None



def strip_ns_dict(raw: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for key, value in raw.items():
        if key.startswith("{"):
            _, local = split_tag(key)
            out[local] = value
        else:
            out[key] = value
    return out



def parse_latlon_pos(pos_text: Optional[str]) -> Tuple[Optional[float], Optional[float]]:
    if not pos_text:
        return None, None
    parts = pos_text.split()
    if len(parts) != 2:
        return None, None
    return to_float(parts[0]), to_float(parts[1])



def parse_dms_value(elem: Optional[ET.Element]) -> Optional[Dict[str, Any]]:
    if elem is None:
        return None
    attrs = strip_ns_dict(dict(elem.attrib))
    degrees = to_float(attrs.get("degrees"))
    minutes = to_float(attrs.get("minutes")) or 0.0
    seconds = to_float(attrs.get("seconds")) or 0.0
    direction = attrs.get("direction")
    decimal = None
    if degrees is not None:
        decimal = degrees + minutes / 60.0 + seconds / 3600.0
        if direction in {"SOUTH", "WEST"}:
            decimal = -decimal
    return {
        "raw": attrs,
        "decimal": decimal,
        "degrees": degrees,
        "minutes": minutes,
        "seconds": seconds,
        "direction": direction,
    }



def parse_position_dms(position_elem: Optional[ET.Element]) -> Optional[Dict[str, Any]]:
    if position_elem is None:
        return None
    lat = parse_dms_value(first_child(child(position_elem, "latitude")))
    lon = parse_dms_value(first_child(child(position_elem, "longitude")))
    return {
        "latitude": lat,
        "longitude": lon,
        "latitude_decimal": lat.get("decimal") if lat else None,
        "longitude_decimal": lon.get("decimal") if lon else None,
    }



def parse_simple_altitude_text(value: Optional[str]) -> Optional[Dict[str, Any]]:
    if value is None:
        return None
    raw = value.strip()
    suffix = raw[-1] if raw and raw[-1].isalpha() else None
    digits = raw[:-1] if suffix else raw
    numeric = to_int(digits)
    altitude_feet = None
    if numeric is not None:
        # In TFMS samples simpleAltitude values like 350C correspond to FL350.
        altitude_feet = numeric * 100
    return {
        "raw": raw,
        "numeric": numeric,
        "suffix": suffix,
        "altitude_feet_estimate": altitude_feet,
    }



def element_to_lossless_json(elem: ET.Element) -> Dict[str, Any]:
    ns, local = split_tag(elem.tag)
    node: Dict[str, Any] = {
        "tag": local,
        "namespace": ns,
        "attributes": strip_ns_dict(dict(elem.attrib)),
        "attributes_raw": dict(elem.attrib),
        "text": text_of(elem),
        "children": [],
    }
    for child_elem in list(elem):
        node["children"].append(element_to_lossless_json(child_elem))
    return node



def indexed_children(parent: Optional[ET.Element]) -> Dict[str, List[ET.Element]]:
    out: Dict[str, List[ET.Element]] = {}
    if parent is None:
        return out
    for elem in list(parent):
        out.setdefault(local_name(elem.tag), []).append(elem)
    return out


def parse_xml_node(elem: Optional[ET.Element]) -> Any:
    if elem is None:
        return None

    attrs = strip_ns_dict(dict(elem.attrib))
    groups = indexed_children(elem)
    text = text_of(elem)

    if not groups:
        if attrs:
            out: Dict[str, Any] = {"attributes": attrs}
            if text is not None:
                out["value"] = text
            return out
        return text

    out: Dict[str, Any] = {}
    if attrs:
        out["attributes"] = attrs
    if text is not None:
        out["text"] = text

    for name, elems in groups.items():
        values = [parse_xml_node(x) for x in elems]
        out[name] = values[0] if len(values) == 1 else values

    return out


# ------------------------------------------------------------
# TFMS known blocks
# ------------------------------------------------------------


@dataclass
class ParsedTfmsPayload:
    payload_type: str
    parsed: Dict[str, Any]



def parse_computer_id(elem: Optional[ET.Element]) -> Optional[Dict[str, Any]]:
    if elem is None:
        return None
    return {
        "facilityIdentifier": text_of(child(elem, "facilityIdentifier")),
        "idNumber": text_of(child(elem, "idNumber")),
        "raw": element_to_lossless_json(elem),
    }



def parse_airport_point(elem: Optional[ET.Element]) -> Optional[Dict[str, Any]]:
    if elem is None:
        return None
    return {
        "airport": text_of(child(elem, "airport")),
        "raw": element_to_lossless_json(elem),
    }



def parse_qualified_aircraft_id(elem: Optional[ET.Element]) -> Optional[Dict[str, Any]]:
    if elem is None:
        return None
    attrs = strip_ns_dict(dict(elem.attrib))
    return {
        "attributes": attrs,
        "aircraftId": text_of(child(elem, "aircraftId")),
        "computerId": parse_computer_id(child(elem, "computerId")),
        "gufi": text_of(child(elem, "gufi")),
        "igtd": text_of(child(elem, "igtd")),
        "departurePoint": parse_airport_point(child(elem, "departurePoint")),
        "arrivalPoint": parse_airport_point(child(elem, "arrivalPoint")),
        "raw": element_to_lossless_json(elem),
    }



def parse_fix_like(elem: ET.Element) -> Dict[str, Any]:
    attrs = strip_ns_dict(dict(elem.attrib))
    return {
        "tag": local_name(elem.tag),
        "value": text_of(elem),
        "sequenceNumber": to_int(attrs.get("sequenceNumber")),
        "elapsedTime": to_int(attrs.get("elapsedTime")),
        "elapsedEntryTime": to_int(attrs.get("elapsedEntryTime")),
        "latitudeDecimal": to_float(attrs.get("latitudeDecimal")),
        "longitudeDecimal": to_float(attrs.get("longitudeDecimal")),
        "attributes": attrs,
        "raw": element_to_lossless_json(elem),
    }



def parse_flight_traversal_data2(elem: Optional[ET.Element]) -> Optional[Dict[str, Any]]:
    if elem is None:
        return None
    groups = indexed_children(elem)
    known_names = ["fix", "waypoint", "airway", "center", "sector"]
    parsed: Dict[str, Any] = {
        "fixes": [parse_fix_like(x) for x in groups.get("fix", [])],
        "waypoints": [parse_fix_like(x) for x in groups.get("waypoint", [])],
        "airways": [parse_fix_like(x) for x in groups.get("airway", [])],
        "centers": [parse_fix_like(x) for x in groups.get("center", [])],
        "sectors": [parse_fix_like(x) for x in groups.get("sector", [])],
        "extras": {},
        "raw": element_to_lossless_json(elem),
    }
    for name, elems in groups.items():
        if name not in known_names:
            parsed["extras"][name] = [element_to_lossless_json(x) for x in elems]
    return parsed



def parse_eta_etd(elem: ET.Element) -> Dict[str, Any]:
    attrs = strip_ns_dict(dict(elem.attrib))
    return {
        "attributes": attrs,
        "timeValue": attrs.get("timeValue"),
        "type": attrs.get("etaType") or attrs.get("etdType"),
        "raw": element_to_lossless_json(elem),
    }



def parse_arrival_or_departure_fix_time(elem: ET.Element) -> Dict[str, Any]:
    attrs = strip_ns_dict(dict(elem.attrib))
    return {
        "fixName": attrs.get("fixName"),
        "arrTime": attrs.get("arrTime"),
        "raw": element_to_lossless_json(elem),
    }



def parse_rvsm(elem: Optional[ET.Element]) -> Optional[Dict[str, Any]]:
    if elem is None:
        return None
    attrs = strip_ns_dict(dict(elem.attrib))
    return {
        "currentCompliance": to_bool(attrs.get("currentCompliance")),
        "equipped": to_bool(attrs.get("equipped")),
        "futureCompliance": to_bool(attrs.get("futureCompliance")),
        "attributes": attrs,
        "raw": element_to_lossless_json(elem),
    }



def parse_next_event_like(elem: Optional[ET.Element]) -> Optional[Dict[str, Any]]:
    if elem is None:
        return None
    attrs = strip_ns_dict(dict(elem.attrib))
    return {
        "latitudeDecimal": to_float(attrs.get("latitudeDecimal")),
        "longitudeDecimal": to_float(attrs.get("longitudeDecimal")),
        "attributes": attrs,
        "raw": element_to_lossless_json(elem),
    }



def parse_reported_altitude(elem: Optional[ET.Element]) -> Optional[Dict[str, Any]]:
    if elem is None:
        return None
    assigned_altitude = child(elem, "assignedAltitude")
    simple_altitude = text_of(child(assigned_altitude, "simpleAltitude")) if assigned_altitude is not None else None
    return {
        "assignedAltitude": {
            "simpleAltitude": parse_simple_altitude_text(simple_altitude),
            "raw": element_to_lossless_json(assigned_altitude) if assigned_altitude is not None else None,
        } if assigned_altitude is not None else None,
        "raw": element_to_lossless_json(elem),
    }



def parse_track_or_route_data(elem: Optional[ET.Element]) -> Optional[Dict[str, Any]]:
    if elem is None:
        return None
    out: Dict[str, Any] = {
        "tag": local_name(elem.tag),
        "eta": [parse_eta_etd(x) for x in children(elem, "eta")],
        "etd": [parse_eta_etd(x) for x in children(elem, "etd")],
        "rvsmData": parse_rvsm(child(elem, "rvsmData")),
        "arrivalFixAndTime": [parse_arrival_or_departure_fix_time(x) for x in children(elem, "arrivalFixAndTime")],
        "departureFixAndTime": [parse_arrival_or_departure_fix_time(x) for x in children(elem, "departureFixAndTime")],
        "nextEvent": parse_next_event_like(child(elem, "nextEvent")),
        "nextPosition": parse_next_event_like(child(elem, "nextPosition")),
        "star": [
            {
                "attributes": strip_ns_dict(dict(x.attrib)),
                "raw": element_to_lossless_json(x),
            }
            for x in children(elem, "star")
        ],
        "starTransitionFix": [text_of(x) for x in children(elem, "starTransitionFix")],
        "routeOfFlight": text_of(child(elem, "routeOfFlight")),
        "diversionIndicator": text_of(child(elem, "diversionIndicator")),
        "flightTraversalData2": parse_flight_traversal_data2(child(elem, "flightTraversalData2")),
        "extras": {},
        "raw": element_to_lossless_json(elem),
    }
    handled = {
        "eta",
        "etd",
        "rvsmData",
        "arrivalFixAndTime",
        "departureFixAndTime",
        "nextEvent",
        "nextPosition",
        "star",
        "starTransitionFix",
        "routeOfFlight",
        "diversionIndicator",
        "flightTraversalData2",
    }
    for name, elems in indexed_children(elem).items():
        if name not in handled:
            out["extras"][name] = [element_to_lossless_json(x) for x in elems]
    return out



def parse_track_information(elem: Optional[ET.Element]) -> Optional[Dict[str, Any]]:
    if elem is None:
        return None
    return {
        "qualifiedAircraftId": parse_qualified_aircraft_id(child(elem, "qualifiedAircraftId")),
        "speed": to_int(text_of(child(elem, "speed"))),
        "reportedAltitude": parse_reported_altitude(child(elem, "reportedAltitude")),
        "position": parse_position_dms(child(elem, "position")),
        "timeAtPosition": text_of(child(elem, "timeAtPosition")),
        "ncsmTrackData": parse_track_or_route_data(child(elem, "ncsmTrackData")),
        "ncsmRouteData": parse_track_or_route_data(child(elem, "ncsmRouteData")),
        "raw": element_to_lossless_json(elem),
    }



def parse_aircraft_specification(elem: Optional[ET.Element]) -> Optional[Dict[str, Any]]:
    if elem is None:
        return None
    attrs = strip_ns_dict(dict(elem.attrib))
    return {
        "value": text_of(elem),
        "attributes": attrs,
        "raw": element_to_lossless_json(elem),
    }


def parse_speed(elem: Optional[ET.Element]) -> Optional[Dict[str, Any]]:
    if elem is None:
        return None

    attrs = strip_ns_dict(dict(elem.attrib))
    out: Dict[str, Any] = {
        "attributes": attrs,
        "value": text_of(elem),
        "filedTrueAirSpeed": to_int(text_of(child(elem, "filedTrueAirSpeed"))),
        "raw": element_to_lossless_json(elem),
    }
    extras: Dict[str, Any] = {}
    for name, elems in indexed_children(elem).items():
        if name == "filedTrueAirSpeed":
            continue
        values = [parse_xml_node(x) for x in elems]
        extras[name] = values[0] if len(values) == 1 else values
    if extras:
        out["extras"] = extras
    return out


def parse_altitude(elem: Optional[ET.Element]) -> Optional[Dict[str, Any]]:
    if elem is None:
        return None

    attrs = strip_ns_dict(dict(elem.attrib))
    out: Dict[str, Any] = {
        "attributes": attrs,
        "value": text_of(elem),
        "simpleAltitude": parse_simple_altitude_text(text_of(child(elem, "simpleAltitude"))),
        "raw": element_to_lossless_json(elem),
    }
    extras: Dict[str, Any] = {}
    for name, elems in indexed_children(elem).items():
        if name == "simpleAltitude":
            continue
        values = [parse_xml_node(x) for x in elems]
        extras[name] = values[0] if len(values) == 1 else values
    if extras:
        out["extras"] = extras
    return out


def parse_position_data(elem: Optional[ET.Element]) -> Optional[Dict[str, Any]]:
    if elem is None:
        return None

    out: Dict[str, Any] = {
        "position": parse_position_dms(child(elem, "position")),
        "altitude": parse_altitude(child(elem, "altitude")),
        "time": text_of(child(elem, "time")),
        "extras": {},
        "raw": element_to_lossless_json(elem),
    }

    known = {"position", "altitude", "time"}
    for name, elems in indexed_children(elem).items():
        if name not in known:
            values = [parse_xml_node(x) for x in elems]
            out["extras"][name] = values[0] if len(values) == 1 else values

    return out


def parse_amendment_data(elem: Optional[ET.Element]) -> Optional[Dict[str, Any]]:
    if elem is None:
        return None

    out: Dict[str, Any] = {
        "newFlightAircraftSpecs": parse_aircraft_specification(child(elem, "newFlightAircraftSpecs")),
        "newRouteOfFlight": text_of(child(elem, "newRouteOfFlight")),
        "newCoordinationPoint": text_of(child(elem, "newCoordinationPoint")),
        "newCoordinationTime": text_of(child(elem, "newCoordinationTime")),
        "newSpeed": parse_speed(child(elem, "newSpeed")),
        "newAltitude": parse_altitude(child(elem, "newAltitude")),
        "extras": {},
        "raw": element_to_lossless_json(elem),
    }

    known = {
        "newFlightAircraftSpecs",
        "newRouteOfFlight",
        "newCoordinationPoint",
        "newCoordinationTime",
        "newSpeed",
        "newAltitude",
    }
    for name, elems in indexed_children(elem).items():
        if name not in known:
            values = [parse_xml_node(x) for x in elems]
            out["extras"][name] = values[0] if len(values) == 1 else values

    return out


def parse_diversion_cancel_data(elem: Optional[ET.Element]) -> Optional[Dict[str, Any]]:
    if elem is None:
        return None

    out: Dict[str, Any] = {
        "canceledFlightReference": text_of(child(elem, "canceledFlightReference")),
        "newFlightReference": text_of(child(elem, "newFlightReference")),
        "extras": {},
        "raw": element_to_lossless_json(elem),
    }

    known = {"canceledFlightReference", "newFlightReference"}
    for name, elems in indexed_children(elem).items():
        if name not in known:
            values = [parse_xml_node(x) for x in elems]
            out["extras"][name] = values[0] if len(values) == 1 else values

    return out


def parse_generic_flight_body(elem: Optional[ET.Element]) -> Optional[Dict[str, Any]]:
    if elem is None:
        return None

    groups = indexed_children(elem)
    out: Dict[str, Any] = {
        "tag": local_name(elem.tag),
        "qualifiedAircraftId": parse_qualified_aircraft_id(child(elem, "qualifiedAircraftId")),
        "flightStatusAndSpec": parse_flight_status_and_spec(child(elem, "flightStatusAndSpec")),
        "flightAircraftSpecs": parse_aircraft_specification(child(elem, "flightAircraftSpecs")),
        "airlineData": parse_airline_data(child(elem, "airlineData")),
        "flightTimeData": parse_flight_time_data(child(elem, "flightTimeData")),
        "ncsmFlightTimeData": parse_flight_time_data(child(elem, "ncsmFlightTimeData")),
        "ncsmTrackData": parse_track_or_route_data(child(elem, "ncsmTrackData")),
        "ncsmRouteData": parse_track_or_route_data(child(elem, "ncsmRouteData")),
        "flightTraversalData2": parse_flight_traversal_data2(child(elem, "flightTraversalData2")),
        "arrivalFixAndTime": [parse_arrival_or_departure_fix_time(x) for x in groups.get("arrivalFixAndTime", [])],
        "departureFixAndTime": [parse_arrival_or_departure_fix_time(x) for x in groups.get("departureFixAndTime", [])],
        "plannedPositionData": parse_position_data(child(elem, "plannedPositionData")),
        "reportedPositionData": parse_position_data(child(elem, "reportedPositionData")),
        "position": parse_position_dms(child(elem, "position")),
        "boundaryPosition": parse_position_dms(child(elem, "boundaryPosition")),
        "reportedAltitude": parse_reported_altitude(child(elem, "reportedAltitude")),
        "altitude": parse_altitude(child(elem, "altitude")),
        "speed": parse_speed(child(elem, "speed")),
        "amendmentData": parse_amendment_data(child(elem, "amendmentData")),
        "diversionCancelData": parse_diversion_cancel_data(child(elem, "diversionCancelData")),
        "newAircraftId": text_of(child(elem, "newAircraftId")),
        "routeOfFlight": text_of(child(elem, "routeOfFlight")),
        "coordinationPoint": text_of(child(elem, "coordinationPoint")),
        "coordinationTime": text_of(child(elem, "coordinationTime")),
        "timeOfDeparture": text_of(child(elem, "timeOfDeparture")),
        "timeOfArrival": text_of(child(elem, "timeOfArrival")),
        "timeAtPosition": text_of(child(elem, "timeAtPosition")),
        "extras": {},
        "raw": element_to_lossless_json(elem),
    }

    handled = {
        "qualifiedAircraftId",
        "flightStatusAndSpec",
        "flightAircraftSpecs",
        "airlineData",
        "flightTimeData",
        "ncsmFlightTimeData",
        "ncsmTrackData",
        "ncsmRouteData",
        "flightTraversalData2",
        "arrivalFixAndTime",
        "departureFixAndTime",
        "plannedPositionData",
        "reportedPositionData",
        "position",
        "boundaryPosition",
        "reportedAltitude",
        "altitude",
        "speed",
        "amendmentData",
        "diversionCancelData",
        "newAircraftId",
        "routeOfFlight",
        "coordinationPoint",
        "coordinationTime",
        "timeOfDeparture",
        "timeOfArrival",
        "timeAtPosition",
    }

    for name, elems in groups.items():
        if name in handled:
            continue
        values = [parse_xml_node(x) for x in elems]
        out["extras"][name] = values[0] if len(values) == 1 else values

    return out



def parse_flight_status_and_spec(elem: Optional[ET.Element]) -> Optional[Dict[str, Any]]:
    if elem is None:
        return None
    return {
        "flightStatus": text_of(child(elem, "flightStatus")),
        "aircraftModel": text_of(child(elem, "aircraftModel")),
        "aircraftSpecification": parse_aircraft_specification(child(elem, "aircraftSpecification")),
        "extras": {
            name: [element_to_lossless_json(x) for x in elems]
            for name, elems in indexed_children(elem).items()
            if name not in {"flightStatus", "aircraftModel", "aircraftSpecification"}
        },
        "raw": element_to_lossless_json(elem),
    }



def parse_flight_time_data(elem: Optional[ET.Element]) -> Optional[Dict[str, Any]]:
    if elem is None:
        return None
    attrs = strip_ns_dict(dict(elem.attrib))
    return {
        "attributes": attrs,
        "airlineInTime": attrs.get("airlineInTime"),
        "airlineOffTime": attrs.get("airlineOffTime"),
        "airlineOnTime": attrs.get("airlineOnTime"),
        "airlineOutTime": attrs.get("airlineOutTime"),
        "flightCreation": attrs.get("flightCreation"),
        "originalArrival": attrs.get("originalArrival"),
        "originalDeparture": attrs.get("originalDeparture"),
        "raw": element_to_lossless_json(elem),
    }



def parse_airline_data(elem: Optional[ET.Element]) -> Optional[Dict[str, Any]]:
    if elem is None:
        return None
    return {
        "flightStatusAndSpec": parse_flight_status_and_spec(child(elem, "flightStatusAndSpec")),
        "eta": [parse_eta_etd(x) for x in children(elem, "eta")],
        "etd": [parse_eta_etd(x) for x in children(elem, "etd")],
        "flightTimeData": parse_flight_time_data(child(elem, "flightTimeData")),
        "diversionIndicator": text_of(child(elem, "diversionIndicator")),
        "rvsmData": parse_rvsm(child(elem, "rvsmData")),
        "extras": {
            name: [element_to_lossless_json(x) for x in elems]
            for name, elems in indexed_children(elem).items()
            if name not in {"flightStatusAndSpec", "eta", "etd", "flightTimeData", "diversionIndicator", "rvsmData"}
        },
        "raw": element_to_lossless_json(elem),
    }



def parse_flight_modify(elem: Optional[ET.Element]) -> Optional[Dict[str, Any]]:
    if elem is None:
        return None
    return {
        "qualifiedAircraftId": parse_qualified_aircraft_id(child(elem, "qualifiedAircraftId")),
        "airlineData": parse_airline_data(child(elem, "airlineData")),
        "extras": {
            name: [element_to_lossless_json(x) for x in elems]
            for name, elems in indexed_children(elem).items()
            if name not in {"qualifiedAircraftId", "airlineData"}
        },
        "raw": element_to_lossless_json(elem),
    }



def parse_flight_times(elem: Optional[ET.Element]) -> Optional[Dict[str, Any]]:
    if elem is None:
        return None
    return {
        "qualifiedAircraftId": parse_qualified_aircraft_id(child(elem, "qualifiedAircraftId")),
        "flightStatusAndSpec": parse_flight_status_and_spec(child(elem, "flightStatusAndSpec")),
        "etd": [parse_eta_etd(x) for x in children(elem, "etd")],
        "eta": [parse_eta_etd(x) for x in children(elem, "eta")],
        "rvsmData": parse_rvsm(child(elem, "rvsmData")),
        "arrivalFixAndTime": [parse_arrival_or_departure_fix_time(x) for x in children(elem, "arrivalFixAndTime")],
        "departureFixAndTime": [parse_arrival_or_departure_fix_time(x) for x in children(elem, "departureFixAndTime")],
        "extras": {
            name: [element_to_lossless_json(x) for x in elems]
            for name, elems in indexed_children(elem).items()
            if name not in {"qualifiedAircraftId", "flightStatusAndSpec", "etd", "eta", "rvsmData", "arrivalFixAndTime", "departureFixAndTime"}
        },
        "raw": element_to_lossless_json(elem),
    }



def parse_fltd_message(elem: ET.Element) -> Dict[str, Any]:
    attrs = strip_ns_dict(dict(elem.attrib))
    msg_type = attrs.get("msgType")
    parsed: Dict[str, Any] = {
        "attributes": attrs,
        "msgType": msg_type,
        "flightRef": attrs.get("flightRef"),
        "acid": attrs.get("acid"),
        "sourceFacility": attrs.get("sourceFacility"),
        "sourceTimeStamp": attrs.get("sourceTimeStamp"),
        "body": None,
        "raw": element_to_lossless_json(elem),
    }
    if msg_type == "trackInformation":
        parsed["body"] = parse_track_information(child(elem, "trackInformation"))
    elif msg_type == "FlightSectors":
        body = child(elem, "ncsmFlightSectors")
        parsed["body"] = {
            "qualifiedAircraftId": parse_qualified_aircraft_id(child(body, "qualifiedAircraftId")),
            "flightTraversalData2": parse_flight_traversal_data2(child(body, "flightTraversalData2")),
            "raw": element_to_lossless_json(body) if body is not None else None,
        }
    elif msg_type == "FlightModify":
        parsed["body"] = parse_flight_modify(child(elem, "ncsmFlightModify"))
    elif msg_type == "FlightTimes":
        parsed["body"] = parse_flight_times(child(elem, "ncsmFlightTimes"))
    else:
        first = first_child(elem)
        parsed["body"] = parse_generic_flight_body(first)
    return parsed



def parse_fltd_output(elem: ET.Element) -> Dict[str, Any]:
    messages = children(elem, "fltdMessage")
    return {
        "outputTag": local_name(elem.tag),
        "messageCount": len(messages),
        "messages": [parse_fltd_message(msg) for msg in messages],
        "raw": element_to_lossless_json(elem),
    }



def parse_name_value_elem(elem: ET.Element) -> Dict[str, Any]:
    attrs = strip_ns_dict(dict(elem.attrib))
    return {"attributes": attrs, "text": text_of(elem), "raw": element_to_lossless_json(elem)}


def parse_generic_fi_block(elem: Optional[ET.Element]) -> Optional[Dict[str, Any]]:
    if elem is None:
        return None
    parsed = parse_xml_node(elem)
    if isinstance(parsed, dict):
        out = dict(parsed)
    else:
        out = {"value": parsed}
    out["tag"] = local_name(elem.tag)
    out["raw"] = element_to_lossless_json(elem)
    return out


def parse_restriction_message(elem: Optional[ET.Element]) -> Optional[Dict[str, Any]]:
    if elem is None:
        return None
    return {
        "eventTime": text_of(child(elem, "eventTime")),
        "entryTime": text_of(child(elem, "entryTime")),
        "facility": text_of(child(elem, "facility")),
        "action": to_int(text_of(child(elem, "action"))),
        "restrictionId": text_of(child(elem, "restrictionId")),
        "restrictedNasElements": text_of(child(elem, "restrictedNasElements")),
        "startTime": text_of(child(elem, "startTime")),
        "stopTime": text_of(child(elem, "stopTime")),
        "airports": text_of(child(elem, "airports")),
        "aircraftType": text_of(child(elem, "aircraftType")),
        "restrictionType": text_of(child(elem, "restrictionType")),
        "restrictionCategory": text_of(child(elem, "restrictionCategory")),
        "mitNumber": text_of(child(elem, "mitNumber")),
        "reasonText": text_of(child(elem, "reasonText")),
        "qualifier": text_of(child(elem, "qualifier")),
        "passback": text_of(child(elem, "passback")),
        "approvalTime": text_of(child(elem, "approvalTime")),
        "providerStatus": text_of(child(elem, "providerStatus")),
        "referenceRestrictionEndTime": text_of(child(elem, "referenceRestrictionEndTime")),
        "referenceRestrictionId": text_of(child(elem, "referenceRestrictionId")),
        "remarks": text_of(child(elem, "remarks")),
        "extras": {
            name: [element_to_lossless_json(x) for x in elems]
            for name, elems in indexed_children(elem).items()
            if name
            not in {
                "eventTime",
                "entryTime",
                "facility",
                "action",
                "restrictionId",
                "restrictedNasElements",
                "startTime",
                "stopTime",
                "airports",
                "aircraftType",
                "restrictionType",
                "restrictionCategory",
                "mitNumber",
                "reasonText",
                "qualifier",
                "passback",
                "approvalTime",
                "providerStatus",
                "referenceRestrictionEndTime",
                "referenceRestrictionId",
                "remarks",
            }
        },
        "raw": element_to_lossless_json(elem),
    }



def parse_flight_block(elem: Optional[ET.Element]) -> Optional[Dict[str, Any]]:
    if elem is None:
        return None
    return {
        "aircraftId": text_of(child(elem, "aircraftId")),
        "gufi": text_of(child(elem, "gufi")),
        "igtd": text_of(child(elem, "igtd")),
        "departurePoint": parse_airport_point(child(elem, "departurePoint")),
        "arrivalPoint": parse_airport_point(child(elem, "arrivalPoint")),
        "raw": element_to_lossless_json(elem),
    }



def parse_fxa_id(elem: Optional[ET.Element]) -> Optional[Dict[str, Any]]:
    if elem is None:
        return None
    return {
        "fcaId": text_of(child(elem, "fcaId")),
        "fcaName": text_of(child(elem, "fcaName")),
        "lastUpdate": text_of(child(elem, "lastUpdate")),
        "raw": element_to_lossless_json(elem),
    }



def parse_fxa_flight(elem: Optional[ET.Element]) -> Optional[Dict[str, Any]]:
    if elem is None:
        return None
    out = {
        "fxaId": parse_fxa_id(child(elem, "fxaId")),
        "bentryTm": text_of(child(elem, "bentryTm")),
        "createTm": text_of(child(elem, "createTm")),
        "eentryTm": text_of(child(elem, "eentryTm")),
        "entryTm": text_of(child(elem, "entryTm")),
        "exitTm": text_of(child(elem, "exitTm")),
        "extendedExitTm": text_of(child(elem, "extendedExitTm")),
        "ientryTm": text_of(child(elem, "ientryTm")),
        "oentryTm": text_of(child(elem, "oentryTm")),
        "entryLat": to_float(text_of(child(elem, "entryLat"))),
        "entryLon": to_float(text_of(child(elem, "entryLon"))),
        "entryHeading": to_int(text_of(child(elem, "entryHeading"))),
        "exitInd": text_of(child(elem, "exitInd")),
        "extras": {},
        "raw": element_to_lossless_json(elem),
    }
    known = {
        "fxaId",
        "bentryTm",
        "createTm",
        "eentryTm",
        "entryTm",
        "exitTm",
        "extendedExitTm",
        "ientryTm",
        "oentryTm",
        "entryLat",
        "entryLon",
        "entryHeading",
        "exitInd",
    }
    for name, elems in indexed_children(elem).items():
        if name not in known:
            out["extras"][name] = [element_to_lossless_json(x) for x in elems]
    return out



def parse_tmi(elem: ET.Element) -> Dict[str, Any]:
    attrs = strip_ns_dict(dict(elem.attrib))
    return {
        "attributes": attrs,
        "fcaId": text_of(child(elem, "fcaId")),
        "raw": element_to_lossless_json(elem),
    }



def parse_tmi_flight_info_list(elem: Optional[ET.Element]) -> Optional[Dict[str, Any]]:
    if elem is None:
        return None
    return {
        "tmi": [parse_tmi(x) for x in children(elem, "tmi")],
        "fxaFlightData": [
            {
                "fxaFlight": parse_fxa_flight(child(x, "fxaFlight")),
                "raw": element_to_lossless_json(x),
            }
            for x in children(elem, "fxaFlightData")
        ],
        "extras": {
            name: [element_to_lossless_json(x) for x in elems]
            for name, elems in indexed_children(elem).items()
            if name not in {"tmi", "fxaFlightData"}
        },
        "raw": element_to_lossless_json(elem),
    }



def parse_tmi_flight_data(elem: ET.Element) -> Dict[str, Any]:
    return {
        "flight": parse_flight_block(child(elem, "flight")),
        "flightReference": text_of(child(elem, "flightReference")),
        "status": text_of(child(elem, "status")),
        "tmiFlightInfoList": parse_tmi_flight_info_list(child(elem, "tmiFlightInfoList")),
        "extras": {
            name: [element_to_lossless_json(x) for x in elems]
            for name, elems in indexed_children(elem).items()
            if name not in {"flight", "flightReference", "status", "tmiFlightInfoList"}
        },
        "raw": element_to_lossless_json(elem),
    }



def parse_fi_message(elem: ET.Element) -> Dict[str, Any]:
    attrs = strip_ns_dict(dict(elem.attrib))
    all_flight_data = children(child(elem, "tmiFlightDataList"), "flightData")
    restriction_message = parse_restriction_message(child(elem, "restrictionMessage"))
    general_advisories = [parse_generic_fi_block(x) for x in children(elem, "generalAdvisory")]
    airport_config_messages = [parse_generic_fi_block(x) for x in children(elem, "airportConfigMessage")]
    fea_fca = [parse_generic_fi_block(x) for x in children(elem, "feaFca")]
    rapt_timeline_messages = [parse_generic_fi_block(x) for x in children(elem, "raptTimelineMessage")]
    gdp_cancels = [parse_generic_fi_block(x) for x in children(elem, "gdpCancel")]
    cdm_update_data = [parse_generic_fi_block(x) for x in children(elem, "cdmUpdateData")]
    return {
        "attributes": attrs,
        "msgType": attrs.get("msgType"),
        "sourceFacility": attrs.get("sourceFacility"),
        "sourceTimeStamp": attrs.get("sourceTimeStamp"),
        "flightDataCount": len(all_flight_data),
        "tmiFlightDataList": [parse_tmi_flight_data(x) for x in all_flight_data],
        "restrictionMessage": restriction_message,
        "restrictionCount": 1 if restriction_message else 0,
        "generalAdvisories": general_advisories,
        "generalAdvisoryCount": len(general_advisories),
        "airportConfigMessages": airport_config_messages,
        "airportConfigCount": len(airport_config_messages),
        "feaFca": fea_fca,
        "feaFcaCount": len(fea_fca),
        "raptTimelineMessages": rapt_timeline_messages,
        "raptTimelineCount": len(rapt_timeline_messages),
        "gdpCancels": gdp_cancels,
        "gdpCancelCount": len(gdp_cancels),
        "cdmUpdateData": cdm_update_data,
        "cdmUpdateCount": len(cdm_update_data),
        "extras": {
            name: [element_to_lossless_json(x) for x in elems]
            for name, elems in indexed_children(elem).items()
            if name
            not in {
                "tmiFlightDataList",
                "restrictionMessage",
                "generalAdvisory",
                "airportConfigMessage",
                "feaFca",
                "raptTimelineMessage",
                "gdpCancel",
                "cdmUpdateData",
            }
        },
        "raw": element_to_lossless_json(elem),
    }



def parse_fi_output(elem: ET.Element) -> Dict[str, Any]:
    messages = children(elem, "fiMessage")
    return {
        "outputTag": local_name(elem.tag),
        "messageCount": len(messages),
        "messages": [parse_fi_message(msg) for msg in messages],
        "raw": element_to_lossless_json(elem),
    }



def parse_status_entry(elem: ET.Element) -> Dict[str, Any]:
    return {
        "service": text_of(child(elem, "service")),
        "businessFunc": text_of(child(elem, "businessFunc")),
        "facility": text_of(child(elem, "facility")),
        "direction": text_of(child(elem, "direction")),
        "state": text_of(child(elem, "state")),
        "time": text_of(child(elem, "time")),
        "numberMsgs": to_int(text_of(child(elem, "numberMsgs"))),
        "raw": element_to_lossless_json(elem),
    }



def parse_tfms_status_output(elem: ET.Element) -> Dict[str, Any]:
    statuses = children(elem, "status")
    return {
        "outputTag": local_name(elem.tag),
        "statusCount": len(statuses),
        "statuses": [parse_status_entry(x) for x in statuses],
        "raw": element_to_lossless_json(elem),
    }


# ------------------------------------------------------------
# Root dispatch
# ------------------------------------------------------------



def detect_tfms_payload(root: ET.Element) -> str:
    if local_name(root.tag) != "tfmDataService":
        return "unknown"
    if child(root, "fltdOutput") is not None:
        return "tfms_flight_data_output"
    if child(root, "fiOutput") is not None:
        return "tfms_flow_information_output"
    if child(root, "tfmsStatusOutput") is not None:
        return "tfms_status_output"
    return "tfms_unknown_service_output"



def parse_tfms_xml(xml_text: Union[str, bytes]) -> Dict[str, Any]:
    root = ET.fromstring(xml_text if isinstance(xml_text, bytes) else xml_text.encode("utf-8"))
    payload_type = detect_tfms_payload(root)
    base = {
        "payload_type": payload_type,
        "root_tag": local_name(root.tag),
        "root_namespace": ns_name(root.tag),
    }
    if payload_type == "tfms_flight_data_output":
        fltd_output = child(root, "fltdOutput")
        return {**base, **parse_fltd_output(fltd_output), "raw": element_to_lossless_json(root)}
    if payload_type == "tfms_flow_information_output":
        fi_output = child(root, "fiOutput")
        return {**base, **parse_fi_output(fi_output), "raw": element_to_lossless_json(root)}
    if payload_type == "tfms_status_output":
        status_output = child(root, "tfmsStatusOutput")
        return {**base, **parse_tfms_status_output(status_output), "raw": element_to_lossless_json(root)}
    return {**base, "raw": element_to_lossless_json(root)}


# ------------------------------------------------------------
# Projection helpers for FastAPI/Postgres/Redis
# ------------------------------------------------------------



def projection_key_for_message(message: Dict[str, Any]) -> str:
    body = message.get("body") or {}
    qid = body.get("qualifiedAircraftId") if isinstance(body, dict) else None
    if not isinstance(qid, dict) and isinstance(body, dict):
        airline_data = body.get("airlineData")
        if isinstance(airline_data, dict):
            nested_qid = airline_data.get("qualifiedAircraftId")
            if isinstance(nested_qid, dict):
                qid = nested_qid
    gufi = qid.get("gufi") if isinstance(qid, dict) else None
    acid = message.get("acid") or (qid.get("aircraftId") if isinstance(qid, dict) else None)
    flight_ref = message.get("flightRef")
    return gufi or acid or flight_ref or "unknown"



def build_projections(parsed_payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    payload_type = parsed_payload.get("payload_type")
    projections: List[Dict[str, Any]] = []

    def _flow_key(prefix: str, *parts: Any) -> str:
        clean = [str(part) for part in parts if part not in (None, "")]
        suffix = "|".join(clean) if clean else "unknown"
        return f"{prefix}:{suffix}"

    if payload_type == "tfms_flight_data_output":
        for message in parsed_payload.get("messages", []):
            body = message.get("body") or {}
            qid = body.get("qualifiedAircraftId") if isinstance(body, dict) else None
            if not isinstance(qid, dict) and isinstance(body, dict):
                airline_data = body.get("airlineData")
                if isinstance(airline_data, dict):
                    nested_qid = airline_data.get("qualifiedAircraftId")
                    if isinstance(nested_qid, dict):
                        qid = nested_qid
            projections.append(
                {
                    "projection_type": "flight_message",
                    "key": projection_key_for_message(message),
                    "acid": message.get("acid") or (qid or {}).get("aircraftId"),
                    "gufi": (qid or {}).get("gufi"),
                    "flightRef": message.get("flightRef"),
                    "msgType": message.get("msgType"),
                    "sourceFacility": message.get("sourceFacility"),
                    "sourceTimeStamp": message.get("sourceTimeStamp"),
                    "data": message,
                }
            )
        return projections

    if payload_type == "tfms_flow_information_output":
        for message in parsed_payload.get("messages", []):
            restriction = message.get("restrictionMessage")
            if isinstance(restriction, dict):
                restriction_id = restriction.get("restrictionId")
                facility = restriction.get("facility") or message.get("sourceFacility")
                start_time = restriction.get("startTime")
                if restriction_id:
                    key = f"restriction:{restriction_id}"
                else:
                    key = f"restriction:{facility or 'unknown'}:{start_time or 'unknown'}"
                projections.append(
                    {
                        "projection_type": "restriction_message",
                        "key": key,
                        "acid": None,
                        "gufi": None,
                        "flightRef": None,
                        "msgType": message.get("msgType"),
                        "sourceFacility": message.get("sourceFacility"),
                        "sourceTimeStamp": message.get("sourceTimeStamp"),
                        "data": restriction,
                    }
                )
            for flight_data in message.get("tmiFlightDataList", []):
                flight = flight_data.get("flight") or {}
                acid = flight.get("aircraftId")
                gufi = flight.get("gufi")
                flight_ref = flight_data.get("flightReference")
                projections.append(
                    {
                        "projection_type": "tmi_flight",
                        "key": gufi or acid or flight_ref or "unknown",
                        "acid": acid,
                        "gufi": gufi,
                        "flightRef": flight_ref,
                        "msgType": message.get("msgType"),
                        "sourceFacility": message.get("sourceFacility"),
                        "sourceTimeStamp": message.get("sourceTimeStamp"),
                        "data": flight_data,
                    }
                )

            for advisory in message.get("generalAdvisories", []):
                if not isinstance(advisory, dict):
                    continue
                key = _flow_key(
                    "gadv",
                    advisory.get("advisoryNumber"),
                    advisory.get("origin") or message.get("sourceFacility"),
                )
                projections.append(
                    {
                        "projection_type": "general_advisory",
                        "key": key,
                        "acid": None,
                        "gufi": None,
                        "flightRef": None,
                        "msgType": message.get("msgType"),
                        "sourceFacility": message.get("sourceFacility"),
                        "sourceTimeStamp": message.get("sourceTimeStamp"),
                        "data": advisory,
                    }
                )

            for config in message.get("airportConfigMessages", []):
                if not isinstance(config, dict):
                    continue
                key = _flow_key(
                    "aptc",
                    config.get("airport"),
                    config.get("facility") or message.get("sourceFacility"),
                )
                projections.append(
                    {
                        "projection_type": "airport_config",
                        "key": key,
                        "acid": None,
                        "gufi": None,
                        "flightRef": None,
                        "msgType": message.get("msgType"),
                        "sourceFacility": message.get("sourceFacility"),
                        "sourceTimeStamp": message.get("sourceTimeStamp"),
                        "data": config,
                    }
                )

            for fca in message.get("feaFca", []):
                if not isinstance(fca, dict):
                    continue
                key = _flow_key("fea_fca", fca.get("fcaId") or fca.get("fcaName"), message.get("sourceFacility"))
                projections.append(
                    {
                        "projection_type": "fea_fca",
                        "key": key,
                        "acid": None,
                        "gufi": None,
                        "flightRef": None,
                        "msgType": message.get("msgType"),
                        "sourceFacility": message.get("sourceFacility"),
                        "sourceTimeStamp": message.get("sourceTimeStamp"),
                        "data": fca,
                    }
                )

            for index, rapt in enumerate(message.get("raptTimelineMessages", []), start=1):
                if not isinstance(rapt, dict):
                    continue
                key = _flow_key(
                    "rapt",
                    message.get("sourceFacility"),
                    message.get("sourceTimeStamp"),
                    index,
                )
                projections.append(
                    {
                        "projection_type": "rapt_timeline",
                        "key": key,
                        "acid": None,
                        "gufi": None,
                        "flightRef": None,
                        "msgType": message.get("msgType"),
                        "sourceFacility": message.get("sourceFacility"),
                        "sourceTimeStamp": message.get("sourceTimeStamp"),
                        "data": rapt,
                    }
                )

            for index, gdp in enumerate(message.get("gdpCancels", []), start=1):
                if not isinstance(gdp, dict):
                    continue
                key = _flow_key(
                    "gdp_cancel",
                    gdp.get("airportId"),
                    gdp.get("center") or message.get("sourceFacility"),
                    gdp.get("adlTime") or message.get("sourceTimeStamp"),
                    index,
                )
                projections.append(
                    {
                        "projection_type": "gdp_cancel",
                        "key": key,
                        "acid": None,
                        "gufi": None,
                        "flightRef": None,
                        "msgType": message.get("msgType"),
                        "sourceFacility": message.get("sourceFacility"),
                        "sourceTimeStamp": message.get("sourceTimeStamp"),
                        "data": gdp,
                    }
                )

            for index, cdm in enumerate(message.get("cdmUpdateData", []), start=1):
                if not isinstance(cdm, dict):
                    continue
                key = _flow_key(
                    "cdm_update",
                    message.get("sourceFacility"),
                    message.get("sourceTimeStamp"),
                    index,
                )
                projections.append(
                    {
                        "projection_type": "cdm_update",
                        "key": key,
                        "acid": None,
                        "gufi": None,
                        "flightRef": None,
                        "msgType": message.get("msgType"),
                        "sourceFacility": message.get("sourceFacility"),
                        "sourceTimeStamp": message.get("sourceTimeStamp"),
                        "data": cdm,
                    }
                )
        return projections

    if payload_type == "tfms_status_output":
        for status in parsed_payload.get("statuses", []):
            key = "|".join(
                [
                    status.get("service") or "",
                    status.get("businessFunc") or "",
                    status.get("facility") or "",
                    status.get("direction") or "",
                ]
            )
            projections.append(
                {
                    "projection_type": "service_status",
                    "key": key,
                    "acid": None,
                    "gufi": None,
                    "flightRef": None,
                    "msgType": "status",
                    "sourceFacility": status.get("facility"),
                    "sourceTimeStamp": status.get("time"),
                    "data": status,
                }
            )
        return projections

    return projections


# ------------------------------------------------------------
# CLI helpers
# ------------------------------------------------------------



def parse_file(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as handle:
        return parse_tfms_xml(handle.read())



def parse_many(paths: Iterable[str]) -> Dict[str, Any]:
    return {path: parse_file(path) for path in paths}



def dumps(data: Dict[str, Any], pretty: bool = True) -> str:
    return json.dumps(data, ensure_ascii=False, indent=2 if pretty else None)
