from __future__ import annotations

import re
import xml.etree.ElementTree as ET
from typing import Any, Optional

TBFM_NS = 'urn:us:gov:dot:faa:atm:tfm:tbfmmeteringpublication'


def is_supported_tbfm_namespace(ns: str | None) -> bool:
    if ns is None:
        return False
    return ns == TBFM_NS or ns.startswith(f'{TBFM_NS}:')


def split_tag(tag: str) -> tuple[str | None, str]:
    if tag.startswith('{'):
        ns, local = tag[1:].split('}', 1)
        return ns, local
    return None, tag


def local_name(tag: str) -> str:
    return split_tag(tag)[1]


def strip_ns_dict(raw: dict[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for key, value in raw.items():
        if key.startswith('{'):
            _, local = split_tag(key)
            out[local] = value
        else:
            out[key] = value
    return out


def text_of(elem: ET.Element | None) -> str | None:
    if elem is None or elem.text is None:
        return None
    value = elem.text.strip()
    return value if value else None


def child(parent: ET.Element | None, name: str) -> ET.Element | None:
    if parent is None:
        return None
    for elem in list(parent):
        if local_name(elem.tag) == name:
            return elem
    return None


def children(parent: ET.Element | None, name: str) -> list[ET.Element]:
    if parent is None:
        return []
    return [elem for elem in list(parent) if local_name(elem.tag) == name]


def indexed_children(parent: ET.Element | None) -> dict[str, list[ET.Element]]:
    out: dict[str, list[ET.Element]] = {}
    if parent is None:
        return out
    for elem in list(parent):
        out.setdefault(local_name(elem.tag), []).append(elem)
    return out


def to_int(value: str | None) -> int | None:
    if value is None or value == '':
        return None
    try:
        return int(value)
    except Exception:
        return None


def to_float(value: str | None) -> float | None:
    if value is None or value == '':
        return None
    try:
        return float(value)
    except Exception:
        return None


def element_to_lossless_json(elem: ET.Element) -> dict[str, Any]:
    ns, local = split_tag(elem.tag)
    node: dict[str, Any] = {
        'tag': local,
        'namespace': ns,
        'attributes': strip_ns_dict(dict(elem.attrib)),
        'attributes_raw': dict(elem.attrib),
        'text': text_of(elem),
        'children': [],
    }
    for child_elem in list(elem):
        node['children'].append(element_to_lossless_json(child_elem))
    return node


def parse_scalar_children(elem: ET.Element | None, known_lists: set[str] | None = None) -> dict[str, Any]:
    out: dict[str, Any] = {}
    if elem is None:
        return out
    groups = indexed_children(elem)
    for name, elems in groups.items():
        if known_lists and name in known_lists:
            continue
        if len(elems) == 1 and not list(elems[0]):
            out[name] = text_of(elems[0])
        elif len(elems) > 1 and all(not list(e) for e in elems):
            out[name] = [text_of(e) for e in elems]
        else:
            out[name] = [element_to_lossless_json(e) for e in elems]
    return out


def split_xml_documents(text: str) -> list[str]:
    cleaned = text.strip()
    if not cleaned:
        return []
    parts = re.split(r'(?=<\?xml\s)', cleaned)
    docs = [part.strip() for part in parts if part.strip()]
    if not docs and cleaned:
        return [cleaned]
    return docs


def parse_flt(elem: ET.Element | None) -> dict[str, Any] | None:
    if elem is None:
        return None
    out = parse_scalar_children(elem)
    out['parsed_numbers'] = {
        key: to_float(out.get(key)) for key in ['spd', 'ara', 'ina'] if isinstance(out.get(key), str) or out.get(key) is None
    }
    out['raw'] = element_to_lossless_json(elem)
    return out


def parse_eta(elem: ET.Element | None) -> dict[str, Any] | None:
    if elem is None:
        return None
    out = parse_scalar_children(elem)
    out['raw'] = element_to_lossless_json(elem)
    return out


def parse_sta(elem: ET.Element | None) -> dict[str, Any] | None:
    if elem is None:
        return None
    out = parse_scalar_children(elem)
    out['raw'] = element_to_lossless_json(elem)
    return out


def parse_sch(elem: ET.Element | None) -> dict[str, Any] | None:
    if elem is None:
        return None
    out = parse_scalar_children(elem)
    out['raw'] = element_to_lossless_json(elem)
    return out


def parse_mrp(elem: ET.Element) -> dict[str, Any]:
    return {'attributes': strip_ns_dict(dict(elem.attrib)), **parse_scalar_children(elem), 'raw': element_to_lossless_json(elem)}


def parse_air(elem: ET.Element) -> dict[str, Any]:
    groups = indexed_children(elem)
    out: dict[str, Any] = {
        'attributes': strip_ns_dict(dict(elem.attrib)),
        'flt': parse_flt(child(elem, 'flt')),
        'eta': parse_eta(child(elem, 'eta')),
        'sta': parse_sta(child(elem, 'sta')),
        'sch': parse_sch(child(elem, 'sch')),
        'mrp': [parse_mrp(x) for x in groups.get('mrp', [])],
        'extras': {},
        'raw': element_to_lossless_json(elem),
    }
    for name, elems in groups.items():
        if name not in {'flt', 'eta', 'sta', 'sch', 'mrp'}:
            out['extras'][name] = [element_to_lossless_json(x) for x in elems]
    return out


def parse_sap(elem: ET.Element) -> dict[str, Any]:
    return {'attributes': strip_ns_dict(dict(elem.attrib)), **parse_scalar_children(elem), 'raw': element_to_lossless_json(elem)}


def parse_sac(elem: ET.Element) -> dict[str, Any]:
    saps = [parse_sap(x) for x in children(child(elem, 'saps'), 'sap')]
    out = {
        'attributes': strip_ns_dict(dict(elem.attrib)),
        **{k: v for k, v in parse_scalar_children(elem, known_lists={'saps'}).items() if k != 'saps'},
        'saps': saps,
        'extras': {},
        'raw': element_to_lossless_json(elem),
    }
    for name, elems in indexed_children(elem).items():
        if name not in {'tra', 'tim', 'saps'}:
            out['extras'][name] = [element_to_lossless_json(x) for x in elems]
    return out


def parse_scl(elem: ET.Element) -> dict[str, Any]:
    return {'attributes': strip_ns_dict(dict(elem.attrib)), **parse_scalar_children(elem), 'raw': element_to_lossless_json(elem)}


def parse_cc(elem: ET.Element) -> dict[str, Any]:
    return {'attributes': strip_ns_dict(dict(elem.attrib)), **parse_scalar_children(elem), 'raw': element_to_lossless_json(elem)}


def parse_ssc(elem: ET.Element) -> dict[str, Any]:
    out = {
        'attributes': strip_ns_dict(dict(elem.attrib)),
        **{k: v for k, v in parse_scalar_children(elem, known_lists={'scls', 'ccs'}).items() if k not in {'scls', 'ccs'}},
        'parsed_numbers': {
            'ssn': to_int(text_of(child(elem, 'ssn'))),
            'ssd': to_float(text_of(child(elem, 'ssd'))),
            'ssmin': to_int(text_of(child(elem, 'ssmin'))),
        },
        'scls': [parse_scl(x) for x in children(child(elem, 'scls'), 'scl')],
        'ccs': [parse_cc(x) for x in children(child(elem, 'ccs'), 'cc')],
        'extras': {},
        'raw': element_to_lossless_json(elem),
    }
    for name, elems in indexed_children(elem).items():
        if name not in {'ssn', 'sscname', 'ssd', 'ssmin', 'sstyp', 'scls', 'ccs'}:
            out['extras'][name] = [element_to_lossless_json(x) for x in elems]
    return out


def parse_scc(elem: ET.Element) -> dict[str, Any]:
    out = {
        'attributes': strip_ns_dict(dict(elem.attrib)),
        **{k: v for k, v in parse_scalar_children(elem, known_lists={'sscs'}).items() if k != 'sscs'},
        'sscs': [parse_ssc(x) for x in children(child(elem, 'sscs'), 'ssc')],
        'extras': {},
        'raw': element_to_lossless_json(elem),
    }
    for name, elems in indexed_children(elem).items():
        if name not in {'tra', 'tim', 'sscs'}:
            out['extras'][name] = [element_to_lossless_json(x) for x in elems]
    return out


def parse_con(elem: ET.Element) -> dict[str, Any]:
    groups = indexed_children(elem)
    out = {
        'attributes': strip_ns_dict(dict(elem.attrib)),
        'sac': [parse_sac(x) for x in groups.get('sac', [])],
        'scc': [parse_scc(x) for x in groups.get('scc', [])],
        'extras': {},
        'raw': element_to_lossless_json(elem),
    }
    for name, elems in groups.items():
        if name not in {'sac', 'scc'}:
            out['extras'][name] = [element_to_lossless_json(x) for x in elems]
    return out


def parse_scn(elem: ET.Element) -> dict[str, Any]:
    return {'attributes': strip_ns_dict(dict(elem.attrib)), **parse_scalar_children(elem), 'raw': element_to_lossless_json(elem)}


def parse_adp(elem: ET.Element) -> dict[str, Any]:
    out = {
        'attributes': strip_ns_dict(dict(elem.attrib)),
        **{k: v for k, v in parse_scalar_children(elem, known_lists={'scns'}).items() if k != 'scns'},
        'scns': [parse_scn(x) for x in children(child(elem, 'scns'), 'scn')],
        'extras': {},
        'raw': element_to_lossless_json(elem),
    }
    for name, elems in indexed_children(elem).items():
        if name not in {'tra', 'scns'}:
            out['extras'][name] = [element_to_lossless_json(x) for x in elems]
    return out


def parse_tma(elem: ET.Element) -> dict[str, Any]:
    groups = indexed_children(elem)
    out = {
        'attributes': strip_ns_dict(dict(elem.attrib)),
        'air': [parse_air(x) for x in groups.get('air', [])],
        'con': [parse_con(x) for x in groups.get('con', [])],
        'adp': [parse_adp(x) for x in groups.get('adp', [])],
        'extras': {},
        'raw': element_to_lossless_json(elem),
    }
    for name, elems in groups.items():
        if name not in {'air', 'con', 'adp'}:
            out['extras'][name] = [element_to_lossless_json(x) for x in elems]
    return out


def parse_env(elem: ET.Element) -> dict[str, Any]:
    groups = indexed_children(elem)
    out = {
        'payload_type': 'tbfm_metering_publication',
        'root_tag': 'env',
        'attributes': strip_ns_dict(dict(elem.attrib)),
        'tma': [parse_tma(x) for x in groups.get('tma', [])],
        'extras': {},
        'raw': element_to_lossless_json(elem),
    }
    for name, elems in groups.items():
        if name != 'tma':
            out['extras'][name] = [element_to_lossless_json(x) for x in elems]
    return out


def parse_mis(elem: ET.Element) -> dict[str, Any]:
    groups = indexed_children(elem)
    attrs = strip_ns_dict(dict(elem.attrib))
    normalized_attrs = dict(attrs)
    if attrs.get('misSrce') and not normalized_attrs.get('envSrce'):
        normalized_attrs['envSrce'] = attrs.get('misSrce')
    if attrs.get('misTime') and not normalized_attrs.get('envTime'):
        normalized_attrs['envTime'] = attrs.get('misTime')

    out = {
        'payload_type': 'tbfm_metering_publication',
        'root_tag': 'mis',
        'attributes': normalized_attrs,
        'tma': [parse_tma(x) for x in groups.get('tma', [])],
        'extras': {},
        'raw': element_to_lossless_json(elem),
    }
    for name, elems in groups.items():
        if name != 'tma':
            out['extras'][name] = [element_to_lossless_json(x) for x in elems]
    return out


def parse_tbfm_document(xml_text: str) -> dict[str, Any]:
    root = ET.fromstring(xml_text)
    ns, local = split_tag(root.tag)
    if not is_supported_tbfm_namespace(ns):
        raise ValueError(f'Unsupported root for TBFM parser: {root.tag}')
    if local == 'env':
        return parse_env(root)
    if local == 'mis':
        return parse_mis(root)
    raise ValueError(f'Unsupported root for TBFM parser: {root.tag}')


def parse_tbfm_text(text: str) -> dict[str, Any]:
    docs = split_xml_documents(text)
    parsed_docs: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []
    for idx, xml_doc in enumerate(docs):
        try:
            parsed_docs.append(parse_tbfm_document(xml_doc))
        except Exception as exc:
            errors.append({'index': idx, 'error': str(exc), 'sample': xml_doc[:300]})
    return {
        'document_count': len(docs),
        'parsed_count': len(parsed_docs),
        'error_count': len(errors),
        'documents': parsed_docs,
        'errors': errors,
    }
