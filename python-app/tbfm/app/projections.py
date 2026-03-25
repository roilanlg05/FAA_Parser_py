from __future__ import annotations

from typing import Any


def _projection_key(*parts: str | None) -> str:
    cleaned = [p for p in parts if p]
    return '|'.join(cleaned) if cleaned else 'unknown'


def build_tbfm_projections(parsed_payload: dict[str, Any]) -> list[dict[str, Any]]:
    projections: list[dict[str, Any]] = []

    for document in parsed_payload.get('documents', []):
        env_attrs = document.get('attributes') or {}
        env_srce = env_attrs.get('envSrce')
        env_time = env_attrs.get('envTime')
        for tma in document.get('tma', []):
            tma_attrs = tma.get('attributes') or {}
            msg_id = tma_attrs.get('msgId')
            msg_time = tma_attrs.get('msgTime')

            for air in tma.get('air', []):
                attrs = air.get('attributes') or {}
                aid = attrs.get('aid') or (air.get('flt') or {}).get('aid')
                tma_id = attrs.get('tmaId')
                dap = attrs.get('dap') or (air.get('flt') or {}).get('dap')
                apt = attrs.get('apt') or (air.get('flt') or {}).get('apt')
                projections.append(
                    {
                        'source_kind': 'tbfm',
                        'projection_type': 'air',
                        'projection_key': _projection_key(tma_id, aid, attrs.get('airType')),
                        'acid': aid,
                        'tma_id': tma_id,
                        'gufi': None,
                        'flight_ref': msg_id,
                        'msg_type': attrs.get('airType'),
                        'source_facility': env_srce,
                        'source_time': msg_time or env_time,
                        'data': {
                            'env': env_attrs,
                            'tma': tma_attrs,
                            'air': air,
                            'origin': dap,
                            'destination': apt,
                        },
                    }
                )

            for con in tma.get('con', []):
                projections.append(
                    {
                        'source_kind': 'tbfm',
                        'projection_type': 'con',
                        'projection_key': _projection_key('con', env_srce, msg_id),
                        'acid': None,
                        'tma_id': None,
                        'gufi': None,
                        'flight_ref': msg_id,
                        'msg_type': 'con',
                        'source_facility': env_srce,
                        'source_time': msg_time or env_time,
                        'data': {'env': env_attrs, 'tma': tma_attrs, 'con': con},
                    }
                )

            for adp in tma.get('adp', []):
                projections.append(
                    {
                        'source_kind': 'tbfm',
                        'projection_type': 'adp',
                        'projection_key': _projection_key('adp', env_srce, msg_id),
                        'acid': None,
                        'tma_id': None,
                        'gufi': None,
                        'flight_ref': msg_id,
                        'msg_type': 'adp',
                        'source_facility': env_srce,
                        'source_time': msg_time or env_time,
                        'data': {'env': env_attrs, 'tma': tma_attrs, 'adp': adp},
                    }
                )

    return projections
