from __future__ import annotations

from app.tbfm_payload_utils import only_raw_fields, strip_raw_fields


def test_strip_raw_fields_removes_raw_recursively() -> None:
    payload = {
        'raw': {'root': True},
        'doc': {
            'raw': {'child': True},
            'items': [
                {'raw': {'leaf': 1}, 'value': 42},
            ],
        },
    }
    out = strip_raw_fields(payload)
    assert 'raw' not in out
    assert 'raw' not in out['doc']
    assert 'raw' not in out['doc']['items'][0]
    assert out['doc']['items'][0]['value'] == 42


def test_only_raw_fields_keeps_raw_only() -> None:
    payload = {
        'raw': {'root': True},
        'doc': {
            'raw': {'child': True},
            'keep': 'no',
            'items': [
                {'raw': {'leaf': 1}, 'value': 42},
                {'value': 99},
            ],
        },
    }
    out = only_raw_fields(payload)
    assert out['raw'] == {'root': True}
    assert out['doc']['raw'] == {'child': True}
    assert 'keep' not in out['doc']
    assert len(out['doc']['items']) == 1
    assert out['doc']['items'][0]['raw'] == {'leaf': 1}
