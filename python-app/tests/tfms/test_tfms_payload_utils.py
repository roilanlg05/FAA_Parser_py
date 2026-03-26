from __future__ import annotations

from app.tfms_payload_utils import only_raw_fields, strip_raw_fields


def test_strip_raw_fields_removes_all_raw_keys() -> None:
    payload = {
        'a': 1,
        'raw': {'x': 1},
        'nested': {
            'raw': {'y': 2},
            'keep': True,
            'list': [
                {'raw': {'z': 3}, 'foo': 'bar'},
            ],
        },
    }
    out = strip_raw_fields(payload)
    assert 'raw' not in out
    assert 'raw' not in out['nested']
    assert 'raw' not in out['nested']['list'][0]
    assert out['nested']['list'][0]['foo'] == 'bar'


def test_only_raw_fields_keeps_raw_branches() -> None:
    payload = {
        'a': 1,
        'raw': {'root': True},
        'nested': {
            'raw': {'child': True},
            'keep': 'nope',
            'list': [
                {'raw': {'leaf': 1}, 'x': 1},
                {'x': 2},
            ],
        },
    }
    out = only_raw_fields(payload)
    assert out['raw'] == {'root': True}
    assert out['nested']['raw'] == {'child': True}
    assert 'keep' not in out['nested']
    assert len(out['nested']['list']) == 1
    assert out['nested']['list'][0]['raw'] == {'leaf': 1}
