import asyncio
import json
from types import SimpleNamespace

import pytest

import smartcitizen_connector.device.device as device_module
from smartcitizen_connector import SCDevice


class Response:
    def __init__(self, payload):
        self.payload = payload

    async def read(self):
        return json.dumps(self.payload).encode()

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        return False


@pytest.fixture
def respond(monkeypatch):
    ''' Replaces the retry client so that every request returns the given payload '''
    def set_payload(payload):
        class RetryClient:
            def __init__(self, *args, **kwargs):
                pass

            def get(self, url, headers=None):
                return Response(payload)

        monkeypatch.setattr(device_module, 'RetryClient', RetryClient)

    return set_payload


@pytest.fixture
def device():
    device = SCDevice.__new__(SCDevice)
    device.id = 1
    device.timezone = 'UTC'
    device.failed_sensors = []
    device.json = SimpleNamespace(id=1, data=SimpleNamespace(sensors=[SimpleNamespace(id=55, name='TEMP')]))
    return device


def get_datum(device):
    return asyncio.run(device.get_datum(asyncio.Semaphore(1), None, 'url', None, 55, False, '1Min', True))


def test_readings(device, respond):
    respond({'readings': [['2026-01-01T00:00:00Z', 20.0], ['2026-01-01T00:01:00Z', 21.0]]})

    df = get_datum(device)

    assert df['TEMP'].tolist() == [20.0, 21.0]
    assert device.failed_sensors == []


def test_sensor_without_data_is_not_failed(device, respond):
    respond({'readings': []})

    assert get_datum(device) is None
    assert device.failed_sensors == []


def test_failed_request(device, respond):
    respond({'message': 'Internal server error'})

    assert get_datum(device) is None
    assert device.failed_sensors == ['TEMP']
