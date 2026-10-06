import importlib
import sys

import pytest

import smartcitizen_connector.device.device as device_module
from smartcitizen_connector.device import check_postprocessing
from smartcitizen_connector.models import Postprocessing

# The package attribute 'config' is the Config instance, not the module
config_module = sys.modules['smartcitizen_connector._config.config']


class Response:
    def __init__(self, payload):
        self.payload = payload

    def json(self):
        if isinstance(self.payload, Exception):
            raise self.payload
        return self.payload


@pytest.fixture
def requested(monkeypatch, hardware):
    ''' Replaces HTTP requests and records the requested urls '''
    urls = []

    def safe_get(url, headers=None):
        urls.append(url)
        return Response(hardware)

    monkeypatch.setattr(device_module, 'safe_get', safe_get)
    return urls


def test_bare_name_uses_base_url(requested):
    url, hardware_postprocessing, ok = check_postprocessing({'device_id': 1, 'hardware_url': 'SCAS220013'})

    expected = f'{device_module.config.BASE_POSTPROCESSING_URL}hardware/SCAS220013.json'
    assert ok is True
    assert url == expected
    assert requested == [expected]
    assert hardware_postprocessing.versions[0].ids['AS_48_32'] == '212830246'


def test_full_url_is_used_as_is(requested):
    full_url = 'https://raw.githubusercontent.com/fablabbcn/smartcitizen-data/master/hardware/SCAS220013.json'
    url, _, ok = check_postprocessing(Postprocessing(device_id=1, hardware_url=full_url))

    assert ok is True
    assert url == full_url
    assert requested == [full_url]


def test_blueprint_url_is_not_rewritten(requested):
    _, hardware_postprocessing, _ = check_postprocessing({'device_id': 1, 'hardware_url': 'SCAS220013'})

    assert hardware_postprocessing.blueprint_url == \
        'https://raw.githubusercontent.com/fablabbcn/smartcitizen-data/master/blueprints/sc_air.json'


def test_no_postprocessing():
    assert check_postprocessing(None) == (None, None, False)


def test_invalid_hardware_file(monkeypatch):
    monkeypatch.setattr(device_module, 'safe_get', lambda url, headers=None: Response(ValueError('not json')))

    url, hardware_postprocessing, ok = check_postprocessing({'device_id': 1, 'hardware_url': 'www.example.com'})

    assert ok is False
    assert hardware_postprocessing is None


def test_request_error(monkeypatch):
    def safe_get(url, headers=None):
        raise ConnectionError('unreachable')

    monkeypatch.setattr(device_module, 'safe_get', safe_get)

    url, hardware_postprocessing, ok = check_postprocessing({'device_id': 1, 'hardware_url': 'SCAS220013'})

    assert (url, hardware_postprocessing, ok) == ('', None, False)


def test_base_postprocessing_url_from_environment(monkeypatch):
    monkeypatch.setenv('BASE_POSTPROCESSING_URL', 'https://flows.example.com/api/v1/')
    try:
        assert importlib.reload(config_module).config.BASE_POSTPROCESSING_URL == 'https://flows.example.com/api/v1/'
    finally:
        monkeypatch.delenv('BASE_POSTPROCESSING_URL')
        importlib.reload(config_module)

    assert config_module.config.BASE_POSTPROCESSING_URL == \
        'https://raw.githubusercontent.com/fablabbcn/smartcitizen-data/master/'


FLOWS = 'https://flows.smartcitizen.me/api/v1/'


@pytest.mark.parametrize('url, expected', [
    ('https://raw.githubusercontent.com/fablabbcn/smartcitizen-data/master/hardware/SCAS220013.json',
     f'{FLOWS}hardware/SCAS220013.json'),
    ('https://raw.githubusercontent.com/fablabbcn/smartcitizen-data/enhacement/flexible-handlers/hardware/SCAS210099.json',
     f'{FLOWS}hardware/SCAS210099.json'),
    ('https://github.com/fablabbcn/smartcitizen-data/blob/master/blueprints/sc_21_station_module.json',
     f'{FLOWS}blueprints/sc_21_station_module.json'),
    ('https://raw.githubusercontent.com/fablabbcn/smartcitizen-data/master/blueprints/sc_air.json',
     f'{FLOWS}blueprints/sc_air.json'),
    # Other urls are not changed
    ('https://smartcitizen.me/kits/14673', 'https://smartcitizen.me/kits/14673'),
    ('https://raw.githubusercontent.com/fablabbcn/smartcitizen-data/master/calibrations/calibrations.json',
     'https://raw.githubusercontent.com/fablabbcn/smartcitizen-data/master/calibrations/calibrations.json'),
    (None, None),
])
def test_to_base_url(monkeypatch, url, expected):
    monkeypatch.setattr(device_module.config, 'BASE_POSTPROCESSING_URL', FLOWS)

    assert device_module.to_base_url(url) == expected


def test_github_hardware_url_uses_base_url(monkeypatch, requested):
    monkeypatch.setattr(device_module.config, 'BASE_POSTPROCESSING_URL', FLOWS)
    github = 'https://raw.githubusercontent.com/fablabbcn/smartcitizen-data/master/hardware/SCAS220013.json'

    url, _, ok = check_postprocessing({'device_id': 1, 'hardware_url': github})

    assert ok is True
    assert url == requested[0] == f'{FLOWS}hardware/SCAS220013.json'


def test_github_blueprint_url_uses_base_url(monkeypatch, requested):
    monkeypatch.setattr(device_module.config, 'BASE_POSTPROCESSING_URL', FLOWS)

    device_module.check_blueprint('https://raw.githubusercontent.com/fablabbcn/smartcitizen-data/master/blueprints/sc_air.json')

    assert requested == [f'{FLOWS}blueprints/sc_air.json']


def test_base_postprocessing_url_gets_trailing_slash(monkeypatch):
    monkeypatch.setenv('BASE_POSTPROCESSING_URL', 'https://flows.smartcitizen.me/api/v1')
    try:
        assert importlib.reload(config_module).config.BASE_POSTPROCESSING_URL == FLOWS
    finally:
        monkeypatch.delenv('BASE_POSTPROCESSING_URL')
        importlib.reload(config_module)
