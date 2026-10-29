from datetime import datetime, timezone
from types import SimpleNamespace

import pytest
from pydantic import TypeAdapter

from smartcitizen_connector import SCDevice
from smartcitizen_connector.models import HardwarePostprocessing


def make_device(blueprint, hardware=None, last_reading_at=datetime(2026, 1, 1, tzinfo=timezone.utc)):
    ''' SCDevice without network: only the attributes used to build the properties '''
    device = SCDevice.__new__(SCDevice)
    device.id = 1
    device.json = SimpleNamespace(last_reading_at=last_reading_at)
    device._blueprint = blueprint
    device._channels = []
    device._checks = []
    device._exports = []
    device._versions = []
    device._filled_properties = []
    device._properties = {}
    device._hardware_postprocessing = None
    if hardware is not None:
        device._hardware_postprocessing = TypeAdapter(HardwarePostprocessing).validate_python(hardware)
    return device


def channel(device, name):
    return next(item for item in device.channels if item['name'] == name)


def test_hardware_fills_channels(blueprint, hardware):
    device = make_device(blueprint, hardware)

    assert device.__get_channels__() is True
    assert channel(device, 'NO2')['kwargs'] == {
        'we': 'ADC_48_3', 'ae': 'ADC_48_2', 't': 'EC_SENSOR_TEMP', 'alphasense_id': '212830246'}
    assert channel(device, 'NO2_WE')['kwargs'] == {'channel': 'ADC_48_3'}
    assert channel(device, 'NO2_AE')['kwargs'] == {'channel': 'ADC_48_2'}
    assert channel(device, 'ASPT1000')['kwargs'] == {
        'pt1000minus': 'ADC_49_3', 'pt1000plus': 'ADC_49_2', 'afe_id': '10-002911'}
    assert channel(device, 'PT1000_POS')['kwargs'] == {'channel': 'ADC_49_2'}


def test_afe_board_in_an_as_slot_fills_the_pt1000(blueprint, hardware):
    hardware['versions'][0]['ids'] = {'AS_48_32': '212830246', 'AS_49_32': '17-000123'}
    device = make_device(blueprint, hardware)

    device.__get_channels__()

    assert channel(device, 'ASPT1000')['kwargs'] == {
        'pt1000minus': 'ADC_49_2', 'pt1000plus': 'ADC_49_3', 'afe_id': '17-000123'}
    assert channel(device, 'PT1000_POS')['kwargs'] == {'channel': 'ADC_49_3'}
    assert channel(device, 'NO2')['kwargs']['alphasense_id'] == '212830246'


def test_version_after_last_reading_is_skipped(blueprint, hardware):
    device = make_device(blueprint, hardware, last_reading_at=datetime(2024, 1, 1, tzinfo=timezone.utc))

    device.__get_channels__()

    assert channel(device, 'NO2')['kwargs']['we'] is None


def test_version_without_last_reading(blueprint, hardware):
    device = make_device(blueprint, hardware, last_reading_at=None)

    device.__get_channels__()

    assert channel(device, 'NO2')['kwargs']['we'] == 'ADC_48_3'


def test_unknown_slot_is_skipped(blueprint, hardware):
    hardware['versions'][0]['ids'] = {'XX_48_01': '1', 'AS_48_32': '212830246'}
    device = make_device(blueprint, hardware)

    device.__get_channels__()

    assert channel(device, 'NO2')['kwargs']['we'] == 'ADC_48_3'


def test_hardware_channel_missing_in_blueprint(blueprint, hardware):
    # O3 sensor, but the blueprint has no O3 or OX channels
    hardware['versions'][0]['ids'] = {'AS_49_10': '214920348'}
    device = make_device(blueprint, hardware)

    assert device.__get_channels__() is True
    assert channel(device, 'NO2')['kwargs']['we'] is None


def test_checks_and_exports(blueprint):
    device = make_device(blueprint)

    assert device.__get_checks__() is True
    assert device.__get_exports__() is True
    assert device.checks[0]['name'] == 'GAPS'
    assert device.checks[0]['module'] == 'scdata.device.check'
    assert device.exports == [{'name': 'all', 'columns': []}, {'name': 'clean', 'columns': ['NO2']}]


@pytest.mark.parametrize('missing', ['checks', 'exports'])
def test_blueprint_without_checks_or_exports(blueprint, missing):
    del blueprint[missing]
    device = make_device(blueprint)

    assert getattr(device, f'__get_{missing}__')() is False
    assert getattr(device, missing) == []


def test_properties_hold_validated_items(blueprint, hardware):
    device = make_device(blueprint, hardware)
    for item in ['channels', 'checks', 'exports']:
        if getattr(device, f'__get_{item}__')():
            device._filled_properties.append(item)

    device.__make_properties__()

    assert device.properties['meta'] == blueprint['meta']
    assert device.properties['exports'][1] == {'name': 'clean', 'columns': ['NO2']}
    assert device.properties['checks'][0]['store_qc'] is True
    no2 = next(item for item in device.properties['channels'] if item['name'] == 'NO2')
    assert no2['kwargs']['alphasense_id'] == '212830246'


def two_versions(hardware):
    ''' Sensor swap: the NO2 sensor moves to another slot in 2025, the PT1000 is removed '''
    hardware['versions'] = [
        {'ids': {'AS_49_10': '212830246'}, 'from': '2025-01-01', 'to': None},
        {'ids': {'AS_48_32': '202760040', 'PT_49_23': '10-002911'}, 'from': '2024-04-01', 'to': '2025-01-01'},
    ]
    return hardware


def test_channels_by_version(blueprint, hardware):
    device = make_device(blueprint, two_versions(hardware))

    device.__get_channels__()

    versions = device.channels_by_version
    assert [(version['from_date'].date().isoformat(), version['to_date']) for version in versions] == [
        ('2024-04-01', versions[0]['to_date']), ('2025-01-01', None)]
    first, second = [{item['name']: item['kwargs'] for item in version['channels']} for version in versions]
    assert first['NO2'] == {'we': 'ADC_48_3', 'ae': 'ADC_48_2', 't': 'EC_SENSOR_TEMP', 'alphasense_id': '202760040'}
    assert second['NO2'] == {'we': 'ADC_49_1', 'ae': 'ADC_49_0', 't': 'EC_SENSOR_TEMP', 'alphasense_id': '212830246'}
    # Sensors of an older version do not leak into the next one
    assert first['ASPT1000']['afe_id'] == '10-002911'
    assert second['ASPT1000']['afe_id'] is None


def test_channels_are_the_latest_version(blueprint, hardware):
    device = make_device(blueprint, two_versions(hardware))

    device.__get_channels__()

    assert channel(device, 'NO2')['kwargs']['alphasense_id'] == '212830246'
    assert channel(device, 'ASPT1000')['kwargs']['afe_id'] is None


def test_future_version_is_left_out(blueprint, hardware):
    device = make_device(blueprint, two_versions(hardware), last_reading_at=datetime(2024, 6, 1, tzinfo=timezone.utc))

    device.__get_channels__()

    assert len(device.channels_by_version) == 1
    assert channel(device, 'NO2')['kwargs']['alphasense_id'] == '202760040'


def test_no_hardware(blueprint):
    device = make_device(blueprint)

    device.__get_channels__()

    assert device.channels_by_version == []


def test_apply_blueprint_fills_another_blueprint(blueprint, hardware):
    device = make_device(blueprint, two_versions(hardware))
    device.__fill_blueprint__()
    long = {'meta': {'kind': 'long'}, 'checks': [{'name': 'GAPS', 'function': 'find_gaps'}],
            'channels': [dict(item, function='alphasense_als', kwargs=dict(item['kwargs'], n_gaps=5))
                         if item['name'] == 'NO2' else item for item in blueprint['channels']]}

    assert device.apply_blueprint(long) is True

    assert channel(device, 'NO2')['function'] == 'alphasense_als'
    assert channel(device, 'NO2')['kwargs'] == {
        'we': 'ADC_49_1', 'ae': 'ADC_49_0', 't': 'EC_SENSOR_TEMP', 'alphasense_id': '212830246', 'n_gaps': 5}
    first = {item['name']: item for item in device.channels_by_version[0]['channels']}
    assert first['NO2']['function'] == 'alphasense_als' and first['NO2']['kwargs']['alphasense_id'] == '202760040'
    assert device.properties['meta'] == {'kind': 'long'}
    assert [check['name'] for check in device.properties['checks']] == ['GAPS']


def test_apply_blueprint_keeps_the_state_when_it_fails(blueprint, hardware):
    device = make_device(blueprint, hardware)
    device.__fill_blueprint__()
    before = (device.channels, device.properties, device._blueprint)

    with pytest.raises(KeyError):
        device.apply_blueprint({'meta': {'kind': 'long'}})

    assert (device.channels, device.properties, device._blueprint) == before


def test_apply_blueprint_without_hardware(blueprint):
    # As a device created with check_postprocessing=False: no hardware to fill the channels with
    device = make_device(None)

    assert device.apply_blueprint(blueprint) is False
    assert [item['name'] for item in device.channels] == [item['name'] for item in blueprint['channels']]


def test_posting_nothing_is_said(blueprint, hardware, monkeypatch, caplog):
    import asyncio
    from pandas import DataFrame, date_range

    monkeypatch.setenv('SC_BEARER', 'token')
    device = make_device(blueprint, hardware)
    device.__get_channels__()
    # Processed data without any of the channels to post
    device.data = DataFrame({'TEMP': [1.0, 2.0]}, index=date_range('2026-01-01', periods=2, freq='min', tz='UTC'))

    # Devices with only channels without an id processed (e.g. CO2) have nothing to post
    assert asyncio.run(device.post_data(columns='channels', dry_run=True)) is True
    assert 'Nothing to post' in caplog.text


def test_posting_says_what_is_posted(blueprint, hardware, monkeypatch, caplog):
    import asyncio
    import logging
    from pandas import DataFrame, date_range

    monkeypatch.setenv('SC_BEARER', 'token')
    device = make_device(blueprint, hardware)
    device.__get_channels__()
    device.url, device._headers = 'https://api.example/v0/devices/1', {}
    next(item for item in device._channels if item.name == 'NO2').id = 123
    device.data = DataFrame({'NO2_WE': [1.0, 2.0], 'NO2': [3.0, None]},
                            index=date_range('2026-01-01', periods=2, freq='min', tz='UTC'))

    with caplog.at_level(logging.INFO, logger='smartcitizen_connector'):
        assert asyncio.run(device.post_data(columns='channels', dry_run=True)) is True

    assert device.posted == {'NO2': {'id': 123, 'readings': 1}}
    assert device.not_posted == {'NO2_WE': 'no sensor id'}
    assert 'Dry run, would post: NO2 (sensor 123, 1 readings)' in caplog.text
    # Channels of the blueprint without data, and the ones without an id, are expected: not warnings
    assert [record.getMessage() for record in caplog.records if record.levelno >= logging.WARNING] == []


def posting_device(blueprint, hardware, monkeypatch, data):
    from pandas import DataFrame, date_range

    monkeypatch.setenv('SC_BEARER', 'token')
    device = make_device(blueprint, hardware)
    device.__get_channels__()
    device.url, device._headers = 'https://api.example/v0/devices/1', {}
    next(item for item in device._channels if item.name == 'NO2').id = 123
    device.data = DataFrame(data, index=date_range('2026-01-01', periods=2, freq='min', tz='UTC'))
    return device


def test_a_post_that_times_out_fails(blueprint, hardware, monkeypatch):
    import asyncio
    from requests.exceptions import ReadTimeout
    from smartcitizen_connector.device import device as device_module

    def post(*args, **kwargs):
        raise ReadTimeout('read timed out')

    monkeypatch.setattr(device_module, 'post', post)
    device = posting_device(blueprint, hardware, monkeypatch, {'NO2': [1.0, 2.0]})

    assert asyncio.run(device.post_data(columns='channels', max_retries=2)) is False
    assert device.posted == {}
    assert device.not_posted == {'NO2': 'post failed'}


def test_columns_without_readings_are_not_posted(blueprint, hardware, monkeypatch):
    import asyncio

    device = posting_device(blueprint, hardware, monkeypatch, {'NO2': [None, None]})

    assert asyncio.run(device.post_data(columns='channels', dry_run=True)) is True
    assert device.posted == {}
    assert device.not_posted == {'NO2': 'no readings'}


def test_columns_asked_for_by_name_say_why_they_are_not_posted(blueprint, hardware, monkeypatch):
    import asyncio

    device = posting_device(blueprint, hardware, monkeypatch, {'NO2': [1.0, 2.0]})
    device.json.data = SimpleNamespace(sensors=[])

    assert asyncio.run(device.post_data(columns=['NO2', 'NO2_AE', 'CO'], dry_run=True)) is True
    assert device.posted == {'NO2': {'id': 123, 'readings': 2}}
    assert device.not_posted == {'NO2_AE': 'not in data', 'CO': 'not a sensor or channel'}


def test_a_partial_post_says_what_was_posted(blueprint, hardware, monkeypatch):
    import asyncio
    from types import SimpleNamespace as Response
    from smartcitizen_connector.device import device as device_module

    responses = iter([Response(status_code=200), Response(status_code=500)])
    monkeypatch.setattr(device_module, 'post', lambda *args, **kwargs: next(responses))
    device = posting_device(blueprint, hardware, monkeypatch, {'NO2': [1.0, 2.0]})

    # Chunks of one reading: the first is posted, the second fails
    assert asyncio.run(device.post_data(columns='channels', chunk_size=1, max_retries=1)) is False
    assert device.posted == {'NO2': {'id': 123, 'readings': 1}}
    assert device.not_posted == {'NO2': 'post failed after 1 of 2 readings'}


def test_filled_columns_without_values_are_not_posted(blueprint, hardware, monkeypatch):
    import asyncio

    device = posting_device(blueprint, hardware, monkeypatch, {'NO2': [None, None]})

    assert asyncio.run(device.post_data(columns='channels', clean_na='fill', dry_run=True)) is True
    assert device.not_posted == {'NO2': 'no readings'}


# Blueprints of the hardware, selected by kind

def served_blueprints(monkeypatch, blueprint):
    ''' Blueprints served by name, of each kind. Returns the requested urls '''
    from smartcitizen_connector.device import device as device_module

    bodies = {'sc_air': blueprint,
              'sc_air_baseline': dict(blueprint, meta={'kind': 'long'}),
              'backup': {'meta': {'kind': 'backup'}, 'channels': []}}
    requested = []

    def safe_get(url, headers=None):
        requested.append(url)
        name = url.rsplit('/', 1)[-1].removesuffix('.json')
        if name not in bodies:
            raise ValueError(f'{url} not found')
        return SimpleNamespace(json=lambda: bodies[name])

    monkeypatch.setattr(device_module, 'safe_get', safe_get)
    return requested


def device_of(hardware, names, kind=None):
    device = make_device(None, dict(hardware, blueprints=names))
    device._blueprint_kind = kind
    device._blueprints = {}
    device._blueprint_name = None
    return device


@pytest.mark.parametrize('names, kind, expected', [
    (['sc_air'], None, 'sc_air'),
    (['sc_air', 'backup'], None, 'sc_air'),
    # Without a process blueprint, the long one
    (['sc_air_baseline'], None, 'sc_air_baseline'),
    (['sc_air', 'sc_air_baseline'], None, 'sc_air'),
    (['sc_air', 'sc_air_baseline'], 'process', 'sc_air'),
    (['sc_air', 'sc_air_baseline'], 'long', 'sc_air_baseline'),
    # Only backed up: nothing to process
    (['backup'], None, None),
    (['sc_air'], 'long', None),
    ([], None, None),
])
def test_blueprint_of_the_hardware_by_kind(monkeypatch, blueprint, hardware, names, kind, expected):
    served_blueprints(monkeypatch, blueprint)
    device = device_of(hardware, names, kind)

    assert device.__check_blueprint__() is (expected is not None)
    assert device.blueprint_name == expected
    if expected is not None:
        device.__fill_blueprint__()
        assert channel(device, 'NO2')['kwargs']['alphasense_id'] == '212830246'


def test_blueprints_by_kind(monkeypatch, blueprint, hardware):
    served_blueprints(monkeypatch, blueprint)
    device = device_of(hardware, ['sc_air', 'sc_air_baseline'], 'long')
    device.__check_blueprint__()

    assert device.blueprints == {'process': 'sc_air', 'long': 'sc_air_baseline'}
    assert device.blueprint_kind == 'long'


def test_the_blueprint_url_of_the_device_is_not_used(monkeypatch, blueprint, hardware):
    from smartcitizen_connector.device import device as device_module

    requested = served_blueprints(monkeypatch, blueprint)
    device = device_of(hardware, ['sc_air'])
    device.json.postprocessing = SimpleNamespace(blueprint_url='https://example.com/own.json', hardware_url='SCAS1')

    device.__check_blueprint__()

    assert device.blueprint_name == 'sc_air'
    assert requested == [f'{device_module.config.BASE_POSTPROCESSING_URL}blueprints/sc_air.json']


def test_a_blueprint_that_cannot_be_loaded_is_left_out(monkeypatch, blueprint, hardware):
    served_blueprints(monkeypatch, blueprint)
    device = device_of(hardware, ['missing', 'sc_air_baseline'])

    assert device.__check_blueprint__() is True
    assert device.blueprints == {'long': 'sc_air_baseline'}


def test_unknown_blueprint_kind():
    with pytest.raises(ValueError, match='Unknown blueprint kind'):
        SCDevice(id=1, blueprint_kind='other')


@pytest.mark.parametrize('body', [['not', 'a', 'blueprint'], {'meta': 'process', 'channels': []},
                                  {'meta': {}, 'channels': 'NO2'}, {'meta': {'kind': 'process'}}])
def test_an_invalid_blueprint_is_left_out(monkeypatch, blueprint, hardware, body):
    from smartcitizen_connector.device import device as device_module

    served_blueprints(monkeypatch, blueprint)
    valid = device_module.safe_get

    def safe_get(url, headers=None):
        if url.endswith('/broken.json'):
            return SimpleNamespace(json=lambda: body)
        return valid(url, headers)

    monkeypatch.setattr(device_module, 'safe_get', safe_get)
    device = device_of(hardware, ['broken', 'sc_air_baseline'])

    # The other blueprint of the hardware is still selected
    assert device.__check_blueprint__() is True
    assert device.blueprints == {'long': 'sc_air_baseline'}


def test_the_first_channel_of_a_name_gets_the_hardware(blueprint, hardware):
    no2 = next(item for item in blueprint['channels'] if item['name'] == 'NO2')
    blueprint['channels'].append(dict(no2, function='alphasense_als'))
    device = make_device(blueprint, hardware)

    device.__get_channels__()

    first, second = [item for item in device.channels if item['name'] == 'NO2']
    assert first['kwargs']['alphasense_id'] == '212830246'
    assert second['kwargs']['alphasense_id'] is None


def test_apply_blueprint_sets_its_name(blueprint, hardware):
    device = make_device(blueprint, hardware)
    device._blueprint_name = 'sc_air'
    device.__fill_blueprint__()

    device.apply_blueprint(dict(blueprint, meta={'kind': 'long'}), name='sc_air_baseline')

    assert (device.blueprint_name, device.blueprint_kind) == ('sc_air_baseline', 'long')
