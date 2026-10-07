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
