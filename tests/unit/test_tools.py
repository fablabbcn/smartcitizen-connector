import pytest
from numpy import nan
from pandas import DataFrame

from smartcitizen_connector.models import Export
from smartcitizen_connector.tools import (clean, convert_freq_to_rollup, dict_fmerge,
                                          find_by_field, get_alphasense, get_pt_temp,
                                          process_headers, url_checker)


@pytest.mark.parametrize('sensor_id, pollutant, electrode', [
    ('212830246', 'NO2', 'NO2'),
    ('214920348', 'O3', 'OX'),
    ('202760040', 'NO2', 'NO2'),
    ('204042163', 'O3', 'OX'),
    ('162031254', 'CO', 'CO'),
])
def test_get_alphasense(sensor_id, pollutant, electrode):
    result = get_alphasense('AS_48_32', sensor_id)

    assert result == [
        {pollutant: {'kwargs': {'we': 'ADC_48_3', 'ae': 'ADC_48_2',
                                't': 'EC_SENSOR_TEMP', 'alphasense_id': sensor_id}}},
        {f'{electrode}_WE': {'kwargs': {'channel': 'ADC_48_3'}}},
        {f'{electrode}_AE': {'kwargs': {'channel': 'ADC_48_2'}}},
    ]


def test_get_alphasense_other_slot():
    result = get_alphasense('AS_49_10', '212830246')

    assert result[0]['NO2']['kwargs']['we'] == 'ADC_49_1'
    assert result[0]['NO2']['kwargs']['ae'] == 'ADC_49_0'


def test_get_pt_temp():
    result = get_pt_temp('PT_49_23', '10-002911')

    assert result == [
        {'ASPT1000': {'kwargs': {'pt1000plus': 'ADC_49_2', 'pt1000minus': 'ADC_49_3',
                                 'afe_id': '10-002911'}}},
        {'PT1000_POS': {'kwargs': {'channel': 'ADC_49_2'}}},
    ]


@pytest.mark.parametrize('string, expected', [
    ('https://raw.githubusercontent.com/fablabbcn/smartcitizen-data/master/hardware/SCAS220013.json',
     ['https://raw.githubusercontent.com/fablabbcn/smartcitizen-data/master/hardware/SCAS220013.json']),
    ('SCAS220013', []),
    ('', []),
    (None, []),
])
def test_url_checker(string, expected):
    assert url_checker(string) == expected


def test_dict_fmerge():
    base = {'kwargs': {'channel': None, 'limits': [0, 1]}, 'name': 'A'}
    merged = dict_fmerge(base, {'kwargs': {'channel': 'ADC_48_3'}, 'unit': 'V'})

    assert merged == {'kwargs': {'channel': 'ADC_48_3', 'limits': [0, 1]}, 'name': 'A', 'unit': 'V'}
    assert base['kwargs']['channel'] is None


def test_dict_fmerge_without_new_keys():
    merged = dict_fmerge({'a': 1}, {'a': 2, 'b': 3}, add_keys=False)

    assert merged == {'a': 2}


def test_find_by_field():
    exports = [Export(name='all'), Export(name='clean')]

    assert find_by_field(exports, 'clean', 'name') is exports[1]
    assert find_by_field(exports, 'missing', 'name') is None


@pytest.mark.parametrize('freq, rollup', [
    ('1Min', '1m'),
    ('5Min', '5m'),
    ('1H', '1h'),
    ('10S', '10s'),
    ('1D', '1d'),
    ('1X', None),
])
def test_convert_freq_to_rollup(freq, rollup):
    assert convert_freq_to_rollup(freq) == rollup


def test_clean_drop():
    df = DataFrame({'a': [1, nan, nan], 'b': [1, 2, nan]})

    assert len(clean(df.copy(), 'drop')) == 2
    assert len(clean(df.copy(), 'drop', how='any')) == 1
    assert len(clean(df.copy(), None)) == 3


def test_process_headers():
    headers = {
        'total': '120',
        'per-page': '100',
        'link': '<https://api/v0/devices?page=2>; rel="next", <https://api/v0/devices?page=2>; rel="last"',
    }

    assert process_headers(headers) == {
        'total_pages': '120',
        'per_page': '100',
        'next': 'https://api/v0/devices?page=2',
        'last': 'https://api/v0/devices?page=2',
    }
