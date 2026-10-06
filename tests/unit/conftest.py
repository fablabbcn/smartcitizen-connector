import pytest


@pytest.fixture
def blueprint():
    ''' Minimal blueprint with the channels filled by hardware info '''
    return {
        'meta': {'documentation': 'https://docs.smartcitizen.me/'},
        'channels': [
            {'name': 'PT1000_POS', 'function': 'channel_names', 'kwargs': {'channel': None}},
            {'name': 'ASPT1000', 'function': 'alphasense_pt1000',
             'kwargs': {'pt1000minus': None, 'pt1000plus': None, 'afe_id': None},
             'depends_on': ['PT1000_POS']},
            {'name': 'NO2_WE', 'function': 'channel_names', 'kwargs': {'channel': None}},
            {'name': 'NO2_AE', 'function': 'channel_names', 'kwargs': {'channel': None}},
            {'name': 'NO2', 'function': 'alphasense_803_04',
             'kwargs': {'we': None, 'ae': None, 't': 'EC_SENSOR_TEMP', 'alphasense_id': None},
             'depends_on': ['NO2_WE', 'NO2_AE']},
        ],
        'checks': [
            {'name': 'GAPS', 'function': 'find_gaps', 'store_qc': True, 'clean': False,
             'kwargs': {'default_gap_size_minutes': 5}},
        ],
        'exports': [
            {'name': 'all'},
            {'name': 'clean', 'columns': ['NO2']},
        ],
    }


@pytest.fixture
def hardware():
    ''' Hardware file as stored in smartcitizen-data/hardware '''
    return {
        'blueprint_url': 'https://raw.githubusercontent.com/fablabbcn/smartcitizen-data/master/blueprints/sc_air.json',
        'description': '1SEN55-2ELEC-AFE',
        'versions': [
            {'ids': {'AS_48_32': '212830246', 'PT_49_23': '10-002911'}, 'from': '2024-04-01', 'to': None}
        ],
    }
