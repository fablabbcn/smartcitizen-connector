from http import HTTPStatus
import logging
import os

# Output config
class Config():
    log_level = logging.INFO

    # Base URL for all methods
    if 'API_URL' in os.environ:
        API_URL = os.environ['API_URL']
    else:
        API_URL = 'https://api.smartcitizen.me/v0/'
    DEVICES_URL =  API_URL + 'devices/'
    WORLD_MAP_URL =  API_URL + 'devices/world_map'
    SENSORS_URL = API_URL + 'sensors/'
    MEASUREMENTS_URL = API_URL + 'measurements/'
    EXPERIMENTS_URL = API_URL + 'experiments/'
    USERS_URL = API_URL + 'users/'
    FRONTEND_URL = 'https://smartcitizen.me/kits/'
    # Hardware and blueprints (e.g. flows: https://flows.smartcitizen.me/api/v1/)
    BASE_POSTPROCESSING_URL = os.environ.get('BASE_POSTPROCESSING_URL',
        'https://raw.githubusercontent.com/fablabbcn/smartcitizen-data/master/').rstrip('/') + '/'
    API_SEARCH_URL = API_URL + "search?q="

    # Alphasense sensor codes: first three digits of the serial number. A-series sensors go on AFE
    # boards, B-series on ISBs. 132, 162, 164, 202, 204, 212 and 214 checked against the datasheet
    # sensitivities and the boards of the hardware that mounts them. 160 and 163 follow the pattern
    # (B-series = A-series + 30 for CO, SO2, NO and H2S); 130, 133 and 134 are not in use yet
    _as_sensor_codes =  {
        '130':  'ASA4_NO',
        '132':  'ASA4_CO',
        '133':  'ASA4_H2S',
        '134':  'ASA4_SO2',
        '212':  'ASA4_NO2',
        '214':  'ASA4_OX',
        '160':  'ASB4_NO',
        '162':  'ASB4_CO',
        '163':  'ASB4_H2S',
        '164':  'ASB4_SO2',
        '202':  'ASB4_NO2',
        '204':  'ASB4_OX'
    }

    # Alphasense model of each sensor type
    _as_sensor_models = {
        'ASA4_CO':  'CO-A4',
        'ASA4_H2S': 'H2S-A4',
        'ASA4_NO':  'NO-A4',
        'ASA4_NO2': 'NO2-A43F',
        'ASA4_OX':  'OX-A431',
        'ASA4_SO2': 'SO2-A4',
        'ASB4_CO':  'CO-B4',
        'ASB4_H2S': 'H2S-B4',
        'ASB4_NO':  'NO-B4',
        'ASB4_NO2': 'NO2-B43F',
        'ASB4_OX':  'OX-B431',
        'ASB4_SO2': 'SO2-B4'
    }

    # Alphasense AFE boards (for A-series sensors, with a PT1000): serial numbers <code>-<number>,
    # e.g. 10-002911. The code does not tell the board model
    _afe_board_codes = ('10', '12', '17')

    # Requests to the API: seconds to connect and to wait for a response. Generous, so that slow
    # responses still arrive, but a request never hangs forever (e.g. a sync with no end)
    _timeout = (30, 300)

    _max_retries = 3
    _retry_interval = 10
    _retry_codes = [
        HTTPStatus.TOO_MANY_REQUESTS,
        HTTPStatus.INTERNAL_SERVER_ERROR,
        HTTPStatus.BAD_GATEWAY,
        HTTPStatus.SERVICE_UNAVAILABLE,
        HTTPStatus.GATEWAY_TIMEOUT,
    ]

    _max_concurrent_requests = 5

config = Config()
