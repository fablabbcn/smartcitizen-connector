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
    SENSORS_URL = API_URL + 'sensors/'
    MEASUREMENTS_URL = API_URL + 'measurements/'
    EXPERIMENTS_URL = API_URL + 'experiments/'
    USERS_URL = API_URL + 'users/'
    FRONTEND_URL = 'https://smartcitizen.me/kits/'
    BASE_POSTPROCESSING_URL='https://raw.githubusercontent.com/fablabbcn/smartcitizen-data/master/'
    API_SEARCH_URL = API_URL + "search?q="

    # Alphasense sensor codes
    _as_sensor_codes =  {
        '132':  'ASA4_CO',
        '133':  'ASA4_H2S',
        '130':  'ASA4_NO',
        '212':  'ASA4_NO2',
        '214':  'ASA4_OX',
        '134':  'ASA4_SO2',
        '162':  'ASB4_CO',
        '133':  'ASB4_H2S',#
        '130':  'ASB4_NO', #
        '202':  'ASB4_NO2',
        '204':  'ASB4_OX',
        '164':  'ASB4_SO2'
    }

    _max_retries = 3
    _retry_interval = 10
    _retry_codes = [
        HTTPStatus.TOO_MANY_REQUESTS,
        HTTPStatus.INTERNAL_SERVER_ERROR,
        HTTPStatus.BAD_GATEWAY,
        HTTPStatus.SERVICE_UNAVAILABLE,
        HTTPStatus.GATEWAY_TIMEOUT,
    ]

    _max_concurrent_requests = 30

config = Config()