from smartcitizen_connector.models import (Sensor, Measurement)
from smartcitizen_connector._config import config
from smartcitizen_connector.tools import *
from pydantic import TypeAdapter
from typing import List
from requests import get

class SCSensor:
    id: int
    url: str
    page: str
    json: Sensor

    def __init__(self, id):
        self.id = id
        self.url = f'{config.SENSORS_URL}{self.id}'
        self.method = 'async'
        r = self.__safe_get__(self.url)
        self.json = TypeAdapter(Sensor).validate_python(r.json())

    def __safe_get__(self, url):
        r = get(url)
        r.raise_for_status()

        return r

def get_sensors():
    isn = True
    result = list()
    url = config.SENSORS_URL
    while isn:
        r = get(url)
        r.raise_for_status()
        # If status code OK, retrieve data
        h = process_headers(r.headers)
        result += TypeAdapter(List[Sensor]).validate_python(r.json())

        if 'next' in h:
            if h['next'] == url: isn = False
            elif h['next'] != url: url = h['next']
        else:
            isn = False
    return result
