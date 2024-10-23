from smartcitizen_connector.models import Measurement
from smartcitizen_connector._config import config
from smartcitizen_connector.tools import *
from pydantic import TypeAdapter
from typing import Optional, List
from smartcitizen_connector.handler import HttpHandler

# TODO - Can this inherit from Measurement?
class MeasurementHandler(HttpHandler):

    def __init__(self, id: int = None, **kwargs):
        self.id = id
        super().__init__(config.MEASUREMENTS_URL)

        if self.id is not None:
            r = self.get()
            self.model = TypeAdapter(Measurement).validate_python(r.json())
        else:
            self.model = Measurement(**kwargs)

    def __getattr__(self, attr):
        return self.model.__getattribute__(attr)

def get_measurements():
    isn = True
    result = list()
    url = config.MEASUREMENTS_URL
    while isn:
        r = get(url)
        r.raise_for_status()
        # If status code OK, retrieve data
        h = process_headers(r.headers)
        result += TypeAdapter(List[Measurement]).validate_python(r.json())

        if 'next' in h:
            if h['next'] == url: isn = False
            elif h['next'] != url: url = h['next']
        else:
            isn = False

    return result
