import pytest
from smartcitizen_connector import get_measurements

def test_measurement():
    id = 7
    name = 'battery'

    measurements = get_measurements()

    assert measurements[0].id == id
    assert measurements[0].name == name
