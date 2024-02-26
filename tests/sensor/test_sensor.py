import pytest
from smartcitizen_connector import get_sensors

def test_sensor():
    id = 3
    name = 'DHT22'

    sensors = get_sensors()

    assert sensors[0].id == id
    assert sensors[0].name == name
