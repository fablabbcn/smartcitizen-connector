import pytest
from smartcitizen_connector import SCDevice
from smartcitizen_connector.utils import localise_date
import asyncio

def test_device():
    id = 16838
    frequency = '1Min'
    resample = False
    uuid = "80e684e5-359f-4755-aec9-30fc0c84415f"
    min_date = '2022-09-10T00:00:00Z'
    max_date = None

    d = SCDevice(id)
    asyncio.run(d.get_data(
        min_date = localise_date(min_date, d.timezone),
        max_date = localise_date(max_date, d.timezone),
        frequency = frequency,
        clean_na = None,
        resample = resample)
    )

    d0 = d.data.index[0].tz_convert('UTC').strftime('%Y-%m-%dT%H:%M:%SZ')

    assert d.json.id == id, resp.text
    assert d.data is not None, resp.text
    assert d.json.uuid == uuid, resp.text
    assert d0 == min_date, resp.text