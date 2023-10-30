import pytest
from smartcitizen_connector import SCDevice
from smartcitizen_connector.utils import localise_date
import asyncio

def test_device():
    id = 16549
    freq = '1Min'
    resample = False
    uuid = "d030cb8a-2c2a-429e-9f04-416888708193"
    min_date = '2023-07-29T09:00:06Z'
    max_date = None

    d = SCDevice(id)
    asyncio.run(d.get_data(
        min_date = localise_date(min_date, d.timezone),
        max_date = localise_date(max_date, d.timezone),
        freq = freq,
        clean_na = None,
        resample = resample)
    )

    d0 = d.data.index[0].tz_convert('UTC').strftime('%Y-%m-%dT%H:%M:%SZ')

    assert d.json.id == 16549, resp.text
    assert d.data is not None, resp.text
    assert d.json.uuid == uuid, resp.text
    assert d0 == min_date, resp.text