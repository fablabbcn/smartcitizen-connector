import pytest
from smartcitizen_connector import search_by_query
from requests import HTTPError

def test_connector():
    d_id = 4498
    d_uuid = "f7bc1d04-cebc-4989-b701-9912ba2ab20f"
    u_uuid = "76affe1f-a1b8-4a47-bc0b-07bb8f9c700f"

    raised_error = False
    with pytest.raises(HTTPError) as exc_info:
        search_by_query(endpoint = 'devices', key="device_token", search_matcher="eq", value="eeb370")

    d = search_by_query(endpoint = 'devices', key="name", search_matcher="eq", value="PlatformTest MQTT")
    u = search_by_query(endpoint = 'users', key="username", search_matcher="eq", value="team")

    assert u.uuid.values[0] == u_uuid, resp.text
    assert d.uuid.values[0] == d_uuid, resp.text
    assert exc_info.type is HTTPError
