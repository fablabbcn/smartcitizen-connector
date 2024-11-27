import pytest
from smartcitizen_connector import search_by_query
from requests import HTTPError

def test_search():
    d_id = 4498
    d_uuid = "f7bc1d04-cebc-4989-b701-9912ba2ab20f"
    u_uuid = "5a3bbb8c-af24-492c-a21a-1d2580f75ec6"

    raised_error = False
    with pytest.raises(HTTPError) as exc_info:
        search_by_query(
            endpoint = 'devices',
            search_items=[{
                'key': "device_token",
                'search_matcher': "eq",
                'value':"eeb370"}
                ]
            )

    d = search_by_query(endpoint = 'devices',
        search_items=[{
            'key': "name",
            'search_matcher': "eq",
            'value': "PlatformTest MQTT"}
            ]
        )
    u = search_by_query(endpoint = 'users',
        search_items=[{
            'key': "username",
            'search_matcher': "eq",
            'value': "oscgonfer"}
            ]
        )

    assert u.uuid.values[0] == u_uuid, resp.text
    assert d.uuid.values[0] == d_uuid, resp.text
    assert exc_info.type is HTTPError
