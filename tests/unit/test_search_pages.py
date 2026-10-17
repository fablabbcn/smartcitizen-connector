import logging

import pytest

from smartcitizen_connector.search import search as search_module
from smartcitizen_connector import search_by_query


class Response:
    def __init__(self, items, headers):
        self.items, self.headers = items, headers

    def raise_for_status(self):
        pass

    def json(self):
        return self.items


@pytest.fixture
def pages(monkeypatch):
    ''' Serves the given pages in order and records the requested urls '''
    requested = []

    def serve(*pages):
        def get(url, headers=None, timeout=None):
            # A request without a timeout can hang forever
            assert timeout is not None
            requested.append(url)
            return pages[len(requested) - 1]
        monkeypatch.setattr(search_module, 'get', get)
        return requested
    return serve


def link(page):
    return {'total': '3', 'link': f'<https://api.example/devices?page={page}>; rel="next"'}


def test_sorted_by_id_with_100_per_page(pages):
    requested = pages(Response([{'id': 1}, {'id': 2}], link(2)), Response([{'id': 3}], {'total': '3'}))
    result = search_by_query(endpoint='devices', search_items=[{'key': 'postprocessing_id', 'value': 'not_null'}])
    assert requested[0].endswith('devices/?q[postprocessing_id_not_null]=1&q[s]=id asc&per_page=100')
    assert requested[1] == 'https://api.example/devices?page=2'
    assert list(result.index) == [1, 2, 3]
    assert result.attrs['total'] == 3


def test_warns_when_items_are_missing(pages, caplog):
    # Unsorted pages: item 2 comes twice and item 3 never
    pages(Response([{'id': 1}, {'id': 2}], link(2)), Response([{'id': 2}], {'total': '3'}))
    with caplog.at_level(logging.WARNING):
        result = search_by_query(endpoint='devices', search_items=[{'key': 'postprocessing_id', 'value': 'not_null'}])
    assert list(result.index) == [1, 2]
    assert result.attrs['total'] == 3
    assert 'Search returned 2 unique items, the API reports 3' in caplog.text


def test_order_and_page_size_can_be_left_out(pages):
    requested = pages(Response([{'id': 1}], {}))
    search_by_query(endpoint='users', search_items=[{'key': 'username', 'search_matcher': 'eq', 'value': 'a'}],
                    order=None, per_page=None)
    assert requested[0].endswith('users/?q[username_eq]=a')


def test_global_search_is_unchanged(pages):
    requested = pages(Response([{'id': 2}, {'id': 1}], {'total': '2'}))
    result = search_module.global_search('rooftop')
    assert requested[0].endswith('rooftop')
    assert sorted(result.index) == [1, 2]
