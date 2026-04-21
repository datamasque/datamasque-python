"""Tests for pagination infrastructure (Page, IfmPage, _iter_paginated, _iter_ifm_paginated)."""

import requests_mock
from pydantic import BaseModel

from datamasque.client import DataMasqueIfmClient, DataMasqueIfmInstanceConfig
from datamasque.client.models.pagination import IfmPage, Page


class Item(BaseModel):
    id: int
    name: str


def test_page_model_validate_round_trip():
    raw = {
        "count": 2,
        "next": "http://test/api/items/?limit=1&offset=1",
        "previous": None,
        "results": [{"id": 1, "name": "a"}],
    }
    page = Page[Item].model_validate(raw)
    assert page.count == 2
    assert page.next == "http://test/api/items/?limit=1&offset=1"
    assert page.previous is None
    assert len(page.results) == 1
    assert isinstance(page.results[0], Item)
    assert page.results[0].id == 1


def test_page_preserves_extra_fields():
    raw = {
        "count": 0,
        "results": [],
        "some_extra": "value",
    }
    page = Page[Item].model_validate(raw)
    assert page.model_extra["some_extra"] == "value"


def test_ifm_page_model_validate_round_trip():
    raw = {
        "items": [{"id": 1, "name": "x"}, {"id": 2, "name": "y"}],
        "total": 5,
        "limit": 2,
        "offset": 0,
    }
    page = IfmPage[Item].model_validate(raw)
    assert page.total == 5
    assert len(page.items) == 2
    assert page.items[1].name == "y"


def test_iter_paginated_follows_next_urls(client):
    with requests_mock.Mocker() as m:
        m.get(
            "http://test-server/api/items/?limit=2&offset=0",
            json={
                "count": 3,
                "next": "http://test-server/api/items/?limit=2&offset=2",
                "previous": None,
                "results": [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}],
            },
        )
        m.get(
            "http://test-server/api/items/?limit=2&offset=2",
            json={
                "count": 3,
                "next": None,
                "previous": "http://test-server/api/items/?limit=2&offset=0",
                "results": [{"id": 3, "name": "c"}],
            },
        )

        items = list(client._iter_paginated("/api/items/", model=Item, page_size=2))

    assert len(items) == 3
    assert [i.id for i in items] == [1, 2, 3]
    assert m.call_count == 2


def test_iter_paginated_stops_when_next_is_none(client):
    with requests_mock.Mocker() as m:
        m.get(
            "http://test-server/api/items/?limit=100&offset=0",
            json={
                "count": 1,
                "next": None,
                "results": [{"id": 1, "name": "only"}],
            },
        )

        items = list(client._iter_paginated("/api/items/", model=Item))

    assert len(items) == 1
    assert m.call_count == 1


def test_iter_ifm_paginated_walks_pages():
    config = DataMasqueIfmInstanceConfig(
        admin_server_base_url="http://admin.test",
        ifm_base_url="http://ifm.test",
        username="u",
        password="p",
    )
    ifm_client = DataMasqueIfmClient(config)
    ifm_client.access_token = "tok"

    with requests_mock.Mocker() as m:
        m.get(
            "http://ifm.test/items/?limit=2&offset=0",
            json={
                "items": [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}],
                "total": 3,
                "limit": 2,
                "offset": 0,
            },
        )
        m.get(
            "http://ifm.test/items/?limit=2&offset=2",
            json={
                "items": [{"id": 3, "name": "c"}],
                "total": 3,
                "limit": 2,
                "offset": 2,
            },
        )

        items = list(ifm_client._iter_ifm_paginated("items/", model=Item, page_size=2))

    assert len(items) == 3
    assert [i.id for i in items] == [1, 2, 3]


def test_iter_ifm_paginated_handles_empty_page():
    config = DataMasqueIfmInstanceConfig(
        admin_server_base_url="http://admin.test",
        ifm_base_url="http://ifm.test",
        username="u",
        password="p",
    )
    ifm_client = DataMasqueIfmClient(config)
    ifm_client.access_token = "tok"

    with requests_mock.Mocker() as m:
        m.get(
            "http://ifm.test/items/?limit=100&offset=0",
            json={"items": [], "total": 0, "limit": 100, "offset": 0},
        )

        items = list(ifm_client._iter_ifm_paginated("items/", model=Item))

    assert items == []
