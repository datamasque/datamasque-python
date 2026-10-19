"""Tests for listing resources with the DataMasque client."""

from datetime import datetime
from typing import Optional

import pytest
import requests_mock

from datamasque.client import (
    ConnectionResource,
    DataMasqueApiError,
    DataMasqueClient,
    MaskType,
    Project,
    ProjectId,
    Resource,
    ResourceTypeName,
    RulesetResource,
)

RULESET = {
    "id": "0b6c2f0e-4a0d-4d8e-9a57-2f0f3c1d9e11",
    "name": "mask_customers",
    "project": "7-banking",
    "created": "2026-10-01T10:00:00Z",
    "modified": "2026-10-02T10:00:00Z",
    "mask_type": "database",
}
CONNECTION = {
    "id": "5e3b8a40-7c2d-4f61-b1a9-0d4e6f2c8b77",
    "name": "customers_db",
    "project": "7-banking",
    "created": None,
    "modified": None,
    "mask_type": "database",
}
BUILT_IN_SEED = {
    "id": "9a1d3c55-2b7e-4c0a-8f16-3e5d7b9c1a24",
    "name": "first_names.csv",
    "project": None,
    "created": "2026-01-01T00:00:00Z",
    "modified": "2026-01-01T00:00:00Z",
}


def page(results: dict[str, list[dict]], next_url: Optional[str] = None) -> dict:
    return {
        "count": sum(len(rows) for rows in results.values()),
        "next": next_url,
        "previous": None,
        "results": results,
    }


def test_list_resources_groups_them_by_type_name(client: DataMasqueClient) -> None:
    with requests_mock.Mocker() as m:
        m.get(
            "http://test-server/api/resources/",
            json=page({"rulesets": [RULESET], "connections": [CONNECTION], "seeds": [BUILT_IN_SEED]}),
        )
        resources = client.list_resources()

    assert sorted(resources) == ["connections", "rulesets", "seeds"]
    (ruleset,) = resources["rulesets"]
    assert isinstance(ruleset, RulesetResource)
    assert ruleset.id == RULESET["id"]
    assert ruleset.name == "mask_customers"
    assert ruleset.project == "7-banking"
    assert ruleset.created == datetime.fromisoformat("2026-10-01T10:00:00+00:00")
    assert ruleset.mask_type is MaskType.database
    (connection,) = resources["connections"]
    assert isinstance(connection, ConnectionResource)
    assert connection.created is None
    (seed,) = resources["seeds"]
    assert type(seed) is Resource
    assert seed.project is None


def test_list_resources_sends_its_filters(client: DataMasqueClient) -> None:
    retail = Project.model_validate(
        {
            "id": 8,
            "reference": "8-retail",
            "name": "Retail",
            "created": "2026-10-01T10:00:00Z",
            "updated": "2026-10-01T10:00:00Z",
        }
    )
    with requests_mock.Mocker() as m:
        m.get("http://test-server/api/resources/", json=page({"rulesets": [], "connections": []}))
        client.list_resources(
            projects=[ProjectId(7), retail],
            types=[ResourceTypeName.rulesets, ResourceTypeName.connections],
            search="customers",
        )

    assert m.last_request.qs == {
        "project": ["7", "8"],
        "type": ["rulesets", "connections"],
        "search": ["customers"],
        "limit": ["500"],
        "offset": ["0"],
    }


@pytest.mark.parametrize(
    ("projects", "types", "expected"),
    [
        ([], None, {}),
        ([], [ResourceTypeName.rulesets], {"rulesets": []}),
        ([ProjectId(7)], [], {}),
    ],
)
def test_list_resources_with_an_empty_filter_returns_no_resources_without_a_request(
    client: DataMasqueClient,
    projects: list[ProjectId],
    types: Optional[list[ResourceTypeName]],
    expected: dict[str, list[Resource]],
) -> None:
    with requests_mock.Mocker() as m:
        resources = client.list_resources(projects=projects, types=types)

    assert resources == expected
    assert m.call_count == 0


def test_list_resources_follows_every_page_and_merges_the_groups(client: DataMasqueClient) -> None:
    next_url = "http://test-server/api/resources/?limit=2&offset=2"
    with requests_mock.Mocker() as m:
        m.get(
            "http://test-server/api/resources/",
            [
                {"json": page({"rulesets": [RULESET], "connections": [CONNECTION]}, next_url=next_url)},
                {"json": page({"rulesets": [RULESET | {"id": "c3d1e5f7-0000-4000-8000-000000000001"}]})},
            ],
        )
        resources = client.list_resources(page_size=2)

    assert [ruleset.id for ruleset in resources["rulesets"]] == [RULESET["id"], "c3d1e5f7-0000-4000-8000-000000000001"]
    assert [connection.id for connection in resources["connections"]] == [CONNECTION["id"]]
    assert m.call_count == 2
    assert m.request_history[1].qs == {"limit": ["2"], "offset": ["2"]}


def test_list_resources_raises_for_a_caller_who_cannot_list_them(client: DataMasqueClient) -> None:
    with requests_mock.Mocker() as m:
        m.get("http://test-server/api/resources/", status_code=403, json={"detail": "Forbidden"})

        with pytest.raises(DataMasqueApiError) as raised:
            client.list_resources()

    assert raised.value.response.status_code == 403
