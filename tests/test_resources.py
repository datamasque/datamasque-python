"""Tests for listing and moving resources with the DataMasque client."""

from datetime import datetime
from typing import Optional

import pytest
import requests_mock

from datamasque.client import (
    ConnectionResource,
    DataMasqueApiError,
    DataMasqueClient,
    ListedConnectionResource,
    ListedResource,
    ListedRulesetResource,
    MaskType,
    Project,
    ProjectId,
    Resource,
    ResourceMoveConflictReason,
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
LIBRARY = {
    "id": "4f2a6c18-9d3b-4e75-a0c2-7b1e5d9f3a60",
    "name": "customer_masks",
    "project": "7-banking",
    "created": "2026-10-01T10:00:00Z",
    "modified": "2026-10-01T10:00:00Z",
}
# A resource in the listing carries its links; one in a link does not.
NO_LINKS = {"uses": {}, "used_by": {}}


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
            json=page(
                {
                    "rulesets": [RULESET | NO_LINKS],
                    "connections": [CONNECTION | NO_LINKS],
                    "seeds": [BUILT_IN_SEED | NO_LINKS],
                }
            ),
        )
        resources = client.list_resources()

    assert sorted(resources) == ["connections", "rulesets", "seeds"]
    (ruleset,) = resources["rulesets"]
    assert isinstance(ruleset, ListedRulesetResource)
    assert isinstance(ruleset, RulesetResource)
    assert ruleset.id == RULESET["id"]
    assert ruleset.name == "mask_customers"
    assert ruleset.project == "7-banking"
    assert ruleset.created == datetime.fromisoformat("2026-10-01T10:00:00+00:00")
    assert ruleset.mask_type is MaskType.database
    (connection,) = resources["connections"]
    assert isinstance(connection, ListedConnectionResource)
    assert connection.created is None
    (seed,) = resources["seeds"]
    assert type(seed) is ListedResource
    assert seed.project is None


def test_list_resources_lists_each_link_as_its_types_summary(client: DataMasqueClient) -> None:
    listed_ruleset = RULESET | {
        "uses": {"connections": [CONNECTION], "ruleset_libraries": [LIBRARY]},
        "used_by": {},
    }
    with requests_mock.Mocker() as m:
        m.get("http://test-server/api/resources/", json=page({"rulesets": [listed_ruleset]}))
        resources = client.list_resources()

    (ruleset,) = resources[ResourceTypeName.rulesets]
    assert sorted(ruleset.uses) == [ResourceTypeName.connections, ResourceTypeName.ruleset_libraries]
    (connection,) = ruleset.uses[ResourceTypeName.connections]
    assert type(connection) is ConnectionResource
    assert connection.mask_type is MaskType.database
    (library,) = ruleset.uses[ResourceTypeName.ruleset_libraries]
    assert type(library) is Resource
    assert library.name == "customer_masks"
    assert ruleset.used_by == {}


def test_move_resources_moves_a_listed_resources_links(client: DataMasqueClient) -> None:
    listed_ruleset = RULESET | {"uses": {"connections": [CONNECTION]}, "used_by": {}}
    with requests_mock.Mocker() as m:
        m.get("http://test-server/api/resources/", json=page({"rulesets": [listed_ruleset]}))
        m.post("http://test-server/api/resources/move/", json={"moved": 1, "conflicts": []})
        (ruleset,) = client.list_resources()[ResourceTypeName.rulesets]
        client.move_resources(ruleset.uses, ProjectId(8))

    assert m.last_request.json() == {
        "destination": "8",
        "resources": [{"type": "connections", "id": CONNECTION["id"]}],
    }


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
                {
                    "json": page(
                        {"rulesets": [RULESET | NO_LINKS], "connections": [CONNECTION | NO_LINKS]},
                        next_url=next_url,
                    )
                },
                {"json": page({"rulesets": [RULESET | NO_LINKS | {"id": "c3d1e5f7-0000-4000-8000-000000000001"}]})},
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


# The move methods accept a listed `Resource` or a plain ID.
MOVING = {
    ResourceTypeName.rulesets: [RulesetResource.model_validate(RULESET)],
    ResourceTypeName.connections: [CONNECTION["id"]],
}
MOVE_REQUEST = {
    "destination": "8",
    "resources": [{"type": "rulesets", "id": RULESET["id"]}, {"type": "connections", "id": CONNECTION["id"]}],
}
NAME_CLASH = {
    "reason": "name_in_use",
    "detail": 'The destination project already contains a connection named "customers_db".',
    "type": "connections",
    "id": CONNECTION["id"],
    "name": "customers_db",
}


def test_get_resource_move_conflicts_sends_the_resources_and_destination(client: DataMasqueClient) -> None:
    with requests_mock.Mocker() as m:
        m.post("http://test-server/api/resources/move-check/", json={"conflicts": [NAME_CLASH]})
        conflicts = client.get_resource_move_conflicts(MOVING, ProjectId(8))

    assert m.last_request.json() == MOVE_REQUEST
    assert len(conflicts) == 1
    assert conflicts[0].reason is ResourceMoveConflictReason.name_in_use
    assert conflicts[0].type is ResourceTypeName.connections
    assert conflicts[0].name == "customers_db"


def test_move_resources_returns_the_count_moved_and_the_conflicts(client: DataMasqueClient) -> None:
    with requests_mock.Mocker() as m:
        m.post("http://test-server/api/resources/move/", json={"moved": 1, "conflicts": [NAME_CLASH]})
        result = client.move_resources(MOVING, ProjectId(8))

    assert m.last_request.json() == MOVE_REQUEST
    assert result.moved == 1
    assert [conflict.id for conflict in result.conflicts] == [CONNECTION["id"]]


def test_move_resources_moves_what_list_resources_returned_to_a_project(client: DataMasqueClient) -> None:
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
        m.get(
            "http://test-server/api/resources/",
            json=page({"rulesets": [RULESET | NO_LINKS], "connections": [CONNECTION | NO_LINKS]}),
        )
        m.post("http://test-server/api/resources/move/", json={"moved": 2, "conflicts": []})
        client.move_resources(client.list_resources(), retail)

    assert m.last_request.json() == MOVE_REQUEST


def test_move_resources_takes_a_listed_resources_project_as_the_destination(client: DataMasqueClient) -> None:
    listed = RulesetResource.model_validate(RULESET)
    with requests_mock.Mocker() as m:
        m.post("http://test-server/api/resources/move/", json={"moved": 2, "conflicts": []})
        client.move_resources(MOVING, listed.project)

    assert m.last_request.json()["destination"] == "7"


@pytest.mark.parametrize("resources", [{}, {ResourceTypeName.rulesets: []}])
def test_moving_no_resources_raises_without_a_request(client: DataMasqueClient, resources: dict) -> None:
    with requests_mock.Mocker() as m:
        with pytest.raises(ValueError, match="at least one resource"):
            client.move_resources(resources, ProjectId(8))

        with pytest.raises(ValueError, match="at least one resource"):
            client.get_resource_move_conflicts(resources, ProjectId(8))

    assert m.call_count == 0
