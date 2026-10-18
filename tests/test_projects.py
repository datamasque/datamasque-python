"""Tests for project support in the DataMasque client."""

from datetime import datetime
from typing import Callable, Union

import pytest
import requests_mock
from pydantic import BaseModel, ValidationError

from datamasque.client import (
    DataMasqueApiError,
    DataMasqueClient,
    Project,
    ProjectCreateRequest,
    ProjectDeleteConflict,
    ProjectDeleteConflictError,
    ProjectDeleteConflictReason,
    ProjectId,
)
from datamasque.client.exceptions import DataMasqueException
from datamasque.client.models.connection import ConnectionId, S3ConnectionConfig
from datamasque.client.models.discovery_config import DiscoveryConfig
from datamasque.client.models.discovery_config_library import DiscoveryConfigLibrary
from datamasque.client.models.rg_config import RGConfig
from datamasque.client.models.ruleset import Ruleset, RulesetType
from datamasque.client.models.ruleset_library import RulesetLibrary
from datamasque.client.models.table_reference import TableReference
from tests.helpers import PROJECT, PROJECT_ID, active_projects_page

CREATED_ID = "aaaaaaaa-1111-2222-3333-444444444444"

BANKING_PROJECT = {
    "id": 7,
    "reference": "7-banking",
    "name": "Banking",
    "description": "Customer banking data",
    "created": "2026-10-01T10:00:00Z",
    "updated": "2026-10-01T10:00:00Z",
}

LEGACY_PROJECT = {
    "id": 8,
    "reference": "8-legacy",
    "name": "Legacy",
    "description": "",
    "created": "2026-09-01T10:00:00Z",
    "updated": "2026-10-01T10:00:00Z",
}

CONFLICTS = [
    {
        "reason": "name_in_use",
        "detail": 'The destination project already contains a ruleset named "3407".',
        "type": "rulesets",
        "id": "0b6c2f0e-4a0d-4d8e-9a57-2f0f3c1d9e11",
        "name": "3407",
    },
    {
        "reason": "file_exists",
        "detail": 'The destination project already has a file at "projects/8-retail/seeds/names.csv".',
        "type": "seeds",
        "id": "5e3b8a40-7c2d-4f61-b1a9-0d4e6f2c8b77",
        "name": "names.csv",
        "path": "projects/8-retail/seeds/names.csv",
    },
    {
        "reason": "unexpected_file",
        "detail": 'The project directory contains "projects/7-banking/notes.txt", which belongs to no resource.',
        "path": "projects/7-banking/notes.txt",
    },
]


def test_create_request_rejects_unknown_fields() -> None:
    with pytest.raises(ValidationError, match="archived"):
        ProjectCreateRequest(name="Banking", archived=True)  # type: ignore[call-arg]


def test_create_project(client: DataMasqueClient) -> None:
    request = ProjectCreateRequest(name="Banking", description="Customer banking data")

    with requests_mock.Mocker() as m:
        m.post("http://test-server/api/projects/", json=BANKING_PROJECT, status_code=201)
        project = client.create_project(request)

    assert m.last_request.json() == {"name": "Banking", "description": "Customer banking data"}
    assert project.id == ProjectId(7)
    assert project.reference == "7-banking"
    assert project.name == "Banking"
    assert project.description == "Customer banking data"
    assert project.created == datetime.fromisoformat("2026-10-01T10:00:00+00:00")
    assert project.updated == datetime.fromisoformat("2026-10-01T10:00:00+00:00")


def test_create_project_returns_the_name_and_description_as_stored(client: DataMasqueClient) -> None:
    """Whitespace in the name and description is sent as given, and the server's stored values are returned."""
    request = ProjectCreateRequest(name="  Banking ", description=" Customer banking data  ")

    with requests_mock.Mocker() as m:
        m.post("http://test-server/api/projects/", json=BANKING_PROJECT, status_code=201)
        project = client.create_project(request)

    assert m.last_request.json() == {"name": "  Banking ", "description": " Customer banking data  "}
    assert project.name == "Banking"
    assert project.description == "Customer banking data"


def test_create_project_rejected_name_raises(client: DataMasqueClient) -> None:
    with requests_mock.Mocker() as m:
        m.post(
            "http://test-server/api/projects/",
            json={"name": ['Project name must not contain "/".']},
            status_code=400,
        )
        with pytest.raises(DataMasqueApiError) as exc_info:
            client.create_project(ProjectCreateRequest(name="bank/au"))

    assert exc_info.value.response.status_code == 400


def test_get_project_delete_conflicts_parses_each_reason(client: DataMasqueClient) -> None:
    with requests_mock.Mocker() as m:
        m.get("http://test-server/api/projects/7/delete-check/", json={"conflicts": CONFLICTS})
        conflicts = client.get_project_delete_conflicts(ProjectId(7), ProjectId(8))

    assert m.last_request.qs == {"destination": ["8"]}
    assert conflicts == [
        ProjectDeleteConflict(
            reason=ProjectDeleteConflictReason.name_in_use,
            detail='The destination project already contains a ruleset named "3407".',
            type="rulesets",
            id="0b6c2f0e-4a0d-4d8e-9a57-2f0f3c1d9e11",
            name="3407",
        ),
        ProjectDeleteConflict(
            reason=ProjectDeleteConflictReason.file_exists,
            detail='The destination project already has a file at "projects/8-retail/seeds/names.csv".',
            type="seeds",
            id="5e3b8a40-7c2d-4f61-b1a9-0d4e6f2c8b77",
            name="names.csv",
            path="projects/8-retail/seeds/names.csv",
        ),
        ProjectDeleteConflict(
            reason=ProjectDeleteConflictReason.unexpected_file,
            detail='The project directory contains "projects/7-banking/notes.txt", which belongs to no resource.',
            path="projects/7-banking/notes.txt",
        ),
    ]
    assert conflicts[0].path is None
    assert conflicts[2].type is None
    assert conflicts[2].id is None
    assert conflicts[2].name is None


def test_get_project_delete_conflicts_returns_empty_list_when_project_can_be_deleted(
    client: DataMasqueClient,
) -> None:
    with requests_mock.Mocker() as m:
        m.get("http://test-server/api/projects/7/delete-check/", json={"conflicts": []})
        conflicts = client.get_project_delete_conflicts(ProjectId(7), ProjectId(8))

    assert conflicts == []


def test_project_delete_calls_accept_projects_as_well_as_ids(client: DataMasqueClient) -> None:
    project = Project.model_validate(BANKING_PROJECT)
    destination = Project.model_validate(BANKING_PROJECT | {"id": 8, "reference": "8-retail", "name": "Retail"})

    with requests_mock.Mocker() as m:
        m.get("http://test-server/api/projects/7/delete-check/", json={"conflicts": []})
        m.delete("http://test-server/api/projects/7/", status_code=204)
        client.get_project_delete_conflicts(project, destination)
        client.delete_project(project, destination)

    assert [request.qs for request in m.request_history] == [{"destination": ["8"]}, {"destination": ["8"]}]


def test_delete_project(client: DataMasqueClient) -> None:
    with requests_mock.Mocker() as m:
        m.delete("http://test-server/api/projects/7/", status_code=204)
        client.delete_project(ProjectId(7), ProjectId(8))

    assert m.last_request.method == "DELETE"
    assert m.last_request.qs == {"destination": ["8"]}


def test_delete_project_conflict_raises_with_conflicts(client: DataMasqueClient) -> None:
    with requests_mock.Mocker() as m:
        m.delete(
            "http://test-server/api/projects/7/",
            json={"detail": 'Project "Banking" cannot be deleted.', "conflicts": CONFLICTS},
            status_code=409,
        )
        with pytest.raises(ProjectDeleteConflictError) as exc_info:
            client.delete_project(ProjectId(7), ProjectId(8))

    assert exc_info.value.response.status_code == 409
    assert str(exc_info.value) == 'Project "Banking" cannot be deleted.'
    assert [conflict.reason for conflict in exc_info.value.conflicts] == [
        ProjectDeleteConflictReason.name_in_use,
        ProjectDeleteConflictReason.file_exists,
        ProjectDeleteConflictReason.unexpected_file,
    ]
    assert exc_info.value.conflicts[0].name == "3407"


@pytest.mark.parametrize("status_code", [400, 404])
def test_delete_project_raises_a_plain_api_error_for_a_status_other_than_409(
    client: DataMasqueClient, status_code: int
) -> None:
    with requests_mock.Mocker() as m:
        m.delete("http://test-server/api/projects/7/", json={"detail": "Not found."}, status_code=status_code)
        with pytest.raises(DataMasqueApiError) as exc_info:
            client.delete_project(ProjectId(7), ProjectId(8))

    assert not isinstance(exc_info.value, ProjectDeleteConflictError)
    assert exc_info.value.response.status_code == status_code


def test_list_projects_follows_pages(client: DataMasqueClient) -> None:
    """With more active projects than fit on a page of 100, the client follows `next` to the last page."""
    projects = [
        {
            **BANKING_PROJECT,
            "id": project_id,
            "reference": f"{project_id}-project-{project_id}",
            "name": f"Project {project_id}",
        }
        for project_id in range(1, 102)
    ]
    with requests_mock.Mocker() as m:
        m.get(
            "http://test-server/api/projects/?limit=100&offset=0",
            json={
                "count": 101,
                "next": "http://test-server/api/projects/?limit=100&offset=100",
                "results": projects[:100],
            },
            complete_qs=True,
        )
        m.get(
            "http://test-server/api/projects/?limit=100&offset=100",
            json={"count": 101, "next": None, "results": projects[100:]},
            complete_qs=True,
        )
        result = client.list_projects()

    assert [project.id for project in result] == list(range(1, 102))


def test_list_projects_include_archived(client: DataMasqueClient) -> None:
    with requests_mock.Mocker() as m:
        m.get(
            "http://test-server/api/projects/?include_archived=true&limit=100&offset=0",
            json={"count": 2, "next": None, "results": [BANKING_PROJECT, LEGACY_PROJECT]},
            complete_qs=True,
        )
        projects = client.list_projects(include_archived=True)

    assert [project.id for project in projects] == [ProjectId(7), ProjectId(8)]


@pytest.mark.parametrize("project", [ProjectId(7), Project.model_validate(BANKING_PROJECT)])
def test_get_project(client: DataMasqueClient, project: Union[ProjectId, Project]) -> None:
    with requests_mock.Mocker() as m:
        m.get("http://test-server/api/projects/7/", json=BANKING_PROJECT, complete_qs=True)
        result = client.get_project(project)

    assert result.id == ProjectId(7)
    assert result.name == "Banking"


def test_get_missing_project_raises(client: DataMasqueClient) -> None:
    with requests_mock.Mocker() as m:
        m.get("http://test-server/api/projects/99/", json={"detail": "Not found."}, status_code=404)
        with pytest.raises(DataMasqueApiError) as exc_info:
            client.get_project(ProjectId(99))

    assert exc_info.value.response.status_code == 404


EMPTY_PAGE = {"count": 0, "next": None, "previous": None, "results": []}

CREATE_OR_UPDATE_WITHOUT_A_PROJECT = [
    pytest.param(
        lambda: S3ConnectionConfig(name="s3", bucket="b", base_directory=""),
        "create_or_update_connection",
        "/api/connections/",
        [],
        id="connection",
    ),
    pytest.param(
        lambda: RGConfig(name="rg", yaml="labels: []"),
        "create_or_update_rg_config",
        "/api/ruleset-generation-configs/",
        EMPTY_PAGE,
        id="rg_config",
    ),
    pytest.param(
        lambda: RulesetLibrary(name="lib", yaml="version: '1.0'"),
        "create_or_update_ruleset_library",
        "/api/ruleset-libraries/",
        EMPTY_PAGE,
        id="ruleset_library",
    ),
    pytest.param(
        lambda: DiscoveryConfig(name="dc", yaml="labels: []", config_type="database"),
        "create_or_update_discovery_config",
        "/api/discovery/configs/",
        EMPTY_PAGE,
        id="discovery_config",
    ),
    pytest.param(
        lambda: DiscoveryConfigLibrary(name="dcl", yaml="labels: []"),
        "create_or_update_discovery_config_library",
        "/api/discovery/config-libraries/",
        [],
        id="discovery_config_library",
    ),
    pytest.param(
        lambda: TableReference(name="tr", connection=ConnectionId("1"), source="customers.csv"),
        "create_or_update_table_reference",
        "/api/table-references/",
        [],
        id="table_reference",
    ),
]
"""
Each `create_or_update_*` that looks up the existing resource itself:
a factory for a resource with no `project`, the method, the path it looks up and creates on,
and that path's empty listing.
"""

RULESET_WITHOUT_A_PROJECT = pytest.param(
    lambda: Ruleset(name="rs", yaml="version: '1.0'\ntasks: []", ruleset_type=RulesetType.database),
    "create_or_update_ruleset",
    "/api/rulesets/",
    None,
    id="ruleset",
)
"""`create_or_update_ruleset`, which leaves finding the existing ruleset to the server's upsert."""


@pytest.mark.parametrize("make_resource, method, path, empty_listing", CREATE_OR_UPDATE_WITHOUT_A_PROJECT)
def test_create_or_update_without_a_project_uses_the_only_active_project(
    client: DataMasqueClient,
    make_resource: Callable[[], BaseModel],
    method: str,
    path: str,
    empty_listing: object,
) -> None:
    """The lookup is in the only active project, and the create leaves the choice of project to the server."""
    resource = make_resource()
    created = resource.model_dump(mode="json", by_alias=True) | {"id": CREATED_ID, "project": PROJECT}

    with requests_mock.Mocker() as m:
        m.get("http://test-server/api/projects/", json=active_projects_page(1), status_code=200)
        m.get(f"http://test-server{path}", json=empty_listing, status_code=200)
        m.post(f"http://test-server{path}", json=created, status_code=201)
        getattr(client, method)(resource)

    assert [(request.method, request.path) for request in m.request_history] == [
        ("GET", "/api/projects/"),
        ("GET", path),
        ("POST", path),
    ]
    assert m.request_history[0].qs == {"limit": ["1"]}
    assert m.request_history[1].qs["project"] == [str(PROJECT_ID)]
    assert "project" not in m.last_request.json()
    assert resource.model_dump()["project"] == PROJECT


def test_create_or_update_ruleset_without_a_project_uses_the_only_active_project(client: DataMasqueClient) -> None:
    """The upsert leaves the choice of project to the server, after checking that exactly one project is active."""
    ruleset = Ruleset(name="rs", yaml="version: '1.0'\ntasks: []", ruleset_type=RulesetType.database)

    with requests_mock.Mocker() as m:
        m.get("http://test-server/api/projects/", json=active_projects_page(1), status_code=200)
        m.post(
            "http://test-server/api/rulesets/?upsert=true",
            json={"id": CREATED_ID, "name": "rs", "project": PROJECT},
            status_code=201,
        )
        client.create_or_update_ruleset(ruleset)

    assert [(request.method, request.path) for request in m.request_history] == [
        ("GET", "/api/projects/"),
        ("POST", "/api/rulesets/"),
    ]
    assert "project" not in m.last_request.json()
    assert ruleset.project == PROJECT


@pytest.mark.parametrize("active_projects", [0, 2])
@pytest.mark.parametrize(
    "make_resource, method, path, empty_listing", [*CREATE_OR_UPDATE_WITHOUT_A_PROJECT, RULESET_WITHOUT_A_PROJECT]
)
def test_create_or_update_without_a_project_needs_exactly_one_active_project(
    client: DataMasqueClient,
    make_resource: Callable[[], BaseModel],
    method: str,
    path: str,
    empty_listing: object,
    active_projects: int,
) -> None:
    """
    With no project, the server only accepts a create when exactly one project is active.

    So the client raises before looking up or writing anything,
    rather than updating a same-named resource in a project the server would not have picked.
    """
    with requests_mock.Mocker() as m:
        m.get("http://test-server/api/projects/", json=active_projects_page(active_projects), status_code=200)
        with pytest.raises(DataMasqueException, match=f"has {active_projects} active projects"):
            getattr(client, method)(make_resource())

    assert [request.path for request in m.request_history] == ["/api/projects/"]
