"""Tests for project support in the DataMasque client."""

from datetime import datetime

import pytest
import requests_mock
from pydantic import ValidationError

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

CREATE_RESPONSE = {
    "id": 7,
    "reference": "7-banking",
    "name": "Banking",
    "description": "Customer banking data",
    "created": "2026-10-01T10:00:00Z",
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
        m.post("http://test-server/api/projects/", json=CREATE_RESPONSE, status_code=201)
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
        m.post("http://test-server/api/projects/", json=CREATE_RESPONSE, status_code=201)
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
    project = Project.model_validate(CREATE_RESPONSE)
    destination = Project.model_validate(CREATE_RESPONSE | {"id": 8, "reference": "8-retail", "name": "Retail"})

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
            json={"detail": "The project's resources cannot be moved.", "conflicts": CONFLICTS},
            status_code=409,
        )
        with pytest.raises(ProjectDeleteConflictError) as exc_info:
            client.delete_project(ProjectId(7), ProjectId(8))

    assert exc_info.value.response.status_code == 409
    assert "The project's resources cannot be moved." in str(exc_info.value)
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
