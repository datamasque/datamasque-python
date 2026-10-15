"""Tests for project support in the DataMasque client."""

from datetime import datetime

import pytest
import requests_mock
from pydantic import ValidationError

from datamasque.client import DataMasqueApiError, DataMasqueClient, ProjectCreateRequest, ProjectId

CREATE_RESPONSE = {
    "id": 7,
    "reference": "7-banking",
    "name": "Banking",
    "description": "Customer banking data",
    "created": "2026-10-01T10:00:00Z",
    "updated": "2026-10-01T10:00:00Z",
}


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
