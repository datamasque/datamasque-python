"""Tests for `RulesetClient`."""

from datetime import datetime

import pytest
import requests_mock

from datamasque.client.exceptions import DataMasqueApiError
from datamasque.client.models.ruleset import RulesetType
from datamasque.client.models.status import ValidationErrorType, ValidationStatus


def test_list_rulesets(client, existing_rulesets_json):
    with requests_mock.Mocker() as m:
        # `/api/v2/rulesets/` is not paginated — the server returns a bare list.
        m.get(
            "http://test-server/api/v2/rulesets/",
            json=existing_rulesets_json,
            status_code=200,
        )
        rulesets = client.list_rulesets()
        assert len(rulesets) == 2
        assert rulesets[0].id == "1"
        assert rulesets[0].is_valid is ValidationStatus.valid
        assert rulesets[0].name == "db_masking_ruleset"
        assert rulesets[0].yaml == "version: '1.0'"
        assert rulesets[0].ruleset_type is RulesetType.database
        assert rulesets[1].id == "2"
        assert rulesets[1].is_valid is ValidationStatus.invalid


def test_create_or_update_ruleset_create(client, ruleset):
    with requests_mock.Mocker() as m:
        # Test creating a new ruleset with upsert
        m.post(
            "http://test-server/api/rulesets/?upsert=true",
            json={"id": "2", "name": "test_ruleset", "is_valid": "in_progress"},
            status_code=201,
        )

        ruleset = client.create_or_update_ruleset(ruleset)
        assert ruleset.id == "2"
        assert ruleset.is_valid is ValidationStatus.in_progress

        # Verify the sent body uses aliases
        sent = m.last_request.json()
        assert sent["config_yaml"] == "version: '1.0'\ntasks: []"
        assert sent["mask_type"] == "database"


def test_create_or_update_ruleset_create_fail(client, ruleset):
    with requests_mock.Mocker() as m:
        # Test upsert failure
        m.post("http://test-server/api/rulesets/?upsert=true", status_code=400)

        with pytest.raises(DataMasqueApiError):
            assert client.create_or_update_ruleset(ruleset) is None
        assert ruleset.id is None
        assert ruleset.is_valid is None


def test_create_or_update_ruleset_update(client, ruleset):
    with requests_mock.Mocker() as m:
        # Test updating an existing ruleset with upsert (returns 200 status for update)
        m.post(
            "http://test-server/api/rulesets/?upsert=true",
            json={"id": "1", "name": "test_ruleset", "is_valid": "valid"},
            status_code=200,
        )

        client.create_or_update_ruleset(ruleset)
        assert ruleset.id == "1"
        assert ruleset.is_valid is ValidationStatus.valid


def test_create_or_update_ruleset_update_fail(client, ruleset):
    with requests_mock.Mocker() as m:
        # Test upsert failure for update
        m.post(
            "http://test-server/api/rulesets/?upsert=true",
            json={"id": "1"},
            status_code=400,
        )

        with pytest.raises(DataMasqueApiError):
            assert client.create_or_update_ruleset(ruleset) is None
        assert ruleset.id is None


def test_delete_ruleset_by_id(client):
    with requests_mock.Mocker() as m:
        m.delete("http://test-server/api/rulesets/1/", status_code=204)
        client.delete_ruleset_by_id_if_exists("1")


def test_delete_ruleset_by_name(client, existing_rulesets_json):
    with requests_mock.Mocker() as m:
        m.get(
            "http://test-server/api/v2/rulesets/",
            json=existing_rulesets_json,
            status_code=200,
        )
        m.delete("http://test-server/api/rulesets/2/", status_code=204)
        client.delete_ruleset_by_name_if_exists("file_masking_ruleset")

    assert m.call_count == 2
    assert m.request_history[0].method == "GET"
    assert m.request_history[1].method == "DELETE"


def test_delete_ruleset_that_does_not_exist(client, existing_rulesets_json):
    with requests_mock.Mocker() as m:
        m.get(
            "http://test-server/api/v2/rulesets/",
            json=existing_rulesets_json,
            status_code=200,
        )
        client.delete_ruleset_by_name_if_exists("not_a_ruleset")

    assert m.call_count == 1
    assert m.request_history[0].method == "GET"


def test_create_or_update_ruleset_populates_validation_error(client, ruleset):
    """The server's `validation_error` string and `validation_error_type` are surfaced on the returned ruleset."""
    with requests_mock.Mocker() as m:
        m.post(
            "http://test-server/api/rulesets/?upsert=true",
            json={
                "id": "2",
                "name": "test_ruleset",
                "is_valid": "invalid",
                "validation_error": "Ruleset error: Missing required field: table",
                "validation_error_type": "ruleset",
            },
            status_code=201,
        )
        result = client.create_or_update_ruleset(ruleset)

    assert result.validation_error == "Ruleset error: Missing required field: table"
    assert result.validation_error_type is ValidationErrorType.ruleset


def test_create_or_update_ruleset_validation_error_none_when_absent(client, ruleset):
    """`validation_error` and `validation_error_type` are `None` when the server omits them (a valid ruleset)."""
    with requests_mock.Mocker() as m:
        m.post(
            "http://test-server/api/rulesets/?upsert=true",
            json={"id": "2", "name": "test_ruleset", "is_valid": "valid"},
            status_code=201,
        )
        result = client.create_or_update_ruleset(ruleset)

    assert result.validation_error is None
    assert result.validation_error_type is None


def test_create_or_update_ruleset_does_not_send_read_only_fields(client, ruleset):
    """Read-only server fields must never be echoed back into a re-submit's request body."""
    with requests_mock.Mocker() as m:
        m.post(
            "http://test-server/api/rulesets/?upsert=true",
            json={
                "id": "2",
                "name": "test_ruleset",
                "is_valid": "invalid",
                "validation_error": "Ruleset error: bad",
                "validation_error_type": "ruleset",
            },
            status_code=201,
        )
        client.create_or_update_ruleset(ruleset)

        # Re-submit the same object (read-only fields now populated).
        client.create_or_update_ruleset(ruleset)

    body = m.last_request.json()
    for read_only_field in ("id", "is_valid", "validation_error", "validation_error_type"):
        assert read_only_field not in body
    # Input fields are still present.
    assert body["config_yaml"] == "version: '1.0'\ntasks: []"


def test_create_or_update_ruleset_collapses_git_snapshot(client, ruleset):
    """The server's flat `git_*` fields are collapsed into a nested `git` snapshot."""
    with requests_mock.Mocker() as m:
        m.post(
            "http://test-server/api/rulesets/?upsert=true",
            json={
                "id": "2",
                "name": "test_ruleset",
                "is_valid": "valid",
                "git_branch": "main",
                "git_commit_sha": "abc123",
                "git_repo_url": "https://git.example.com/repo.git",
                "git_synced_at": "2025-06-01T10:00:00Z",
            },
            status_code=201,
        )
        result = client.create_or_update_ruleset(ruleset)

    assert result.git is not None
    assert result.git.branch == "main"
    assert result.git.commit_sha == "abc123"
    assert result.git.repo_url == "https://git.example.com/repo.git"
    assert result.git.synced_at == datetime.fromisoformat("2025-06-01T10:00:00+00:00")


def test_create_or_update_ruleset_git_none_when_not_synced(client, ruleset):
    """`git` is `None` when the server reports no git provenance."""
    with requests_mock.Mocker() as m:
        m.post(
            "http://test-server/api/rulesets/?upsert=true",
            json={
                "id": "2",
                "name": "test_ruleset",
                "is_valid": "valid",
                "git_branch": None,
                "git_commit_sha": None,
                "git_repo_url": None,
                "git_synced_at": None,
            },
            status_code=201,
        )
        result = client.create_or_update_ruleset(ruleset)

    assert result.git is None


def test_list_rulesets_collapses_git_snapshot(client):
    """`git_*` fields are collapsed (and consumed, not left as extras) when listing rulesets."""
    with requests_mock.Mocker() as m:
        m.get(
            "http://test-server/api/v2/rulesets/",
            json=[
                {
                    "id": "1",
                    "name": "synced_ruleset",
                    "mask_type": "database",
                    "config_yaml": "version: '1.0'",
                    "is_valid": "valid",
                    "git_branch": "main",
                    "git_commit_sha": "abc123",
                    "git_repo_url": "https://git.example.com/repo.git",
                    "git_synced_at": "2025-06-01T10:00:00Z",
                },
                {
                    "id": "2",
                    "name": "local_ruleset",
                    "mask_type": "database",
                    "config_yaml": "version: '1.0'",
                    "is_valid": "valid",
                },
            ],
            status_code=200,
        )
        rulesets = client.list_rulesets()

    assert rulesets[0].git is not None
    assert rulesets[0].git.branch == "main"
    assert rulesets[1].git is None
    # The flat keys were consumed, not retained as extras that would leak back on re-submit.
    assert "git_branch" not in rulesets[0].model_dump(by_alias=True)
    assert "git" not in rulesets[0].model_dump(by_alias=True)
