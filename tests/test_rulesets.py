"""Tests for `RulesetClient`."""

import pytest
import requests_mock

from datamasque.client.exceptions import DataMasqueApiError
from datamasque.client.models.ruleset import RulesetType
from datamasque.client.models.status import ValidationStatus


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
            json={"id": "2", "is_valid": "in_progress"},
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
            json={"id": "1", "is_valid": "valid"},
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
