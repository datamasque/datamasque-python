"""Tests for `DataMasqueIfmClient`."""

import pytest
import requests_mock

from datamasque.client import (
    DataMasqueIfmClient,
    DataMasqueIfmInstanceConfig,
    IfmAuthError,
    IfmMaskRequest,
    RulesetPlanCreateRequest,
    RulesetPlanPartialUpdateRequest,
    RulesetPlanUpdateRequest,
)
from datamasque.client.exceptions import DataMasqueApiError, DataMasqueUserError

ADMIN = "http://admin.test"
IFM = "http://ifm.test"


@pytest.fixture
def ifm_config():
    return DataMasqueIfmInstanceConfig(
        admin_server_base_url=ADMIN,
        ifm_base_url=IFM,
        username="ifm_user",
        password="ifm_password",
    )


@pytest.fixture
def authed_ifm_client(ifm_config):
    client = DataMasqueIfmClient(ifm_config)
    # Pre-seed tokens to skip the login round-trip in tests that don't care about it.
    client.access_token = "access-1"
    client.refresh_token = "refresh-1"
    return client


def test_ifm_config_rejects_neither_password_nor_token_source():
    with pytest.raises(DataMasqueUserError, match="Exactly one of `password` or `token_source`"):
        DataMasqueIfmInstanceConfig(admin_server_base_url=ADMIN, ifm_base_url=IFM, username="u")


def test_ifm_config_rejects_both_password_and_token_source():
    with pytest.raises(DataMasqueUserError, match="Exactly one of `password` or `token_source`"):
        DataMasqueIfmInstanceConfig(
            admin_server_base_url=ADMIN,
            ifm_base_url=IFM,
            username="u",
            password="p",
            token_source=lambda: "t",
        )


def test_authenticate_via_jwt_login(ifm_config):
    client = DataMasqueIfmClient(ifm_config)

    with requests_mock.Mocker() as m:
        m.post(
            f"{ADMIN}/api/auth/jwt/login/",
            json={"access_token": "ACC", "refresh_token": "REF"},
            status_code=200,
        )
        client.authenticate()

    assert client.access_token == "ACC"
    assert client.refresh_token == "REF"


def test_authenticate_failure_raises_ifm_auth_error(ifm_config):
    client = DataMasqueIfmClient(ifm_config)

    with requests_mock.Mocker() as m:
        m.post(f"{ADMIN}/api/auth/jwt/login/", status_code=401)
        with pytest.raises(IfmAuthError):
            client.authenticate()


def test_authenticate_uses_token_source_when_provided():
    config = DataMasqueIfmInstanceConfig(
        admin_server_base_url=ADMIN,
        ifm_base_url=IFM,
        username="u",
        token_source=lambda: "callable-jwt",
    )
    client = DataMasqueIfmClient(config)

    with requests_mock.Mocker() as m:
        client.authenticate()
        assert m.call_count == 0  # No HTTP call when token_source provides the JWT.

    assert client.access_token == "callable-jwt"


def test_401_triggers_refresh_then_retries(authed_ifm_client):
    with requests_mock.Mocker() as m:
        m.get(
            f"{IFM}/ruleset-plans/",
            [{"status_code": 401}, {"json": {"items": [], "total": 0, "limit": 100, "offset": 0}, "status_code": 200}],
        )
        m.post(
            f"{ADMIN}/api/auth/jwt/refresh/",
            json={"access_token": "ACC2"},
            status_code=200,
        )

        result = authed_ifm_client.list_ruleset_plans()

    assert result == []
    assert authed_ifm_client.access_token == "ACC2"


def test_401_then_failed_refresh_falls_back_to_full_login(authed_ifm_client):
    with requests_mock.Mocker() as m:
        m.get(
            f"{IFM}/ruleset-plans/",
            [{"status_code": 401}, {"json": {"items": [], "total": 0, "limit": 100, "offset": 0}, "status_code": 200}],
        )
        m.post(f"{ADMIN}/api/auth/jwt/refresh/", status_code=401)
        m.post(
            f"{ADMIN}/api/auth/jwt/login/",
            json={"access_token": "ACC3", "refresh_token": "REF3"},
            status_code=200,
        )

        authed_ifm_client.list_ruleset_plans()

    assert authed_ifm_client.access_token == "ACC3"
    assert authed_ifm_client.refresh_token == "REF3"


def test_401_with_token_source_skips_refresh_and_re_authenticates(ifm_config):
    """When `token_source` is configured, a 401 triggers a direct `authenticate` call, not a JWT refresh round-trip."""
    call_count = {"n": 0}

    def token_source() -> str:
        call_count["n"] += 1
        return f"callable-jwt-{call_count['n']}"

    config = DataMasqueIfmInstanceConfig(
        admin_server_base_url=ADMIN,
        ifm_base_url=IFM,
        username="u",
        token_source=token_source,
    )
    client = DataMasqueIfmClient(config)

    with requests_mock.Mocker() as m:
        m.get(
            f"{IFM}/ruleset-plans/",
            [{"status_code": 401}, {"json": {"items": [], "total": 0, "limit": 100, "offset": 0}, "status_code": 200}],
        )
        client.list_ruleset_plans()

    # The refresh endpoint must not have been hit — token_source is authoritative.
    assert all("auth/jwt/refresh" not in req.url for req in m.request_history)
    assert client.access_token == "callable-jwt-2"


def test_401_without_refresh_token_falls_through_to_full_login(ifm_config):
    """When the client has no cached refresh token, a 401 triggers a full `authenticate` rather than a refresh call."""
    client = DataMasqueIfmClient(ifm_config)
    client.access_token = "stale-access"
    client.refresh_token = ""  # never had one

    with requests_mock.Mocker() as m:
        m.get(
            f"{IFM}/ruleset-plans/",
            [{"status_code": 401}, {"json": {"items": [], "total": 0, "limit": 100, "offset": 0}, "status_code": 200}],
        )
        m.post(
            f"{ADMIN}/api/auth/jwt/login/",
            json={"access_token": "FRESH", "refresh_token": "FRESH_REF"},
            status_code=200,
        )
        client.list_ruleset_plans()

    # Refresh endpoint skipped; login was called instead.
    assert all("auth/jwt/refresh" not in req.url for req in m.request_history)
    assert client.access_token == "FRESH"


def test_verify_token(authed_ifm_client):
    with requests_mock.Mocker() as m:
        m.get(
            f"{IFM}/verify-token/",
            json={"scopes": ["ifm/mask"]},
            status_code=200,
        )
        info = authed_ifm_client.verify_token()

    assert "ifm/mask" in info.scopes


def test_list_ruleset_plans(authed_ifm_client):
    with requests_mock.Mocker() as m:
        m.get(
            f"{IFM}/ruleset-plans/",
            json={
                "items": [
                    {
                        "name": "p1",
                        "created_time": "2025-01-01T00:00:00Z",
                        "modified_time": "2025-01-02T00:00:00Z",
                        "serial": 1,
                        "options": {},
                    },
                    {
                        "name": "p2",
                        "created_time": "2025-02-01T00:00:00Z",
                        "modified_time": "2025-02-02T00:00:00Z",
                        "serial": 2,
                        "options": {},
                    },
                ],
                "total": 2,
                "limit": 100,
                "offset": 0,
            },
            status_code=200,
        )
        plans = authed_ifm_client.list_ruleset_plans()

    assert [p.name for p in plans] == ["p1", "p2"]


def test_get_ruleset_plan(authed_ifm_client):
    with requests_mock.Mocker() as m:
        m.get(
            f"{IFM}/ruleset-plans/p1/",
            json={
                "name": "p1",
                "created_time": "2025-01-01T00:00:00Z",
                "modified_time": "2025-01-02T00:00:00Z",
                "serial": 1,
                "options": {},
                "ruleset_yaml": "version: '1.0'",
            },
            status_code=200,
        )
        plan = authed_ifm_client.get_ruleset_plan("p1")

    assert plan.name == "p1"
    assert plan.ruleset_yaml == "version: '1.0'"


def test_create_ruleset_plan(authed_ifm_client):
    req = RulesetPlanCreateRequest(name="p1", ruleset_yaml="version: '1.0'")

    with requests_mock.Mocker() as m:
        m.post(
            f"{IFM}/ruleset-plans/",
            json={
                "name": "p1",
                "created_time": "2025-01-01T00:00:00Z",
                "modified_time": "2025-01-01T00:00:00Z",
                "serial": 1,
                "options": {},
                "ruleset_yaml": "version: '1.0'",
                "logs": [],
                "url": f"{IFM}/ruleset-plans/p1/",
            },
            status_code=201,
        )
        result = authed_ifm_client.create_ruleset_plan(req)

    assert result.name == "p1"
    assert result.url.endswith("/ruleset-plans/p1/")
    assert m.last_request.json() == {"name": "p1", "ruleset_yaml": "version: '1.0'"}


def test_update_ruleset_plan(authed_ifm_client):
    req = RulesetPlanUpdateRequest(ruleset_yaml="version: '2.0'", options={"enabled": True})

    with requests_mock.Mocker() as m:
        m.put(
            f"{IFM}/ruleset-plans/p1/",
            json={
                "name": "p1",
                "created_time": "2025-01-01T00:00:00Z",
                "modified_time": "2025-06-01T00:00:00Z",
                "serial": 2,
                "options": {"enabled": True},
                "ruleset_yaml": "version: '2.0'",
                "logs": [],
            },
            status_code=200,
        )
        result = authed_ifm_client.update_ruleset_plan("p1", req)

    assert result.serial == 2
    assert m.last_request.json() == {"ruleset_yaml": "version: '2.0'", "options": {"enabled": True}}


def test_patch_ruleset_plan_omits_unset_fields(authed_ifm_client):
    req = RulesetPlanPartialUpdateRequest(options={"enabled": False})

    with requests_mock.Mocker() as m:
        m.patch(
            f"{IFM}/ruleset-plans/p1/",
            json={
                "name": "p1",
                "created_time": "2025-01-01T00:00:00Z",
                "modified_time": "2025-06-01T00:00:00Z",
                "serial": 3,
                "options": {"enabled": False},
                "ruleset_yaml": "still here",
                "logs": [],
            },
            status_code=200,
        )
        authed_ifm_client.patch_ruleset_plan("p1", req)

    body = m.last_request.json()
    assert body == {"options": {"enabled": False}}
    assert "ruleset_yaml" not in body  # not set on the partial-update request


def test_delete_ruleset_plan(authed_ifm_client):
    with requests_mock.Mocker() as m:
        m.delete(f"{IFM}/ruleset-plans/p1/", status_code=204)
        authed_ifm_client.delete_ruleset_plan("p1")

    assert m.call_count == 1


def test_mask_success(authed_ifm_client):
    req = IfmMaskRequest(data=[{"id": 1, "email": "a@b.com"}])

    with requests_mock.Mocker() as m:
        m.post(
            f"{IFM}/ruleset-plans/p1/mask/",
            json={
                "request_id": "req-1",
                "ruleset_plan": {"name": "p1", "serial": 1},
                "logs": [],
                "data": [{"id": 1, "email": "***@***.***"}],
            },
            status_code=200,
        )
        result = authed_ifm_client.mask("p1", req)

    assert result.success is True
    assert result.data == [{"id": 1, "email": "***@***.***"}]
    assert result.ruleset_plan.serial == 1
    sent = m.last_request.json()
    assert sent["data"] == [{"id": 1, "email": "a@b.com"}]


def test_mask_request_omits_unset_optionals():
    req = IfmMaskRequest(data=[])
    assert req.model_dump(exclude_none=True, mode="json") == {"data": []}


def test_mask_raises_api_error_on_server_error(authed_ifm_client):
    req = IfmMaskRequest(data=[{"x": 1}])

    with requests_mock.Mocker() as m:
        m.post(f"{IFM}/ruleset-plans/p1/mask/", status_code=500)
        with pytest.raises(DataMasqueApiError):
            authed_ifm_client.mask("p1", req)


def test_mask_soft_failure_returns_result_with_no_data(authed_ifm_client):
    """A 400 with the full `IfmMaskResult` shape is a soft failure — return the result, don't raise."""
    req = IfmMaskRequest(data=[[42]])

    with requests_mock.Mocker() as m:
        m.post(
            f"{IFM}/ruleset-plans/p1/mask/",
            json={
                "request_id": "req-soft",
                "ruleset_plan": {"name": "p1", "serial": 1},
                "logs": [
                    {
                        "log_level": "error",
                        "timestamp": "2026-04-20T12:00:00Z",
                        "message": "replace_regex received a non-string value",
                    },
                ],
            },
            status_code=400,
        )
        result = authed_ifm_client.mask("p1", req)

    assert result.success is False
    assert result.data is None
    assert result.ruleset_plan is not None and result.ruleset_plan.name == "p1"
    assert result.logs is not None and result.logs[0].log_level == "error"


def test_mask_raises_api_error_on_400_without_result_shape(authed_ifm_client):
    """A 400 that doesn't carry an `IfmMaskResult` body (e.g. malformed request) is still a hard error."""
    req = IfmMaskRequest(data=[])

    with requests_mock.Mocker() as m:
        m.post(
            f"{IFM}/ruleset-plans/p1/mask/",
            json={"detail": "Malformed request body"},
            status_code=400,
        )
        with pytest.raises(DataMasqueApiError):
            authed_ifm_client.mask("p1", req)
