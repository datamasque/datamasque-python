"""Tests for Snowflake SPCS app gateway auth (`spcs_pat` on the client)."""

from unittest.mock import patch

import pytest
import requests_mock

from datamasque.client import DataMasqueClient
from datamasque.client.exceptions import SpcsGatewayAuthError
from datamasque.client.models.dm_instance import DataMasqueInstanceConfig
from datamasque.client.spcs import (
    SPCS_GATEWAY_AUTH_HEADER,
    _hint_for_gateway_detail,
)

BASE_URL = "https://my-app.snowflakecomputing.app"
PAT = "PAT123"
EXPECTED_HEADER = 'Snowflake Token="PAT123"'
VALID_UUID = "12345678-1234-1234-1234-123456789abc"

# Headers/body that together mark a response as a gateway-originated rejection.
GATEWAY_HEADERS = {"Server": "_"}


def _gateway_error_body(detail="Invalid token", response_type="ERROR_INVALID_TOKEN"):
    return {"responseType": response_type, "requestId": VALID_UUID, "detail": detail}


@pytest.fixture
def spcs_config():
    return DataMasqueInstanceConfig(
        base_url=BASE_URL,
        username="api_user",
        password="api_password",
        spcs_pat=PAT,
    )


@pytest.fixture
def spcs_client(spcs_config):
    client = DataMasqueClient(spcs_config)
    client.token = "Token dm-token"  # pretend we're already authenticated with DM
    return client


def test_spcs_header_present_on_authenticated_request(spcs_client):
    with requests_mock.Mocker() as m:
        m.get(f"{BASE_URL}/api/anything/", json={}, status_code=200)
        spcs_client.make_request("GET", "/api/anything/")

    assert m.last_request.headers[SPCS_GATEWAY_AUTH_HEADER] == EXPECTED_HEADER
    # DM's own auth header rides alongside, untouched.
    assert m.last_request.headers["Authorization"] == "Token dm-token"


def test_spcs_header_present_on_login_request(spcs_config):
    """The header must ride on the unauthenticated login POST too (it must clear the gateway)."""
    client = DataMasqueClient(spcs_config)
    with requests_mock.Mocker() as m:
        m.post(f"{BASE_URL}/api/auth/token/login/", json={"key": "k"}, status_code=200)
        client.authenticate()

    assert m.last_request.headers[SPCS_GATEWAY_AUTH_HEADER] == EXPECTED_HEADER
    # Login is unauthenticated — no DM Authorization header on this request.
    assert "Authorization" not in m.last_request.headers


def test_no_spcs_pat_means_no_header_and_no_hook(client):
    """The default client (no spcs_pat) is entirely unaffected."""
    assert SPCS_GATEWAY_AUTH_HEADER not in client._session.headers
    assert client._session.hooks["response"] == []

    client.token = "Token dm-token"
    with requests_mock.Mocker() as m:
        m.get("http://test-server/api/anything/", json={}, status_code=200)
        client.make_request("GET", "/api/anything/")
    assert SPCS_GATEWAY_AUTH_HEADER not in m.last_request.headers


def test_gateway_401_raises_and_does_not_retry(spcs_config):
    """A gateway rejection aborts immediately — no re-auth, no retry loop."""
    with patch.object(DataMasqueClient, "authenticate") as mock_auth:
        client = DataMasqueClient(spcs_config)
        client.token = "Token dm-token"
        with requests_mock.Mocker() as m:
            # A single response: a second call would 404 and fail the test loudly.
            m.get(
                f"{BASE_URL}/api/anything/",
                json=_gateway_error_body(),
                status_code=401,
                headers=GATEWAY_HEADERS,
            )
            with pytest.raises(SpcsGatewayAuthError) as exc:
                client.make_request("GET", "/api/anything/")

        assert m.call_count == 1
        mock_auth.assert_not_called()
    # The helpful hint is surfaced.
    assert "PAT is malformed, expired, or revoked" in str(exc.value)
    assert VALID_UUID in str(exc.value)


def test_normal_dm_401_still_retries(spcs_config):
    """A genuine DataMasque 401 (no gateway signature) flows to the normal re-auth retry."""
    client = DataMasqueClient(spcs_config)
    with requests_mock.Mocker() as m:
        m.post(f"{BASE_URL}/api/auth/token/login/", json={"key": "k"}, status_code=200)
        m.get(
            f"{BASE_URL}/api/anything/",
            [
                # DRF-shaped 401, no gateway Server header → not a gateway rejection.
                {"json": {"detail": "Authentication credentials were not provided."}, "status_code": 401},
                {"json": {"ok": True}, "status_code": 200},
            ],
        )
        # Should NOT raise SpcsGatewayAuthError; should re-auth and succeed.
        response = client.make_request("GET", "/api/anything/")

    assert response.status_code == 200
    assert client.token == "Token k"  # re-auth happened


def test_gateway_signature_without_body_shape_passes_through(spcs_config):
    """Gateway header present but DM-shaped body → treated as a normal DM 401, not a gateway rejection."""
    client = DataMasqueClient(spcs_config)
    with requests_mock.Mocker() as m:
        m.post(f"{BASE_URL}/api/auth/token/login/", json={"key": "k"}, status_code=200)
        m.get(
            f"{BASE_URL}/api/anything/",
            [
                {"json": {"detail": "nope"}, "status_code": 401, "headers": GATEWAY_HEADERS},
                {"json": {"ok": True}, "status_code": 200},
            ],
        )
        response = client.make_request("GET", "/api/anything/")  # must not raise

    assert response.status_code == 200


@pytest.mark.parametrize(
    "detail, expected_substring",
    [
        ("Request failed network policy check", "network policy"),
        ("Invalid token supplied", "malformed, expired, or revoked"),
        ("The token has expired", "PAT has expired"),
        ("Unauthorized request", "Generic auth rejection"),
        ("Something totally unexpected", "Unknown gateway rejection"),
    ],
)
def test_hint_for_gateway_detail(detail, expected_substring):
    assert expected_substring in _hint_for_gateway_detail(detail)


def test_spcs_pat_coexists_with_password_and_token_source():
    """`spcs_pat` is orthogonal to the password/token_source XOR — both combos validate."""
    DataMasqueInstanceConfig(base_url=BASE_URL, username="u", password="p", spcs_pat=PAT)
    DataMasqueInstanceConfig(base_url=BASE_URL, username="u", token_source=lambda: "t", spcs_pat=PAT)
