"""Tests for `InstanceSecretClient` (instance secret fingerprint and instance secret linking)."""

from datetime import datetime, timezone
from typing import Optional

import pytest
import requests_mock

from datamasque.client.exceptions import DataMasqueApiError, InstanceSecretLinkError, InstanceSecretLinkErrorCode
from datamasque.client.models.instance_secret import InFlightSyncResult, InstanceSecretEventKind

LINK_URL = "http://test-server/api/instance-secret/link"

LINK_REQUEST_JSON = {
    "request_code": "DMLINK-REQUEST",
    "fingerprint": "7KQ2-9MXA-3HDP-T0WZ",
    "expires_at": "2026-10-30T12:00:00Z",
}


def event_json(kind: str, *, peer_hostname: Optional[str] = None, peer_fingerprint: Optional[str] = None) -> dict:
    return {
        "id": "3f2b8c1e-5d4a-4e6f-9a7b-1c2d3e4f5a6b",
        "created_at": "2026-10-30T11:00:00Z",
        "kind": kind,
        "peer_hostname": peer_hostname,
        "peer_fingerprint": peer_fingerprint,
        "username": "admin",
    }


def test_get_instance_secret_fingerprint_returns_the_fingerprint(client):
    with requests_mock.Mocker() as m:
        m.get("http://test-server/api/instance-secret/", json={"fingerprint": "7KQ2-9MXA-3HDP-T0WZ"})
        assert client.get_instance_secret_fingerprint() == "7KQ2-9MXA-3HDP-T0WZ"


def test_get_instance_secret_link_history_parses_events(client):
    shared = event_json("shared", peer_hostname="other.example.com", peer_fingerprint="ABCD-EFGH-JKMN-PQRS")
    with requests_mock.Mocker() as m:
        m.get(f"{LINK_URL}/history/", json={"current_links": [shared], "events": [shared, event_json("initialised")]})
        history = client.get_instance_secret_link_history()

    assert history.current_links[0].kind is InstanceSecretEventKind.shared
    assert history.current_links[0].peer_hostname == "other.example.com"
    assert [event.kind for event in history.events] == [
        InstanceSecretEventKind.shared,
        InstanceSecretEventKind.initialised,
    ]


def test_create_instance_secret_link_request_posts_without_a_body(client):
    with requests_mock.Mocker() as m:
        m.post(f"{LINK_URL}/request/", json=LINK_REQUEST_JSON, status_code=201)
        link_request = client.create_instance_secret_link_request()

    assert m.last_request.body is None
    assert link_request.request_code == "DMLINK-REQUEST"


def test_get_pending_instance_secret_link_request_returns_the_request(client):
    with requests_mock.Mocker() as m:
        m.get(f"{LINK_URL}/request/", json=LINK_REQUEST_JSON)
        link_request = client.get_pending_instance_secret_link_request()

    assert link_request is not None
    assert link_request.fingerprint == "7KQ2-9MXA-3HDP-T0WZ"
    assert link_request.expires_at == datetime(2026, 10, 30, 12, 0, tzinfo=timezone.utc)


def test_get_pending_instance_secret_link_request_returns_none_on_404(client):
    with requests_mock.Mocker() as m:
        m.get(f"{LINK_URL}/request/", json={"detail": "There is no pending request code."}, status_code=404)
        assert client.get_pending_instance_secret_link_request() is None


def test_get_pending_instance_secret_link_request_raises_on_403(client):
    with requests_mock.Mocker() as m:
        m.get(f"{LINK_URL}/request/", json={"detail": "Not licensed."}, status_code=403)
        with pytest.raises(DataMasqueApiError):
            client.get_pending_instance_secret_link_request()


def test_verify_instance_secret_link_request_returns_what_the_admins_compare(client):
    with requests_mock.Mocker() as m:
        m.post(
            f"{LINK_URL}/request/verify/",
            json={
                "hostname": "joining.example.com",
                "fingerprint": "7KQ2-9MXA-3HDP-T0WZ",
                "expires_at": "2026-10-30T12:00:00Z",
                "datamasque_version": None,
            },
        )
        verification = client.verify_instance_secret_link_request("DMLINK-REQUEST")

    assert m.last_request.json() == {"request_code": "DMLINK-REQUEST"}
    assert verification.hostname == "joining.example.com"
    assert verification.fingerprint == "7KQ2-9MXA-3HDP-T0WZ"
    assert verification.datamasque_version is None


def test_accept_instance_secret_link_request_returns_the_response_code(client):
    with requests_mock.Mocker() as m:
        m.post(
            f"{LINK_URL}/request/accept/",
            json={"response_code": "DMLINK-RESPONSE", "response_fingerprint": "ABCD-EFGH-JKMN-PQRS"},
        )
        link_response = client.accept_instance_secret_link_request("DMLINK-REQUEST")

    assert m.last_request.json() == {"request_code": "DMLINK-REQUEST"}
    assert link_response.response_code == "DMLINK-RESPONSE"


# Every error code that the server's API schema lists for a linking step.
SERVER_LINK_ERROR_CODES = (
    "malformed_code",
    "newer_link_format",
    "wrong_code_kind",
    "invalid_public_key",
    "own_request",
    "request_expired",
    "unknown_request",
    "unseal_failed",
    "invalid_secret",
    "same_secret",
    "active_runs",
    "verification_stale",
    "host_header_too_long",
)


@pytest.mark.parametrize("error_code", SERVER_LINK_ERROR_CODES)
def test_a_failed_linking_step_raises_instance_secret_link_error_with_the_code(client, error_code):
    message = "The linking step failed."
    with requests_mock.Mocker() as m:
        m.post(
            f"{LINK_URL}/request/verify/",
            json={"code": error_code, "message": message},
            status_code=400,
        )
        with pytest.raises(InstanceSecretLinkError, match=message) as error_info:
            client.verify_instance_secret_link_request("DMLINK-REQUEST")

    assert error_info.value.code is InstanceSecretLinkErrorCode(error_code)
    assert error_info.value.response.status_code == 400


@pytest.mark.parametrize(
    "error_body",
    [
        {"code": "a_code_from_a_newer_server", "message": "Something new went wrong."},
        {"detail": "Bad request."},
        ["Bad request."],
    ],
)
def test_a_400_that_is_not_a_known_link_error_raises_data_masque_api_error(client, error_body):
    with requests_mock.Mocker() as m:
        m.post(f"{LINK_URL}/request/verify/", json=error_body, status_code=400)
        with pytest.raises(DataMasqueApiError) as error_info:
            client.verify_instance_secret_link_request("DMLINK-REQUEST")

    assert not isinstance(error_info.value, InstanceSecretLinkError)
    assert error_info.value.response.status_code == 400


def test_a_failed_response_step_does_not_log_the_response_code(client, caplog):
    with requests_mock.Mocker() as m:
        m.post(f"{LINK_URL}/response/accept/", json={"detail": "Server error."}, status_code=500)
        with pytest.raises(DataMasqueApiError):
            client.accept_instance_secret_link_response("DMLINK-RESPONSE", "token")

    assert "Request data was" in caplog.text
    assert "DMLINK-RESPONSE" not in caplog.text


def test_a_linking_step_without_an_instance_secret_raises_data_masque_api_error(client):
    with requests_mock.Mocker() as m:
        m.post(f"{LINK_URL}/request/accept/", json={"detail": "No instance secret."}, status_code=503)
        with pytest.raises(DataMasqueApiError) as error_info:
            client.accept_instance_secret_link_request("DMLINK-REQUEST")

    assert not isinstance(error_info.value, InstanceSecretLinkError)


def test_verify_instance_secret_link_response_parses_the_links_ending(client):
    with requests_mock.Mocker() as m:
        m.post(
            f"{LINK_URL}/response/verify/",
            json={
                "hostname": "other.example.com",
                "response_fingerprint": "ABCD-EFGH-JKMN-PQRS",
                "links_ending": [event_json("adopted", peer_hostname="third.example.com")],
                "verification_token": "token",
            },
        )
        verification = client.verify_instance_secret_link_response("DMLINK-RESPONSE")

    assert m.last_request.json() == {"response_code": "DMLINK-RESPONSE"}
    assert verification.links_ending[0].kind is InstanceSecretEventKind.adopted
    assert verification.verification_token == "token"


def test_accept_instance_secret_link_response_returns_the_in_flight_sync_result(client):
    with requests_mock.Mocker() as m:
        m.post(f"{LINK_URL}/response/accept/", json={"in_flight_sync": "notify_failed"})
        result = client.accept_instance_secret_link_response("DMLINK-RESPONSE", "token")

    assert m.last_request.json() == {"response_code": "DMLINK-RESPONSE", "verification_token": "token"}
    assert result is InFlightSyncResult.notify_failed


@pytest.mark.parametrize(
    ("method_name", "arguments"),
    [
        ("verify_instance_secret_link_request", ("",)),
        ("accept_instance_secret_link_request", ("",)),
        ("verify_instance_secret_link_response", ("",)),
        ("accept_instance_secret_link_response", ("", "token")),
        ("accept_instance_secret_link_response", ("DMLINK-RESPONSE", "")),
    ],
)
def test_an_empty_code_raises_value_error_before_any_request(client, method_name, arguments):
    with requests_mock.Mocker() as m:
        with pytest.raises(ValueError):
            getattr(client, method_name)(*arguments)

    assert m.call_count == 0
