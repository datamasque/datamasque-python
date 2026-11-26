"""Tests for `InstanceSecretClient` (instance secret fingerprint and instance secret linking)."""

import re
from datetime import datetime, timezone

import pytest
import requests_mock
from pydantic import ValidationError

from datamasque.client.exceptions import DataMasqueApiError, InstanceSecretLinkError, InstanceSecretLinkErrorCode
from datamasque.client.models.instance_secret import InFlightSyncResult, InstanceSecretLink, InstanceSecretLinkKind

LINK_URL = "http://test-server/api/instance-secret/link"

LINK_REQUEST_JSON = {
    "request_code": "DMLINK-REQUEST",
    "request_fingerprint": "7KQ2-9MXA-3HDP-T0WZ",
    "expires_at": "2026-10-30T12:00:00Z",
}

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
    "insecure_instance_secret",
)


def link_json(
    kind: str,
    *,
    peer_hostname: str = "peer.example.com",
    request_fingerprint: str = "7KQ2-9MXA-3HDP-T0WZ",
    response_fingerprint: str = "9GGM-DR1H-ETXX-41Q6",
) -> dict:
    return {
        "id": "3f2b8c1e-5d4a-4e6f-9a7b-1c2d3e4f5a6b",
        "created_at": "2026-10-30T11:00:00Z",
        "kind": kind,
        "peer_hostname": peer_hostname,
        "request_fingerprint": request_fingerprint,
        "response_fingerprint": response_fingerprint,
        "username": "admin",
    }


def test_get_instance_secret_fingerprint_returns_the_fingerprint(client):
    with requests_mock.Mocker() as m:
        m.get("http://test-server/api/instance-secret/", json={"fingerprint": "7KQ2-9MXA-3HDP-T0WZ"})
        assert client.get_instance_secret_fingerprint() == "7KQ2-9MXA-3HDP-T0WZ"


def test_get_current_instance_secret_links_returns_the_links_in_order(client):
    shared = link_json(
        "shared",
        peer_hostname="joining.example.com",
        request_fingerprint="7KQ2-9MXA-3HDP-T0WZ",
        response_fingerprint="0F2A-9QXH-7RZW-B4ND",
    )
    adopted = {
        **link_json(
            "adopted",
            peer_hostname="sharing.example.com",
            request_fingerprint="EBDV-ECVC-8QJ4-9XT5",
            response_fingerprint="9GGM-DR1H-ETXX-41Q6",
        ),
        "id": "8a1c2d3e-4f5a-4b6c-8d7e-9f0a1b2c3d4e",
    }
    with requests_mock.Mocker() as m:
        m.get("http://test-server/api/instance-secret/current-links/", json={"links": [shared, adopted]})
        links = client.get_current_instance_secret_links()

    assert [(link.kind, link.peer_hostname) for link in links] == [
        (InstanceSecretLinkKind.shared, "joining.example.com"),
        (InstanceSecretLinkKind.adopted, "sharing.example.com"),
    ]
    assert links[0].id == "3f2b8c1e-5d4a-4e6f-9a7b-1c2d3e4f5a6b"
    assert links[0].username == "admin"
    assert links[0].request_fingerprint == "7KQ2-9MXA-3HDP-T0WZ"
    assert links[0].created_at == datetime(2026, 10, 30, 11, 0, tzinfo=timezone.utc)
    assert links[1].response_fingerprint == "9GGM-DR1H-ETXX-41Q6"


@pytest.mark.parametrize("kind", ["rotated", "initialised"])
def test_get_current_instance_secret_links_rejects_an_unknown_kind(client, kind):
    with requests_mock.Mocker() as m:
        m.get("http://test-server/api/instance-secret/current-links/", json={"links": [link_json(kind)]})
        with pytest.raises(ValidationError, match="kind"):
            client.get_current_instance_secret_links()


def test_get_current_instance_secret_links_returns_an_empty_list_when_there_are_none(client):
    with requests_mock.Mocker() as m:
        m.get("http://test-server/api/instance-secret/current-links/", json={"links": []})
        assert client.get_current_instance_secret_links() == []


def test_get_current_instance_secret_links_by_a_user_who_is_not_an_admin_raises_data_masque_api_error(client):
    with requests_mock.Mocker() as m:
        m.get(
            "http://test-server/api/instance-secret/current-links/",
            json={"detail": "You do not have permission to perform this action."},
            status_code=403,
        )
        with pytest.raises(
            DataMasqueApiError,
            match=re.escape(
                "API request to http://test-server/api/instance-secret/current-links/ failed with status 403"
            ),
        ) as error_info:
            client.get_current_instance_secret_links()

    assert error_info.value.response.status_code == 403


@pytest.mark.parametrize("in_flight_sync", list(InFlightSyncResult))
def test_rotate_instance_secret_returns_the_in_flight_sync_result(client, in_flight_sync):
    with requests_mock.Mocker() as m:
        m.post("http://test-server/api/instance-secret/rotate/", json={"in_flight_sync": in_flight_sync.value})
        result = client.rotate_instance_secret()

    assert m.call_count == 1
    assert result is in_flight_sync


def test_rotate_instance_secret_by_a_user_who_is_not_an_admin_raises_data_masque_api_error(client):
    with requests_mock.Mocker() as m:
        m.post(
            "http://test-server/api/instance-secret/rotate/",
            json={"detail": "You do not have permission to perform this action."},
            status_code=403,
        )
        with pytest.raises(
            DataMasqueApiError,
            match=re.escape("API request to http://test-server/api/instance-secret/rotate/ failed with status 403"),
        ) as error_info:
            client.rotate_instance_secret()

    assert error_info.value.response.status_code == 403


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
    assert link_request.request_code == "DMLINK-REQUEST"
    assert link_request.request_fingerprint == "7KQ2-9MXA-3HDP-T0WZ"
    assert link_request.expires_at == datetime(2026, 10, 30, 12, 0, tzinfo=timezone.utc)


def test_get_pending_instance_secret_link_request_returns_none_on_404(client):
    with requests_mock.Mocker() as m:
        m.get(f"{LINK_URL}/request/", json={"detail": "There is no pending request code."}, status_code=404)
        assert client.get_pending_instance_secret_link_request() is None


def test_get_pending_instance_secret_link_request_raises_on_403(client):
    with requests_mock.Mocker() as m:
        m.get(f"{LINK_URL}/request/", json={"detail": "Not licensed."}, status_code=403)
        with pytest.raises(
            DataMasqueApiError,
            match=re.escape(f"API request to {LINK_URL}/request/ failed with status 403"),
        ) as error_info:
            client.get_pending_instance_secret_link_request()

    assert error_info.value.response.status_code == 403


def test_cancel_instance_secret_link_request_sends_a_delete_and_returns_none(client):
    with requests_mock.Mocker() as m:
        m.delete(f"{LINK_URL}/request/", status_code=204)
        result = client.cancel_instance_secret_link_request()

    assert m.call_count == 1
    assert m.last_request.method == "DELETE"
    assert m.last_request.url == f"{LINK_URL}/request/"
    assert result is None


def test_cancel_instance_secret_link_request_raises_on_403(client):
    with requests_mock.Mocker() as m:
        m.delete(f"{LINK_URL}/request/", json={"detail": "Not licensed."}, status_code=403)
        with pytest.raises(
            DataMasqueApiError,
            match=re.escape(f"API request to {LINK_URL}/request/ failed with status 403"),
        ) as error_info:
            client.cancel_instance_secret_link_request()

    assert error_info.value.response.status_code == 403


def test_verify_instance_secret_link_request_returns_what_the_admins_compare(client):
    with requests_mock.Mocker() as m:
        m.post(
            f"{LINK_URL}/request/verify/",
            json={
                "peer_hostname": "joining.example.com",
                "request_fingerprint": "7KQ2-9MXA-3HDP-T0WZ",
                "expires_at": "2026-10-30T12:00:00Z",
                "datamasque_version": None,
            },
        )
        verification = client.verify_instance_secret_link_request("DMLINK-REQUEST")

    assert m.last_request.json() == {"request_code": "DMLINK-REQUEST"}
    assert verification.peer_hostname == "joining.example.com"
    assert verification.request_fingerprint == "7KQ2-9MXA-3HDP-T0WZ"
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


@pytest.mark.parametrize("error_code", ["verification_stale", "active_runs"])
def test_a_failed_accept_of_a_response_code_raises_instance_secret_link_error_without_logging_request_data(
    client, caplog, error_code
):
    message = "The response code was not accepted."
    with requests_mock.Mocker() as m:
        m.post(f"{LINK_URL}/response/accept/", json={"code": error_code, "message": message}, status_code=400)
        with pytest.raises(InstanceSecretLinkError, match=message) as error_info:
            client.accept_instance_secret_link_response("DMLINK-RESPONSE", "token")

    assert error_info.value.code is InstanceSecretLinkErrorCode(error_code)
    assert error_info.value.response.status_code == 400
    assert "Request data was" not in caplog.text


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
        with pytest.raises(
            DataMasqueApiError,
            match=re.escape(f"API request to {LINK_URL}/request/verify/ failed with status 400"),
        ) as error_info:
            client.verify_instance_secret_link_request("DMLINK-REQUEST")

    assert not isinstance(error_info.value, InstanceSecretLinkError)
    assert error_info.value.response.status_code == 400


def test_a_400_with_a_body_that_is_not_json_raises_data_masque_api_error(client):
    with requests_mock.Mocker() as m:
        m.post(f"{LINK_URL}/request/verify/", text="<html>Bad Request</html>", status_code=400)
        with pytest.raises(
            DataMasqueApiError,
            match=re.escape(f"API request to {LINK_URL}/request/verify/ failed with status 400"),
        ) as error_info:
            client.verify_instance_secret_link_request("DMLINK-REQUEST")

    assert not isinstance(error_info.value, InstanceSecretLinkError)
    assert error_info.value.response.status_code == 400


def test_a_failed_response_step_does_not_log_the_response_code(client, caplog):
    with requests_mock.Mocker() as m:
        m.post(f"{LINK_URL}/response/accept/", json={"detail": "Server error."}, status_code=500)
        with pytest.raises(
            DataMasqueApiError,
            match=re.escape(f"API request to {LINK_URL}/response/accept/ failed with status 500"),
        ) as error_info:
            client.accept_instance_secret_link_response("DMLINK-RESPONSE", "VERIFICATION-TOKEN")

    assert error_info.value.response.status_code == 500
    assert "Request data was: {'response_code': '<redacted>', 'verification_token': '<redacted>'}" in caplog.text
    assert "DMLINK-RESPONSE" not in caplog.text
    assert "VERIFICATION-TOKEN" not in caplog.text


def test_a_linking_step_without_an_instance_secret_raises_data_masque_api_error(client):
    with requests_mock.Mocker() as m:
        m.post(f"{LINK_URL}/request/accept/", json={"detail": "No instance secret."}, status_code=503)
        with pytest.raises(
            DataMasqueApiError,
            match=re.escape(f"API request to {LINK_URL}/request/accept/ failed with status 503"),
        ) as error_info:
            client.accept_instance_secret_link_request("DMLINK-REQUEST")

    assert not isinstance(error_info.value, InstanceSecretLinkError)
    assert error_info.value.response.status_code == 503


@pytest.mark.parametrize("link_kind", [InstanceSecretLinkKind.shared, InstanceSecretLinkKind.adopted])
def test_verify_instance_secret_link_response_parses_the_links_ending(client, link_kind):
    with requests_mock.Mocker() as m:
        m.post(
            f"{LINK_URL}/response/verify/",
            json={
                "peer_hostname": "other.example.com",
                "response_fingerprint": "ABCD-EFGH-JKMN-PQRS",
                "links_ending": [
                    link_json(
                        link_kind.value,
                        peer_hostname="third.example.com",
                        request_fingerprint="7KQ2-9MXA-3HDP-T0WZ",
                        response_fingerprint="WXYZ-2345-6789-BCDF",
                    )
                ],
                "verification_token": "token",
            },
        )
        verification = client.verify_instance_secret_link_response("DMLINK-RESPONSE")

    assert m.last_request.json() == {"response_code": "DMLINK-RESPONSE"}
    assert verification.peer_hostname == "other.example.com"
    link_ending = verification.links_ending[0]
    assert isinstance(link_ending, InstanceSecretLink)
    assert link_ending.kind is link_kind
    assert link_ending.peer_hostname == "third.example.com"
    assert link_ending.request_fingerprint == "7KQ2-9MXA-3HDP-T0WZ"
    assert link_ending.response_fingerprint == "WXYZ-2345-6789-BCDF"
    assert verification.verification_token == "token"


def test_accept_instance_secret_link_response_returns_the_in_flight_sync_result(client):
    with requests_mock.Mocker() as m:
        m.post(f"{LINK_URL}/response/accept/", json={"in_flight_sync": "notify_failed"})
        result = client.accept_instance_secret_link_response("DMLINK-RESPONSE", "token")

    assert m.last_request.json() == {"response_code": "DMLINK-RESPONSE", "verification_token": "token"}
    assert result is InFlightSyncResult.notify_failed


@pytest.mark.parametrize(
    ("method_name", "arguments", "empty_name"),
    [
        ("verify_instance_secret_link_request", ("",), "request_code"),
        ("accept_instance_secret_link_request", ("",), "request_code"),
        ("verify_instance_secret_link_response", ("",), "response_code"),
        ("accept_instance_secret_link_response", ("", "token"), "response_code"),
        ("accept_instance_secret_link_response", ("DMLINK-RESPONSE", ""), "verification_token"),
    ],
)
def test_an_empty_code_raises_value_error_before_any_request(client, method_name, arguments, empty_name):
    with requests_mock.Mocker() as m:
        with pytest.raises(ValueError, match=re.escape(f"`{empty_name}` must not be empty.")):
            getattr(client, method_name)(*arguments)

    assert m.call_count == 0
