"""Tests for `BaseClient` (auth, healthcheck, make_request, re-auth retry)."""

import logging
import warnings
from unittest.mock import patch

import pytest
import requests
import requests_mock
from urllib3.exceptions import InsecureRequestWarning

from datamasque.client import DataMasqueClient, RunId
from datamasque.client.exceptions import (
    DataMasqueApiError,
    DataMasqueNotReadyError,
    DataMasqueTransportError,
    DataMasqueUserError,
)
from datamasque.client.models.dm_instance import DataMasqueInstanceConfig
from tests.helpers import make_ok_response


def test_authenticate(client):
    with requests_mock.Mocker() as m:
        m.post(
            "http://test-server/api/auth/token/login/",
            json={"key": "test_token"},
            status_code=200,
        )
        client.authenticate()
        assert client.token == "Token test_token"


def test_authenticate_failure(client):
    with requests_mock.Mocker() as m:
        m.post("http://test-server/api/auth/token/login/", status_code=400)
        with pytest.raises(DataMasqueApiError):
            client.authenticate()


def test_healthcheck_ok(client):
    """`healthcheck` returns without error when the server responds 200."""
    with requests_mock.Mocker() as m:
        m.get("http://test-server/api/healthcheck/", status_code=200)
        client.healthcheck()

    assert m.call_count == 1
    assert m.last_request.method == "GET"
    assert "Authorization" not in m.last_request.headers


def test_healthcheck_server_not_ready(client):
    """
    `healthcheck` raises `DataMasqueNotReadyError` on a 502 response.

    A 502 from the ingress/gateway typically means the application container
    is still starting up and not yet accepting connections.
    """
    with requests_mock.Mocker() as m:
        m.get("http://test-server/api/healthcheck/", status_code=502)
        with pytest.raises(DataMasqueNotReadyError):
            client.healthcheck()


def test_healthcheck_transport_failure(client):
    """`healthcheck` raises `DataMasqueTransportError` when the server cannot be reached."""
    with requests_mock.Mocker() as m:
        m.get(
            "http://test-server/api/healthcheck/",
            exc=requests.exceptions.ConnectionError("connection refused"),
        )
        with pytest.raises(DataMasqueTransportError):
            client.healthcheck()


@pytest.mark.parametrize("verify_ssl", [True, False])
def test_make_request_verify_ssl_true_by_default(config, verify_ssl):
    """Verifies SSL setting is passed through to the `requests` call."""
    config_with_ssl = DataMasqueInstanceConfig(
        base_url=config.base_url,
        username=config.username,
        password=config.password,
        verify_ssl=verify_ssl,
    )
    client = DataMasqueClient(config_with_ssl)

    with patch(
        "datamasque.client.base.requests.request",
        return_value=make_ok_response(),
    ) as mock_request:
        client.make_request("GET", "/api/test/")

    _, kwargs = mock_request.call_args
    assert kwargs["verify"] is verify_ssl


def test_make_request_verify_ssl_true_does_not_touch_global_warning_filter(client):
    """With `verify_ssl=True`, the client should not modify `warnings.filters`."""
    filters_before = list(warnings.filters)

    with patch(
        "datamasque.client.base.requests.request",
        return_value=make_ok_response(),
    ):
        client.make_request("GET", "/api/test/")

    assert warnings.filters == filters_before


def test_make_request_verify_ssl_false_suppresses_warning_locally(config):
    """With `verify_ssl=False`, `InsecureRequestWarning` is suppressed only for the duration of the request."""
    insecure_config = DataMasqueInstanceConfig(
        base_url=config.base_url,
        username=config.username,
        password=config.password,
        verify_ssl=False,
    )
    client = DataMasqueClient(insecure_config)

    def raise_insecure_warning_then_respond(*_args, **_kwargs):
        warnings.warn("unverified HTTPS request", InsecureRequestWarning, stacklevel=2)
        return make_ok_response()

    filters_before = list(warnings.filters)

    with warnings.catch_warnings(record=True) as captured:
        warnings.simplefilter("always")  # ensure we'd otherwise see the warning
        with patch(
            "datamasque.client.base.requests.request",
            side_effect=raise_insecure_warning_then_respond,
        ):
            client.make_request("GET", "/api/test/")

        # The warning raised inside the request call was suppressed by the client.
        assert not any(issubclass(w.category, InsecureRequestWarning) for w in captured)

    # The outer filter stack is restored — no leaked `ignore` entry.
    assert warnings.filters == filters_before


def test_make_request_redacts_sensitive_fields_in_error_log(client, caplog):
    """Secrets in `data` must not be written to the error log when a request fails."""
    request_data = {
        "username": "joebloggs",
        "password": "hunter2",
        "re_password": "hunter2",
        "api_token": "sk-live-xyz",
        "access_key_id": "AKIAIOSFODNN7EXAMPLE",
        "secret_access_key": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
        "my_credential": "blob",
        "PublicKey": "upper-case still matches 'key' case-insensitively",
        "description": "not secret",
    }

    with requests_mock.Mocker() as m:
        m.post("http://test-server/api/anything/", text="boom", status_code=500)
        with caplog.at_level(logging.ERROR, logger="datamasque.client.base"):
            with pytest.raises(DataMasqueApiError):
                client.make_request("POST", "/api/anything/", data=request_data)

    request_log_lines = [r.getMessage() for r in caplog.records if "Request data was" in r.getMessage()]
    assert len(request_log_lines) == 1
    log_line = request_log_lines[0]

    for secret in [
        "hunter2",
        "sk-live-xyz",
        "AKIAIOSFODNN7EXAMPLE",
        "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
        "blob",
        "upper-case still matches 'key' case-insensitively",
    ]:
        assert secret not in log_line, f'Leaked secret "{secret}" in log: {log_line}'

    for sensitive_key in [
        "password",
        "re_password",
        "api_token",
        "access_key_id",
        "secret_access_key",
        "my_credential",
        "PublicKey",
    ]:
        assert f"'{sensitive_key}': '<redacted>'" in log_line, f"Missing redaction for {sensitive_key} in: {log_line}"

    # Non-sensitive fields pass through unchanged
    assert "'username': 'joebloggs'" in log_line
    assert "'description': 'not secret'" in log_line


def test_make_request_non_dict_request_data_not_logged(client, caplog):
    """When request data = a non-dict, it should not be logged."""
    with requests_mock.Mocker() as m:
        m.post("http://test-server/api/anything/", status_code=500)
        with caplog.at_level(logging.ERROR, logger="datamasque.client.base"):
            with pytest.raises(DataMasqueApiError):
                # make_request's signature says `data: Optional[dict]`,
                # but guard against a caller passing e.g. a list anyway.
                client.make_request("POST", "/api/anything/", data=["not", "a", "dict"])  # type: ignore[arg-type]

    assert not any("Request data was" in r.getMessage() for r in caplog.records)


def test_make_request_empty_dict_logged(client, caplog):
    """Request data = empty dict is still logged."""
    with requests_mock.Mocker() as m:
        m.post("http://test-server/api/anything/", status_code=500)
        with caplog.at_level(logging.ERROR, logger="datamasque.client.base"):
            with pytest.raises(DataMasqueApiError):
                client.make_request("POST", "/api/anything/", data={})

    assert any("Request data was: {}" in r.getMessage() for r in caplog.records)


def test_re_authenticate(config):
    with patch.object(DataMasqueClient, "authenticate") as mock_auth:
        client = DataMasqueClient(config)
        with requests_mock.Mocker() as m:
            m.get(
                "http://test-server/api/runs/1/",
                [
                    {"status_code": 401},
                    {
                        "json": {
                            "id": 1,
                            "status": "finished",
                            "mask_type": "database",
                            "source_connection_name": "c",
                            "ruleset_name": "r",
                        },
                        "status_code": 200,
                    },
                ],
            )
            client.get_run_info(RunId(1))
            mock_auth.assert_called_once()


def test_authenticate_uses_token_source_when_provided():
    """`authenticate` invokes `token_source` instead of POSTing username/password."""
    token_source = lambda: "callable-token"  # noqa: E731
    config = DataMasqueInstanceConfig(
        base_url="http://test-server",
        username="test_user",
        token_source=token_source,
    )
    client = DataMasqueClient(config)

    with requests_mock.Mocker() as m:
        # If `authenticate` mistakenly went over HTTP, this matcher would assert and the request would 404.
        client.authenticate()
        assert m.call_count == 0

    assert client.token == "Token callable-token"


def test_token_source_called_again_on_401_retry():
    """A 401 mid-request triggers re-auth, which must call `token_source` again (token may have rotated)."""
    tokens = iter(["t1", "t2"])
    config = DataMasqueInstanceConfig(
        base_url="http://test-server",
        username="test_user",
        token_source=lambda: next(tokens),
    )
    client = DataMasqueClient(config)
    client.authenticate()  # consumes "t1"
    assert client.token == "Token t1"

    with requests_mock.Mocker() as m:
        m.get(
            "http://test-server/api/runs/1/",
            [
                {"status_code": 401},
                {
                    "json": {
                        "id": 1,
                        "status": "finished",
                        "mask_type": "database",
                        "source_connection_name": "c",
                        "ruleset_name": "r",
                    },
                    "status_code": 200,
                },
            ],
        )
        client.get_run_info(RunId(1))

    # The retry consumed the second token from the iterator.
    assert client.token == "Token t2"


def test_token_source_callable_exception_propagates():
    """Errors from `token_source` are surfaced to the caller, not swallowed."""

    def boom() -> str:
        raise RuntimeError("provider unavailable")

    config = DataMasqueInstanceConfig(
        base_url="http://test-server",
        username="test_user",
        token_source=boom,
    )
    client = DataMasqueClient(config)

    with pytest.raises(RuntimeError, match="provider unavailable"):
        client.authenticate()


def test_instance_config_rejects_neither_password_nor_token_source():
    with pytest.raises(DataMasqueUserError, match="Exactly one of `password` or `token_source`"):
        DataMasqueInstanceConfig(base_url="http://test-server", username="test_user")


def test_instance_config_rejects_both_password_and_token_source():
    with pytest.raises(DataMasqueUserError, match="Exactly one of `password` or `token_source`"):
        DataMasqueInstanceConfig(
            base_url="http://test-server",
            username="test_user",
            password="pw",
            token_source=lambda: "tok",
        )
