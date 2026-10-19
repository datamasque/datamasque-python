"""Every request the clients send has a connect and a read timeout, so a server that never answers cannot hang them."""

from typing import Optional
from unittest.mock import patch

import pytest
import requests
from requests.adapters import HTTPAdapter

from datamasque.client import DataMasqueClient, DataMasqueIfmClient, DataMasqueIfmInstanceConfig
from datamasque.client.models.dm_instance import (
    DEFAULT_CONNECT_TIMEOUT_SECONDS,
    DEFAULT_READ_TIMEOUT_SECONDS,
    DataMasqueInstanceConfig,
)


def _sent_timeout(session: requests.Session, timeout: Optional[float] = None) -> object:
    """The timeout the session's adapter hands to the transport for one GET, given `timeout` by the caller."""
    response = requests.Response()
    response.status_code = 200
    with patch.object(HTTPAdapter, "send", return_value=response) as send:
        session.get("https://datamasque.test/api/healthcheck/", timeout=timeout)
    return send.call_args.kwargs["timeout"]


def test_a_request_gets_the_default_timeouts(config: DataMasqueInstanceConfig) -> None:
    client = DataMasqueClient(config)

    assert _sent_timeout(client._session) == (DEFAULT_CONNECT_TIMEOUT_SECONDS, DEFAULT_READ_TIMEOUT_SECONDS)


def test_the_timeouts_come_from_the_config(config: DataMasqueInstanceConfig) -> None:
    client = DataMasqueClient(config.model_copy(update={"connect_timeout_seconds": 3, "read_timeout_seconds": 900}))

    assert _sent_timeout(client._session) == (3, 900)


def test_a_timeout_given_to_one_request_wins(config: DataMasqueInstanceConfig) -> None:
    client = DataMasqueClient(config)

    assert _sent_timeout(client._session, timeout=7) == 7


def test_the_ifm_client_gets_its_config_timeouts_too() -> None:
    ifm_config = DataMasqueIfmInstanceConfig(
        admin_server_base_url="https://admin.test",
        ifm_base_url="https://ifm.test",
        username="ifm_user",
        password="ifm_password",
        read_timeout_seconds=60,
    )

    client = DataMasqueIfmClient(ifm_config)

    assert _sent_timeout(client._session) == (DEFAULT_CONNECT_TIMEOUT_SECONDS, 60)


@pytest.mark.parametrize("url", ["http://datamasque.test/", "https://datamasque.test/"])
def test_both_schemes_go_through_the_timeout_adapter(config: DataMasqueInstanceConfig, url: str) -> None:
    client = DataMasqueClient(config)

    assert type(client._session.get_adapter(url)).__name__ == "_DefaultTimeoutAdapter"
