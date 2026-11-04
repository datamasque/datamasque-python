"""Every request the clients send has a connect and a read timeout."""

import requests_mock

from datamasque.client import DataMasqueClient, DataMasqueIfmClient, DataMasqueIfmInstanceConfig
from datamasque.client.models.dm_instance import DEFAULT_CONNECT_TIMEOUT_SECONDS, DEFAULT_READ_TIMEOUT_SECONDS

ADMIN = "http://admin.test"
IFM = "http://ifm.test"


def test_a_request_gets_the_default_timeouts(client):
    with requests_mock.Mocker() as m:
        m.get("http://test-server/api/healthcheck/", status_code=200)
        client.healthcheck()

    assert m.request_history[0].timeout == (DEFAULT_CONNECT_TIMEOUT_SECONDS, DEFAULT_READ_TIMEOUT_SECONDS)


def test_the_timeouts_come_from_the_config(config):
    client = DataMasqueClient(config.model_copy(update={"connect_timeout_seconds": 3, "read_timeout_seconds": 900}))

    with requests_mock.Mocker() as m:
        m.post("http://test-server/api/auth/token/login/", json={"key": "test_token"}, status_code=200)
        client.authenticate()

    assert m.request_history[0].timeout == (3, 900)


def test_every_ifm_request_gets_the_config_timeouts():
    client = DataMasqueIfmClient(
        DataMasqueIfmInstanceConfig(
            admin_server_base_url=ADMIN,
            ifm_base_url=IFM,
            username="ifm_user",
            password="ifm_password",
            read_timeout_seconds=60,
        )
    )
    client.access_token = "access-1"
    client.refresh_token = "refresh-1"

    with requests_mock.Mocker() as m:
        m.get(
            f"{IFM}/ruleset-plans/",
            [{"status_code": 401}, {"json": {"items": [], "total": 0, "limit": 100, "offset": 0}, "status_code": 200}],
        )
        m.post(f"{ADMIN}/api/auth/jwt/refresh/", status_code=401)
        m.post(f"{ADMIN}/api/auth/jwt/login/", json={"access_token": "ACC", "refresh_token": "REF"}, status_code=200)
        client.list_ruleset_plans()

    assert [request.path for request in m.request_history] == [
        "/ruleset-plans/",
        "/api/auth/jwt/refresh/",
        "/api/auth/jwt/login/",
        "/ruleset-plans/",
    ]
    assert {request.timeout for request in m.request_history} == {(DEFAULT_CONNECT_TIMEOUT_SECONDS, 60)}
