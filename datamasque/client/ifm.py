"""
Client for the DataMasque IFM (in-flight masking) HTTP API.

`DataMasqueIfmClient` mirrors the public IFM endpoints in a typed Python interface.
Authentication is JWT-based:
the access token is obtained from the admin server's `/api/auth/jwt/login/` endpoint
and refreshed via `/api/auth/jwt/refresh/` on a 401.
Users may also supply a `token_source` callable in the connection config to bypass admin-server login entirely.
"""

import logging
from contextlib import contextmanager
from typing import Callable, Iterator, Optional, Type, TypeVar, Union
from urllib.parse import urljoin

import requests
from pydantic import BaseModel
from requests import Response

from datamasque.client.base import suppress_insecure_warning_if_needed
from datamasque.client.exceptions import (
    DataMasqueApiError,
    DataMasqueNotReadyError,
    DataMasqueTransportError,
    IfmAuthError,
)
from datamasque.client.models.ifm import (
    DataMasqueIfmInstanceConfig,
    IfmMaskRequest,
    IfmMaskResult,
    IfmTokenInfo,
    RulesetPlan,
    RulesetPlanCreateRequest,
    RulesetPlanPartialUpdateRequest,
    RulesetPlanUpdateRequest,
)
from datamasque.client.models.pagination import IfmPage

logger = logging.getLogger(__name__)

_IfmT = TypeVar("_IfmT", bound=BaseModel)


class DataMasqueIfmClient:
    """
    Client for a DataMasque IFM service.

    Example usage:

    .. code-block:: python

        from datamasque.client import DataMasqueIfmClient, DataMasqueIfmInstanceConfig

        config = DataMasqueIfmInstanceConfig(
            admin_server_base_url="https://datamasque.example.com",
            ifm_base_url="https://datamasque.example.com/ifm",
            username="ifm_user",
            password="ifm_password",
        )
        client = DataMasqueIfmClient(config)

        for plan in client.list_ruleset_plans():
            print(plan.name)

    Authentication happens transparently on the first request,
    with automatic token refresh on expiry.
    """

    access_token: str = ""
    refresh_token: str = ""
    admin_server_base_url: str
    ifm_base_url: str
    username: str
    password: Optional[str]
    verify_ssl: bool
    token_source: Optional[Callable[[], str]]

    def __init__(self, connection_config: DataMasqueIfmInstanceConfig) -> None:
        self.admin_server_base_url = connection_config.admin_server_base_url
        self.ifm_base_url = connection_config.ifm_base_url
        self.username = connection_config.username
        self.password = connection_config.password
        self.verify_ssl = connection_config.verify_ssl
        self.token_source = connection_config.token_source

    def authenticate(self) -> None:
        """Obtain an access (and refresh) token from the admin server, or via `token_source`."""

        if self.token_source is not None:
            self.access_token = self.token_source()
            self.refresh_token = ""
            logger.debug("IFM login success via token_source")
            return

        login_url = urljoin(self.admin_server_base_url, "/api/auth/jwt/login/")
        try:
            with self._maybe_suppress_insecure_warning():
                response = requests.post(
                    login_url,
                    json={"username": self.username, "password": self.password},
                    verify=self.verify_ssl,
                )
        except requests.RequestException as e:
            raise DataMasqueTransportError(f"Failed to reach admin server at {login_url}: {e}") from e

        if response.status_code != 200:
            logger.error("IFM JWT login failed: status %s", response.status_code)
            raise IfmAuthError(f"Unable to obtain IFM JWT from admin server (status {response.status_code}).")

        body = response.json()
        self.access_token = body["access_token"]
        self.refresh_token = body.get("refresh_token", "")
        logger.debug("IFM JWT login success")

    def _refresh_or_reauth(self) -> None:
        """Refresh the access token using the cached refresh token, or fall back to a full re-login."""

        if self.token_source is not None or not self.refresh_token:
            self.authenticate()
            return

        refresh_url = urljoin(self.admin_server_base_url, "/api/auth/jwt/refresh/")
        try:
            with self._maybe_suppress_insecure_warning():
                response = requests.post(
                    refresh_url,
                    json={"refresh": self.refresh_token},
                    verify=self.verify_ssl,
                )
        except requests.RequestException as e:
            raise DataMasqueTransportError(f"Failed to reach admin server at {refresh_url}: {e}") from e

        if response.status_code == 200:
            self.access_token = response.json()["access_token"]
            logger.debug("IFM JWT refresh success")
            return

        # Refresh failed (probably expired) — fall back to a full login.
        logger.debug("IFM JWT refresh failed (status %s); re-authenticating", response.status_code)
        self.authenticate()

    @contextmanager
    def _maybe_suppress_insecure_warning(self) -> Iterator[None]:
        with suppress_insecure_warning_if_needed(self.verify_ssl):
            yield

    def _iter_ifm_paginated(
        self,
        path: str,
        model: Type[_IfmT],
        *,
        page_size: int = 100,
    ) -> Iterator[_IfmT]:
        """Iterate every `T` across all pages of an IFM list endpoint."""

        offset = 0
        while True:
            response = self._make_request("GET", path, params={"limit": page_size, "offset": offset})
            page = IfmPage[model].model_validate(response.json())  # type: ignore[valid-type]
            yield from page.items
            offset += len(page.items)
            if not page.items or offset >= page.total:
                return

    def _make_request(
        self,
        method: str,
        path: str,
        *,
        json_body: Optional[Union[dict, list]] = None,
        params: Optional[dict] = None,
        require_status_check: bool = True,
    ) -> Response:
        """
        Send an authenticated HTTP request to the IFM service.

        Adds `Authorization: Bearer <jwt>`,
        triggers a refresh-and-retry on a 401,
        and raises `DataMasqueApiError` on a non-2xx final response when `require_status_check` is true.
        """

        if not self.access_token:
            self.authenticate()

        url = urljoin(self.ifm_base_url.rstrip("/") + "/", path.lstrip("/"))

        def send() -> Response:
            try:
                with self._maybe_suppress_insecure_warning():
                    return requests.request(
                        method,
                        url,
                        json=json_body,
                        params=params,
                        headers={"Authorization": f"Bearer {self.access_token}"},
                        verify=self.verify_ssl,
                    )
            except requests.RequestException as e:
                raise DataMasqueTransportError(f"Failed to reach IFM server at {url}: {e}") from e

        response = send()
        if response.status_code == 401:
            logger.debug("IFM 401 — refreshing token and retrying")
            self._refresh_or_reauth()
            response = send()

        if require_status_check and not response.ok:
            if response.status_code == 502:
                raise DataMasqueNotReadyError

            raise DataMasqueApiError(
                f"IFM API request to {response.request.url} failed with status {response.status_code}",
                response=response,
            )

        return response

    def verify_token(self) -> IfmTokenInfo:
        """`GET /verify-token/` — returns the list of scopes granted to the current JWT."""

        return IfmTokenInfo.model_validate(self._make_request("GET", "verify-token/").json())

    def iter_ruleset_plans(self) -> Iterator[RulesetPlan]:
        """Lazily iterate all ruleset plans via the paginated IFM endpoint."""

        return self._iter_ifm_paginated("ruleset-plans/", model=RulesetPlan)

    def list_ruleset_plans(self) -> list[RulesetPlan]:
        """`GET /ruleset-plans/` — list every ruleset plan visible to the current JWT."""

        return list(self.iter_ruleset_plans())

    def get_ruleset_plan(self, plan_name: str) -> RulesetPlan:
        """`GET /ruleset-plans/{plan_name}/` — fetch one plan including its ruleset YAML."""

        return RulesetPlan.model_validate(self._make_request("GET", f"ruleset-plans/{plan_name}/").json())

    def create_ruleset_plan(self, plan: RulesetPlanCreateRequest) -> RulesetPlan:
        """`POST /ruleset-plans/` — create a new plan; returns the persisted view including its URL."""

        data = plan.model_dump(exclude_none=True, mode="json")
        return RulesetPlan.model_validate(self._make_request("POST", "ruleset-plans/", json_body=data).json())

    def update_ruleset_plan(self, plan_name: str, plan: RulesetPlanUpdateRequest) -> RulesetPlan:
        """`PUT /ruleset-plans/{plan_name}/` — full replace of an existing plan."""

        data = plan.model_dump(exclude_none=True, mode="json")
        return RulesetPlan.model_validate(
            self._make_request("PUT", f"ruleset-plans/{plan_name}/", json_body=data).json()
        )

    def patch_ruleset_plan(self, plan_name: str, plan: RulesetPlanPartialUpdateRequest) -> RulesetPlan:
        """`PATCH /ruleset-plans/{plan_name}/` — partial update; only fields set on `plan` are sent."""

        data = plan.model_dump(exclude_none=True, mode="json")
        return RulesetPlan.model_validate(
            self._make_request("PATCH", f"ruleset-plans/{plan_name}/", json_body=data).json()
        )

    def delete_ruleset_plan(self, plan_name: str) -> None:
        """`DELETE /ruleset-plans/{plan_name}/` — no-op on the client side; raises on non-2xx server response."""

        self._make_request("DELETE", f"ruleset-plans/{plan_name}/")

    def mask(self, plan_name: str, request: IfmMaskRequest) -> IfmMaskResult:
        """
        `POST /ruleset-plans/{plan_name}/mask/` — execute the named ruleset plan against `request.data`.

        Returns an `IfmMaskResult` with `success=True` when the server returns 2xx
        (`data` carries the masked records),
        or `success=False` when the server returns a soft failure
        (HTTP 400 with the full mask-result shape — `data` omitted, `logs` populated).
        Network, auth, and other hard errors still raise
        `DataMasqueApiError` / `IfmAuthError` / `DataMasqueNotReadyError`.
        """

        data = request.model_dump(exclude_none=True, mode="json")
        response = self._make_request(
            "POST",
            f"ruleset-plans/{plan_name}/mask/",
            json_body=data,
            require_status_check=False,
        )
        body = response.json() if response.content else {}

        if response.ok:
            return IfmMaskResult.model_validate(body | {"success": True})

        # The server returns soft failures as HTTP 400 with the full IfmMaskResult body
        # (`ruleset_plan` populated, `data` omitted, `logs` carries the detail).
        # Any other 4xx/5xx is a hard error and still raises.
        if response.status_code == 400 and isinstance(body, dict) and "ruleset_plan" in body:
            return IfmMaskResult.model_validate(body | {"success": False})

        if response.status_code == 502:
            raise DataMasqueNotReadyError

        raise DataMasqueApiError(
            f"IFM API request to {response.request.url} failed with status {response.status_code}",
            response=response,
        )
