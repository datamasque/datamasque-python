import logging
import warnings
from contextlib import contextmanager
from dataclasses import dataclass
from io import BufferedIOBase, BytesIO, TextIOBase
from pathlib import Path
from typing import Any, Callable, Iterator, Optional, Type, TypeVar, Union
from urllib.parse import urljoin

import requests
from pydantic import BaseModel
from requests import Response
from urllib3.exceptions import InsecureRequestWarning

from datamasque.client.exceptions import (
    DataMasqueApiError,
    DataMasqueNotReadyError,
    DataMasqueTransportError,
)
from datamasque.client.models.dm_instance import DataMasqueInstanceConfig

logger = logging.getLogger(__name__)

FileOrContent = Union[str, bytes, TextIOBase, BufferedIOBase, Path]
_T = TypeVar("_T", bound=BaseModel)

# Substrings (case-insensitive) that mark a key whose value should be redacted
# before logging on an error path, so that passwords, API tokens, and similar secrets don't
# end up in user-visible logs when a request fails.
# Applied to both outgoing request bodies and incoming response bodies (if JSON-parseable to a dict).
SENSITIVE_DATA_KEYS = ("password", "secret", "token", "key", "credential")


def _redact_sensitive(value: Any) -> Any:
    """Return `value` with sensitive keys redacted, if it's a dict; otherwise unchanged."""

    if isinstance(value, dict):
        return {
            k: "<redacted>" if any(word in str(k).lower() for word in SENSITIVE_DATA_KEYS) else v
            for k, v in value.items()
        }

    return value


@contextmanager
def suppress_insecure_warning_if_needed(verify_ssl: bool) -> Iterator[None]:
    """Scope-limited suppression of `InsecureRequestWarning` when TLS verification is disabled."""

    if verify_ssl:
        yield
        return
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", category=InsecureRequestWarning)
        yield


@dataclass
class UploadFile:
    """Represents a file to upload in a multipart form request."""

    field_name: str
    filename: str
    content: BufferedIOBase
    content_type: Optional[str] = None


class BaseClient:
    """
    Shared state and HTTP plumbing for every feature client mixin.

    Holds the connection config, cached auth token, and the core `make_request` dispatcher
    used by all per-feature mixins that compose `DataMasqueClient`.
    """

    token: str = ""
    base_url: str
    username: str
    password: Optional[str]
    verify_ssl: bool
    token_source: Optional[Callable[[], str]]

    def __init__(self, connection_config: DataMasqueInstanceConfig) -> None:
        self.base_url = connection_config.base_url
        self.username = connection_config.username
        self.password = connection_config.password
        self.verify_ssl = connection_config.verify_ssl
        self.token_source = connection_config.token_source

    @contextmanager
    def _maybe_suppress_insecure_warning(self) -> Iterator[None]:
        # `urllib3.disable_warnings` is global,
        # so instead we scope the suppression to this single call via `warnings.catch_warnings`.
        # Clients that leave `verify_ssl=True` never touch the warning filter at all.
        with suppress_insecure_warning_if_needed(self.verify_ssl):
            yield

    def authenticate(self) -> None:
        """
        Authenticate against the DataMasque server and cache the resulting token.

        Called implicitly by `make_request` on the first request and on a 401 response,
        so you generally do not need to call this yourself.

        When the client was constructed with a `token_source` callable,
        the callable is invoked instead of POSTing to the login endpoint.
        """

        if self.token_source is not None:
            self.token = f"Token {self.token_source()}"
            logger.debug("Login Success via token_source")
            return

        login_url = urljoin(self.base_url, "/api/auth/token/login/")
        response = self.make_request(
            method="POST",
            path=login_url,
            data={"username": self.username, "password": self.password},
            requires_authorization=False,
            require_status_check=False,
        )

        if response.status_code == 200:
            self.token = f"Token {response.json()['key']}"
            logger.debug("Login Success: %s", self.token)
        else:
            logger.error("Login Failure")
            raise DataMasqueApiError(
                "Unable to login to DataMasque Client, please ensure that login credentials are correct",
                response=response,
            )

    def healthcheck(self) -> None:
        """
        Pings the server's unauthenticated healthcheck endpoint.

        Returns without error when the server is up and ready to accept requests.
        """

        self.make_request("GET", "/api/healthcheck/", requires_authorization=False)

    def make_request(
        self,
        method: str,
        path: str,
        *,
        data: Optional[dict] = None,
        params: Optional[dict] = None,
        files: Optional[list[UploadFile]] = None,
        requires_authorization: bool = True,
        require_status_check: bool = True,
    ) -> Response:
        """
        Sends an HTTP request to the DataMasque server and returns the `Response`.

        When `requires_authorization` is true (the default),
        the current auth token is sent in the request headers,
        and a 401 response triggers one re-auth-and-retry.

        Args:
            method: HTTP method (e.g. `"GET"`, `"POST"`).
            path: URL path such as `/api/license/`.
              Must include a trailing slash.
            data: Request body.
              Serialised as JSON for normal requests,
              and as multipart form data when `files` is also provided.
            params: Query string parameters,
              merged into the URL as `?key=value&...`.
            files: Multipart form uploads;
              when set, the request is sent as `multipart/form-data` and `data` is sent alongside as form fields.
            requires_authorization: When true (the default),
              the current auth token is attached and a 401 triggers one re-auth-and-retry.
            require_status_check: When true (the default),
              a non-2xx response raises one of the exceptions below;
              when false, the `Response` is returned regardless of status so the caller can inspect it directly.

        Raises:
            DataMasqueApiError: When `require_status_check` is true (the default) and the response is non-2xx.
              The response object is available on the `.response` attribute of the exception.
            DataMasqueNotReadyError: When `require_status_check` is true and the response is 502.
              502 typically indicates the server is still starting up.
            DataMasqueTransportError: When the request fails before any response is received
              (connection refused, timeout, DNS failure, SSL handshake failure, etc.).
        """

        url = urljoin(self.base_url, path)

        def send() -> Response:
            headers: Optional[dict] = {"Authorization": self.token} if requires_authorization else None
            try:
                with self._maybe_suppress_insecure_warning():
                    if files:
                        files_payload = {f.field_name: (f.filename, f.content, f.content_type or "") for f in files}
                        return requests.request(
                            method,
                            url,
                            data=data,
                            params=params,
                            headers=headers,
                            files=files_payload,
                            verify=self.verify_ssl,
                        )
                    return requests.request(
                        method, url, json=data, params=params, headers=headers, verify=self.verify_ssl
                    )
            except requests.RequestException as e:
                raise DataMasqueTransportError(f"Failed to reach DataMasque server at {url}: {e}") from e

        response = send()
        if response.status_code == 401:
            logger.debug("Re-authenticating")
            self.authenticate()
            # Reset file pointers so the retry doesn't send empty files
            if files:
                for f in files:
                    f.content.seek(0)
            response = send()

        if require_status_check:
            self._raise_for_status(response, request_data=data)

        return response

    def _raise_for_status(self, response: Response, *, request_data: Optional[dict] = None) -> None:
        if response.ok:
            return

        if response.status_code == 502:
            # Bad Gateway error returned when DM is still initializing
            raise DataMasqueNotReadyError

        # Redact sensitive keys from the response body before logging,
        # in case the server echoes back caller-supplied credentials in an error payload.
        try:
            response_body: Any = response.json()
        except ValueError:
            response_body = response.text or response.content
        logger.error("Error when calling API: %s", _redact_sensitive(response_body))
        if isinstance(request_data, dict):
            logger.error("Request data was: %s", _redact_sensitive(request_data))

        raise DataMasqueApiError(
            f"API request to {response.request.url} failed with status {response.status_code}",
            response=response,
        )

    def _delete_if_exists(self, path: str, *, params: Optional[dict] = None) -> None:
        response = self.make_request("DELETE", path, params=params, require_status_check=False)
        if response.status_code == 404:
            return

        self._raise_for_status(response)

    def _iter_paginated(
        self,
        path: str,
        model: Type[_T],
        *,
        params: Optional[dict] = None,
        page_size: int = 100,
    ) -> Iterator[_T]:
        """
        Iterate every `T` across all pages of an admin-server list endpoint.

        Opts into pagination by sending `limit`/`offset` on the first request,
        then follows the absolute `next` URL returned by the server.
        """

        first_params = dict(params or {})
        first_params.setdefault("limit", page_size)
        first_params.setdefault("offset", 0)

        url: Optional[str] = path
        current_params: Optional[dict] = first_params

        while url:
            response = self.make_request("GET", url, params=current_params)
            data = response.json()
            yield from (model.model_validate(item) for item in data["results"])
            url = data.get("next")
            # The `next` URL is absolute and already contains the pagination cursor;
            # do not re-send our initial params alongside it.
            current_params = None


def read_file_or_content(file_or_content: FileOrContent, fallback_file_name: str) -> tuple[str, BufferedIOBase]:
    """
    Takes either a filename (str), file path (Path), or some file content.

    Where content is provided, the filename is given by `fallback_file_name`.
    Returns a tuple of the filename and a BytesIO containing the file content.
    """

    if isinstance(file_or_content, (str, Path)):
        file_name = Path(file_or_content).name
        with open(file_or_content, "rb") as file:
            return file_name, BytesIO(file.read())

    if isinstance(file_or_content, bytes):
        file_or_content = BytesIO(file_or_content)
    elif isinstance(file_or_content, TextIOBase):
        file_or_content = BytesIO(file_or_content.read().encode())

    return fallback_file_name, file_or_content
