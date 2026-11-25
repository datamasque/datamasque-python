"""Remote provider models for the DataMasque API."""

import enum
from datetime import datetime
from typing import Any, NewType, Optional, Union

from pydantic import BaseModel, ConfigDict, Field, field_serializer, field_validator

from datamasque.client.models.files import CertificateFile, FileId

RemoteProviderId = NewType("RemoteProviderId", str)

Path = list[Union[str, int]]
"""
A path of keys and indexes into JSON, such as `["body", "values", "*"]`.

A request path starts with `body`, `query` or `header`.
`*` marks where each value of a request goes, or where each output of a response is.
"""

ParameterValue = Union[str, int, float, bool]


class CertificateVerification(enum.Enum):
    """How DataMasque verifies the certificate of an `https` base URL."""

    uploaded_root_ca = "uploaded_root_ca"
    """Trust only the CA certificates in the provider's `certificate_file`."""
    public_cas = "public_cas"
    """Trust the public certificate authorities."""
    none = "none"
    """Encrypt without verifying the certificate."""


class RemoteProviderAuthType(enum.Enum):
    """How DataMasque authenticates to a remote provider."""

    none = "none"
    static_header = "static_header"
    """Send the secret as the value of the `header_name` header."""
    oauth2_client_credentials = "oauth2_client_credentials"
    """Get a token from `token_url` with `client_id` and the secret, and send it as a bearer token."""


class TokenBodyFormat(enum.Enum):
    """The format of the OAuth2 token request body."""

    form = "form"
    json = "json"


class RemoteProviderMethod(enum.Enum):
    """The HTTP method of a request to a remote provider."""

    POST = "POST"
    PUT = "PUT"


class RemoteProviderParameter(BaseModel):
    """
    A parameter that a ruleset gives a remote provider, and where it goes in a request.

    A `body` path can have one `*` to repeat the parameter for each value.
    A `query` or `header` path has two elements and no `*`.
    The names `dm_values`, `dm_value` and `dm_key`, in any case, are reserved and refused by the server.
    """

    model_config = ConfigDict(extra="allow")

    name: str
    required: bool
    paths: list[Path]


class RemoteProviderItemValue(BaseModel):
    """
    A fixed value that DataMasque writes into every item of the repeated list in a request body.

    The path starts with `body` and has one `*`, followed by the field to write.
    The part before `*` is the list that the value paths repeat,
    for example `["body", "items", "*", "type"]` with the value path `["body", "items", "*", "data"]`.
    """

    model_config = ConfigDict(extra="allow")

    path: Path
    value: Any
    """Any JSON value."""


class RemoteProvider(BaseModel):
    """
    A service, hosted by the customer, that the `remote_http` mask sends values to over HTTP.

    Only an administrator can read or change remote providers.
    The server never returns the secret: `has_secret` says whether one is saved.
    Set `secret` to send a new one; leave it as `None` to keep the saved one.
    The server refuses an update that changes the base URL or the token URL,
    or turns certificate verification to `none`, without a new secret.

    The server clears the fields that the chosen options do not use:
    the certificate fields for an `http` base URL,
    and the fields of the authentication types other than `auth_type`.
    """

    model_config = ConfigDict(extra="allow", populate_by_name=True)

    name: str
    """The name rulesets use for the provider. It cannot change after the provider is created."""
    base_url: str
    """The `http` or `https` URL of the provider."""
    certificate_verification: Optional[CertificateVerification] = CertificateVerification.uploaded_root_ca
    """How DataMasque verifies the certificate of an `https` base URL; `None` for an `http` base URL."""
    certificate_file: Optional[FileId] = None
    """
    The `CertificateFile` whose CA certificates DataMasque trusts, with `uploaded_root_ca`.

    Accepts a `FileId` or a `CertificateFile` (its `id` is extracted).
    """
    timeout_seconds: int = 30
    auth_type: RemoteProviderAuthType = RemoteProviderAuthType.none
    token_url: str = ""
    """OAuth2 only: the token URL, as an absolute URL or a path relative to the base URL."""
    token_body_format: Optional[TokenBodyFormat] = None
    """OAuth2 only: whether the token request body is a form or JSON. The server defaults it to `form`."""
    client_id: str = ""
    """OAuth2 only."""
    token_field_path: Optional[Path] = None
    """OAuth2 only: the path of the token in the token response. The server defaults it to `["access_token"]`."""
    header_name: str = ""
    """Static header only: the header that carries the secret, for example `X-API-Key`."""
    secret: Optional[str] = Field(default=None, repr=False)
    """The client secret for OAuth2, or the header value for a static header. Write-only."""
    method: RemoteProviderMethod = RemoteProviderMethod.POST
    path: str
    """The path of a request, relative to the base URL, for example `/api/v1/protect`."""
    headers: dict[str, str] = {}
    """Headers that every request carries. A header that also carries the secret is refused."""
    query: dict[str, str] = {}
    """
    Fixed query string pairs, name to value, that every request carries.

    For example, `{"tenant": "claims"}` adds `?tenant=claims`.
    They follow the path, in name order, before the parameters' query pairs.
    A value cannot hold a `{{...}}` placeholder,
    and a name cannot also be the name of a parameter's `query` path.
    """
    body: dict[str, Any] = {}
    """The JSON body that every request starts from."""
    value_paths: list[Path]
    """Where the values go in a request. Each path starts with `body` and has one `*`."""
    item_values: list[RemoteProviderItemValue] = []
    """
    Fixed fields that DataMasque writes into every item of the repeated list in a request body.

    Refused when a value path ends in `*`, since a list of plain values has no item to hold a field.
    A path cannot equal or overlap a value path, a parameter path, or another item value's path.
    """
    parameters: list[RemoteProviderParameter] = []
    response_value_path: Path
    """Where the outputs are in the response, with one `*`."""
    max_values_per_request: int = 1000
    """The most values in one request, up to 100000. `0` is no limit."""
    max_request_bytes: int = 1048576
    """The largest request body, in bytes, up to 104857600. `0` is no limit."""
    max_concurrent_requests: int = 4
    """How many requests can be open at once, from 1 to 64."""

    # Server-populated read-only fields, excluded from request bodies.
    id: Optional[RemoteProviderId] = Field(default=None, exclude=True)
    has_secret: bool = Field(default=False, exclude=True)
    created_date: Optional[datetime] = Field(default=None, exclude=True)
    modified_date: Optional[datetime] = Field(default=None, exclude=True)

    @field_validator("certificate_file", mode="before")
    @classmethod
    def _unwrap_certificate_file(cls, value: object) -> object:
        if isinstance(value, CertificateFile):
            if value.id is None:
                raise ValueError("Certificate file has not been uploaded yet (id is None)")
            return value.id

        return value

    @field_validator("token_body_format", mode="before")
    @classmethod
    def _blank_token_body_format_is_none(cls, value: object) -> object:
        # The server stores no format, as a blank string, for an authentication type other than OAuth2.
        return None if value == "" else value

    @field_serializer("token_body_format")
    def _serialize_token_body_format(self, value: Optional[TokenBodyFormat]) -> str:
        return "" if value is None else value.value


class RemoteProviderTestResult(BaseModel):
    """The provider's output for a test value, or the error. One of the two is `None`."""

    model_config = ConfigDict(extra="allow")

    output: Optional[str]
    error: Optional[str]
