from typing import Callable, Optional

from pydantic import BaseModel, ConfigDict, model_validator

from datamasque.client.exceptions import DataMasqueUserError

DEFAULT_CONNECT_TIMEOUT_SECONDS = 10.0
DEFAULT_READ_TIMEOUT_SECONDS = 300.0


class DataMasqueInstanceConfig(BaseModel):
    """
    Connection configuration for `DataMasqueClient`.

    `base_url` is the root URL of the DataMasque admin server
    (e.g. `https://datamasque.example.com/`).
    Set `verify_ssl=False` to skip TLS certificate verification
    (only use this with a self-signed certificate;
    do not disable it otherwise).
    Exactly one of `password` or `token_source` must be set.
    `token_source` is a user-supplied callable that returns the bare API token string —
    the hex value returned by `POST /api/auth/token/login/`;
    the client prepends it with `Token ` when sending the `Authorization` header.
    The client calls `token_source` on each authentication attempt,
    so the callable is free to fetch and refresh tokens out-of-band (e.g. from a secrets manager).

    `spcs_pat` is an optional Snowflake Programmatic Access Token
    for reaching a DataMasque instance hosted on Snowpark Container Services (SPCS).
    It layers underneath whichever DataMasque auth method
    (`password` or `token_source`) you choose.
    """

    model_config = ConfigDict(arbitrary_types_allowed=True)

    base_url: str
    username: str
    password: Optional[str] = None
    verify_ssl: bool = True
    token_source: Optional[Callable[[], str]] = None
    spcs_pat: Optional[str] = None
    """Snowflake Programmatic Access Token
    for a DataMasque instance hosted on Snowpark Container Services (SPCS),
    where `base_url` ends in `.snowflakecomputing.app`.

    Create the token in Snowsight (User profile → Programmatic access tokens)
    for an account that can reach the SPCS app.
    Leave unset for instances that are not hosted on SPCS."""
    connect_timeout_seconds: float = DEFAULT_CONNECT_TIMEOUT_SECONDS
    """How long a request may take to connect, including the TLS handshake."""
    read_timeout_seconds: float = DEFAULT_READ_TIMEOUT_SECONDS
    """How long a request may wait for the server between bytes of its response."""
    default_project: Optional[str] = None
    """The project a create goes into when the resource names none, as a `<id>-<slug>` reference (e.g. `1-default`)
    or a bare project id.

    Applies to connections, rulesets, discovery configs, RG configs, table references and libraries.
    Leave unset to use the server's only active project."""

    @model_validator(mode="after")
    def _validate_auth_source(self) -> "DataMasqueInstanceConfig":
        if (self.password is None) == (self.token_source is None):
            raise DataMasqueUserError(
                "Exactly one of `password` or `token_source` must be provided to `DataMasqueInstanceConfig`."
            )
        return self
