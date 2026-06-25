from typing import Callable, Optional

from pydantic import BaseModel, ConfigDict, model_validator

from datamasque.client.exceptions import DataMasqueUserError


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

    `spcs_pat` is an optional Snowflake Programmatic Access Token for reaching a
    DataMasque instance hosted behind Snowflake SPCS (Snowpark Container Services)
    app ingress, where `base_url` ends in `.snowflakecomputing.app`. It is sent on
    every request via the `X-SF-SPCS-Authorization` header to clear the Snowflake
    gateway, which strips it before forwarding — so it is independent of, and
    layers underneath, whichever DataMasque auth method (`password` or
    `token_source`) you choose.
    """

    model_config = ConfigDict(arbitrary_types_allowed=True)

    base_url: str
    username: str
    password: Optional[str] = None
    verify_ssl: bool = True
    token_source: Optional[Callable[[], str]] = None
    spcs_pat: Optional[str] = None
    """Snowflake Programmatic Access Token for a DataMasque instance hosted behind
    Snowflake SPCS app ingress (a ``*.snowflakecomputing.app`` ``base_url``).

    Mint the PAT in Snowsight (User profile → Programmatic access tokens) for an
    account that can reach the SPCS app. The client sends it on the
    ``X-SF-SPCS-Authorization`` header so the Snowflake gateway lets the request
    through to DataMasque; the gateway strips the header before forwarding, leaving
    DataMasque's own ``Authorization`` flow untouched. Leave it unset for
    instances that are not behind an SPCS gateway."""

    @model_validator(mode="after")
    def _validate_auth_source(self) -> "DataMasqueInstanceConfig":
        if (self.password is None) == (self.token_source is None):
            raise DataMasqueUserError(
                "Exactly one of `password` or `token_source` must be provided to `DataMasqueInstanceConfig`."
            )
        return self
