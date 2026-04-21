"""Typed request and response shapes for the IFM (in-flight masking) HTTP API."""

from datetime import datetime
from typing import Any, Callable, Optional

from pydantic import BaseModel, ConfigDict, model_validator

from datamasque.client.exceptions import DataMasqueUserError


class RulesetPlanOptions(BaseModel):
    """
    Server-defined defaults applied when a mask request omits the corresponding fields.

    All keys are optional;
    callers can supply any subset (or none) and the IFM server fills in remaining defaults.
    """

    model_config = ConfigDict(extra="forbid")

    enabled: Optional[bool] = None
    # NB: Encoding and charset are not currently implemented for IFM.
    # These fields are here just to ensure we can round-trip a `RulesetPlan` object.
    default_encoding: Optional[str] = None
    default_charset: Optional[str] = None
    default_log_level: Optional[str] = None


class IfmLog(BaseModel):
    """A single log entry produced by IFM during a mask call or a ruleset-plan validation."""

    model_config = ConfigDict(extra="allow")

    log_level: str
    timestamp: str
    message: str


class IfmRulesetPlanRef(BaseModel):
    """Reference to a ruleset plan embedded in a mask response."""

    model_config = ConfigDict(extra="allow")

    name: str
    serial: int


class RulesetPlan(BaseModel):
    """
    Unified model for IFM ruleset plans.

    Collapses the list/detail/create/update response shapes into one model
    with optional fields for parts that differ by endpoint.
    """

    model_config = ConfigDict(extra="allow")

    name: str
    serial: int
    created_time: datetime
    modified_time: datetime
    options: RulesetPlanOptions
    ruleset_yaml: Optional[str] = None
    logs: Optional[list[IfmLog]] = None
    url: Optional[str] = None


class RulesetPlanCreateRequest(BaseModel):
    """Request body for `POST /ifm/ruleset-plans/`."""

    model_config = ConfigDict(extra="forbid")

    name: str
    ruleset_yaml: str
    options: Optional[RulesetPlanOptions] = None


class RulesetPlanUpdateRequest(BaseModel):
    """Request body for `PUT /ifm/ruleset-plans/{name}/`."""

    model_config = ConfigDict(extra="forbid")

    ruleset_yaml: str
    options: Optional[RulesetPlanOptions] = None


class RulesetPlanPartialUpdateRequest(BaseModel):
    """Request body for `PATCH /ifm/ruleset-plans/{name}/` — every field is optional."""

    model_config = ConfigDict(extra="forbid")

    ruleset_yaml: Optional[str] = None
    options: Optional[RulesetPlanOptions] = None


class IfmMaskRequest(BaseModel):
    """
    Request body for `POST /ruleset-plans/{name}/mask/`.

    `data` is the list of records to be masked;
    every other field overrides server defaults configured on the plan.
    """

    model_config = ConfigDict(extra="forbid")

    data: list[Any]
    disable_instance_secret: Optional[bool] = None
    run_secret: Optional[str] = None
    hash_values: Optional[Any] = None
    log_level: Optional[str] = None
    request_id: Optional[str] = None
    ai_engine_url: Optional[str] = None


class IfmMaskResult(BaseModel):
    """
    Response shape for `POST /ruleset-plans/{name}/mask/`.

    `success` is populated by the client based on the HTTP status the server returned:

    - `True` — masking completed;
      `data` carries the masked records (possibly an empty list if the request had no input).
    - `False` — the server rejected the request with a soft failure
      (e.g. a masking function received an unsupported value type);
      `data` is omitted and details surface in `logs`.

    Hard failures (plan not found, auth, transport) still raise rather than producing an `IfmMaskResult`.
    """

    model_config = ConfigDict(extra="allow")

    success: bool
    request_id: Optional[str] = None
    ruleset_plan: Optional[IfmRulesetPlanRef] = None
    logs: Optional[list[IfmLog]] = None
    data: Optional[list[Any]] = None


class IfmTokenInfo(BaseModel):
    """Response body for `GET /verify-token/` — the list of scopes granted to the current JWT."""

    model_config = ConfigDict(extra="allow")

    scopes: list[str]


class DataMasqueIfmInstanceConfig(BaseModel):
    """
    Connection configuration for `DataMasqueIfmClient`.

    `admin_server_base_url` is where JWTs are obtained and refreshed;
    `ifm_base_url` is where the IFM API itself lives
    (typically a separate hostname or the admin server with `/ifm` prefix).
    Exactly one of `password` or `token_source` must be set.
    `token_source` is a user-supplied callable that returns the bare JWT access token string —
    the value issued by the admin server's `/api/auth/jwt/login/` endpoint;
    the client prepends it with `Bearer ` when sending the `Authorization` header.
    The client calls `token_source` on each authentication and refresh,
    so the callable is free to fetch and refresh tokens out-of-band (e.g. from a secrets manager).
    """

    model_config = ConfigDict(arbitrary_types_allowed=True)

    admin_server_base_url: str
    ifm_base_url: str
    username: str
    password: Optional[str] = None
    verify_ssl: bool = True
    token_source: Optional[Callable[[], str]] = None

    @model_validator(mode="after")
    def _validate_auth_source(self) -> "DataMasqueIfmInstanceConfig":
        if (self.password is None) == (self.token_source is None):
            raise DataMasqueUserError(
                "Exactly one of `password` or `token_source` must be provided to `DataMasqueIfmInstanceConfig`."
            )
        return self
