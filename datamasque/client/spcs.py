"""
Snowflake SPCS app gateway authentication for `DataMasqueClient`.

When a DataMasque instance is hosted on Snowpark Container Services (SPCS),
its app ingress (`*.snowflakecomputing.app`) fronts every request
with a Snowflake gateway that must be cleared first.
We authenticate to the gateway with a Programmatic Access Token (PAT),
sent on `X-SF-SPCS-Authorization: Snowflake Token="<PAT>"`.
The gateway accepts the PAT on this alternate header
and strips it before forwarding to the container,
so DataMasque's own `Authorization: Token <key>` flow rides through untouched.

`install_spcs_gateway_auth` attaches this behaviour to a client's `requests.Session`:
it sets the header on the session
(so it is sent on every request, including the unauthenticated login)
and registers a response hook
that turns a gateway-originated rejection into a clear `SpcsGatewayAuthError`.
"""

import re
from typing import Any, Optional

import requests

from datamasque.client.exceptions import SpcsGatewayAuthError

SPCS_GATEWAY_AUTH_HEADER = "X-SF-SPCS-Authorization"

# Body-shape discriminators for SPCS gateway error responses.
# The gateway emits JSON with `responseType` (ERROR_<UPPER_SNAKE>), `requestId`
# (canonical UUID), and `detail` (free text). All three must be present and
# match these patterns for the body to count as gateway-originated.
_GATEWAY_RESPONSE_TYPE_RE = re.compile(r"^ERROR_[A-Z][A-Z0-9_]+$")
_UUID_RE = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
    re.IGNORECASE,
)

# Header-shape discriminators for "this response transited a Snowflake SPCS
# gateway". The Server header and the `sfc-ss-` cookie name prefix both appear
# on every gateway-handled response (success and error alike) and aren't
# plausible to spoof by accident.
_SPCS_GATEWAY_SERVER_VALUE = "_"
_SPCS_COOKIE_PREFIX = "sfc-ss-"


def _has_spcs_gateway_header_signature(response: requests.Response) -> bool:
    """
    True if at least one header-level Snowflake gateway marker is present.

    Looks for either `Server: _` (the gateway's literal Server header value)
    or any `Set-Cookie` carrying the `sfc-ss-` cookie name prefix.
    Either is sufficient — both indicate the response was emitted by,
    or transited, Snowflake's SPCS ingress.
    """
    if response.headers.get("server", "").strip() == _SPCS_GATEWAY_SERVER_VALUE:
        return True
    # `Set-Cookie` may appear multiple times; `requests` flattens duplicates
    # via a comma-separated value in `.headers`, but our prefix substring
    # check is order- and count-insensitive.
    return _SPCS_COOKIE_PREFIX in response.headers.get("set-cookie", "")


def _is_spcs_gateway_error_body(response: requests.Response) -> Optional[dict]:
    """
    Return the parsed body iff it is a structurally-valid gateway error.

    All four conditions must hold:
      1. The body parses as JSON and is a dict.
      2. Keys `responseType`, `requestId`, `detail` are all present and string-typed.
      3. `responseType` matches `^ERROR_<UPPER_SNAKE>$`.
      4. `requestId` is a canonical 8-4-4-4-12 UUID.

    Returns the parsed dict (truthy) on match, `None` on miss.
    """
    try:
        data = response.json()
    except ValueError:
        return None
    if not isinstance(data, dict):
        return None
    response_type = data.get("responseType")
    request_id = data.get("requestId")
    detail = data.get("detail")
    if not (isinstance(response_type, str) and isinstance(request_id, str) and isinstance(detail, str)):
        return None
    if _GATEWAY_RESPONSE_TYPE_RE.match(response_type) and _UUID_RE.match(request_id):
        return data
    return None


def _hint_for_gateway_detail(detail: str) -> str:
    """Map common Snowflake gateway `detail` strings to a one-line cause hint."""
    d = (detail or "").lower()
    if "network policy" in d:
        return (
            "PAT requires a network policy attached to the user (or account) "
            "that permits your current public IP. Run `CREATE NETWORK POLICY "
            "... ALLOWED_IP_LIST = ('<your.ip>/32')` and `ALTER USER <pat-user> "
            "SET NETWORK_POLICY = <policy>`."
        )
    if "invalid" in d and "token" in d:
        return (
            "PAT is malformed, expired, or revoked. Create a new PAT in Snowsight "
            "(User profile -> Programmatic access tokens) and update `spcs_pat`."
        )
    if "expired" in d:
        return "PAT has expired. Create a fresh one in Snowsight and update `spcs_pat`."
    if "authentication" in d or "unauthorized" in d:
        return (
            "Generic auth rejection. Verify the PAT was created by a user that "
            "has access to this SPCS app, and that any account-level network "
            "policy includes your current public IP."
        )
    return "Unknown gateway rejection — see the Snowflake `detail` string above and the Snowflake PAT docs."


def _check_spcs_gateway_response(response: requests.Response) -> None:
    """
    Raise `SpcsGatewayAuthError` iff `response` is a gateway-originated rejection.

    Two-layer discriminator — both must hold:
      * **Body originated at the gateway**:
        strict shape match on the JSON body
        (multiple fields, typed, with format constraints)
        via `_is_spcs_gateway_error_body`.
      * **Response transited an SPCS gateway**:
        header signature confirms via `_has_spcs_gateway_header_signature`.

    Either layer alone could in principle false-positive on an unrelated upstream
    that happened to emit one of those signals;
    the conjunction is what makes the check robust.
    Legitimate DataMasque 401s (DRF `{"detail": "..."}`)
    have the gateway header signature but fail the body shape —
    so they correctly flow through
    to the client's normal re-auth-and-retry path untouched.
    """
    if response.status_code not in (401, 403):
        return
    if not _has_spcs_gateway_header_signature(response):
        return
    data = _is_spcs_gateway_error_body(response)
    if data is None:
        return

    response_type = data["responseType"]
    request_id = data["requestId"]
    detail = data["detail"]
    hint = _hint_for_gateway_detail(detail)
    raise SpcsGatewayAuthError(
        f"SPCS gateway rejected the PAT (HTTP {response.status_code}, "
        f"{response_type}). The request never reached DataMasque.\n"
        f'  Snowflake said:   "{detail}"\n'
        f"  Snowflake reqId:  {request_id}\n"
        f"  Likely cause:     {hint}\n"
        f"  Fix in Snowsight on the account hosting this SPCS app, then retry."
    )


def _spcs_gateway_response_hook(response: requests.Response, *args: Any, **kwargs: Any) -> None:
    """`requests` response hook: raise on a gateway-originated auth rejection."""
    _check_spcs_gateway_response(response)


def install_spcs_gateway_auth(session: requests.Session, pat: str) -> None:
    """
    Configure `session` to authenticate to a Snowflake SPCS app gateway.

    Sets the `X-SF-SPCS-Authorization` header on the session
    (so it rides on every request, including the unauthenticated login)
    and registers a response hook
    that raises `SpcsGatewayAuthError` on a gateway rejection.

    Scoping is automatic:
    the client's session only ever talks to its own `base_url`,
    so there is no need to match per-request hosts.
    """
    session.headers[SPCS_GATEWAY_AUTH_HEADER] = f'Snowflake Token="{pat}"'
    session.hooks["response"].append(_spcs_gateway_response_hook)
