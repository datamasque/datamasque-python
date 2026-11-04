import enum

from requests import Response


class DataMasqueException(Exception):
    """Generic exception base class."""


class DataMasqueUserError(DataMasqueException):
    """Raised when error occurs during user creation or configuration."""


class DataMasqueApiError(DataMasqueException):
    """
    Raised when the DataMasque server responds to a request with a non-2xx status code.

    The triggering `Response` is always available on the `.response` attribute,
    so callers can inspect the status code, headers, and body for richer error handling.

    502 Bad Gateway responses are raised as `DataMasqueNotReadyError` instead.
    """

    def __init__(self, message: str, *, response: Response) -> None:
        super().__init__(message)
        self.response = response


class FailedToStartError(DataMasqueApiError):
    """
    Raised when `start_masking_run` fails to create the run.

    Inherits `.response` from `DataMasqueApiError`,
    so callers can read the server's status code and error body directly.
    """


class InvalidRulesetError(FailedToStartError):
    """Specific error for when runs fail to start due to having an invalid ruleset."""


class InvalidLibraryError(FailedToStartError):
    """Specific error for when runs fail to start due to having an invalid ruleset library."""


class InvalidDiscoveryConfigError(FailedToStartError):
    """
    Raised when a discovery run fails to start because the referenced config is present but unusable.

    The config exists but is not in a `valid` validation state, or its YAML is rejected when the run starts.
    A config that cannot be found raises `DiscoveryConfigNotFoundError` instead.
    """


class DiscoveryConfigNotFoundError(FailedToStartError):
    """
    Raised when a discovery run references a discovery config that cannot be found.

    The config does not exist or is the wrong type for the run.
    """


class RGConfigNotFoundError(DataMasqueApiError):
    """
    Raised when ruleset generation references an RG config that cannot be found.

    The config does not exist, or has been deleted.
    """


class InstanceSecretLinkErrorCode(enum.Enum):
    """Why a step of instance secret linking failed."""

    malformed_code = "malformed_code"
    """The code is not a valid link code."""
    newer_link_format = "newer_link_format"
    """The code was created by a newer version of DataMasque."""
    wrong_code_kind = "wrong_code_kind"
    """A response code was given where a request code was expected, or the other way round."""
    invalid_public_key = "invalid_public_key"
    """The request code contains a key that is not valid."""
    own_request = "own_request"
    """The linking request was sent to the same DataMasque instance that generated it."""
    request_expired = "request_expired"
    """The request code has expired. Create a new request code."""
    unknown_request = "unknown_request"
    """The request that matches the response code was not found."""
    unseal_failed = "unseal_failed"
    """The instance secret in the response code could not be opened."""
    invalid_secret = "invalid_secret"
    """The response code contains an invalid instance secret."""
    same_secret = "same_secret"
    """This DataMasque instance already has the same instance secret as the other DataMasque instance."""
    active_runs = "active_runs"
    """Runs are in progress. Cancel them or wait for them to finish."""
    verification_stale = "verification_stale"
    """The instance secret or its history changed after the response code was verified. Verify the response again."""
    host_header_too_long = "host_header_too_long"
    """The hostname used to access DataMasque is longer than 253 characters, the longest that DNS allows."""


class InstanceSecretLinkError(DataMasqueApiError):
    """
    Raised when a step of instance secret linking fails, for example because a code has expired.

    The reason is on the `.code` attribute, and the message says what to do next.
    """

    def __init__(self, message: str, *, code: InstanceSecretLinkErrorCode, response: Response) -> None:
        super().__init__(message, response=response)
        self.code = code


class DataMasqueTransportError(DataMasqueException):
    """
    Raised when a request to the DataMasque server fails before any response is received.

    Covers connection refused, timeout, DNS failure, SSL handshake failure,
    and similar transport-layer errors.
    The originating `requests` exception is chained via `__cause__`.
    """


class DataMasqueNotReadyError(DataMasqueException):
    """Raised when the DataMasque server is not healthy, normally because it is still starting up."""


class AsyncRulesetGenerationInProgressError(DataMasqueException):
    """Raised when attempting to retrieve results from a ruleset generation request that has not yet completed."""


class DataMasqueIfmError(DataMasqueException):
    """Generic base exception for IFM (in-flight masking) client errors."""


class IfmAuthError(DataMasqueIfmError):
    """Raised when the IFM client cannot obtain or refresh a JWT (e.g. invalid credentials, missing scope)."""


class SpcsGatewayAuthError(DataMasqueException):
    """
    Raised when a Snowflake SPCS app gateway rejects the configured `spcs_pat`.

    The message includes the Snowflake-provided detail, request id,
    and a hint at the likely cause
    (for example an expired token, or a network policy that excludes your IP).

    Deliberately a direct subclass of `DataMasqueException` rather than
    `DataMasqueApiError`:
    the client's 401 re-authenticate-and-retry path keys off `DataMasqueApiError`,
    so keeping this outside that subtree
    ensures a gateway rejection aborts immediately instead of looping.
    """


class RunNotCancellableError(DataMasqueUserError):
    """
    Raised when `cancel_run` is called against a run that is no longer eligible for cancellation.

    Typically this happens when the run is already finished, failed, or in the cancelling state itself.
    """
